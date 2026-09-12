"""
Sidecar 2.0 — capture logic. Ported from selenium-uc/app.py onto nodriver's
event-driven CDP model + session_attach.py's iframe-session-attach (see that
file for why nodriver needs it at all). This module is source-name-free by
design (see project public-repo hygiene convention) — every constant here is
generic (ad-network domains, CSS selectors, CDN/platform vendor names), never
a scraped target site.

Phase 1 scope (single persistent tab, no pool yet — see the sidecar-2.0 plan,
Phase 1): this module owns the actual /capture logic; app.py owns the
FastAPI shell + persistent-browser lifecycle around it.
"""

import asyncio
import base64
import json
import logging
import time

import nodriver as uc
import requests

from session_attach import attach_to_target

logger = logging.getLogger(__name__)

# ── Ported from selenium-uc/app.py — pure Python, no browser-lib dependency ──

MATCH_PATTERNS = ("m3u8", "application/x-mpegurl", "application/vnd.apple.mpegurl")
INCLUDE_TYPES = ("Media", "Fetch", "XHR", "Document", "Other")
# /proxy/ catches HLS playlists served from generic proxy paths whose filename
# has been disguised (.css/.csv/.txt/.json) to evade scrapers that key on
# .m3u8 — the body-sniff below handles validation.
JSON_STREAM_PATTERNS = ("ngtv.io", "/api/", "/media/", "/stream", "anvato", "uplynk", "/proxy/")

IFRAME_SKIP_SUBSTRINGS = (
    "chatango.com", "adbanner", "/ads/", "/ad-", "google.com/recaptcha",
    "doubleclick.net", "googletagmanager.com", "googlesyndication", "googleadservices",
    # Generic auth/SSO-frame patterns — added after the spike picked an SSO
    # iframe instead of the real player on one real source. These are login/
    # identity-provider infrastructure patterns, not target-site names.
    "auth.", "/sso", "sso-frame", "/login", "accounts.google.com",
    # Generic ad-tech/analytics/tag-management iframe vendors observed
    # empirically across real fleet testing — every one of these is common
    # third-party ad/tracking infrastructure embedded on many unrelated
    # sites, not a target site itself. Without these, the try-every-
    # candidate-in-sequence logic below burns its whole per-candidate wait
    # on each of these before ever reaching the real player iframe (real
    # regression observed: a source that resolved in ~5s during Phase 0
    # took 85s+ once several of these were present and untagged).
    "dtscout.com", "lijit.com", "sharethis.com", "crwdcntrl.net",
    "imasdk.googleapis.com", "quantserve.com", "scorecardresearch.com",
    "adsrvr.org", "adnxs.com", "criteo.com", "taboola.com", "outbrain.com",
)
IFRAME_SKIP_SCHEMES = ("javascript:", "about:", "data:", "blob:")

# Bounds for the try-every-iframe-candidate-in-sequence loop in run_capture —
# see that loop's comment for the real regression this guards against.
MAX_IFRAME_CANDIDATES = 5
PER_CANDIDATE_WAIT_SECONDS = 3

PLAY_SELECTORS = (
    ".play-button", ".vjs-big-play-button", ".jw-icon-display", "[class*='play']",
    "button[aria-label*='play' i]", ".btn-play", "#play-btn",
    ".plyr__control--overlaid", "video", ".video-player", ".player", "#player",
    ".jw-wrapper",
)

# Ported verbatim from selenium-uc/app.py::_scan_all_frames_for_skip — each
# phrase's specific false-positive rationale lives in that function's
# comments; reproduced faithfully here, not paraphrased.
SKIP_PHRASES = [
    "premium only", "premium members only",
    "subscribe to watch", "upgrade to watch",
    "unlock this stream",
    "live stream starting soon", "stream starting soon",
    "event has not started", "stream will begin shortly",
    "broadcast will begin",
    # Some sites render "DELAYED START" as a status badge when the broadcast
    # hasn't gone live yet — the player never initializes, so no manifest is
    # ever requested and we'd otherwise wait the full timeout for nothing.
    # Safe here because this only runs inside iframes (where the player +
    # its status overlay live), not the top-level page (where related-game
    # sidebars list other games' "delayed start" badges).
    "delayed start",
    # The bare word 'upcoming' false-triggers on sites with secondary
    # 'Upcoming Listings' sections while a live stream is playing. Match
    # contextual pregame wording instead.
    "upcoming event", "upcoming broadcast", "upcoming stream",
    # Post-game box-score state: 'FINAL' label + summary sections appear on
    # the same game URL after it ends, with no live player. Live pages show
    # the video player instead, never these summary headers.
    "top performers today", "team comparison",
    # End-of-stream wording — page renders a "game over" card instead of the
    # player. Without these, we wait the full deadline scanning for a
    # manifest that will never arrive.
    "stream has ended", "stream ended",
    "event has ended", "match has ended",
    "game has ended", "broadcast has ended",
    "event is over", "game is over",
]

CLICK_PLAY_JS = """
(function(){
  var sels = %s;
  for (var i=0;i<sels.length;i++){
    try {
      var el = document.querySelector(sels[i]);
      if (el) { el.click(); if (el.play) { el.play().catch(function(){}); } return sels[i]; }
    } catch(e) {}
  }
  var v = document.querySelector('video');
  if (v) { v.click(); if (v.play) v.play().catch(function(){}); return 'video-fallback'; }
  return null;
})()
""" % (list(PLAY_SELECTORS),)

SKIP_SCAN_JS = """
(function(){
  var t = (document.body ? document.body.innerText : '') || '';
  return t.toLowerCase();
})()
"""


def _matches(url: str, mime: str = "") -> bool:
    hay = f"{url} {mime}".lower()
    return any(p in hay for p in MATCH_PATTERNS)


def _looks_like_json_stream(url: str) -> bool:
    return any(p in url.lower() for p in JSON_STREAM_PATTERNS)


def _find_m3u8_in_json(obj):
    """Recursively search a parsed JSON object for an m3u8 URL. Ported
    verbatim from selenium-uc/app.py::_find_m3u8_in_json."""
    if isinstance(obj, str):
        if ".m3u8" in obj and obj.startswith("http"):
            return obj
        return None
    if isinstance(obj, dict):
        for v in obj.values():
            result = _find_m3u8_in_json(v)
            if result:
                return result
    if isinstance(obj, list):
        for item in obj:
            result = _find_m3u8_in_json(item)
            if result:
                return result
    return None


def _decode_body(body_str, base64_encoded):
    if not body_str:
        return None
    raw = base64.b64decode(body_str) if base64_encoded else body_str.encode("utf-8", errors="ignore")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1", errors="ignore")
    return text


def is_vod_endlist(body_text: str) -> bool:
    """Ported verbatim from selenium-uc/app.py:1302-1312 — a live playlist
    never contains #EXT-X-ENDLIST; its presence means a fixed-length VOD
    (typically a short outro/replay clip after a game ends). Reject so
    auto-channel-creation doesn't produce ghost channels that immediately
    404. The playlist structure is the only reliable signal — any status
    text overlay lives inside the video frame, not HTML."""
    return "#EXT-X-ENDLIST" in body_text


async def _find_iframe_candidates(browser):
    """Enumerate browser.targets (the UNFILTERED list — browser.tabs
    deliberately filters iframe-type targets out, see session_attach.py's
    module docstring) and rank iframe candidates, same substring-skip
    selection app.py uses, but returning ALL of them in try-order (not just
    the first) — the iframe-selection-robustness improvement from the
    sidecar-2.0 plan (motivated by the Phase 0 spike picking a wrong/SSO
    iframe on one real source). Each returned item is the raw Connection
    object browser.targets holds (has `.target.target_id`), not a bare ID."""
    await browser.update_targets()
    iframe_targets = [
        t for t in browser.targets
        if getattr(t.target, "type_", None) == "iframe"
    ]
    if not iframe_targets:
        return None

    def _skip(url_: str) -> bool:
        low = url_.lower()
        if any(low.startswith(s) for s in IFRAME_SKIP_SCHEMES):
            return True
        return any(s in low for s in IFRAME_SKIP_SUBSTRINGS)

    candidates = [(t, str(getattr(t.target, "url", "") or "")) for t in iframe_targets]
    good = [t for t, u in candidates if not _skip(u)]
    ordered = good + [t for t, u in candidates if _skip(u)]  # skip-matched ones last, still tried
    return ordered  # caller tries each in order, not just the first


async def try_click_play(connection) -> bool:
    """Works uniformly on a full Tab OR a bare attached Connection (iframe
    session) — see session_attach.py for why .select()/.evaluate() (Tab-only)
    aren't used here; raw Runtime.evaluate works on both."""
    try:
        result = await connection.send(
            uc.cdp.runtime.evaluate(expression=CLICK_PLAY_JS, return_by_value=True,
                                     await_promise=True, user_gesture=True)
        )
        remote_obj = result[0] if isinstance(result, tuple) else result
        return bool(getattr(remote_obj, "value", None))
    except Exception:
        return False


async def scan_for_skip_phrase(connection) -> str | None:
    """Ported from selenium-uc/app.py::_scan_all_frames_for_skip — iframe-
    only scanning (the real player + its status overlay live inside the
    iframe; scanning the top-level page produced false positives from
    sidebar/related-content sections on other sources)."""
    try:
        result = await connection.send(
            uc.cdp.runtime.evaluate(expression=SKIP_SCAN_JS, return_by_value=True, timeout=5)
        )
        remote_obj = result[0] if isinstance(result, tuple) else result
        text = getattr(remote_obj, "value", "") or ""
        for phrase in SKIP_PHRASES:
            if phrase in text:
                return phrase
    except Exception:
        pass
    return None


async def get_cookies(connection) -> list[dict]:
    """CDP Network.getAllCookies — not per-domain get_cookies() — deliberately,
    per app.py's own reasoning: segment/key auth often lives on a different
    subdomain than the page, and per-domain lookups miss it."""
    try:
        result = await connection.send(uc.cdp.network.get_all_cookies())
        raw = result if isinstance(result, list) else getattr(result, "cookies", []) or []
        out = []
        for c in raw:
            out.append({
                "name": getattr(c, "name", None),
                "value": getattr(c, "value", None),
                "domain": getattr(c, "domain", None),
                "path": getattr(c, "path", None) or "/",
                "secure": bool(getattr(c, "secure", False)),
                "httpOnly": bool(getattr(c, "http_only", False)),
                "expiry": int(getattr(c, "expires", -1)) if getattr(c, "expires", -1) and getattr(c, "expires", -1) > 0 else None,
                "sameSite": str(getattr(c, "same_site", None)) if getattr(c, "same_site", None) else None,
            })
        return out
    except Exception:
        return []


class CaptureOutcome:
    def __init__(self):
        self.ok = False
        self.error = None
        self.manifest_url = None
        self.body = None
        self.mime = None
        self.headers = None
        self.user_agent = None
        self.referer = None
        self.cookies = []


def _make_handlers(capture_tab, outcome: CaptureOutcome, found_event: asyncio.Event,
                    req_headers: dict, t0: float, label: str):
    """Event-driven capture — replaces app.py's 80ms poll-and-hand-parse-
    get_log("performance") loop with nodriver's real CDP event push. See the
    sidecar-2.0 plan's Phase 0 results for why this is strictly better
    (lower latency, no destructive-drain races, no restart-warmup quirk).

    t0/label are diagnostic-only (timing checkpoints) — added to chase a
    real regression where this pipeline hangs to its full deadline on real
    sources the Phase 0 spike resolves in ~10s. Not load-bearing logic."""

    def _elapsed():
        return f"{time.time() - t0:.2f}s"

    async def on_request(event):
        try:
            resource_type = event.type_.value if getattr(event, "type_", None) else None
            if resource_type and resource_type not in INCLUDE_TYPES:
                return
            req_headers[event.request_id] = {
                "headers": dict(getattr(event.request, "headers", {}) or {}),
                "url": event.request.url,
            }
        except Exception:
            pass

    async def on_response(event):
        if found_event.is_set():
            return
        try:
            rid = event.request_id
            resp = event.response
            url_ = resp.url
            mime = getattr(resp, "mime_type", "") or ""
            if not (_matches(url_, mime) or _looks_like_json_stream(url_)):
                return

            logger.info("[%s][%s] candidate response: %s (mime=%s)", label, _elapsed(), url_[:120], mime)

            meta = req_headers.get(rid, {})
            headers = meta.get("headers", {})

            body_text = None
            # CDP body-fetch FIRST, HTTP short-circuit only as a fallback —
            # matching app.py's actual behavior and the Phase 0 spike (which
            # reliably succeeds via CDP in ~10ms). Getting this order backwards
            # is a real regression that was live here: an unauthenticated
            # plain requests.get() against a real, anti-bot-protected .m3u8
            # URL can itself take the full 10s to resolve (slow-drip/
            # challenge response, not a quick reject) on EVERY matching
            # response — with a live player firing multiple matching
            # requests (master + media playlist, periodic re-polls), trying
            # HTTP first before ever attempting the fast CDP path turned a
            # ~5-10s capture into 85s+. asyncio.to_thread below keeps the
            # (now-fallback-only) HTTP call from blocking the event loop
            # that drives the CDP websocket read loop, but the real fix is
            # the ordering itself.
            cdp_start = time.time()
            try:
                cdp_result = await asyncio.wait_for(
                    capture_tab.send(uc.cdp.network.get_response_body(request_id=rid)),
                    timeout=5.0,
                )
                if cdp_result:
                    body_str, b64 = cdp_result
                    body_text = _decode_body(body_str, b64)
                logger.info("[%s][%s] CDP body-fetch for %s: %s (%.2fs)", label, _elapsed(),
                            url_[:80], "got body" if body_text else "empty/no body", time.time() - cdp_start)
            except Exception as e:
                logger.info("[%s][%s] CDP body-fetch for %s FAILED: %s (%.2fs)", label, _elapsed(),
                            url_[:80], e, time.time() - cdp_start)
                body_text = None

            if body_text is None and ".m3u8" in url_.lower():
                http_start = time.time()
                try:
                    r = await asyncio.to_thread(requests.get, url_, headers=headers, timeout=10)
                    if r.status_code == 200 and "#EXTM3U" in r.text:
                        body_text = r.text
                    logger.info("[%s][%s] HTTP fallback for %s: status=%s (%.2fs)", label, _elapsed(),
                                url_[:80], r.status_code, time.time() - http_start)
                except Exception as e:
                    logger.info("[%s][%s] HTTP fallback for %s FAILED: %s (%.2fs)", label, _elapsed(),
                                url_[:80], e, time.time() - http_start)

            found_manifest_url = url_
            if body_text and "#EXTM3U" not in body_text:
                # Disguised-HLS body-sniff: check first 4096 chars even when
                # URL/mime didn't match MATCH_PATTERNS (catches .css/.csv/.txt-
                # disguised manifests) or a JSON-API wrapping an embedded m3u8.
                if "#EXTM3U" in body_text[:4096]:
                    pass  # already true, kept branch for clarity/symmetry with app.py
                else:
                    try:
                        parsed = json.loads(body_text)
                        embedded = _find_m3u8_in_json(parsed)
                        if embedded:
                            r = await asyncio.to_thread(requests.get, embedded, headers=headers, timeout=10)
                            if r.status_code == 200 and "#EXTM3U" in r.text:
                                body_text = r.text
                                found_manifest_url = embedded
                    except Exception:
                        body_text = None

            if not body_text or "#EXTM3U" not in body_text:
                return

            if is_vod_endlist(body_text):
                outcome.error = "Skipped: stream ended (VOD playlist)"
                found_event.set()
                return

            outcome.ok = True
            outcome.manifest_url = found_manifest_url
            outcome.body = body_text
            outcome.mime = mime or "application/vnd.apple.mpegurl"
            outcome.headers = dict(getattr(resp, "headers", {}) or {})
            outcome.referer = headers.get("Referer") or headers.get("referer")
            logger.info("[%s][%s] SUCCESS: %s", label, _elapsed(), found_manifest_url[:120])
            found_event.set()
        except Exception as e:
            logger.exception("[%s][%s] on_response error", label, _elapsed())
            outcome.error = f"on_response error: {e}"

    return on_request, on_response


async def attach_capture(capture_tab, outcome, found_event, req_headers, t0: float, label: str = "?"):
    on_request, on_response = _make_handlers(capture_tab, outcome, found_event, req_headers, t0, label)
    await capture_tab.send(uc.cdp.network.enable())
    capture_tab.add_handler(uc.cdp.network.RequestWillBeSent, lambda e: asyncio.create_task(on_request(e)))
    capture_tab.add_handler(uc.cdp.network.ResponseReceived, lambda e: asyncio.create_task(on_response(e)))
    logger.info("[%s][%.2fs] capture attached", label, time.time() - t0)


def _detach_capture(capture_tab, label: str = "?"):
    """Undo attach_capture's add_handler calls. Required because nodriver's
    add_handler unconditionally appends to self.handlers[event_type] (never
    dedups, no built-in per-call scoping) -- confirmed via its own source.
    app.py reuses ONE persistent tab across every /capture call, so without
    this, every call's handlers -- especially every failed call's, whose
    found_event never got set and so never early-returns -- stay registered
    forever, all still firing on every future navigation's network events.
    This was a real, confirmed regression: repeated calls against the same
    persistent tab progressively slow down (N stacked handler pairs doing
    matching + CDP get_response_body sends per event) and eventually hang
    every subsequent capture to its full deadline, while a fresh one-off
    tab/browser (zero prior handlers) always succeeds in seconds."""
    remove = getattr(capture_tab, "remove_handler", None)
    if not remove:
        return
    for evt in (uc.cdp.network.RequestWillBeSent, uc.cdp.network.ResponseReceived):
        try:
            remove(evt)
        except Exception:
            pass
    logger.info("[%s] capture detached", label)


async def run_capture(browser, tab, url: str, timeout: int = 60, switch_iframe: bool = True) -> CaptureOutcome:
    """Full capture pipeline against an already-open, persistent tab: attach
    capture handlers, navigate, drill into an iframe if warranted (trying
    EACH non-skip candidate in turn, not just the first — the iframe-
    selection-robustness improvement from the sidecar-2.0 plan), scan for
    skip-phrases, click play, wait for a manifest."""
    outcome = CaptureOutcome()
    found_event = asyncio.Event()
    req_headers = {}
    t0 = time.time()
    iframe_sessions = []

    def _log(msg):
        logger.info("[run_capture][%.2fs] %s", time.time() - t0, msg)

    try:
        return await _run_capture_body(
            browser, tab, url, timeout, switch_iframe,
            outcome, found_event, req_headers, iframe_sessions, t0, _log,
        )
    finally:
        # Always detach, success or failure or exception -- see
        # _detach_capture's docstring for why this is load-bearing on a
        # persistent, cross-call-reused tab.
        _detach_capture(tab, "top")
        registry = getattr(tab, "_session_registry", None)
        if registry:
            for s in iframe_sessions:
                registry.pop(str(s.session_id), None)


async def _run_capture_body(browser, tab, url, timeout, switch_iframe,
                             outcome, found_event, req_headers, iframe_sessions, t0, _log):
    await attach_capture(tab, outcome, found_event, req_headers, t0, "top")
    _log(f"navigating to {url[:120]}")
    await tab.get(url)
    _log("tab.get() returned, sleeping 3s to let the page settle")
    await asyncio.sleep(3)
    _log(f"post-settle: found_event.is_set()={found_event.is_set()}")

    if not found_event.is_set() and switch_iframe:
        candidates = await _find_iframe_candidates(browser)
        _log(f"iframe candidates found: {len(candidates or [])}")
        # Bound worst case regardless of how many iframes a source embeds
        # (ad/tracking iframes not covered by IFRAME_SKIP_SUBSTRINGS are a
        # real, observed failure mode otherwise — a source with a dozen
        # third-party tags could otherwise burn minutes before ever
        # reaching the real player). MAX_IFRAME_CANDIDATES candidates,
        # PER_CANDIDATE_WAIT_SECONDS each, keeps the worst case bounded and
        # small relative to the overall capture timeout.
        for i, candidate in enumerate((candidates or [])[:MAX_IFRAME_CANDIDATES]):
            cand_url = str(getattr(candidate.target, "url", "") or "")
            _log(f"iframe candidate #{i}: {cand_url[:100]}")
            try:
                session = await attach_to_target(tab, candidate.target.target_id)
            except Exception as e:
                _log(f"  attach_to_target failed: {e}")
                continue
            iframe_sessions.append(session)
            skip = await scan_for_skip_phrase(session)
            if skip:
                _log(f"  skip-phrase matched ({skip!r}), trying next candidate")
                continue
            await attach_capture(session, outcome, found_event, req_headers, t0, f"iframe#{i}")
            if not found_event.is_set():
                clicked = await try_click_play(session)
                _log(f"  click-play on iframe#{i}: clicked={clicked}")
            if found_event.is_set():
                break
            try:
                await asyncio.wait_for(found_event.wait(), timeout=PER_CANDIDATE_WAIT_SECONDS)
                break
            except asyncio.TimeoutError:
                _log(f"  iframe#{i} candidate window expired, moving on")
                continue

    if not found_event.is_set():
        # nothing drilled found it — try the top page's own click as a
        # cheap fallback (some sources autoplay/click on the top level)
        clicked = await try_click_play(iframe_sessions[-1] if iframe_sessions else tab)
        _log(f"fallback click-play: clicked={clicked}")

    _log(f"entering final wait (timeout={timeout}s)")
    try:
        await asyncio.wait_for(found_event.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        if not outcome.error:
            outcome.error = f"No manifest found within {timeout}s"
    _log(f"final wait done: ok={outcome.ok} error={outcome.error}")

    if outcome.ok:
        # user_agent + cookies are gathered from whichever target actually
        # succeeded — cheap, no behavioral dependency downstream (both
        # confirmed dead-data-adjacent per the plan; included for contract-
        # shape completeness only).
        source = iframe_sessions[-1] if iframe_sessions else tab
        try:
            ua_result = await source.send(uc.cdp.runtime.evaluate(expression="navigator.userAgent", return_by_value=True))
            ua_obj = ua_result[0] if isinstance(ua_result, tuple) else ua_result
            outcome.user_agent = getattr(ua_obj, "value", None)
        except Exception:
            pass
        outcome.cookies = await get_cookies(source)

    return outcome
