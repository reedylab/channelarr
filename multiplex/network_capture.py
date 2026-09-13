"""
multiplex — capture logic. Ported from selenium-uc/app.py onto nodriver's
event-driven CDP model + session_attach.py's iframe-session-attach (see that
file for why nodriver needs it at all). This module is source-name-free by
design (see project public-repo hygiene convention) — every constant here is
generic (ad-network domains, CSS selectors, CDN/platform vendor names), never
a scraped target site.

This module owns the actual /capture logic and is tab-agnostic by design --
app.py owns the FastAPI shell + browser/tab lifecycle around it. As of
Phase 2, app.py opens a fresh, single-use tab per capture (closed after) to
sidestep a whole class of cross-call state-leak bug found during Phase 1
(see _detach_capture's docstring); nothing in this module assumes or
requires that, though -- it works identically against a short-lived
ephemeral tab or a long-lived reused one.
"""

import asyncio
import base64
import json
import logging
import os
import time

import nodriver as uc
import requests

from session_attach import attach_to_target

logger = logging.getLogger(__name__)

DEBUG_DIR = "/tmp/capdbg"


async def _debug_screenshot(target, debug_dir: str, stage: str):
    """Best-effort checkpoint screenshot for AI/human troubleshooting of a
    stuck or failed capture -- mirrors selenium-uc/app.py's
    _dbg_screenshot() at the same four pipeline stages (01_after_load,
    02_after_iframe_switch, 03_after_click, 04_after_wait), via CDP
    Page.captureScreenshot (nodriver: cdp.page.capture_screenshot(),
    returns a plain base64 PNG string directly per its own type hint --
    not a typed object, so this isn't at risk of the Cookie.from_json()-
    style schema-crash class of bug found earlier). Never raises -- a
    failed screenshot must never fail the actual capture."""
    try:
        b64_png = await target.send(uc.cdp.page.capture_screenshot())
        if not b64_png:
            return
        os.makedirs(debug_dir, exist_ok=True)
        path = os.path.join(debug_dir, f"{stage}.png")
        with open(path, "wb") as f:
            f.write(base64.b64decode(b64_png))
    except Exception as e:
        logger.info("[debug-screenshot] %s failed: %s", stage, e)

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

# How long to actively poll for at least one real content iframe to appear
# before giving up on iframe drilling entirely, matching v1's
# WebDriverWait(browser, 10).until(lambda d: len(d.find_elements(By.TAG_NAME,
# "iframe")) > 0) (selenium-uc/app.py:1231-1233) -- confirmed via real fleet
# testing to be a real gap: a single point-in-time check right after the
# fixed 3s settle sleep can miss a player iframe that injects later (ad-load
# delay, lazy player init), where v1's poll-until-appear-or-timeout would
# still catch it.
IFRAME_APPEAR_TIMEOUT = 10
IFRAME_POLL_INTERVAL = 0.5

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

# Locates the same candidate element CLICK_PLAY_JS would click, but returns
# its viewport center coordinates instead of clicking it directly -- used to
# dispatch a REAL CDP Input.dispatchMouseEvent (see try_click_play) rather
# than a JS-level .click(), which produces an event.isTrusted=false event
# some sites silently ignore for gating playback on a genuine user gesture.
FIND_PLAYABLE_RECT_JS = """
(function(){
  var sels = %s;
  function rectOf(el) {
    var r = el.getBoundingClientRect();
    if (r.width > 0 && r.height > 0) {
      return JSON.stringify({x: r.x + r.width/2, y: r.y + r.height/2});
    }
    return null;
  }
  for (var i=0;i<sels.length;i++){
    try {
      var el = document.querySelector(sels[i]);
      if (el) { var rect = rectOf(el); if (rect) return rect; }
    } catch(e) {}
  }
  var v = document.querySelector('video');
  if (v) { var rect = rectOf(v); if (rect) return rect; }
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


def _is_content_iframe(url_: str) -> bool:
    """A real player iframe is always http(s) — chrome-untrusted://,
    chrome://, devtools:// etc. are Chrome's own internal UI surfaces that
    show up in browser.targets as type_=="iframe" too (confirmed via real
    fleet testing: chrome-untrusted://new-tab-page/one-google-bar showed up
    as a candidate on EVERY real capture that reached iframe drilling,
    every time, regardless of source). These can never serve a manifest, so
    they're excluded outright rather than merely deprioritized like the
    substring-matched skip list below -- letting one through as a "good"
    (non-skip) candidate meant it got tried FIRST, ahead of any real
    candidate, burning a full PER_CANDIDATE_WAIT_SECONDS window every time."""
    return url_.lower().startswith(("http://", "https://"))


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
        and _is_content_iframe(str(getattr(t.target, "url", "") or ""))
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
    aren't used here; raw Runtime.evaluate/Input.dispatchMouseEvent both
    work on either.

    Tries a REAL trusted mouse click first (CDP Input.dispatchMouseEvent at
    the candidate element's coordinates — matches what Selenium's native
    WebElement.click() does under the hood in v1, a genuine OS-level input
    event with event.isTrusted=true), falling back to the plain JS-level
    .click() below only if no clickable candidate was found. This is a real,
    observed gap ported back from comparing against v1: some sites gate
    playback on a trusted user gesture and silently no-op on a synthetic
    click, which JS-level .click() always is."""
    try:
        rect_result = await connection.send(
            uc.cdp.runtime.evaluate(expression=FIND_PLAYABLE_RECT_JS, return_by_value=True)
        )
        remote_obj = rect_result[0] if isinstance(rect_result, tuple) else rect_result
        raw = getattr(remote_obj, "value", None)
        if raw:
            info = json.loads(raw)
            x, y = float(info["x"]), float(info["y"])
            await connection.send(uc.cdp.input_.dispatch_mouse_event(
                type_="mousePressed", x=x, y=y,
                button=uc.cdp.input_.MouseButton.LEFT, click_count=1,
            ))
            await connection.send(uc.cdp.input_.dispatch_mouse_event(
                type_="mouseReleased", x=x, y=y,
                button=uc.cdp.input_.MouseButton.LEFT, click_count=1,
            ))
            return True
    except Exception:
        pass

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


def _raw_cmd(method: str, params: dict | None = None):
    """A minimal CDP-command generator mimicking the shape nodriver's own
    typed cdp.* functions use (yield the command dict once, receive the raw
    response dict back via .send()) but WITHOUT any typed response parsing.

    Exists to work around a real, confirmed nodriver bug: Transaction.__call__
    (nodriver/core/connection.py) does
        try: self.__cdp_obj__.send(response["result"])
        except KeyError as e: raise KeyError(...)
    -- it catches KeyError but only re-raises it, NEVER calls
    self.set_exception(...). nodriver's typed Cookie.from_json() accesses a
    `sameParty` field real Chrome (152/153, as run here) no longer sends,
    raising exactly this KeyError on every single Network.getAllCookies /
    Storage.getCookies response. Since the Transaction's future never gets
    marked done either way, and this KeyError propagates fully unguarded out
    of Connection._listener's `tx(**message)` call (no try/except there
    either -- confirmed via that method's own source), the ENTIRE listener
    task dies silently on the very first cookie extraction attempt.  Every
    subsequent .send() on that connection then hangs forever. This was a
    real, live bug: get_cookies() was silently returning [] on every single
    real capture (masked by its own timeout-and-swallow except clause) --
    not "no cookies present," but "nodriver's own typed parser crashed
    before ever handing back real data."

    Feeding a plain dict-returning generator into the SAME unmodified
    Transaction/Connection.send() machinery sidesteps Cookie.from_json()
    entirely -- we parse the handful of fields we need ourselves, via plain
    dict access below, immune to whatever fields nodriver's stubs expect
    that current Chrome may or may not still send."""
    response = yield {"method": method, "params": params or {}}
    return response


async def get_cookies(connection, scoped: bool = False) -> list[dict]:
    """Two modes, both via _raw_cmd (see its docstring for why):

    scoped=False (default, /capture's use case): Storage.getCookies --
    ALL cookies in the whole browser profile, not just the current page's
    origin. Deliberate, per app.py's own reasoning: segment/key auth often
    lives on a different subdomain than the page, and a per-page-scoped
    lookup would miss it.

    scoped=True (/cookies/youtube's use case): Network.getCookies with no
    `urls` param, which CDP itself defines as "the URLs of the page and all
    of its subframes" -- i.e. scoped to whatever this specific tab is
    currently on. Needed because unscoped Storage.getCookies against a
    long-lived, shared, persistent Chrome profile returns EVERY cookie
    accumulated from every site ever visited in that profile (confirmed:
    234 cookies from unrelated domains on a real run), not just youtube.com's
    -- v1 avoids this for free via Selenium's browser.get_cookies(), which
    is naturally page-scoped by the browser itself; nodriver's raw CDP calls
    have no such default, so the scoping has to be explicit here.

    Timeout-wrapped for defense-in-depth even though _raw_cmd already fixes
    the specific listener-killing bug this call used to hit (see _raw_cmd's
    docstring) -- a separate, different failure (e.g. the mapper.pop()
    KeyError documented elsewhere in this file) could still wedge the
    connection for unrelated reasons. This call runs AFTER the manifest is
    already found (/capture) or after settling on the target page
    (/cookies/youtube), so a timeout here just means slightly less-complete
    metadata, never a lost capture."""
    method = "Network.getCookies" if scoped else "Storage.getCookies"
    try:
        result = await asyncio.wait_for(connection.send(_raw_cmd(method)), timeout=5.0)
        raw = (result or {}).get("cookies") or []
        out = []
        for c in raw:
            expires = c.get("expires")
            out.append({
                "name": c.get("name"),
                "value": c.get("value"),
                "domain": c.get("domain"),
                "path": c.get("path") or "/",
                "secure": bool(c.get("secure")),
                "httpOnly": bool(c.get("httpOnly")),
                "expiry": int(expires) if expires and expires > 0 else None,
                "sameSite": c.get("sameSite"),
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
        # The actual tab/session whose on_response handler found the
        # manifest -- set in _make_handlers right where outcome.ok is set.
        # NOT necessarily the last iframe candidate tried: if the top-level
        # page's own handler succeeds while iframe candidates are still
        # being attempted in sequence, "last iframe tried" would silently
        # extract UA/cookies from the wrong (possibly ad/tracking) target.
        # Real bug, fixed here -- cookies are genuinely load-bearing per the
        # sidecar-2.0 plan's gap-map, not just contract-shape padding.
        self.source = None


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
            outcome.source = capture_tab
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

    Historical note: this was originally load-bearing because Phase 1's
    app.py reused ONE persistent tab across every /capture call -- without
    this, every call's handlers (especially every failed call's, whose
    found_event never got set and so never early-returns) stayed registered
    forever, all still firing on every future navigation's network events,
    a real, confirmed regression (progressively slower captures, eventually
    hanging every subsequent one to its full deadline). Phase 2's app.py
    now opens a fresh, single-use tab per capture and closes it after, which
    already prevents this on its own -- a closed tab's handlers die with it.
    Kept anyway as cheap defense-in-depth (this module doesn't assume
    anything about the caller's tab lifecycle, see the module docstring)."""
    remove = getattr(capture_tab, "remove_handler", None)
    if not remove:
        return
    for evt in (uc.cdp.network.RequestWillBeSent, uc.cdp.network.ResponseReceived):
        try:
            remove(evt)
        except Exception:
            pass
    logger.info("[%s] capture detached", label)


async def run_capture(browser, tab, url: str, timeout: int = 60, switch_iframe: bool = True,
                       debug: bool = False, debug_id: str | None = None) -> CaptureOutcome:
    """Full capture pipeline against an already-open tab (ephemeral or
    persistent, this module doesn't care -- see module docstring): attach
    capture handlers, navigate, drill into an iframe if warranted (trying
    EACH non-skip candidate in turn, not just the first — the iframe-
    selection-robustness improvement from the sidecar-2.0 plan), scan for
    skip-phrases, click play, wait for a manifest.

    debug=True (with a caller-supplied debug_id) saves checkpoint
    screenshots to DEBUG_DIR/{debug_id}/ -- see _debug_screenshot."""
    outcome = CaptureOutcome()
    found_event = asyncio.Event()
    req_headers = {}
    t0 = time.time()
    iframe_sessions = []
    debug_dir = os.path.join(DEBUG_DIR, debug_id) if (debug and debug_id) else None

    def _log(msg):
        logger.info("[run_capture][%.2fs] %s", time.time() - t0, msg)

    try:
        return await _run_capture_body(
            browser, tab, url, timeout, switch_iframe,
            outcome, found_event, req_headers, iframe_sessions, t0, _log,
            debug_dir,
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
                             outcome, found_event, req_headers, iframe_sessions, t0, _log,
                             debug_dir=None):
    await attach_capture(tab, outcome, found_event, req_headers, t0, "top")
    _log(f"navigating to {url[:120]}")
    await tab.get(url)
    _log("tab.get() returned, sleeping 3s to let the page settle")
    await asyncio.sleep(3)
    _log(f"post-settle: found_event.is_set()={found_event.is_set()}")
    if debug_dir:
        await _debug_screenshot(tab, debug_dir, "01_after_load")

    if not found_event.is_set() and switch_iframe:
        # Actively poll for at least one real content iframe to appear
        # (up to IFRAME_APPEAR_TIMEOUT total) instead of a single point-in-
        # time check -- see IFRAME_APPEAR_TIMEOUT's comment for the real
        # regression this fixes.
        candidates = None
        poll_deadline = time.time() + IFRAME_APPEAR_TIMEOUT
        while True:
            candidates = await _find_iframe_candidates(browser)
            if candidates or found_event.is_set() or time.time() >= poll_deadline:
                break
            await asyncio.sleep(IFRAME_POLL_INTERVAL)
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

    if debug_dir:
        # One screenshot regardless of whether iframe drilling happened at
        # all -- matches v1's fixed four-stage naming
        # (02_after_iframe_switch) even though v2 may have tried several
        # candidates in sequence; reflects whichever target (top page or
        # the last-attempted iframe session) is current at this point.
        await _debug_screenshot(iframe_sessions[-1] if iframe_sessions else tab,
                                 debug_dir, "02_after_iframe_switch")

    if not found_event.is_set():
        # nothing drilled found it — try the top page's own click as a
        # cheap fallback (some sources autoplay/click on the top level)
        clicked = await try_click_play(iframe_sessions[-1] if iframe_sessions else tab)
        _log(f"fallback click-play: clicked={clicked}")

    if debug_dir:
        await _debug_screenshot(iframe_sessions[-1] if iframe_sessions else tab,
                                 debug_dir, "03_after_click")

    _log(f"entering final wait (timeout={timeout}s)")
    try:
        await asyncio.wait_for(found_event.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        if not outcome.error:
            outcome.error = f"No manifest found within {timeout}s"
    _log(f"final wait done: ok={outcome.ok} error={outcome.error}")
    _log(f"pending asyncio tasks at completion: {len(asyncio.all_tasks())}")
    if debug_dir:
        await _debug_screenshot(iframe_sessions[-1] if iframe_sessions else tab,
                                 debug_dir, "04_after_wait")

    if outcome.ok:
        # user_agent is cosmetic/dead-data-adjacent, but cookies are
        # genuinely load-bearing (segment/key auth) -- both gathered from
        # outcome.source, the ACTUAL target whose handler found the
        # manifest (set in _make_handlers), not "the last iframe candidate
        # attempted" -- those aren't always the same thing: the top-level
        # page's own handler can succeed while iframe candidates are still
        # being tried in sequence, in which case the last-tried iframe is
        # irrelevant (often an ad/tracking frame) and pulling cookies from
        # it would be wrong. Falls back to the old heuristic only if
        # outcome.source somehow wasn't set (defensive, shouldn't happen).
        # Timeout-wrapped below for the same reason as get_cookies() -- see
        # that function's docstring: a busy real page can silently kill
        # nodriver's listener task via an unguarded KeyError, after which
        # this send() would otherwise hang forever.
        source = outcome.source or (iframe_sessions[-1] if iframe_sessions else tab)
        try:
            ua_result = await asyncio.wait_for(
                source.send(uc.cdp.runtime.evaluate(expression="navigator.userAgent", return_by_value=True)),
                timeout=5.0,
            )
            ua_obj = ua_result[0] if isinstance(ua_result, tuple) else ua_result
            outcome.user_agent = getattr(ua_obj, "value", None)
        except Exception:
            pass
        outcome.cookies = await get_cookies(source)

    return outcome
