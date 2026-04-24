"""Tab-proxy mode — persistent browser tabs via zendriver.

zendriver is a fork of nodriver with faster CDP schema updates. Same API
surface; chosen here because Chrome 147 trips up nodriver 0.48's bindings
with KeyError spam on Network.requestWillBeSentExtraInfo events.

Runs a second Chrome process (separate from the undetected-chromedriver
singleton in app.py) dedicated to keeping live-stream tabs open. Used
for CDNs with session-bound m3u8 tokens that the capture-and-replay
architecture cannot handle.

Single-slot: at most one tab is open at any time. Opening a new tab
closes any existing one. Idle timeout of 120s closes an unused tab on
the next endpoint call (lazy cleanup — no background task).

Captures both playlists and segment bytes from Chrome's CDP network
events so channelarr can relay them to IPTV clients without needing to
replay session-locked CDN tokens.
"""

import asyncio
import base64
import logging
import os
import time
from collections import OrderedDict, deque

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Config ─────────────────────────────────────────────────────────────
_PROFILE_DIR = os.getenv("NODRIVER_PROFILE_DIR", "/data/nodriver-profile")
_IDLE_TIMEOUT = int(os.getenv("TAB_IDLE_TIMEOUT", "120"))
_OPEN_TIMEOUT_DEFAULT = int(os.getenv("TAB_OPEN_TIMEOUT", "60"))
# Cap segment cache per tab. Live HLS segments are typically 2-5s each;
# 60 of them ≈ 2-5 minutes of buffered playback, ~300MB worst case at
# 1080p. Chrome evicts response bodies on its own schedule anyway — we
# fetch them into this cache as soon as loadingFinished fires.
_SEGMENT_CACHE_SIZE = int(os.getenv("TAB_SEGMENT_CACHE_SIZE", "60"))
_SEGMENT_WAIT_S = float(os.getenv("TAB_SEGMENT_WAIT_S", "3.0"))

M3U8_PATTERNS = ("m3u8", "application/x-mpegurl", "application/vnd.apple.mpegurl")
SEGMENT_EXTS = (".ts", ".m4s", ".aac", ".mp4", ".mp3", ".fmp4")

SKIP_PHRASES = [
    "premium only", "premium members only",
    "subscribe to watch", "upgrade to watch",
    "unlock this stream",
    "live stream starting soon", "stream starting soon",
    "event has not started", "stream will begin shortly",
    "broadcast will begin", "upcoming",
    # End-of-stream wording — stream card replaces the player after the
    # event finishes; we want to skip fast rather than wait for a
    # manifest that will never arrive.
    "stream has ended", "stream ended",
    "event has ended", "match has ended",
    "game has ended", "broadcast has ended",
    "event is over", "game is over",
]

# ── State (module-level singletons) ────────────────────────────────────
_browser = None
_browser_lock = asyncio.Lock()
_tabs: dict[str, dict] = {}  # tab_id -> state dict
_tabs_lock = asyncio.Lock()


# ── Request models ─────────────────────────────────────────────────────
class TabOpenRequest(BaseModel):
    url: str
    tab_id: str
    timeout: int = _OPEN_TIMEOUT_DEFAULT
    # Optional plugin-driven behavior config. Keys handled:
    #   "dismiss_modals": bool
    #   "click_sequence": list of {"action": ..., ...} dicts
    # See core/tab_proxy_config.py for the schema. When absent, the
    # sidecar uses its built-in defaults.
    config: dict | None = None


# ── Browser lifecycle ──────────────────────────────────────────────────
def _clear_profile_locks():
    """Chrome leaves SingletonLock/Cookie/Socket on unclean shutdown (container
    kill, OOM, crash). On the next start it refuses to launch with 'Failed to
    connect to browser'. Clearing them is safe — they're just lockfiles."""
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        p = os.path.join(_PROFILE_DIR, name)
        try:
            if os.path.lexists(p):
                os.remove(p)
                logger.info("[TAB-PROXY] removed stale %s", name)
        except OSError as e:
            logger.warning("[TAB-PROXY] failed to remove %s: %s", name, e)


async def _get_browser():
    """Return the zendriver browser singleton, starting on first use.

    If the browser instance is dead, the next operation on it will raise;
    we catch that at the callsite and recreate via _reset_browser()."""
    global _browser
    async with _browser_lock:
        if _browser is not None:
            return _browser
        import zendriver as nd
        os.makedirs(_PROFILE_DIR, exist_ok=True)
        _clear_profile_locks()
        logger.info("[TAB-PROXY] Starting zendriver Chrome (profile=%s)", _PROFILE_DIR)
        _browser = await nd.start(
            user_data_dir=_PROFILE_DIR,
            headless=False,
            no_sandbox=True,
            browser_args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--autoplay-policy=no-user-gesture-required",
                "--mute-audio",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                # Site isolation DISABLED — cross-origin iframes collapse
                # into the parent target so their Network events reach
                # our handlers. Target auto-attach didn't reliably surface
                # OOPIFs in browser.tabs, so this is the pragmatic path.
                "--disable-site-isolation-trials",
                # Disable Chrome's Private Network Access preflight
                # enforcement and Mixed Content Blocking so iframes
                # served over HTTPS can POST capture chunks back to
                # our HTTP localhost sidecar.
                "--disable-features=IsolateOrigins,site-per-process,"
                "PrivateNetworkAccessSendPreflights,"
                "BlockInsecurePrivateNetworkRequests",
                "--allow-running-insecure-content",
                # Mixed-content / insecure-origin override: treat
                # http://localhost:4445 as a trusted origin so fetch()
                # from https pages isn't blocked.
                "--unsafely-treat-insecure-origin-as-secure=http://localhost:4445",
                # Blunt hammer — disables same-origin policy entirely for
                # this profile. Necessary because localhost POSTs from
                # cross-origin iframes hit a web of overlapping Chrome
                # security policies (mixed content, CORS, private-network-
                # access, subresource-integrity). We're running this in a
                # VPN-isolated container with a separate user-data-dir,
                # so the broader security tradeoff is acceptable.
                "--disable-web-security",
                "--window-size=1280,720",
            ],
        )
        return _browser


async def _reset_browser():
    """Drop the cached browser so the next _get_browser() starts a fresh one."""
    global _browser
    async with _browser_lock:
        b = _browser
        _browser = None
    if b is not None:
        try:
            b.stop()
        except Exception:
            pass


# ── Per-tab network capture ────────────────────────────────────────────
def _classify(url: str, mime: str) -> str:
    """Return 'playlist', 'segment', or 'other' for a given URL+mime."""
    u = url.lower().split("?")[0]
    m = (mime or "").lower()
    if any(p in url.lower() for p in M3U8_PATTERNS) or "mpegurl" in m:
        return "playlist"
    if any(u.endswith(e) for e in SEGMENT_EXTS):
        return "segment"
    if m.startswith("video/") or m.startswith("audio/") or "mp2t" in m:
        return "segment"
    # Some CDNs serve segments as application/octet-stream. Only accept if
    # the URL path also looks segment-ish (contains "seg", "chunk", etc.).
    if "octet-stream" in m and any(h in u for h in ("/seg", "/chunk", "/frag")):
        return "segment"
    return "other"


async def _read_body(tab, cdp_mod, rid):
    """Fetch a response body via CDP. Returns (bytes, is_text) or (None, False)."""
    try:
        result = await tab.send(cdp_mod.network.get_response_body(rid))
    except Exception as e:
        logger.info("[TAB-PROXY] getResponseBody failed for rid=%s: %s", rid, e)
        return None, False
    # Return shape varies across zendriver/nodriver releases; try tuple, object,
    # then the underlying response-body dict.
    body, b64 = None, False
    if isinstance(result, tuple) and len(result) >= 2:
        body, b64 = result[0], result[1]
    elif isinstance(result, dict):
        body = result.get("body") or result.get("Body")
        b64 = result.get("base64Encoded") or result.get("base64_encoded") or False
    else:
        body = getattr(result, "body", None)
        b64 = (getattr(result, "base64_encoded", None)
               or getattr(result, "base64Encoded", None)
               or False)
        if body is None:
            logger.info("[TAB-PROXY] unexpected getResponseBody result type: %s repr=%r",
                        type(result).__name__, result)
    if not body:
        return None, False
    if b64:
        try:
            return base64.b64decode(body), False
        except Exception:
            return None, False
    if isinstance(body, bytes):
        return body, True
    try:
        return body.encode("utf-8", errors="replace"), True
    except Exception:
        return None, False


async def _attach_network_capture(tab, state: dict):
    """Wire up CDP Network events so we keep the latest m3u8 playlists
    AND recent segment bodies in memory, both keyed by URL.

    Response bodies are only available while Chrome has them in memory,
    so we fetch inside loadingFinished and stash — later lookups hit
    our own dict, not Chrome."""
    from zendriver import cdp

    await tab.send(cdp.network.enable())

    req_meta: dict = {}  # rid -> {"url": ..., "mime": ...}

    async def on_request(event):
        try:
            req_meta[event.request_id] = {"url": event.request.url, "mime": ""}
        except Exception:
            pass

    async def on_response(event):
        try:
            meta = req_meta.setdefault(event.request_id, {"url": "", "mime": ""})
            meta["url"] = event.response.url or meta["url"]
            meta["mime"] = (event.response.mime_type or "").lower()
        except Exception:
            pass

    async def on_finished(event):
        try:
            rid = event.request_id
            meta = req_meta.pop(rid, None)
            if not meta or not meta.get("url"):
                return
            url = meta["url"]
            kind = _classify(url, meta.get("mime", ""))
            if kind == "other":
                # Diagnostic: log every non-boring response so we can find
                # where the real video comes from. Skip known boring hosts
                # (analytics, ads, google).
                host = url.split("/")[2] if "//" in url else ""
                boring = any(k in host for k in (
                    "google-analytics", "googletagmanager", "doubleclick",
                    "facebook", "cloudflareinsights", "googlesyndication",
                    "fonts.googleapis", "fonts.gstatic", "i.imgur",
                    "adsco.re", "premiumvertising",
                ))
                if not boring:
                    logger.info("[TAB-PROXY] other: %s (mime=%s)",
                                url[:180], meta.get("mime"))
                return
            logger.info("[TAB-PROXY] %s response: %s (mime=%s)",
                        kind, url[:140], meta.get("mime"))
            body_bytes, was_text = await _read_body(tab, cdp, rid)
            if body_bytes is None:
                logger.debug("[TAB-PROXY] %s body unavailable (rid=%s, url=%s)",
                              kind, rid, url[:80])
                return
            if kind == "playlist":
                try:
                    text = body_bytes.decode("utf-8", errors="replace")
                except Exception as e:
                    logger.debug("[TAB-PROXY] playlist decode failed: %s", e)
                    return
                if "#EXTM3U" not in text:
                    # Upstream 404 / error HTML masquerading as an m3u8 URL.
                    # Log once per distinct URL so we notice dead streams.
                    state.setdefault("_logged_empty", set())
                    if url not in state["_logged_empty"]:
                        state["_logged_empty"].add(url)
                        logger.info("[TAB-PROXY] playlist-shaped URL has no "
                                    "#EXTM3U (len=%d head=%r) url=%s",
                                    len(text), text[:60], url[:160])
                    return
                state["playlists"][url] = text
                is_new = state.get("latest_playlist_url") != url
                state["latest_playlist_url"] = url
                if not state.get("manifest_url"):
                    state["manifest_url"] = url
                    logger.info("[TAB-PROXY] captured manifest_url for %s: %s",
                                state.get("tab_id"), url[:160])
                elif is_new:
                    logger.info("[TAB-PROXY] switched to new playlist url: %s",
                                url[:160])
            elif kind == "segment":
                segs: OrderedDict = state["segments"]
                segs[url] = {"bytes": body_bytes, "mime": meta.get("mime") or "video/mp2t"}
                while len(segs) > _SEGMENT_CACHE_SIZE:
                    segs.popitem(last=False)
        except Exception as e:
            logger.debug("[TAB-PROXY] on_finished error: %s", e)

    tab.add_handler(cdp.network.RequestWillBeSent, on_request)
    tab.add_handler(cdp.network.ResponseReceived, on_response)
    tab.add_handler(cdp.network.LoadingFinished, on_finished)

    # WebSocket diagnostic: some streaming stacks deliver chunks via WS
    # frames, which don't show up in request/response events. Log once
    # per distinct WS URL so we can tell if that's what's happening.
    ws_seen: set = state.setdefault("_ws_logged", set())

    async def on_ws_created(event):
        try:
            url = getattr(event, "url", "")
            if url and url not in ws_seen:
                ws_seen.add(url)
                logger.info("[TAB-PROXY] websocket created: %s", url[:160])
        except Exception:
            pass

    async def on_ws_frame(event):
        # Only log the first frame per connection — full frame logging
        # would flood for high-volume WS streams
        try:
            rid = getattr(event, "request_id", None)
            seen_frames = state.setdefault("_ws_frames_logged", set())
            if rid and rid not in seen_frames:
                seen_frames.add(rid)
                payload = getattr(event, "response", None)
                length = 0
                try:
                    length = len(getattr(payload, "payload_data", "") or "")
                except Exception:
                    pass
                logger.info("[TAB-PROXY] websocket frame rid=%s len=%d",
                            rid, length)
        except Exception:
            pass

    try:
        tab.add_handler(cdp.network.WebSocketCreated, on_ws_created)
        tab.add_handler(cdp.network.WebSocketFrameReceived, on_ws_frame)
    except Exception as e:
        logger.debug("[TAB-PROXY] ws handler registration failed: %s", e)


# ── Fetch-domain interception ──────────────────────────────────────────
# Captures requests that the Network domain misses — WASM-initiated
# fetches, requests from dedicated workers, exotic media pipelines.
# Plugin-opt-in via TAB_PROXY_CONFIG.fetch_intercept — zero activity
# unless a plugin explicitly asks for it.
async def _enable_fetch_interception(tab, state: dict, patterns: list):
    """Turn on CDP Fetch domain on the tab with the given patterns.
    Every matched request is paused at RESPONSE stage, the body is
    read + classified, and the request is immediately continued so
    the page never hangs."""
    from zendriver import cdp

    cdp_patterns = []
    for p in patterns or []:
        url_pat = p.get("url_pattern", "*")
        res_type = p.get("resource_type")
        kwargs = {"url_pattern": url_pat, "request_stage": cdp.fetch.RequestStage.RESPONSE}
        if res_type:
            try:
                kwargs["resource_type"] = cdp.network.ResourceType(res_type)
            except Exception:
                pass
        try:
            cdp_patterns.append(cdp.fetch.RequestPattern(**kwargs))
        except Exception as e:
            logger.debug("[TAB-PROXY] fetch pattern build failed: %s", e)

    if not cdp_patterns:
        return

    try:
        await tab.send(cdp.fetch.enable(
            patterns=cdp_patterns,
            handle_auth_requests=False,
        ))
        logger.info("[TAB-PROXY] fetch interception enabled (%d patterns)",
                    len(cdp_patterns))
    except Exception as e:
        logger.warning("[TAB-PROXY] fetch.enable failed: %s", e)
        return

    async def on_paused(event):
        """Fires at response stage for every matched request. Guaranteed
        to call continueRequest — failures log only."""
        rid = getattr(event, "request_id", None)
        if rid is None:
            return
        try:
            url = ""
            mime = ""
            try:
                url = event.request.url
            except Exception:
                pass
            try:
                # Walk headers for content-type
                for h in (event.response_headers or []):
                    name = (getattr(h, "name", "") or "").lower()
                    if name == "content-type":
                        mime = (getattr(h, "value", "") or "").lower()
                        break
            except Exception:
                pass

            # Diagnostic: log first N distinct Fetch hits regardless of
            # classification so we can see what the Fetch domain surfaces
            # beyond what Network showed.
            _log_first_fetch_hit_diag(state, url, mime)
            kind = _classify(url, mime) if url else "other"
            if kind != "other":
                body_bytes, _was_text = await _read_fetch_body(tab, cdp, rid)
                if body_bytes is not None:
                    _store_body(state, kind, url, mime, body_bytes)
                    _log_first_fetch_hit(state, kind, url, mime)
        except Exception as e:
            logger.debug("[TAB-PROXY] fetch on_paused error: %s", e)
        finally:
            # Always release the request — never hang the page
            try:
                await tab.send(cdp.fetch.continue_request(request_id=rid))
            except Exception as e:
                logger.debug("[TAB-PROXY] fetch continue failed for %s: %s",
                             rid, e)

    tab.add_handler(cdp.fetch.RequestPaused, on_paused)


async def _read_fetch_body(tab, cdp_mod, rid):
    """Fetch-domain body read. Same defensive shape handling as the
    Network version."""
    try:
        result = await tab.send(cdp_mod.fetch.get_response_body(request_id=rid))
    except Exception as e:
        logger.debug("[TAB-PROXY] fetch getResponseBody failed for rid=%s: %s",
                     rid, e)
        return None, False
    body, b64 = None, False
    if isinstance(result, tuple) and len(result) >= 2:
        body, b64 = result[0], result[1]
    elif isinstance(result, dict):
        body = result.get("body") or result.get("Body")
        b64 = result.get("base64Encoded") or result.get("base64_encoded") or False
    else:
        body = getattr(result, "body", None)
        b64 = (getattr(result, "base64_encoded", None)
               or getattr(result, "base64Encoded", None)
               or False)
    if not body:
        return None, False
    if b64:
        try:
            return base64.b64decode(body), False
        except Exception:
            return None, False
    if isinstance(body, bytes):
        return body, True
    try:
        return body.encode("utf-8", errors="replace"), True
    except Exception:
        return None, False


def _store_body(state: dict, kind: str, url: str, mime: str, body_bytes: bytes):
    """Store captured body in the tab's state, matching the Network-path
    storage shape so downstream code (playlist rewriter, segment endpoint)
    doesn't care which CDP domain caught it."""
    if kind == "playlist":
        try:
            text = body_bytes.decode("utf-8", errors="replace")
        except Exception:
            return
        if "#EXTM3U" not in text:
            return
        state["playlists"][url] = text
        state["latest_playlist_url"] = url
        if not state.get("manifest_url"):
            state["manifest_url"] = url
            logger.info("[TAB-PROXY] (fetch) captured manifest_url for %s: %s",
                        state.get("tab_id"), url[:160])
    elif kind == "segment":
        segs: OrderedDict = state["segments"]
        segs[url] = {"bytes": body_bytes, "mime": mime or "video/mp2t"}
        while len(segs) > _SEGMENT_CACHE_SIZE:
            segs.popitem(last=False)


def _log_first_fetch_hit(state: dict, kind: str, url: str, mime: str):
    """Log each distinct URL we catch via Fetch exactly once, so logs
    show what Fetch surfaced that Network missed without flooding."""
    seen = state.setdefault("_fetch_logged", set())
    if url in seen:
        return
    seen.add(url)
    logger.info("[TAB-PROXY] (fetch) %s: %s (mime=%s)", kind, url[:160], mime)


def _log_first_fetch_hit_diag(state: dict, url: str, mime: str, max_count: int = 80):
    """One-off diagnostic: log up to max_count distinct URLs that the
    Fetch domain surfaces so we can see what's happening beyond what
    Network showed. Skips noisy well-known hosts."""
    seen = state.setdefault("_fetch_diag_logged", set())
    if url in seen or len(seen) >= max_count:
        return
    host = url.split("/")[2] if "//" in url else ""
    if any(k in host for k in (
        "google-analytics", "googletagmanager", "doubleclick",
        "cloudflareinsights", "googlesyndication", "fonts.googleapis",
        "fonts.gstatic", "adsco.re", "premiumvertising",
    )):
        return
    seen.add(url)
    logger.info("[TAB-PROXY] (fetch-diag) %s (mime=%s)", url[:180], mime)


# ── Skip detection (paywall / upcoming) ────────────────────────────────
async def _scan_for_skip(tab) -> str | None:
    """Look at main document text for premium/upcoming phrases. Cross-
    origin iframes aren't readable from the parent document — acceptable
    limitation: if the outer page shows nothing useful, we'll time out
    waiting for m3u8, which is a valid skip indicator on its own."""
    try:
        text = await tab.evaluate(
            "document.body ? document.body.innerText : ''"
        ) or ""
        lower = text.lower()
        for phrase in SKIP_PHRASES:
            if phrase in lower:
                return phrase
    except Exception as e:
        logger.debug("[TAB-PROXY] skip scan error: %s", e)
    return None


# ── Deep target auto-attach ────────────────────────────────────────────
async def _enable_browser_discovery(browser):
    """Browser-level: discover all targets so zendriver populates
    browser.tabs with every page and iframe target."""
    try:
        from zendriver import cdp
        conn = getattr(browser, "connection", None)
        if conn is None:
            return
        try:
            await conn.send(cdp.target.set_discover_targets(discover=True))
        except Exception as e:
            logger.debug("[TAB-PROXY] set_discover_targets: %s", e)
    except Exception as e:
        logger.debug("[TAB-PROXY] browser discovery failed: %s", e)


async def _enable_tab_auto_attach(tab):
    """Page-level: auto-attach to this tab's children (iframes, workers).
    Must be called per-tab — the auto-attach scope is the target's own
    children, not a global setting."""
    try:
        from zendriver import cdp
        await tab.send(cdp.target.set_auto_attach(
            auto_attach=True,
            wait_for_debugger_on_start=False,
            flatten=True,
        ))
    except Exception as e:
        logger.debug("[TAB-PROXY] tab set_auto_attach: %s", e)


# ── MediaSource probe ──────────────────────────────────────────────────
# Injects a JS shim that patches SourceBuffer.appendBuffer so every call
# console.logs the byte size. Paired with a Runtime.consoleAPICalled
# handler to capture those logs out of any frame. Tells us if the
# player is being fed chunks via MediaSource (byte-based streaming)
# vs. fetching segments through HTTP directly.
_MEDIASOURCE_PROBE_JS = r"""
(function(){
  if (window.__msProbe) return;
  window.__msProbe = true;
  if (typeof SourceBuffer === 'undefined') return;
  var orig = SourceBuffer.prototype.appendBuffer;
  if (!orig) return;
  SourceBuffer.prototype.appendBuffer = function(buf){
    try {
      var size = 0;
      if (buf) {
        if (typeof buf.byteLength === 'number') size = buf.byteLength;
        else if (buf.buffer && typeof buf.buffer.byteLength === 'number') size = buf.buffer.byteLength;
      }
      console.log('__MSPROBE__ ' + JSON.stringify({
        size: size,
        mime: this.mime || '',
        origin: location.origin || ''
      }));
    } catch(e) {}
    return orig.apply(this, arguments);
  };
  // Also probe URL.createObjectURL to see when a MediaSource blob is
  // registered — that's the signal the player is using MSE
  try {
    var origURL = URL.createObjectURL;
    URL.createObjectURL = function(obj){
      try {
        var kind = obj && obj.constructor ? obj.constructor.name : typeof obj;
        console.log('__MSPROBE__ ' + JSON.stringify({
          kind: 'createObjectURL',
          obj: kind,
          origin: location.origin || ''
        }));
      } catch(e) {}
      return origURL.apply(this, arguments);
    };
  } catch(e) {}
})();
"""


async def _install_mediasource_probe(tab, state: dict):
    """Use Page.addScriptToEvaluateOnNewDocument so the probe runs in
    every frame that loads in this target. Also register a Runtime
    handler to capture the probe's console.log output."""
    try:
        from zendriver import cdp
        await tab.send(cdp.page.enable())
        await tab.send(cdp.runtime.enable())
        await tab.send(cdp.page.add_script_to_evaluate_on_new_document(
            source=_MEDIASOURCE_PROBE_JS,
        ))
        logger.info("[TAB-PROXY] MediaSource probe installed")
    except Exception as e:
        logger.debug("[TAB-PROXY] probe install failed: %s", e)
        return

    # Catch the probe's console.log lines
    async def on_console(event):
        try:
            args = getattr(event, "args", None) or []
            if not args:
                return
            # Concatenate string values of args
            parts = []
            for a in args:
                v = getattr(a, "value", None)
                if v is None:
                    v = getattr(a, "description", None)
                if v is not None:
                    parts.append(str(v))
            msg = " ".join(parts)
            # Route marker-prefixed console output to the sidecar logs so
            # in-tab JS probes (mediasource, capture-stream) are visible
            if "__MSPROBE__" in msg:
                idx = msg.find("__MSPROBE__")
                blob = msg[idx + len("__MSPROBE__"):].strip()
                logger.info("[TAB-PROXY] (ms-probe) %s", blob[:200])
                probe_log = state.setdefault("_ms_probe_log", [])
                probe_log.append({"t": time.time(), "msg": blob})
                if len(probe_log) > 500:
                    del probe_log[:250]
            elif "__CAP__" in msg:
                idx = msg.find("__CAP__")
                blob = msg[idx + len("__CAP__"):].strip()
                logger.info("[TAB-PROXY] (cap-probe) %s", blob[:200])
        except Exception:
            pass

    try:
        from zendriver import cdp
        tab.add_handler(cdp.runtime.ConsoleAPICalled, on_console)
    except Exception as e:
        logger.debug("[TAB-PROXY] console handler registration failed: %s", e)


async def _fix_worker_ws_url(conn, target, ttype: str, state: dict):
    """Zendriver hardcodes `/devtools/page/{target_id}` for every target
    type when building its Connection websocket URL. Dedicated workers
    return 404 on that path. Swap to known-working paths for each worker
    type so Connection.aopen() can succeed."""
    ws = getattr(conn, "websocket_url", None) or str(getattr(conn, "url", ""))
    if not ws:
        return
    # Work out the right path for this worker type
    if "shared" in ttype:
        new_path = "/devtools/shared_worker/"
    elif "service" in ttype:
        new_path = "/devtools/service_worker/"
    else:  # dedicated worker
        new_path = "/devtools/worker/"
    if "/devtools/page/" in ws:
        fixed = ws.replace("/devtools/page/", new_path)
        # Mutate on the Connection — try common attribute names
        for attr in ("websocket_url", "url"):
            if hasattr(conn, attr):
                try:
                    setattr(conn, attr, fixed)
                except Exception:
                    pass
        logger.debug("[TAB-PROXY] worker ws: %s -> %s", ws[-30:], fixed[-30:])


async def _subscribe_workers(browser, state: dict, fetch_patterns=None):
    """Attach Network (and optionally Fetch) to every worker-type target
    Chrome is running. Zendriver's browser.tabs filters these out, but
    browser.targets includes Connections for every target — pages,
    iframes, and workers alike. Workers are where WASM-based streaming
    players typically fetch their chunks, hidden from the page's own
    CDP session."""
    try:
        await browser.update_targets()
    except Exception as e:
        logger.debug("[TAB-PROXY] update_targets failed: %s", e)
        return

    already = state.setdefault("_subscribed_workers", set())
    try:
        all_targets = list(getattr(browser, "targets", []))
        # One-shot diagnostic: log types/URLs of every entry on first
        # call so we can see if workers appear at all here
        if not state.get("_workers_logged_once"):
            state["_workers_logged_once"] = True
            for conn in all_targets:
                target = getattr(conn, "target", None)
                ttype = getattr(target, "type_", None) or getattr(target, "type", None)
                url = getattr(target, "url", "") or ""
                logger.info("[TAB-PROXY] browser.targets entry: type=%s url=%s",
                            ttype, url[:120])
        for conn in all_targets:
            target = getattr(conn, "target", None)
            if target is None:
                continue
            ttype_raw = getattr(target, "type_", None) or getattr(target, "type", None)
            ttype = str(ttype_raw).lower()
            if "worker" not in ttype:
                continue
            tid = getattr(target, "target_id", None) or id(conn)
            if tid in already:
                continue
            url = getattr(target, "url", "") or ""
            logger.info("[TAB-PROXY] attempting attach to worker: %s (type=%s)",
                        url[:100], ttype_raw)

            # zendriver assumes /devtools/page/{tid} for every target type.
            # Dedicated workers reject that with 404 — swap the URI to one
            # of the known working alternatives before Connection tries
            # the handshake.
            try:
                await _fix_worker_ws_url(conn, target, ttype, state)
                await _attach_network_capture(conn, state)
                if fetch_patterns:
                    await _enable_fetch_interception(conn, state, fetch_patterns)
                already.add(tid)
                logger.info("[TAB-PROXY] attached to worker: %s", url[:120])
            except Exception as e:
                logger.warning("[TAB-PROXY] worker attach failed for %s: %s",
                               tid[:16] if isinstance(tid, str) else tid, e)
    except Exception as e:
        logger.debug("[TAB-PROXY] _subscribe_workers failed: %s", e)


async def _subscribe_all_targets(browser, state: dict, already: set,
                                  fetch_patterns=None):
    """Walk browser.tabs and attach network capture to any target we
    haven't subscribed to yet. Key off target id / url to avoid double
    subscribe. Also recursively enables auto-attach on each new target
    so nested iframes under that target also get surfaced. If
    fetch_patterns is given (plugin opted in), also enables Fetch
    interception on each new target — this is how worker/deep-frame
    fetches get caught."""
    try:
        for t in list(browser.tabs):
            tid = getattr(t, "target_id", None) or id(t)
            if tid in already:
                continue
            url = (t.url or "")
            if not url or url.startswith("about:") or url.startswith("chrome"):
                continue
            try:
                await _attach_network_capture(t, state)
                await _enable_tab_auto_attach(t)
                if fetch_patterns:
                    await _enable_fetch_interception(t, state, fetch_patterns)
                already.add(tid)
                logger.info("[TAB-PROXY] subscribed to target: %s", url[:120])
            except Exception as e:
                logger.debug("[TAB-PROXY] attach to %s failed: %s", url[:80], e)
    except Exception as e:
        logger.debug("[TAB-PROXY] subscribe_all failed: %s", e)


# ── Popup-aware click sequence ─────────────────────────────────────────
async def _close_popup_tabs(browser, keep_tab_id: str):
    """Close any tabs in the browser that AREN'T our main play tab.
    Some streaming players open popunder windows on first click; we
    dump them so they don't take focus or consume resources."""
    try:
        closed = 0
        async with _tabs_lock:
            state = _tabs.get(keep_tab_id)
        keep_target = state["tab"] if state else None
        # Give the popup a moment to register as a target
        await asyncio.sleep(0.3)
        for t in list(browser.tabs):
            if t is keep_target:
                continue
            url = (t.url or "")
            # Skip the root about:blank that zendriver starts with
            if url in ("", "about:blank", "chrome://newtab/"):
                continue
            try:
                await t.close()
                closed += 1
            except Exception:
                pass
        if closed:
            logger.info("[TAB-PROXY] closed %d popup tab(s)", closed)
    except Exception as e:
        logger.debug("[TAB-PROXY] popup cleanup failed: %s", e)


async def _cdp_click_iframe_center(tab):
    """Send a trusted browser-level click (mouseMoved → press → release)
    at the first iframe's center. Returns True on success."""
    try:
        from zendriver import cdp
        box = await tab.evaluate(
            "(function(){var f=document.querySelector('iframe');"
            "if(!f)return null;var r=f.getBoundingClientRect();"
            "return JSON.stringify({x:r.left+r.width/2,y:r.top+r.height/2});})()"
        )
        if not (box and isinstance(box, str)):
            logger.debug("[TAB-PROXY] no iframe to click into")
            return False
        import json as _j
        coords = _j.loads(box)
        x, y = float(coords["x"]), float(coords["y"])
        logger.info("[TAB-PROXY] CDP click at iframe (%.0f,%.0f)", x, y)
        await tab.send(cdp.input_.dispatch_mouse_event(
            type_="mouseMoved", x=x, y=y,
        ))
        await asyncio.sleep(0.05)
        await tab.send(cdp.input_.dispatch_mouse_event(
            type_="mousePressed", x=x, y=y,
            button=cdp.input_.MouseButton.LEFT, click_count=1,
        ))
        await asyncio.sleep(0.05)
        await tab.send(cdp.input_.dispatch_mouse_event(
            type_="mouseReleased", x=x, y=y,
            button=cdp.input_.MouseButton.LEFT, click_count=1,
        ))
        return True
    except Exception as e:
        logger.debug("[TAB-PROXY] cdp iframe-center click failed: %s", e)
        return False


# Default click sequence — works for sites that (a) have a cross-origin
# iframe player, (b) gate the first click with a popunder. Plugins can
# override via TAB_PROXY_CONFIG.click_sequence.
# ── Phase C capture JS ─────────────────────────────────────────────────
# Hooks the <video> element and pipes captureStream() → MediaRecorder →
# HTTP POST to the sidecar. Runs inside the tab (main doc + same-origin
# iframes it can reach via contentDocument). Works regardless of how
# the bytes got into the video element — bypasses the entire capture
# problem for DRM-free playback. Trades CPU (re-encode inside Chrome)
# for source-agnostic compatibility.
_CAPTURE_STREAM_JS = r"""
(function(){
  if (window.__capInstalled) return;
  window.__capInstalled = true;
  var TAB_ID = '__TAB_ID__';
  var POST = 'http://localhost:4445/tab/' + TAB_ID + '/capture-chunk';

  function findPlayingVideo(){
    var list = [];
    try { list = list.concat(Array.prototype.slice.call(document.querySelectorAll('video'))); } catch(e){}
    try {
      var ifs = document.querySelectorAll('iframe');
      for (var i=0;i<ifs.length;i++){
        try {
          var d = ifs[i].contentDocument;
          if (d) list = list.concat(Array.prototype.slice.call(d.querySelectorAll('video')));
        } catch(e){}
      }
    } catch(e){}
    for (var i=0;i<list.length;i++){
      var v = list[i];
      if (v.readyState >= 2 && !v.paused && v.videoWidth > 0 && v.videoHeight > 0) {
        return v;
      }
    }
    return null;
  }

  function attach(video){
    if (video.__cap) return true;
    video.__cap = true;
    var stream;
    try { stream = video.captureStream ? video.captureStream() : video.mozCaptureStream(); }
    catch(e){ console.log('__CAP__ captureStream err: ' + e.message); return false; }
    if (!stream) { console.log('__CAP__ no stream'); return false; }
    var mimes = [
      'video/webm;codecs=vp9,opus',
      'video/webm;codecs=vp8,opus',
      'video/webm'
    ];
    var mime = '';
    for (var i=0;i<mimes.length;i++){
      if (window.MediaRecorder && MediaRecorder.isTypeSupported(mimes[i])) { mime = mimes[i]; break; }
    }
    if (!mime) { console.log('__CAP__ no supported webm mime'); return false; }
    var rec;
    try { rec = new MediaRecorder(stream, {mimeType: mime, videoBitsPerSecond: 4000000}); }
    catch(e){ console.log('__CAP__ MR ctor err: ' + e.message); return false; }
    var seq = 0;
    rec.ondataavailable = function(ev){
      if (!ev.data || !ev.data.size) return;
      var n = seq++;
      ev.data.arrayBuffer().then(function(ab){
        fetch(POST, {
          method: 'POST',
          body: ab,
          headers: {
            'Content-Type': 'application/octet-stream',
            'X-Chunk-Seq': String(n),
            'X-Chunk-Mime': mime,
          },
          // no-cors would block our custom headers; sidecar serves CORS
        }).catch(function(err){
          console.log('__CAP__ post err: ' + err.message);
        });
      });
    };
    rec.onerror = function(ev){ console.log('__CAP__ MR err: ' + (ev.error && ev.error.name)); };
    rec.start(1000);  // one cluster per second
    console.log('__CAP__ started mime=' + mime + ' w=' + video.videoWidth + ' h=' + video.videoHeight);
    window.__capRec = rec;
    return true;
  }

  var attempts = 0;
  var MAX_ATTEMPTS = 240;  // 120s — video player may take a while
  var timer = setInterval(function(){
    attempts++;
    var v = findPlayingVideo();
    if (v && attach(v)) { clearInterval(timer); return; }
    if (attempts >= MAX_ATTEMPTS) {
      clearInterval(timer);
      console.log('__CAP__ timeout — no playing video found');
    }
  }, 500);
})()
"""


_DEFAULT_CLICK_SEQUENCE = [
    {"action": "iframe_dom_click"},          # click player selectors inside same-origin iframe
    {"action": "click_iframe_center"},       # 1st CDP click → popunder bait
    {"action": "delay", "seconds": 1.0},
    {"action": "close_popups"},
    {"action": "click_iframe_center"},       # 2nd CDP click → real play
    {"action": "delay", "seconds": 0.5},
    {"action": "close_popups"},
]


async def _run_click_sequence(tab, browser, tab_id: str, sequence: list):
    """Execute a plugin-declared click sequence. Each step is a dict with
    an 'action' key. Unknown actions are skipped with a debug log."""
    for step in sequence or []:
        action = (step.get("action") or "").strip()
        try:
            if action == "delay":
                await asyncio.sleep(float(step.get("seconds", 0.5)))
            elif action == "close_popups":
                await _close_popup_tabs(browser, tab_id)
            elif action == "dismiss_modals":
                await _dismiss_modals(tab)
            elif action == "click_iframe_center":
                await _cdp_click_iframe_center(tab)
            elif action == "iframe_dom_click":
                result = await tab.evaluate(_IFRAME_CLICK_JS)
                if result and isinstance(result, str) and result != "[]":
                    logger.info("[TAB-PROXY] iframe_dom_click: %s", result[:200])
            elif action == "evaluate":
                js = step.get("js", "")
                if js:
                    await tab.evaluate(js)
            elif action == "start_capture_stream":
                # No-op here — capture-stream JS is installed earlier in
                # tab_open via Page.addScriptToEvaluateOnNewDocument so
                # it runs in every frame (including cross-origin ones
                # we couldn't reach with tab.evaluate). Kept as a valid
                # action so plugin configs are forward-compatible.
                logger.debug("[TAB-PROXY] start_capture_stream: already installed")
            else:
                logger.debug("[TAB-PROXY] unknown action: %s", action)
        except Exception as e:
            logger.debug("[TAB-PROXY] action %s failed: %s", action, e)


# ── Iframe target helpers ──────────────────────────────────────────────
async def _find_iframe_tab(browser, main_tab, retries: int = 8, delay: float = 0.5):
    """Return a zendriver Tab wrapping the embedded player's iframe target,
    or None if the page doesn't use a cross-origin iframe.

    Cross-origin iframes in Chrome are separate CDP targets (OOPIF).
    zendriver exposes targets via browser.tabs (confusingly, "tabs" includes
    any page-like target — windows, iframes, service workers, etc)."""
    for attempt in range(retries):
        try:
            tabs = list(browser.tabs)
            # On first attempt, log what we see so we can diagnose why no
            # iframe target is detected
            if attempt == 0:
                for t in tabs:
                    ttype = getattr(t, "type_", None) or getattr(t, "type", None)
                    logger.info("[TAB-PROXY] target: type=%s url=%s",
                                ttype, (t.url or "")[:120])
            for t in tabs:
                if t is main_tab:
                    continue
                url = (t.url or "")
                ttype = getattr(t, "type_", None) or getattr(t, "type", None)
                if ttype in ("iframe", "page") and url.startswith("http") \
                        and not url.startswith("about:") \
                        and not url.startswith("chrome"):
                    return t
        except Exception as e:
            logger.debug("[TAB-PROXY] iframe enumeration error: %s", e)
        await asyncio.sleep(delay)
    return None


# ── In-iframe click ────────────────────────────────────────────────────
# For same-origin iframes (e.g. an /embed nested inside the watch page),
# parent JS CAN traverse into the iframe's document. Walk each iframe's
# contentDocument looking for a player-shaped element and click it.
_IFRAME_CLICK_JS = r"""
(function() {
    var out = [];
    var ifs = document.querySelectorAll('iframe');
    var sels = [
        ".vjs-big-play-button", ".jw-icon-display", ".plyr__control--overlaid",
        "button[aria-label*='play' i]", ".play-button", "#play-btn",
        ".btn-play", ".player-container", ".video-player", ".player",
        "#player", ".jw-wrapper", "video"
    ];
    for (var i = 0; i < ifs.length; i++) {
        var d = null;
        try { d = ifs[i].contentDocument; } catch(e) { continue; }
        if (!d) continue;
        for (var j = 0; j < sels.length; j++) {
            var el = d.querySelector(sels[j]);
            if (el) {
                var r = el.getBoundingClientRect();
                if (r.width > 0 && r.height > 0) {
                    try {
                        el.click();
                        if (el.tagName === 'VIDEO' && el.play) {
                            el.play().catch(function(){});
                        }
                        out.push('iframe:' + sels[j]);
                        // Don't break — some players layer controls so
                        // extra clicks on the video element help too
                    } catch(e) {}
                }
            }
        }
    }
    return JSON.stringify(out);
})()
"""


# ── Modal dismissal ────────────────────────────────────────────────────
# Generic JS that runs in the tab to find and click close buttons on any
# visible promotional overlay / modal / popup (ad overlays, newsletter
# popups, GDPR banners, "continue to site" gates, etc).
_MODAL_DISMISS_JS = r"""
(function() {
    var clicked = [];
    // Strategy 1: explicit close buttons by common selector patterns
    var sels = [
        "button[aria-label*='close' i]", "button[aria-label*='dismiss' i]",
        "[aria-label='Close ad' i]",
        ".ad-popup-close", ".modal__close", ".modal-close", ".btn-close",
        ".close-button", ".overlay__close", ".popup-close", ".dialog-close",
        ".close-icon", ".icon-close", ".fa-times", ".fa-xmark",
        "[data-dismiss='modal']", "[data-close='true']",
        "#adPopupClose", "#closeBtn", "#ad-close", ".ad-close",
        "button.close", "a.close"
    ];
    for (var i = 0; i < sels.length; i++) {
        var nodes = document.querySelectorAll(sels[i]);
        for (var j = 0; j < nodes.length; j++) {
            var el = nodes[j];
            var r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0) {
                try { el.click(); clicked.push(sels[i]); } catch(e) {}
            }
        }
    }
    // Strategy 2: any element with aria-label containing 'close' or
    // 'dismiss' that's visible (catches framework-agnostic X buttons)
    var nodes = document.querySelectorAll("[aria-label], [title]");
    for (var i = 0; i < nodes.length; i++) {
        var el = nodes[i];
        var label = ((el.getAttribute('aria-label') || '') + ' ' +
                     (el.getAttribute('title') || '')).toLowerCase();
        if (/\b(close|dismiss)\b/.test(label)) {
            var r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0 && r.width < 100 && r.height < 100) {
                try { el.click(); clicked.push('label:'+label.slice(0,30)); } catch(e) {}
            }
        }
    }
    return JSON.stringify(clicked);
})()
"""


async def _dismiss_modals(tab):
    """Best-effort dismiss of any promotional modals / popups that sit
    between the user and the player (casino ads, newsletter popups, GDPR
    banners, etc). Generic — runs JS in the tab to find close buttons by
    common selector patterns and aria-label hints."""
    # Escape first — fastest path for well-behaved modals
    try:
        from zendriver import cdp
        await tab.send(cdp.input_.dispatch_key_event(
            type_="keyDown", key="Escape", code="Escape",
            windows_virtual_key_code=27, native_virtual_key_code=27,
        ))
        await tab.send(cdp.input_.dispatch_key_event(
            type_="keyUp", key="Escape", code="Escape",
            windows_virtual_key_code=27, native_virtual_key_code=27,
        ))
        await asyncio.sleep(0.2)
    except Exception as e:
        logger.debug("[TAB-PROXY] escape dispatch failed: %s", e)

    # Then the JS sweep — much more reliable than tab.select() for this
    try:
        result = await tab.evaluate(_MODAL_DISMISS_JS)
        if result and isinstance(result, str) and result != "[]":
            logger.info("[TAB-PROXY] dismissed modals: %s", result[:200])
    except Exception as e:
        logger.debug("[TAB-PROXY] modal dismiss eval failed: %s", e)


# ── Click-play fallback ────────────────────────────────────────────────
async def _try_click_play_on(tab):
    """Best-effort click on a play button so autoplay-locked players
    start fetching segments. Must be called on the tab that owns the
    player's DOM — for iframe-embedded players, that's the iframe Tab."""
    selectors = [
        ".vjs-big-play-button", ".jw-icon-display", ".plyr__control--overlaid",
        "button[aria-label*='play' i]", ".play-button", "#play-btn",
        ".btn-play", ".video-player", ".player", "#player", ".jw-wrapper",
        "video",
    ]
    for sel in selectors:
        try:
            el = await tab.select(sel, timeout=1)
            if el:
                try:
                    await el.click()
                    logger.info("[TAB-PROXY] clicked %s", sel)
                    await asyncio.sleep(1)
                    return
                except Exception:
                    continue
        except Exception:
            continue
    try:
        await tab.evaluate(
            "var v=document.querySelector('video');"
            "if(v){v.click();v.play&&v.play().catch(function(){});}"
        )
    except Exception:
        pass


# ── Idle cleanup (lazy, runs on every endpoint call) ───────────────────
async def _cleanup_idle():
    """Close tabs whose last_access is older than _IDLE_TIMEOUT."""
    now = time.time()
    to_close: list[str] = []
    async with _tabs_lock:
        for tab_id, st in list(_tabs.items()):
            if now - st.get("last_access", now) > _IDLE_TIMEOUT:
                to_close.append(tab_id)
    for tab_id in to_close:
        logger.info("[TAB-PROXY] idle timeout, closing tab %s", tab_id)
        await _close_tab(tab_id)


async def _close_tab(tab_id: str):
    async with _tabs_lock:
        state = _tabs.pop(tab_id, None)
    if not state:
        return
    tab = state.get("tab")
    if tab is None:
        return
    try:
        await tab.close()
    except Exception as e:
        logger.debug("[TAB-PROXY] tab.close failed: %s", e)


# ── Endpoints ──────────────────────────────────────────────────────────
@router.post("/tab/open")
async def tab_open(req: TabOpenRequest):
    """Close any existing tab (single-slot), open a new one, navigate,
    wait up to req.timeout seconds for an m3u8 to appear. Returns the
    manifest URL Chrome fetched (for observability — clients should
    fetch playlist via /tab/{id}/playlist, not this URL)."""
    await _cleanup_idle()

    # Single-slot: evict any existing tab
    async with _tabs_lock:
        existing_ids = list(_tabs.keys())
    for tid in existing_ids:
        if tid != req.tab_id:
            logger.info("[TAB-PROXY] single-slot: closing %s for %s", tid, req.tab_id)
            await _close_tab(tid)

    # If the same tab_id is already open, reuse it
    async with _tabs_lock:
        if req.tab_id in _tabs:
            st = _tabs[req.tab_id]
            st["last_access"] = time.time()
            return {
                "ok": True,
                "tab_id": req.tab_id,
                "manifest_url": st.get("manifest_url"),
                "reused": True,
            }

    browser = await _get_browser()
    # Browser-level: discover all targets so zendriver's tabs collection
    # populates as iframes/children spawn.
    await _enable_browser_discovery(browser)

    # Create the tab at about:blank FIRST so we can install Page-scoped
    # scripts (capture-stream, mediasource probe) before the target URL
    # loads. Page.addScriptToEvaluateOnNewDocument only affects *future*
    # document loads, so installing after navigation would miss the
    # main document and iframes.
    try:
        tab = await browser.get("about:blank", new_tab=True)
    except Exception as e:
        logger.exception("[TAB-PROXY] tab creation failed")
        raise HTTPException(status_code=502, detail=f"tab create failed: {e}")

    # Page-level: auto-attach to this tab's child iframes/workers
    await _enable_tab_auto_attach(tab)

    # state must exist before probe install since it writes to
    # state["_ms_probe_log"]. Create it here so both probes use it.
    state = {
        "tab_id": req.tab_id,
        "tab": tab,
        "url": req.url,
        "opened_at": time.time(),
        "last_access": time.time(),
        "manifest_url": None,
        "latest_playlist_url": None,
        "playlists": {},
        "segments": OrderedDict(),
        "_subscribed_targets": set(),
        "capture_header": None,
        "capture_clusters": deque(maxlen=60),
        "capture_mime": None,
        "capture_event": asyncio.Event(),
    }
    async with _tabs_lock:
        _tabs[req.tab_id] = state

    # Diagnostic MediaSource probe — installed pre-nav so it reaches
    # every frame including cross-origin ones.
    await _install_mediasource_probe(tab, state)

    # Install capture-stream JS BEFORE navigation so every frame (main
    # document + cross-origin iframes) runs it on load. Only for webm
    # mode — HLS mode stays on the Network/Fetch capture path.
    if (req.config or {}).get("stream_type") == "webm":
        try:
            from zendriver import cdp
            js = _CAPTURE_STREAM_JS.replace("__TAB_ID__", req.tab_id)
            await tab.send(cdp.page.enable())
            await tab.send(cdp.page.add_script_to_evaluate_on_new_document(
                source=js,
            ))
            logger.info("[TAB-PROXY] capture-stream JS installed on all "
                        "frames for %s", req.tab_id)
        except Exception as e:
            logger.warning("[TAB-PROXY] capture-stream install failed: %s", e)

    # Now navigate to the real URL; all pre-navigation scripts fire in
    # every frame as they load.
    try:
        await tab.get(req.url)
    except Exception as e:
        logger.exception("[TAB-PROXY] tab.get failed for %s", req.url)
        raise HTTPException(status_code=502, detail=f"navigation failed: {e}")

    await _attach_network_capture(tab, state)
    state["_subscribed_targets"].add(getattr(tab, "target_id", None) or id(tab))

    # Plugin-opt-in Fetch-domain interception. Catches requests the
    # Network domain doesn't — WASM / worker fetches. Stays off unless
    # the plugin declares fetch_intercept.
    cfg_fetch = (req.config or {}).get("fetch_intercept")
    if cfg_fetch:
        await _enable_fetch_interception(tab, state, cfg_fetch)

    # Early skip check (premium / upcoming) on the outer document
    await asyncio.sleep(2)  # let the outer page paint
    skip = await _scan_for_skip(tab)
    if skip:
        await _close_tab(req.tab_id)
        return {"ok": False, "error": f"Skipped: {skip}"}

    # Promotional modals (casino ads, newsletter popups, GDPR banners)
    # intercept clicks before they reach the player. Runs by default;
    # plugin can disable with "dismiss_modals": False.
    cfg = req.config or {}
    if cfg.get("dismiss_modals", True):
        await _dismiss_modals(tab)
        # A second pass after a short wait catches modals that animate in
        # late. Plugins can override timing by putting dismiss_modals +
        # delay in their click_sequence directly.
        await asyncio.sleep(1.5)
        await _dismiss_modals(tab)

    # If the iframe happens to be a cross-origin OOPIF (separate CDP
    # target), attach capture there too — we still need its network
    # events. Shape-independent of click strategy.
    iframe_tab = await _find_iframe_tab(browser, tab)
    if iframe_tab is not None:
        logger.info("[TAB-PROXY] attached to iframe target: %s",
                    (iframe_tab.url or "")[:120])
        await _attach_network_capture(iframe_tab, state)

    # Execute the plugin-declared click sequence (or the default). All
    # site-specific behavior lives here as data, not code.
    sequence = cfg.get("click_sequence") or _DEFAULT_CLICK_SEQUENCE
    await _run_click_sequence(tab, browser, req.tab_id, sequence)

    # Wait for m3u8 to appear. Nested iframe targets (embedsports.top,
    # any ad frames) are discovered asynchronously; poll browser.tabs
    # while we wait so their Network events start reaching us as soon
    # as Chrome attaches them.
    # Success condition depends on the plugin's stream model:
    # - HLS (default): wait for manifest_url + playlists from upstream
    # - WebM capture: wait for the first MediaRecorder chunk to arrive
    #   from the in-tab JS shim (capture_header is set).
    is_webm = (req.config or {}).get("stream_type") == "webm"

    deadline = time.time() + req.timeout
    last_subscribe = 0.0
    while time.time() < deadline:
        if time.time() - last_subscribe > 1.0:
            await _subscribe_all_targets(
                browser, state, state["_subscribed_targets"],
                fetch_patterns=cfg_fetch,
            )
            await _subscribe_workers(browser, state, fetch_patterns=cfg_fetch)
            last_subscribe = time.time()

        hls_ready = state.get("manifest_url") and state["playlists"]
        webm_ready = state.get("capture_header") is not None

        if (is_webm and webm_ready) or (not is_webm and hls_ready):
            await _subscribe_all_targets(
                browser, state, state["_subscribed_targets"],
                fetch_patterns=cfg_fetch,
            )
            await _subscribe_workers(browser, state, fetch_patterns=cfg_fetch)
            return {
                "ok": True,
                "tab_id": req.tab_id,
                "manifest_url": state.get("manifest_url"),
                "stream_type": "webm" if is_webm else "hls",
                "reused": False,
            }
        await asyncio.sleep(0.3)

    logger.warning("[TAB-PROXY] no m3u8 captured for %s within %ds", req.url, req.timeout)
    await _close_tab(req.tab_id)
    return {"ok": False, "error": f"No manifest appeared within {req.timeout}s"}


@router.get("/tab/{tab_id}/playlist")
async def tab_playlist(tab_id: str, url: str | None = None):
    """Return the latest HLS playlist body. If `url` is given, return the
    body for that specific playlist (variants). Without url, returns the
    most recently updated playlist (the player's current rendition).

    Response carries X-Source-Url so the caller can urljoin segment URIs."""
    await _cleanup_idle()
    async with _tabs_lock:
        st = _tabs.get(tab_id)
    if not st:
        raise HTTPException(status_code=404, detail="tab not open")
    st["last_access"] = time.time()

    playlists = st.get("playlists") or {}
    src_url = url
    body = None
    if url:
        body = playlists.get(url)
        if not body:
            # Short wait in case Chrome hasn't completed this fetch yet
            for _ in range(10):
                await asyncio.sleep(0.2)
                body = playlists.get(url)
                if body:
                    break
    else:
        src_url = st.get("latest_playlist_url")
        if src_url:
            body = playlists.get(src_url)

    if not body:
        raise HTTPException(status_code=503, detail="no playlist captured yet")
    return Response(
        body,
        media_type="application/vnd.apple.mpegurl",
        headers={"X-Source-Url": src_url or ""},
    )


@router.get("/tab/{tab_id}/segment")
async def tab_segment(tab_id: str, url: str):
    """Return the cached segment bytes Chrome fetched for this URL.

    If Chrome hasn't finished the fetch yet, wait up to TAB_SEGMENT_WAIT_S
    before 404ing. Segments that have been evicted from the tab's FIFO
    cache return 410 (gone)."""
    await _cleanup_idle()
    async with _tabs_lock:
        st = _tabs.get(tab_id)
    if not st:
        raise HTTPException(status_code=404, detail="tab not open")
    st["last_access"] = time.time()

    segs: OrderedDict = st["segments"]
    entry = segs.get(url)
    if entry is None:
        # Segment not fetched yet — poll briefly
        deadline = time.time() + _SEGMENT_WAIT_S
        while time.time() < deadline:
            await asyncio.sleep(0.1)
            entry = segs.get(url)
            if entry is not None:
                break
    if entry is None:
        raise HTTPException(status_code=404, detail="segment not in cache")

    return Response(
        content=entry["bytes"],
        media_type=entry.get("mime") or "video/mp2t",
    )


class EvalRequest(BaseModel):
    expr: str


# ── Phase C capture endpoints ──────────────────────────────────────────
_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-Chunk-Seq, X-Chunk-Mime",
    "Access-Control-Max-Age": "86400",
    # Chrome's Private Network Access: public origin → localhost is
    # blocked unless the server explicitly consents to the preflight.
    "Access-Control-Allow-Private-Network": "true",
}


@router.options("/tab/{tab_id}/capture-chunk")
async def capture_chunk_preflight(tab_id: str):
    return Response(status_code=204, headers=_CORS_HEADERS)


@router.post("/tab/{tab_id}/capture-chunk")
async def capture_chunk(tab_id: str, request: Request):
    """Receive a WebM chunk from the in-tab MediaRecorder and append it
    to the tab's rolling cluster buffer in arrival order. The first
    chunks from MediaRecorder contain the EBML header + Tracks; later
    ones are plain Clusters — we don't need to distinguish them.
    Pings capture_event so feed consumers can wake up."""
    async with _tabs_lock:
        st = _tabs.get(tab_id)
    if not st:
        return Response(status_code=404, headers=_CORS_HEADERS)
    body = await request.body()
    if not body:
        return Response(status_code=204, headers=_CORS_HEADERS)
    mime = request.headers.get("x-chunk-mime") or st.get("capture_mime") or "video/webm"
    st["capture_mime"] = mime
    # Keep the FIRST chunk always — MediaRecorder on Chrome 147 splits
    # the EBML header across a tiny 1-byte first chunk and a larger
    # second chunk, and we need both to produce a valid stream.
    if st.get("capture_header") is None:
        st["capture_header"] = body
        logger.info("[TAB-PROXY] captured WebM header chunk (%d bytes, "
                    "mime=%s, first8=%s) for %s",
                    len(body), mime, body[:8].hex(), tab_id)
    else:
        st["capture_clusters"].append({"t": time.time(), "bytes": body})
    st["capture_event"].set()
    st["last_access"] = time.time()
    return Response(status_code=204, headers=_CORS_HEADERS)


@router.get("/tab/{tab_id}/capture-feed")
async def capture_feed(tab_id: str):
    """Stream the captured WebM to one consumer. Sends the stored
    header first, then any buffered clusters, then waits on
    capture_event to yield new clusters as they land.

    Single-viewer model: if two feeds connect at once they both share
    capture_event, but both drain the same queue — only one gets each
    new chunk. Fine for our homelab one-viewer pattern."""
    async with _tabs_lock:
        st = _tabs.get(tab_id)
    if not st:
        raise HTTPException(status_code=404, detail="tab not open")
    st["last_access"] = time.time()

    async def gen():
        # Wait briefly for header if not present yet
        for _ in range(30):
            if st.get("capture_header") is not None:
                break
            await asyncio.sleep(0.5)
        header = st.get("capture_header")
        if header:
            yield header
        # Replay any buffered clusters so the viewer has ~60s of backfill
        for c in list(st["capture_clusters"]):
            yield c["bytes"]
        last_len = len(st["capture_clusters"])
        # Then wait + yield new clusters as they arrive
        ev = st["capture_event"]
        while True:
            try:
                await asyncio.wait_for(ev.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                break
            ev.clear()
            cur = list(st["capture_clusters"])
            if len(cur) > last_len:
                for c in cur[last_len:]:
                    yield c["bytes"]
                last_len = len(cur)
            st["last_access"] = time.time()

    return StreamingResponse(
        gen(),
        media_type=st.get("capture_mime") or "video/webm",
        headers=_CORS_HEADERS,
    )


@router.get("/tab/{tab_id}/mediasource")
async def tab_mediasource(tab_id: str):
    """Diagnostic: return the MediaSource probe log. Each entry is a
    parsed JSON string from SourceBuffer.appendBuffer / createObjectURL
    calls — shows chunk sizes + origins if the player is feeding an
    MSE pipeline."""
    async with _tabs_lock:
        st = _tabs.get(tab_id)
    if not st:
        raise HTTPException(status_code=404, detail="tab not open")
    st["last_access"] = time.time()
    return {"count": len(st.get("_ms_probe_log") or []),
            "log": st.get("_ms_probe_log") or []}


@router.get("/tab/{tab_id}/targets")
async def tab_targets(tab_id: str):
    """Diagnostic: return the full CDP Target.getTargets result for the
    whole browser — not just what zendriver exposes via browser.tabs.
    Surfaces workers, service workers, shared workers, and any iframe
    targets that weren't promoted to Tab objects."""
    await _cleanup_idle()
    async with _tabs_lock:
        st = _tabs.get(tab_id)
    if not st:
        raise HTTPException(status_code=404, detail="tab not open")
    st["last_access"] = time.time()
    try:
        from zendriver import cdp
        browser = await _get_browser()
        conn = getattr(browser, "connection", None)
        if conn is None:
            raise HTTPException(status_code=502, detail="no browser connection")
        result = await conn.send(cdp.target.get_targets())
        targets = []
        # Result may be a list of TargetInfo or an iterable wrapping it
        infos = result if isinstance(result, (list, tuple)) else \
                getattr(result, "target_infos", None) or []
        for info in infos:
            def _g(key):
                return getattr(info, key, None)
            targets.append({
                "target_id": _g("target_id"),
                "type": _g("type_") or _g("type"),
                "title": _g("title"),
                "url": _g("url"),
                "attached": _g("attached"),
                "opener_id": _g("opener_id"),
                "browser_context_id": _g("browser_context_id"),
            })
        return {"count": len(targets), "targets": targets}
    except Exception as e:
        logger.exception("[TAB-PROXY] targets endpoint failed")
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/tab/{tab_id}/eval")
async def tab_eval(tab_id: str, req: EvalRequest):
    """Diagnostic: evaluate arbitrary JS in the tab. Returns whatever the
    expression produces (coerced to string)."""
    await _cleanup_idle()
    async with _tabs_lock:
        st = _tabs.get(tab_id)
    if not st:
        raise HTTPException(status_code=404, detail="tab not open")
    st["last_access"] = time.time()
    try:
        result = await st["tab"].evaluate(req.expr)
        return {"ok": True, "result": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/tab/{tab_id}/screenshot")
async def tab_screenshot(tab_id: str, full: bool = False):
    """Diagnostic: return a PNG screenshot of the tab. full=true captures the
    full page; default is just the viewport."""
    await _cleanup_idle()
    async with _tabs_lock:
        st = _tabs.get(tab_id)
    if not st:
        raise HTTPException(status_code=404, detail="tab not open")
    st["last_access"] = time.time()
    tab = st["tab"]
    try:
        png = await tab.save_screenshot(filename=":bytes:", full_page=full)
    except Exception:
        try:
            png = await tab.screenshot()  # older nodriver/zendriver API
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"screenshot failed: {e}")
    # zendriver's save_screenshot may return str (base64), bytes, or path
    if isinstance(png, str):
        try:
            png = base64.b64decode(png)
        except Exception:
            # It may have returned a file path; try to read that file
            try:
                with open(png, "rb") as f:
                    png = f.read()
            except Exception:
                raise HTTPException(status_code=502, detail="unexpected screenshot type")
    return Response(content=png, media_type="image/png")


@router.get("/tab/{tab_id}/status")
async def tab_status(tab_id: str):
    await _cleanup_idle()
    async with _tabs_lock:
        st = _tabs.get(tab_id)
    if not st:
        raise HTTPException(status_code=404, detail="tab not open")
    now = time.time()
    return {
        "alive": True,
        "tab_id": tab_id,
        "url": st.get("url"),
        "manifest_url": st.get("manifest_url"),
        "uptime_seconds": int(now - st.get("opened_at", now)),
        "last_access_s_ago": int(now - st.get("last_access", now)),
        "has_playlist": bool(st.get("playlists")),
        "playlist_count": len(st.get("playlists") or {}),
        "segment_count": len(st.get("segments") or {}),
    }


@router.delete("/tab/{tab_id}")
async def tab_delete(tab_id: str):
    async with _tabs_lock:
        existed = tab_id in _tabs
    if existed:
        await _close_tab(tab_id)
    return {"ok": True, "closed": existed}


@router.get("/tabs")
async def tabs_list():
    await _cleanup_idle()
    now = time.time()
    async with _tabs_lock:
        return {
            "tabs": [
                {
                    "tab_id": tid,
                    "url": st.get("url"),
                    "manifest_url": st.get("manifest_url"),
                    "uptime_seconds": int(now - st.get("opened_at", now)),
                    "last_access_s_ago": int(now - st.get("last_access", now)),
                }
                for tid, st in _tabs.items()
            ],
            "idle_timeout_s": _IDLE_TIMEOUT,
        }
