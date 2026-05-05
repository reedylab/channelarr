"""Headless browser sidecar using undetected-chromedriver.

Uses a persistent Chrome session with a profile dir, so cookies, localStorage,
and fingerprint persist across captures. To target sites we look like one
returning user, not a bot army.

Exposes:
  POST /capture {"url": str, "timeout": int, "switch_iframe": bool}
  POST /restart   — forcibly recycle the browser
  GET  /health
"""

import base64
import json
import logging
import os
import re
import subprocess
import threading
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import requests as http_requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

# Tab-proxy subsystem (separate nodriver Chrome, single-slot persistent tabs).
# Imported lazily so a broken nodriver install doesn't crash the capture path.
try:
    from tab_proxy import router as tab_proxy_router
    app.include_router(tab_proxy_router)
    logger.info("Tab-proxy router mounted")
except Exception as e:
    logger.warning("Tab-proxy router unavailable: %s", e)

MATCH_PATTERNS = ("m3u8", "application/x-mpegurl", "application/vnd.apple.mpegurl")
INCLUDE_TYPES = ("Media", "Fetch", "XHR", "Document", "Other")
# /proxy/ catches HLS playlists served from generic proxy paths whose
# filename has been disguised (.css/.csv/.txt/.json) to evade scrapers
# that key on .m3u8. The body still has #EXTM3U; the body-sniff path in
# _wait_for_manifest handles validation.
JSON_STREAM_PATTERNS = ("ngtv.io", "/api/", "/media/", "/stream", "anvato",
                        "uplynk", "/proxy/")

# Persistent browser singleton. --user-data-dir is required — without it,
# chromedriver's perf-log subscription is flaky and captures end up with
# zero Network events. YouTube cookies are re-harvested per download via
# /cookies/youtube; other sources are anonymous.
_PROFILE_DIR = os.getenv("CHROME_PROFILE_DIR", "/data/chrome-profile")
_STARTUP_TIMEOUT = int(os.getenv("CHROME_STARTUP_TIMEOUT", "60"))  # seconds

# Pre-patch chromedriver at import time so uc.Chrome() doesn't re-patch on every call.
# This avoids the "patching driver executable" hang during captures.
try:
    _chrome_binary = os.getenv("CHROME_BINARY", "/usr/bin/google-chrome")
    _chrome_version = None
    _out = subprocess.check_output([_chrome_binary, "--version"], text=True).strip()
    _m = re.search(r"(\d+)\.\d+\.\d+\.\d+", _out)
    if _m:
        _chrome_version = int(_m.group(1))
    _patcher = uc.Patcher(version_main=_chrome_version)
    _patcher.auto()
    logger.info("Pre-patched chromedriver for Chrome %s", _chrome_version)
except Exception as e:
    logger.warning("Chromedriver pre-patch failed (will patch on first use): %s", e)
_browser_lock = threading.RLock()
_browser = None
_capture_count = 0


class CaptureRequest(BaseModel):
    url: str
    timeout: int = 60
    switch_iframe: bool = True
    debug: bool = False


def _kill_chrome_processes():
    """Kill any orphaned chrome/chromedriver processes."""
    for name in ("chrome", "chromedriver"):
        try:
            subprocess.run(["pkill", "-9", "-f", name], capture_output=True, timeout=5)
        except Exception:
            pass


def _clear_chrome_singleton_locks():
    """Remove stale Chrome singleton lock files from the profile dir.

    Chrome leaves SingletonLock/Cookie/Socket behind on unclean shutdown
    (container kill, OOM, crash). On the next start it sees them and refuses
    to launch with 'cannot connect to chrome', because it thinks another
    instance owns the profile. Clearing them is safe — they're just lockfiles.
    """
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        p = os.path.join(_PROFILE_DIR, name)
        try:
            if os.path.lexists(p):
                os.remove(p)
                logger.info("Removed stale %s", name)
        except OSError as e:
            logger.warning("Failed to remove %s: %s", name, e)


def _make_browser():
    """Build a fresh undetected Chrome instance with a persistent profile.

    Runs uc.Chrome() in a thread with a timeout so a hung startup
    doesn't permanently block the sidecar.
    """
    os.makedirs(_PROFILE_DIR, exist_ok=True)
    _clear_chrome_singleton_locks()
    options = uc.ChromeOptions()
    # Anti-bot flags
    options.add_argument("--no-sandbox")
    options.add_argument("--no-zygote")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--mute-audio")
    options.add_argument("--autoplay-policy=no-user-gesture-required")
    options.add_argument("--disable-site-isolation-trials")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_argument(f"--user-data-dir={_PROFILE_DIR}")

    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    # Default Chrome strategy ('normal') waits for window.onload, which can
    # hang for >90s on heavy pages with slow ad/tracker SDKs (observed on
    # Bally Sports MiLB live-game pages — DOMContentLoaded fires fast but
    # onload never does, so browser.get() times out and the capture is
    # killed). 'eager' returns as soon as DOMContentLoaded fires; the m3u8
    # network request fires well before then anyway, so we don't lose
    # anything by not waiting for trailing trackers/ads.
    options.set_capability("pageLoadStrategy", "eager")

    chrome_binary = os.getenv("CHROME_BINARY", "/usr/bin/google-chrome")
    options.binary_location = chrome_binary

    # Detect installed Chrome major version so uc downloads a matching driver
    version_main = None
    try:
        out = subprocess.check_output([chrome_binary, "--version"], text=True).strip()
        m = re.search(r"(\d+)\.\d+\.\d+\.\d+", out)
        if m:
            version_main = int(m.group(1))
            logger.info("Detected Chrome version: %d", version_main)
    except Exception as e:
        logger.warning("Could not detect Chrome version: %s", e)

    logger.info("Starting persistent Chrome (profile=%s, timeout=%ds)",
                _PROFILE_DIR, _STARTUP_TIMEOUT)

    result = [None]
    error = [None]

    def _start():
        try:
            result[0] = uc.Chrome(
                options=options,
                use_subprocess=True,
                version_main=version_main,
            )
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=_start, daemon=True)
    t.start()
    t.join(timeout=_STARTUP_TIMEOUT)

    if t.is_alive():
        logger.error("Chrome startup hung after %ds, killing orphaned processes", _STARTUP_TIMEOUT)
        _kill_chrome_processes()
        raise RuntimeError(f"Chrome failed to start within {_STARTUP_TIMEOUT}s")

    if error[0]:
        raise error[0]

    if result[0] is None:
        raise RuntimeError("Chrome startup returned None")

    logger.info("Chrome started successfully")
    # Set page load timeout so browser.get() can't block forever
    try:
        result[0].set_page_load_timeout(120)
    except Exception:
        pass

    # Warm the session before the first real capture. Without this, the very
    # first capture after Chrome starts reliably times out with zero Network
    # events — the perf-log subscription and TLS/DNS layers haven't been
    # exercised yet, so events for the first page don't surface. A cheap GET
    # to about:blank + draining the log primes both.
    try:
        result[0].get("about:blank")
        result[0].get_log("performance")
        logger.info("Session warmed (perf-log primed)")
    except Exception as e:
        logger.warning("Session warmup failed (continuing anyway): %s", e)

    return result[0]


def _get_browser():
    """Return the persistent browser instance, creating or recreating if needed.

    Retries startup once after killing orphans and nuking the profile —
    if Chrome hung last time, leftover processes or profile corruption
    may block the next launch.
    """
    global _browser
    if _browser is None:
        try:
            _browser = _make_browser()
        except Exception as e:
            logger.warning("First startup attempt failed (%s), nuking profile and retrying", e)
            _kill_chrome_processes()
            import shutil
            shutil.rmtree(_PROFILE_DIR, ignore_errors=True)
            time.sleep(2)
            _browser = _make_browser()  # let this one propagate if it fails
        return _browser
    # Health check — if dead, recreate
    try:
        _ = _browser.current_url
        return _browser
    except Exception:
        logger.warning("Browser session unresponsive, recreating")
        try:
            _browser.quit()
        except Exception:
            pass
        _kill_chrome_processes()
        _browser = None
        try:
            _browser = _make_browser()
        except Exception as e:
            logger.error("Browser recreation failed: %s", e)
            _kill_chrome_processes()
            raise
        return _browser


def _release_browser():
    """Reset state between captures: drain logs, return to default frame, blank page.

    Cookies and localStorage are preserved via --user-data-dir on disk.
    Raw CDP Network.clearBrowser* calls were tried here; they desynchronize
    chromedriver's perf-log subscription so the next capture sees zero
    Network events. Don't reintroduce them.
    """
    global _browser, _capture_count, _health_failures
    _health_failures = 0  # capture completed, lock about to release — not stuck
    _capture_count += 1
    if _browser is None:
        return
    try:
        _browser.switch_to.default_content()
    except Exception:
        pass
    try:
        _browser.get_log("performance")  # drain
    except Exception:
        pass
    try:
        _browser.get("about:blank")
    except Exception:
        pass


def _find_m3u8_in_json(obj):
    """Recursively search a parsed JSON object for an m3u8 URL."""
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


def _wait_for_manifest(browser, *, timeout=60, preloaded_entries=None):
    """Poll Chrome DevTools performance logs for an m3u8 manifest.

    `preloaded_entries` is an optional list of perf-log entries already drained
    from Chrome (e.g. by the DIAG pass in _do_capture). selenium's
    get_log('performance') is destructive — anything drained earlier is gone
    from Chrome's buffer — so the caller must hand them back here or the
    m3u8 request/response events are lost and we time out with req/resp=0.
    """
    start = time.time()
    req_meta = {}
    resp_meta = {}
    want_body = set()
    http_fetched_rids = set()  # rids where we've already fired the HTTP-fetch shortcut
    captured_heartbeat = None
    _event_counts = {"total": 0, "req": 0, "resp": 0, "fin": 0}
    _pending = list(preloaded_entries or [])

    # Detect a dead chromedriver session (Chrome crashed, port closed) and
    # bail out instead of looping. Without this the loop hammers
    # browser.get_log() ~12x/sec until `timeout` expires; selenium's urllib3
    # adds 3 retries per call, each emitting log lines — produces ~125
    # log lines/sec for the full timeout and burns CPU on a session that
    # cannot be revived. The outer capture handler will recreate the
    # browser on the next request.
    from urllib3.exceptions import MaxRetryError, NewConnectionError
    from selenium.common.exceptions import (
        InvalidSessionIdException, WebDriverException,
    )

    while (time.time() - start) < timeout:
        try:
            entries = _pending + browser.get_log("performance")
            _pending = []
            _event_counts["total"] += len(entries)
            for entry in entries:
                try:
                    msg = json.loads(entry["message"])["message"]
                except Exception:
                    continue

                method = msg.get("method", "")
                p = msg.get("params", {})
                rid = p.get("requestId")
                if not rid:
                    continue

                rtype = p.get("type") or (p.get("initiator", {}) or {}).get("type")
                if rtype and rtype not in INCLUDE_TYPES:
                    continue

                if method == "Network.requestWillBeSent":
                    req = p.get("request", {}) or {}
                    url = req.get("url", "")
                    accept = req.get("headers", {}).get("Accept", "")
                    headers = req.get("headers", {}) or {}

                    req_meta[rid] = {"url": url, "method": req.get("method"), "headers": headers}

                    url_match = any(pat.lower() in url.lower() for pat in MATCH_PATTERNS)
                    accept_match = any(pat.lower() in accept.lower() for pat in MATCH_PATTERNS) if accept else False
                    api_match = any(pat.lower() in url.lower() for pat in JSON_STREAM_PATTERNS)

                    if url_match or accept_match or api_match:
                        want_body.add(rid)

                    if "heartbeat" in url.lower():
                        captured_heartbeat = {
                            "heartbeat_url": url,
                            "Authorization": headers.get("authorization") or headers.get("Authorization"),
                            "x-channel-key": headers.get("x-channel-key") or headers.get("X-Channel-Key"),
                            "x-client-token": headers.get("x-client-token") or headers.get("X-Client-Token"),
                            "x-user-agent": headers.get("x-user-agent") or headers.get("X-User-Agent"),
                            "Referer": headers.get("referer") or headers.get("Referer"),
                            "User-Agent": headers.get("user-agent") or headers.get("User-Agent"),
                        }
                        logger.info("Captured heartbeat auth from %s", url)

                if method == "Network.responseReceived":
                    resp = p.get("response", {}) or {}
                    url = resp.get("url", "")
                    mime = resp.get("mimeType", "") or ""
                    status = resp.get("status")

                    resp_meta[rid] = {
                        "status": status,
                        "mime": mime,
                        "headers": resp.get("headers", {}) or {},
                        "url": url,
                    }

                    if any(pat.lower() in (url + " " + mime).lower() for pat in MATCH_PATTERNS):
                        want_body.add(rid)

                    # Fire the HTTP-fetch path IMMEDIATELY for direct .m3u8
                    # responses. Historically we waited for Network.loading-
                    # Finished, but Chrome sometimes stops emitting events
                    # after the page's initial burst (cross-origin iframe
                    # isolation routes later events to a different CDP
                    # target than selenium is attached to), so loadingFinished
                    # never reaches us and the capture deadlines out even
                    # though the manifest URL + headers are already in hand.
                    # By responseReceived time we know the URL, status, and
                    # the request headers (cached on requestWillBeSent), so
                    # we can fetch the manifest ourselves right now.
                    if (rid not in http_fetched_rids and
                            url and ".m3u8" in url and
                            (status is None or 200 <= status < 400)):
                        http_fetched_rids.add(rid)
                        req_info = req_meta.get(rid, {})
                        req_headers = req_info.get("headers", {}) or {}
                        ua = req_headers.get("User-Agent") or req_headers.get("user-agent") or "Mozilla/5.0"
                        referer = req_headers.get("Referer") or req_headers.get("referer")
                        fetch_headers = {"User-Agent": ua}
                        if referer:
                            fetch_headers["Referer"] = referer
                        logger.info("HTTP fetch (early): %s (ref=%s)", url[:120], (referer or "")[:80])
                        try:
                            rr = http_requests.get(url, headers=fetch_headers, timeout=15)
                        except Exception as e:
                            logger.warning("HTTP fetch (early) failed for %s: %s", url, e)
                            continue
                        logger.info("HTTP fetch (early) result: status=%s body_len=%d has_extm3u=%s",
                                    rr.status_code, len(rr.text), "#EXTM3U" in rr.text)
                        if rr.status_code == 200 and "#EXTM3U" in rr.text:
                            return {
                                "url": url,
                                "status": rr.status_code,
                                "mime": rr.headers.get("Content-Type") or mime,
                                "req_headers": fetch_headers,
                                "resp_headers": dict(rr.headers),
                                "body": rr.text,
                                "base64Encoded": False,
                                "heartbeat": captured_heartbeat,
                                "_short_circuit": True,
                                "_user_agent": ua,
                            }
                        # Not HLS after all (e.g. CDN returned an error page
                        # or token-gated 403/404). Fall through — the
                        # loadingFinished path below may still salvage it via
                        # the CDP body, and we keep polling for later m3u8s.

                if method == "Network.loadingFinished" and rid in want_body and rid not in http_fetched_rids:
                    req_info = req_meta.get(rid, {})
                    resp_info = resp_meta.get(rid, {})
                    req_url = req_info.get("url") or resp_info.get("url", "")

                    # Direct m3u8 — fetch body via HTTP GET using the request's
                    # own UA + Referer. Bypasses CDP entirely: Chrome Site
                    # Isolation puts cross-origin iframes on their own CDP
                    # target, and execute_cdp_cmd("Network.getResponseBody")
                    # against the current target hangs when the request lives
                    # on a different target. HTTP fetch works because the CDN
                    # URL is signed (st/e tokens) and only needs UA + Referer.
                    if req_url and ".m3u8" in req_url:
                        req_headers = req_info.get("headers", {}) or {}
                        ua = req_headers.get("User-Agent") or req_headers.get("user-agent")
                        if not ua:
                            try:
                                # Switch to top target before execute_script —
                                # same cross-origin concern.
                                browser.switch_to.default_content()
                                ua = browser.execute_script("return navigator.userAgent")
                            except Exception:
                                ua = "Mozilla/5.0"
                        referer = req_headers.get("Referer") or req_headers.get("referer")
                        fetch_headers = {"User-Agent": ua}
                        if referer:
                            fetch_headers["Referer"] = referer
                        logger.info("HTTP fetch: %s (ref=%s)", req_url[:120], (referer or "")[:80])
                        try:
                            resp = http_requests.get(req_url, headers=fetch_headers, timeout=15)
                        except Exception as e:
                            logger.warning("HTTP fetch failed for %s: %s", req_url, e)
                            continue
                        logger.info("HTTP fetch result: status=%s body_len=%d has_extm3u=%s",
                                    resp.status_code, len(resp.text),
                                    "#EXTM3U" in resp.text)
                        if resp.status_code == 200 and "#EXTM3U" in resp.text:
                            # `_short_circuit=True` tells _do_capture this is
                            # already a complete capture result — skip any
                            # further browser.* calls, which can hang on a
                            # wedged cross-origin iframe target.
                            return {
                                "url": req_url,
                                "status": resp.status_code,
                                "mime": resp.headers.get("Content-Type") or resp_info.get("mime"),
                                "req_headers": fetch_headers,
                                "resp_headers": dict(resp.headers),
                                "body": resp.text,
                                "base64Encoded": False,
                                "heartbeat": captured_heartbeat,
                                "_short_circuit": True,
                                "_user_agent": ua,
                            }
                        # Not HLS after all — fall through to CDP path below
                        # in case this URL happened to match the pattern but
                        # the body is JSON.

                    # CDP path: needed for JSON APIs whose body must be the
                    # JSON Chrome received (HTTP fetch would race the session).
                    # Switch to top target first — cross-origin iframes have
                    # their own target and CDP commands stall when asking a
                    # child target about requests registered on the parent.
                    body = ""
                    is_b64 = False
                    try:
                        browser.switch_to.default_content()
                    except Exception:
                        pass
                    try:
                        body_res = browser.execute_cdp_cmd("Network.getResponseBody", {"requestId": rid})
                        body = body_res.get("body", "")
                        is_b64 = body_res.get("base64Encoded", False)
                    except Exception as e:
                        logger.warning("CDP getResponseBody failed for rid=%s: %s", rid, e)
                        continue

                    # Decode body if base64-encoded (Chrome returns binary content as base64)
                    decoded_body = body
                    if is_b64 and body:
                        try:
                            decoded_body = base64.b64decode("".join(body.split())).decode("utf-8", errors="replace")
                        except Exception:
                            decoded_body = ""

                    # Direct HLS body: some proxies serve manifests with
                    # disguised extensions (.css/.csv/.txt) to evade scrapers
                    # that key on .m3u8. The body still has #EXTM3U.
                    text_body = decoded_body if is_b64 else body
                    if text_body and "#EXTM3U" in text_body[:4096]:
                        logger.info("Found disguised HLS manifest at %s", req_url[:200])
                        req_headers = req_info.get("headers", {}) or {}
                        ua = req_headers.get("User-Agent") or req_headers.get("user-agent") or "Mozilla/5.0"
                        referer = req_headers.get("Referer") or req_headers.get("referer")
                        fetch_headers = {"User-Agent": ua}
                        if referer:
                            fetch_headers["Referer"] = referer
                        return {
                            "url": req_url,
                            "status": resp_info.get("status"),
                            "mime": resp_info.get("mime") or "application/vnd.apple.mpegurl",
                            "req_headers": fetch_headers,
                            "resp_headers": resp_info.get("headers", {}),
                            "body": text_body,
                            "base64Encoded": False,
                            "heartbeat": captured_heartbeat,
                            "_user_agent": ua,
                        }

                    # JSON API response with embedded m3u8
                    if not is_b64 and body:
                        try:
                            json_data = json.loads(body)
                            stream_url = _find_m3u8_in_json(json_data)
                            if stream_url:
                                logger.info("Found m3u8 in JSON from %s: %s", req_url, stream_url[:200])
                                ua = browser.execute_script("return navigator.userAgent")
                                resp = http_requests.get(stream_url, timeout=15,
                                                         headers={"User-Agent": ua, "Referer": req_url})
                                if resp.status_code == 200 and "#EXTM3U" in resp.text:
                                    return {
                                        "url": stream_url,
                                        "status": resp.status_code,
                                        "mime": resp.headers.get("Content-Type", "application/vnd.apple.mpegurl"),
                                        "req_headers": {"User-Agent": ua, "Referer": req_url},
                                        "resp_headers": dict(resp.headers),
                                        "body": resp.text,
                                        "base64Encoded": False,
                                        "heartbeat": captured_heartbeat,
                                        "source_api_url": req_url,
                                    }
                        except (json.JSONDecodeError, ValueError):
                            pass

        except (InvalidSessionIdException, MaxRetryError, NewConnectionError) as e:
            logger.error("Chromedriver session is dead, aborting capture: %s", str(e)[:200])
            return None
        except WebDriverException as e:
            msg = str(e).lower()
            if "connection refused" in msg or "no such session" in msg \
                    or "chrome not reachable" in msg or "session deleted" in msg:
                logger.error("Chromedriver unreachable, aborting capture: %s", str(e)[:200])
                return None
            logger.error("Error processing performance logs: %s", e)
        except Exception as e:
            logger.error("Error processing performance logs: %s", e)

        time.sleep(0.08)

    logger.warning("No manifest found within %ds timeout (events seen: %s)",
                   timeout, _event_counts)
    return None


def _decode_body(result):
    """Decode + validate HLS body. Returns text or None."""
    if result.get("base64Encoded"):
        raw = result["body"]
        if not isinstance(raw, str):
            raw = str(raw)
        try:
            binary = base64.b64decode("".join(raw.split()), validate=True)
            try:
                text = binary.decode("utf-8")
            except UnicodeDecodeError:
                text = binary.decode("latin-1", errors="replace")
        except base64.binascii.Error:
            return None
    else:
        text = result.get("body")

    if not text or "#EXTM3U" not in text:
        return None
    return text


_health_failures = 0
_MAX_HEALTH_FAILURES = 12  # 12 failures × 30s = 6 min stuck → self-kill
# Normal 90s captures hold the lock for ~3 health checks. Only a true
# hang (browser.get stuck, Chrome zombie) should exceed 6 minutes.

@app.get("/health")
def health():
    """Health check that detects a stuck browser lock.

    Tries to acquire _browser_lock with a 5s timeout. If it can't,
    Chrome startup or a capture is hung. After repeated failures,
    kills the process so Docker restarts the container.
    """
    global _health_failures
    acquired = _browser_lock.acquire(timeout=5)
    if not acquired:
        _health_failures += 1
        logger.warning("Health check failed (%d/%d): browser_lock stuck",
                       _health_failures, _MAX_HEALTH_FAILURES)
        if _health_failures >= _MAX_HEALTH_FAILURES:
            logger.error("Browser stuck for too long, killing process for Docker restart")
            _kill_chrome_processes()
            os._exit(1)
        return JSONResponse(
            status_code=503,
            content={"ready": False, "error": "browser_lock stuck",
                     "failures": _health_failures, "capture_count": _capture_count},
        )
    try:
        _health_failures = 0  # reset on success
        alive = False
        if _browser is not None:
            try:
                _ = _browser.current_url
                alive = True
            except Exception:
                pass
        return {"ready": True, "browser_alive": alive, "capture_count": _capture_count}
    finally:
        _browser_lock.release()


@app.post("/restart")
def restart():
    """Forcibly recycle the browser instance."""
    global _browser, _capture_count
    with _browser_lock:
        if _browser:
            try:
                _browser.quit()
            except Exception:
                pass
        _browser = None
        _capture_count = 0
    return {"ok": True}


@app.get("/screenshot")
def screenshot():
    """Return a PNG of the uc Chrome's current viewport via CDP, without
    taking _browser_lock. Useful for inspecting browser state between
    captures or shortly after a successful capture completes.

    Limitation: when a capture is TRULY hung in page load (stuck in
    browser.get() waiting on Chrome's renderer), selenium's CDP command
    queue is also blocked and this endpoint will time out. Skip phrases
    are a much more reliable way to avoid hang-debugging in the first
    place. For true hung-state debugging we'd need a separate CDP
    session via Target.attachToTarget on a second connection — not done
    here."""
    import base64 as _b64
    if _browser is None:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": "no browser yet"},
        )
    try:
        # Page.captureScreenshot returns {"data": "<base64 png>"}
        result = _browser.execute_cdp_cmd("Page.captureScreenshot", {})
        png = _b64.b64decode(result.get("data", ""))
        if not png:
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": "empty screenshot data"},
            )
        return Response(content=png, media_type="image/png")
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"ok": False, "error": str(e)},
        )


def _selenium_to_netscape(cookies: list) -> str:
    """Convert selenium cookie dicts to Netscape cookies.txt format (yt-dlp compatible)."""
    lines = [
        "# Netscape HTTP Cookie File",
        "# Generated by channelarr selenium-uc",
        "",
    ]
    for c in cookies:
        domain = c.get("domain") or ""
        if not domain:
            continue
        include_sub = "TRUE" if domain.startswith(".") else "FALSE"
        path = c.get("path") or "/"
        secure = "TRUE" if c.get("secure") else "FALSE"
        # Session cookies (no expiry) → set far future so yt-dlp accepts them
        expiry = int(c.get("expiry") or (time.time() + 365 * 86400))
        name = c.get("name") or ""
        value = c.get("value") or ""
        if not name:
            continue
        lines.append("\t".join([domain, include_sub, path, secure, str(expiry), name, value]))
    return "\n".join(lines) + "\n"


@app.post("/cookies/youtube")
def cookies_youtube():
    """Navigate the persistent browser to youtube.com and return cookies in Netscape format.

    Reuses the singleton browser under _browser_lock — no parallel sessions.
    Cookies persist across browser restarts via --user-data-dir on disk.
    """
    with _browser_lock:
        try:
            logger.info("Cookie warmup: navigating to youtube.com (capture_count=%d)", _capture_count)
            browser = _get_browser()
            try:
                browser.set_page_load_timeout(30)
            except Exception:
                pass
            try:
                browser.get("https://www.youtube.com")
            except Exception:
                logger.debug("YouTube page load timeout, continuing with available cookies")
            try:
                WebDriverWait(browser, 15).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except Exception:
                logger.debug("readyState wait timed out, continuing")
            time.sleep(2)  # let any deferred cookies settle

            raw = browser.get_cookies() or []
            netscape = _selenium_to_netscape(raw)
            logger.info("Cookie warmup: returning %d cookies", len(raw))
            return {"ok": True, "count": len(raw), "netscape": netscape}

        except Exception as e:
            logger.exception("Cookie warmup failed")
            return {"ok": False, "error": str(e), "count": 0, "netscape": ""}

        finally:
            _release_browser()


def _check_paywall(browser):
    """Check for premium/paywall elements in the current frame.

    Returns a descriptive string if a paywall is detected, None otherwise.
    Checks both element selectors and page text for common premium gates.
    """
    paywall_selectors = [
        "[class*='premium']", "[class*='Premium']",
        "[class*='paywall']", "[class*='Paywall']",
        "[class*='subscribe']", "[class*='Subscribe']",
        "[class*='upgrade']", "[class*='Upgrade']",
        "a[href*='premium']", "a[href*='subscribe']",
        "button[class*='premium']", ".get-premium",
        "#premium-overlay", ".premium-wall",
    ]
    for sel in paywall_selectors:
        try:
            el = browser.find_element(By.CSS_SELECTOR, sel)
            if el.is_displayed():
                text = (el.text or "").strip()[:80]
                return text or sel
        except Exception:
            continue
    # Check visible text AND innerHTML for premium/not-yet-live keywords
    try:
        # Get both visible text and raw HTML (some overlays use hidden text)
        body_text = browser.find_element(By.TAG_NAME, "body").text or ""
        body_html = ""
        try:
            body_html = browser.execute_script("return document.body ? document.body.innerHTML : ''") or ""
        except Exception:
            pass
        combined = (body_text + " " + body_html).lower()
        # Log what we see for debugging
        if body_text.strip():
            logger.info("Paywall check body text: %s", body_text.strip()[:300])
        elif body_html.strip():
            logger.info("Paywall check innerHTML (first 300): %s", body_html.strip()[:300])
        else:
            logger.info("Paywall check: iframe body is empty")
        for phrase in ["premium only", "premium members only",
                       "subscribe to watch", "upgrade to watch",
                       "unlock this stream",
                       "live stream starting soon", "stream starting soon",
                       "event has not started", "stream will begin shortly",
                       "broadcast will begin",
                       "upcoming event", "upcoming broadcast", "upcoming stream",
                       "stream has ended", "stream ended",
                       "event has ended", "match has ended",
                       "game has ended", "broadcast has ended",
                       "event is over", "game is over"]:
            if phrase in combined:
                return phrase
    except Exception as e:
        logger.debug("Paywall check error: %s", e)
    return None


def _scan_all_frames_for_skip(browser):
    """Check the main page and all iframes for skip-worthy content.

    Looks at the outer page and each iframe for text indicating the stream
    is premium-locked or not yet live. Returns the matched phrase or None.
    """
    # Only match phrases that definitively mean "this stream is not available".
    # Do NOT match generic site navigation text like "Go Premium" or "Register"
    # which appear on every page regardless of stream status.
    skip_phrases = [
        "premium only", "premium members only",
        "subscribe to watch", "upgrade to watch",
        "unlock this stream",
        "live stream starting soon", "stream starting soon",
        "event has not started", "stream will begin shortly",
        "broadcast will begin",
        # Some sites render "DELAYED START" as a status badge when the
        # broadcast hasn't gone live yet — the player never initializes,
        # so no manifest is ever requested and we'd otherwise wait the
        # full timeout for nothing. Safe to match here because the scan
        # below only runs inside iframes (where the player + its status
        # overlay live), not on the top-level page (where related-game
        # sidebars list other games' "delayed start" badges).
        "delayed start",
        # The bare word 'upcoming' false-triggers on sites with secondary
        # 'Upcoming Listings' sections while a live stream is playing.
        # Match contextual pregame wording instead.
        "upcoming event", "upcoming broadcast", "upcoming stream",
        # Bally Sports post-game state: 'FINAL' label + box-score sections
        # ('Top Performers Today', 'Team Comparison') appear on the same
        # game URL after it ends, with no live player. Live pages show the
        # video player instead, never these summary headers.
        "top performers today", "team comparison",
        # End-of-stream wording — page renders a "game over" card instead
        # of the player. Without these, uc waits the full deadline
        # scanning for a manifest that will never arrive.
        "stream has ended", "stream ended",
        "event has ended", "match has ended",
        "game has ended", "broadcast has ended",
        "event is over", "game is over",
    ]

    def _check_current_frame():
        try:
            text = browser.execute_script(
                "return document.body ? document.body.innerText : ''"
            ) or ""
            lower = text.lower()
            for phrase in skip_phrases:
                if phrase in lower:
                    return phrase
        except Exception:
            pass
        return None

    # Iframes only — the top-level document on every live-stream source
    # we resolve is just chrome (nav, ads, related-content sidebars).
    # Skip phrases on the top page produced false positives that blocked
    # legitimate captures (Bally MiLB sidebars list other games' "DELAYED
    # START" badges, which falsely matched on live game pages). The
    # actual player + its real-time status overlay always live inside an
    # iframe, so scanning iframes only gives us accurate skip detection.
    # Worst case for sources without an iframe player: we wait the full
    # timeout instead of fast-skipping — same outcome as if no skip
    # phrase existed for them.
    try:
        browser.switch_to.default_content()
        time.sleep(1)
        iframes = browser.find_elements(By.TAG_NAME, "iframe")
        for idx, _ in enumerate(iframes):
            try:
                browser.switch_to.default_content()
                browser.switch_to.frame(idx)
                result = _check_current_frame()
                if result:
                    browser.switch_to.default_content()
                    return result
            except Exception:
                continue

        browser.switch_to.default_content()
    except Exception as e:
        logger.debug("Frame scan error: %s", e)
        try:
            browser.switch_to.default_content()
        except Exception:
            pass
    return None


def _try_click_play(browser):
    """Attempt to click play button overlays to start a stream.

    Many streaming sites require a user click before the HLS manifest
    loads — especially for pregame/upcoming events. Tries common play
    button selectors, then falls back to clicking the video element.
    """
    play_selectors = [
        # Common play button patterns
        ".play-button", ".vjs-big-play-button", ".jw-icon-display",
        "[class*='play']", "button[aria-label*='play' i]",
        ".btn-play", "#play-btn", ".plyr__control--overlaid",
        # Generic video click
        "video",
        # Player container (last resort)
        ".video-player", ".player", "#player", ".jw-wrapper",
    ]
    for sel in play_selectors:
        try:
            el = browser.find_element(By.CSS_SELECTOR, sel)
            if el.is_displayed():
                el.click()
                logger.info("Clicked play element: %s", sel)
                time.sleep(2)  # let the player initialize
                return
        except Exception:
            continue
    # Final fallback: JS click on any video element
    try:
        browser.execute_script("""
            var v = document.querySelector('video');
            if (v) { v.click(); v.play && v.play().catch(function(){}); }
        """)
        logger.info("JS fallback: clicked/played video element")
        time.sleep(2)
    except Exception:
        pass


_DBG_DIR = "/tmp/capdbg"


def _dbg_screenshot(browser, label: str):
    """Save a PNG of the current viewport. Tries selenium's native method
    first (uses Page.captureScreenshot under the hood but with proper window
    clip), falls back to raw CDP. No-op on failure."""
    try:
        os.makedirs(_DBG_DIR, exist_ok=True)
        path = os.path.join(_DBG_DIR, f"{label}.png")

        # Try CDP with explicit clip and full-page, which forces a layout
        # pass and avoids the empty-viewport 1x1 case selenium's get_log
        # reuses sometimes when the renderer hasn't painted yet.
        try:
            metrics = browser.execute_cdp_cmd("Page.getLayoutMetrics", {})
            content = metrics.get("contentSize") or metrics.get("cssContentSize") or {}
            w = int(content.get("width") or 1920)
            h = int(content.get("height") or 1080)
            # Cap absurdly tall pages
            h = min(h, 4000)
            res = browser.execute_cdp_cmd("Page.captureScreenshot", {
                "format": "png",
                "clip": {"x": 0, "y": 0, "width": w, "height": h, "scale": 1},
                "captureBeyondViewport": True,
            })
            data = res.get("data", "")
            if data:
                with open(path, "wb") as f:
                    f.write(base64.b64decode(data))
                logger.info("[DBG-SHOT %s] saved %s (clip %dx%d, %d b64 chars)",
                            label, path, w, h, len(data))
                return
            logger.info("[DBG-SHOT %s] CDP returned empty data", label)
        except Exception as e:
            logger.info("[DBG-SHOT %s] CDP failed: %s", label, e)

        # Fallback: selenium's native method
        try:
            png = browser.get_screenshot_as_png()
            if png:
                with open(path, "wb") as f:
                    f.write(png)
                logger.info("[DBG-SHOT %s] saved %s (selenium fallback, %d bytes)",
                            label, path, len(png))
                return
        except Exception as e:
            logger.info("[DBG-SHOT %s] selenium fallback failed: %s", label, e)
    except Exception as e:
        logger.info("[DBG-SHOT %s] outer error: %s", label, e)


def _dbg_dump_urls(entries: list, label: str, max_log: int = 80):
    """Walk perf-log entries and log distinct request URLs (top N) to spot
    non-m3u8 traffic like CSV / token endpoints. Saves full URL list to disk."""
    try:
        urls = []
        seen = set()
        for entry in entries:
            try:
                msg = json.loads(entry["message"])["message"]
            except Exception:
                continue
            method = msg.get("method", "")
            if method != "Network.requestWillBeSent":
                continue
            req = (msg.get("params", {}) or {}).get("request", {}) or {}
            u = req.get("url", "")
            if not u or u in seen:
                continue
            seen.add(u)
            urls.append(u)
        os.makedirs(_DBG_DIR, exist_ok=True)
        path = os.path.join(_DBG_DIR, f"{label}_urls.txt")
        with open(path, "w") as f:
            f.write("\n".join(urls))
        logger.info("[DBG-URLS %s] %d distinct requests, saved %s",
                    label, len(urls), path)
        # Log the first N non-noise URLs to journal
        noise = ("google-analytics", "googletagmanager", "histats.com",
                 "doubleclick.net", "/recaptcha/", "googlesyndication",
                 "/_next/", "/static/", ".css", ".woff", ".png", ".jpg",
                 ".svg", ".gif", ".ico", ".woff2", "fonts.gstatic")
        emitted = 0
        for u in urls:
            if any(n in u for n in noise):
                continue
            logger.info("[DBG-URLS %s]  %s", label, u[:200])
            emitted += 1
            if emitted >= max_log:
                break
    except Exception as e:
        logger.info("[DBG-URLS %s] error: %s", label, e)


def _do_capture(browser, url, timeout, switch_iframe, debug=False):
    """Inner capture logic — runs inside a deadline thread."""
    # Set page load timeout
    try:
        browser.set_page_load_timeout(timeout + 30)
    except Exception:
        pass

    _page_load_timed_out = False
    try:
        browser.get(url)
    except Exception as e:
        logger.warning("Page load timeout/error for %s: %s", url, e)
        _page_load_timed_out = True

    # DIAG: drain perf log immediately after page load so we can see what
    # Chrome actually received during autoplay before skip-scan / iframe-
    # switch / click-play consume more buffer budget. Log any m3u8-looking
    # URLs here so we know if the request was ever emitted at all. CRITICAL:
    # get_log('performance') is destructive — entries drained here must be
    # passed forward to _wait_for_manifest (via preloaded_entries) or the
    # m3u8 request/response events are lost and capture times out.
    _early = []
    try:
        _early = browser.get_log("performance")
        _m3u8_early = []
        for _e in _early:
            _msg = _e.get("message", "")
            if "m3u8" in _msg.lower():
                import re as _re
                _m = _re.search(r'https?://[^"\s]+\.m3u8[^"\s]*', _msg)
                if _m:
                    _m3u8_early.append(_m.group(0))
        logger.info("[DIAG] post-get: perf entries=%d m3u8 urls=%d",
                    len(_early), len(_m3u8_early))
        for _u in _m3u8_early[:3]:
            logger.info("[DIAG]   m3u8: %s", _u[:200])
    except Exception as _e:
        logger.info("[DIAG] post-get drain failed: %s", _e)

    if debug:
        _dbg_screenshot(browser, "01_after_load")
        _dbg_dump_urls(_early, "01_after_load")

    # Scan all iframes for skip conditions
    skip_reason = _scan_all_frames_for_skip(browser)
    if skip_reason:
        logger.info("Skip detected for %s: %s", url, skip_reason)
        return {"ok": False, "error": f"Skipped: {skip_reason}"}

    if switch_iframe:
        # Pick the iframe most likely to be the actual player. Naive
        # "switch to first iframe" picks placeholder/chat/ad iframes on
        # sites that nest the player behind one or more decoys.
        try:
            browser.switch_to.default_content()
            WebDriverWait(browser, 10).until(
                lambda d: len(d.find_elements(By.TAG_NAME, "iframe")) > 0
            )
            iframes = browser.find_elements(By.TAG_NAME, "iframe")
            _SKIP_PREFIXES = ("javascript:", "about:", "data:", "blob:")
            _SKIP_SUBSTR = (
                "chatango.com", "adbanner", "/ads/", "/ad-",
                "google.com/recaptcha", "doubleclick.net",
                "googletagmanager.com", "googlesyndication",
                "googleadservices",
            )
            target = None
            for ifr in iframes:
                try:
                    src = (ifr.get_attribute("src") or "").lower()
                    if not src or src.startswith(_SKIP_PREFIXES):
                        continue
                    if any(s in src for s in _SKIP_SUBSTR):
                        continue
                    target = ifr
                    break
                except Exception:
                    continue
            if target is None and iframes:
                target = iframes[0]
            if target is not None:
                src_log = (target.get_attribute("src") or "(no src)")[:140]
                browser.switch_to.frame(target)
                logger.info("Switched to iframe: %s", src_log)
        except Exception:
            logger.debug("No iframe, continuing in main frame")

    if debug:
        _dbg_screenshot(browser, "02_after_iframe_switch")

    _try_click_play(browser)

    _post_click_entries = []
    if debug:
        _dbg_screenshot(browser, "03_after_click")
        # Destructive drain — feed back to _wait_for_manifest below.
        _post_click_entries = browser.get_log("performance")
        _dbg_dump_urls(_post_click_entries, "03_after_click")

    # If the page itself hung during load, cap the manifest wait short.
    # Chrome has already had ~120s — any manifest that was going to be
    # fetched is already in the performance log. Waiting the full
    # `timeout` on top guarantees we hit the 135s deadline when nothing
    # is coming. 15s is enough to drain the log and match m3u8s that
    # ARE there, without burning the wall clock for empty cases.
    wait_timeout = 15 if _page_load_timed_out else timeout
    result = _wait_for_manifest(
        browser, timeout=wait_timeout,
        preloaded_entries=(_early + _post_click_entries),
    )
    if debug:
        _dbg_screenshot(browser, "04_after_wait")
        # Final URL dump from any remaining perf entries
        _dbg_dump_urls(browser.get_log("performance"), "04_after_wait")
    if not result:
        if _page_load_timed_out:
            return {"ok": False, "error": "Skipped: page hung and no manifest arrived"}
        return {"ok": False, "error": "No manifest captured within timeout"}

    body_text = _decode_body(result)
    if not body_text:
        return {
            "ok": False,
            "error": f"Captured resource is not HLS: {result.get('url')} (MIME: {result.get('mime')})",
        }

    # A live playlist never contains #EXT-X-ENDLIST. Its presence means
    # the stream is a fixed-length VOD — typically a 10-30s "Thank You
    # for Watching" outro or replay clip after a game ends. Skip these
    # so auto-channel-creation doesn't produce ghost channels that
    # immediately 404. The text overlay lives inside the video frame,
    # not in HTML, so the earlier phrase scan can't catch it — the
    # playlist structure is the only reliable signal.
    if "#EXT-X-ENDLIST" in body_text:
        logger.info("Skip detected for %s: playlist is VOD (#EXT-X-ENDLIST "
                    "present — stream has ended)", url)
        return {"ok": False, "error": "Skipped: stream ended (VOD playlist)"}

    # Short-circuit path: _wait_for_manifest completed the capture via plain
    # HTTP (cross-origin iframe case), so everything we need is already in
    # `result`. Skip all further browser.* calls — the iframe's CDP target
    # may be wedged and ANY call against it (switch_to, execute_script,
    # get_cookies) can hang indefinitely. Cookies come back empty; OK for
    # CDN-signed URLs, which is the entire reason we needed the HTTP path.
    # `_browser_needs_reset=True` tells /capture to quit+replace the browser
    # before the NEXT call — the wedged cross-origin target poisons the
    # whole session, so current_url() in _get_browser()'s health check
    # hangs for minutes on the reused instance.
    if result.get("_short_circuit"):
        return {
            "ok": True,
            "manifest_url": result["url"],
            "body": body_text,
            "mime": result.get("mime"),
            "headers": result.get("resp_headers", {}),
            "heartbeat": result.get("heartbeat"),
            "user_agent": result.get("_user_agent") or result.get("req_headers", {}).get("User-Agent") or "Mozilla/5.0",
            "referer": result.get("req_headers", {}).get("Referer"),
            "cookies": [],
            "_browser_needs_reset": True,
        }

    ua = browser.execute_script("return navigator.userAgent")

    # Capture cookies for ALL domains touched during the capture, not just
    # the top-level page. Session-bound streams often serve segments / keys
    # from a different subdomain (e.g. proxy CDN) than the page itself, so
    # selenium's per-domain get_cookies() misses the auth needed for
    # downstream playback. CDP Network.getAllCookies returns the full jar.
    cookies = []
    try:
        browser.switch_to.default_content()
        try:
            res = browser.execute_cdp_cmd("Network.getAllCookies", {})
            raw = res.get("cookies", []) or []
            # Normalize CDP cookies to the same shape selenium returns so the
            # streamer doesn't need to know which path produced them.
            for c in raw:
                cookies.append({
                    "name": c.get("name"),
                    "value": c.get("value"),
                    "domain": c.get("domain"),
                    "path": c.get("path") or "/",
                    "secure": bool(c.get("secure")),
                    "httpOnly": bool(c.get("httpOnly")),
                    "expiry": int(c["expires"]) if c.get("expires", -1) and c.get("expires", -1) > 0 else None,
                    "sameSite": c.get("sameSite"),
                })
            logger.info("Captured %d cross-domain cookies (CDP)", len(cookies))
        except Exception as e:
            logger.warning("CDP getAllCookies failed (%s); falling back to per-domain", e)
            cookies = browser.get_cookies() or []
            logger.info("Captured %d session cookies (selenium)", len(cookies))
    except Exception:
        pass

    return {
        "ok": True,
        "manifest_url": result["url"],
        "body": body_text,
        "mime": result.get("mime"),
        "headers": result.get("resp_headers", {}),
        "heartbeat": result.get("heartbeat"),
        "user_agent": ua,
        "referer": result.get("req_headers", {}).get("Referer"),
        "cookies": cookies,
    }


@app.post("/capture")
def capture(req: CaptureRequest):
    """Navigate to a URL and capture an m3u8 manifest using the persistent session.

    Runs the entire capture inside a deadline thread so one hung page
    can never block the sidecar permanently.
    """
    # Hard deadline: timeout + 45s covers page load + manifest wait + overhead
    deadline = req.timeout + 45

    with _browser_lock:
        global _browser
        try:
            logger.info("Starting capture: %s (timeout=%ds, deadline=%ds, count=%d)",
                        req.url, req.timeout, deadline, _capture_count)
            browser = _get_browser()

            # Run capture in a thread with a hard deadline
            capture_result = [None]
            capture_error = [None]

            def _run():
                try:
                    capture_result[0] = _do_capture(
                        browser, req.url, req.timeout, req.switch_iframe,
                        debug=req.debug,
                    )
                except Exception as e:
                    capture_error[0] = e

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            t.join(timeout=deadline)

            if t.is_alive():
                logger.error("Capture deadline exceeded for %s (%ds), killing browser",
                             req.url, deadline)
                try:
                    _browser.quit()
                except Exception:
                    pass
                _kill_chrome_processes()
                _browser = None
                return {"ok": False, "error": f"Capture deadline exceeded ({deadline}s)"}

            if capture_error[0]:
                raise capture_error[0]

            result_dict = capture_result[0] or {"ok": False, "error": "Capture returned no result"}

            # Cross-origin short-circuit path left the browser session in a
            # wedged state. Proactively quit+replace now so the next call
            # doesn't spend minutes inside current_url()'s selenium timeout
            # before noticing. Strip the internal flag before returning.
            if result_dict.pop("_browser_needs_reset", False):
                logger.info("Resetting browser after short-circuit capture (session wedged)")
                try:
                    _browser.quit()
                except Exception:
                    pass
                _kill_chrome_processes()
                _browser = None

            return result_dict

        except Exception as e:
            logger.exception("Capture failed for %s", req.url)
            try:
                if _browser:
                    _browser.quit()
            except Exception:
                pass
            _kill_chrome_processes()
            _browser = None
            return {"ok": False, "error": str(e)}

        finally:
            _release_browser()
