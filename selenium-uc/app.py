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
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import requests as http_requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

MATCH_PATTERNS = ("m3u8", "application/x-mpegurl", "application/vnd.apple.mpegurl")
INCLUDE_TYPES = ("Media", "Fetch", "XHR", "Document", "Other")
JSON_STREAM_PATTERNS = ("ngtv.io", "/api/", "/media/", "/stream", "anvato", "uplynk")

# Persistent browser singleton
_PROFILE_DIR = os.getenv("CHROME_PROFILE_DIR", "/data/chrome-profile")
_RECYCLE_AFTER = int(os.getenv("CHROME_RECYCLE_AFTER", "50"))  # captures
_STARTUP_TIMEOUT = int(os.getenv("CHROME_STARTUP_TIMEOUT", "60"))  # seconds
_browser_lock = threading.RLock()
_browser = None
_capture_count = 0


class CaptureRequest(BaseModel):
    url: str
    timeout: int = 60
    switch_iframe: bool = True


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


def _kill_chrome_processes():
    """Kill any orphaned chrome/chromedriver processes."""
    for name in ("chrome", "chromedriver"):
        try:
            subprocess.run(["pkill", "-9", "-f", name], capture_output=True, timeout=5)
        except Exception:
            pass


def _make_browser():
    """Build a fresh undetected Chrome instance with persistent profile dir.

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
    # Persistence — note: NO --incognito (it would defeat the user-data-dir)
    options.add_argument(f"--user-data-dir={_PROFILE_DIR}")

    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

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

    logger.info("Starting persistent Chrome (profile=%s, timeout=%ds)", _PROFILE_DIR, _STARTUP_TIMEOUT)

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
    return result[0]


def _get_browser():
    """Return the persistent browser instance, creating or recreating if needed.

    Retries startup once after killing orphans — if Chrome hung last time,
    leftover processes may block the profile lock.
    """
    global _browser
    if _browser is None:
        try:
            _browser = _make_browser()
        except Exception as e:
            logger.warning("First startup attempt failed (%s), killing orphans and retrying", e)
            _kill_chrome_processes()
            _clear_chrome_singleton_locks()
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

    Cookies and localStorage are preserved (the user-data-dir is on disk).
    """
    global _browser, _capture_count, _health_failures
    _health_failures = 0  # capture completed, lock about to release — not stuck
    if _browser is None:
        return
    try:
        _browser.switch_to.default_content()
    except Exception:
        pass
    try:
        # Drain performance logs so the next capture starts with a clean buffer
        _browser.get_log("performance")
    except Exception:
        pass
    try:
        # Free DOM/JS state but keep cookies on disk
        _browser.get("about:blank")
    except Exception:
        pass

    _capture_count += 1
    if _capture_count >= _RECYCLE_AFTER:
        logger.info("Recycle threshold reached (%d captures), restarting browser", _capture_count)
        try:
            _browser.quit()
        except Exception:
            pass
        _browser = None
        _capture_count = 0


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


def _wait_for_manifest(browser, *, timeout=60):
    """Poll Chrome DevTools performance logs for an m3u8 manifest."""
    start = time.time()
    req_meta = {}
    resp_meta = {}
    want_body = set()
    captured_heartbeat = None

    while (time.time() - start) < timeout:
        try:
            for entry in browser.get_log("performance"):
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

                    resp_meta[rid] = {
                        "status": resp.get("status"),
                        "mime": mime,
                        "headers": resp.get("headers", {}) or {},
                        "url": url,
                    }

                    if any(pat.lower() in (url + " " + mime).lower() for pat in MATCH_PATTERNS):
                        want_body.add(rid)

                if method == "Network.loadingFinished" and rid in want_body:
                    body = ""
                    is_b64 = False
                    try:
                        body_res = browser.execute_cdp_cmd("Network.getResponseBody", {"requestId": rid})
                        body = body_res.get("body", "")
                        is_b64 = body_res.get("base64Encoded", False)
                    except Exception as e:
                        logger.warning("Failed to get body for rid=%s: %s", rid, e)
                        continue

                    req_url = req_meta.get(rid, {}).get("url") or resp_meta.get(rid, {}).get("url", "")

                    # Decode body if base64-encoded (Chrome returns binary content as base64)
                    decoded_body = body
                    if is_b64 and body:
                        try:
                            decoded_body = base64.b64decode("".join(body.split())).decode("utf-8", errors="replace")
                        except Exception:
                            decoded_body = ""

                    # Direct m3u8 response
                    if decoded_body and ("#EXTM3U" in decoded_body or ".m3u8" in (req_url or "")):
                        if "#EXTM3U" in decoded_body:
                            return {
                                "url": req_url,
                                "status": resp_meta.get(rid, {}).get("status"),
                                "mime": resp_meta.get(rid, {}).get("mime"),
                                "req_headers": req_meta.get(rid, {}).get("headers", {}),
                                "resp_headers": resp_meta.get(rid, {}).get("headers", {}),
                                "body": decoded_body,
                                "base64Encoded": False,
                                "heartbeat": captured_heartbeat,
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

        except Exception as e:
            logger.error("Error processing performance logs: %s", e)

        time.sleep(0.08)

    logger.warning("No manifest found within %ds timeout", timeout)
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
    Cookies persist across browser recycles via the user-data-dir.
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
    # Also check page text for premium/not-yet-live keywords
    try:
        body = browser.find_element(By.TAG_NAME, "body").text or ""
        lower = body.lower()
        for phrase in ["get premium", "go premium", "premium only",
                       "subscribe to watch", "upgrade to watch",
                       "premium members only", "unlock this stream",
                       "live stream starting soon", "stream starting soon",
                       "event has not started", "stream will begin shortly",
                       "coming soon", "broadcast will begin"]:
            if phrase in lower:
                return phrase
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


@app.post("/capture")
def capture(req: CaptureRequest):
    """Navigate to a URL and capture an m3u8 manifest using the persistent session."""
    with _browser_lock:
        try:
            logger.info("Starting capture: %s (timeout=%ds, count=%d)",
                        req.url, req.timeout, _capture_count)
            browser = _get_browser()

            # Set page load timeout to match capture timeout + buffer
            try:
                browser.set_page_load_timeout(req.timeout + 30)
            except Exception:
                pass

            try:
                browser.get(req.url)
            except Exception as e:
                # Page load timeout — page may still be partially loaded,
                # continue to check for manifests that arrived before timeout
                logger.warning("Page load timeout/error for %s: %s", req.url, e)

            # Check for premium/paywall/not-live on the main page BEFORE iframe switch
            paywall = _check_paywall(browser)
            if paywall:
                logger.info("Paywall detected (main frame) for %s: %s", req.url, paywall)
                return {"ok": False, "error": f"Premium content: {paywall}"}

            if req.switch_iframe:
                try:
                    WebDriverWait(browser, 10).until(
                        EC.frame_to_be_available_and_switch_to_it((By.TAG_NAME, "iframe"))
                    )
                    logger.info("Switched to iframe")
                except Exception:
                    logger.debug("No iframe, continuing in main frame")

            # Check again inside the iframe
            paywall = _check_paywall(browser)
            if paywall:
                logger.info("Paywall detected (iframe) for %s: %s", req.url, paywall)
                return {"ok": False, "error": f"Premium content: {paywall}"}

            # Try clicking play button — pregame streams often need a click
            # before the m3u8 loads. Try common selectors, then fall back to
            # clicking the video element or player container.
            _try_click_play(browser)

            result = _wait_for_manifest(browser, timeout=req.timeout)
            if not result:
                return {"ok": False, "error": "No manifest captured within timeout"}

            body_text = _decode_body(result)
            if not body_text:
                return {
                    "ok": False,
                    "error": f"Captured resource is not HLS: {result.get('url')} (MIME: {result.get('mime')})",
                }

            ua = browser.execute_script("return navigator.userAgent")

            return {
                "ok": True,
                "manifest_url": result["url"],
                "body": body_text,
                "mime": result.get("mime"),
                "headers": result.get("resp_headers", {}),
                "heartbeat": result.get("heartbeat"),
                "user_agent": ua,
            }

        except Exception as e:
            logger.exception("Capture failed for %s", req.url)
            # Force a recycle on errors — browser may be in a bad state
            global _browser
            try:
                if _browser:
                    _browser.quit()
            except Exception:
                pass
            _browser = None
            return {"ok": False, "error": str(e)}

        finally:
            _release_browser()
