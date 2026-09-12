"""
Sidecar 2.0 — Phase 1: single persistent nodriver browser, one tab, real
FastAPI service satisfying the exact same /capture contract as selenium-uc/
app.py (see the sidecar-2.0 plan for the full contract trace). No tab pool
yet — that's Phase 2. This proves the capture logic (network_capture.py) is
correct against a REUSED persistent browser/tab before adding concurrency.

Endpoints (per the plan's confirmed load-bearing set — everything else,
including /tab/*, /restart, /screenshot, is out of scope: /tab/* has zero
active callers since tab_proxy mode is dead, /restart and /screenshot aren't
called by any core/web code):
  POST /capture         {url, timeout, switch_iframe, priority}
  GET  /health
  POST /cookies/youtube
"""

import asyncio
import logging
import os
import threading

import nodriver as uc
from fastapi import FastAPI
from pydantic import BaseModel

import network_capture as nc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

_STARTUP_TIMEOUT = int(os.getenv("CHROME_STARTUP_TIMEOUT", "60"))
_PROFILE_DIR = os.getenv("CHROME_PROFILE_DIR", "/data/chrome-profile")

_browser = None
_browser_lock = asyncio.Lock()
_capture_count = 0


def _stop_browser_safely(browser, timeout=8):
    """nodriver's Browser.stop() is a plain sync call that sends
    terminate()/kill() OS signals directly (see nodriver/core/browser.py) —
    much lower hang-risk than selenium's .quit() (an HTTP round-trip to a
    possibly-wedged chromedriver), but still wrapped in a thread+timeout for
    the same defense-in-depth reasoning as selenium-uc's
    _quit_browser_safely: a hung stop() should cost at most `timeout`
    seconds, never block the caller indefinitely."""
    if browser is None:
        return
    done = threading.Event()

    def _do_stop():
        try:
            browser.stop()
        except Exception:
            pass
        finally:
            done.set()

    threading.Thread(target=_do_stop, daemon=True).start()
    if not done.wait(timeout=timeout):
        logger.warning("browser.stop() didn't return within %ds — abandoning it", timeout)


async def _get_browser():
    """Lazily create the persistent browser, or recreate it if the existing
    one has died. Single instance for Phase 1 — see the plan's §1 for the
    tab-pool layer this grows into in Phase 2."""
    global _browser
    if _browser is not None:
        try:
            await asyncio.wait_for(
                _browser.main_tab.send(uc.cdp.runtime.evaluate(expression="1")),
                timeout=5.0,
            )
            return _browser
        except Exception:
            logger.warning("Existing browser unresponsive, recreating")
            _stop_browser_safely(_browser)
            _browser = None

    try:
        _browser = await asyncio.wait_for(
            uc.start(headless=False, user_data_dir=_PROFILE_DIR),
            timeout=_STARTUP_TIMEOUT,
        )
    except Exception as e:
        logger.error("Browser startup failed: %s", e)
        raise
    return _browser


class CaptureRequest(BaseModel):
    url: str
    timeout: int = 60
    switch_iframe: bool = True
    debug: bool = False
    # "low" = background/fallback-warming work; unused in Phase 1 (no pool
    # to prioritize across yet) but accepted now so the request contract is
    # already stable before Phase 2 adds the dispatch queue.
    priority: str = "high"


@app.post("/capture")
async def capture(req: CaptureRequest):
    global _capture_count, _browser
    deadline = req.timeout + 45  # same "+45s covers page load + wait + overhead" convention as v1

    async with _browser_lock:
        try:
            browser = await _get_browser()
            tab = browser.main_tab

            logger.info("Starting capture: %s (timeout=%ds, deadline=%ds, count=%d)",
                        req.url, req.timeout, deadline, _capture_count)

            outcome = await asyncio.wait_for(
                nc.run_capture(browser, tab, req.url, timeout=req.timeout,
                                switch_iframe=req.switch_iframe),
                timeout=deadline,
            )
            _capture_count += 1

            if not outcome.ok:
                return {"ok": False, "error": outcome.error or "Capture failed"}

            return {
                "ok": True,
                "manifest_url": outcome.manifest_url,
                "body": outcome.body,
                "mime": outcome.mime,
                "headers": outcome.headers,
                "user_agent": outcome.user_agent,
                "referer": outcome.referer,
                "cookies": outcome.cookies,
                # heartbeat intentionally omitted — confirmed dead data, see
                # the sidecar-2.0 plan's Phase 1 gap-map item 4.
            }
        except asyncio.TimeoutError:
            logger.error("Capture deadline exceeded for %s (%ds), recycling browser",
                         req.url, deadline)
            _stop_browser_safely(_browser)
            _browser = None
            return {"ok": False, "error": f"Capture deadline exceeded ({deadline}s)"}
        except Exception as e:
            logger.exception("Capture failed for %s", req.url)
            return {"ok": False, "error": str(e)}


@app.get("/health")
async def health():
    """Never hangs (fixed timeout on the probe) — any response, even a
    ready=False one, reads as "sidecar alive" to core's check_selenium()."""
    global _browser
    if _browser is None:
        return {"ready": True, "browser_alive": False, "capture_count": _capture_count}
    try:
        await asyncio.wait_for(
            _browser.main_tab.send(uc.cdp.runtime.evaluate(expression="1")),
            timeout=5.0,
        )
        alive = True
    except Exception:
        alive = False
    return {"ready": True, "browser_alive": alive, "capture_count": _capture_count}


@app.post("/cookies/youtube")
async def cookies_youtube():
    """Navigate the persistent browser to youtube.com and return cookies in
    Netscape format (yt-dlp compatible) — ported from selenium-uc/app.py's
    /cookies/youtube, confirmed load-bearing (core/youtube.py calls this)."""
    async with _browser_lock:
        try:
            browser = await _get_browser()
            tab = browser.main_tab
            await tab.get("https://youtube.com")
            await asyncio.sleep(2)
            cookies = await nc.get_cookies(tab)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    lines = ["# Netscape HTTP Cookie File"]
    for c in cookies:
        if not c.get("domain"):
            continue
        domain = c["domain"]
        flag = "TRUE" if domain.startswith(".") else "FALSE"
        path = c.get("path") or "/"
        secure = "TRUE" if c.get("secure") else "FALSE"
        expiry = c.get("expiry") or 0
        lines.append(f"{domain}\t{flag}\t{path}\t{secure}\t{expiry}\t{c['name']}\t{c['value']}")
    return {"ok": True, "cookies_txt": "\n".join(lines)}
