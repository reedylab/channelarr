"""
Sidecar 2.0 — Phase 2: bounded concurrent captures via ephemeral per-capture
tabs in one shared, persistent nodriver browser instance, dispatched through
a priority-aware slot limiter.

Deliberately NOT a fixed pool of long-lived, reused tabs (the shape sketched
in the original sidecar-2.0 plan's §1/§5/§6). Phase 1's real debugging
session found that reusing one tab across calls is exactly what caused both
real bugs hit that night: nodriver's add_handler leaking state across calls
forever, and — the actual root cause of the hang-to-full-deadline
regression — a busy real page's burst of concurrent CDP commands silently
killing nodriver's Connection._listener (an unguarded KeyError in its own
source), after which every future .send() on that SAME connection hangs
forever. A tab that's opened fresh per capture and closed afterward sidesteps
that whole class of bug: a dead listener only ever affects the one capture
that killed it, never any other request, past or future. Tabs are cheap CDP
targets within one Chrome process (confirmed in Phase 0's crash-isolation
test) — "open, capture, close" per request is the natural fit given that,
not a compromise relative to a real tab pool.

Concurrency comes from however many tabs are open at once (bounded by
SIDECAR_MAX_TABS), not from a fixed set of pre-warmed tabs waiting to be
assigned. Priority (high = live-viewer-driven, low = background fallback-
warming) only matters once every slot is in use — see PrioritySemaphore.

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
from collections import deque

import nodriver as uc
from fastapi import FastAPI
from pydantic import BaseModel

import network_capture as nc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

_STARTUP_TIMEOUT = int(os.getenv("CHROME_STARTUP_TIMEOUT", "60"))
_PROFILE_DIR = os.getenv("CHROME_PROFILE_DIR", "/data/chrome-profile")
_MAX_TABS = int(os.getenv("SIDECAR_MAX_TABS", "3"))

_browser = None
_browser_lock = asyncio.Lock()
_capture_count = 0


class PrioritySemaphore:
    """A counting semaphore where, once every slot is taken, a newly-freed
    slot goes to the longest-waiting HIGH-priority request before any
    already-queued LOW-priority one — same no-preemption, FIFO-within-tier
    semantics as the original sidecar-2.0 plan's §6 (a shared browser lock
    version of this same idea). Priority is moot whenever a slot is free —
    which is the actual concurrency win Phase 2 exists for: most requests
    won't queue at all as long as the tab cap isn't exhausted."""

    def __init__(self, value: int):
        self._value = value
        self._waiters = {"high": deque(), "low": deque()}

    async def acquire(self, priority: str = "high"):
        priority = priority if priority in self._waiters else "high"
        if self._value > 0:
            self._value -= 1
            return
        fut = asyncio.get_running_loop().create_future()
        self._waiters[priority].append(fut)
        await fut

    def release(self):
        for tier in ("high", "low"):
            q = self._waiters[tier]
            while q:
                fut = q.popleft()
                if not fut.done():
                    fut.set_result(None)
                    return
        self._value += 1

    def status(self) -> dict:
        return {
            "free": self._value,
            "cap": _MAX_TABS,
            "queued_high": len(self._waiters["high"]),
            "queued_low": len(self._waiters["low"]),
        }


_slots = PrioritySemaphore(_MAX_TABS)


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


async def _close_tab_safely(tab, timeout=5):
    """Tab.close() sends Target.closeTarget over that tab's own CDP
    connection — if THIS tab's listener already died (the same failure mode
    fixed in network_capture.py's timeout-wrapped tail calls), this would
    otherwise hang forever too. Timeout-wrapped for the same reason; a tab
    that fails to close cleanly just leaks until the whole browser is
    eventually recycled (existing _get_browser() health check), not a
    request-blocking hang.

    Known residual risk, not yet mitigated: nothing tracks or bounds actual
    live Chrome tab count independently of PrioritySemaphore's logical slot
    count -- the semaphore gates how many NEW captures can start, not
    whether every previously-opened tab actually got closed. If close
    silently times out occasionally over many hours of real operation,
    leaked tabs (each a real renderer process) could accumulate slowly
    until the next full browser recycle. Worth watching via the sustained
    resource-observation loop (chrome process count) rather than fixing
    preemptively tonight -- a real fix would mean tracking open tab IDs and
    reconciling against browser.targets periodically, more Phase 2.5 scope
    than a quick patch."""
    if tab is None:
        return
    try:
        await asyncio.wait_for(tab.close(), timeout=timeout)
    except Exception:
        pass


async def _get_browser():
    """Lazily create the persistent browser, or recreate it if the existing
    one has died. One shared instance; concurrency comes from multiple
    ephemeral tabs within it (see module docstring), not multiple instances.

    Double-checked locking around creation only (not the whole capture) —
    with captures now running concurrently (Phase 2), several requests can
    hit an empty _browser at once (e.g. right at startup); without the lock
    here each would race to call uc.start() independently, launching
    multiple Chrome processes. The health-check probe above stays outside
    the lock so already-healthy calls never contend on it."""
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

    async with _browser_lock:
        if _browser is not None:  # another concurrent caller may have won the race
            return _browser
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
    # "low" = background/fallback-warming work; "high" = demand-driven/live-
    # viewer work. Only matters once every tab slot is in use — see
    # PrioritySemaphore.
    priority: str = "high"


@app.post("/capture")
async def capture(req: CaptureRequest):
    global _capture_count
    deadline = req.timeout + 45  # same "+45s covers page load + wait + overhead" convention as v1

    await _slots.acquire(req.priority)
    tab = None
    try:
        browser = await _get_browser()
        # Fresh tab per capture (see module docstring for why) — run_capture
        # does its own navigation to req.url, so open blank here.
        tab = await browser.get("about:blank", new_tab=True)

        logger.info("Starting capture: %s (timeout=%ds, deadline=%ds, count=%d, priority=%s)",
                    req.url, req.timeout, deadline, _capture_count, req.priority)

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
        logger.error("Capture deadline exceeded for %s (%ds)", req.url, deadline)
        return {"ok": False, "error": f"Capture deadline exceeded ({deadline}s)"}
    except Exception as e:
        logger.exception("Capture failed for %s", req.url)
        return {"ok": False, "error": str(e)}
    finally:
        await _close_tab_safely(tab)
        _slots.release()


@app.get("/health")
async def health():
    """Never hangs (fixed timeout on the probe) — any response, even a
    ready=False one, reads as "sidecar alive" to core's check_selenium()."""
    global _browser
    if _browser is None:
        return {"ready": True, "browser_alive": False, "capture_count": _capture_count,
                "tabs": _slots.status()}
    try:
        await asyncio.wait_for(
            _browser.main_tab.send(uc.cdp.runtime.evaluate(expression="1")),
            timeout=5.0,
        )
        alive = True
    except Exception:
        alive = False
    return {"ready": True, "browser_alive": alive, "capture_count": _capture_count,
            "tabs": _slots.status()}


@app.post("/cookies/youtube")
async def cookies_youtube():
    """Navigate a fresh tab to youtube.com and return cookies in Netscape
    format (yt-dlp compatible) — ported from selenium-uc/app.py's
    /cookies/youtube, confirmed load-bearing (core/youtube.py calls this).
    Uses its own ephemeral tab rather than main_tab, same reasoning as
    /capture (see module docstring)."""
    await _slots.acquire("high")
    tab = None
    try:
        browser = await _get_browser()
        tab = await browser.get("https://youtube.com", new_tab=True)
        await asyncio.sleep(2)
        cookies = await nc.get_cookies(tab, scoped=True)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        await _close_tab_safely(tab)
        _slots.release()

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
    # Field names (netscape/count) match v1's exact contract -- confirmed
    # via core/youtube.py, which reads data.get("netscape") and
    # data.get("count", 0) specifically. A prior version of this endpoint
    # used "cookies_txt" instead of "netscape" -- a real contract bug that
    # would have made every response silently useless downstream (always
    # read as empty via `data.get("netscape") or ""`) regardless of
    # whether cookie extraction itself worked.
    return {"ok": True, "netscape": "\n".join(lines), "count": len(cookies)}
