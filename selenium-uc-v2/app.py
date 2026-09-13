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

Endpoints (per the plan's confirmed load-bearing set, plus a debug/vision
capability added for AI/human troubleshooting -- /tab/* and /restart stay
out of scope: /tab/* has zero active callers since tab_proxy mode is dead,
/restart isn't called by any core/web code):
  POST /capture                              {url, timeout, switch_iframe, priority, debug}
  GET  /health
  POST /cookies/youtube
  GET  /screenshot                           live snapshot of the persistent main_tab
  GET  /debug/{debug_id}/screenshots         list checkpoint screenshots from a debug=True capture
  GET  /debug/{debug_id}/screenshot/{stage}  fetch one as PNG
"""

import asyncio
import logging
import os
import threading
import time
import uuid
from collections import deque

import nodriver as uc
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

import network_capture as nc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

_STARTUP_TIMEOUT = int(os.getenv("CHROME_STARTUP_TIMEOUT", "60"))
_PROFILE_DIR = os.getenv("CHROME_PROFILE_DIR", "/data/chrome-profile")
_MAX_TABS = int(os.getenv("SIDECAR_MAX_TABS", "3"))
# A tab that's still tracked as "open" this long after being opened is
# almost certainly leaked, not legitimately still running -- deadline is
# req.timeout+45 and req.timeout defaults to 60 (callers can set it higher,
# but this is generous headroom over any realistic capture, matching
# "definitely stale" rather than "might still be working").
_TAB_STALE_SECONDS = int(os.getenv("SIDECAR_TAB_STALE_SECONDS", "300"))
_TAB_SWEEP_INTERVAL = int(os.getenv("SIDECAR_TAB_SWEEP_INTERVAL", "60"))
# Cap how many past debug=True captures' screenshot dirs stick around --
# oldest-evicted. Small, real hardening detail: debug traffic could
# otherwise slowly fill the container's disk over a long session, the same
# "small things that accumulate silently over hours" lesson as the tab-leak
# fix above.
_MAX_DEBUG_DIRS = int(os.getenv("SIDECAR_MAX_DEBUG_DIRS", "20"))


def _evict_old_debug_dirs():
    try:
        if not os.path.isdir(nc.DEBUG_DIR):
            return
        dirs = [os.path.join(nc.DEBUG_DIR, d) for d in os.listdir(nc.DEBUG_DIR)]
        dirs = [d for d in dirs if os.path.isdir(d)]
        dirs.sort(key=lambda d: os.path.getmtime(d))
        for d in dirs[:-_MAX_DEBUG_DIRS] if len(dirs) > _MAX_DEBUG_DIRS else []:
            import shutil
            shutil.rmtree(d, ignore_errors=True)
    except Exception as e:
        logger.warning("Debug-dir eviction failed: %s", e)

_browser = None
_browser_lock = asyncio.Lock()
_capture_count = 0

# target_id -> opened_at timestamp, for every tab opened via _open_tracked_tab
# and not yet confirmed closed via _close_tab_safely. See that function's
# docstring and _sweep_stale_tabs for why this exists: PrioritySemaphore
# gates how many NEW captures can start, not whether every previously-opened
# tab actually got closed -- this dict plus the sweeper is what actually
# bounds real Chrome tab count over a long-running session.
_active_tabs: dict[str, float] = {}


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


def _tab_target_id(tab):
    try:
        return tab.target.target_id
    except Exception:
        return None


async def _open_tracked_tab(browser, url: str):
    """Wraps browser.get(url, new_tab=True) with registration into
    _active_tabs, so a tab that never gets confirmed-closed (see
    _close_tab_safely) is visible to _sweep_stale_tabs even if the request
    that opened it dies in some way that skips its own finally block
    (shouldn't happen given /capture's try/finally, but tracked at open
    time rather than only at close time so there's no gap where a tab
    exists but isn't yet trackable)."""
    tab = await browser.get(url, new_tab=True)
    tid = _tab_target_id(tab)
    if tid:
        _active_tabs[tid] = time.time()
    return tab


async def _close_tab_safely(tab, timeout=5):
    """Tab.close() sends Target.closeTarget over that tab's own CDP
    connection — if THIS tab's listener already died (the same failure mode
    fixed in network_capture.py's timeout-wrapped tail calls), this would
    otherwise hang forever too. Timeout-wrapped for the same reason.

    On success, untracks the tab immediately. On failure/timeout, the tab
    stays in _active_tabs -- _sweep_stale_tabs (a periodic background task,
    see its own docstring) is what actually bounds real Chrome tab count
    over a long-running session by force-closing anything that's been
    tracked open far longer than any legitimate capture could take."""
    if tab is None:
        return
    tid = _tab_target_id(tab)
    try:
        await asyncio.wait_for(tab.close(), timeout=timeout)
        if tid:
            _active_tabs.pop(tid, None)
    except Exception:
        if tid:
            logger.warning("Tab %s failed to close within %ds -- leaving it "
                            "tracked for the stale-tab sweeper", tid, timeout)


async def _sweep_stale_tabs():
    """Background loop, started at app startup: periodically reconciles
    _active_tabs against wall-clock time and force-closes anything that's
    been tracked open longer than _TAB_STALE_SECONDS -- a tab this old can
    only be one _close_tab_safely already tried and failed on (see that
    function), since a normal capture's own try/finally always attempts a
    close well before this threshold. This is the actual fix for the
    tab-leak residual risk found during tonight's code review: without it,
    PrioritySemaphore's logical slot count has no relationship to how many
    real Chrome renderer processes are actually still alive.

    Force-close goes straight through browser.main_tab (always kept alive
    by _get_browser's own health check) rather than needing the original
    Tab object -- Target.closeTarget only needs a target_id, and the
    original object may itself be part of why closing failed the first
    time."""
    while True:
        await asyncio.sleep(_TAB_SWEEP_INTERVAL)
        if _browser is None or not _active_tabs:
            continue
        now = time.time()
        stale = [tid for tid, opened_at in list(_active_tabs.items())
                 if now - opened_at > _TAB_STALE_SECONDS]
        for tid in stale:
            try:
                await asyncio.wait_for(
                    _browser.main_tab.send(uc.cdp.target.close_target(target_id=tid)),
                    timeout=5.0,
                )
                logger.warning("Stale-tab sweeper force-closed leaked tab %s "
                                "(open %ds)", tid, int(now - _active_tabs[tid]))
            except Exception as e:
                logger.warning("Stale-tab sweeper failed to force-close %s: %s", tid, e)
            # Remove regardless of outcome -- a target_id that's already
            # gone (closed some other way) would otherwise re-trigger this
            # every sweep forever; one force-close attempt per stale tab is
            # enough effort, not an infinite retry loop.
            _active_tabs.pop(tid, None)


@app.on_event("startup")
async def _start_sweeper():
    asyncio.create_task(_sweep_stale_tabs())


def _clear_stale_profile_locks():
    """Chrome's process-singleton mechanism (SingletonLock/SingletonCookie/
    SingletonSocket, symlinks in the profile dir) only gets cleaned up on a
    GRACEFUL Chrome exit -- an abrupt kill (container OOM, force-recreate,
    crash) leaves them behind, and every future launch against that same
    profile dir then fails immediately with "profile appears to be in use
    by another Google Chrome process" even though that process is long
    dead. Real, confirmed failure mode (not hypothetical): hit this exact
    thing after a chain of container recreates during testing -- 20.3s
    consistent failures on every single capture until these were cleared
    by hand. v1 (selenium-uc/app.py) already does exactly this before its
    own browser startup; v2 never ported it. _PROFILE_DIR is a persistent
    named volume (survives container recreate) specifically so the cookie/
    session state carries over -- but that means a stale lock survives
    right along with it unless something clears it, unlike a fresh-each-
    time temp profile that would never have this problem."""
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        path = os.path.join(_PROFILE_DIR, name)
        try:
            if os.path.lexists(path):
                os.remove(path)
                logger.info("Removed stale %s", name)
        except Exception as e:
            logger.warning("Failed to remove stale %s: %s", name, e)


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
            # Any tracked tabs belonged to the now-dead instance -- clear
            # them rather than leaving the sweeper trying to force-close
            # target_ids on a browser process that no longer exists.
            _active_tabs.clear()

    async with _browser_lock:
        if _browser is not None:  # another concurrent caller may have won the race
            return _browser
        _clear_stale_profile_locks()
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

    debug_id = uuid.uuid4().hex[:12] if req.debug else None

    await _slots.acquire(req.priority)
    tab = None
    try:
        browser = await _get_browser()
        # Fresh tab per capture (see module docstring for why) — run_capture
        # does its own navigation to req.url, so open blank here.
        tab = await _open_tracked_tab(browser, "about:blank")

        logger.info("Starting capture: %s (timeout=%ds, deadline=%ds, count=%d, priority=%s, debug_id=%s)",
                    req.url, req.timeout, deadline, _capture_count, req.priority, debug_id)

        outcome = await asyncio.wait_for(
            nc.run_capture(browser, tab, req.url, timeout=req.timeout,
                            switch_iframe=req.switch_iframe,
                            debug=req.debug, debug_id=debug_id),
            timeout=deadline,
        )
        _capture_count += 1

        if not outcome.ok:
            return {"ok": False, "error": outcome.error or "Capture failed", "debug_id": debug_id}

        return {
            "ok": True,
            "manifest_url": outcome.manifest_url,
            "body": outcome.body,
            "mime": outcome.mime,
            "headers": outcome.headers,
            "user_agent": outcome.user_agent,
            "referer": outcome.referer,
            "cookies": outcome.cookies,
            "debug_id": debug_id,
            # heartbeat intentionally omitted — confirmed dead data, see
            # the sidecar-2.0 plan's Phase 1 gap-map item 4.
        }
    except asyncio.TimeoutError:
        logger.error("Capture deadline exceeded for %s (%ds)", req.url, deadline)
        return {"ok": False, "error": f"Capture deadline exceeded ({deadline}s)", "debug_id": debug_id}
    except Exception as e:
        logger.exception("Capture failed for %s", req.url)
        return {"ok": False, "error": str(e), "debug_id": debug_id}
    finally:
        await _close_tab_safely(tab)
        _slots.release()
        if debug_id:
            _evict_old_debug_dirs()


def _tracked_tabs_status() -> dict:
    now = time.time()
    ages = [now - t for t in _active_tabs.values()]
    return {
        "tracked_open": len(_active_tabs),
        "oldest_open_seconds": int(max(ages)) if ages else 0,
        "stale_threshold_seconds": _TAB_STALE_SECONDS,
    }


@app.get("/health")
async def health():
    """Never hangs (fixed timeout on the probe) — any response, even a
    ready=False one, reads as "sidecar alive" to core's check_selenium()."""
    global _browser
    if _browser is None:
        return {"ready": True, "browser_alive": False, "capture_count": _capture_count,
                "tabs": _slots.status(), "tracked_tabs": _tracked_tabs_status()}
    try:
        await asyncio.wait_for(
            _browser.main_tab.send(uc.cdp.runtime.evaluate(expression="1")),
            timeout=5.0,
        )
        alive = True
    except Exception:
        alive = False
    return {"ready": True, "browser_alive": alive, "capture_count": _capture_count,
            "tabs": _slots.status(), "tracked_tabs": _tracked_tabs_status()}


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
        tab = await _open_tracked_tab(browser, "https://youtube.com")
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


@app.get("/screenshot")
async def screenshot():
    """Live snapshot of the persistent main_tab -- matches v1's /screenshot
    naming/endpoint (selenium-uc/app.py:793-822). Less useful than the
    debug=True checkpoint captures for debugging a SPECIFIC failed capture
    (v2's ephemeral tabs are closed by the time anyone could screenshot
    them otherwise) but cheap to keep for parity and quick manual "what
    does headful Chrome look like right now" checks between requests."""
    if _browser is None:
        raise HTTPException(status_code=503, detail="no browser yet")
    try:
        b64_png = await asyncio.wait_for(
            _browser.main_tab.send(uc.cdp.page.capture_screenshot()), timeout=10.0,
        )
        if not b64_png:
            raise HTTPException(status_code=502, detail="empty screenshot data")
        import base64
        return Response(content=base64.b64decode(b64_png), media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


def _safe_token(value: str) -> bool:
    """debug_id/stage come straight from the URL path -- reject anything
    that isn't a plain alnum/underscore/hyphen token before using it in a
    filesystem path, so a caller can't path-traverse out of DEBUG_DIR
    (e.g. stage="../../../etc/passwd"). Cheap defense-in-depth even on an
    internal-only sidecar."""
    return bool(value) and all(c.isalnum() or c in "_-" for c in value)


@app.get("/debug/{debug_id}/screenshots")
async def list_debug_screenshots(debug_id: str):
    """List checkpoint screenshot stage names available for a debug=True
    capture (see network_capture.py::_debug_screenshot for the four
    stages: 01_after_load, 02_after_iframe_switch, 03_after_click,
    04_after_wait -- not all four are guaranteed present, e.g. a capture
    that succeeded at the top level never reaches the iframe/click
    stages)."""
    if not _safe_token(debug_id):
        raise HTTPException(status_code=400, detail="invalid debug_id")
    debug_dir = os.path.join(nc.DEBUG_DIR, debug_id)
    if not os.path.isdir(debug_dir):
        raise HTTPException(status_code=404, detail="unknown debug_id")
    stages = sorted(f[:-4] for f in os.listdir(debug_dir) if f.endswith(".png"))
    return {"ok": True, "debug_id": debug_id, "stages": stages}


@app.get("/debug/{debug_id}/screenshot/{stage}")
async def get_debug_screenshot(debug_id: str, stage: str):
    if not (_safe_token(debug_id) and _safe_token(stage)):
        raise HTTPException(status_code=400, detail="invalid debug_id/stage")
    path = os.path.join(nc.DEBUG_DIR, debug_id, f"{stage}.png")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="no screenshot for that debug_id/stage")
    with open(path, "rb") as f:
        return Response(content=f.read(), media_type="image/png")
