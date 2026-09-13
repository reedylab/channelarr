"""
multiplex — Phase 2: bounded concurrent captures via ephemeral per-capture
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
  POST /capture                              {url, timeout, switch_iframe, priority, debug, click_sequence}
  POST /capture/multi-player                 {wrapper_url, candidates, per_candidate_timeout, priority}
  POST /test/watch-channel                   {channel_id, duration_seconds, poll_interval_seconds, priority}
  POST /relay/start                          {url, click_sequence} -> {session_id}
  GET  /relay/{session_id}/chunks            drains accumulated captureStream()+MediaRecorder chunks
  POST /relay/{session_id}/stop
  POST /relay/{session_id}/debug-eval        {expression} -- interactive tuning only, not called by channelarr
  GET  /relay/{session_id}/debug-targets     lists every raw CDP target (incl. workers) -- tuning only
  POST /debug/sniff-network                  {url, click_sequence, url_substrings} -- investigation tool only
  GET  /health
  POST /cookies/youtube
  GET  /screenshot                           live snapshot of the persistent main_tab
  GET  /debug/{debug_id}/screenshots         list checkpoint screenshots from a debug=True capture
  GET  /debug/{debug_id}/screenshot/{stage}  fetch one as PNG
"""

import asyncio
import logging
import os
import signal
import threading
import time
import uuid
from collections import deque

import nodriver as uc
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

import network_capture as nc
import relay_capture as rc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _patch_transaction_call():
    """Fix a real, general bug in nodriver's own Connection/Transaction
    machinery, found via a live hang (a real capture went silent right after
    page-settle and never logged anything again until the outer deadline
    killed it -- no iframe-candidate discovery, nothing).

    nodriver.core.connection.Transaction.__call__ parses a command response
    against its typed cdp.* schema (`self.__cdp_obj__.send(response["result"])`).
    If that schema doesn't match what THIS Chrome version actually sent (a
    missing/renamed field -- already confirmed to happen at least twice this
    session: Cookie.from_json()'s 'sameParty', and whatever field disagreement
    killed the capture above), it catches the KeyError but only re-raises it
    -- never calling self.set_exception(), so the awaiting Transaction's
    future never resolves, AND the bare re-raise propagates out of
    Connection._listener's unguarded `tx(**message)` call (no try/except
    there, unlike the sibling event-parsing branch a few lines below it,
    which already handles this correctly). That silently kills the whole
    connection's listener task -- every future `.send()` on it hangs forever,
    with zero log output, until whatever outer deadline eventually fires.

    This isn't a one-field bug -- it's structural, and can be triggered by
    ANY typed command response Chrome's actual shape has drifted on. Patched
    generally here rather than chasing individual field names as they turn
    up (already found two; a newer Chrome will likely find more). Process-
    local monkeypatch only, nothing written to the installed package."""
    from nodriver.core.connection import Transaction, ProtocolException

    def _fixed_call(self, **response):
        if "error" in response:
            return self.set_exception(ProtocolException(response["error"]))
        try:
            self.__cdp_obj__.send(response["result"])
        except StopIteration as e:
            if not self.done():
                self.set_result(e.value)
        except Exception as e:
            # Broad on purpose, not just KeyError -- a Chrome/cdp schema
            # mismatch can surface as several exception shapes depending on
            # which field the typed generator was mid-parsing when it hit
            # something unexpected (renamed/missing/differently-typed field).
            # Any of them must resolve this Transaction rather than escape
            # unguarded into Connection._listener, which has no try/except
            # around its `tx(**message)` call site.
            logger.warning(
                "Transaction typed-response parse failed for %s (Chrome/cdp "
                "schema drift, not fatal to the connection): %s: %s",
                self.method, type(e).__name__, e,
            )
            if not self.done():
                self.set_exception(e)

    Transaction.__call__ = _fixed_call


def _patch_listener_id_dispatch():
    """Second half of the same fix. _patch_transaction_call above guards
    Transaction.__call__ itself, but Connection._listener's id-response
    branch has a SEPARATE unguarded failure point one line earlier:

        if "id" in message:
            tx: Transaction = self.mapper.pop(message["id"])   # <-- here
            tx(**message)

    self.mapper.pop(message["id"]) has no default -- if a response ever
    arrives for an id genuinely not in the mapper (a duplicate/stale
    response, or one for a transaction something else already removed),
    the bare KeyError escapes right here, before tx(**message) is ever
    reached, same as the schema-drift case but at a different point in
    the same branch. Not yet observed directly (unlike the schema-drift
    case, which killed a real capture), but it's the same class of gap in
    the same unguarded branch, cheap to close defensively, and matches
    this exact concern already raised once this session for this same
    line. Full copy of the real _listener (connection.py) with one
    change: the id-branch wrapped in try/except instead of bare -- same
    technique session_attach.py's own hand-written listener copy uses for
    its event branch, now applied symmetrically to the id branch in both
    places (see the matching edit in session_attach.py)."""
    import asyncio as _asyncio
    import json as _json
    import logging as _logging
    from asyncio import iscoroutine, iscoroutinefunction
    import websockets.exceptions
    from nodriver.core.connection import Connection, ProtocolException
    from nodriver import cdp

    _logger = _logging.getLogger("nodriver.core.connection")

    async def _fixed_listener(self):
        while True:
            try:
                async with self._lock:
                    raw = await _asyncio.wait_for(self.websocket.recv(), 0.05)
            except ProtocolException:
                break
            except websockets.exceptions.ConnectionClosedOK:
                await self.disconnect()
                break
            except websockets.exceptions.ConnectionClosed:
                await self.disconnect()
                break
            except _asyncio.TimeoutError:
                await _asyncio.sleep(0.05)
                continue
            except Exception as e:
                _logger.info("error when receiving websocket response: %s" % e, exc_info=True)
                raise
            else:
                message = _json.loads(raw)
                if "id" in message:
                    try:
                        tx = self.mapper.pop(message["id"])
                        tx(**message)
                    except Exception as e:
                        # Same principle as _patch_transaction_call: never let
                        # one bad message kill this connection's whole
                        # listener. Whatever awaits this id (if anything still
                        # does) times out normally instead of hanging forever
                        # with no explanation.
                        logger.warning(
                            "Listener id-dispatch failed for message id=%s "
                            "(dropping, not fatal to the connection): %s: %s",
                            message.get("id"), type(e).__name__, e,
                        )
                    continue
                try:
                    event = cdp.util.parse_json_event(message)
                except Exception:
                    continue
                if type(event) not in self.handlers:
                    continue
                callbacks = self.handlers[type(event)]
                if not callbacks:
                    continue
                for callback in callbacks:
                    try:
                        if iscoroutinefunction(callback) or iscoroutine(callback):
                            try:
                                _asyncio.create_task(callback(event, self))
                            except TypeError:
                                _asyncio.create_task(callback(event))
                        else:
                            try:
                                callback(event, self)
                            except TypeError:
                                callback(event)
                    except Exception as e:
                        _logger.warning(
                            "exception in callback %s for event %s => %s",
                            callback, event.__class__.__name__, e, exc_info=True,
                        )

    # Connection uses a custom metaclass (CantTouchThis) whose __setattr__
    # blocks plain class-attribute assignment here ("don't set '_listener' on
    # the Connection class directly") -- it wants per-instance patching
    # instead (see session_attach.py's own _ensure_patched for that pattern,
    # used there because only tabs that actually attach a child session need
    # the extra dispatch logic). This fix is semantically global though --
    # ANY connection's id-response branch can hit this, not just tabs that
    # attach sessions -- so bypass the metaclass guard directly via type's
    # own __setattr__ (skips CantTouchThis.__setattr__ entirely) rather than
    # patching every tab instance individually.
    type.__setattr__(Connection, "_listener", _fixed_listener)


_patch_transaction_call()
_patch_listener_id_dispatch()

app = FastAPI()

_STARTUP_TIMEOUT = int(os.getenv("CHROME_STARTUP_TIMEOUT", "60"))
_PROFILE_DIR = os.getenv("CHROME_PROFILE_DIR", "/data/chrome-profile")
_MAX_TABS = int(os.getenv("SIDECAR_MAX_TABS", "3"))
# channelarr itself, reachable at this address because multiplex shares its
# network_mode: service:gluetun namespace (confirmed via compose) -- used
# only by /test/watch-channel to drive channelarr's own /diagnostics-wall
# page for repeatable browser+diagnostics playback testing.
_CHANNELARR_BASE_URL = os.getenv("CHANNELARR_BASE_URL", "http://localhost:5045")
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

# Long-lived browser-tab-relay sessions (see relay_capture.py) -- a
# fundamentally different lifecycle than every other tab in this service
# (open, single-shot, close): a relay tab stays open, playing, for the
# whole channel's runtime. Deliberately NOT registered in _active_tabs /
# _sweep_stale_tabs -- that sweeper's whole job is killing tabs that have
# been open "too long", which is exactly wrong for a tab that's SUPPOSED
# to run for hours. Idle sweep here means "hasn't been polled recently"
# (the consumer, TabRelaySource, almost certainly died) rather than "has
# been open too long".
# session_id -> {"tab": Tab, "started_at": float, "last_poll_at": float}
_relay_sessions: dict[str, dict] = {}
_relay_sessions_lock = asyncio.Lock()
# session_ids reserved (counted against _RELAY_MAX_SESSIONS) while their tab
# is still starting up -- kept OUT of _relay_sessions itself so a
# concurrent sweep-loop tick can never iterate a half-built entry.
_relay_sessions_starting: set = set()
_RELAY_MAX_SESSIONS = int(os.getenv("SIDECAR_MAX_RELAY_SESSIONS", "2"))
_RELAY_IDLE_TIMEOUT_SECONDS = int(os.getenv("SIDECAR_RELAY_IDLE_TIMEOUT_SECONDS", "45"))
_RELAY_SWEEP_INTERVAL = int(os.getenv("SIDECAR_RELAY_SWEEP_INTERVAL", "15"))


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

    Real, confirmed bug (found under sustained production load, direct
    process inspection): Target.closeTarget succeeding does NOT reliably
    terminate the underlying OS renderer process on a real, content-heavy
    page. A tab that ran to its full capture timeout (still-loading ads/
    trackers/media, retrying network requests) can leave its renderer
    process alive and actively burning CPU (confirmed: 30-40% CPU,
    sustained, well after a "successful" close) even though CDP's own
    bookkeeping (browser.targets, _active_tabs) shows a clean, closed
    state — accumulating roughly one leaked process per timed-out capture
    under real load. Reproduced directly: 10 sequential + 8 concurrent
    SYNTHETIC (data:) captures leaked nothing at all; real-site captures
    that ran to their full timeout did, every time. Navigating to
    about:blank BEFORE closing (a well-known technique in browser-
    automation tooling for tearing down heavy pages) stops the page's own
    JS/network/media activity first, giving Chrome a clean, idle target to
    actually tear down -- rather than asking it to close a target with
    live in-flight work still attached.

    On success, untracks the tab immediately. On failure/timeout, the tab
    stays in _active_tabs -- _sweep_stale_tabs (a periodic background task,
    see its own docstring) is what actually bounds real Chrome tab count
    over a long-running session by force-closing anything that's been
    tracked open far longer than any legitimate capture could take."""
    if tab is None:
        return
    tid = _tab_target_id(tab)
    try:
        try:
            await asyncio.wait_for(tab.get("about:blank"), timeout=timeout)
        except Exception:
            pass  # best-effort -- still attempt the real close below regardless
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
        logger.info("[sweeper] tick (browser=%s, active_tabs=%d)",
                    "alive" if _browser is not None else "none", len(_active_tabs))
        # OS-level orphaned-renderer reap runs every pass regardless of
        # _active_tabs state -- it's checking real OS processes CDP-level
        # tracking can't see at all, not reconciling _active_tabs itself.
        if _browser is not None:
            killed = await _reap_orphaned_renderers()
            if killed:
                logger.info("[sweeper] OS-level reap killed %d orphaned renderer(s)", killed)
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


async def _sweep_idle_relay_sessions():
    """Background loop, started at app startup: closes any relay session
    that hasn't been polled in _RELAY_IDLE_TIMEOUT_SECONDS. A relay tab is
    SUPPOSED to stay open for a long time, so age-since-open (the metric
    _sweep_stale_tabs uses) is the wrong signal here -- age-since-last-poll
    is what actually indicates the consumer (TabRelaySource, in channelarr)
    died or stopped without calling /relay/{id}/stop (container restart,
    crash, network blip) and this tab is now just burning CPU decoding
    video nobody is reading."""
    while True:
        await asyncio.sleep(_RELAY_SWEEP_INTERVAL)
        if not _relay_sessions:
            continue
        now = time.time()
        idle = [sid for sid, s in list(_relay_sessions.items())
                if now - s["last_poll_at"] > _RELAY_IDLE_TIMEOUT_SECONDS]
        for sid in idle:
            logger.warning("[relay-sweeper] session %s idle >%ds (no poll) -- closing",
                            sid, _RELAY_IDLE_TIMEOUT_SECONDS)
            await _close_relay_session(sid)


def _proc_age_seconds(pid: int) -> float | None:
    """Process age via /proc/<pid>/stat's starttime field (clock ticks since
    boot, field 22) compared against /proc/uptime -- the standard, precise
    way to get a process's real age without needing to have tracked its
    creation ourselves. Returns None if the process is already gone or
    unreadable (races are expected/harmless here, caller treats as skip)."""
    try:
        with open("/proc/uptime") as f:
            uptime = float(f.read().split()[0])
        with open(f"/proc/{pid}/stat") as f:
            # cmdline can contain ')' and spaces, so split on the LAST ')'
            # to safely find the field-22 boundary regardless of its content.
            fields = f.read().rsplit(")", 1)[1].split()
        clk_tck = os.sysconf("SC_CLK_TCK")
        starttime_ticks = float(fields[19])  # index 19 = field 22 minus the 2 we split off
        proc_start_seconds = starttime_ticks / clk_tck
        return uptime - proc_start_seconds
    except Exception:
        return None


_protected_renderer_pids: set[int] = set()


def _renderer_pids_now() -> set[int]:
    """All current /proc-visible chrome renderer PIDs."""
    pids = set()
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().decode(errors="replace")
        except Exception:
            continue
        if "--type=renderer" in cmdline:
            pids.add(pid)
    return pids


def _snapshot_protected_renderer_pids():
    """Called right after a fresh browser.start() succeeds, before any
    capture has run. Whatever renderer PID(s) exist at that moment belong
    to the persistent main tab + chrome-internal helper targets (new-tab-
    page, omnibox popups, etc.) -- legitimate, meant to live for the whole
    container lifetime, and must never be touched by the reaper below.

    Found the hard way that CDP's own SystemInfo.getProcessInfo() does NOT
    distinguish these from orphaned ones -- it reports every renderer
    process still alive regardless of whether its target was closed,
    confirmed by watching two real natural sweep ticks report the exact
    same "known" PID set both before and after a real leak was created and
    left unreaped. A protected-baseline-at-startup snapshot is a much
    simpler, more reliable signal: multiplex's own design guarantees no
    ephemeral per-capture tab should ever legitimately outlive one capture
    (~105s at defaults), so anything NOT in this baseline that's still
    alive well past that is unambiguously leaked, full stop."""
    global _protected_renderer_pids
    _protected_renderer_pids = _renderer_pids_now()
    logger.info("[sweeper] protected baseline renderer pids: %s",
                sorted(_protected_renderer_pids))


async def _reap_orphaned_renderers(grace_seconds: int = 120):
    """OS-level enforcement, independent of and in addition to CDP-level tab
    tracking (_active_tabs/_sweep_stale_tabs above). Real, confirmed bug
    (found under sustained production load): Target.closeTarget succeeding
    does NOT reliably terminate a real, content-heavy page's renderer
    process -- confirmed via direct /proc inspection, a renderer can still
    be alive and actively burning CPU (30-40%, sustained) minutes after its
    tab was "successfully" closed and untracked. CDP-level bookkeeping
    (browser.targets, _active_tabs) cannot see this at all.

    Any /proc-visible renderer PID that is (a) NOT in the startup-time
    protected baseline (_snapshot_protected_renderer_pids) and (b) older
    than grace_seconds is definitionally orphaned -- no legitimate
    ephemeral capture tab lives anywhere near that long -- and gets killed
    directly by PID. grace_seconds defaults well above the ~105s outer
    per-capture deadline specifically so a real, still-in-flight capture
    is never mistaken for a leak."""
    killed = 0
    for pid in _renderer_pids_now():
        if pid in _protected_renderer_pids:
            continue
        age = _proc_age_seconds(pid)
        if age is None or age < grace_seconds:
            continue
        try:
            os.kill(pid, signal.SIGKILL)
            killed += 1
            logger.warning(
                "OS-level reaper killed orphaned renderer pid=%d (age=%.0fs, "
                "not in protected baseline) -- Target.closeTarget succeeded "
                "but the OS process never actually died",
                pid, age,
            )
        except ProcessLookupError:
            pass  # already gone, fine
        except Exception as e:
            logger.warning("OS-level reaper failed to kill pid=%d: %s", pid, e)
    return killed


@app.on_event("startup")
async def _start_sweeper():
    logger.info("[sweeper] starting background task (interval=%ds)", _TAB_SWEEP_INTERVAL)
    task = asyncio.create_task(_sweep_stale_tabs())

    def _log_if_died(t):
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            logger.error("[sweeper] background task died: %s: %s", type(exc).__name__, exc, exc_info=exc)

    task.add_done_callback(_log_if_died)

    logger.info("[relay-sweeper] starting background task (interval=%ds, idle_timeout=%ds)",
                _RELAY_SWEEP_INTERVAL, _RELAY_IDLE_TIMEOUT_SECONDS)
    relay_task = asyncio.create_task(_sweep_idle_relay_sessions())
    relay_task.add_done_callback(_log_if_died)


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
                uc.start(
                    headless=False,
                    user_data_dir=_PROFILE_DIR,
                    # Matches v1's exact flags (selenium-uc/app.py) -- confirmed
                    # to be the real reason v1 reliably sees a multi-iframe
                    # source's manifest request regardless of which iframe is
                    # actually streaming, while this sidecar's per-target CDP
                    # capture could miss it entirely: with Site Isolation on
                    # (the default), a cross-origin iframe is a separate
                    # out-of-process renderer/CDP target, invisible to the
                    # top-level tab's own network handlers unless the iframe-
                    # candidate loop happens to attach to it before its
                    # request fires. Disabling Site Isolation keeps same-
                    # process (in most cases no longer even a separate CDP
                    # target at all) iframes' network traffic visible on the
                    # top-level tab's session directly -- the same browser-
                    # wide visibility v1's performance-log polling gets for
                    # free, independent of iframe-candidate selection.
                    browser_args=[
                        "--disable-site-isolation-trials",
                        "--disable-features=IsolateOrigins,site-per-process",
                    ],
                ),
                timeout=_STARTUP_TIMEOUT,
            )
            _snapshot_protected_renderer_pids()
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
    # Optional per-site interaction steps (relay_capture.py's action-keyed
    # vocabulary) run right after the initial settle, before the generic
    # iframe-drilling/click-play fallback -- for sources whose real
    # manifest is genuinely visible to normal capture (MATCH_PATTERNS
    # already matches it), but only ever gets requested after a specific
    # click sequence the generic fallback doesn't know how to perform.
    click_sequence: list | None = None


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
                            debug=req.debug, debug_id=debug_id,
                            click_sequence=req.click_sequence,
                            click_sequence_fn=rc.run_click_sequence),
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


class PlayerCandidate(BaseModel):
    path: str
    url: str


class MultiPlayerCaptureRequest(BaseModel):
    wrapper_url: str
    candidates: list[PlayerCandidate]
    per_candidate_timeout: int = 15
    priority: str = "low"


@app.post("/capture/multi-player")
async def capture_multi_player(req: MultiPlayerCaptureRequest):
    """New capability: browser-render every candidate player path for a
    multi-player source in one real page session, not just whichever the
    page loads by default. See network_capture.run_multi_player_capture's
    docstring for the full motivation/mechanism.

    One shared deadline for the WHOLE sweep (not per-candidate) since the
    caller (the native-resolver plugin, via player_health.py) already only
    invokes this as an expensive, rare fallback when pure-HTTP discovery
    found nothing across every path -- bound the total cost explicitly
    rather than letting len(candidates) x per_candidate_timeout run
    unbounded if the caller passes a long candidate list."""
    deadline = len(req.candidates) * req.per_candidate_timeout + 45
    await _slots.acquire(req.priority)
    tab = None
    try:
        browser = await _get_browser()
        tab = await _open_tracked_tab(browser, "about:blank")
        logger.info("Starting multi-player capture: wrapper=%s candidates=%d deadline=%ds",
                    req.wrapper_url, len(req.candidates), deadline)
        results = await asyncio.wait_for(
            nc.run_multi_player_capture(
                tab, req.wrapper_url,
                [{"path": c.path, "url": c.url} for c in req.candidates],
                per_candidate_timeout=req.per_candidate_timeout,
            ),
            timeout=deadline,
        )
        return {"ok": True, "results": results}
    except asyncio.TimeoutError:
        logger.error("Multi-player capture deadline exceeded for %s (%ds)", req.wrapper_url, deadline)
        return {"ok": False, "error": f"Multi-player capture deadline exceeded ({deadline}s)", "results": []}
    except Exception as e:
        logger.exception("Multi-player capture failed for %s", req.wrapper_url)
        return {"ok": False, "error": str(e), "results": []}
    finally:
        await _close_tab_safely(tab)
        _slots.release()


class WatchTestRequest(BaseModel):
    channel_id: str
    duration_seconds: int = 60
    poll_interval_seconds: float = 2.0
    priority: str = "low"


@app.post("/test/watch-channel")
async def test_watch_channel(req: WatchTestRequest):
    """The repeatable browser+diagnostics test harness -- drives
    channelarr's own /diagnostics-wall page exactly the way a human would
    (real Hls.js <video>, real SSE diagnostics overlay) and reports
    time-to-first-frame, stall episodes, client-side video errors, and
    active-source-label changes over a fixed window. See
    network_capture.run_watch_test's docstring for the full motivation --
    real browser playback catches things Jellyfin/server-log checks miss."""
    deadline = req.duration_seconds + 45
    await _slots.acquire(req.priority)
    tab = None
    try:
        browser = await _get_browser()
        tab = await _open_tracked_tab(browser, "about:blank")
        logger.info("Starting watch-test: channel=%s duration=%ds", req.channel_id, req.duration_seconds)
        result = await asyncio.wait_for(
            nc.run_watch_test(tab, _CHANNELARR_BASE_URL, req.channel_id,
                               duration_seconds=req.duration_seconds,
                               poll_interval_seconds=req.poll_interval_seconds),
            timeout=deadline,
        )
        return result
    except asyncio.TimeoutError:
        logger.error("Watch-test deadline exceeded for channel %s (%ds)", req.channel_id, deadline)
        return {"ok": False, "error": f"Watch-test deadline exceeded ({deadline}s)",
                "channel_id": req.channel_id, "samples": []}
    except Exception as e:
        logger.exception("Watch-test failed for channel %s", req.channel_id)
        return {"ok": False, "error": str(e), "channel_id": req.channel_id, "samples": []}
    finally:
        await _close_tab_safely(tab)
        _slots.release()


async def _close_relay_session(session_id: str):
    session = _relay_sessions.pop(session_id, None)
    if not session:
        return
    try:
        await asyncio.wait_for(rc.stop_relay(session["contexts"]), timeout=10.0)
    except Exception:
        pass
    await _close_tab_safely(session["tab"])


class RelayStartRequest(BaseModel):
    url: str
    # Per-site interaction steps to reach real playback -- see
    # relay_capture.py's run_click_sequence docstring for the step shapes.
    # Lives in the CALLER's config (scrapers/_tab_relay_configs.py via
    # core.resolver.segment_sources.get_tab_relay_source_config), not
    # hardcoded here -- this service stays source-name-free. None (not
    # just an empty list) falls back to relay_capture.DEFAULT_CLICK_SEQUENCE.
    click_sequence: list | None = None


@app.post("/relay/start")
async def relay_start(req: RelayStartRequest):
    """Starts a long-lived browser-tab relay session: opens a tab, drives
    it to real playback via click_sequence, and starts capturing the
    decoded <video> output via captureStream()+MediaRecorder. The tab
    stays open and playing until /relay/{session_id}/stop is called or the
    idle sweeper decides nobody's polling it anymore.

    Deliberately NOT gated by the ephemeral-capture PrioritySemaphore
    (_slots) -- that's sized/tuned for short-lived captures that come and
    go in seconds; a relay session occupies a tab for the channel's whole
    runtime, a completely different resource-commitment shape. Gated by
    its own, much smaller cap (_RELAY_MAX_SESSIONS) instead."""
    async with _relay_sessions_lock:
        if len(_relay_sessions) + len(_relay_sessions_starting) >= _RELAY_MAX_SESSIONS:
            return {"ok": False, "error": f"relay session cap reached ({_RELAY_MAX_SESSIONS})"}
        session_id = uuid.uuid4().hex[:12]
        _relay_sessions_starting.add(session_id)

    tab = None
    try:
        browser = await _get_browser()
        tab = await browser.get("about:blank", new_tab=True)
        keep_target_id = _tab_target_id(tab)
        logger.info("Starting relay session %s: %s", session_id, req.url)
        result = await asyncio.wait_for(
            rc.start_relay(tab, browser, keep_target_id, req.url, req.click_sequence),
            timeout=90,
        )
        if not result.get("ok"):
            logger.warning("Relay session %s failed to start: %s (click_log=%s)",
                            session_id, result.get("error"), result.get("click_log"))
            await _close_tab_safely(tab)
            return {"ok": False, "error": result.get("error"), "click_log": result.get("click_log")}

        now = time.time()
        async with _relay_sessions_lock:
            _relay_sessions[session_id] = {"tab": tab, "contexts": result["contexts"],
                                            "started_at": now, "last_poll_at": now}
        logger.info("Relay session %s started (contexts=%d, click_log=%s)",
                    session_id, len(result["contexts"]), result.get("click_log"))
        return {"ok": True, "session_id": session_id, "click_log": result.get("click_log")}
    except Exception as e:
        logger.exception("Relay session %s start failed", session_id)
        await _close_tab_safely(tab)
        return {"ok": False, "error": str(e)}
    finally:
        _relay_sessions_starting.discard(session_id)


@app.get("/relay/{session_id}/chunks")
async def relay_chunks(session_id: str):
    session = _relay_sessions.get(session_id)
    if not session:
        return {"ok": False, "error": "unknown or closed session", "chunks": [], "ended": True}
    session["last_poll_at"] = time.time()
    try:
        result = await asyncio.wait_for(rc.poll_relay(session["contexts"]), timeout=15.0)
    except Exception as e:
        return {"ok": False, "error": str(e), "chunks": [], "ended": False}
    return result


@app.post("/relay/{session_id}/stop")
async def relay_stop(session_id: str):
    await _close_relay_session(session_id)
    return {"ok": True}


class RelayDebugEvalRequest(BaseModel):
    expression: str
    # Index into start_relay's context list -- 0 is always the top-level
    # tab; 1+ are attached cross-origin iframe candidates in the order
    # _find_iframe_candidates returned them. Lets tuning work probe
    # exactly the context that turns out to actually hold the real
    # <video>, not just the top tab.
    context_index: int = 0


@app.post("/relay/{session_id}/debug-eval")
async def relay_debug_eval(session_id: str, req: RelayDebugEvalRequest):
    """Not called by channelarr -- an interactive tool for tuning a new
    site's click_sequence (finding real selectors/coordinates, checking
    <video> state) without a full deploy/test/redeploy cycle per guess."""
    session = _relay_sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="unknown or closed session")
    contexts = session["contexts"]
    if req.context_index < 0 or req.context_index >= len(contexts):
        raise HTTPException(status_code=400,
                            detail=f"context_index out of range (0..{len(contexts)-1})")
    result = await rc._eval_json(contexts[req.context_index], req.expression)
    return {"ok": True, "result": result, "context_count": len(contexts)}


@app.get("/relay/{session_id}/debug-targets")
async def relay_debug_targets(session_id: str):
    """Not called by channelarr -- lists every raw CDP target the browser
    currently knows about (type/url/target_id), including types
    _find_child_frame_contexts/_find_iframe_candidates don't cover at all
    (workers, shared/service workers) -- e.g. a WASM decoder running in a
    dedicated Worker has its own real CDP target regardless of Site
    Isolation, but isn't a frame and won't show up in either of those."""
    session = _relay_sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="unknown or closed session")
    browser = await _get_browser()
    await browser.update_targets()
    targets = [{"type": str(getattr(t.target, "type_", None)),
                "url": str(getattr(t.target, "url", "") or ""),
                "target_id": getattr(t.target, "target_id", None)}
               for t in browser.targets]
    return {"ok": True, "targets": targets}


class SniffNetworkRequest(BaseModel):
    url: str
    click_sequence: list | None = None
    url_substrings: list
    settle_seconds: float = 8.0
    priority: str = "low"


@app.post("/debug/sniff-network")
async def debug_sniff_network(req: SniffNetworkRequest):
    """Not called by channelarr -- a one-off investigation tool. Captures
    every response body whose URL contains any of req.url_substrings,
    regardless of MATCH_PATTERNS/JSON_STREAM_PATTERNS (see
    relay_capture.sniff_network's docstring for why /capture itself can't
    already do this)."""
    await _slots.acquire(req.priority)
    tab = None
    try:
        browser = await _get_browser()
        tab = await _open_tracked_tab(browser, "about:blank")
        keep_target_id = _tab_target_id(tab)
        results = await asyncio.wait_for(
            rc.sniff_network(tab, browser, keep_target_id, req.url,
                              req.click_sequence, req.url_substrings, req.settle_seconds),
            timeout=90,
        )
        return {"ok": True, "results": results}
    except Exception as e:
        logger.exception("sniff-network failed for %s", req.url)
        return {"ok": False, "error": str(e), "results": []}
    finally:
        await _close_tab_safely(tab)
        _slots.release()


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
    relay_status = {"active": len(_relay_sessions), "starting": len(_relay_sessions_starting),
                     "cap": _RELAY_MAX_SESSIONS}
    if _browser is None:
        return {"ready": True, "browser_alive": False, "capture_count": _capture_count,
                "tabs": _slots.status(), "tracked_tabs": _tracked_tabs_status(),
                "relay_sessions": relay_status}
    try:
        await asyncio.wait_for(
            _browser.main_tab.send(uc.cdp.runtime.evaluate(expression="1")),
            timeout=5.0,
        )
        alive = True
    except Exception:
        alive = False
    return {"ready": True, "browser_alive": alive, "capture_count": _capture_count,
            "tabs": _slots.status(), "tracked_tabs": _tracked_tabs_status(),
            "relay_sessions": relay_status}


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
