"""Real-time per-stream diagnostics — live snapshot + per-channel history,
plus SSE push variants for near-real-time updates (no polling)."""

import asyncio
import json
import queue

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from web import shared_state

router = APIRouter()

# SSE heartbeat — how long the generator blocks waiting for a ping before
# re-checking client-disconnect and looping again. Keeps idle connections
# from hanging forever on a dead socket the server hasn't noticed yet.
_SSE_HEARTBEAT_SECONDS = 15


# ── Static-path routes MUST be registered before the /{channel_id} param
# route below — Starlette matches in registration order, so /diagnostics/
# stream would otherwise be swallowed as channel_id="stream". ──

@router.get("/diagnostics/stream")
async def diagnostics_live_stream(request: Request):
    from core.diagnostics import get_live_snapshot, subscribe, unsubscribe

    async def gen():
        q = subscribe(None)
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    await asyncio.to_thread(q.get, True, _SSE_HEARTBEAT_SECONDS)
                except queue.Empty:
                    pass
                statuses = shared_state.streamer_mgr.get_all_status()
                payload = {"streams": get_live_snapshot(statuses)}
                yield f"data: {json.dumps(payload)}\n\n"
        finally:
            unsubscribe(None, q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/diagnostics")
def diagnostics_live():
    from core.diagnostics import get_live_snapshot
    statuses = shared_state.streamer_mgr.get_all_status()
    return {"streams": get_live_snapshot(statuses)}


@router.get("/diagnostics/sources")
def diagnostics_sources():
    """Read-only source/domain health panel -- Pass 1 of the "source
    control panel" design: raw failure signal (which domains, how long,
    how many times in a row, last error) plus the VPN block-rotation
    backoff state (when the next block-triggered rotation is actually
    eligible to fire), all in one place. Deliberately does NOT classify
    a domain as "down" vs "blocked" -- confirmed live this session that
    distinction can't be reliably made from our own network's failures
    alone (a domain that looked identically dead from here was reachable
    fine from a direct, non-VPN connection at the same moment) -- the UI
    shows the raw data, a human makes the call. Must be registered before
    /diagnostics/{channel_id} (below) so "sources" doesn't get swallowed
    as a channel_id path param."""
    from core.block_detector import get_all_domain_status
    from core.vpn_monitor import get_block_rotation_status
    return {
        "domains": sorted(get_all_domain_status(), key=lambda d: d["last_failure_at"], reverse=True),
        "vpn_block_rotation": get_block_rotation_status(),
    }


@router.get("/diagnostics/{channel_id}/stream")
async def diagnostics_channel_stream(channel_id: str, request: Request):
    from core.diagnostics import get_summary, subscribe, unsubscribe

    async def gen():
        q = subscribe(channel_id)
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    await asyncio.to_thread(q.get, True, _SSE_HEARTBEAT_SECONDS)
                except queue.Empty:
                    pass
                payload = {"summary": get_summary(channel_id)}
                yield f"data: {json.dumps(payload)}\n\n"
        finally:
            unsubscribe(channel_id, q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/diagnostics/{channel_id}")
def diagnostics_history(channel_id: str, minutes: int = Query(default=60, ge=1, le=1440)):
    from core.diagnostics import get_history, get_summary
    return {
        "summary": get_summary(channel_id),
        **get_history(channel_id, minutes),
    }


@router.get("/diagnostics/{channel_id}/players")
def diagnostics_player_rankings(channel_id: str):
    """Player-path health rankings for a multi-player source -- empty list
    for any channel that isn't one (no PlayerHealthScore rows tracked for
    it yet). Surfaces player_health.py's scoring model directly in the UI
    so it's visible/debuggable without grepping logs, per the same
    reasoning as the diagnostics dashboard itself."""
    from core.resolver import player_health

    scores = player_health.get_scores(channel_id)
    if not scores:
        return {"channel_id": channel_id, "primary_path": None, "paths": []}

    ch = shared_state.channel_mgr.get_channel(channel_id)
    primary_manifest_id = ch.get("manifest_id") if ch else None
    primary_path = (player_health.get_primary_player_path(primary_manifest_id)
                     if primary_manifest_id else None)

    # Map each tracked path back to a real, switchable manifest_id where one
    # exists -- "(player: X)" tagged fallbacks for intra-source sub-paths,
    # or the manifest_id itself for foreign-fallback-tracked rows (see
    # player_evaluator._foreign_fallback_targets, which uses the raw
    # manifest_id AS the player_path key). Lets the UI offer "make primary"
    # only for candidates that actually correspond to a real, resolvable
    # manifest -- an untested sub-path with no stored fallback yet has
    # nothing to switch to.
    manifest_id_by_path = {}
    for fb in (ch.get("fallback_sources") or []) if ch else []:
        mid = fb.get("manifest_id")
        if not mid:
            continue
        title = fb.get("title") or ""
        path = title.rsplit("(player: ", 1)[1].rstrip(")") if "(player: " in title else mid
        manifest_id_by_path[path] = mid

    paths = [
        {
            "path": path,
            # Opaque display text sourced from whichever plugin/manifest
            # discovered this candidate (see PlayerHealthScore.label) --
            # falls back to the bare path for older/unlabeled rows.
            "label": row.label or path,
            "score": round(player_health.score_of(row), 3),
            "is_primary": path == primary_path,
            "manifest_id": (primary_manifest_id if path == primary_path
                           else manifest_id_by_path.get(path)),
            "success_count": row.success_count,
            "failure_count": row.failure_count,
            "consecutive_failures": row.consecutive_failures,
            "last_ok": row.last_ok,
            "last_probed_at": row.last_probed_at.isoformat() if row.last_probed_at else None,
            "last_latency_ms": row.last_latency_ms,
            "last_error": row.last_error,
        }
        for path, row in scores.items()
    ]
    paths.sort(key=lambda p: p["score"], reverse=True)
    return {"channel_id": channel_id, "primary_path": primary_path, "paths": paths}


@router.post("/diagnostics/{channel_id}/stop")
def diagnostics_stop(channel_id: str):
    """Manual physical stop button. Kills the encoder/poller for this
    channel; each mode's own stop() already clears its on-disk HLS cache
    (see ChannelStream/ProxyStream/RemuxStream/ResolvedChannelStream
    _clean_hls_dir()). The channel stays off until the next playlist
    request boots it again from the schedule/fallback chain — same as any
    other stop_channel() call in this app."""
    from core.diagnostics import record_event
    stopped = shared_state.streamer_mgr.stop_channel(channel_id)
    record_event(channel_id, "manual_stop", {})
    return {"ok": True, "stopped": stopped}


@router.post("/diagnostics/{channel_id}/reload")
def diagnostics_reload(channel_id: str):
    """Stall-recovery button: stop the current encoder/poller (clears the
    HLS cache) and immediately re-resolve + restart from the schedule,
    rather than waiting for the player's next playlist request to notice
    the stream is down and reboot it on its own. Runs the exact same
    _pick_working_manifest() fallback-chain logic a passive restart would."""
    from web.routers.hls import _start_from_schedule
    from core.diagnostics import record_event
    shared_state.streamer_mgr.stop_channel(channel_id)
    ok, msg = _start_from_schedule(channel_id)
    # Recorded after the restart, not before — a fresh start already resets
    # this channel's diagnostics history (clear_channel(), called from every
    # start_* path), so logging it first would just get wiped immediately.
    record_event(channel_id, "manual_reload", {"restarted": ok})
    return {"ok": ok, "message": msg}


@router.post("/diagnostics/{channel_id}/simulate-stall")
def diagnostics_simulate_stall(channel_id: str, count: int = 2):
    """TEST-ONLY: injects real source_stall events into the same live
    diagnostics store the switch watchdog (core/resolver/player_evaluator.
    py's _channel_is_struggling) reads, without needing to actually break
    the real upstream connection — attempting that externally (blocking
    DNS for the CDN host) had zero effect on an already-open keep-alive
    connection (2026-09-13), and there was no other side-channel into the
    live process's in-memory diagnostics state from outside it.

    This lets the ACTUAL watchdog code run unmodified on its normal ~10s
    cycle against a genuine (if synthetic-origin) signal — timing from this
    call to the resulting "promoted ... restarting now" log line is a real
    measurement of end-to-end switchover latency, not a guess. Causes a
    REAL promotion + hot-restart of the channel if it has a scored
    candidate that beats the current primary — same as a genuine stall
    would, on purpose.

    `count` defaults to 2 — _STRUGGLING_SOURCE_STALLS_5M's threshold — so
    the default call is exactly enough to cross it, no more.
    """
    from core.diagnostics import record_event, incr_counter
    for _ in range(max(1, count)):
        record_event(channel_id, "source_stall", {"gap_seconds": 999, "simulated": True})
        incr_counter(channel_id, "source_stalls")
    return {"ok": True, "injected": count}


@router.post("/diagnostics/{channel_id}/client-event")
async def diagnostics_client_event(channel_id: str, request: Request):
    """The video element itself reporting a stall or a live-catch-up seek —
    the most direct "is the viewer actually seeing a problem" signal there
    is, as opposed to every other metric here inferring it server-side. See
    core/diagnostics.py's CLIENT_EVENT_TYPES for the allowlist."""
    from core.diagnostics import record_client_event
    body = await request.json()
    event_type = body.get("event_type", "")
    detail = body.get("detail") or {}
    ok = record_client_event(channel_id, event_type, detail)
    if not ok:
        return JSONResponse(status_code=400, content={"error": f"unknown event_type {event_type!r}"})
    return {"ok": True}
