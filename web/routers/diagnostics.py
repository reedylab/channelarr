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
