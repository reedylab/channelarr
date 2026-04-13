"""Channel CRUD + logo endpoints."""

import logging
import os

from fastapi import APIRouter, File, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from web import shared_state
from core.channels import materialize_schedule, get_now_playing, current_placeholder_block

router = APIRouter()


def _enrich(ch: dict) -> dict:
    """Attach runtime fields (stream_url, logo_url, status, now_playing) to a
    channel dict before returning it from the API. Type-aware: scheduled
    channels get streamer status + now_playing; resolved channels get the
    /live-resolved/ URL pattern and skip the streamer lookup."""
    cid = ch["id"]
    logo_path = os.path.join(shared_state.LOGO_DIR, f"{cid}.png")
    ch["logo_url"] = f"/api/logo/{cid}" if os.path.isfile(logo_path) else None

    if ch.get("type") == "resolved":
        mid = ch.get("manifest_id")
        if ch.get("transcode_mediated"):
            # Transcode-mediated resolved channels use the same /live/{id}
            # endpoint as scheduled channels — output goes through the
            # unified HLS pipeline so the URL pattern is identical.
            ch["stream_url"] = f"/live/{cid}/stream.m3u8"
            ch["stream_status"] = shared_state.streamer_mgr.get_status(cid)
        else:
            ch["stream_url"] = f"/live-resolved/{mid}.m3u8" if mid else None
            ch["stream_status"] = {"running": False, "uptime": 0}
        ch["now_playing"] = current_placeholder_block(ch.get("name", "Live"))
    else:
        ch["stream_url"] = f"/live/{cid}/stream.m3u8"
        ch["stream_status"] = shared_state.streamer_mgr.get_status(cid)
        ch["now_playing"] = get_now_playing(ch)
    return ch


@router.get("/status")
def api_status():
    channels = shared_state.channel_mgr.list_channels()
    running = shared_state.streamer_mgr.running_count()
    return {
        "channels_total": len(channels),
        "channels_streaming": running,
    }


@router.get("/channels/shuffle-modes")
def api_shuffle_modes():
    """Describe available shuffle modes and example payloads for AI agents."""
    return {
        "modes": {
            "none": {
                "description": "No shuffling, items play in listed order",
            },
            "random": {
                "description": "Fully random shuffle of all items",
            },
            "round_robin": {
                "description": "Interleave episodes across shows in alternating order (A1,B1,A2,B2,...). Scales to any number of shows.",
            },
            "weighted": {
                "description": "Random shuffle weighted by per-show percentages",
                "parameters": {
                    "weights": "Object mapping show paths to integer percentages. Must sum to 100.",
                },
            },
        },
        "usage": {
            "field": "shuffle_config",
            "location": "Channel create/update payload body",
            "schema": {
                "mode": "string (required) — one of: none, random, round_robin, weighted",
                "weights": "object (required for weighted mode) — {show_path: percentage}",
            },
        },
        "example_payloads": {
            "round_robin": {
                "name": "Alternating Shows",
                "items": [
                    {"type": "show", "path": "/media/tv/ShowA", "title": "Show A"},
                    {"type": "show", "path": "/media/tv/ShowB", "title": "Show B"},
                ],
                "shuffle_config": {"mode": "round_robin"},
                "loop": True,
            },
            "weighted": {
                "name": "Mostly Show A",
                "items": [
                    {"type": "show", "path": "/media/tv/ShowA", "title": "Show A"},
                    {"type": "show", "path": "/media/tv/ShowB", "title": "Show B"},
                ],
                "shuffle_config": {
                    "mode": "weighted",
                    "weights": {"/media/tv/ShowA": 75, "/media/tv/ShowB": 25},
                },
                "loop": True,
            },
            "random": {
                "name": "Random Mix",
                "items": [
                    {"type": "show", "path": "/media/tv/ShowA", "title": "Show A"},
                    {"type": "show", "path": "/media/tv/ShowB", "title": "Show B"},
                ],
                "shuffle_config": {"mode": "random"},
                "loop": True,
            },
        },
    }


@router.get("/channels")
def api_list_channels():
    channels = shared_state.channel_mgr.list_channels()
    return [_enrich(ch) for ch in channels]


@router.post("/channels")
async def api_create_channel(request: Request):
    """Create a channel.

    Two channel types are supported:
      - "scheduled" (default): local + YouTube items, with bumps/shuffle/loop
      - "resolved": references a captured manifest from the library. Body must
        include `manifest_id`. Items/bumps/shuffle are ignored.
    """
    data = await request.json()
    ctype = data.get("type", "scheduled")

    if ctype == "resolved":
        manifest_id = data.get("manifest_id")
        if not manifest_id:
            return JSONResponse({"error": "manifest_id required for resolved channel"}, status_code=400)
        ch = shared_state.channel_mgr.create_resolved_channel(
            manifest_id, data.get("name"),
            tags=data.get("tags"),
            event_start=data.get("event_start"),
            event_end=data.get("event_end"),
        )
        if not ch:
            return JSONResponse({"error": "manifest not found"}, status_code=404)
        shared_state.regenerate_m3u()
        return JSONResponse(_enrich(ch), status_code=201)

    # Default: scheduled channel
    ch = shared_state.channel_mgr.create_channel(data)
    materialize_schedule(ch, shared_state.bump_mgr, media_library=shared_state.media_lib)
    shared_state.channel_mgr.save_channel(ch)
    shared_state.regenerate_m3u()
    return JSONResponse(_enrich(ch), status_code=201)


@router.get("/channels/{channel_id}")
def api_get_channel(channel_id: str):
    ch = shared_state.channel_mgr.get_channel(channel_id)
    if not ch:
        return JSONResponse({"error": "Not found"}, status_code=404)
    return _enrich(ch)


@router.put("/channels/{channel_id}")
async def api_update_channel(channel_id: str, request: Request):
    data = await request.json()
    existing = shared_state.channel_mgr.get_channel(channel_id)
    if not existing:
        return JSONResponse({"error": "Not found"}, status_code=404)

    ch = shared_state.channel_mgr.update_channel(channel_id, data)
    if not ch:
        return JSONResponse({"error": "Not found"}, status_code=404)

    if existing.get("type") != "resolved":
        # Re-materialize schedule on scheduled channels (resolved have none)
        materialize_schedule(ch, shared_state.bump_mgr, media_library=shared_state.media_lib)
        shared_state.channel_mgr.save_channel(ch)
        shared_state.streamer_mgr.stop_channel(channel_id)
    else:
        # Resolved channels: any settings change (transcode toggle, bump
        # folders, show_next) needs the running stream to restart so the
        # orchestrator picks up the new config. The next /live request will
        # boot a fresh stream.
        shared_state.streamer_mgr.stop_channel(channel_id)

    shared_state.regenerate_m3u()
    return _enrich(ch)


@router.delete("/channels/{channel_id}")
def api_delete_channel(channel_id: str):
    existing = shared_state.channel_mgr.get_channel(channel_id)
    if existing and existing.get("type") != "resolved":
        shared_state.streamer_mgr.stop_channel(channel_id)
    ok = shared_state.channel_mgr.delete_channel(channel_id)
    if not ok:
        return JSONResponse({"error": "Not found"}, status_code=404)
    shared_state.regenerate_m3u()
    return {"status": "deleted"}


@router.get("/channel-tags")
def api_channel_tags():
    """Return all known tags (in-use + configured) and tag config."""
    from core.config import get_tag_config
    tag_config = get_tag_config()
    # Collect tags in use across all channels
    in_use = set()
    for ch in shared_state.channel_mgr.list_channels():
        for tag in (ch.get("tags") or []):
            in_use.add(tag)
    # Union with configured tags
    all_tags = sorted(in_use | set(tag_config.keys()))
    return {"tags": all_tags, "config": tag_config}


@router.get("/logo/{channel_id}")
def api_get_logo(channel_id: str):
    logo_path = os.path.join(shared_state.LOGO_DIR, f"{channel_id}.png")
    if not os.path.isfile(logo_path):
        return JSONResponse({"error": "No logo"}, status_code=404)
    return FileResponse(logo_path, media_type="image/png")


@router.post("/logo/{channel_id}")
async def api_upload_logo(channel_id: str, file: UploadFile = File(...)):
    data = await file.read()
    if not data:
        return JSONResponse({"error": "Empty file"}, status_code=400)
    if data[:4] == b"\x89PNG":
        pass
    elif data[:2] in (b"\xff\xd8",):
        pass
    else:
        return JSONResponse({"error": "Only PNG or JPEG allowed"}, status_code=400)
    os.makedirs(shared_state.LOGO_DIR, exist_ok=True)
    logo_path = os.path.join(shared_state.LOGO_DIR, f"{channel_id}.png")
    with open(logo_path, "wb") as out:
        out.write(data)
    shared_state.regenerate_m3u()
    logging.info("[LOGO] Uploaded logo for %s", channel_id)
    return {"status": "ok"}


@router.delete("/logo/{channel_id}")
def api_delete_logo(channel_id: str):
    logo_path = os.path.join(shared_state.LOGO_DIR, f"{channel_id}.png")
    if os.path.isfile(logo_path):
        os.remove(logo_path)
        shared_state.regenerate_m3u()
        logging.info("[LOGO] Deleted logo for %s", channel_id)
        return {"status": "deleted"}
    return JSONResponse({"error": "No logo"}, status_code=404)
