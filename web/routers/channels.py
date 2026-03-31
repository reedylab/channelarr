"""Channel CRUD + logo endpoints."""

import logging
import os

from fastapi import APIRouter, File, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from web import shared_state
from core.channels import materialize_schedule, get_now_playing

router = APIRouter()


@router.get("/status")
def api_status():
    channels = shared_state.channel_mgr.list_channels()
    running = shared_state.streamer_mgr.running_count()
    return {
        "channels_total": len(channels),
        "channels_streaming": running,
    }


@router.get("/channels")
def api_list_channels():
    channels = shared_state.channel_mgr.list_channels()
    statuses = shared_state.streamer_mgr.get_all_status()
    for ch in channels:
        st = statuses.get(ch["id"], {"running": False, "uptime": 0})
        ch["stream_status"] = st
        now_playing = get_now_playing(ch)
        ch["now_playing"] = now_playing
    return channels


@router.post("/channels")
async def api_create_channel(request: Request):
    data = await request.json()
    ch = shared_state.channel_mgr.create_channel(data)
    materialize_schedule(ch, shared_state.bump_mgr, media_library=shared_state.media_lib)
    shared_state.channel_mgr.save_channel(ch)
    shared_state.regenerate_m3u()
    return JSONResponse(ch, status_code=201)


@router.get("/channels/{channel_id}")
def api_get_channel(channel_id: str):
    ch = shared_state.channel_mgr.get_channel(channel_id)
    if not ch:
        return JSONResponse({"error": "Not found"}, status_code=404)
    ch["stream_status"] = shared_state.streamer_mgr.get_status(channel_id)
    ch["now_playing"] = get_now_playing(ch)
    return ch


@router.put("/channels/{channel_id}")
async def api_update_channel(channel_id: str, request: Request):
    data = await request.json()
    ch = shared_state.channel_mgr.update_channel(channel_id, data)
    if not ch:
        return JSONResponse({"error": "Not found"}, status_code=404)
    materialize_schedule(ch, shared_state.bump_mgr, media_library=shared_state.media_lib)
    shared_state.channel_mgr.save_channel(ch)
    shared_state.streamer_mgr.stop_channel(channel_id)
    shared_state.regenerate_m3u()
    return ch


@router.delete("/channels/{channel_id}")
def api_delete_channel(channel_id: str):
    shared_state.streamer_mgr.stop_channel(channel_id)
    ok = shared_state.channel_mgr.delete_channel(channel_id)
    if not ok:
        return JSONResponse({"error": "Not found"}, status_code=404)
    shared_state.regenerate_m3u()
    return {"status": "deleted"}


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
