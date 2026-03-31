"""Bump clip management endpoints."""

import os
import subprocess as sp

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import JSONResponse

from web import shared_state
from core.config import get_setting

router = APIRouter()


@router.get("/bumps")
def api_bumps():
    mgr = shared_state.bump_mgr
    all_clips = mgr.get_all()
    detail = {}
    for folder, paths in sorted(all_clips.items()):
        detail[folder] = [{"name": os.path.basename(p), "path": p} for p in paths]
    return {
        "folders": {k: len(v) for k, v in sorted(all_clips.items())},
        "total": sum(len(v) for v in all_clips.values()),
        "clips": detail,
    }


@router.post("/bumps/scan")
def api_bumps_scan():
    shared_state.bump_mgr.scan()
    return shared_state.bump_mgr.summary()


@router.delete("/bumps/clip")
async def api_bumps_delete(request: Request):
    data = await request.json()
    path = data.get("path", "").strip()
    if not path:
        return JSONResponse({"error": "Path required"}, status_code=400)
    ok = shared_state.bump_mgr.delete(path)
    if not ok:
        return JSONResponse({"error": "File not found or outside bumps directory"}, status_code=404)
    return {"status": "ok", "message": "Clip deleted"}


@router.post("/bumps/download")
async def api_bumps_download(request: Request):
    data = await request.json()
    url = data.get("url", "").strip()
    folder = data.get("folder", "").strip()
    resolution = data.get("resolution", "1080").strip()
    if not url:
        return JSONResponse({"error": "URL required"}, status_code=400)
    if not folder:
        return JSONResponse({"error": "Folder required"}, status_code=400)
    if resolution not in ("480", "720", "1080"):
        resolution = "1080"
    shared_state.bump_mgr.download_url(url, folder, resolution=resolution)
    return {"status": "ok", "message": f"Downloading to {folder}/ (max {resolution}p)... check logs for progress."}


@router.get("/bumps/thumbnail")
def api_bump_thumbnail(path: str = Query(default="")):
    path = path.strip()
    if not path or not os.path.isfile(path):
        return Response(content="", status_code=404)
    bumps_path = get_setting("BUMPS_PATH", "/bumps")
    if not os.path.normpath(path).startswith(os.path.normpath(bumps_path)):
        return JSONResponse({"error": "Invalid path"}, status_code=403)
    try:
        result = sp.run(
            ["ffmpeg", "-y", "-loglevel", "quiet", "-ss", "1", "-i", path,
             "-vframes", "1", "-vf", "scale=160:90:force_original_aspect_ratio=decrease,pad=160:90:(ow-iw)/2:(oh-ih)/2",
             "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"],
            capture_output=True, timeout=10,
        )
        if result.returncode != 0 or not result.stdout:
            return Response(content="", status_code=404)
        return Response(content=result.stdout, media_type="image/jpeg")
    except Exception:
        return Response(content="", status_code=404)
