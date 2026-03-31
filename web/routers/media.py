"""Media library endpoints."""

import logging
import os

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse, JSONResponse

from web import shared_state
from core.config import get_setting

router = APIRouter()


@router.get("/media/movies")
def api_movies():
    return shared_state.media_lib.get_movies()


@router.get("/media/tv")
def api_tv_shows():
    return shared_state.media_lib.get_shows()


@router.get("/media/tv/episodes")
def api_tv_episodes(path: str = Query(default="")):
    path = path.strip()
    if not path:
        return JSONResponse({"error": "path is required"}, status_code=400)
    media_path = get_setting("MEDIA_PATH", "/media")
    norm_path = os.path.normpath(path)
    norm_media = os.path.normpath(media_path)
    if not norm_path.startswith(norm_media + os.sep) and norm_path != norm_media:
        logging.warning("[API] Episodes path rejected: %s not under %s", norm_path, norm_media)
        return JSONResponse({"error": "path must be under MEDIA_PATH"}, status_code=403)
    if not os.path.isdir(path):
        logging.warning("[API] Episodes path not a directory: %s", path)
        return JSONResponse({"error": "directory not found"}, status_code=404)
    return shared_state.media_lib.get_episodes(path)


@router.get("/media/poster")
def api_media_poster(path: str = Query(default="")):
    from core.nfo import find_poster
    path = path.strip()
    if not path:
        return JSONResponse({"error": "path required"}, status_code=400)
    poster = find_poster(path)
    if not poster:
        return JSONResponse("", status_code=404)
    mime = "image/jpeg" if poster.lower().endswith(".jpg") else "image/png"
    return FileResponse(poster, media_type=mime)
