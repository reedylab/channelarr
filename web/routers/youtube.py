"""YouTube browse endpoint for channel editor."""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from core.youtube import yt_browse

router = APIRouter()


@router.post("/youtube/browse")
async def api_yt_browse(request: Request):
    data = await request.json()
    url = data.get("url", "").strip()
    if not url:
        return JSONResponse({"error": "URL required"}, status_code=400)
    try:
        videos = yt_browse(url)
        return {"videos": videos}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
