"""Logo search + apply API. Backed by the SearxNG sidecar."""

import logging
import os

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from web import shared_state
from core import logo_search

router = APIRouter(tags=["logo-search"])
logger = logging.getLogger(__name__)


@router.get("/channels/{channel_id}/logo-search")
def channel_logo_search(channel_id: str, q: str | None = Query(default=None)):
    """Return ranked logo candidates. Falls back to the channel name when q is empty."""
    ch = shared_state.channel_mgr.get_channel(channel_id) if shared_state.channel_mgr else None
    if not ch:
        raise HTTPException(status_code=404, detail="channel not found")
    query = (q or ch.get("name") or "").strip()
    if not query:
        return {"query": "", "candidates": []}
    candidates = logo_search.search(query, max_results=8)
    return {"query": query, "candidates": candidates}


class LogoPick(BaseModel):
    url: str


@router.post("/channels/{channel_id}/logo-pick")
def channel_logo_pick(channel_id: str, body: LogoPick):
    """Download the chosen URL and save as the channel's logo."""
    ch = shared_state.channel_mgr.get_channel(channel_id) if shared_state.channel_mgr else None
    if not ch:
        raise HTTPException(status_code=404, detail="channel not found")
    os.makedirs(shared_state.LOGO_DIR, exist_ok=True)
    dest = os.path.join(shared_state.LOGO_DIR, f"{channel_id}.png")
    ok, msg = logo_search.download_to_logo(body.url, dest)
    if not ok:
        raise HTTPException(status_code=502, detail=msg)
    shared_state.regenerate_m3u()
    logger.info("[LOGO] auto-picked logo for %s: %s", channel_id, body.url[:140])
    return {"status": "ok", "message": msg}
