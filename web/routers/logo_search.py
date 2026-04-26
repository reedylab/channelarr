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
    logger.info("[LOGO] picked logo for %s: %s", channel_id, body.url[:140])
    return {"status": "ok", "message": msg}


@router.post("/channels/{channel_id}/logo-auto")
def channel_logo_auto(channel_id: str, q: str | None = Query(default=None)):
    """One-shot 'generate a logo for me' — runs the search and picks the
    top candidate, no manual confirmation. Returns 200 + skipped=True
    when the top hit is below the auto-pick score threshold (caller can
    then offer the manual picker)."""
    ch = shared_state.channel_mgr.get_channel(channel_id) if shared_state.channel_mgr else None
    if not ch:
        raise HTTPException(status_code=404, detail="channel not found")
    query = (q or ch.get("name") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="empty query")
    os.makedirs(shared_state.LOGO_DIR, exist_ok=True)
    dest = os.path.join(shared_state.LOGO_DIR, f"{channel_id}.png")
    ok, msg = logo_search.auto_pick(channel_id, query, dest)
    if ok:
        shared_state.regenerate_m3u()
        return {"status": "ok", "applied": True, "message": msg}
    return {"status": "ok", "applied": False, "message": msg}


@router.post("/logo-search/backfill")
def logo_backfill():
    """Walk every channel and run auto-pick for any without a logo file.
    Returns a per-channel summary so the caller can show results."""
    if not shared_state.channel_mgr:
        raise HTTPException(status_code=503)
    os.makedirs(shared_state.LOGO_DIR, exist_ok=True)
    results = []
    filled = skipped = failed = 0
    for ch in shared_state.channel_mgr.list_channels():
        cid = ch["id"]
        name = ch.get("name") or ""
        dest = os.path.join(shared_state.LOGO_DIR, f"{cid}.png")
        if os.path.isfile(dest):
            skipped += 1
            continue
        if not name:
            failed += 1
            results.append({"channel_id": cid, "name": name, "applied": False,
                            "message": "no name"})
            continue
        ok, msg = logo_search.auto_pick(cid, name, dest)
        results.append({"channel_id": cid, "name": name, "applied": ok, "message": msg})
        if ok:
            filled += 1
        else:
            failed += 1
    if filled:
        shared_state.regenerate_m3u()
    logger.info("[LOGO] backfill: filled=%d skipped=%d failed=%d", filled, skipped, failed)
    return {"filled": filled, "skipped_existing": skipped, "failed": failed,
            "results": results}
