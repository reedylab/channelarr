"""EPG, schedule, and export endpoints."""

import os

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse, JSONResponse

from web import shared_state
from core.config import get_setting
from core.channels import materialize_all_channels, get_now_playing, placeholder_entries_in_window
from core.xmltv import _iterate_schedule_window, _merge_bump_gaps

router = APIRouter()


@router.get("/epg/now")
def api_epg_now():
    channels = shared_state.channel_mgr.list_channels()
    result = {}
    for ch in channels:
        np = get_now_playing(ch)
        if np:
            result[ch["id"]] = {
                "channel_name": ch["name"],
                "now": {
                    "title": np["entry"].get("title", ""),
                    "desc": np["entry"].get("desc", ""),
                    "type": np["entry"].get("type", ""),
                    "start": np["entry"].get("start", ""),
                    "stop": np["entry"].get("stop", ""),
                    "duration": np["entry"].get("duration", 0),
                    "progress": np["progress"],
                    "seek_offset": np["seek_offset"],
                },
                "next": None,
            }
            if "next" in np:
                result[ch["id"]]["next"] = {
                    "title": np["next"].get("title", ""),
                    "desc": np["next"].get("desc", ""),
                    "type": np["next"].get("type", ""),
                    "start": np["next"].get("start", ""),
                    "stop": np["next"].get("stop", ""),
                    "duration": np["next"].get("duration", 0),
                }
    return result


@router.get("/epg/guide")
def api_epg_guide(hours: int = Query(default=6)):
    from datetime import datetime, timedelta, timezone

    hours = min(hours, 48)
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=1)
    horizon = now + timedelta(hours=hours)

    channels = shared_state.channel_mgr.list_channels()
    guide = []
    for ch in channels:
        is_resolved = ch.get("type") == "resolved"
        schedule = ch.get("materialized_schedule", [])
        epoch_str = ch.get("schedule_epoch")
        cycle_dur = ch.get("schedule_cycle_duration", 0)

        entries = []
        if not is_resolved and schedule and epoch_str and cycle_dur > 0:
            merged = _merge_bump_gaps(
                _iterate_schedule_window(schedule, epoch_str, cycle_dur,
                                          ch.get("loop", True), window_start, horizon)
            )
            for entry in merged:
                ep = {
                    "title": entry.get("title", ""),
                    "desc": entry.get("desc", ""),
                    "type": entry.get("type", ""),
                    "path": entry.get("path", ""),
                    "start": entry["start"].isoformat() if hasattr(entry["start"], "isoformat") else entry["start"],
                    "stop": entry["stop"].isoformat() if hasattr(entry["stop"], "isoformat") else entry["stop"],
                    "duration": entry.get("duration", 0),
                }
                if entry.get("thumbnail"):
                    ep["thumbnail"] = entry["thumbnail"]
                entries.append(ep)
        else:
            # Resolved channels (always live) and empty scheduled channels
            # both fall back to placeholder blocks. Block boundaries align
            # to :00/:30 so they match the channel-tile and EPG export.
            entries = placeholder_entries_in_window(
                ch["name"], window_start, horizon, is_live=is_resolved
            )

        guide.append({
            "id": ch["id"],
            "name": ch["name"],
            "entries": entries,
        })

    window_start = now - timedelta(hours=1)
    window_end = now + timedelta(hours=hours)
    return {
        "now": now.isoformat(),
        "start": window_start.isoformat(),
        "end": window_end.isoformat(),
        "hours": hours,
        "channels": guide,
    }


@router.post("/schedule/refresh")
def api_schedule_refresh():
    shared_state.regenerate_m3u()
    return {"status": "ok", "message": "M3U and EPG refreshed."}


@router.post("/schedule/regenerate")
def api_schedule_regenerate():
    shared_state.streamer_mgr.stop_all()
    materialize_all_channels(shared_state.channel_mgr, shared_state.bump_mgr, shared_state.media_lib)
    shared_state.regenerate_m3u()
    channels = shared_state.channel_mgr.list_channels()
    return {
        "status": "ok",
        "message": f"Regenerated schedules for {len(channels)} channels.",
        "channels": len(channels),
    }


@router.get("/export/m3u")
def api_export_m3u():
    m3u_path = get_setting("M3U_OUTPUT_PATH", "/m3u")
    filepath = os.path.join(m3u_path, "channelarr.m3u")
    if not os.path.isfile(filepath):
        shared_state.regenerate_m3u()
    if not os.path.isfile(filepath):
        return JSONResponse({"error": "M3U not yet generated"}, status_code=404)
    return FileResponse(filepath, media_type="application/octet-stream", filename="channelarr.m3u")


@router.get("/export/xmltv")
def api_export_xmltv():
    m3u_path = get_setting("M3U_OUTPUT_PATH", "/m3u")
    filepath = os.path.join(m3u_path, "channelarr.xml")
    if not os.path.isfile(filepath):
        shared_state.regenerate_m3u()
    if not os.path.isfile(filepath):
        return JSONResponse({"error": "XMLTV not yet generated"}, status_code=404)
    return FileResponse(filepath, media_type="application/octet-stream", filename="channelarr.xml")
