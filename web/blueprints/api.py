"""REST API + SETTINGS_SCHEMA for Channelarr."""

from flask import Blueprint, current_app, jsonify, request, send_file
from pathlib import Path
import collections
import logging
import os
import threading
import time

import psutil

LOGO_DIR = os.getenv("LOGO_DIR", "/app/data/logos")

from core.config import get_setting, get_all_settings, save_settings
from core.channels import (
    generate_schedule, materialize_schedule, materialize_all_channels,
    find_schedule_position, get_now_playing,
)
from core.xmltv import generate_channelarr_xmltv

api_bp = Blueprint("api", __name__)

# ----------------------- system stats collector -----------------------
_cpu_history = collections.deque(maxlen=2880)    # 24h at 30s intervals
_ram_history = collections.deque(maxlen=2880)
_stats_timestamps = collections.deque(maxlen=2880)


def _stats_collector():
    """Background thread: sample CPU + RAM every 30 seconds."""
    while True:
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        _cpu_history.append(cpu)
        _ram_history.append(mem.percent)
        _stats_timestamps.append(time.time())
        time.sleep(29)


_stats_thread = threading.Thread(target=_stats_collector, daemon=True)
_stats_thread.start()

# ----------------------- settings schema -----------------------
SETTINGS_SCHEMA = {
    "paths": {
        "label": "Paths",
        "fields": {
            "MEDIA_PATH": {"label": "Media Path", "type": "text", "placeholder": "/media"},
            "BUMPS_PATH": {"label": "Bumps Path", "type": "text", "placeholder": "/bumps"},
            "HLS_OUTPUT_PATH": {"label": "HLS Output Path", "type": "text", "placeholder": "/app/data/hls"},
            "M3U_OUTPUT_PATH": {"label": "M3U Output Path", "type": "text", "placeholder": "/m3u"},
        },
    },
    "streaming": {
        "label": "Streaming",
        "fields": {
            "HLS_TIME": {"label": "HLS Segment Duration (s)", "type": "text", "placeholder": "6"},
            "HLS_LIST_SIZE": {"label": "HLS Playlist Window", "type": "text", "placeholder": "10"},
            "FFMPEG_LOGLEVEL": {
                "label": "FFmpeg Log Level", "type": "select",
                "options": [
                    {"value": "warning", "label": "Warning"},
                    {"value": "info", "label": "Info"},
                    {"value": "verbose", "label": "Verbose"},
                    {"value": "quiet", "label": "Quiet"},
                ],
            },
            "BASE_URL": {"label": "Base URL", "type": "text", "placeholder": "http://192.168.20.34:5045"},
        },
    },
    "encoding": {
        "label": "Encoding",
        "fields": {
            "VIDEO_PRESET": {
                "label": "x264 Preset", "type": "select",
                "options": [
                    {"value": "ultrafast", "label": "Ultrafast"},
                    {"value": "superfast", "label": "Superfast"},
                    {"value": "veryfast", "label": "Veryfast"},
                    {"value": "faster", "label": "Faster"},
                    {"value": "fast", "label": "Fast"},
                    {"value": "medium", "label": "Medium"},
                    {"value": "slow", "label": "Slow"},
                ],
            },
            "VIDEO_CRF": {
                "label": "Video CRF", "type": "select",
                "options": [
                    {"value": "", "label": "Codec Default"},
                    {"value": "18", "label": "18 — Visually Lossless"},
                    {"value": "20", "label": "20"},
                    {"value": "22", "label": "22"},
                    {"value": "23", "label": "23 — x264 Default"},
                    {"value": "25", "label": "25"},
                    {"value": "28", "label": "28"},
                ],
            },
            "FFMPEG_THREADS": {
                "label": "FFmpeg Threads", "type": "select",
                "options": [
                    {"value": "1", "label": "1"},
                    {"value": "2", "label": "2"},
                    {"value": "4", "label": "4"},
                    {"value": "8", "label": "8"},
                    {"value": "0", "label": "Auto (all cores)"},
                ],
            },
            "X264_THREADS": {
                "label": "x264 Threads", "type": "select",
                "options": [
                    {"value": "1", "label": "1"},
                    {"value": "2", "label": "2"},
                    {"value": "4", "label": "4"},
                    {"value": "8", "label": "8"},
                    {"value": "0", "label": "Auto (all cores)"},
                ],
            },
            "AUDIO_BITRATE": {
                "label": "Audio Bitrate", "type": "select",
                "options": [
                    {"value": "128k", "label": "128k"},
                    {"value": "192k", "label": "192k"},
                    {"value": "256k", "label": "256k"},
                    {"value": "320k", "label": "320k"},
                    {"value": "448k", "label": "448k"},
                ],
            },
        },
    },
}


# ----------------------- helpers -----------------------
def _streamer():
    return current_app.config["STREAMER_MGR"]

def _channels():
    return current_app.config["CHANNEL_MGR"]

def _bumps():
    return current_app.config["BUMP_MGR"]

def _media():
    return current_app.config["MEDIA_LIB"]


def _regenerate_m3u():
    """Regenerate the M3U playlist file and XMLTV EPG."""
    m3u_path = get_setting("M3U_OUTPUT_PATH", "/m3u")
    base_url = get_setting("BASE_URL", "http://192.168.20.34:5045")
    channels = _channels().list_channels()

    os.makedirs(m3u_path, exist_ok=True)
    out = os.path.join(m3u_path, "channelarr.m3u")

    with open(out, "w") as f:
        f.write("#EXTM3U\n")
        for i, ch in enumerate(channels):
            cid = ch["id"]
            name = ch["name"]
            chno = i + 1
            logo_path = os.path.join(LOGO_DIR, f"{cid}.png")
            logo_tag = ""
            if os.path.isfile(logo_path):
                logo_tag = f' tvg-logo="{base_url}/api/logo/{cid}"'
            f.write(f'#EXTINF:-1 tvg-id="{cid}" tvg-chno="{chno}" tvg-name="{name}"{logo_tag} group-title="Channelarr",{name}\n')
            f.write(f"{base_url}/live/{cid}/stream.m3u8\n")

    logging.info("[M3U] Regenerated %s with %d channels", out, len(channels))

    # Generate XMLTV EPG alongside M3U
    xmltv_out = os.path.join(m3u_path, "channelarr.xml")
    generate_channelarr_xmltv(channels, xmltv_out, base_url)


# ----------------------- status -----------------------
@api_bp.get("/status")
def api_status():
    channels = _channels().list_channels()
    running = _streamer().running_count()
    return jsonify({
        "channels_total": len(channels),
        "channels_streaming": running,
    })


# ----------------------- channels -----------------------
@api_bp.get("/channels")
def api_list_channels():
    channels = _channels().list_channels()
    statuses = _streamer().get_all_status()
    for ch in channels:
        st = statuses.get(ch["id"], {"running": False, "uptime": 0})
        ch["stream_status"] = st
        # Include now-playing from schedule
        now_playing = get_now_playing(ch)
        ch["now_playing"] = now_playing
    return jsonify(channels)


@api_bp.post("/channels")
def api_create_channel():
    data = request.get_json() or {}
    ch = _channels().create_channel(data)
    # Materialize schedule for the new channel
    materialize_schedule(ch, _bumps(), media_library=_media())
    _channels().save_channel(ch)
    _regenerate_m3u()
    return jsonify(ch), 201


@api_bp.get("/channels/<channel_id>")
def api_get_channel(channel_id):
    ch = _channels().get_channel(channel_id)
    if not ch:
        return jsonify({"error": "Not found"}), 404
    ch["stream_status"] = _streamer().get_status(channel_id)
    ch["now_playing"] = get_now_playing(ch)
    return jsonify(ch)


@api_bp.put("/channels/<channel_id>")
def api_update_channel(channel_id):
    data = request.get_json() or {}
    ch = _channels().update_channel(channel_id, data)
    if not ch:
        return jsonify({"error": "Not found"}), 404
    # Re-materialize schedule after content changes
    materialize_schedule(ch, _bumps(), media_library=_media())
    _channels().save_channel(ch)
    # Stop running stream so it picks up new schedule on next request
    _streamer().stop_channel(channel_id)
    _regenerate_m3u()
    return jsonify(ch)


@api_bp.delete("/channels/<channel_id>")
def api_delete_channel(channel_id):
    _streamer().stop_channel(channel_id)
    ok = _channels().delete_channel(channel_id)
    if not ok:
        return jsonify({"error": "Not found"}), 404
    _regenerate_m3u()
    return jsonify({"status": "deleted"})


# ----------------------- EPG / Schedule -----------------------
@api_bp.get("/epg/now")
def api_epg_now():
    """Return what's currently playing on all channels."""
    channels = _channels().list_channels()
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
    return jsonify(result)


@api_bp.get("/epg/guide")
def api_epg_guide():
    """Return schedule entries for all channels for the next N hours.

    Query params: hours (default 6)
    """
    from datetime import datetime, timedelta, timezone
    from core.xmltv import _iterate_schedule_window, _merge_bump_gaps

    hours = int(request.args.get("hours", "6"))
    hours = min(hours, 48)
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=1)
    horizon = now + timedelta(hours=hours)

    channels = _channels().list_channels()
    guide = []
    for ch in channels:
        schedule = ch.get("materialized_schedule", [])
        epoch_str = ch.get("schedule_epoch")
        cycle_dur = ch.get("schedule_cycle_duration", 0)

        entries = []
        if schedule and epoch_str and cycle_dur > 0:
            merged = _merge_bump_gaps(
                _iterate_schedule_window(schedule, epoch_str, cycle_dur,
                                          ch.get("loop", True), window_start, horizon)
            )
            for entry in merged:
                entries.append({
                    "title": entry.get("title", ""),
                    "desc": entry.get("desc", ""),
                    "type": entry.get("type", ""),
                    "path": entry.get("path", ""),
                    "start": entry["start"].isoformat() if hasattr(entry["start"], "isoformat") else entry["start"],
                    "stop": entry["stop"].isoformat() if hasattr(entry["stop"], "isoformat") else entry["stop"],
                    "duration": entry.get("duration", 0),
                })

        guide.append({
            "id": ch["id"],
            "name": ch["name"],
            "entries": entries,
        })

    # Start window 1 hour ago so user can see recent past
    window_start = now - timedelta(hours=1)
    window_end = now + timedelta(hours=hours)
    return jsonify({
        "now": now.isoformat(),
        "start": window_start.isoformat(),
        "end": window_end.isoformat(),
        "hours": hours,
        "channels": guide,
    })


@api_bp.post("/schedule/refresh")
def api_schedule_refresh():
    """Soft refresh: regenerate M3U + XMLTV from existing schedules."""
    _regenerate_m3u()
    return jsonify({"status": "ok", "message": "M3U and EPG refreshed."})


@api_bp.post("/schedule/regenerate")
def api_schedule_regenerate():
    """Hard regenerate: re-materialize all schedules, then regen M3U + XMLTV.

    Stops any running streams (they restart on next client request with the new schedule).
    """
    _streamer().stop_all()
    materialize_all_channels(_channels(), _bumps(), _media())
    _regenerate_m3u()
    channels = _channels().list_channels()
    return jsonify({
        "status": "ok",
        "message": f"Regenerated schedules for {len(channels)} channels.",
        "channels": len(channels),
    })


# ----------------------- export -----------------------
@api_bp.get("/export/m3u")
def api_export_m3u():
    """Serve the M3U playlist file for download."""
    m3u_path = get_setting("M3U_OUTPUT_PATH", "/m3u")
    filepath = os.path.join(m3u_path, "channelarr.m3u")
    if not os.path.isfile(filepath):
        _regenerate_m3u()
    if not os.path.isfile(filepath):
        return jsonify({"error": "M3U not yet generated"}), 404
    return send_file(filepath, mimetype="application/octet-stream",
                     as_attachment=True, download_name="channelarr.m3u")


@api_bp.get("/export/xmltv")
def api_export_xmltv():
    """Serve the XMLTV EPG file for download."""
    m3u_path = get_setting("M3U_OUTPUT_PATH", "/m3u")
    filepath = os.path.join(m3u_path, "channelarr.xml")
    if not os.path.isfile(filepath):
        _regenerate_m3u()
    if not os.path.isfile(filepath):
        return jsonify({"error": "XMLTV not yet generated"}), 404
    return send_file(filepath, mimetype="application/octet-stream",
                     as_attachment=True, download_name="channelarr.xml")


# ----------------------- media -----------------------
@api_bp.get("/media/movies")
def api_movies():
    return jsonify(_media().get_movies())


@api_bp.get("/media/tv")
def api_tv_shows():
    return jsonify(_media().get_shows())


@api_bp.get("/media/tv/episodes")
def api_tv_episodes():
    path = request.args.get("path", "").strip()
    if not path:
        return jsonify({"error": "path is required"}), 400
    media_path = get_setting("MEDIA_PATH", "/media")
    norm_path = os.path.normpath(path)
    norm_media = os.path.normpath(media_path)
    if not norm_path.startswith(norm_media + os.sep) and norm_path != norm_media:
        logging.warning("[API] Episodes path rejected: %s not under %s", norm_path, norm_media)
        return jsonify({"error": "path must be under MEDIA_PATH"}), 403
    if not os.path.isdir(path):
        logging.warning("[API] Episodes path not a directory: %s", path)
        return jsonify({"error": "directory not found"}), 404
    return jsonify(_media().get_episodes(path))


# ----------------------- bumps -----------------------
@api_bp.get("/bumps")
def api_bumps():
    mgr = _bumps()
    all_clips = mgr.get_all()
    detail = {}
    for folder, paths in sorted(all_clips.items()):
        detail[folder] = [{"name": os.path.basename(p), "path": p} for p in paths]
    return jsonify({
        "folders": {k: len(v) for k, v in sorted(all_clips.items())},
        "total": sum(len(v) for v in all_clips.values()),
        "clips": detail,
    })


@api_bp.post("/bumps/scan")
def api_bumps_scan():
    _bumps().scan()
    return jsonify(_bumps().summary())


@api_bp.delete("/bumps/clip")
def api_bumps_delete():
    data = request.get_json() or {}
    path = data.get("path", "").strip()
    if not path:
        return jsonify({"error": "Path required"}), 400
    ok = _bumps().delete(path)
    if not ok:
        return jsonify({"error": "File not found or outside bumps directory"}), 404
    return jsonify({"status": "ok", "message": "Clip deleted"})


@api_bp.post("/bumps/download")
def api_bumps_download():
    data = request.get_json() or {}
    url = data.get("url", "").strip()
    folder = data.get("folder", "").strip()
    resolution = data.get("resolution", "1080").strip()
    if not url:
        return jsonify({"error": "URL required"}), 400
    if not folder:
        return jsonify({"error": "Folder required"}), 400
    if resolution not in ("480", "720", "1080"):
        resolution = "1080"
    _bumps().download_url(url, folder, resolution=resolution)
    return jsonify({"status": "ok", "message": f"Downloading to {folder}/ (max {resolution}p)... check logs for progress."})


# ----------------------- logos -----------------------
@api_bp.get("/logo/<channel_id>")
def api_get_logo(channel_id):
    logo_path = os.path.join(LOGO_DIR, f"{channel_id}.png")
    if not os.path.isfile(logo_path):
        return jsonify({"error": "No logo"}), 404
    return send_file(logo_path, mimetype="image/png")


@api_bp.post("/logo/<channel_id>")
def api_upload_logo(channel_id):
    if "file" not in request.files:
        return jsonify({"error": "No file"}), 400
    f = request.files["file"]
    data = f.read()
    if not data:
        return jsonify({"error": "Empty file"}), 400
    if data[:4] == b"\x89PNG":
        pass
    elif data[:2] in (b"\xff\xd8",):
        pass
    else:
        return jsonify({"error": "Only PNG or JPEG allowed"}), 400
    os.makedirs(LOGO_DIR, exist_ok=True)
    logo_path = os.path.join(LOGO_DIR, f"{channel_id}.png")
    with open(logo_path, "wb") as out:
        out.write(data)
    _regenerate_m3u()
    logging.info("[LOGO] Uploaded logo for %s", channel_id)
    return jsonify({"status": "ok"})


@api_bp.delete("/logo/<channel_id>")
def api_delete_logo(channel_id):
    logo_path = os.path.join(LOGO_DIR, f"{channel_id}.png")
    if os.path.isfile(logo_path):
        os.remove(logo_path)
        _regenerate_m3u()
        logging.info("[LOGO] Deleted logo for %s", channel_id)
        return jsonify({"status": "deleted"})
    return jsonify({"error": "No logo"}), 404


# ----------------------- media poster -----------------------
@api_bp.get("/media/poster")
def api_media_poster():
    from core.nfo import find_poster
    path = request.args.get("path", "").strip()
    if not path:
        return jsonify({"error": "path required"}), 400
    poster = find_poster(path)
    if not poster:
        return "", 404
    mime = "image/jpeg" if poster.lower().endswith(".jpg") else "image/png"
    return send_file(poster, mimetype=mime)


# ----------------------- bump thumbnail -----------------------
@api_bp.get("/bumps/thumbnail")
def api_bump_thumbnail():
    import subprocess as sp
    path = request.args.get("path", "").strip()
    if not path or not os.path.isfile(path):
        return "", 404
    bumps_path = get_setting("BUMPS_PATH", "/bumps")
    if not os.path.normpath(path).startswith(os.path.normpath(bumps_path)):
        return jsonify({"error": "Invalid path"}), 403
    try:
        result = sp.run(
            ["ffmpeg", "-y", "-loglevel", "quiet", "-ss", "1", "-i", path,
             "-vframes", "1", "-vf", "scale=160:90:force_original_aspect_ratio=decrease,pad=160:90:(ow-iw)/2:(oh-ih)/2",
             "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"],
            capture_output=True, timeout=10,
        )
        if result.returncode != 0 or not result.stdout:
            return "", 404
        from flask import Response
        return Response(result.stdout, mimetype="image/jpeg")
    except Exception:
        return "", 404


# ----------------------- logs -----------------------
@api_bp.get("/logs/tail")
def api_logs_tail():
    log_path = current_app.config.get("LOG_PATH", "/app/logs/channelarr.log")
    p = Path(log_path)

    if not p.exists():
        return jsonify({"text": "", "pos": 0, "inode": None, "reset": True})

    st = p.stat()
    inode_token = f"{st.st_dev}:{st.st_ino}"

    try:
        pos = int(request.args.get("pos", "0"))
    except Exception:
        pos = 0
    client_inode = request.args.get("inode")

    reset = False
    if client_inode and client_inode != inode_token:
        reset = True
        pos = 0
    elif pos > st.st_size:
        reset = True
        pos = 0

    with open(p, "rb") as f:
        f.seek(pos)
        data = f.read()
        new_pos = pos + len(data)

    text = data.decode("utf-8", errors="replace").replace("\r\n", "\n")
    return jsonify({"text": text, "pos": new_pos, "inode": inode_token, "reset": reset})


# ----------------------- settings -----------------------
@api_bp.get("/settings")
def api_get_settings():
    try:
        values = get_all_settings()
        return jsonify({"schema": SETTINGS_SCHEMA, "values": values})
    except Exception as e:
        logging.exception("[SETTINGS] Failed to get settings")
        return jsonify({"error": str(e)}), 500


@api_bp.post("/settings")
def api_save_settings():
    data = request.get_json() or {}
    valid_keys = set()
    for section in SETTINGS_SCHEMA.values():
        valid_keys.update(section["fields"].keys())
    filtered = {k: str(v) for k, v in data.items() if k in valid_keys}
    if not filtered:
        return jsonify({"status": "ok", "message": "No changes."})
    save_settings(filtered)
    return jsonify({"status": "ok", "updated": list(filtered.keys()), "message": "Settings saved."})


# ----------------------- system stats -----------------------
@api_bp.get("/system/stats")
def api_system_stats():
    mem = psutil.virtual_memory()
    current = {
        "cpu_percent": psutil.cpu_percent(interval=0),
        "ram_percent": mem.percent,
        "ram_used": mem.used,
        "ram_total": mem.total,
        "disk": None,
    }
    try:
        hls_path = get_setting("HLS_OUTPUT_PATH", "/app/data/hls")
        du = psutil.disk_usage(hls_path)
        current["disk"] = {
            "total": du.total,
            "used": du.used,
            "free": du.free,
            "percent": du.percent,
        }
    except Exception:
        pass
    return jsonify({
        "current": current,
        "history": {
            "timestamps": list(_stats_timestamps),
            "cpu": list(_cpu_history),
            "ram": list(_ram_history),
        },
    })
