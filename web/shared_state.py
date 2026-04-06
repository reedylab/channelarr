"""Module-level shared state for Channelarr.

Managers are set during app lifespan startup and accessed by routers
and background threads via this module.
"""

import collections
import logging
import os
import threading
import time

import psutil

from core.config import get_setting
from core.xmltv import generate_channelarr_xmltv

# ── Managers (set by app.py lifespan) ──
bump_mgr = None
media_lib = None
channel_mgr = None
streamer_mgr = None
log_path: str = ""

LOGO_DIR = os.getenv("LOGO_DIR", "/app/data/logos")

# ── Settings schema ──
SETTINGS_SCHEMA = {
    "paths": {
        "label": "Paths",
        "fields": {
            "MEDIA_PATH": {"label": "Media Path", "type": "text", "placeholder": "/media"},
            "BUMPS_PATH": {"label": "Bumps Path", "type": "text", "placeholder": "/bumps"},
            "HLS_OUTPUT_PATH": {"label": "HLS Output Path", "type": "text", "placeholder": "/app/data/hls"},
            "M3U_OUTPUT_PATH": {"label": "M3U Output Path", "type": "text", "placeholder": "/m3u"},
            "YT_CACHE_PATH": {"label": "YouTube Cache Path", "type": "text", "placeholder": "/yt_cache"},
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
            "BASE_URL": {"label": "Base URL", "type": "text", "placeholder": "http://your-server-ip:5045"},
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

# ── Stats collector ──
_cpu_history = collections.deque(maxlen=2880)
_ram_history = collections.deque(maxlen=2880)
_yt_cache_history = collections.deque(maxlen=2880)
_stats_timestamps = collections.deque(maxlen=2880)
_stats_started = False


def _stats_collector():
    from core.youtube import yt_cache_size
    while True:
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        _cpu_history.append(cpu)
        _ram_history.append(mem.percent)
        _yt_cache_history.append(yt_cache_size())
        _stats_timestamps.append(time.time())
        time.sleep(29)


def start_stats_collector():
    global _stats_started
    if _stats_started:
        return
    _stats_started = True
    t = threading.Thread(target=_stats_collector, daemon=True)
    t.start()


def get_stats_snapshot():
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
    from core.youtube import yt_cache_size
    current["yt_cache_bytes"] = yt_cache_size()

    return {
        "current": current,
        "history": {
            "timestamps": list(_stats_timestamps),
            "cpu": list(_cpu_history),
            "ram": list(_ram_history),
            "yt_cache": list(_yt_cache_history),
        },
    }


# ── M3U + XMLTV regeneration ──
def regenerate_m3u():
    m3u_path = get_setting("M3U_OUTPUT_PATH", "/m3u")
    base_url = get_setting("BASE_URL", "http://localhost:5045")
    channels = channel_mgr.list_channels()

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

    xmltv_out = os.path.join(m3u_path, "channelarr.xml")
    generate_channelarr_xmltv(channels, xmltv_out, base_url)
