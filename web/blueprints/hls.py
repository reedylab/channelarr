"""HLS segment and playlist serving + bump preview.

Channels are always on standby. When a client requests an HLS playlist,
the channel starts encoding from wherever the schedule says it should be
at that moment — even mid-file.
"""

import logging
import os
import threading
import time

from flask import Blueprint, send_from_directory, abort, request, current_app
from core.config import get_setting
from core.channels import find_schedule_position

hls_bp = Blueprint("hls", __name__)

# Boot locks prevent concurrent auto-start attempts for the same channel
_boot_locks = {}
_boot_locks_lock = threading.Lock()


def _get_boot_lock(channel_id):
    with _boot_locks_lock:
        if channel_id not in _boot_locks:
            _boot_locks[channel_id] = threading.Lock()
        return _boot_locks[channel_id]


def _start_from_schedule(channel_id):
    """Start a channel stream from its current schedule position.

    Returns (ok, msg).
    """
    channel_mgr = current_app.config["CHANNEL_MGR"]
    streamer_mgr = current_app.config["STREAMER_MGR"]

    ch = channel_mgr.get_channel(channel_id)
    if not ch:
        return False, "Channel not found"

    schedule = ch.get("materialized_schedule", [])
    if not schedule:
        return False, "No materialized schedule — run Regenerate first"

    idx, seek = find_schedule_position(ch)
    if idx is None:
        return False, "Schedule ended (non-looping channel)"

    bump_cfg = ch.get("bump_config", {})
    ok = streamer_mgr.start_channel(
        channel_id,
        schedule=schedule,
        start_index=idx,
        start_seek=seek,
        loop=ch.get("loop", True),
        show_next=bump_cfg.get("show_next", False),
        channel_mgr=channel_mgr,
    )
    return ok, "Started" if ok else "Already running"


@hls_bp.get("/live/<channel_id>/stream.m3u8")
def hls_playlist(channel_id):
    hls_base = get_setting("HLS_OUTPUT_PATH", "/app/data/hls")
    hls_dir = os.path.join(hls_base, channel_id)
    playlist = os.path.join(hls_dir, "stream.m3u8")

    # Touch streamer so idle timeout resets
    streamer_mgr = current_app.config["STREAMER_MGR"]
    streamer_mgr.touch(channel_id)

    # Check if streamer is actually running — stale playlist files from previous
    # runs must not be served without an active encoder behind them.
    stream_status = streamer_mgr.get_status(channel_id)
    need_start = not stream_status.get("running", False)

    if need_start:
        lock = _get_boot_lock(channel_id)
        if not lock.acquire(timeout=20):
            abort(503)
        try:
            # Double-check — another request may have started it
            if not os.path.isfile(playlist):
                logging.info("[HLS] Auto-starting channel %s from schedule", channel_id)
                ok, msg = _start_from_schedule(channel_id)
                if not ok:
                    logging.warning("[HLS] Auto-start failed for %s: %s", channel_id, msg)
                    abort(404)
                # Wait for .m3u8 to appear
                deadline = time.time() + 20
                while time.time() < deadline:
                    if os.path.isfile(playlist):
                        break
                    time.sleep(0.3)
                else:
                    logging.error("[HLS] Timed out waiting for playlist: %s", channel_id)
                    abort(503)
        finally:
            lock.release()

    return send_from_directory(
        hls_dir, "stream.m3u8",
        mimetype="application/vnd.apple.mpegurl",
    )


@hls_bp.get("/live/<channel_id>/<segment>")
def hls_segment(channel_id, segment):
    if not segment.endswith(".ts"):
        abort(400)
    hls_base = get_setting("HLS_OUTPUT_PATH", "/app/data/hls")
    hls_dir = os.path.join(hls_base, channel_id)
    seg_path = os.path.join(hls_dir, segment)
    if not os.path.isfile(seg_path):
        abort(404)
    # Touch streamer — client is actively watching
    streamer_mgr = current_app.config["STREAMER_MGR"]
    streamer_mgr.touch(channel_id)
    return send_from_directory(hls_dir, segment, mimetype="video/mp2t")


@hls_bp.get("/preview/bump")
def preview_bump():
    """Serve a bump clip for in-browser playback. ?path=/bumps/nature/clip.mp4"""
    filepath = request.args.get("path", "")
    if not filepath:
        abort(400)
    bumps_path = get_setting("BUMPS_PATH", "/bumps")
    real = os.path.realpath(filepath)
    if not real.startswith(os.path.realpath(bumps_path)):
        abort(403)
    if not os.path.isfile(real):
        abort(404)
    directory = os.path.dirname(real)
    filename = os.path.basename(real)
    return send_from_directory(directory, filename)
