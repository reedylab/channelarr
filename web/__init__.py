import atexit
import logging
import os
import shutil
from flask import Flask
from core.config import get_setting
from core.logging_setup import setup_logging
from core.bumps import BumpManager
from core.media import MediaLibrary
from core.channels import ChannelManager, materialize_all_channels
from core.streamer import StreamerManager
from .blueprints.api import api_bp, _regenerate_m3u
from .blueprints.ui import ui_bp
from .blueprints.hls import hls_bp


def _clean_stale_hls():
    """Remove stale HLS files from previous runs so auto-start works correctly."""
    hls_base = get_setting("HLS_OUTPUT_PATH", "/app/data/hls")
    if not os.path.isdir(hls_base):
        return
    for name in os.listdir(hls_base):
        ch_dir = os.path.join(hls_base, name)
        if os.path.isdir(ch_dir):
            for f in os.listdir(ch_dir):
                if f.endswith(".ts") or f.endswith(".m3u8") or f == "concat.txt":
                    os.remove(os.path.join(ch_dir, f))
    logging.info("[APP] Cleaned stale HLS files")


def create_app():
    log_file = get_setting("LOG_FILE", "/app/logs/channelarr.log")
    setup_logging(log_file)

    app = Flask(__name__)
    app.config["LOG_PATH"] = log_file

    # Core managers
    bump_mgr = BumpManager(get_setting_fn=get_setting)
    bump_mgr.scan()

    media_lib = MediaLibrary(get_setting_fn=get_setting)
    channel_mgr = ChannelManager()
    streamer_mgr = StreamerManager(get_setting_fn=get_setting)

    app.config["BUMP_MGR"] = bump_mgr
    app.config["MEDIA_LIB"] = media_lib
    app.config["CHANNEL_MGR"] = channel_mgr
    app.config["STREAMER_MGR"] = streamer_mgr

    @atexit.register
    def shutdown():
        logging.info("[APP] Shutting down — stopping all streams")
        streamer_mgr.stop_all()

    app.register_blueprint(ui_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(hls_bp)

    # Clean stale HLS files from previous runs
    _clean_stale_hls()

    # Auto-materialize channels that don't have a schedule yet
    channels = channel_mgr.list_channels()
    needs_materialize = any(not ch.get("materialized_schedule") for ch in channels)
    if needs_materialize:
        logging.info("[APP] Materializing schedules for channels without one...")
        for ch in channels:
            if not ch.get("materialized_schedule"):
                from core.channels import materialize_schedule
                materialize_schedule(ch, bump_mgr, media_library=media_lib)
                channel_mgr.save_channel(ch)
        logging.info("[APP] Schedule materialization complete")

    # Generate M3U + XMLTV on startup
    with app.app_context():
        _regenerate_m3u()

    # Start idle stream cleanup
    streamer_mgr.start_idle_cleanup(interval=60, timeout=300)

    logging.info("[APP] Channelarr ready")
    return app
