import atexit
import logging
from flask import Flask
from core.config import get_setting
from core.logging_setup import setup_logging
from core.bumps import BumpManager
from core.media import MediaLibrary
from core.channels import ChannelManager
from core.streamer import StreamerManager
from .blueprints.api import api_bp
from .blueprints.ui import ui_bp
from .blueprints.hls import hls_bp


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

    logging.info("[APP] Channelarr ready")
    return app
