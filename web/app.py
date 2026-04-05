"""FastAPI application for Channelarr."""

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from core.config import get_setting
from core.logging_setup import setup_logging
from core.bumps import BumpManager
from core.media import MediaLibrary
from core.channels import ChannelManager, materialize_schedule
from core.streamer import StreamerManager

from web import shared_state
from web.routers import channels, epg, media, bumps, settings, system, hls, hdhr, youtube


def _clean_stale_hls():
    """Remove stale HLS files from previous runs."""
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


def _materialize_missing(channel_mgr, bump_mgr, media_lib):
    """Auto-materialize channels that don't have a schedule yet."""
    channel_list = channel_mgr.list_channels()
    needs = any(not ch.get("materialized_schedule") for ch in channel_list)
    if not needs:
        return
    logging.info("[APP] Materializing schedules for channels without one...")
    for ch in channel_list:
        if not ch.get("materialized_schedule"):
            materialize_schedule(ch, bump_mgr, media_library=media_lib)
            channel_mgr.save_channel(ch)
    logging.info("[APP] Schedule materialization complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── STARTUP ──
    log_file = get_setting("LOG_FILE", "/app/logs/channelarr.log")
    setup_logging(log_file)

    bump_mgr = BumpManager(get_setting_fn=get_setting)
    bump_mgr.scan()
    media_lib = MediaLibrary(get_setting_fn=get_setting)
    channel_mgr = ChannelManager()
    streamer_mgr = StreamerManager(get_setting_fn=get_setting)

    # Store on app.state
    app.state.bump_mgr = bump_mgr
    app.state.media_lib = media_lib
    app.state.channel_mgr = channel_mgr
    app.state.streamer_mgr = streamer_mgr
    app.state.log_path = log_file

    # Set on shared_state module (for background threads + helpers)
    shared_state.bump_mgr = bump_mgr
    shared_state.media_lib = media_lib
    shared_state.channel_mgr = channel_mgr
    shared_state.streamer_mgr = streamer_mgr
    shared_state.log_path = log_file

    from core.youtube import yt_cleanup_all
    yt_cleanup_all()
    _clean_stale_hls()
    _materialize_missing(channel_mgr, bump_mgr, media_lib)
    shared_state.regenerate_m3u()
    shared_state.start_stats_collector()
    streamer_mgr.start_idle_cleanup(interval=60, timeout=300)

    logging.info("[APP] Channelarr ready")

    yield

    # ── SHUTDOWN ──
    logging.info("[APP] Shutting down — stopping all streams")
    streamer_mgr.stop_all()


_web_dir = Path(__file__).parent

app = FastAPI(title="Channelarr", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=_web_dir / "static"), name="static")
_templates = Jinja2Templates(directory=_web_dir / "templates")

app.include_router(channels.router, prefix="/api")
app.include_router(epg.router, prefix="/api")
app.include_router(media.router, prefix="/api")
app.include_router(bumps.router, prefix="/api")
app.include_router(settings.router, prefix="/api")
app.include_router(system.router, prefix="/api")
app.include_router(hls.router)
app.include_router(hdhr.router)
app.include_router(youtube.router, prefix="/api")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return _templates.TemplateResponse("ui.html", {"request": request, "api_base": "/api"})
