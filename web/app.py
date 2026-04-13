"""FastAPI application for Channelarr."""

import logging
import os
import threading
import time
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
from web.routers import channels, epg, media, bumps, settings, system, hls, hdhr, youtube, resolve, resolved_stream


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
    """Auto-materialize scheduled channels that don't have a schedule yet.
    Resolved channels are skipped — they're pure live streams with no schedule."""
    channel_list = [ch for ch in channel_mgr.list_channels() if ch.get("type") != "resolved"]
    needs = any(not ch.get("materialized_schedule") for ch in channel_list)
    if not needs:
        return
    logging.info("[APP] Materializing schedules for channels without one...")
    for ch in channel_list:
        if not ch.get("materialized_schedule"):
            materialize_schedule(ch, bump_mgr, media_library=media_lib)
            channel_mgr.save_channel(ch)
    logging.info("[APP] Schedule materialization complete")


def _start_vpn_threads():
    """Start VPN latency sampler and auto-rotate checker as daemon threads.

    Both threads run every 60s. The sampler runs in all modes (vpn and local)
    so the chart always has data. The auto-rotate checker only acts when
    gluetun is configured AND the vpn_auto_rotate_minutes setting is > 0.
    """
    from core.vpn_monitor import sample_latency, maybe_auto_rotate

    # Take one immediate sample so the chart isn't empty for the first 60s
    try:
        sample_latency()
    except Exception:
        pass

    def _vpn_sample_loop():
        while True:
            time.sleep(60)
            try:
                sample_latency()
            except Exception as e:
                logging.error("[VPN-MONITOR] sample failed: %s", e)

    def _vpn_rotate_loop():
        while True:
            time.sleep(60)
            try:
                maybe_auto_rotate()
            except Exception as e:
                logging.error("[VPN-MONITOR] rotate check failed: %s", e)

    import threading
    threading.Thread(target=_vpn_sample_loop, daemon=True).start()

    if get_setting("GLUETUN_CONTROL_URL", ""):
        threading.Thread(target=_vpn_rotate_loop, daemon=True).start()
        logging.info("[VPN-MONITOR] Started VPN sample + auto-rotate threads")
    else:
        logging.info("[VPN-MONITOR] Started network latency sampler (no gluetun configured)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── STARTUP ──
    log_file = get_setting("LOG_FILE", "/app/logs/channelarr.log")
    setup_logging(log_file)

    # Initialize Postgres for resolver storage (existing channels stay in JSON)
    from core.database import init_engine, get_engine
    from core.models import Base
    from sqlalchemy import text, inspect
    try:
        init_engine()
        engine = get_engine()
        insp = inspect(engine)
        tables = insp.get_table_names()
        # Schema migrations (idempotent). B5 also drops legacy columns.
        if "manifests" in tables:
            manifest_cols = [c["name"] for c in insp.get_columns("manifests")]
            with engine.connect() as conn:
                # Add columns introduced before B5 (still needed in case
                # someone is upgrading from an older version)
                for col, coltype in [
                    ("expires_at", "TIMESTAMPTZ"),
                    ("last_refreshed_at", "TIMESTAMPTZ"),
                    ("last_accessed_at", "TIMESTAMPTZ"),
                ]:
                    if col not in manifest_cols:
                        conn.execute(text(f"ALTER TABLE manifests ADD COLUMN {col} {coltype}"))
                # B5: drop the legacy channelarr_channel_id linker column
                if "channelarr_channel_id" in manifest_cols:
                    conn.execute(text("ALTER TABLE manifests DROP COLUMN channelarr_channel_id"))
                    logging.info("[DB] Dropped legacy column manifests.channelarr_channel_id (B5)")
                conn.commit()
        # B6: add the transcode_mediated column to channels (idempotent)
        if "channels" in tables:
            channel_cols = [c["name"] for c in insp.get_columns("channels")]
            if "transcode_mediated" not in channel_cols:
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE channels ADD COLUMN transcode_mediated BOOLEAN NOT NULL DEFAULT FALSE"))
                    conn.commit()
                logging.info("[DB] Added column channels.transcode_mediated (B6)")
            if "profile_name" not in channel_cols:
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE channels ADD COLUMN profile_name VARCHAR NOT NULL DEFAULT 'auto'"))
                    conn.commit()
                logging.info("[DB] Added column channels.profile_name (B6.2)")
        Base.metadata.create_all(engine)
        logging.info("[DB] Resolver tables ready")
        # Start demand-driven refresh worker (only if DB is reachable)
        from core.resolver.manifest_resolver import start_refresh_worker
        start_refresh_worker()
        # B5 finalization: migrate legacy ch-{8} IDs to UUIDs, retire the
        # JSON safety net, clean orphaned resolved channels.
        from core.channels import (
            migrate_channel_ids_to_uuids,
            backup_channels_json,
            backfill_resolved_manifests_to_channels,
        )
        migrate_channel_ids_to_uuids()
        backfill_resolved_manifests_to_channels()
        backup_channels_json()
    except Exception as e:
        logging.error("[DB] Failed to initialize Postgres: %s", e)
        logging.error("[DB] Resolver features will be unavailable. Set PG_PASS in env or settings.")

    bump_mgr = BumpManager(get_setting_fn=get_setting)
    bump_mgr.scan()
    bump_mgr.precache_bumps()
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

    # VPN latency sampling + auto-rotate (only when gluetun is wired in)
    _start_vpn_threads()

    from core.youtube import start_yt_cache_worker
    start_yt_cache_worker(channel_mgr)

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
app.include_router(resolve.router, prefix="/api")
app.include_router(resolved_stream.router)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return _templates.TemplateResponse("ui.html", {"request": request, "api_base": "/api"})
