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
from web.routers import channels, epg, media, bumps, settings, system, hls, hdhr, youtube, resolve, resolved_stream, scrapers, scraped_events, integrations, logo_search


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
                    ("cookies", "JSONB NOT NULL DEFAULT '[]'"),
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
            if "encoder_mode" not in channel_cols:
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE channels ADD COLUMN encoder_mode VARCHAR NOT NULL DEFAULT 'single'"))
                    conn.commit()
                logging.info("[DB] Added column channels.encoder_mode")
            if "tags" not in channel_cols:
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE channels ADD COLUMN tags JSONB NOT NULL DEFAULT '[]'"))
                    conn.commit()
                logging.info("[DB] Added column channels.tags")
            if "event_start" not in channel_cols:
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE channels ADD COLUMN event_start TIMESTAMPTZ"))
                    conn.commit()
                logging.info("[DB] Added column channels.event_start")
            if "event_end" not in channel_cols:
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE channels ADD COLUMN event_end TIMESTAMPTZ"))
                    conn.commit()
                logging.info("[DB] Added column channels.event_end")
        Base.metadata.create_all(engine)
        logging.info("[DB] Resolver tables ready")
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
        from core.event_resolver import backfill_from_channels
        backfill_from_channels()
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

    # ── Register all background tasks with the central scheduler ──
    from core.scheduler import add_job

    # Stats collector
    shared_state.start_stats_collector()  # still uses its own thread for CPU sampling
    # Note: stats_collector stays as a thread because psutil.cpu_percent(interval=1)
    # blocks for 1 second — not suitable for APScheduler's thread pool.

    # Stream idle cleanup
    add_job("stream_cleanup", lambda: streamer_mgr.cleanup_idle(300), seconds=60)

    # VPN latency sampling + auto-rotate
    from core.vpn_monitor import sample_latency, maybe_auto_rotate
    try:
        sample_latency()  # immediate first sample
    except Exception:
        pass
    add_job("vpn_sampler", sample_latency, seconds=60)
    if get_setting("GLUETUN_CONTROL_URL", ""):
        # Auto-rotate "checker" runs every 60s and self-skips unless the
        # vpn_auto_rotate_minutes setting > 0 AND enough time has elapsed.
        # Pass seconds=60 explicitly here; the saved-interval override
        # (TASK_INTERVALS) is the only way users should change this.
        add_job("vpn_auto_rotate", maybe_auto_rotate, seconds=60)
        # Scheduled rotate fires once a day at HH:MM in CRON_TZ (Eastern by
        # default). Default to 04:00 so the task is always visible in the
        # Tasks UI for VPN-mode users and they can adjust from the time
        # picker.
        from core.scheduler import update_vpn_scheduled_rotate
        sched_time = (get_setting("vpn_scheduled_rotate_time", "") or "").strip() or "04:00"
        update_vpn_scheduled_rotate(sched_time)
    logging.info("[VPN-MONITOR] Started VPN scheduler jobs")

    # Event channel cleanup
    from core.channels import cleanup_expired_event_channels
    add_job("event_cleanup", cleanup_expired_event_channels, seconds=60)
    logging.info("[CLEANUP] Scheduled event channel cleanup (60s interval)")

    # JIT event resolver — drains scraped_events queue as kickoffs approach
    from core.event_resolver import resolve_due_events, expire_stale_events
    add_job("event_resolver", resolve_due_events, seconds=120, max_instances=1)
    logging.info("[QUEUE] Scheduled JIT event resolver (120s interval)")
    add_job("event_expire", expire_stale_events, seconds=300)
    logging.info("[QUEUE] Scheduled event queue expire job (300s interval)")

    # Manifest refresh tick — keeps resolved channels (esp. 24/7) tunable.
    # Shares pipeline_lock with event_resolver so only one is using the
    # single-threaded sidecar at a time.
    from core.resolver.manifest_resolver import refresh_due_manifests
    add_job("manifest_refresh", refresh_due_manifests, seconds=60, max_instances=1)
    logging.info("[RESOLVER] Scheduled manifest refresh tick (60s interval)")

    # YouTube pre-cache worker (stays as thread — cookie warmup + failure tracking)
    from core.youtube import start_yt_cache_worker
    start_yt_cache_worker(channel_mgr)

    # Scraper plugin scheduler
    try:
        from core.scraper_runner import start_scraper_scheduler
        start_scraper_scheduler()
    except Exception as e:
        logging.warning("[SCRAPER] Scheduler startup failed: %s", e)

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
app.include_router(scrapers.router, prefix="/api")
app.include_router(scraped_events.router, prefix="/api")
app.include_router(integrations.router, prefix="/api")
app.include_router(logo_search.router, prefix="/api")
app.include_router(resolved_stream.router)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return _templates.TemplateResponse("ui.html", {"request": request, "api_base": "/api"})
