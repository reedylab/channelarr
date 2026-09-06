"""HLS segment and playlist serving + bump preview."""

import logging
import os
import threading
import time

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from web import shared_state
from core.config import get_setting
from core.channels import find_schedule_position

router = APIRouter()

_boot_locks = {}
_boot_locks_lock = threading.Lock()


def _get_boot_lock(channel_id):
    with _boot_locks_lock:
        if channel_id not in _boot_locks:
            _boot_locks[channel_id] = threading.Lock()
        return _boot_locks[channel_id]


def _is_expired(expires_at) -> bool:
    if not expires_at:
        return False
    try:
        from datetime import datetime, timezone
        dt = (datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
              if isinstance(expires_at, str) else expires_at)
        return dt <= datetime.now(timezone.utc)
    except Exception:
        return False


def _reload_manifest_url(manifest_id):
    from core.database import get_session
    from core.models.manifest import Manifest
    with get_session() as session:
        row = session.query(Manifest.url).filter_by(id=manifest_id).first()
        return row[0] if row else None


def _pick_working_manifest(ch):
    """Walk [primary] + fallback_sources in order, returning the first
    candidate that looks usable as (manifest_id, manifest_url), or
    (None, None) if the channel has no primary manifest at all.

    A candidate whose stored expires_at hasn't passed is trusted as-is —
    same as everywhere else expiry is checked in this app — costing nothing
    extra on the common healthy-primary path. An expired candidate gets one
    cheap light_refresh_manifest attempt (HTTP-only, no browser) before
    being skipped for the next candidate in the chain.

    If every candidate is exhausted, falls back to today's exact pre-chain
    behavior: heavy-refresh the primary via the sidecar and use it
    best-effort, so a channel with no working fallback is no worse off than
    before this existed.
    """
    from core.resolver.manifest_resolver import ManifestResolverService

    primary_id = ch.get("manifest_id")
    candidates = []
    if primary_id:
        candidates.append({"manifest_id": primary_id, "manifest_url": ch.get("manifest_url"),
                            "expires_at": ch.get("expires_at")})
    candidates.extend(ch.get("fallback_sources") or [])

    for i, cand in enumerate(candidates):
        mid, murl = cand.get("manifest_id"), cand.get("manifest_url")
        if not mid or not murl:
            continue
        if not _is_expired(cand.get("expires_at")):
            if i > 0:
                logging.info("[HLS] %s: using fallback source #%d (%s)", ch.get("id"), i, mid)
            return mid, murl
        try:
            if ManifestResolverService.light_refresh_manifest(mid).get("ok"):
                fresh = _reload_manifest_url(mid)
                if fresh:
                    logging.info("[HLS] %s: light-refreshed %s candidate #%d",
                                 ch.get("id"), "primary" if i == 0 else "fallback", i)
                    return mid, fresh
        except Exception as e:
            logging.warning("[HLS] light refresh failed for candidate %s: %s", mid, e)

    if not primary_id:
        return None, None

    # Whole chain exhausted — heavy-refresh the primary (sidecar/browser) and
    # use it best-effort, matching pre-chain behavior exactly.
    logging.info("[HLS] %s: all %d candidate(s) failed light refresh, heavy-refreshing primary %s",
                 ch.get("id"), len(candidates), primary_id)
    try:
        ManifestResolverService.refresh_manifest(primary_id)
        fresh = _reload_manifest_url(primary_id)
        if fresh:
            return primary_id, fresh
    except Exception as e:
        logging.warning("[HLS] Refresh before start failed for %s: %s", primary_id, e)
    return primary_id, ch.get("manifest_url")


def _start_from_schedule(channel_id):
    ch = shared_state.channel_mgr.get_channel(channel_id)
    if not ch:
        return False, "Channel not found"

    if ch.get("type") == "resolved":
        manifest_id, manifest_url = _pick_working_manifest(ch)
        if not manifest_id or not manifest_url:
            return False, "Resolved channel missing manifest"
        encoder_mode = ch.get("encoder_mode", "proxy")

        # Proxy mode — download segments with auth, serve locally. No encode.
        if encoder_mode == "proxy":
            ok = shared_state.streamer_mgr.start_proxy_channel(
                channel_id,
                manifest_id=manifest_id,
                manifest_url=manifest_url,
            )
            return ok, "Started" if ok else "Already running"

        # Remux mode — ffmpeg -c copy from the master URL. Handles fMP4/CMAF +
        # demuxed audio the proxy can't carry. No re-encode.
        if encoder_mode == "remux":
            ok = shared_state.streamer_mgr.start_remux_channel(
                channel_id,
                manifest_id=manifest_id,
                manifest_url=manifest_url,
            )
            return ok, "Started" if ok else "Already running"

        # Transcode mode — full re-encode with bump insertion.
        if ch.get("transcode_mediated"):
            ok = shared_state.streamer_mgr.start_resolved_channel(
                channel_id,
                manifest_id=manifest_id,
                manifest_url=manifest_url,
                bump_config=ch.get("bump_config", {}),
                bump_manager=shared_state.bump_mgr,
                channel_name=ch.get("name", ""),
                logo_dir=shared_state.LOGO_DIR,
                profile_name=ch.get("profile_name", "auto"),
                branding_logo_path=shared_state.streamer_mgr._resolve_branding_path(ch.get("branding_logo")),
                encoder_mode=encoder_mode,
            )
            return ok, "Started" if ok else "Already running"

        # Direct passthrough — raw CDN URL (handled by resolved_stream.py)
        return False, "Passthrough channels use /live-resolved/ endpoint"

    schedule = ch.get("materialized_schedule", [])
    if not schedule:
        return False, "No materialized schedule — run Regenerate first"

    idx, seek = find_schedule_position(ch)
    if idx is None:
        return False, "Schedule ended (non-looping channel)"

    bump_cfg = ch.get("bump_config", {})
    ok = shared_state.streamer_mgr.start_channel(
        channel_id,
        schedule=schedule,
        start_index=idx,
        start_seek=seek,
        loop=ch.get("loop", True),
        show_next=bump_cfg.get("show_next", False),
        channel_mgr=shared_state.channel_mgr,
        branding_logo_path=shared_state.streamer_mgr._resolve_branding_path(ch.get("branding_logo")),
    )
    return ok, "Started" if ok else "Already running"


@router.get("/live/{channel_id}/stream.m3u8")
def hls_playlist(channel_id: str):
    hls_base = get_setting("HLS_OUTPUT_PATH", "/app/data/hls")
    hls_dir = os.path.join(hls_base, channel_id)
    playlist = os.path.join(hls_dir, "stream.m3u8")

    shared_state.streamer_mgr.touch(channel_id)

    stream_status = shared_state.streamer_mgr.get_status(channel_id)
    need_start = not stream_status.get("running", False)

    if need_start:
        lock = _get_boot_lock(channel_id)
        if not lock.acquire(timeout=95):
            raise HTTPException(status_code=503)
        try:
            stream_status = shared_state.streamer_mgr.get_status(channel_id)
            if not stream_status.get("running", False):
                logging.info("[HLS] Auto-starting channel %s from schedule", channel_id)
                ok, msg = _start_from_schedule(channel_id)
                if not ok:
                    logging.warning("[HLS] Auto-start failed for %s: %s", channel_id, msg)
                    raise HTTPException(status_code=404)
            deadline = time.time() + 90
            while time.time() < deadline:
                if os.path.isfile(playlist):
                    break
                time.sleep(0.3)
            else:
                logging.error("[HLS] Timed out waiting for playlist: %s", channel_id)
                raise HTTPException(status_code=503)
        finally:
            lock.release()

    if not os.path.isfile(playlist):
        deadline = time.time() + 10
        while time.time() < deadline:
            if os.path.isfile(playlist):
                break
            time.sleep(0.3)
        else:
            raise HTTPException(status_code=503, detail="Stream not ready")

    return FileResponse(
        playlist,
        media_type="application/vnd.apple.mpegurl",
    )


@router.get("/live/{channel_id}/{segment}")
def hls_segment(channel_id: str, segment: str):
    if not segment.endswith(".ts"):
        raise HTTPException(status_code=400)
    hls_base = get_setting("HLS_OUTPUT_PATH", "/app/data/hls")
    hls_dir = os.path.join(hls_base, channel_id)
    seg_path = os.path.join(hls_dir, segment)
    if not os.path.isfile(seg_path):
        raise HTTPException(status_code=404)
    shared_state.streamer_mgr.touch(channel_id)
    return FileResponse(seg_path, media_type="video/mp2t")


@router.get("/preview/bump")
def preview_bump(path: str = Query(default="")):
    if not path:
        raise HTTPException(status_code=400)
    bumps_path = get_setting("BUMPS_PATH", "/bumps")
    real = os.path.realpath(path)
    if not real.startswith(os.path.realpath(bumps_path)):
        raise HTTPException(status_code=403)
    if not os.path.isfile(real):
        raise HTTPException(status_code=404)
    return FileResponse(real)
