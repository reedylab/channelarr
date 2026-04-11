"""Channel CRUD, schedule materialization, and position calculation."""

import json
import os
import random
import logging
import subprocess
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from core.nfo import read_nfo_title, read_nfo_plot

# Path of the legacy JSON store. Post-B5 the app no longer reads or writes
# this file — backup_channels_json() moves it aside on first startup. The
# constant is kept so the backup helper can find the original location.
CHANNELS_FILE = os.getenv("CHANNELS_FILE", "/app/data/channels.json")


# ── Postgres-backed channel store (Phase B2) ────────────────────────────────
# In B2 the channels table becomes the source of truth. JSON is still written
# as a safety net so we can roll back to B1 behavior by switching reads back.
# If the DB is unreachable at read time, calls fall back to the JSON store
# (so resolver features go offline gracefully but local/YT channels keep
# working).

def _row_to_dict(row, manifest=None) -> dict:
    """Convert a Channel SQLAlchemy row to the dict shape callers expect.

    Both scheduled and resolved channels return the same top-level shape,
    differentiated by the `type` field. Resolved channels include manifest
    info (manifest_id, manifest_url, source_domain, expires_at) so the
    frontend can render them and route to the right stream URL.
    """
    base = {
        "id": row.id,
        "name": row.name,
        "type": row.type,
        "logo_filename": row.logo_filename,
        "items": row.items or [],
        "bump_config": row.bump_config or {},
        "shuffle_config": row.shuffle_config or {"mode": "none"},
        "loop": bool(row.loop),
        "materialized_schedule": row.materialized_schedule or [],
        "schedule_epoch": row.schedule_epoch.isoformat() if row.schedule_epoch else None,
        "schedule_cycle_duration": row.schedule_cycle_duration or 0,
        "manifest_id": row.manifest_id,
    }
    # Legacy boolean shuffle field for backward-compat with code that hasn't
    # been updated to read shuffle_config.
    sc_mode = (row.shuffle_config or {}).get("mode")
    base["shuffle"] = sc_mode == "random"

    if row.type == "resolved":
        m = manifest if manifest is not None else row.manifest
        if m is not None:
            base["manifest_url"] = m.url
            base["source_domain"] = m.source_domain
            base["expires_at"] = m.expires_at.isoformat() if m.expires_at else None
        else:
            base["manifest_url"] = None
            base["source_domain"] = None
            base["expires_at"] = None
    return base


def _db_list_all() -> list:
    """Return every channel (scheduled + resolved) from the DB.

    Eager-loads the manifest relationship for resolved channels so we don't
    issue N+1 queries. Raises if the DB is unreachable.
    """
    from core.database import get_session
    from core.models import Channel as ChannelRow
    from sqlalchemy.orm import joinedload

    with get_session() as session:
        rows = (
            session.query(ChannelRow)
            .options(joinedload(ChannelRow.manifest))
            .all()
        )
        # Materialize before session closes
        return [_row_to_dict(r, r.manifest) for r in rows]


def _db_get_one(channel_id: str) -> dict | None:
    from core.database import get_session
    from core.models import Channel as ChannelRow
    from sqlalchemy.orm import joinedload

    with get_session() as session:
        row = (
            session.query(ChannelRow)
            .options(joinedload(ChannelRow.manifest))
            .filter_by(id=channel_id)
            .first()
        )
        if row is None:
            return None
        return _row_to_dict(row, row.manifest)


def _db_upsert(channel: dict):
    """Write a scheduled channel to the DB (insert or update). Raises on failure."""
    from core.database import get_session
    from core.models import Channel as ChannelRow

    with get_session() as session:
        row = session.query(ChannelRow).filter_by(id=channel["id"]).first()
        if row is None:
            row = ChannelRow(id=channel["id"], type="scheduled")
            session.add(row)
        _apply_dict_to_row(row, channel)


def _db_delete(channel_id: str):
    from core.database import get_session
    from core.models import Channel as ChannelRow

    with get_session() as session:
        row = session.query(ChannelRow).filter_by(id=channel_id).first()
        if row is not None:
            session.delete(row)


# B5: JSON safety net is retired. Postgres is the only source of truth.
# channels.json is moved aside on first startup after the migration runs.


# ── B5 finalization migrations ──────────────────────────────────────────────

def migrate_channel_ids_to_uuids():
    """Replace legacy ch-{8} and ch-res-{8} channel IDs with plain UUIDs.

    Idempotent: channels already on UUID format are skipped. For each
    migrated channel, the logo file (if any) is renamed to match the new ID.
    Old HLS dirs are not touched here — _clean_stale_hls() handles them on
    every startup anyway.

    Channel.id has no foreign-key dependents, so the UPDATE is safe.
    """
    try:
        from core.database import get_session
        from core.models import Channel as ChannelRow
    except Exception as e:
        logging.warning("[CHANNELS] DB unavailable, skipping ID migration: %s", e)
        return

    logo_dir = os.getenv("LOGO_DIR", "/app/data/logos")
    migrated = 0
    try:
        with get_session() as session:
            rows = (
                session.query(ChannelRow)
                .filter(ChannelRow.id.like("ch-%"))
                .all()
            )
            renames: list[tuple[str, str]] = []
            for r in rows:
                old_id = r.id
                new_id = str(uuid.uuid4())
                r.id = new_id
                renames.append((old_id, new_id))
                migrated += 1

        # Rename logo files outside the DB session — best-effort, failures
        # are logged but don't roll back the DB change.
        for old_id, new_id in renames:
            old_logo = os.path.join(logo_dir, f"{old_id}.png")
            new_logo = os.path.join(logo_dir, f"{new_id}.png")
            if os.path.isfile(old_logo):
                try:
                    os.rename(old_logo, new_logo)
                    logging.info("[CHANNELS] Renamed logo %s -> %s", old_id, new_id)
                except OSError as e:
                    logging.warning("[CHANNELS] Logo rename failed for %s: %s", old_id, e)

        if migrated:
            logging.info("[CHANNELS] Migrated %d channel IDs to UUIDs", migrated)
    except Exception as e:
        logging.warning("[CHANNELS] ID migration failed: %s", e)


def backup_channels_json():
    """Move channels.json out of the way after the DB has become source of truth.

    Renames it to channels.json.b5-backup-{timestamp}. Keeps the file on disk
    for emergency recovery but stops the app from touching it. Idempotent:
    if the file is already backed up or doesn't exist, this is a no-op.
    """
    if not os.path.isfile(CHANNELS_FILE):
        return
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    backup_path = f"{CHANNELS_FILE}.b5-backup-{ts}"
    try:
        os.rename(CHANNELS_FILE, backup_path)
        logging.info("[CHANNELS] channels.json moved to %s (B5 safety net retirement)", backup_path)
    except OSError as e:
        logging.warning("[CHANNELS] channels.json backup failed: %s", e)
# ─────────────────────────────────────────────────────────────────────────────


def _apply_dict_to_row(row, channel: dict):
    """Copy fields from a JSON channel dict onto a Channel SQLAlchemy row."""
    row.name = channel.get("name", "Unnamed")
    row.items = channel.get("items", []) or []
    row.bump_config = channel.get("bump_config", {}) or {}
    sc = channel.get("shuffle_config")
    if sc is None and channel.get("shuffle"):
        sc = {"mode": "random"}
    row.shuffle_config = sc or {"mode": "none"}
    row.loop = bool(channel.get("loop", True))
    row.materialized_schedule = channel.get("materialized_schedule", []) or []
    row.schedule_cycle_duration = float(channel.get("schedule_cycle_duration", 0) or 0)
    epoch_str = channel.get("schedule_epoch")
    if epoch_str:
        try:
            row.schedule_epoch = datetime.fromisoformat(epoch_str)
        except (TypeError, ValueError):
            row.schedule_epoch = None
    else:
        row.schedule_epoch = None


def backfill_resolved_manifests_to_channels():
    """Clean up orphaned resolved channels.

    Post-B5 this function is purely a cleanup pass — it deletes resolved
    Channel rows whose manifest_id is NULL (manifest was hard-deleted but
    the row leaked). New resolved manifests no longer auto-create channels
    (B3 decoupling), so the create branch from earlier phases is gone.
    """
    try:
        from core.database import get_session
        from core.models import Channel as ChannelRow
    except Exception as e:
        logging.warning("[CHANNELS] DB modules unavailable, skipping orphan cleanup: %s", e)
        return

    cleaned = 0
    try:
        with get_session() as session:
            orphans = (
                session.query(ChannelRow)
                .filter(ChannelRow.type == "resolved")
                .filter(ChannelRow.manifest_id.is_(None))
                .all()
            )
            for o in orphans:
                logging.info("[CHANNELS] Removing orphaned resolved channel: %s (%s)", o.id, o.name)
                session.delete(o)
                cleaned += 1
        if cleaned:
            logging.info("[CHANNELS] Cleaned %d orphaned resolved channels", cleaned)
    except Exception as e:
        logging.warning("[CHANNELS] Orphan cleanup failed: %s", e)
# ─────────────────────────────────────────────────────────────────────────────

# Duration cache: {filepath: seconds} — avoids re-probing unchanged files
_duration_cache = {}


class ChannelManager:
    def list_channels(self) -> list:
        """Return all channels (scheduled + resolved) from the DB."""
        return _db_list_all()

    def get_channel(self, channel_id: str) -> dict | None:
        return _db_get_one(channel_id)

    def create_resolved_channel(self, manifest_id: str, name: str | None = None) -> dict | None:
        """Create a new resolved-type Channel referencing an existing manifest.

        Used by the library "Create Channel" flow. The manifest must exist
        and be active. Returns the new channel dict (enrichable shape) or
        None if the manifest doesn't exist.
        """
        from urllib.parse import urlparse
        try:
            from core.database import get_session
            from core.models import Channel as ChannelRow, Manifest
        except Exception as e:
            logging.error("[CHANNELS] DB unavailable, cannot create resolved channel: %s", e)
            return None

        with get_session() as session:
            m = (
                session.query(Manifest)
                .filter(Manifest.id == manifest_id)
                .filter(Manifest.active == True)  # noqa: E712
                .first()
            )
            if m is None:
                return None

            # Default name: provided → manifest title → m3u8 hostname → fallback
            display_name = (name or "").strip()
            if not display_name and m.title:
                display_name = m.title.strip()
            if not display_name:
                try:
                    host = urlparse(m.url).hostname or ""
                    if host:
                        display_name = host
                except Exception:
                    pass
            if not display_name:
                display_name = m.source_domain or "Unnamed Resolved"

            new_id = str(uuid.uuid4())
            row = ChannelRow(
                id=new_id,
                name=display_name,
                type="resolved",
                manifest_id=m.id,
                items=[],
                bump_config={},
                shuffle_config={"mode": "none"},
                loop=False,
                schedule_cycle_duration=0,
                materialized_schedule=[],
            )
            session.add(row)
            session.flush()

        logging.info("[CHANNELS] Created resolved channel %s -> manifest %s", new_id, manifest_id)
        return self.get_channel(new_id)

    def create_channel(self, data: dict) -> dict:
        channel = {
            "id": str(uuid.uuid4()),
            "type": "scheduled",
            "name": data.get("name", "New Channel"),
            "items": data.get("items", []),
            "bump_config": data.get("bump_config", {
                "enabled": False,
                "folders": [],
                "frequency": "between",
                "count": 1,
                "start_bumps": False,
                "show_next": False,
            }),
            "shuffle": data.get("shuffle", False),
            "shuffle_config": data.get("shuffle_config", None),
            "loop": data.get("loop", True),
        }
        _db_upsert(channel)
        logging.info("[CHANNELS] Created channel %s: %s", channel["id"], channel["name"])
        return channel

    def update_channel(self, channel_id: str, data: dict) -> dict | None:
        existing = self.get_channel(channel_id)
        if existing is None:
            return None
        if existing.get("type") == "resolved":
            return self._update_resolved(channel_id, data, existing)
        return self._update_scheduled(channel_id, existing, data)

    def _update_scheduled(self, channel_id: str, existing: dict, data: dict) -> dict | None:
        for key in ("name", "items", "bump_config", "shuffle", "shuffle_config", "loop"):
            if key in data:
                existing[key] = data[key]
        try:
            _db_upsert(existing)
        except Exception as e:
            logging.error("[CHANNELS] DB update failed for %s: %s", channel_id, e)
            return None
        logging.info("[CHANNELS] Updated channel %s", channel_id)
        existing["type"] = "scheduled"
        return existing

    def _update_resolved(self, channel_id: str, data: dict, existing: dict) -> dict | None:
        try:
            from core.database import get_session
            from core.models import Channel as ChannelRow
            with get_session() as session:
                row = session.query(ChannelRow).filter_by(id=channel_id).first()
                if row is None:
                    return None
                if "name" in data:
                    row.name = data["name"]
        except Exception as e:
            logging.error("[CHANNELS] Resolved update failed for %s: %s", channel_id, e)
            return None
        return self.get_channel(channel_id)

    def delete_channel(self, channel_id: str) -> bool:
        if self.get_channel(channel_id) is None:
            return False
        try:
            _db_delete(channel_id)
        except Exception as e:
            logging.error("[CHANNELS] DB delete failed for %s: %s", channel_id, e)
            return False
        logging.info("[CHANNELS] Deleted channel %s", channel_id)
        return True

    def save_channel(self, channel: dict):
        """Persist an updated channel dict back to the store.

        Used by the streamer/materialize loop to write computed schedule
        data back. No-op for resolved channels (they have no schedule).
        """
        if channel.get("type") == "resolved":
            return
        try:
            _db_upsert(channel)
        except Exception as e:
            logging.error("[CHANNELS] DB save failed for %s: %s", channel.get("id"), e)


# ---------------------------------------------------------------------------
# ffprobe duration
# ---------------------------------------------------------------------------

def ffprobe_duration(filepath: str) -> float:
    """Probe file duration in seconds via ffprobe. Results are cached."""
    if filepath in _duration_cache:
        return _duration_cache[filepath]
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", filepath],
            capture_output=True, text=True, timeout=30,
        )
        info = json.loads(result.stdout)
        dur = float(info["format"]["duration"])
        _duration_cache[filepath] = dur
        return dur
    except Exception as e:
        logging.warning("[PROBE] ffprobe failed for %s: %s", filepath, e)
        return 0.0


# ---------------------------------------------------------------------------
# Bump cycle helper
# ---------------------------------------------------------------------------

def _make_bump_cycle(all_clips: list, total_needed: int) -> list:
    """Shuffle the full bump pool and tile it to fill all insertion slots."""
    if not all_clips:
        return []
    pool = list(all_clips)
    random.shuffle(pool)
    result = []
    while len(result) < total_needed:
        batch = list(pool)
        random.shuffle(batch)
        result.extend(batch)
    return result[:total_needed]


# ---------------------------------------------------------------------------
# Shuffle config helpers
# ---------------------------------------------------------------------------

def _normalize_shuffle_config(channel: dict) -> dict:
    """Return a shuffle_config dict, converting legacy boolean if needed."""
    cfg = channel.get("shuffle_config")
    if cfg and isinstance(cfg, dict):
        return cfg
    # Legacy boolean fallback
    if channel.get("shuffle"):
        return {"mode": "random"}
    return {"mode": "none"}


def _group_by_source(items: list, raw_items: list) -> list:
    """Group expanded episodes back by their source show/item.

    Returns list of (source_key, [items]) in original item order.
    Each movie/standalone item is its own group.
    """
    groups = OrderedDict()
    # Build a mapping of episode path prefix → source show path
    show_paths = [it["path"] for it in raw_items if it.get("type") == "show"]

    for item in items:
        # Find which show this episode belongs to
        source = None
        for sp in show_paths:
            if item["path"].startswith(sp):
                source = sp
                break
        if source is None:
            # Standalone item (movie or non-show) — its own group
            source = item["path"]
        groups.setdefault(source, []).append(item)

    return list(groups.items())


def _shuffle_round_robin(grouped: list) -> list:
    """Interleave items across groups in round-robin order."""
    queues = [list(items) for _, items in grouped]
    result = []
    while any(queues):
        for q in queues:
            if q:
                result.append(q.pop(0))
    return result


def _shuffle_weighted(grouped: list, weights: dict, total_episodes: int) -> list:
    """Build a weighted random schedule from grouped episodes.

    weights maps source path → integer percentage.
    """
    if total_episodes == 0:
        return []

    result = []
    remaining_slots = total_episodes
    remaining_pct = 100

    for source, episodes in grouped:
        pct = weights.get(source, 0)
        if pct <= 0:
            continue
        # Calculate target count, distributing rounding to last group
        if remaining_pct > 0:
            target = round(pct / remaining_pct * remaining_slots)
        else:
            target = 0
        target = min(target, len(episodes))
        target = max(target, 0)
        result.extend(episodes[:target])
        remaining_slots -= target
        remaining_pct -= pct

    # If rounding left slots unfilled, add remaining episodes from largest groups
    if remaining_slots > 0:
        for source, episodes in grouped:
            already = sum(1 for r in result if r in episodes)
            for ep in episodes[already:]:
                if remaining_slots <= 0:
                    break
                result.append(ep)
                remaining_slots -= 1

    random.shuffle(result)
    return result


# ---------------------------------------------------------------------------
# Transient schedule generation (ordering only, no timestamps)
# ---------------------------------------------------------------------------

def _schedule_entry(item: dict) -> dict:
    """Build a schedule entry from a channel item, preserving YouTube fields."""
    entry = {
        "type": item.get("type", "content"),
        "path": item.get("path", ""),
        "title": item.get("title", ""),
    }
    if item.get("type") == "youtube":
        entry["url"] = item.get("url", "")
        entry["yt_id"] = item.get("yt_id", "")
        entry["duration"] = item.get("duration", 0)
        entry["thumbnail"] = item.get("thumbnail", "")
    return entry


def generate_schedule(channel: dict, bump_manager, media_library=None) -> list:
    """Build the playback schedule, interleaving bumps if configured.

    Returns a list of dicts with type/path/title — no durations or timestamps.
    Used internally by materialize_schedule().
    """
    raw_items = list(channel.get("items", []))
    if not raw_items:
        return []

    items = []
    for item in raw_items:
        if item.get("type") == "show" and media_library:
            eps = media_library.get_episodes(item["path"])
            for ep in eps:
                items.append({
                    "type": "episode",
                    "path": ep["path"],
                    "title": item.get("title", "") + " " + ep.get("label", ""),
                })
            if not eps:
                logging.warning("[SCHEDULE] No episodes found for show: %s", item.get("path"))
        else:
            items.append(item)

    if not items:
        return []

    shuffle_cfg = _normalize_shuffle_config(channel)
    mode = shuffle_cfg.get("mode", "none")

    if mode == "random":
        random.shuffle(items)
    elif mode == "round_robin":
        grouped = _group_by_source(items, raw_items)
        items = _shuffle_round_robin(grouped)
    elif mode == "weighted":
        grouped = _group_by_source(items, raw_items)
        weights = shuffle_cfg.get("weights", {})
        items = _shuffle_weighted(grouped, weights, len(items))

    bump_cfg = channel.get("bump_config", {})
    folders = bump_cfg.get("folders") or []
    if not folders and bump_cfg.get("folder"):
        folders = [bump_cfg["folder"]]
    if not bump_cfg.get("enabled") or not folders:
        return [_schedule_entry(item) for item in items]

    count = bump_cfg.get("count", 1)
    freq = bump_cfg.get("frequency", "between")
    start_bumps = bump_cfg.get("start_bumps", False)

    n_insertions = 0
    if freq == "between":
        n_insertions = max(0, len(items) - 1)
    else:
        try:
            n = int(freq)
        except ValueError:
            n = 1
        n_insertions = sum(1 for i in range(1, len(items)) if i % n == 0)

    if start_bumps:
        n_insertions += 1

    all_clips = []
    for folder in folders:
        all_clips.extend(bump_manager.get_clips(folder))
    total_bumps_needed = n_insertions * count
    bump_cycle = _make_bump_cycle(all_clips, total_bumps_needed)
    bump_idx = 0

    if not bump_cycle:
        logging.warning("[SCHEDULE] No bumps found in folders %s", folders)

    schedule = []

    if start_bumps and bump_cycle:
        for _ in range(count):
            if bump_idx < len(bump_cycle):
                b = bump_cycle[bump_idx]
                bump_idx += 1
                schedule.append({"type": "bump", "path": b, "title": os.path.basename(b)})

    for i, item in enumerate(items):
        insert_here = False
        if freq == "between" and i > 0:
            insert_here = True
        elif freq != "between":
            try:
                n = int(freq)
            except ValueError:
                n = 1
            if i > 0 and i % n == 0:
                insert_here = True

        if insert_here and bump_cycle:
            for _ in range(count):
                if bump_idx < len(bump_cycle):
                    b = bump_cycle[bump_idx]
                    bump_idx += 1
                    schedule.append({"type": "bump", "path": b, "title": os.path.basename(b)})

        schedule.append(_schedule_entry(item))

    logging.info("[SCHEDULE] Built schedule: %d content + %d bumps", len(items), bump_idx)
    return schedule


# ---------------------------------------------------------------------------
# Schedule materialization — the heart of the EPG system
# ---------------------------------------------------------------------------

def materialize_schedule(channel: dict, bump_manager, media_library=None) -> dict:
    """Build and persist a materialized schedule with real timestamps.

    Returns the updated channel dict with:
      - materialized_schedule: list of entries with type/path/title/desc/duration/start/stop
      - schedule_epoch: ISO timestamp when the schedule was generated
      - schedule_cycle_duration: total seconds for one complete cycle
    """
    ordered = generate_schedule(channel, bump_manager, media_library=media_library)
    if not ordered:
        channel["materialized_schedule"] = []
        channel["schedule_epoch"] = datetime.now(timezone.utc).isoformat()
        channel["schedule_cycle_duration"] = 0
        return channel

    epoch = datetime.now(timezone.utc)
    elapsed = 0.0
    materialized = []

    for entry in ordered:
        if entry.get("type") == "youtube":
            from core.youtube import yt_cache_path, yt_get_duration
            yt_id = entry.get("yt_id", "")
            filepath = yt_cache_path(yt_id)
            duration = entry.get("duration", 0)
            if duration <= 0:
                duration = yt_get_duration(entry.get("url", ""))
            if duration <= 0:
                logging.warning("[MATERIALIZE] Skipping YouTube item %s — no duration", yt_id)
                continue
            title = entry.get("title", "") or yt_id
            desc = ""
        else:
            filepath = entry["path"]
            duration = ffprobe_duration(filepath)
            if duration <= 0:
                logging.warning("[MATERIALIZE] Skipping %s — could not determine duration", filepath)
                continue
            title = entry.get("title", "") or os.path.basename(filepath)
            desc = ""
            if entry["type"] != "bump":
                title = read_nfo_title(filepath)
                desc = read_nfo_plot(filepath)

        start_time = epoch + timedelta(seconds=elapsed)
        stop_time = start_time + timedelta(seconds=duration)

        mat_entry = {
            "type": entry["type"],
            "path": filepath,
            "title": title,
            "desc": desc,
            "duration": duration,
            "start": start_time.isoformat(),
            "stop": stop_time.isoformat(),
        }
        if entry.get("type") == "youtube":
            mat_entry["url"] = entry.get("url", "")
            mat_entry["yt_id"] = entry.get("yt_id", "")
            mat_entry["thumbnail"] = entry.get("thumbnail", "")
        materialized.append(mat_entry)
        elapsed += duration

    channel["materialized_schedule"] = materialized
    channel["schedule_epoch"] = epoch.isoformat()
    channel["schedule_cycle_duration"] = elapsed

    logging.info("[MATERIALIZE] Channel %s: %d entries, %.1f seconds (%.1f hours)",
                 channel.get("name", channel["id"]),
                 len(materialized), elapsed, elapsed / 3600)
    return channel


def materialize_all_channels(channel_mgr, bump_mgr, media_lib):
    """Materialize schedules for all channels and persist."""
    channels = channel_mgr.list_channels()
    for ch in channels:
        materialize_schedule(ch, bump_mgr, media_library=media_lib)
        channel_mgr.save_channel(ch)
    logging.info("[MATERIALIZE] All %d channels materialized", len(channels))


# ---------------------------------------------------------------------------
# Schedule position calculation
# ---------------------------------------------------------------------------

def find_schedule_position(channel: dict) -> tuple:
    """Determine what should be playing right now and how far into it.

    Returns (entry_index, seek_offset_seconds) or (None, None) if nothing to play.
    """
    schedule = channel.get("materialized_schedule", [])
    if not schedule:
        return (None, None)

    epoch_str = channel.get("schedule_epoch")
    cycle_dur = channel.get("schedule_cycle_duration", 0)
    if not epoch_str or cycle_dur <= 0:
        return (None, None)

    epoch = datetime.fromisoformat(epoch_str)
    now = datetime.now(timezone.utc)
    elapsed_since_epoch = (now - epoch).total_seconds()

    if not channel.get("loop", True):
        if elapsed_since_epoch >= cycle_dur:
            return (None, None)
        position_in_cycle = elapsed_since_epoch
    else:
        position_in_cycle = elapsed_since_epoch % cycle_dur

    accumulated = 0.0
    for i, entry in enumerate(schedule):
        entry_end = accumulated + entry["duration"]
        if position_in_cycle < entry_end:
            seek_offset = position_in_cycle - accumulated
            return (i, seek_offset)
        accumulated = entry_end

    # Edge case: floating point at exact end — wrap to first
    return (0, 0.0)


def get_now_playing(channel: dict) -> dict | None:
    """Get what's currently playing on a channel with progress info.

    Returns dict with current entry + progress, or None.
    """
    idx, seek = find_schedule_position(channel)
    if idx is None:
        return None

    schedule = channel.get("materialized_schedule", [])
    entry = schedule[idx]
    progress = seek / entry["duration"] if entry["duration"] > 0 else 0

    result = {
        "index": idx,
        "entry": entry,
        "seek_offset": seek,
        "progress": min(progress, 1.0),
    }

    # Next entry
    next_idx = idx + 1
    if next_idx >= len(schedule) and channel.get("loop", True):
        next_idx = 0
    if next_idx < len(schedule):
        result["next"] = schedule[next_idx]

    return result


# ── Placeholder programme blocks for live/empty channels ────────────────────

PLACEHOLDER_BLOCK_MINUTES = 30


def placeholder_entries_in_window(channel_name: str, window_start: datetime,
                                   window_end: datetime, *, is_live: bool = False) -> list:
    """Generate 30-minute placeholder programme blocks within a time window.

    Block boundaries align to :00 and :30 of every hour so this function
    agrees with current_placeholder_block() and the XMLTV exporter on which
    block contains any given moment. Used by the in-app guide endpoint for
    channels with no materialized schedule (resolved channels and empty
    scheduled channels alike).
    """
    block_minutes = PLACEHOLDER_BLOCK_MINUTES
    block = timedelta(minutes=block_minutes)
    block_start_minute = (window_start.minute // block_minutes) * block_minutes
    current = window_start.replace(minute=block_start_minute, second=0, microsecond=0)
    entries = []
    while current < window_end:
        stop = current + block
        entries.append({
            "title": channel_name,
            "desc": f"{channel_name} — Live Stream" if is_live else f"{channel_name} — Scheduled Programming",
            "type": "live" if is_live else "placeholder",
            "path": "",
            "start": current.isoformat(),
            "stop": stop.isoformat(),
            "duration": block_minutes * 60,
        })
        current = stop
    return entries


def current_placeholder_block(channel_name: str) -> dict:
    """Compute the current 30-minute placeholder block for a channel.

    Returns a dict matching the get_now_playing() shape so the UI can render
    it with the same progress-bar template. Used for resolved channels (which
    have no schedule by design) and as a deterministic fallback for any
    channel without a materialized schedule. Block boundaries align to :00
    and :30 of every hour, so multiple consumers (API, XMLTV) agree on which
    block is current at any moment.
    """
    block_minutes = PLACEHOLDER_BLOCK_MINUTES
    now = datetime.now(timezone.utc)
    block_start_minute = (now.minute // block_minutes) * block_minutes
    block_start = now.replace(minute=block_start_minute, second=0, microsecond=0)
    block_end = block_start + timedelta(minutes=block_minutes)
    duration = block_minutes * 60
    elapsed = (now - block_start).total_seconds()
    progress = max(0.0, min(1.0, elapsed / duration))
    next_start = block_end
    next_end = next_start + timedelta(minutes=block_minutes)
    return {
        "index": 0,
        "entry": {
            "type": "live",
            "title": channel_name,
            "desc": "Live stream",
            "start": block_start.isoformat(),
            "stop": block_end.isoformat(),
            "duration": duration,
        },
        "seek_offset": elapsed,
        "progress": progress,
        "next": {
            "type": "live",
            "title": channel_name,
            "desc": "Live stream",
            "start": next_start.isoformat(),
            "stop": next_end.isoformat(),
            "duration": duration,
        },
    }
