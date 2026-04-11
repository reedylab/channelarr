"""Channel CRUD, schedule materialization, and position calculation."""

import json
import os
import random
import logging
import subprocess
import tempfile
import threading
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from core.nfo import read_nfo_title, read_nfo_plot

CHANNELS_FILE = os.getenv("CHANNELS_FILE", "/app/data/channels.json")

_file_lock = threading.Lock()


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


# In B2, JSON is no longer the source of truth — DB writes come first, then
# JSON is mirrored as a safety net for one phase (retired in B5). If a DB
# write fails, we still write JSON and warn loudly; the next startup backfill
# will reconcile the DB from JSON.


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


def backfill_scheduled_channels_to_db():
    """Mirror every JSON channel into the Postgres channels table.

    Idempotent: existing rows are updated, new rows are inserted. Does NOT
    delete Postgres rows that are missing from JSON — resolved-channel rows
    must survive this pass.
    """
    try:
        from core.database import get_session
        from core.models import Channel as ChannelRow
    except Exception as e:
        logging.warning("[CHANNELS] DB modules unavailable, skipping backfill: %s", e)
        return

    json_channels = _load()
    count = 0
    try:
        with get_session() as session:
            for ch in json_channels:
                row = session.query(ChannelRow).filter_by(id=ch["id"]).first()
                if row is None:
                    row = ChannelRow(id=ch["id"], type="scheduled")
                    session.add(row)
                _apply_dict_to_row(row, ch)
                count += 1
        logging.info("[CHANNELS] Backfilled %d scheduled channels to DB", count)
    except Exception as e:
        logging.warning("[CHANNELS] Scheduled-channel backfill failed: %s", e)


def backfill_resolved_manifests_to_channels():
    """Create Channel rows for active resolved manifests that don't yet have one.

    Preserves the current "every resolved manifest is also a channel" behavior
    during the cutover. Decoupling happens in B3.
    """
    try:
        from core.database import get_session
        from core.models import Channel as ChannelRow, Manifest
    except Exception as e:
        logging.warning("[CHANNELS] DB modules unavailable, skipping resolved backfill: %s", e)
        return

    count = 0
    try:
        with get_session() as session:
            manifests = (
                session.query(Manifest)
                .filter(Manifest.active == True)  # noqa: E712
                .filter(Manifest.tags.contains(["resolved"]))
                .all()
            )
            for m in manifests:
                channel_id = m.channelarr_channel_id or f"ch-res-{uuid.uuid4().hex[:8]}"
                existing = session.query(ChannelRow).filter_by(id=channel_id).first()
                if existing is not None:
                    continue
                # Also catch the case where a different ID already maps to this manifest
                bymanifest = (
                    session.query(ChannelRow)
                    .filter_by(manifest_id=m.id, type="resolved")
                    .first()
                )
                if bymanifest is not None:
                    continue
                row = ChannelRow(
                    id=channel_id,
                    name=m.title or "Unnamed Resolved",
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
                count += 1
        if count:
            logging.info("[CHANNELS] Backfilled %d resolved manifests as channels", count)
    except Exception as e:
        logging.warning("[CHANNELS] Resolved-manifest backfill failed: %s", e)
# ─────────────────────────────────────────────────────────────────────────────

# Duration cache: {filepath: seconds} — avoids re-probing unchanged files
_duration_cache = {}


def _load() -> list:
    with _file_lock:
        try:
            with open(CHANNELS_FILE, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []


def _save(channels: list):
    with _file_lock:
        dir_name = os.path.dirname(CHANNELS_FILE)
        os.makedirs(dir_name, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(channels, f, indent=2, default=str)
            os.replace(tmp_path, CHANNELS_FILE)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


class ChannelManager:
    def list_channels(self) -> list:
        """Return all channels (scheduled + resolved) from the DB.

        Falls back to JSON (scheduled only — resolver features go offline)
        if the DB is unreachable.
        """
        try:
            return _db_list_all()
        except Exception as e:
            logging.warning("[CHANNELS] DB list failed, using JSON fallback: %s", e)
            return _load()

    def get_channel(self, channel_id: str) -> dict | None:
        try:
            ch = _db_get_one(channel_id)
            if ch is not None:
                return ch
        except Exception as e:
            logging.warning("[CHANNELS] DB get failed for %s, using JSON fallback: %s", channel_id, e)
        # Fall back to JSON (scheduled channels only)
        for ch in _load():
            if ch["id"] == channel_id:
                # Stamp type so callers can branch consistently
                ch.setdefault("type", "scheduled")
                return ch
        return None

    def create_channel(self, data: dict) -> dict:
        channel = {
            "id": f"ch-{uuid.uuid4().hex[:8]}",
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
        # DB first (source of truth in B2), then JSON safety net
        try:
            _db_upsert(channel)
        except Exception as e:
            logging.error("[CHANNELS] DB create failed for %s, JSON only: %s", channel["id"], e)
        channels = _load()
        channels.append(channel)
        _save(channels)
        logging.info("[CHANNELS] Created channel %s: %s", channel["id"], channel["name"])
        return channel

    def update_channel(self, channel_id: str, data: dict) -> dict | None:
        # Resolved channels: only `name` is editable (no items/schedule).
        # Look up the channel via the unified path so we know its type.
        existing = self.get_channel(channel_id)
        if existing is None:
            return None
        if existing.get("type") == "resolved":
            return self._update_resolved(channel_id, data, existing)
        return self._update_scheduled(channel_id, data)

    def _update_scheduled(self, channel_id: str, data: dict) -> dict | None:
        # Update the JSON record (still safety-net) and DB row in lockstep.
        channels = _load()
        target = None
        for i, ch in enumerate(channels):
            if ch["id"] == channel_id:
                for key in ("name", "items", "bump_config", "shuffle", "shuffle_config", "loop"):
                    if key in data:
                        ch[key] = data[key]
                channels[i] = ch
                target = ch
                break
        if target is None:
            # Channel exists in DB but not in JSON (e.g., a resolved channel
            # with type=scheduled — shouldn't happen, but be defensive).
            return None
        try:
            _db_upsert(target)
        except Exception as e:
            logging.error("[CHANNELS] DB update failed for %s, JSON only: %s", channel_id, e)
        _save(channels)
        logging.info("[CHANNELS] Updated channel %s", channel_id)
        target["type"] = "scheduled"
        return target

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
                # logo_filename, items/bumps are intentionally not updatable
                # for resolved channels in B2.
        except Exception as e:
            logging.error("[CHANNELS] Resolved update failed for %s: %s", channel_id, e)
            return None
        return self.get_channel(channel_id)

    def delete_channel(self, channel_id: str) -> bool:
        existing = self.get_channel(channel_id)
        if existing is None:
            return False
        # Delete from DB first
        try:
            _db_delete(channel_id)
        except Exception as e:
            logging.error("[CHANNELS] DB delete failed for %s: %s", channel_id, e)
        # JSON safety net only contains scheduled channels
        if existing.get("type") != "resolved":
            channels = _load()
            new = [ch for ch in channels if ch["id"] != channel_id]
            _save(new)
        logging.info("[CHANNELS] Deleted channel %s", channel_id)
        return True

    def save_channel(self, channel: dict):
        """Persist an updated channel dict back to the store.

        Used by the streamer/materialize loop to write back computed schedule
        data. For scheduled channels, dual-writes DB + JSON. For resolved
        channels, this is a no-op (they have no schedule to materialize).
        """
        if channel.get("type") == "resolved":
            return
        try:
            _db_upsert(channel)
        except Exception as e:
            logging.error("[CHANNELS] DB save failed for %s, JSON only: %s", channel.get("id"), e)
        channels = _load()
        for i, ch in enumerate(channels):
            if ch["id"] == channel["id"]:
                channels[i] = channel
                _save(channels)
                return
        channels.append(channel)
        _save(channels)


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
