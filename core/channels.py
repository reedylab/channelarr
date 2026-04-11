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


# ── DB mirror (Phase B1 dual-write) ──────────────────────────────────────────
# JSON is still the source of truth in B1. Every write to JSON also mirrors
# to the Postgres `channels` table. Mirror failures log a warning but never
# break the JSON path — the next startup backfill catches up missed records.

def _mirror_upsert(channel: dict):
    try:
        from core.database import get_session
        from core.models import Channel as ChannelRow
        from sqlalchemy.exc import SQLAlchemyError

        with get_session() as session:
            row = session.query(ChannelRow).filter_by(id=channel["id"]).first()
            if row is None:
                row = ChannelRow(id=channel["id"], type="scheduled")
                session.add(row)
            _apply_dict_to_row(row, channel)
    except SQLAlchemyError as e:
        logging.warning("[CHANNELS] DB mirror upsert failed for %s: %s", channel.get("id"), e)
    except Exception as e:
        logging.warning("[CHANNELS] DB mirror upsert error for %s: %s", channel.get("id"), e)


def _mirror_delete(channel_id: str):
    try:
        from core.database import get_session
        from core.models import Channel as ChannelRow
        from sqlalchemy.exc import SQLAlchemyError

        with get_session() as session:
            row = session.query(ChannelRow).filter_by(id=channel_id).first()
            if row is not None:
                session.delete(row)
    except SQLAlchemyError as e:
        logging.warning("[CHANNELS] DB mirror delete failed for %s: %s", channel_id, e)
    except Exception as e:
        logging.warning("[CHANNELS] DB mirror delete error for %s: %s", channel_id, e)


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
        return _load()

    def get_channel(self, channel_id: str) -> dict | None:
        for ch in _load():
            if ch["id"] == channel_id:
                return ch
        return None

    def create_channel(self, data: dict) -> dict:
        channels = _load()
        channel = {
            "id": f"ch-{uuid.uuid4().hex[:8]}",
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
        channels.append(channel)
        _save(channels)
        _mirror_upsert(channel)
        logging.info("[CHANNELS] Created channel %s: %s", channel["id"], channel["name"])
        return channel

    def update_channel(self, channel_id: str, data: dict) -> dict | None:
        channels = _load()
        for i, ch in enumerate(channels):
            if ch["id"] == channel_id:
                for key in ("name", "items", "bump_config", "shuffle", "shuffle_config", "loop"):
                    if key in data:
                        ch[key] = data[key]
                channels[i] = ch
                _save(channels)
                _mirror_upsert(ch)
                logging.info("[CHANNELS] Updated channel %s", channel_id)
                return ch
        return None

    def delete_channel(self, channel_id: str) -> bool:
        channels = _load()
        new = [ch for ch in channels if ch["id"] != channel_id]
        if len(new) == len(channels):
            return False
        _save(new)
        _mirror_delete(channel_id)
        logging.info("[CHANNELS] Deleted channel %s", channel_id)
        return True

    def save_channel(self, channel: dict):
        """Persist an updated channel dict back to the store."""
        channels = _load()
        for i, ch in enumerate(channels):
            if ch["id"] == channel["id"]:
                channels[i] = channel
                _save(channels)
                _mirror_upsert(channel)
                return
        # Channel not found — append it
        channels.append(channel)
        _save(channels)
        _mirror_upsert(channel)


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
