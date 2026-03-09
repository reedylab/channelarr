"""Channel CRUD, scheduling, and concat file generation."""

import json
import os
import random
import logging
import uuid

CHANNELS_FILE = os.getenv("CHANNELS_FILE", "/app/data/channels.json")


def _load() -> list:
    try:
        with open(CHANNELS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save(channels: list):
    os.makedirs(os.path.dirname(CHANNELS_FILE), exist_ok=True)
    with open(CHANNELS_FILE, "w") as f:
        json.dump(channels, f, indent=2)


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
            "loop": data.get("loop", True),
        }
        channels.append(channel)
        _save(channels)
        logging.info("[CHANNELS] Created channel %s: %s", channel["id"], channel["name"])
        return channel

    def update_channel(self, channel_id: str, data: dict) -> dict | None:
        channels = _load()
        for i, ch in enumerate(channels):
            if ch["id"] == channel_id:
                for key in ("name", "items", "bump_config", "shuffle", "loop"):
                    if key in data:
                        ch[key] = data[key]
                channels[i] = ch
                _save(channels)
                logging.info("[CHANNELS] Updated channel %s", channel_id)
                return ch
        return None

    def delete_channel(self, channel_id: str) -> bool:
        channels = _load()
        new = [ch for ch in channels if ch["id"] != channel_id]
        if len(new) == len(channels):
            return False
        _save(new)
        logging.info("[CHANNELS] Deleted channel %s", channel_id)
        return True


def _make_bump_cycle(all_clips: list, total_needed: int) -> list:
    """Shuffle the full bump pool and tile it to fill all insertion slots.

    This guarantees maximum variety — every bump plays before any repeats.
    """
    if not all_clips:
        return []
    pool = list(all_clips)
    random.shuffle(pool)
    # Tile the shuffled pool to cover total_needed picks
    result = []
    while len(result) < total_needed:
        batch = list(pool)
        random.shuffle(batch)
        result.extend(batch)
    return result[:total_needed]


def generate_schedule(channel: dict, bump_manager, media_library=None) -> list:
    """Build the playback schedule, interleaving bumps if configured."""
    raw_items = list(channel.get("items", []))
    if not raw_items:
        return []

    # Expand show items into individual episodes
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

    if channel.get("shuffle"):
        random.shuffle(items)

    bump_cfg = channel.get("bump_config", {})
    # Backward compat: old "folder" (string) → wrap in list
    folders = bump_cfg.get("folders") or []
    if not folders and bump_cfg.get("folder"):
        folders = [bump_cfg["folder"]]
    if not bump_cfg.get("enabled") or not folders:
        return [{"type": item.get("type", "content"), "path": item["path"],
                 "title": item.get("title", "")} for item in items]

    count = bump_cfg.get("count", 1)
    freq = bump_cfg.get("frequency", "between")
    start_bumps = bump_cfg.get("start_bumps", False)

    # Figure out how many insertion points we have
    if freq == "between":
        n_insertions = max(0, len(items) - 1)
    else:
        try:
            n = int(freq)
        except ValueError:
            n = 1
        n_insertions = sum(1 for i in range(1, len(items)) if i % n == 0)

    # Add start bumps to total
    if start_bumps:
        n_insertions += 1

    # Gather clips from ALL folders into one pool
    all_clips = []
    for folder in folders:
        all_clips.extend(bump_manager.get_clips(folder))
    total_bumps_needed = n_insertions * count
    bump_cycle = _make_bump_cycle(all_clips, total_bumps_needed)
    bump_idx = 0

    if not bump_cycle:
        logging.warning("[SCHEDULE] No bumps found in folders %s", folders)

    schedule = []

    # Insert bumps at start if configured
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

        schedule.append({
            "type": item.get("type", "content"),
            "path": item["path"],
            "title": item.get("title", ""),
        })

    logging.info("[SCHEDULE] Built schedule: %d content + %d bumps", len(items), bump_idx)
    return schedule


def start_channel_stream(channel_id, channel_mgr, bump_mgr, media_lib, streamer_mgr, get_setting_fn):
    """Generate schedule + concat, start FFmpeg. Returns (ok, msg)."""
    ch = channel_mgr.get_channel(channel_id)
    if not ch:
        return False, "Channel not found"
    if not ch.get("items"):
        return False, "Channel has no content"

    schedule = generate_schedule(ch, bump_mgr, media_library=media_lib)
    if not schedule:
        return False, "Empty schedule"

    hls_base = get_setting_fn("HLS_OUTPUT_PATH", "/app/data/hls")
    concat_path = os.path.join(hls_base, channel_id, "concat.txt")
    generate_concat_file(schedule, concat_path)

    def on_finished(cid):
        ch2 = channel_mgr.get_channel(cid)
        if not ch2:
            return None
        sched2 = generate_schedule(ch2, bump_mgr, media_library=media_lib)
        if not sched2:
            return None
        return generate_concat_file(sched2, concat_path)

    bump_cfg = ch.get("bump_config", {})
    ok = streamer_mgr.start_channel(
        channel_id, concat_path,
        loop=ch.get("loop", True),
        on_finished=on_finished,
        show_next=bump_cfg.get("show_next", False),
    )
    return ok, "Started" if ok else "Already running"


def generate_concat_file(schedule: list, output_path: str) -> str:
    """Write FFmpeg concat demuxer file with type metadata. Returns the file path."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        for entry in schedule:
            entry_type = entry.get("type", "content")
            f.write(f"# type={entry_type}\n")
            escaped = entry["path"].replace("'", "'\\''")
            f.write(f"file '{escaped}'\n")
    logging.info("[CHANNELS] Wrote concat file: %s (%d entries)", output_path, len(schedule))
    return output_path
