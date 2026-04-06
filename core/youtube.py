"""YouTube metadata, download, cache management, and background pre-cache worker."""

import logging
import os
import subprocess
import threading
import time

from core.config import get_setting


def _cache_dir() -> str:
    return get_setting("YT_CACHE_PATH", "/yt_cache")


def yt_cache_path(yt_id: str) -> str:
    """Deterministic path for a cached YouTube video."""
    return os.path.join(_cache_dir(), f"{yt_id}.mp4")


def yt_cache_size() -> int:
    """Return total size of all files in the cache directory in bytes."""
    cache = _cache_dir()
    if not os.path.isdir(cache):
        return 0
    total = 0
    for f in os.listdir(cache):
        fp = os.path.join(cache, f)
        if os.path.isfile(fp):
            total += os.path.getsize(fp)
    return total


def yt_cleanup(yt_id: str):
    """Delete a single cached video file."""
    path = yt_cache_path(yt_id)
    if os.path.isfile(path):
        os.remove(path)
        logging.info("[YT] Cleaned up %s", yt_id)


def yt_cleanup_all():
    """Wipe the entire YouTube cache directory."""
    cache = _cache_dir()
    os.makedirs(cache, exist_ok=True)
    for f in os.listdir(cache):
        fp = os.path.join(cache, f)
        if os.path.isfile(fp):
            try:
                os.remove(fp)
            except OSError:
                pass
    logging.info("[YT] Cache cleared")


def yt_get_duration(url: str) -> float:
    """Get video duration in seconds without downloading."""
    try:
        result = subprocess.run(
            ["yt-dlp", "--print", "duration", "--no-download", url],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            return float(result.stdout.strip())
    except (subprocess.TimeoutExpired, ValueError) as e:
        logging.warning("[YT] Duration fetch failed for %s: %s", url, e)
    return 0.0


def yt_browse(url: str) -> list[dict]:
    """List videos in a YouTube channel, playlist, or single video URL.

    Returns list of {yt_id, url, title, duration, thumbnail}.
    """
    try:
        result = subprocess.run(
            [
                "yt-dlp",
                "--flat-playlist",
                "--print", "%(id)s\t%(title)s\t%(duration)s\t%(thumbnails.-1.url)s",
                "--no-download",
                url,
            ],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            err = result.stderr[-300:] if result.stderr else "Unknown error"
            logging.error("[YT] Browse failed for %s: %s", url, err)
            return []

        videos = []
        for line in result.stdout.strip().splitlines():
            parts = line.split("\t", 3)
            if len(parts) < 2:
                continue
            yt_id = parts[0].strip()
            title = parts[1].strip()
            dur_str = parts[2].strip() if len(parts) > 2 else ""
            thumb = parts[3].strip() if len(parts) > 3 else ""

            duration = 0.0
            try:
                duration = float(dur_str)
            except (ValueError, TypeError):
                pass

            if not thumb or thumb == "NA":
                thumb = f"https://i.ytimg.com/vi/{yt_id}/hqdefault.jpg"

            videos.append({
                "yt_id": yt_id,
                "url": f"https://www.youtube.com/watch?v={yt_id}",
                "title": title,
                "duration": duration,
                "thumbnail": thumb,
            })

        logging.info("[YT] Browsed %s — %d videos found", url, len(videos))
        return videos

    except subprocess.TimeoutExpired:
        logging.error("[YT] Browse timed out for %s", url)
        return []
    except Exception as e:
        logging.error("[YT] Browse error for %s: %s", url, e)
        return []


def yt_download(url: str, dest_path: str, resolution: str = "1080") -> bool:
    """Download a YouTube video to a specific path. Returns True on success."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    fmt = (
        f"bestvideo[height<={resolution}][vcodec^=avc1]+bestaudio[ext=m4a]"
        f"/bestvideo[height<={resolution}][ext=mp4][vcodec^=avc1]+bestaudio"
        f"/best[height<={resolution}][vcodec^=avc1]"
        f"/bestvideo[height<={resolution}][ext=mp4]+bestaudio[ext=m4a]"
        f"/best[height<={resolution}]/best"
    )
    try:
        cmd = [
            "yt-dlp",
            "--no-playlist",
            "-f", fmt,
            "--merge-output-format", "mp4",
            "-o", dest_path,
            "--no-overwrites",
            url,
        ]
        logging.info("[YT] Downloading %s -> %s", url, dest_path)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)
        if result.returncode == 0:
            logging.info("[YT] Download complete: %s", dest_path)
            return True
        else:
            err = result.stderr[-300:] if result.stderr else "Unknown error"
            logging.error("[YT] Download failed for %s: %s", url, err)
            return False
    except subprocess.TimeoutExpired:
        logging.error("[YT] Download timed out (20 min): %s", url)
        return False
    except Exception as e:
        logging.error("[YT] Download error: %s", e)
        return False


# ── Background pre-cache worker ──

_worker_started = False
_channel_mgr = None


def start_yt_cache_worker(channel_mgr, interval: int = 60):
    """Start a background thread that keeps YouTube videos pre-cached.

    For each channel with YouTube content, ensures the current + next 2
    YouTube entries are downloaded. Cleans up entries that have already played.
    """
    global _worker_started, _channel_mgr
    if _worker_started:
        return
    _worker_started = True
    _channel_mgr = channel_mgr
    t = threading.Thread(target=_cache_worker_loop, args=(interval,), daemon=True)
    t.start()
    logging.info("[YT] Cache worker started (interval=%ds)", interval)


def _cache_worker_loop(interval: int):
    # Wait a bit for app to finish starting
    time.sleep(10)
    while True:
        try:
            _cache_worker_tick()
        except Exception as e:
            logging.error("[YT] Cache worker error: %s", e)
        time.sleep(interval)


def _cache_worker_tick():
    from core.channels import find_schedule_position

    if not _channel_mgr:
        return

    channels = _channel_mgr.list_channels()
    needed_ids = set()

    for ch in channels:
        schedule = ch.get("materialized_schedule", [])
        if not schedule:
            continue

        # Check if this channel has any YouTube entries
        has_yt = any(e.get("type") == "youtube" for e in schedule)
        if not has_yt:
            continue

        # Find current position
        idx, _ = find_schedule_position(ch)
        if idx is None:
            idx = 0

        # Collect current + next 2 YouTube entries
        count = 0
        i = idx
        checked = 0
        while count < 3 and checked < len(schedule):
            entry = schedule[i % len(schedule)]
            if entry.get("type") == "youtube" and entry.get("yt_id"):
                yt_id = entry["yt_id"]
                needed_ids.add(yt_id)
                path = entry["path"]
                # Skip if file exists or a .part file indicates download in progress
                if not os.path.isfile(path) and not any(
                    f.startswith(yt_id) and f.endswith(".part")
                    for f in os.listdir(_cache_dir())
                ):
                    logging.info("[YT] Cache worker: downloading %s", yt_id)
                    yt_download(entry.get("url", ""), path)
                count += 1
            i += 1
            checked += 1

    # Clean up cached files that are no longer needed
    cache = _cache_dir()
    if os.path.isdir(cache):
        for f in os.listdir(cache):
            if not f.endswith(".mp4"):
                continue
            yt_id = f.replace(".mp4", "")
            if yt_id not in needed_ids:
                fp = os.path.join(cache, f)
                try:
                    os.remove(fp)
                    logging.info("[YT] Cache worker: cleaned up %s (no longer needed)", yt_id)
                except OSError:
                    pass
