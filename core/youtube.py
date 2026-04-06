"""YouTube metadata, download, and cache management for ephemeral streaming."""

import json
import logging
import os
import shutil
import subprocess

YT_CACHE_DIR = "/app/data/yt_cache"


def yt_cache_path(yt_id: str) -> str:
    """Deterministic path for a cached YouTube video."""
    return os.path.join(YT_CACHE_DIR, f"{yt_id}.mp4")


def yt_cleanup(yt_id: str):
    """Delete a single cached video file."""
    path = yt_cache_path(yt_id)
    if os.path.isfile(path):
        os.remove(path)
        logging.info("[YT] Cleaned up %s", yt_id)


def yt_cleanup_all():
    """Wipe the entire YouTube cache directory."""
    if os.path.isdir(YT_CACHE_DIR):
        shutil.rmtree(YT_CACHE_DIR, ignore_errors=True)
    os.makedirs(YT_CACHE_DIR, exist_ok=True)
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

            # Use standard thumbnail URL if yt-dlp didn't provide one
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
