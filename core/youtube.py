"""YouTube metadata, download, cache management, and background pre-cache worker."""

import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time

import requests as http_requests

from core.config import get_setting


# ── Cookie management (shared with selenium-uc sidecar) ──

_COOKIES_PATH = "/app/data/yt_cookies.txt"
_COOKIE_REFRESH_MIN_INTERVAL = 300  # seconds — don't hammer selenium-uc
_AUTH_ERROR_MARKERS = (
    "from-browser or --cookies",
    "Sign in to confirm",
    "confirm you're not a bot",
    "cookies for the authentication",
)

_cookie_lock = threading.Lock()
_last_cookie_refresh = 0.0


def _selenium_url() -> str:
    return get_setting("SELENIUM_URL", "http://localhost:4445")


def _has_cookies() -> bool:
    return os.path.isfile(_COOKIES_PATH) and os.path.getsize(_COOKIES_PATH) > 0


def refresh_youtube_cookies(force: bool = False) -> bool:
    """Fetch fresh YouTube cookies from the selenium-uc sidecar.

    Rate-limited to once per _COOKIE_REFRESH_MIN_INTERVAL unless force=True.
    Returns True if cookies were successfully refreshed (or were already fresh).
    """
    global _last_cookie_refresh
    with _cookie_lock:
        now = time.time()
        if not force and (now - _last_cookie_refresh) < _COOKIE_REFRESH_MIN_INTERVAL:
            logging.debug("[YT] Cookie refresh skipped (cooldown)")
            return _has_cookies()

        try:
            url = f"{_selenium_url()}/cookies/youtube"
            logging.info("[YT] Refreshing YouTube cookies via selenium-uc")
            resp = http_requests.post(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                logging.error("[YT] Cookie refresh failed: %s", data.get("error"))
                return False
            netscape = data.get("netscape") or ""
            if not netscape.strip():
                logging.error("[YT] Cookie refresh returned empty cookies")
                return False

            os.makedirs(os.path.dirname(_COOKIES_PATH), exist_ok=True)
            tmp = _COOKIES_PATH + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(netscape)
            os.replace(tmp, _COOKIES_PATH)
            _last_cookie_refresh = now
            logging.info("[YT] Cookies refreshed (%d entries)", data.get("count", 0))
            return True
        except Exception as e:
            logging.error("[YT] Cookie refresh error: %s", e)
            return False


def _cookies_for_invocation() -> str | None:
    """Return a tmp copy of the cookies file for one yt-dlp invocation, or None.

    yt-dlp rewrites its --cookies file in place; using a per-invocation copy
    avoids races between concurrent calls (browse, duration, download).
    """
    if not _has_cookies():
        return None
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".txt", prefix="ytcookies_")
        os.close(fd)
        shutil.copy(_COOKIES_PATH, tmp_path)
        return tmp_path
    except Exception as e:
        logging.warning("[YT] Failed to prepare cookies tmp: %s", e)
        return None


def _cleanup_cookies_tmp(path: str | None):
    if not path:
        return
    try:
        os.remove(path)
    except OSError:
        pass


def _is_auth_error(stderr: str) -> bool:
    if not stderr:
        return False
    return any(marker in stderr for marker in _AUTH_ERROR_MARKERS)


# ── Failure tracking for cache worker backoff ──

_failure_tracker: dict[str, tuple[int, float]] = {}  # yt_id -> (count, last_fail_ts)
_failure_lock = threading.Lock()
_FAILURE_COOLDOWN = 600  # 10 minutes
_FAILURE_THRESHOLD = 2   # skip after this many failures inside the cooldown window


def _record_yt_failure(yt_id: str):
    if not yt_id:
        return
    with _failure_lock:
        count, _ = _failure_tracker.get(yt_id, (0, 0.0))
        _failure_tracker[yt_id] = (count + 1, time.time())


def _record_yt_success(yt_id: str):
    if not yt_id:
        return
    with _failure_lock:
        _failure_tracker.pop(yt_id, None)


def _should_skip_yt(yt_id: str) -> bool:
    if not yt_id:
        return False
    with _failure_lock:
        entry = _failure_tracker.get(yt_id)
        if not entry:
            return False
        count, last = entry
        if (time.time() - last) > _FAILURE_COOLDOWN:
            # Cooldown expired — give it another chance
            _failure_tracker.pop(yt_id, None)
            return False
        return count >= _FAILURE_THRESHOLD


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


def _run_ytdlp(args: list[str], timeout: int) -> subprocess.CompletedProcess:
    """Run yt-dlp with cookies attached, retrying once on auth error after refresh."""
    cookies_tmp = _cookies_for_invocation()
    try:
        cmd = ["yt-dlp"]
        if cookies_tmp:
            cmd.extend(["--cookies", cookies_tmp])
        cmd.extend(args)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

        if result.returncode != 0 and _is_auth_error(result.stderr):
            logging.warning("[YT] Auth error detected, refreshing cookies and retrying")
            _cleanup_cookies_tmp(cookies_tmp)
            cookies_tmp = None
            if refresh_youtube_cookies(force=True):
                cookies_tmp = _cookies_for_invocation()
                cmd = ["yt-dlp"]
                if cookies_tmp:
                    cmd.extend(["--cookies", cookies_tmp])
                cmd.extend(args)
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result
    finally:
        _cleanup_cookies_tmp(cookies_tmp)


def yt_get_duration(url: str) -> float:
    """Get video duration in seconds without downloading."""
    try:
        result = _run_ytdlp(["--print", "duration", "--no-download", url], timeout=30)
        if result.returncode == 0:
            return float(result.stdout.strip())
        else:
            err = result.stderr[-300:] if result.stderr else "Unknown error"
            logging.warning("[YT] Duration fetch failed for %s: %s", url, err)
    except (subprocess.TimeoutExpired, ValueError) as e:
        logging.warning("[YT] Duration fetch failed for %s: %s", url, e)
    return 0.0


def yt_browse(url: str) -> list[dict]:
    """List videos in a YouTube channel, playlist, or single video URL.

    Returns list of {yt_id, url, title, duration, thumbnail}.
    """
    try:
        result = _run_ytdlp(
            [
                "--flat-playlist",
                "--print", "%(id)s\t%(title)s\t%(duration)s\t%(thumbnails.-1.url)s",
                "--no-download",
                url,
            ],
            timeout=60,
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


def yt_download(url: str, dest_path: str, resolution: str = "1080", yt_id: str | None = None) -> bool:
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
        args = [
            "--no-playlist",
            "-f", fmt,
            "--merge-output-format", "mp4",
            "-o", dest_path,
            "--no-overwrites",
            url,
        ]
        logging.info("[YT] Downloading %s -> %s", url, dest_path)
        result = _run_ytdlp(args, timeout=1200)
        if result.returncode == 0:
            logging.info("[YT] Download complete: %s", dest_path)
            _record_yt_success(yt_id or "")
            return True
        else:
            err = result.stderr[-300:] if result.stderr else "Unknown error"
            logging.error("[YT] Download failed for %s: %s", url, err)
            _record_yt_failure(yt_id or "")
            return False
    except subprocess.TimeoutExpired:
        logging.error("[YT] Download timed out (20 min): %s", url)
        _record_yt_failure(yt_id or "")
        return False
    except Exception as e:
        logging.error("[YT] Download error: %s", e)
        _record_yt_failure(yt_id or "")
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
    # Prime the cookie jar from selenium-uc on startup so first downloads have auth
    try:
        refresh_youtube_cookies(force=True)
    except Exception as e:
        logging.warning("[YT] Initial cookie refresh failed: %s", e)
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
    consecutive_failures = 0
    aborted = False

    for ch in channels:
        if aborted:
            break
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
                already_present = os.path.isfile(path) or any(
                    f.startswith(yt_id) and f.endswith(".part")
                    for f in os.listdir(_cache_dir())
                )
                if not already_present:
                    if _should_skip_yt(yt_id):
                        logging.info("[YT] Cache worker: skipping %s (in failure cooldown)", yt_id)
                    else:
                        logging.info("[YT] Cache worker: downloading %s", yt_id)
                        ok = yt_download(entry.get("url", ""), path, yt_id=yt_id)
                        if ok:
                            consecutive_failures = 0
                        else:
                            consecutive_failures += 1
                            if consecutive_failures >= 3:
                                logging.warning(
                                    "[YT] Cache worker: 3 consecutive failures, aborting tick "
                                    "(will retry next interval)"
                                )
                                aborted = True
                                break
                count += 1
            i += 1
            checked += 1

    # Clean up cached files that are no longer needed.
    # Skip cleanup if the tick aborted early — needed_ids would be incomplete
    # and we'd risk deleting files still wanted by un-walked channels.
    if aborted:
        return
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
