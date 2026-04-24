"""Bump clip discovery, random selection, YouTube downloading, and TS pre-cache."""

import os
import random
import logging
import threading
import subprocess

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".ts", ".webm", ".flv", ".wmv"}
CACHE_SUFFIX = ".cache.ts"


class BumpManager:
    def __init__(self, get_setting_fn):
        self._get = get_setting_fn
        self._clips = {}  # folder_name -> [file_paths]

    def scan(self) -> dict:
        bumps_path = self._get("BUMPS_PATH", "/bumps")
        self._clips = {}

        if not os.path.isdir(bumps_path):
            logging.warning("[BUMPS] Bumps path does not exist: %s", bumps_path)
            return self._clips

        for entry in os.scandir(bumps_path):
            if entry.is_dir():
                folder_name = entry.name
                clips = []
                for root, _, files in os.walk(entry.path):
                    for f in files:
                        if f.endswith(CACHE_SUFFIX):
                            continue
                        if os.path.splitext(f)[1].lower() in VIDEO_EXTS:
                            clips.append(os.path.join(root, f))
                if clips:
                    clips.sort()
                    self._clips[folder_name] = clips
                    logging.info("[BUMPS] Found %d clips in %s/", len(clips), folder_name)

        # Also pick up clips directly in bumps_path (no subfolder)
        root_clips = []
        for f in os.listdir(bumps_path):
            if f.endswith(CACHE_SUFFIX):
                continue
            fp = os.path.join(bumps_path, f)
            if os.path.isfile(fp) and os.path.splitext(f)[1].lower() in VIDEO_EXTS:
                root_clips.append(fp)
        if root_clips:
            root_clips.sort()
            self._clips["_root"] = root_clips

        total = sum(len(v) for v in self._clips.values())
        logging.info("[BUMPS] Scan complete: %d folders, %d total clips", len(self._clips), total)
        return self._clips

    def get_all(self) -> dict:
        return dict(self._clips)

    def get_folders(self) -> list:
        return sorted(self._clips.keys())

    def get_clips(self, folder: str) -> list:
        """Return all clip paths for a folder (for shuffle-cycling)."""
        return list(self._clips.get(folder, []))

    def pick_random(self, folder: str, count: int = 1) -> list:
        clips = self._clips.get(folder, [])
        if not clips:
            return []
        return random.sample(clips, min(count, len(clips)))

    def delete(self, filepath: str) -> bool:
        """Delete a bump clip from disk and remove from index."""
        bumps_path = self._get("BUMPS_PATH", "/bumps")
        real = os.path.realpath(filepath)
        if not real.startswith(os.path.realpath(bumps_path)):
            logging.warning("[BUMPS] Delete rejected — path outside bumps dir: %s", filepath)
            return False
        if not os.path.isfile(real):
            logging.warning("[BUMPS] Delete target not found: %s", filepath)
            return False
        os.remove(real)
        cache_path = real + CACHE_SUFFIX
        if os.path.isfile(cache_path):
            os.remove(cache_path)
        logging.info("[BUMPS] Deleted %s", real)
        # Remove from in-memory index
        for folder, clips in self._clips.items():
            if real in [os.path.realpath(c) for c in clips]:
                self._clips[folder] = [c for c in clips if os.path.realpath(c) != real]
                if not self._clips[folder]:
                    del self._clips[folder]
                break
        return True

    def summary(self) -> dict:
        return {
            "folders": {k: len(v) for k, v in sorted(self._clips.items())},
            "total": sum(len(v) for v in self._clips.values()),
        }

    # ─── TS Pre-Cache ───

    def precache_bumps(self, width=1280, height=720, fps=30, preset="veryfast",
                       profile="main", audio_bitrate="192k", audio_rate=48000):
        """Pre-encode all bumps to MPEG-TS at target resolution.

        Runs in background. Cached files are stored alongside originals as
        {path}.cache.ts. The resolved-channel feeder uses these to avoid
        the sub-ffmpeg decode+scale step during playback.
        """
        t = threading.Thread(target=self._do_precache, daemon=True,
                             args=(width, height, fps, preset, profile,
                                   audio_bitrate, audio_rate),
                             name="bump-precache")
        t.start()

    def _do_precache(self, width, height, fps, preset, profile,
                     audio_bitrate, audio_rate):
        all_clips = []
        for clips in self._clips.values():
            all_clips.extend(clips)
        if not all_clips:
            return

        cached = 0
        skipped = 0
        for clip_path in all_clips:
            cache_path = clip_path + CACHE_SUFFIX
            if os.path.isfile(cache_path):
                if os.path.getmtime(cache_path) >= os.path.getmtime(clip_path):
                    skipped += 1
                    continue
            vf = (
                f"scale=w={width}:h={height}:force_original_aspect_ratio=decrease,"
                f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,setsar=1"
            )
            cmd = [
                "ffmpeg", "-y",
                "-loglevel", "error",
                "-i", clip_path,
                "-map", "0:v:0?", "-map", "0:a:0?",
                "-vf", vf,
                "-r", str(fps),
                "-c:v", "libx264",
                "-preset", preset,
                "-profile:v", profile,
                "-pix_fmt", "yuv420p",
                "-c:a", "aac",
                "-b:a", audio_bitrate,
                "-ar", str(audio_rate),
                "-ac", "2",
                "-f", "mpegts",
                cache_path,
            ]
            try:
                result = subprocess.run(cmd, capture_output=True, timeout=300)
                if result.returncode == 0:
                    cached += 1
                else:
                    err = result.stderr.decode("utf-8", errors="replace")[-200:]
                    logging.warning("[BUMPS] Pre-cache failed for %s: %s",
                                    os.path.basename(clip_path), err)
            except subprocess.TimeoutExpired:
                logging.warning("[BUMPS] Pre-cache timed out for %s",
                                os.path.basename(clip_path))
            except Exception as e:
                logging.warning("[BUMPS] Pre-cache error for %s: %s",
                                os.path.basename(clip_path), e)

        logging.info("[BUMPS] Pre-cache complete: %d encoded, %d already cached, %d total",
                     cached, skipped, len(all_clips))

    # ─── YouTube Download ───

    def download_url(self, url: str, folder: str, resolution: str = "1080", callback=None):
        """Download a video from URL via yt-dlp into a bump subfolder. Runs in background."""
        bumps_path = self._get("BUMPS_PATH", "/bumps")
        dest_dir = os.path.join(bumps_path, folder)
        os.makedirs(dest_dir, exist_ok=True)

        t = threading.Thread(target=self._do_download, args=(url, dest_dir, folder, resolution, callback), daemon=True)
        t.start()
        return True

    def _do_download(self, url: str, dest_dir: str, folder: str, resolution: str, callback):
        dl_id = url.split("=")[-1][:11] if "=" in url else url.split("/")[-1][:11]
        logging.info("[BUMPS] Downloading %s -> %s/ (max %sp)", url, folder, resolution)

        # Cap resolution: prefer mp4 up to the chosen height
        fmt = f"bestvideo[height<={resolution}][ext=mp4]+bestaudio[ext=m4a]/best[height<={resolution}][ext=mp4]/best[height<={resolution}]/best"

        try:
            cmd = [
                "yt-dlp",
                "--no-playlist",
                "-f", fmt,
                "--merge-output-format", "mp4",
                "-o", os.path.join(dest_dir, "%(title)s.%(ext)s"),
                "--no-overwrites",
                url,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode == 0:
                logging.info("[BUMPS] Download complete: %s -> %s/", url, folder)
                self.scan()
                self.precache_bumps()
                if callback:
                    callback(True, f"Downloaded to {folder}/")
            else:
                err = result.stderr[-300:] if result.stderr else "Unknown error"
                logging.error("[BUMPS] yt-dlp failed: %s", err)
                if callback:
                    callback(False, err)
        except subprocess.TimeoutExpired:
            logging.error("[BUMPS] Download timed out: %s", url)
            if callback:
                callback(False, "Download timed out (10 min limit)")
        except Exception as e:
            logging.error("[BUMPS] Download error: %s", e)
            if callback:
                callback(False, str(e))

    def get_downloads_status(self) -> list:
        """Check for any active download threads."""
        active = threading.active_count()
        return {"active_threads": active}
