"""Media discovery: filesystem-only scanning."""

import os
import re
import logging

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".ts", ".webm"}

_SE_RE = re.compile(r"[Ss](\d+)[Ee](\d+)")


def _count_videos(directory: str) -> int:
    """Count video files recursively under a directory."""
    count = 0
    for _, _, files in os.walk(directory):
        for f in files:
            if os.path.splitext(f)[1].lower() in VIDEO_EXTS:
                count += 1
    return count


class MediaLibrary:
    def __init__(self, get_setting_fn):
        self._get = get_setting_fn

    def get_movies(self) -> list:
        media_path = self._get("MEDIA_PATH", "/media")
        movies_dir = os.path.join(media_path, "movies")
        if not os.path.isdir(movies_dir):
            return []
        movies = []
        for root, _, files in os.walk(movies_dir):
            for f in files:
                if os.path.splitext(f)[1].lower() in VIDEO_EXTS:
                    fp = os.path.join(root, f)
                    name = os.path.splitext(f)[0]
                    movies.append({
                        "id": None,
                        "title": name,
                        "year": 0,
                        "runtime": 0,
                        "genres": [],
                        "path": fp,
                        "size": os.path.getsize(fp),
                    })
        movies.sort(key=lambda x: x["title"].lower())
        return movies

    def get_shows(self) -> list:
        media_path = self._get("MEDIA_PATH", "/media")
        tv_dir = os.path.join(media_path, "tv")
        if not os.path.isdir(tv_dir):
            logging.warning("[MEDIA] TV directory not found: %s", tv_dir)
            return []
        shows = []
        for entry in sorted(os.scandir(tv_dir), key=lambda e: e.name.lower()):
            if entry.is_dir():
                ep_count = _count_videos(entry.path)
                shows.append({
                    "id": None,
                    "title": entry.name,
                    "year": 0,
                    "genres": [],
                    "seasons": 0,
                    "episodeCount": ep_count,
                    "path": entry.path,
                })
        logging.info("[MEDIA] Found %d shows under %s", len(shows), tv_dir)
        return shows

    def get_episodes(self, show_path: str) -> list:
        if not os.path.isdir(show_path):
            logging.warning("[MEDIA] Show path not a directory: %s", show_path)
            return []
        episodes = []
        for root, _, files in os.walk(show_path):
            for f in files:
                if os.path.splitext(f)[1].lower() not in VIDEO_EXTS:
                    continue
                fp = os.path.join(root, f)
                name = os.path.splitext(f)[0]
                m = _SE_RE.search(f)
                season = int(m.group(1)) if m else 0
                episode = int(m.group(2)) if m else 0
                label = f"S{season:02d}E{episode:02d} - {name}" if m else name
                episodes.append({
                    "id": None,
                    "season": season,
                    "episode": episode,
                    "title": name,
                    "runtime": 0,
                    "path": fp,
                    "label": label,
                })
        episodes.sort(key=lambda x: (x["season"], x["episode"], x["title"].lower()))
        logging.info("[MEDIA] Found %d episodes in %s", len(episodes), show_path)
        return episodes
