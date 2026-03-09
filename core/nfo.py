"""NFO reader and poster finder for media files."""

import os
import logging
import xml.etree.ElementTree as ET


def read_nfo_title(video_path: str) -> str:
    """Read the NFO sidecar for a video file and return a formatted title.

    Returns e.g. "Movie Name (2024)" or "Show Name - S01E05 Episode Title".
    Falls back to the video filename if no NFO is found.
    """
    nfo_path = _find_nfo(video_path)
    if not nfo_path:
        return os.path.splitext(os.path.basename(video_path))[0]

    try:
        tree = ET.parse(nfo_path)
        root = tree.getroot()
        tag = root.tag.lower()

        if tag == "episodedetails":
            showtitle = _text(root, "showtitle") or ""
            title = _text(root, "title") or ""
            season = _text(root, "season") or ""
            episode = _text(root, "episode") or ""
            if showtitle and season and episode:
                se = f"S{int(season):02d}E{int(episode):02d}"
                if title:
                    return f"{showtitle} - {se} {title}"
                return f"{showtitle} - {se}"
            return title or showtitle or os.path.splitext(os.path.basename(video_path))[0]

        elif tag == "movie":
            title = _text(root, "title") or ""
            year = _text(root, "year") or ""
            if title and year:
                return f"{title} ({year})"
            return title or os.path.splitext(os.path.basename(video_path))[0]

        else:
            # Unknown NFO type — try title element
            title = _text(root, "title")
            return title or os.path.splitext(os.path.basename(video_path))[0]

    except Exception as e:
        logging.warning("[NFO] Failed to parse %s: %s", nfo_path, e)
        return os.path.splitext(os.path.basename(video_path))[0]


def find_poster(video_path: str) -> str | None:
    """Find a poster image near the video file.

    Checks (in order):
      1. If path is a directory, check poster.jpg/png directly in it
      2. <stem>-poster.jpg / .png in same dir
      3. poster.jpg / poster.png in same dir
      4. poster.jpg / poster.png in parent dir (for movies in subdirs)
    Returns absolute path or None.
    """
    # Handle directory paths (e.g. show folders, movie folders)
    if os.path.isdir(video_path):
        for name in ("poster.jpg", "poster.png", "folder.jpg"):
            p = os.path.join(video_path, name)
            if os.path.isfile(p):
                return p
        return None

    video_dir = os.path.dirname(video_path)
    stem = os.path.splitext(os.path.basename(video_path))[0]

    candidates = [
        os.path.join(video_dir, f"{stem}-poster.jpg"),
        os.path.join(video_dir, f"{stem}-poster.png"),
        os.path.join(video_dir, "poster.jpg"),
        os.path.join(video_dir, "poster.png"),
    ]
    # Also check parent dir (movie folders often have poster at folder level)
    parent = os.path.dirname(video_dir)
    if parent != video_dir:
        candidates.append(os.path.join(parent, "poster.jpg"))
        candidates.append(os.path.join(parent, "poster.png"))

    for path in candidates:
        if os.path.isfile(path):
            return path
    return None


def _find_nfo(video_path: str) -> str | None:
    """Find <stem>.nfo next to the video file."""
    stem = os.path.splitext(video_path)[0]
    nfo = stem + ".nfo"
    if os.path.isfile(nfo):
        return nfo
    return None


def _text(root, tag: str) -> str | None:
    """Get text content of first matching child element."""
    el = root.find(tag)
    if el is not None and el.text:
        return el.text.strip()
    return None
