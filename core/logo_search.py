"""Logo search via SearxNG sidecar.

Queries the local SearxNG instance for image results, ranks them by a
small heuristic (domain quality, format, URL keywords, resolution), and
returns the top candidates. Picking a candidate downloads it and saves
it as the channel's standard PNG.

The sidecar is reachable at http://localhost:8080 because both channelarr
and SearxNG join the gluetun-shared network namespace — no API keys, no
external dependencies, and outgoing engine requests run through the same
VPN as the rest of the stack.
"""

import io
import logging
import os
import re
import urllib.parse
from typing import Optional

import requests

logger = logging.getLogger(__name__)

SEARXNG_URL = os.getenv("SEARXNG_URL", "http://localhost:8080")
SEARCH_TIMEOUT = 20
DOWNLOAD_TIMEOUT = 20

# Generic, source-agnostic domain hints. Encyclopedic + brand archives
# tend to ship cleaner transparent PNG/SVG logos; stock-photo and
# social-media domains tend to ship watermarked or unrelated content.
_DOMAIN_BONUS = {
    "wikimedia.org": 100,
    "wikipedia.org": 90,
    "brandsoftheworld.com": 70,
    "logos-world.net": 60,
    "1000logos.net": 60,
    "seeklogo.com": 55,
    "logodownload.org": 50,
    "freepnglogos.com": 50,
    "vectorlogo.zone": 60,
    "vector.me": 40,
}
_DOMAIN_PENALTY = {
    "shutterstock.com": -100,
    "alamy.com": -100,
    "istockphoto.com": -100,
    "gettyimages.com": -100,
    "pinterest.com": -50,
    "youtube.com": -40,
    "tiktok.com": -50,
    "facebook.com": -30,
    "instagram.com": -30,
    "reddit.com": -40,
}


def _score(item: dict) -> float:
    img = (item.get("img_src") or "").strip()
    if not img.lower().startswith(("http://", "https://")):
        return -1
    img_l = img.lower()
    title = (item.get("title") or "").lower()
    score = 0.0

    # Format priority: SVG > PNG > JPEG/WebP
    if img_l.endswith(".svg") or "image/svg" in img_l:
        score += 60
    elif img_l.endswith(".png"):
        score += 35
    elif img_l.endswith((".jpg", ".jpeg", ".webp")):
        score += 5

    # URL/title keyword boost — "logo" usually means a real logo
    if "logo" in img_l:
        score += 25
    if "logo" in title:
        score += 15

    # Domain reputation
    try:
        host = urllib.parse.urlparse(img).netloc.lower()
        for d, bonus in _DOMAIN_BONUS.items():
            if d in host:
                score += bonus
                break
        for d, pen in _DOMAIN_PENALTY.items():
            if d in host:
                score += pen
                break
    except Exception:
        pass

    # Resolution + aspect — only when the engine supplied it
    try:
        w = int(item.get("width") or 0)
        h = int(item.get("height") or 0)
        if w and h:
            short = min(w, h)
            if short >= 512:
                score += 30
            elif short >= 256:
                score += 15
            elif short < 96:
                score -= 30
            ratio = w / h
            if 0.6 <= ratio <= 1.7:
                score += 10
    except (ValueError, TypeError):
        pass

    return score


def search(query: str, max_results: int = 8) -> list[dict]:
    """Search SearxNG for `<query> logo` and return ranked candidates.

    Each candidate is {url, thumbnail, title, source, domain, score, width, height}.
    Returns [] on any failure — the UI should show an empty state.
    """
    if not query.strip():
        return []
    params = {
        "q": f"{query.strip()} logo",
        "format": "json",
        "categories": "images",
        "safesearch": "0",
        "language": "en",
    }
    try:
        r = requests.get(f"{SEARXNG_URL}/search", params=params, timeout=SEARCH_TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.warning("[LOGO] search failed for %r: %s", query, e)
        return []

    raw = data.get("results") or []
    scored = []
    for item in raw:
        s = _score(item)
        if s < 0:
            continue
        img = item.get("img_src") or ""
        host = urllib.parse.urlparse(img).netloc
        scored.append({
            "url": img,
            "thumbnail": item.get("thumbnail_src") or img,
            "title": (item.get("title") or "")[:140],
            "source": item.get("engine") or "",
            "domain": host,
            "width": item.get("width"),
            "height": item.get("height"),
            "score": round(s, 1),
        })

    scored.sort(key=lambda c: c["score"], reverse=True)
    # Dedup by (host, basename) so we don't show 5 of the same logo
    seen = set()
    out = []
    for c in scored:
        key = (c["domain"], c["url"].rsplit("/", 1)[-1].split("?", 1)[0])
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
        if len(out) >= max_results:
            break
    return out


def download_to_logo(url: str, dest_path: str) -> tuple[bool, str]:
    """Fetch a candidate URL and save as PNG at dest_path. PNG is preserved
    byte-for-byte; JPEG/WebP/SVG are converted via Pillow / cairosvg so the
    rest of channelarr always sees a single canonical format."""
    if not url.lower().startswith(("http://", "https://")):
        return False, "invalid url scheme"
    try:
        r = requests.get(
            url,
            timeout=DOWNLOAD_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "image/*"},
            allow_redirects=True,
        )
        r.raise_for_status()
        data = r.content
    except Exception as e:
        return False, f"download failed: {e}"

    if not data:
        return False, "empty response"

    if data[:4] == b"\x89PNG":
        with open(dest_path, "wb") as f:
            f.write(data)
        return True, "ok"
    if data[:2] == b"\xff\xd8" or data[:4] == b"RIFF":
        try:
            from PIL import Image
            im = Image.open(io.BytesIO(data))
            if im.mode not in ("RGB", "RGBA"):
                im = im.convert("RGBA")
            im.save(dest_path, format="PNG")
            return True, "ok (converted)"
        except Exception as e:
            return False, f"raster conversion failed: {e}"
    head = data[:200].lstrip().lower()
    if head.startswith(b"<svg") or head.startswith(b"<?xml"):
        try:
            import cairosvg
            cairosvg.svg2png(bytestring=data, write_to=dest_path, output_width=512)
            return True, "ok (svg→png)"
        except Exception as e:
            return False, f"svg conversion failed: {e}"
    return False, f"unrecognized format (magic={data[:8].hex()})"
