"""Stream proxy for resolver-created channels.

Serves `/live-resolved/{manifest_id}.m3u8` as a rewritten HLS playlist pointing
at channelarr's own `/live-resolved/proxy?url=...` byte proxy. Includes a 403/401
safety net that triggers a synchronous re-resolve and retries once — this is how
expired CDN tokens are handled transparently.

Separate from the existing `hls.py` router (which serves channelarr's own
FFmpeg-encoded content for local media + YouTube) because the URL pattern and
the serving model are different.
"""

import logging
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, quote

import requests as http_requests
from fastapi import APIRouter, Query, HTTPException
from starlette.responses import Response, StreamingResponse, RedirectResponse

from core.database import get_session
from core.models.manifest import Manifest
from core.models.channel import Channel

logger = logging.getLogger(__name__)
router = APIRouter()


def _find_transcode_channel_for_manifest(manifest_id: str) -> str | None:
    """Return the channel_id of any active transcode-mediated channel that
    references this manifest, or None if no such channel exists. Used by the
    legacy /live-resolved/{mid}.m3u8 endpoint to transparently route stale
    clients to the new transcoded HLS pipeline."""
    try:
        with get_session() as session:
            row = (
                session.query(Channel.id)
                .filter(Channel.manifest_id == manifest_id)
                .filter(Channel.type == "resolved")
                .filter(Channel.transcode_mediated == True)  # noqa: E712
                .first()
            )
        return row[0] if row else None
    except Exception as e:
        logger.debug("[RESOLVED-STREAM] transcode lookup failed: %s", e)
        return None

MANIFEST_ID_RE = re.compile(r"^[a-f0-9-]+$")
CHUNK = 16384
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _touch_access(manifest_id: str):
    """Update last_accessed_at for demand-driven refresh tracking. Best-effort."""
    try:
        with get_session() as session:
            session.query(Manifest).filter_by(id=manifest_id).update(
                {"last_accessed_at": datetime.now(timezone.utc)}
            )
    except Exception as e:
        logger.debug("[RESOLVED-STREAM] touch access failed: %s", e)


def _refresh_and_get_url(mid: str) -> str | None:
    """Trigger a synchronous refresh of a resolved manifest and return its new URL."""
    from core.resolver.manifest_resolver import ManifestResolverService
    result = ManifestResolverService.refresh_manifest(mid)
    if not result.get("ok"):
        logger.warning("[RESOLVED-STREAM] sync refresh failed for %s: %s", mid, result.get("error"))
        return None
    with get_session() as session:
        row = session.query(Manifest.url).filter(Manifest.id == mid).first()
    return row[0] if row else None


def _proxy_m3u8(mid: str, url: str, _retried: bool = False):
    try:
        r = http_requests.get(url, headers={"User-Agent": UA}, timeout=15, allow_redirects=True)
        if r.status_code in (401, 403) and not _retried:
            logger.warning("[RESOLVED-STREAM] upstream %s for %s — triggering sync refresh", r.status_code, mid)
            new_url = _refresh_and_get_url(mid)
            if new_url and new_url != url:
                return _proxy_m3u8(mid, new_url, _retried=True)
        r.raise_for_status()
    except http_requests.HTTPError as e:
        logger.error("[RESOLVED-STREAM] proxy m3u8 failed: %s", e)
        raise HTTPException(status_code=502)
    except Exception as e:
        logger.error("[RESOLVED-STREAM] proxy m3u8 failed: %s", e)
        raise HTTPException(status_code=502)

    lines = []
    for line in r.text.splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            a = urljoin(r.url, s)
            if any(s.endswith(x) for x in (".m3u8", ".m3u")):
                s = f"/live-resolved/{mid}.m3u8?src={quote(a, safe='')}"
            else:
                s = f"/live-resolved/proxy?url={quote(a, safe='')}"
        lines.append(s)
    return Response("\n".join(lines) + "\n", media_type="application/vnd.apple.mpegurl")


def _proxy_bytes(url: str):
    try:
        r = http_requests.get(url, headers={"User-Agent": UA}, stream=True, timeout=15, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        logger.error("[RESOLVED-STREAM] proxy bytes failed: %s", e)
        raise HTTPException(status_code=502)

    def gen():
        try:
            for c in r.iter_content(chunk_size=CHUNK):
                yield c
        except Exception:
            pass
        finally:
            r.close()

    return StreamingResponse(gen(), media_type=r.headers.get("Content-Type", "video/mp2t"))


@router.get("/live-resolved/{manifest_id}.m3u8")
def resolved_playlist(manifest_id: str, src: str = Query(default=None)):
    if not MANIFEST_ID_RE.match(manifest_id):
        raise HTTPException(status_code=400)
    # Nested playlist fetch (when HLS.js follows a variant) keeps the same mid
    if src:
        return _proxy_m3u8(manifest_id, src)

    # Safety net for stale clients: if any active channel using this manifest
    # has transcode_mediated enabled, redirect to the unified /live/ endpoint
    # instead of serving the raw passthrough proxy. Means an IPTV client
    # (Jellyfin/Plex/etc) holding a cached /live-resolved/ URL will pick up
    # the transcoded stream automatically without re-importing the M3U.
    transcode_channel_id = _find_transcode_channel_for_manifest(manifest_id)
    if transcode_channel_id:
        return RedirectResponse(
            url=f"/live/{transcode_channel_id}/stream.m3u8",
            status_code=302,
        )

    with get_session() as session:
        row = session.query(Manifest.url).filter(Manifest.id == manifest_id, Manifest.active == True).first()
    if not row:
        raise HTTPException(status_code=404)
    url = row[0]
    _touch_access(manifest_id)
    return _proxy_m3u8(manifest_id, url)


@router.get("/live-resolved/proxy")
def resolved_proxy(url: str = Query(default=None)):
    if not url:
        raise HTTPException(status_code=400)
    return _proxy_bytes(url)
