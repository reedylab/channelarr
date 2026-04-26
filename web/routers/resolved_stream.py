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

from core.config import get_setting
from core.database import get_session
from core.models.manifest import Manifest, Capture
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


def _find_tab_proxy_channel_for_manifest(manifest_id: str) -> str | None:
    """Return the channel_id of a tab_proxy-mode channel referencing this
    manifest, or None. Used to route /live-resolved/{mid}.m3u8 requests
    into the nodriver tab subsystem when the channel's encoder_mode is
    tab_proxy."""
    try:
        with get_session() as session:
            row = (
                session.query(Channel.id)
                .filter(Channel.manifest_id == manifest_id)
                .filter(Channel.type == "resolved")
                .filter(Channel.encoder_mode == "tab_proxy")
                .first()
            )
        return row[0] if row else None
    except Exception as e:
        logger.debug("[RESOLVED-STREAM] tab_proxy lookup failed: %s", e)
        return None


def _get_page_url(manifest_id: str) -> str | None:
    """Look up the original page_url for a manifest (from its Capture row)."""
    try:
        with get_session() as session:
            row = (
                session.query(Capture.page_url)
                .join(Manifest, Manifest.capture_id == Capture.id)
                .filter(Manifest.id == manifest_id)
                .first()
            )
        return row[0] if row else None
    except Exception as e:
        logger.debug("[RESOLVED-STREAM] page_url lookup failed: %s", e)
        return None


def _sidecar_url() -> str:
    return get_setting("SELENIUM_URL", "http://localhost:4445")


def _rewrite_tab_playlist(body: str, src_url: str, channel_id: str) -> str:
    """Rewrite playlist lines so nested playlists go through /tab-playlist
    and segments go through /tab-segment. URLs are resolved absolute
    against src_url (the actual upstream playlist URL Chrome fetched)."""
    lines = []
    for line in body.splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            abs_url = urljoin(src_url, s)
            enc = quote(abs_url, safe="")
            if any(s.split("?")[0].endswith(x) for x in (".m3u8", ".m3u")):
                s = f"/live-resolved/tab-playlist?cid={channel_id}&src={enc}"
            else:
                s = f"/live-resolved/tab-segment?cid={channel_id}&url={enc}"
        lines.append(s)
    return "\n".join(lines) + "\n"


def _ensure_tab_open(channel_id: str, manifest_id: str) -> None:
    """Open (or reuse) the channel's tab. Idempotent on the sidecar side.

    Looks up a plugin-declared TAB_PROXY_CONFIG for the page_url's
    domain and forwards it to the sidecar. All source-specific behavior
    (click sequences, custom JS, etc.) lives in scrapers/*.py — the
    sidecar is source-agnostic."""
    page_url = _get_page_url(manifest_id)
    if not page_url:
        raise HTTPException(status_code=404, detail="no page_url for manifest")

    # Optional plugin config — None if no scraper declares this domain
    try:
        from core.tab_proxy_config import get_tab_proxy_config
        plugin_cfg = get_tab_proxy_config(page_url)
    except Exception as e:
        logger.debug("[RESOLVED-STREAM] tab-proxy config lookup failed: %s", e)
        plugin_cfg = None
    if plugin_cfg:
        logger.info("[RESOLVED-STREAM] using %s plugin config for %s",
                    plugin_cfg.get("_plugin"), page_url)

    # 120s sidecar timeout, 150s HTTP timeout — generous for webm mode
    # which has to wait for video playback to start.
    payload = {"url": page_url, "tab_id": channel_id, "timeout": 120}
    if plugin_cfg:
        payload["config"] = plugin_cfg

    sidecar = _sidecar_url()
    try:
        r = http_requests.post(
            f"{sidecar}/tab/open",
            json=payload,
            timeout=150,
        )
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise HTTPException(status_code=502, detail=f"tab open failed: {data.get('error')}")
    except http_requests.RequestException as e:
        logger.error("[RESOLVED-STREAM] tab open error: %s", e)
        raise HTTPException(status_code=502, detail="sidecar unreachable")


def _fetch_tab_playlist(channel_id: str, url: str | None) -> tuple[str, str]:
    """Pull a playlist body from the sidecar. Returns (body, src_url)."""
    sidecar = _sidecar_url()
    params = {"url": url} if url else {}
    try:
        r = http_requests.get(
            f"{sidecar}/tab/{channel_id}/playlist", params=params, timeout=15,
        )
        r.raise_for_status()
    except http_requests.RequestException as e:
        logger.error("[RESOLVED-STREAM] tab playlist fetch failed: %s", e)
        raise HTTPException(status_code=502, detail="playlist fetch failed")
    return r.text, r.headers.get("X-Source-Url") or url or ""


def _tab_proxy_playlist(channel_id: str, manifest_id: str):
    """Open (or reuse) the channel's tab. If the source plugin opted
    into capture-stream mode (stream_type='webm'), proxy the sidecar's
    WebM feed directly. Otherwise fetch the upstream HLS playlist from
    the tab and rewrite segment/variant URLs through channelarr."""
    _ensure_tab_open(channel_id, manifest_id)

    # Check plugin config for stream_type. If webm, skip HLS handling
    # entirely and stream the in-tab captureStream output.
    try:
        from core.tab_proxy_config import get_tab_proxy_config
        page_url = _get_page_url(manifest_id)
        plugin_cfg = get_tab_proxy_config(page_url) if page_url else None
    except Exception:
        plugin_cfg = None
    if plugin_cfg and plugin_cfg.get("stream_type") == "webm":
        sidecar = _sidecar_url()
        try:
            r = http_requests.get(
                f"{sidecar}/tab/{channel_id}/capture-feed",
                timeout=60, stream=True,
            )
            r.raise_for_status()
        except http_requests.RequestException as e:
            logger.error("[RESOLVED-STREAM] capture-feed fetch failed: %s", e)
            raise HTTPException(status_code=502)
        _touch_access(manifest_id)

        def gen():
            try:
                for chunk in r.iter_content(chunk_size=CHUNK):
                    yield chunk
            except Exception:
                pass
            finally:
                r.close()

        return StreamingResponse(
            gen(),
            media_type=r.headers.get("Content-Type", "video/webm"),
        )

    # Default HLS path — rewrite segment/variant URLs to tab-segment /
    # tab-playlist endpoints
    body, src_url = _fetch_tab_playlist(channel_id, None)
    if not src_url:
        logger.warning("[RESOLVED-STREAM] tab playlist missing X-Source-Url")
        return Response(body, media_type="application/vnd.apple.mpegurl")
    rewritten = _rewrite_tab_playlist(body, src_url, channel_id)
    _touch_access(manifest_id)
    return Response(rewritten, media_type="application/vnd.apple.mpegurl")

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


def _get_source_domain(mid: str) -> str:
    """Look up the source_domain for a manifest."""
    try:
        with get_session() as session:
            row = session.query(Manifest.source_domain).filter_by(id=mid).first()
        return row[0] if row and row[0] else ""
    except Exception:
        return ""


def _get_cookies(mid: str) -> list:
    """Look up stored session cookies for a manifest."""
    try:
        with get_session() as session:
            row = session.query(Manifest.cookies).filter_by(id=mid).first()
        return row[0] if row and row[0] else []
    except Exception:
        return []


def _build_cookie_header(cookies: list) -> str:
    """Convert stored cookie list to Cookie header string."""
    if not cookies:
        return ""
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies if c.get("name"))


def _build_headers(source_domain: str, cookies: list = None) -> dict:
    """Build request headers for upstream fetches."""
    headers = {"User-Agent": UA}
    if source_domain:
        headers["Referer"] = f"https://{source_domain}/"
        headers["Origin"] = f"https://{source_domain}"
    cookie_str = _build_cookie_header(cookies or [])
    if cookie_str:
        headers["Cookie"] = cookie_str
    return headers


def _proxy_m3u8(mid: str, url: str, source_domain: str = "", _retried: bool = False):
    cookies = _get_cookies(mid)
    headers = _build_headers(source_domain, cookies)
    try:
        r = http_requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        if r.status_code in (401, 403, 404) and not _retried:
            logger.warning("[RESOLVED-STREAM] upstream %s for %s — triggering sync refresh", r.status_code, mid)
            new_url = _refresh_and_get_url(mid)
            if new_url and new_url != url:
                return _proxy_m3u8(mid, new_url, source_domain=source_domain, _retried=True)
        r.raise_for_status()
    except http_requests.HTTPError as e:
        logger.error("[RESOLVED-STREAM] proxy m3u8 failed: %s", e)
        raise HTTPException(status_code=502)
    except Exception as e:
        logger.error("[RESOLVED-STREAM] proxy m3u8 failed: %s", e)
        raise HTTPException(status_code=502)

    ref_param = f"&ref={quote(source_domain, safe='')}" if source_domain else ""
    mid_param = f"&mid={mid}"

    # Tags that carry a URI="..." attribute referencing a resource the player
    # will fetch separately (AES key, init segment, etc). The HLS spec says
    # these URIs are resolved relative to the playlist, so the same urljoin
    # logic used for segments applies. Without this rewriting the client
    # tries to fetch the key from the original CDN with no cookies → 401.
    _KEY_URI_TAGS = ("#EXT-X-KEY:", "#EXT-X-SESSION-KEY:", "#EXT-X-MAP:")
    _URI_RE = re.compile(r'URI="([^"]+)"')

    def _rewrite_uri_attr(line: str) -> str:
        m = _URI_RE.search(line)
        if not m:
            return line
        absolute = urljoin(r.url, m.group(1))
        proxied = f"/live-resolved/proxy.ts?url={quote(absolute, safe='')}{ref_param}{mid_param}"
        return line[:m.start(1)] + proxied + line[m.end(1):]

    lines = []
    for line in r.text.splitlines():
        s = line.strip()
        if s.startswith("#"):
            if any(s.startswith(t) for t in _KEY_URI_TAGS):
                s = _rewrite_uri_attr(s)
        elif s:
            a = urljoin(r.url, s)
            if any(s.endswith(x) for x in (".m3u8", ".m3u")):
                s = f"/live-resolved/{mid}.m3u8?src={quote(a, safe='')}"
            else:
                s = f"/live-resolved/proxy.ts?url={quote(a, safe='')}{ref_param}{mid_param}"
        lines.append(s)
    return Response("\n".join(lines) + "\n", media_type="application/vnd.apple.mpegurl")


def _proxy_bytes(url: str, source_domain: str = "", cookies: list = None):
    headers = _build_headers(source_domain, cookies)
    try:
        r = http_requests.get(url, headers=headers, stream=True, timeout=15, allow_redirects=True)
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

    source_domain = _get_source_domain(manifest_id)

    # Nested playlist fetch (when HLS.js follows a variant) keeps the same mid
    if src:
        return _proxy_m3u8(manifest_id, src, source_domain=source_domain)

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

    # Tab-proxy mode — session-locked CDNs (NTV). Serve the playlist out
    # of a persistent browser tab in the nodriver sidecar instead of
    # replaying the stored m3u8 URL.
    tab_channel_id = _find_tab_proxy_channel_for_manifest(manifest_id)
    if tab_channel_id:
        return _tab_proxy_playlist(tab_channel_id, manifest_id)

    with get_session() as session:
        row = session.query(Manifest.url).filter(Manifest.id == manifest_id, Manifest.active == True).first()
    if not row:
        raise HTTPException(status_code=404)
    url = row[0]
    _touch_access(manifest_id)
    return _proxy_m3u8(manifest_id, url, source_domain=source_domain)


@router.get("/live-resolved/proxy")
@router.get("/live-resolved/proxy.ts")
def resolved_proxy(url: str = Query(default=None), ref: str = Query(default=""), mid: str = Query(default="")):
    """Proxy a single upstream resource (segment, key, init segment) with
    cookies attached. The .ts alias exists so ffmpeg's allowed_segment_
    extensions check sees a media-like extension on the path; ffmpeg
    rejects URLs whose extension isn't whitelisted, and many sources serve
    encrypted segments under disguised non-media extensions. The original
    extensionless route stays for backwards-compatible clients."""
    if not url:
        raise HTTPException(status_code=400)
    cookies = _get_cookies(mid) if mid else []
    return _proxy_bytes(url, source_domain=ref, cookies=cookies)


@router.get("/live-resolved/tab-playlist")
def tab_playlist(cid: str = Query(...), src: str = Query(...)):
    """Fetch a nested/variant HLS playlist out of the channel's browser tab
    and rewrite its segment URLs to route back through channelarr."""
    body, src_url = _fetch_tab_playlist(cid, src)
    rewritten = _rewrite_tab_playlist(body, src_url or src, cid)
    return Response(rewritten, media_type="application/vnd.apple.mpegurl")


@router.get("/live-resolved/tab-segment")
def tab_segment(cid: str = Query(...), url: str = Query(...)):
    """Stream an HLS segment out of the channel's browser tab cache."""
    sidecar = _sidecar_url()
    try:
        r = http_requests.get(
            f"{sidecar}/tab/{cid}/segment",
            params={"url": url},
            timeout=15,
            stream=True,
        )
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail="segment not in tab cache")
        r.raise_for_status()
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[RESOLVED-STREAM] tab-segment fetch failed: %s", e)
        raise HTTPException(status_code=502)

    media_type = r.headers.get("Content-Type", "video/mp2t")

    def gen():
        try:
            for c in r.iter_content(chunk_size=CHUNK):
                yield c
        except Exception:
            pass
        finally:
            r.close()

    return StreamingResponse(gen(), media_type=media_type)
