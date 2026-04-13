"""Resolve m3u8 manifests via the selenium-uc sidecar.

The actual browser work happens in the sidecar (Chrome + undetected_chromedriver).
This service is a thin HTTP client that calls the sidecar and stores results
in channelarr's resolver Postgres tables (parallel to existing JSON storage).
"""

import hashlib
import logging
import re
import threading
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urljoin

import requests as http_requests

from core.config import get_setting
from core.database import get_session
from core.models.manifest import Capture, Manifest, Variant, HeaderProfile
from core.resolver.expiry_parser import parse_expiry, parse_body_expiry

logger = logging.getLogger(__name__)

# Status tracking for async resolve jobs
_status = {"running": False, "last_url": None, "last_error": None, "last_manifest_id": None}

# Batch state
_batch = {"running": False, "total": 0, "completed": 0, "current_url": None, "results": []}

# ── Circuit breaker for sidecar calls ──────────────────────────────────
# Tracks consecutive failures per page_url. After MAX_FAILURES, the URL is
# blocked for COOLDOWN_SECONDS to prevent one bad source from DOS-ing the
# sidecar and starving legitimate refresh requests.
_CB_MAX_FAILURES = 3
_CB_COOLDOWN_SECONDS = 600  # 10 minutes
_circuit_breaker: dict[str, dict] = {}  # page_url -> {"failures": int, "blocked_until": float}
_cb_lock = threading.Lock()

# ── In-flight dedup ────────────────────────────────────────────────────
# If a resolve/refresh is already in progress for a page_url, subsequent
# callers wait for it instead of queuing another sidecar capture.
_inflight: dict[str, threading.Event] = {}  # page_url -> Event (set when done)
_inflight_results: dict[str, dict] = {}     # page_url -> result dict
_inflight_lock = threading.Lock()


def _cb_check(page_url: str) -> str | None:
    """Return an error string if this URL is circuit-broken, else None."""
    with _cb_lock:
        cb = _circuit_breaker.get(page_url)
        if not cb:
            return None
        if cb["blocked_until"] and time.time() < cb["blocked_until"]:
            remaining = int(cb["blocked_until"] - time.time())
            return f"Circuit breaker: {page_url} blocked for {remaining}s after {cb['failures']} consecutive failures"
        # Cooldown expired — reset
        if cb["blocked_until"] and time.time() >= cb["blocked_until"]:
            del _circuit_breaker[page_url]
    return None


def _cb_record_failure(page_url: str):
    """Record a capture failure. Trips the breaker after MAX_FAILURES."""
    with _cb_lock:
        cb = _circuit_breaker.setdefault(page_url, {"failures": 0, "blocked_until": 0})
        cb["failures"] += 1
        if cb["failures"] >= _CB_MAX_FAILURES:
            cb["blocked_until"] = time.time() + _CB_COOLDOWN_SECONDS
            logger.warning("[RESOLVER] Circuit breaker tripped for %s — blocked for %ds after %d failures",
                           page_url, _CB_COOLDOWN_SECONDS, cb["failures"])


def _cb_record_success(page_url: str):
    """Clear the circuit breaker on success."""
    with _cb_lock:
        _circuit_breaker.pop(page_url, None)


def _default_resolved_name(title: str | None, manifest_url: str, source_domain: str | None) -> str:
    """Pick a sensible display name for a resolved channel when the user
    didn't provide a title. Tries: explicit title → manifest URL hostname
    → source_domain → constant fallback."""
    if title and title.strip():
        return title.strip()
    try:
        host = urlparse(manifest_url).hostname or ""
        if host:
            return host
    except Exception:
        pass
    if source_domain:
        return source_domain
    return "Unnamed Resolved"


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _sha256(text: str | None) -> str | None:
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()


def _sanitize_body(text: str | None) -> str | None:
    """Strip control characters, keep #EXT lines intact."""
    if not text:
        return None
    sanitized = re.sub(r'[\x00-\x1F\x7F]', '', text)
    lines = sanitized.splitlines()
    clean = []
    for line in lines:
        if line.startswith('#EXT'):
            clean.append(line)
        else:
            clean.append(re.sub(r'[\x00-\x1F\x7F\x80-\xFF]', '', line))
    return '\n'.join(clean) or None


def _parse_master_variants(body_text: str, manifest_url: str) -> list[dict]:
    """Parse EXT-X-STREAM-INF entries from a master playlist."""
    if not body_text:
        return []
    lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]
    out = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        if ln.startswith("#EXT-X-STREAM-INF"):
            attrs = {}
            for kv in re.split(r',(?=[A-Z0-9\-]+=)', ln.split(":", 1)[1]):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    attrs[k] = v.strip('"')
            uri = lines[i + 1] if i + 1 < len(lines) else ""
            abs_url = urljoin(manifest_url, uri)
            res = attrs.get("RESOLUTION")
            w = h = None
            if res and "x" in res:
                try:
                    w, h = map(int, res.split("x"))
                except Exception:
                    pass
            out.append({
                "uri": uri,
                "abs_url": abs_url,
                "bandwidth": int(attrs.get("BANDWIDTH", "0") or 0),
                "resolution": res,
                "frame_rate": float(attrs.get("FRAME-RATE", "0") or 0),
                "codecs": attrs.get("CODECS"),
                "audio_group": attrs.get("AUDIO"),
                "width": w,
                "height": h,
            })
            i += 2
        else:
            i += 1
    return out


import threading

_refresh_worker_started = False
_refresh_worker_lock = threading.Lock()


def _refresh_worker_loop():
    """Daemon loop: every 60s, refresh resolved manifests with recent access near expiry.

    Demand-driven — dormant channels are left alone. First request after dormancy
    is handled by the 403 safety net in the stream router.
    """
    import time
    while True:
        try:
            now = datetime.now(timezone.utc)
            soon = now + timedelta(minutes=5)
            cooldown = now - timedelta(minutes=3)
            watching_window = now - timedelta(minutes=10)
            with get_session() as session:
                rows = (
                    session.query(Manifest.id)
                    .filter(Manifest.tags.contains(["resolved"]))
                    .filter(Manifest.active == True)
                    .filter(Manifest.last_accessed_at.isnot(None))
                    .filter(Manifest.last_accessed_at > watching_window)
                    .filter(
                        (Manifest.expires_at.is_(None)) |
                        (Manifest.expires_at < soon)
                    )
                    .filter(
                        (Manifest.last_refreshed_at.is_(None)) |
                        (Manifest.last_refreshed_at < cooldown)
                    )
                    .limit(5)
                    .all()
                )
                ids = [r[0] for r in rows]
            if ids:
                logger.info("[RESOLVER] Demand refresh: %d manifests due", len(ids))
                for mid in ids:
                    try:
                        ManifestResolverService.refresh_manifest(mid)
                    except Exception as e:
                        logger.warning("[RESOLVER] refresh %s failed: %s", mid, e)
        except Exception as e:
            logger.exception("[RESOLVER] refresh worker error: %s", e)
        time.sleep(60)


def start_refresh_worker():
    """Spawn the demand-driven refresh worker daemon thread (idempotent)."""
    global _refresh_worker_started
    with _refresh_worker_lock:
        if _refresh_worker_started:
            return
        t = threading.Thread(target=_refresh_worker_loop, name="resolver-refresh", daemon=True)
        t.start()
        _refresh_worker_started = True
        logger.info("[RESOLVER] Demand refresh worker started")


def _call_sidecar(url: str, timeout: int) -> dict:
    """POST to the selenium-uc sidecar /capture endpoint."""
    sidecar_url = f"{get_setting('SELENIUM_URL', 'http://localhost:4445')}/capture"
    # HTTP timeout = browser timeout + 30s buffer for startup/teardown
    http_timeout = timeout + 30
    logger.info("Calling sidecar %s for %s", sidecar_url, url)
    resp = http_requests.post(
        sidecar_url,
        json={"url": url, "timeout": timeout, "switch_iframe": True},
        timeout=http_timeout,
    )
    resp.raise_for_status()
    return resp.json()


class ManifestResolverService:

    @staticmethod
    def get_status():
        return dict(_status)

    @staticmethod
    def check_selenium() -> bool:
        """Check if the selenium-uc sidecar is reachable."""
        try:
            r = http_requests.get(f"{get_setting('SELENIUM_URL', 'http://localhost:4445')}/health", timeout=5)
            return r.status_code == 200 and r.json().get("ready", False)
        except Exception:
            return False

    @staticmethod
    def resolve(url: str, title: str | None = None, timeout: int = 60,
                existing_manifest_id: str | None = None) -> dict:
        """Capture an m3u8 manifest via the sidecar and store it in DB.

        If existing_manifest_id is provided, the specified row is updated in place
        (used for token refresh — keeps the same manifest ID so streams don't break).
        """
        # Circuit breaker — reject if this URL has failed too many times recently
        cb_err = _cb_check(url)
        if cb_err:
            logger.info("[RESOLVER] %s", cb_err)
            return {"ok": False, "manifest_id": existing_manifest_id, "manifest_url": None, "error": cb_err}

        # In-flight dedup — if another thread is already resolving this URL, wait for it
        with _inflight_lock:
            if url in _inflight:
                event = _inflight[url]
                logger.info("[RESOLVER] Waiting on in-flight resolve for %s", url)
            else:
                event = None
                _inflight[url] = threading.Event()

        if event:
            event.wait(timeout=timeout + 45)
            result = _inflight_results.pop(url, None)
            if result:
                return result
            return {"ok": False, "manifest_id": existing_manifest_id, "manifest_url": None,
                    "error": "In-flight resolve timed out"}

        _status["running"] = True
        _status["last_url"] = url
        _status["last_error"] = None
        result = None

        try:
            capture = _call_sidecar(url, timeout)

            if not capture.get("ok"):
                err = capture.get("error", "Unknown error from sidecar")
                _status["last_error"] = err
                result = {"ok": False, "manifest_id": None, "manifest_url": None, "error": err}
                return result

            body_text = _sanitize_body(capture.get("body"))
            if not body_text or "#EXTM3U" not in body_text:
                err = "Captured body is not valid HLS"
                _status["last_error"] = err
                result = {"ok": False, "manifest_id": None, "manifest_url": None, "error": err}
                return result

            # Build context with heartbeat info
            context = {}
            heartbeat = capture.get("heartbeat")
            if heartbeat:
                context["heartbeat_url"] = heartbeat.get("heartbeat_url")
                context["heartbeat_interval"] = 30
                context["auth_headers"] = {
                    k: v for k, v in heartbeat.items()
                    if k != "heartbeat_url" and v is not None
                }
                key_match = re.search(r'#EXT-X-KEY:.*?URI="([^"]+)"', body_text)
                if key_match:
                    context["drm_key_url"] = key_match.group(1)

            manifest_url = capture["manifest_url"]
            manifest_id = _store_manifest(
                page_url=url,
                user_agent=capture.get("user_agent", ""),
                manifest_url=manifest_url,
                mime=capture.get("mime"),
                resp_headers=capture.get("headers"),
                body_text=body_text,
                title=title,
                context=context,
                heartbeat=heartbeat,
                existing_manifest_id=existing_manifest_id,
            )

            _status["last_manifest_id"] = manifest_id
            now_utc = datetime.now(timezone.utc)
            expires_at = parse_body_expiry(body_text, manifest_url) or (now_utc + timedelta(minutes=30))
            logger.info("[RESOLVER] Manifest resolved and stored: %s -> %s (expires %s)",
                        url, manifest_id, expires_at.isoformat())
            try:
                from web import shared_state
                shared_state.regenerate_m3u()
            except Exception as e:
                logger.debug("[RESOLVER] regenerate_m3u after resolve failed: %s", e)
            result = {
                "ok": True,
                "manifest_id": manifest_id,
                "manifest_url": manifest_url,
                "expires_at": expires_at.isoformat() if expires_at else None,
                "error": None,
            }
            return result

        except http_requests.exceptions.RequestException as e:
            err = f"Sidecar communication failed: {e}"
            logger.exception("Sidecar call failed for %s", url)
            _status["last_error"] = err
            result = {"ok": False, "manifest_id": None, "manifest_url": None, "error": err}
            return result

        except Exception as e:
            logger.exception("Resolve failed for %s", url)
            _status["last_error"] = str(e)
            result = {"ok": False, "manifest_id": None, "manifest_url": None, "error": str(e)}
            return result

        finally:
            _status["running"] = False
            # Circuit breaker bookkeeping
            if result and result.get("ok"):
                _cb_record_success(url)
            elif result:
                _cb_record_failure(url)
            # Signal in-flight waiters
            with _inflight_lock:
                evt = _inflight.pop(url, None)
            if evt:
                if result:
                    _inflight_results[url] = result
                evt.set()

    @staticmethod
    def refresh_manifest(manifest_id: str, timeout: int = 60) -> dict:
        """Re-resolve an existing manifest using its stored page_url.

        Updates the same row in place (preserves manifest_id) so active streams
        see a seamless URL swap on their next playlist poll.
        """
        with get_session() as session:
            row = (
                session.query(Manifest.title, Capture.page_url)
                .outerjoin(Capture, Manifest.capture_id == Capture.id)
                .filter(Manifest.id == manifest_id)
                .first()
            )
        if not row:
            return {"ok": False, "manifest_id": manifest_id, "error": "manifest not found"}
        title, page_url = row
        if not page_url:
            return {"ok": False, "manifest_id": manifest_id, "error": "no page_url for refresh"}

        logger.info("Refreshing manifest %s from %s", manifest_id, page_url)
        return ManifestResolverService.resolve(
            url=page_url, title=title, timeout=timeout,
            existing_manifest_id=manifest_id,
        )

    @staticmethod
    def get_batch_status():
        return dict(_batch)

    @staticmethod
    def resolve_batch(urls: list[dict], timeout: int = 60):
        """Resolve a list of URLs sequentially."""
        _batch["running"] = True
        _batch["total"] = len(urls)
        _batch["completed"] = 0
        _batch["results"] = [
            {"url": u["url"], "title": u.get("title"), "status": "pending",
             "manifest_id": None, "manifest_url": None, "expires_at": None, "error": None}
            for u in urls
        ]

        for i, entry in enumerate(urls):
            _batch["current_url"] = entry["url"]
            _batch["results"][i]["status"] = "resolving"

            result = ManifestResolverService.resolve(
                url=entry["url"], title=entry.get("title"), timeout=timeout
            )

            if result["ok"]:
                _batch["results"][i]["status"] = "done"
                _batch["results"][i]["manifest_id"] = result["manifest_id"]
                _batch["results"][i]["manifest_url"] = result["manifest_url"]
                _batch["results"][i]["expires_at"] = result.get("expires_at")
            else:
                _batch["results"][i]["status"] = "failed"
                _batch["results"][i]["error"] = result["error"]

            _batch["completed"] = i + 1

        _batch["running"] = False
        _batch["current_url"] = None
        logger.info("Batch resolve complete: %d/%d succeeded",
                     sum(1 for r in _batch["results"] if r["status"] == "done"),
                     _batch["total"])

    @staticmethod
    def retry_batch_item(index: int, timeout: int = 60):
        """Retry a single failed item in the batch."""
        if index < 0 or index >= len(_batch["results"]):
            return {"ok": False, "error": "Invalid index"}

        item = _batch["results"][index]
        if item["status"] != "failed":
            return {"ok": False, "error": "Item is not in failed state"}

        _batch["running"] = True
        _batch["current_url"] = item["url"]
        item["status"] = "resolving"
        item["error"] = None

        result = ManifestResolverService.resolve(
            url=item["url"], title=item.get("title"), timeout=timeout
        )

        if result["ok"]:
            item["status"] = "done"
            item["manifest_id"] = result["manifest_id"]
            item["manifest_url"] = result["manifest_url"]
            item["expires_at"] = result.get("expires_at")
        else:
            item["status"] = "failed"
            item["error"] = result["error"]

        _batch["running"] = False
        _batch["current_url"] = None
        return result


def _store_manifest(
    *,
    page_url: str,
    user_agent: str,
    manifest_url: str,
    mime: str | None,
    resp_headers: dict | None,
    body_text: str,
    title: str | None,
    context: dict,
    heartbeat: dict | None,
    existing_manifest_id: str | None = None,
) -> str:
    """Insert or update a manifest in Manifold's DB. Returns manifest ID.

    If existing_manifest_id is given, the specified row is updated in place
    regardless of hash changes (used for token refresh).
    """
    source_domain = urlparse(manifest_url).netloc
    kind = "master" if "#EXT-X-STREAM-INF" in body_text else "media"
    url_hash = _md5(manifest_url)
    body_hash = _sha256(body_text)
    now = datetime.now(timezone.utc)
    # Try to parse real expiry from URL/body; fall back to 30min default so the
    # scheduler refreshes all resolved channels periodically regardless of token format.
    expires_at = parse_body_expiry(body_text, manifest_url) or (now + timedelta(minutes=30))

    # DRM detection
    drm_method = None
    is_drm = False
    if "#EXT-X-KEY" in body_text:
        if "METHOD=SAMPLE-AES" in body_text:
            drm_method, is_drm = "SAMPLE-AES", True
        elif "METHOD=AES-128" in body_text:
            drm_method, is_drm = "AES-128", False

    with get_session() as session:
        cap = Capture(page_url=page_url, user_agent=user_agent, context=context)
        session.add(cap)
        session.flush()

        header_profile_id = None
        if heartbeat:
            profile_name = f"resolved-{source_domain}"
            hp = session.query(HeaderProfile).filter_by(name=profile_name).first()
            auth_headers = {k: v for k, v in heartbeat.items()
                           if k != "heartbeat_url" and v is not None}
            if hp:
                hp.headers = auth_headers
            else:
                hp = HeaderProfile(name=profile_name, headers=auth_headers)
                session.add(hp)
                session.flush()
            header_profile_id = hp.id

        # Find existing row for in-place update:
        # 1. Explicit manifest_id (refresh path) takes precedence
        # 2. Otherwise match by active resolved title (re-resolving same channel)
        # 3. Otherwise fall back to hash dedup (generic content)
        manifest = None
        if existing_manifest_id:
            manifest = session.query(Manifest).filter_by(id=existing_manifest_id).first()
        if not manifest and title:
            manifest = session.query(Manifest).filter(
                Manifest.title == title,
                Manifest.active == True,
                Manifest.tags.contains(["resolved"]),
            ).first()
        if not manifest:
            manifest = session.query(Manifest).filter(
                Manifest.url_hash == url_hash,
                Manifest.sha256 == body_hash,
            ).first()

        if manifest:
            # Update in place — preserves manifest_id across token refreshes
            manifest.capture_id = cap.id
            if header_profile_id:
                manifest.header_profile_id = header_profile_id
                manifest.requires_headers = True
            manifest.url = manifest_url
            manifest.url_hash = url_hash
            manifest.source_domain = source_domain
            manifest.mime = mime
            manifest.kind = kind
            manifest.headers = resp_headers or {}
            manifest.body = body_text
            manifest.sha256 = body_hash
            manifest.drm_method = drm_method
            manifest.is_drm = is_drm
            manifest.expires_at = expires_at
            manifest.last_refreshed_at = now
            # Refresh variants for master playlists (drop old, insert new)
            if kind == "master":
                session.query(Variant).filter_by(manifest_id=manifest.id).delete()
                session.flush()
                for v in _parse_master_variants(body_text, manifest_url):
                    session.add(Variant(manifest_id=manifest.id, **v))
        else:
            manifest = Manifest(
                capture_id=cap.id,
                header_profile_id=header_profile_id,
                url=manifest_url,
                url_hash=url_hash,
                source_domain=source_domain,
                mime=mime,
                kind=kind,
                headers=resp_headers or {},
                requires_headers=bool(header_profile_id),
                body=body_text,
                sha256=body_hash,
                drm_method=drm_method,
                is_drm=is_drm,
                title=title,
                tags=["live", "captured", "resolved"],
                active=True,
                expires_at=expires_at,
                last_refreshed_at=now,
            )
            session.add(manifest)
            session.flush()

            if kind == "master":
                for v in _parse_master_variants(body_text, manifest_url):
                    session.add(Variant(manifest_id=manifest.id, **v))

            # B3: Resolves create manifests only — they no longer auto-create
            # a Channel row. The library is the manifest collection; channels
            # are created explicitly from a library entry via /api/channels
            # with type=resolved + manifest_id.

        return manifest.id
