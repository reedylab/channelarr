"""Integration logic for Jellyfin, Manifold, and Plex."""

import logging
import threading

import requests

from core.config import get_setting

logger = logging.getLogger(__name__)

_TUNER_NAME = "Channelarr"
_TIMEOUT = 10


def _jf_headers(api_key: str) -> dict:
    return {"X-MediaBrowser-Token": api_key, "Content-Type": "application/json"}


def test_jellyfin(url: str, api_key: str) -> dict:
    """Test Jellyfin connectivity via /System/Info."""
    try:
        r = requests.get(f"{url.rstrip('/')}/System/Info", headers=_jf_headers(api_key), timeout=_TIMEOUT)
        r.raise_for_status()
        info = r.json()
        return {
            "ok": True,
            "server_name": info.get("ServerName", ""),
            "version": info.get("Version", ""),
        }
    except requests.exceptions.ConnectionError:
        return {"ok": False, "error": "Connection refused — check URL"}
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            return {"ok": False, "error": "Unauthorized — check API key"}
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def refresh_jellyfin(url: str, api_key: str) -> dict:
    """Trigger Jellyfin's guide data refresh scheduled task."""
    base = url.rstrip("/")
    headers = _jf_headers(api_key)
    try:
        r = requests.get(f"{base}/ScheduledTasks", headers=headers, timeout=_TIMEOUT)
        r.raise_for_status()
        for task in r.json():
            if "guide" in (task.get("Name") or "").lower() or \
               "guide" in (task.get("Key") or "").lower():
                requests.post(f"{base}/ScheduledTasks/Running/{task['Id']}",
                              headers=headers, timeout=_TIMEOUT)
                logger.info("[INTEGRATION] Triggered Jellyfin guide refresh")
                return {"ok": True, "mode": "refresh"}
        return {"ok": False, "error": "Guide refresh task not found"}
    except Exception as e:
        logger.exception("[INTEGRATION] Jellyfin refresh failed")
        return {"ok": False, "error": str(e)}


def rebind_jellyfin(url: str, api_key: str) -> dict:
    """Force Jellyfin to re-bind all XMLTV listings providers pointing at
    channelarr.xml. Snapshots each matching provider config, DELETEs it,
    then re-POSTs with the same settings — drops stale channel mappings
    and re-auto-matches against the current M3U/XMLTV. Guide refresh
    fires after.

    Non-channelarr XMLTV providers are left alone.
    """
    base = url.rstrip("/")
    headers = _jf_headers(api_key)
    try:
        r = requests.get(f"{base}/System/Configuration/livetv", headers=headers, timeout=_TIMEOUT)
        r.raise_for_status()
        livetv = r.json()
        providers = livetv.get("ListingProviders", []) or []

        rebound = 0
        for p in providers:
            if (p.get("Type") or "").lower() != "xmltv":
                continue
            path = (p.get("Path") or "").lower()
            if "channelarr" not in path:
                continue
            pid = p.get("Id")
            if not pid:
                continue
            # Snapshot all settings; drop Id so Jellyfin generates a new one
            fresh = {k: v for k, v in p.items() if k != "Id"}
            try:
                requests.delete(f"{base}/LiveTv/ListingProviders", headers=headers,
                                params={"id": pid}, timeout=_TIMEOUT)
                requests.post(f"{base}/LiveTv/ListingProviders", headers=headers,
                              json=fresh, timeout=_TIMEOUT).raise_for_status()
                rebound += 1
                logger.info("[INTEGRATION] Rebound Jellyfin XMLTV provider: %s", p.get("Path"))
            except Exception as e:
                logger.warning("[INTEGRATION] Rebind failed for provider %s: %s", pid, e)

        if rebound == 0:
            return {"ok": False, "error": "No XMLTV provider found pointing at channelarr"}

        # Refresh guide so the re-bound provider parses the XMLTV immediately
        refresh_result = refresh_jellyfin(url, api_key)
        return {"ok": True, "mode": "rebind", "rebound": rebound, "refresh": refresh_result}
    except Exception as e:
        logger.exception("[INTEGRATION] Jellyfin rebind failed")
        return {"ok": False, "error": str(e)}


def _refresh_or_rebind_jellyfin(url: str, api_key: str) -> dict:
    """Dispatch based on the persistent JELLYFIN_REBIND_MODE setting."""
    if get_setting("JELLYFIN_REBIND_MODE") == "true":
        return rebind_jellyfin(url, api_key)
    return refresh_jellyfin(url, api_key)


def test_manifold(url: str) -> dict:
    """Ping manifold /health to confirm reachability."""
    try:
        r = requests.get(f"{url.rstrip('/')}/health", timeout=_TIMEOUT)
        r.raise_for_status()
        return {"ok": True, "status": r.json()}
    except requests.exceptions.ConnectionError:
        return {"ok": False, "error": "Connection refused — check URL"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def sync_manifold(url: str, m3u_name: str, epg_name: str, regenerate: bool = True) -> dict:
    """Call manifold's /api/integrations/sync with the configured source names.

    This re-ingests only the named channelarr sources (not all 4700+ channels
    from other sources), then regenerates manifold's combined M3U/XMLTV outputs.
    If manifold has Jellyfin auto-refresh enabled, it cascades automatically.
    """
    try:
        r = requests.post(
            f"{url.rstrip('/')}/api/integrations/sync",
            json={
                "m3u_source": m3u_name or "",
                "epg_source": epg_name or "",
                "regenerate": regenerate,
            },
            timeout=120,
        )
        r.raise_for_status()
        result = r.json()
        logger.info("[INTEGRATION] Manifold sync succeeded: %s", result)
        return result
    except Exception as e:
        logger.warning("[INTEGRATION] Manifold sync failed: %s", e)
        return {"ok": False, "error": str(e)}


def auto_push():
    """Push updates to enabled integrations. Called after M3U regeneration."""
    # Jellyfin
    if get_setting("JELLYFIN_AUTO_REFRESH") == "true":
        jf_url = get_setting("JELLYFIN_URL")
        jf_key = get_setting("JELLYFIN_API_KEY")
        if jf_url and jf_key:
            try:
                result = _refresh_or_rebind_jellyfin(jf_url, jf_key)
                if result["ok"]:
                    logger.info("[INTEGRATION] Auto-push to Jellyfin succeeded")
                else:
                    logger.warning("[INTEGRATION] Auto-push to Jellyfin failed: %s", result["error"])
            except Exception as e:
                logger.warning("[INTEGRATION] Auto-push to Jellyfin error: %s", e)

    # Manifold
    if get_setting("MANIFOLD_AUTO_SYNC") == "true":
        mf_url = get_setting("MANIFOLD_URL")
        if mf_url:
            m3u_name = get_setting("MANIFOLD_M3U_SOURCE_NAME", "Channelarr")
            epg_name = get_setting("MANIFOLD_EPG_SOURCE_NAME", "Channelarr")
            try:
                result = sync_manifold(mf_url, m3u_name, epg_name, regenerate=True)
                if result.get("ok"):
                    logger.info("[INTEGRATION] Auto-sync to Manifold succeeded")
                else:
                    logger.warning("[INTEGRATION] Auto-sync to Manifold failed: %s", result.get("error"))
            except Exception as e:
                logger.warning("[INTEGRATION] Auto-sync to Manifold error: %s", e)


def auto_push_async():
    """Fire auto_push in a background thread so M3U regen isn't blocked."""
    threading.Thread(target=auto_push, daemon=True).start()
