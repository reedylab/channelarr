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
    """Cache-busting Jellyfin refresh: delete tuner + re-add, then refresh guide.

    Uses the global export strategy (url vs local) from settings.
    """
    base = url.rstrip("/")
    headers = _jf_headers(api_key)
    strategy = get_setting("EXPORT_STRATEGY", "url")
    base_url = get_setting("BASE_URL", "http://localhost:5045")
    local_path = get_setting("EXPORT_LOCAL_PATH", "")

    if strategy == "local" and local_path:
        tuner_source = f"{local_path}/channelarr.m3u"
    else:
        tuner_source = f"{base_url}/m3u/channelarr.m3u"

    if not tuner_source:
        return {"ok": False, "error": "No M3U source configured"}

    try:
        # 1. Find existing channelarr tuner
        r = requests.get(f"{base}/LiveTv/TunerHosts", headers=headers, timeout=_TIMEOUT)
        r.raise_for_status()
        tuners = r.json()
        existing_id = None
        for t in tuners:
            if t.get("FriendlyName") == _TUNER_NAME or _TUNER_NAME.lower() in (t.get("Url") or "").lower():
                existing_id = t.get("Id")
                break

        # 2. Delete existing tuner (cache bust)
        if existing_id:
            requests.delete(f"{base}/LiveTv/TunerHosts?id={existing_id}",
                            headers=headers, timeout=_TIMEOUT)
            logger.info("[INTEGRATION] Deleted Jellyfin tuner %s", existing_id)

        # 3. Re-add tuner
        tuner_body = {
            "Type": "M3U",
            "Url": tuner_source,
            "FriendlyName": _TUNER_NAME,
            "ImportFavoritesOnly": False,
            "AllowHWTranscoding": True,
            "EnableStreamLooping": False,
            "TunerCount": 4,
        }
        r = requests.post(f"{base}/LiveTv/TunerHosts", headers=headers,
                          json=tuner_body, timeout=_TIMEOUT)
        r.raise_for_status()
        new_tuner = r.json()
        logger.info("[INTEGRATION] Re-added Jellyfin tuner: %s (source: %s)",
                     new_tuner.get("Id"), tuner_source)

        # 4. Trigger guide refresh via scheduled task
        try:
            tasks_r = requests.get(f"{base}/ScheduledTasks", headers=headers, timeout=_TIMEOUT)
            tasks_r.raise_for_status()
            guide_task = None
            for task in tasks_r.json():
                if "guide" in (task.get("Name") or "").lower() or \
                   "guide" in (task.get("Key") or "").lower():
                    guide_task = task.get("Id")
                    break
            if guide_task:
                requests.post(f"{base}/ScheduledTasks/Running/{guide_task}",
                              headers=headers, timeout=_TIMEOUT)
                logger.info("[INTEGRATION] Triggered Jellyfin guide refresh task %s", guide_task)
        except Exception as e:
            logger.warning("[INTEGRATION] Guide refresh trigger failed: %s", e)

        return {"ok": True, "tuner_id": new_tuner.get("Id"), "source": tuner_source}

    except Exception as e:
        logger.exception("[INTEGRATION] Jellyfin refresh failed")
        return {"ok": False, "error": str(e)}


def test_manifold(url: str) -> dict:
    """Test manifold connectivity."""
    try:
        r = requests.get(f"{url.rstrip('/')}/api/status", timeout=_TIMEOUT)
        r.raise_for_status()
        return {"ok": True, "status": r.json()}
    except requests.exceptions.ConnectionError:
        return {"ok": False, "error": "Connection refused — check URL"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def refresh_manifold(url: str) -> dict:
    """Trigger manifold to re-ingest M3U and EPG sources."""
    base = url.rstrip("/")
    errors = []
    try:
        r = requests.post(f"{base}/api/m3u-sources/ingest", timeout=30)
        r.raise_for_status()
        logger.info("[INTEGRATION] Triggered manifold M3U ingest")
    except Exception as e:
        errors.append(f"M3U ingest: {e}")

    try:
        r = requests.post(f"{base}/api/epg-sources/ingest", timeout=30)
        r.raise_for_status()
        logger.info("[INTEGRATION] Triggered manifold EPG ingest")
    except Exception as e:
        errors.append(f"EPG ingest: {e}")

    if errors:
        return {"ok": False, "error": "; ".join(errors)}
    return {"ok": True}


def auto_push():
    """Push updates to enabled integrations. Called after M3U regeneration."""
    # Jellyfin
    if get_setting("JELLYFIN_AUTO_REFRESH") == "true":
        jf_url = get_setting("JELLYFIN_URL")
        jf_key = get_setting("JELLYFIN_API_KEY")
        if jf_url and jf_key:
            try:
                result = refresh_jellyfin(jf_url, jf_key)
                if result["ok"]:
                    logger.info("[INTEGRATION] Auto-push to Jellyfin succeeded")
                else:
                    logger.warning("[INTEGRATION] Auto-push to Jellyfin failed: %s", result["error"])
            except Exception as e:
                logger.warning("[INTEGRATION] Auto-push to Jellyfin error: %s", e)

    # Manifold
    if get_setting("MANIFOLD_AUTO_REFRESH") == "true":
        mf_url = get_setting("MANIFOLD_URL")
        if mf_url:
            try:
                result = refresh_manifold(mf_url)
                if result["ok"]:
                    logger.info("[INTEGRATION] Auto-push to Manifold succeeded")
                else:
                    logger.warning("[INTEGRATION] Auto-push to Manifold failed: %s", result["error"])
            except Exception as e:
                logger.warning("[INTEGRATION] Auto-push to Manifold error: %s", e)


def auto_push_async():
    """Fire auto_push in a background thread so M3U regen isn't blocked."""
    threading.Thread(target=auto_push, daemon=True).start()
