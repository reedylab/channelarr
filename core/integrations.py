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
    """Cache-busting Jellyfin refresh via the livetv configuration API.

    Reads the full livetv config, updates the channelarr tuner's URL
    (using global export strategy), writes it back, then triggers
    a guide data refresh task.
    """
    base = url.rstrip("/")
    headers = _jf_headers(api_key)
    strategy = get_setting("EXPORT_STRATEGY", "url")
    base_url = get_setting("BASE_URL", "http://localhost:5045")
    local_path = get_setting("EXPORT_LOCAL_PATH", "")

    if strategy == "local" and local_path:
        m3u_source = f"{local_path}/channelarr.m3u"
        xmltv_source = f"{local_path}/channelarr.xml"
    else:
        m3u_source = f"{base_url}/m3u/channelarr.m3u"
        xmltv_source = f"{base_url}/m3u/channelarr.xml"

    try:
        # 1. Read current livetv config
        r = requests.get(f"{base}/System/Configuration/livetv", headers=headers, timeout=_TIMEOUT)
        r.raise_for_status()
        config = r.json()

        # 2. Find or create channelarr tuner
        tuners = config.get("TunerHosts", [])
        found = False
        for t in tuners:
            if _TUNER_NAME.lower() in (t.get("Url") or "").lower() or \
               t.get("FriendlyName") == _TUNER_NAME:
                t["Url"] = m3u_source
                found = True
                logger.info("[INTEGRATION] Updated Jellyfin tuner to %s", m3u_source)
                break
        if not found:
            tuners.append({
                "Type": "m3u",
                "Url": m3u_source,
                "FriendlyName": _TUNER_NAME,
                "ImportFavoritesOnly": False,
                "AllowHWTranscoding": False,
                "AllowStreamSharing": True,
                "EnableStreamLooping": False,
                "TunerCount": 0,
                "IgnoreDts": True,
            })
            logger.info("[INTEGRATION] Added Jellyfin tuner: %s", m3u_source)
        config["TunerHosts"] = tuners

        # 3. Find or create XMLTV listing provider
        listings = config.get("ListingProviders", [])
        listing_found = False
        for lp in listings:
            if lp.get("Type") == "xmltv" and (
                _TUNER_NAME.lower() in (lp.get("Path") or "").lower() or
                _TUNER_NAME.lower() in (lp.get("ListingsId") or "").lower()
            ):
                lp["Path"] = xmltv_source
                listing_found = True
                logger.info("[INTEGRATION] Updated Jellyfin XMLTV to %s", xmltv_source)
                break
        if not listing_found:
            listings.append({
                "Type": "xmltv",
                "Path": xmltv_source,
                "EnableAllTuners": True,
                "EnabledTuners": [],
            })
            logger.info("[INTEGRATION] Added Jellyfin XMLTV listing: %s", xmltv_source)
        config["ListingProviders"] = listings

        # 4. Write config back
        r = requests.post(f"{base}/System/Configuration/livetv", headers=headers,
                          json=config, timeout=_TIMEOUT)
        r.raise_for_status()
        logger.info("[INTEGRATION] Saved Jellyfin livetv config")

        # 5. Trigger guide refresh via scheduled task
        try:
            tasks_r = requests.get(f"{base}/ScheduledTasks", headers=headers, timeout=_TIMEOUT)
            tasks_r.raise_for_status()
            for task in tasks_r.json():
                if "guide" in (task.get("Name") or "").lower() or \
                   "guide" in (task.get("Key") or "").lower():
                    requests.post(f"{base}/ScheduledTasks/Running/{task['Id']}",
                                  headers=headers, timeout=_TIMEOUT)
                    logger.info("[INTEGRATION] Triggered Jellyfin guide refresh")
                    break
        except Exception as e:
            logger.warning("[INTEGRATION] Guide refresh trigger failed: %s", e)

        return {"ok": True, "source": m3u_source}

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
