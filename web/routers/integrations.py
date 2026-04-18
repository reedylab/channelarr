"""Integration management API for Jellyfin, Manifold, and Plex."""

import logging
import threading

from fastapi import APIRouter
from pydantic import BaseModel

from core.config import get_setting, get_all_settings, save_settings

router = APIRouter(tags=["integrations"])
logger = logging.getLogger(__name__)


class JellyfinConfig(BaseModel):
    url: str = ""
    api_key: str = ""
    strategy: str = "url"
    local_path: str = ""
    auto_refresh: bool = False


class ManifoldConfig(BaseModel):
    url: str = ""
    auto_refresh: bool = False


@router.get("/integrations/status")
def integrations_status():
    """Return all integration configs and connection status."""
    s = get_all_settings()
    base_url = s.get("BASE_URL", "http://localhost:5045")
    return {
        "jellyfin": {
            "url": s.get("JELLYFIN_URL", ""),
            "api_key": s.get("JELLYFIN_API_KEY", ""),
            "strategy": s.get("JELLYFIN_STRATEGY", "url"),
            "local_path": s.get("JELLYFIN_LOCAL_PATH", ""),
            "auto_refresh": s.get("JELLYFIN_AUTO_REFRESH") == "true",
            "configured": bool(s.get("JELLYFIN_URL") and s.get("JELLYFIN_API_KEY")),
        },
        "manifold": {
            "url": s.get("MANIFOLD_URL", ""),
            "auto_refresh": s.get("MANIFOLD_AUTO_REFRESH") == "true",
            "configured": bool(s.get("MANIFOLD_URL")),
        },
        "plex": {
            "hdhr_url": f"{base_url}/discover.json",
            "lineup_url": f"{base_url}/lineup.json",
        },
    }


@router.put("/integrations/jellyfin/config")
def jellyfin_save_config(body: JellyfinConfig):
    save_settings({
        "JELLYFIN_URL": body.url,
        "JELLYFIN_API_KEY": body.api_key,
        "JELLYFIN_STRATEGY": body.strategy,
        "JELLYFIN_LOCAL_PATH": body.local_path,
        "JELLYFIN_AUTO_REFRESH": "true" if body.auto_refresh else "false",
    })
    return {"ok": True}


@router.post("/integrations/jellyfin/test")
def jellyfin_test():
    from core.integrations import test_jellyfin
    url = get_setting("JELLYFIN_URL")
    key = get_setting("JELLYFIN_API_KEY")
    if not url or not key:
        return {"ok": False, "error": "Jellyfin URL and API key required"}
    return test_jellyfin(url, key)


@router.post("/integrations/jellyfin/refresh")
def jellyfin_refresh():
    from core.integrations import refresh_jellyfin
    url = get_setting("JELLYFIN_URL")
    key = get_setting("JELLYFIN_API_KEY")
    strategy = get_setting("JELLYFIN_STRATEGY", "url")
    local_path = get_setting("JELLYFIN_LOCAL_PATH", "")
    base_url = get_setting("BASE_URL", "http://localhost:5045")
    m3u_url = f"{base_url}/m3u/channelarr.m3u"
    xmltv_url = f"{base_url}/m3u/channelarr.xml"
    local_m3u = f"{local_path}/channelarr.m3u" if local_path else ""
    local_xmltv = f"{local_path}/channelarr.xml" if local_path else ""
    if not url or not key:
        return {"ok": False, "error": "Jellyfin URL and API key required"}

    def _run():
        result = refresh_jellyfin(url, key, strategy, m3u_url, xmltv_url, local_m3u, local_xmltv)
        if not result["ok"]:
            logger.warning("[INTEGRATION] Jellyfin refresh failed: %s", result["error"])

    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "message": "Jellyfin refresh started"}


@router.put("/integrations/manifold/config")
def manifold_save_config(body: ManifoldConfig):
    save_settings({
        "MANIFOLD_URL": body.url,
        "MANIFOLD_AUTO_REFRESH": "true" if body.auto_refresh else "false",
    })
    return {"ok": True}


@router.post("/integrations/manifold/test")
def manifold_test():
    from core.integrations import test_manifold
    url = get_setting("MANIFOLD_URL")
    if not url:
        return {"ok": False, "error": "Manifold URL required"}
    return test_manifold(url)


@router.post("/integrations/manifold/refresh")
def manifold_refresh():
    from core.integrations import refresh_manifold
    url = get_setting("MANIFOLD_URL")
    if not url:
        return {"ok": False, "error": "Manifold URL required"}

    def _run():
        result = refresh_manifold(url)
        if not result["ok"]:
            logger.warning("[INTEGRATION] Manifold refresh failed: %s", result["error"])

    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "message": "Manifold refresh started"}
