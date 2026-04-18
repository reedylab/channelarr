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
    auto_refresh: bool = False
    rebind_mode: bool = False


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
            "auto_refresh": s.get("JELLYFIN_AUTO_REFRESH") == "true",
            "rebind_mode": s.get("JELLYFIN_REBIND_MODE") == "true",
            "configured": bool(s.get("JELLYFIN_URL") and s.get("JELLYFIN_API_KEY")),
        },
        "manifold": {
            "url": s.get("MANIFOLD_URL", ""),
            "auto_refresh": s.get("MANIFOLD_AUTO_REFRESH") == "true",
            "configured": bool(s.get("MANIFOLD_URL")),
        },
    }


@router.put("/integrations/jellyfin/config")
def jellyfin_save_config(body: JellyfinConfig):
    save_settings({
        "JELLYFIN_URL": body.url,
        "JELLYFIN_API_KEY": body.api_key,
        "JELLYFIN_AUTO_REFRESH": "true" if body.auto_refresh else "false",
        "JELLYFIN_REBIND_MODE": "true" if body.rebind_mode else "false",
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
    from core.integrations import _refresh_or_rebind_jellyfin
    url = get_setting("JELLYFIN_URL")
    key = get_setting("JELLYFIN_API_KEY")
    if not url or not key:
        return {"ok": False, "error": "Jellyfin URL and API key required"}
    mode = "rebind" if get_setting("JELLYFIN_REBIND_MODE") == "true" else "refresh"

    def _run():
        result = _refresh_or_rebind_jellyfin(url, key)
        if not result["ok"]:
            logger.warning("[INTEGRATION] Jellyfin %s failed: %s", mode, result["error"])

    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "message": f"Jellyfin {mode} started", "mode": mode}


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
