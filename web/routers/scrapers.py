"""Scraper plugin management API."""

import json
import os
import re
import threading
import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

router = APIRouter(tags=["scrapers"])
logger = logging.getLogger(__name__)

SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/app/scrapers")
_SAFE_NAME = re.compile(r"^[a-zA-Z0-9_-]+$")


class ScraperConfigUpdate(BaseModel):
    enabled: bool
    interval_hours: float = 6
    default_tags: list[str] = []
    timeout: int = 90
    use_event_queue: bool = True
    title_filter: str = ""
    title_filter_invert: bool = False


def _validate_name(name: str) -> str:
    """Sanitize scraper name to prevent path traversal."""
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=400, detail="Invalid scraper name")
    return name


@router.get("/scrapers/status")
def scrapers_status():
    """Return all scrapers with config, status, and scheduler info."""
    from core.scraper_runner import get_status
    return get_status()


@router.post("/scrapers/{name}/run")
def scrapers_run(name: str):
    """Manually trigger a scraper. Runs in background thread."""
    name = _validate_name(name)
    from core.scraper_runner import run_scraper
    from core.config import get_scraper_config

    script_path = os.path.join(SCRAPERS_DIR, f"{name}.py")
    if not os.path.isfile(script_path):
        raise HTTPException(status_code=404, detail=f"Script {name}.py not found")

    config = get_scraper_config()
    cfg = config.get("scrapers", {}).get(name, {})
    thread = threading.Thread(
        target=run_scraper,
        args=(name, cfg),
        daemon=True,
    )
    thread.start()
    return {"ok": True, "message": f"Running scraper: {name}"}


@router.get("/scrapers/{name}/source")
def scrapers_source(name: str):
    """Return the plugin source code as plain text."""
    name = _validate_name(name)
    path = os.path.join(SCRAPERS_DIR, f"{name}.py")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"Script {name}.py not found")
    with open(path, "r") as f:
        return PlainTextResponse(f.read())


@router.put("/scrapers/{name}/config")
def scrapers_update_config(name: str, body: ScraperConfigUpdate):
    """Update a scraper's config and reschedule its job."""
    name = _validate_name(name)
    from core.config import get_scraper_config, save_settings
    from core.scraper_runner import reschedule_scraper, disable_scraper

    config = get_scraper_config()
    scrapers = config.get("scrapers", {})
    scrapers[name] = {
        "enabled": body.enabled,
        "interval_hours": body.interval_hours,
        "default_tags": body.default_tags,
        "timeout": body.timeout,
        "use_event_queue": body.use_event_queue,
        "title_filter": body.title_filter,
        "title_filter_invert": body.title_filter_invert,
    }
    config["scrapers"] = scrapers
    save_settings({"SCRAPER_CONFIG": json.dumps(config)})

    if body.enabled:
        reschedule_scraper(name, scrapers[name])
    else:
        disable_scraper(name)

    return {"ok": True, "message": f"Config saved for {name}"}


@router.delete("/scrapers/{name}/config")
def scrapers_delete_config(name: str):
    """Remove a scraper from config and unschedule it."""
    name = _validate_name(name)
    from core.config import get_scraper_config, save_settings
    from core.scraper_runner import disable_scraper

    config = get_scraper_config()
    scrapers = config.get("scrapers", {})
    if name in scrapers:
        del scrapers[name]
        config["scrapers"] = scrapers
        save_settings({"SCRAPER_CONFIG": json.dumps(config)})

    disable_scraper(name)
    return {"ok": True, "message": f"Config removed for {name}"}
