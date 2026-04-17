"""Scraper plugin runner with APScheduler.

Loads user-provided scraper scripts from the scrapers/ directory, runs them
on a configurable schedule, deduplicates results against existing channels,
and feeds new events to the batch resolver with auto_create=True.

Scraper scripts are plain Python files that export:
    def scrape(logger) -> list[dict]

Each returned dict should have at minimum {url, title} and optionally
{event_start, event_end, tags}.
"""

import importlib.util
import logging
import os
import threading
import time
from datetime import datetime, timezone

from apscheduler.jobstores.base import JobLookupError

logger = logging.getLogger(__name__)

SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/app/scrapers")

_last_runs: dict[str, dict] = {}  # name -> {time, events, error}
_running: set[str] = set()
_run_lock = threading.Lock()


def _load_script(name: str):
    """Load a scraper script by name from the scrapers directory."""
    path = os.path.join(SCRAPERS_DIR, f"{name}.py")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Scraper script not found: {path}")
    spec = importlib.util.spec_from_file_location(f"scraper_{name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "scrape") or not callable(module.scrape):
        raise AttributeError(f"Scraper {name} has no scrape(logger) function")
    return module


def _dedup_events(events: list[dict]) -> list[dict]:
    """Filter out events that already have channels (by matching page URL
    against existing capture page_urls)."""
    try:
        from core.database import get_session
        from core.models.manifest import Manifest, Capture

        with get_session() as session:
            existing_urls = set()
            rows = (
                session.query(Capture.page_url)
                .join(Manifest, Manifest.capture_id == Capture.id)
                .filter(Manifest.active == True)
                .all()
            )
            for row in rows:
                if row[0]:
                    existing_urls.add(row[0])

        before = len(events)
        filtered = [e for e in events if e.get("url") not in existing_urls]
        if before != len(filtered):
            logger.info("[SCRAPER] Dedup: %d events → %d new (skipped %d existing)",
                        before, len(filtered), before - len(filtered))
        return filtered
    except Exception as e:
        logger.warning("[SCRAPER] Dedup failed, passing all events: %s", e)
        return events


def run_scraper(name: str, config: dict | None = None):
    """Execute a single scraper and feed results to the batch resolver."""
    config = config or {}
    default_tags = config.get("default_tags", [])
    timeout = config.get("timeout", 90)

    with _run_lock:
        _running.add(name)
        logger.info("[SCRAPER] Running scraper: %s", name)
        run_record = {"time": datetime.now(timezone.utc).isoformat(), "events": 0, "error": None}

        try:
            module = _load_script(name)
            events = module.scrape(logger)
            if not isinstance(events, list):
                raise ValueError(f"scrape() returned {type(events).__name__}, expected list")

            # Merge default tags from config
            if default_tags:
                for event in events:
                    event_tags = event.get("tags") or []
                    merged = list(dict.fromkeys(event_tags + default_tags))
                    event["tags"] = merged

            # Dedup against existing channels
            events = _dedup_events(events)
            run_record["events"] = len(events)

            if not events:
                logger.info("[SCRAPER] %s: no new events to resolve", name)
                _last_runs[name] = run_record
                return

            # Feed to batch resolver
            from core.resolver.manifest_resolver import ManifestResolverService
            logger.info("[SCRAPER] %s: feeding %d events to batch resolver (auto_create=True)",
                        name, len(events))

            urls = []
            for e in events:
                entry = {"url": e["url"], "title": e.get("title")}
                if e.get("tags"):
                    entry["tags"] = e["tags"]
                if e.get("event_start"):
                    entry["event_start"] = e["event_start"]
                if e.get("event_end"):
                    entry["event_end"] = e["event_end"]
                if e.get("logo_urls"):
                    entry["logo_urls"] = e["logo_urls"]
                urls.append(entry)

            ManifestResolverService.resolve_batch(urls, timeout=timeout, auto_create=True)

        except Exception as e:
            logger.exception("[SCRAPER] %s failed: %s", name, e)
            run_record["error"] = str(e)

        finally:
            _running.discard(name)

        _last_runs[name] = run_record


def get_status() -> dict:
    """Return scraper config, last run info, running state, and scheduler info."""
    from core.config import get_scraper_config
    config = get_scraper_config()

    # Discover available scripts
    available = []
    if os.path.isdir(SCRAPERS_DIR):
        for f in sorted(os.listdir(SCRAPERS_DIR)):
            if f.endswith(".py") and not f.startswith("_"):
                available.append(f[:-3])

    # Build next_run_time lookup from central scheduler
    next_runs = {}
    try:
        from core.scheduler import get_scheduler
        sched = get_scheduler()
        for job in sched.get_jobs():
            if job.id.startswith("scraper_"):
                name = job.id.replace("scraper_", "", 1)
                if job.next_run_time:
                    next_runs[name] = job.next_run_time.isoformat()
    except Exception:
        pass

    scrapers = {}
    for name in set(list(config.get("scrapers", {}).keys()) + available):
        cfg = config.get("scrapers", {}).get(name, {})
        scrapers[name] = {
            "enabled": cfg.get("enabled", False),
            "interval_hours": cfg.get("interval_hours", 6),
            "default_tags": cfg.get("default_tags", []),
            "timeout": cfg.get("timeout", 90),
            "has_script": name in available,
            "running": name in _running,
            "last_run": _last_runs.get(name),
            "next_run_time": next_runs.get(name),
        }

    return {"scrapers": scrapers}


def reschedule_scraper(name: str, cfg: dict):
    """Add or update a scraper job on the central scheduler."""
    from core.scheduler import get_scheduler

    script_path = os.path.join(SCRAPERS_DIR, f"{name}.py")
    if not os.path.isfile(script_path):
        logger.warning("[SCRAPER] Cannot schedule %s: script not found", name)
        return

    hours = cfg.get("interval_hours", 6)
    sched = get_scheduler()
    sched.add_job(
        run_scraper, "interval", hours=hours,
        args=[name, cfg],
        id=f"scraper_{name}", name=f"Scraper: {name}",
        replace_existing=True,
    )
    logger.info("[SCRAPER] Rescheduled %s (every %sh)", name, hours)


def disable_scraper(name: str):
    """Remove a scraper job from the central scheduler."""
    try:
        from core.scheduler import get_scheduler
        get_scheduler().remove_job(f"scraper_{name}")
        logger.info("[SCRAPER] Disabled scheduler job for %s", name)
    except (JobLookupError, Exception):
        pass


def start_scraper_scheduler():
    """Register configured scraper jobs on the central scheduler."""
    from core.config import get_scraper_config

    config = get_scraper_config()
    scrapers = config.get("scrapers", {})
    if not scrapers:
        logger.info("[SCRAPER] No scrapers configured")
        return

    count = 0
    for name, cfg in scrapers.items():
        if not cfg.get("enabled"):
            continue
        script_path = os.path.join(SCRAPERS_DIR, f"{name}.py")
        if not os.path.isfile(script_path):
            logger.warning("[SCRAPER] Script %s.py not found in %s, skipping", name, SCRAPERS_DIR)
            continue
        reschedule_scraper(name, cfg)
        count += 1

    if count:
        logger.info("[SCRAPER] Registered %d scraper(s) on central scheduler", count)
    else:
        logger.info("[SCRAPER] No enabled scrapers with scripts found")
