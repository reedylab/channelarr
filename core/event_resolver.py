"""JIT event resolver — drains the scraped_events queue as events near kickoff.

Scrapers populate `scraped_events` during discovery. This module runs on a
tight interval, selects pending rows whose `event_start` is within the lead
window, and hands them off to the existing `ManifestResolverService.resolve_batch`
(reusing the selenium sidecar, in-flight dedup, and batch progress UI).

Also provides:
  - `expire_stale_events()`: housekeeping for events whose window has passed
    without ever resolving.
  - `backfill_from_channels()`: one-time seed of the queue from existing
    scraper-origin channels so they show up in the UI as `resolved`.
"""

import hashlib
import logging
import os
import threading
from datetime import datetime, timezone, timedelta

from sqlalchemy import or_

logger = logging.getLogger(__name__)

_last_tick = {"time": None, "picked": 0, "resolved": 0, "failed": 0}


def _list_scraper_plugins() -> list[str]:
    """Return scraper plugin names (filenames without .py) from SCRAPERS_DIR."""
    scrapers_dir = os.getenv("SCRAPERS_DIR", "/app/scrapers")
    try:
        return [
            f[:-3] for f in os.listdir(scrapers_dir)
            if f.endswith(".py") and not f.startswith("_")
        ]
    except (FileNotFoundError, OSError):
        return []


def _get_settings() -> tuple[int, int, int]:
    """Read queue tunables from settings with sensible defaults."""
    from core.config import get_setting
    def _ival(key: str, default: int) -> int:
        try:
            return int(get_setting(key, str(default)))
        except (ValueError, TypeError):
            return default
    lead = _ival("EVENT_RESOLVE_LEAD_MINUTES", 15)
    backoff = _ival("EVENT_RETRY_MINUTES", 5)
    max_attempts = _ival("EVENT_MAX_ATTEMPTS", 20)
    return lead, backoff, max_attempts


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _parse_dt(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return None


def upsert_events(scraper_name: str, events: list[dict]) -> dict:
    """Upsert a batch of discovered events into `scraped_events`.

    Returns a summary dict: {inserted, updated, skipped}.

    Semantics (per plan):
      - New row → status=pending, attempt_count=0
      - Existing row with status in {resolved, failed_final} → metadata refresh
        only (don't reset status; let cleanup/expire own its lifecycle)
      - Existing row otherwise → metadata refresh, status unchanged
    """
    from core.database import get_session
    from core.models import ScrapedEvent

    summary = {"inserted": 0, "updated": 0, "skipped": 0}
    if not events:
        return summary

    now = datetime.now(timezone.utc)
    with get_session() as session:
        for ev in events:
            url = ev.get("url")
            if not url:
                summary["skipped"] += 1
                continue
            url_hash = _md5(url)

            event_start = _parse_dt(ev.get("event_start")) or now
            event_end = _parse_dt(ev.get("event_end"))
            tags = list(ev.get("tags") or [])
            logo_urls = list(ev.get("logo_urls") or [])
            title = ev.get("title")

            if ev.get("event_start") is None:
                logger.info("[QUEUE] Event has no event_start, defaulting to now: %s", url)

            row = (
                session.query(ScrapedEvent)
                .filter(ScrapedEvent.scraper_name == scraper_name)
                .filter(ScrapedEvent.url_hash == url_hash)
                .first()
            )
            if row is None:
                row = ScrapedEvent(
                    scraper_name=scraper_name,
                    url=url,
                    url_hash=url_hash,
                    title=title,
                    tags=tags,
                    logo_urls=logo_urls,
                    event_start=event_start,
                    event_end=event_end,
                    status="pending",
                    attempt_count=0,
                    discovered_at=now,
                )
                session.add(row)
                summary["inserted"] += 1
            else:
                row.title = title or row.title
                row.tags = tags
                row.logo_urls = logo_urls
                row.event_start = event_start
                row.event_end = event_end
                summary["updated"] += 1

    if summary["inserted"] or summary["updated"]:
        logger.info("[QUEUE] Upserted %s: %d inserted, %d updated, %d skipped",
                    scraper_name, summary["inserted"], summary["updated"], summary["skipped"])
    return summary


def resolve_due_events():
    """Select pending events whose kickoff is within the lead window and hand
    them to `ManifestResolverService.resolve_batch`. Reconcile statuses from
    the batch result."""
    from core.database import get_session
    from core.models import ScrapedEvent

    lead_minutes, backoff_minutes, max_attempts = _get_settings()
    now = datetime.now(timezone.utc)
    _last_tick["time"] = now.isoformat()
    _last_tick["picked"] = 0
    _last_tick["resolved"] = 0
    _last_tick["failed"] = 0

    # Self-heal: any row stuck in 'resolving' for > 15 min is an orphan from a
    # JIT process that was killed mid-batch (container restart, OOM) before
    # reaching the reconcile block. Revert to pending so the next tick retries.
    # 15 min is safely larger than the longest realistic batch (sidecar timeout
    # is 90s per URL, JIT max_instances=1 caps concurrent batches).
    stale_cutoff = now - timedelta(minutes=15)
    url_list: list[dict] = []
    url_to_event: dict[str, str] = {}

    with get_session() as session:
        stale = (
            session.query(ScrapedEvent)
            .filter(ScrapedEvent.status == "resolving")
            .filter(ScrapedEvent.last_attempt_at < stale_cutoff)
            .all()
        )
        if stale:
            for ev in stale:
                ev.status = "pending"
                ev.last_error = "Reconcile lost — JIT process killed mid-batch"
            logger.info("[QUEUE] Self-healed %d stuck 'resolving' rows back to pending", len(stale))

        candidates = (
            session.query(ScrapedEvent)
            .filter(ScrapedEvent.status == "pending")
            .filter(ScrapedEvent.event_start < now + timedelta(minutes=lead_minutes))
            .filter(
                or_(
                    ScrapedEvent.last_attempt_at.is_(None),
                    ScrapedEvent.last_attempt_at < now - timedelta(minutes=backoff_minutes),
                )
            )
            .order_by(ScrapedEvent.event_start.asc())
            .all()
        )

        if not candidates:
            return

        for ev in candidates:
            ev.status = "resolving"
            ev.attempt_count = (ev.attempt_count or 0) + 1
            ev.last_attempt_at = now
            url_to_event[ev.url] = ev.id
            url_list.append({
                "url": ev.url,
                "title": ev.title,
                "tags": list(ev.tags or []),
                "event_start": ev.event_start.isoformat() if ev.event_start else None,
                "event_end": ev.event_end.isoformat() if ev.event_end else None,
                "logo_urls": list(ev.logo_urls or []),
            })

    _last_tick["picked"] = len(url_list)
    logger.info("[QUEUE] JIT resolve: %d events due (lead=%dm, backoff=%dm)",
                len(url_list), lead_minutes, backoff_minutes)

    from core.resolver.manifest_resolver import ManifestResolverService
    ManifestResolverService.resolve_batch(url_list, auto_create=True)

    # Reconcile — match on URL (our submitted list is authoritative)
    batch = ManifestResolverService.get_batch_status()
    results_by_url = {r["url"]: r for r in batch.get("results", [])}

    with get_session() as session:
        for submitted in url_list:
            event_id = url_to_event.get(submitted["url"])
            if not event_id:
                continue
            ev = session.query(ScrapedEvent).filter_by(id=event_id).first()
            if ev is None:
                continue
            result = results_by_url.get(submitted["url"])
            if not result:
                # Batch state got overwritten by a concurrent run; roll back
                # to pending so the next tick retries.
                ev.status = "pending"
                continue
            if result.get("status") == "done":
                ev.status = "resolved"
                ev.channel_id = result.get("channel_id")
                ev.last_error = None
                _last_tick["resolved"] += 1
            else:
                ev.last_error = result.get("error")
                if ev.attempt_count >= max_attempts:
                    ev.status = "failed_final"
                else:
                    ev.status = "pending"
                _last_tick["failed"] += 1


def expire_stale_events():
    """Mark pending/failed rows whose event_end has passed as expired.

    Skips rows without an event_end — those are effectively 24/7 streams
    and stay pending/resolved until explicitly dismissed or deleted.
    """
    from core.database import get_session
    from core.models import ScrapedEvent

    now = datetime.now(timezone.utc)
    with get_session() as session:
        rows = (
            session.query(ScrapedEvent)
            .filter(ScrapedEvent.event_end.isnot(None))
            .filter(ScrapedEvent.event_end < now)
            .filter(ScrapedEvent.status.in_(["pending", "failed", "resolving"]))
            .all()
        )
        expired = 0
        for ev in rows:
            ev.status = "expired"
            expired += 1
    if expired:
        logger.info("[QUEUE] Expired %d stale scraped_events rows", expired)


def backfill_from_channels():
    """Seed `scraped_events` from existing scraper-origin channels.

    Runs at most once — skips if the table already has rows. For each channel
    with a manifest_id whose channel looks scraper-origin (has event_start OR
    has an auto-cleanup tag), inserts a `resolved` row pointing at it.
    """
    try:
        from core.database import get_session
        from core.models import Channel as ChannelRow, Manifest, ScrapedEvent
        from core.models.manifest import Capture
        from core.config import get_tag_config
    except Exception as e:
        logger.warning("[QUEUE] Backfill skipped, DB modules unavailable: %s", e)
        return

    tag_config = get_tag_config()
    cleanup_tags = {tag for tag, cfg in tag_config.items() if cfg.get("auto_cleanup")}

    with get_session() as session:
        existing = session.query(ScrapedEvent.id).limit(1).first()
        if existing is not None:
            return

        rows = (
            session.query(ChannelRow, Manifest, Capture)
            .join(Manifest, ChannelRow.manifest_id == Manifest.id)
            .outerjoin(Capture, Manifest.capture_id == Capture.id)
            .filter(ChannelRow.manifest_id.isnot(None))
            .all()
        )

        inserted = 0
        for ch, m, cap in rows:
            channel_tags = set(ch.tags or [])
            looks_scraped = ch.event_start is not None or bool(channel_tags & cleanup_tags)
            if not looks_scraped:
                continue

            page_url = cap.page_url if cap else None
            if not page_url:
                continue

            scraper_name = "legacy"
            for t in channel_tags:
                tl = t.lower()
                matched = False
                for plugin_name in _list_scraper_plugins():
                    if plugin_name.lower() in tl:
                        scraper_name = plugin_name
                        matched = True
                        break
                if matched:
                    break

            ev = ScrapedEvent(
                scraper_name=scraper_name,
                url=page_url,
                url_hash=_md5(page_url),
                title=ch.name,
                tags=list(channel_tags),
                logo_urls=[],
                event_start=ch.event_start,
                event_end=ch.event_end,
                status="resolved",
                channel_id=ch.id,
                discovered_at=ch.created_at or datetime.now(timezone.utc),
            )
            session.add(ev)
            inserted += 1

        if inserted:
            logger.info("[QUEUE] Backfilled %d scraped_events rows from existing channels", inserted)
