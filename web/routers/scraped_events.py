"""Admin API for the scraped_events queue (JIT event resolver)."""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import func

router = APIRouter(tags=["scraped-events"])


def _row_to_dict(row) -> dict:
    return {
        "id": row.id,
        "scraper_name": row.scraper_name,
        "url": row.url,
        "title": row.title,
        "tags": row.tags or [],
        "logo_urls": row.logo_urls or [],
        "event_start": row.event_start.isoformat() if row.event_start else None,
        "event_end": row.event_end.isoformat() if row.event_end else None,
        "status": row.status,
        "channel_id": row.channel_id,
        "attempt_count": row.attempt_count or 0,
        "last_attempt_at": row.last_attempt_at.isoformat() if row.last_attempt_at else None,
        "last_error": row.last_error,
        "discovered_at": row.discovered_at.isoformat() if row.discovered_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@router.get("/scraped-events")
def list_scraped_events(
    scraper: str | None = Query(None),
    status: list[str] | None = Query(None),
    window: str = Query("upcoming"),
    limit: int = Query(500, ge=1, le=5000),
):
    """List scraped_events with optional filters.

    - `scraper`: filter by scraper_name
    - `status`: repeatable, e.g. status=pending&status=resolving
    - `window`: 'upcoming' (event_end >= now OR null), '24h' (starts within 24h),
      'all' (no time filter)
    """
    from core.database import get_session
    from core.models import ScrapedEvent
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    with get_session() as session:
        q = session.query(ScrapedEvent)
        if scraper:
            q = q.filter(ScrapedEvent.scraper_name == scraper)
        if status:
            q = q.filter(ScrapedEvent.status.in_(status))
        if window == "upcoming":
            q = q.filter(
                (ScrapedEvent.event_end.is_(None)) |
                (ScrapedEvent.event_end >= now)
            )
        elif window == "24h":
            q = q.filter(ScrapedEvent.event_start.isnot(None))
            q = q.filter(ScrapedEvent.event_start <= now + timedelta(hours=24))
            q = q.filter(ScrapedEvent.event_start >= now - timedelta(hours=24))
        # window == "all" → no filter

        rows = (
            q.order_by(ScrapedEvent.event_start.asc().nullslast())
             .limit(limit)
             .all()
        )
        return {"results": [_row_to_dict(r) for r in rows]}


@router.get("/scraped-events/jit-status")
def scraped_events_jit_status():
    """Status of the JIT event resolver for the Scrapers-tab health badge.

    Returns: enabled flag (job registered), next_run_time (when the next tick
    fires), last_tick snapshot (time/picked/resolved/failed from the last run).
    """
    from core.event_resolver import _last_tick
    enabled = False
    next_run_time = None
    try:
        from core.scheduler import get_scheduler
        sched = get_scheduler()
        job = sched.get_job("event_resolver")
        if job:
            enabled = True
            if job.next_run_time:
                next_run_time = job.next_run_time.isoformat()
    except Exception:
        pass
    return {
        "enabled": enabled,
        "next_run_time": next_run_time,
        "last_tick": dict(_last_tick),
    }


@router.get("/scraped-events/summary")
def scraped_events_summary():
    """Per-scraper counts for the scraper card summary line.

    Returns: {scraper: {pending, resolving, resolved_24h, failed, failed_final, expired}}
    """
    from core.database import get_session
    from core.models import ScrapedEvent
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)

    with get_session() as session:
        # Broad counts by scraper+status across all rows
        rows = (
            session.query(
                ScrapedEvent.scraper_name,
                ScrapedEvent.status,
                func.count(ScrapedEvent.id),
            )
            .group_by(ScrapedEvent.scraper_name, ScrapedEvent.status)
            .all()
        )
        # Resolved-in-last-24h uses updated_at as the resolution proxy
        resolved_24h_rows = (
            session.query(
                ScrapedEvent.scraper_name,
                func.count(ScrapedEvent.id),
            )
            .filter(ScrapedEvent.status == "resolved")
            .filter(ScrapedEvent.updated_at >= cutoff_24h)
            .group_by(ScrapedEvent.scraper_name)
            .all()
        )

    summary: dict[str, dict] = {}
    for scraper, status, count in rows:
        s = summary.setdefault(scraper, {
            "pending": 0, "resolving": 0, "resolved": 0,
            "failed": 0, "failed_final": 0, "expired": 0,
        })
        s[status] = count
    for scraper, count in resolved_24h_rows:
        summary.setdefault(scraper, {
            "pending": 0, "resolving": 0, "resolved": 0,
            "failed": 0, "failed_final": 0, "expired": 0,
        })
        summary[scraper]["resolved_24h"] = count
    for s in summary.values():
        s.setdefault("resolved_24h", 0)

    return {"scrapers": summary}


@router.post("/scraped-events/{event_id}/resolve")
def scraped_events_resolve_now(event_id: str):
    """Force an immediate retry by resetting last_attempt_at so the JIT loop
    picks the row up on its next tick (or manual trigger)."""
    from core.database import get_session
    from core.models import ScrapedEvent

    with get_session() as session:
        ev = session.query(ScrapedEvent).filter_by(id=event_id).first()
        if ev is None:
            raise HTTPException(status_code=404, detail="scraped event not found")
        if ev.status in ("resolved", "resolving"):
            return JSONResponse(
                {"error": f"cannot resolve: status={ev.status}"}, status_code=409
            )
        ev.status = "pending"
        ev.last_attempt_at = None
        ev.last_error = None
    return {"ok": True, "id": event_id}


@router.post("/scraped-events/{event_id}/dismiss")
def scraped_events_dismiss(event_id: str):
    """Transition to failed_final so the JIT loop skips future attempts."""
    from core.database import get_session
    from core.models import ScrapedEvent

    with get_session() as session:
        ev = session.query(ScrapedEvent).filter_by(id=event_id).first()
        if ev is None:
            raise HTTPException(status_code=404, detail="scraped event not found")
        ev.status = "failed_final"
    return {"ok": True, "id": event_id}


@router.delete("/scraped-events/{event_id}")
def scraped_events_delete(event_id: str):
    """Permanently remove the row. Does not touch any associated Channel."""
    from core.database import get_session
    from core.models import ScrapedEvent

    with get_session() as session:
        ev = session.query(ScrapedEvent).filter_by(id=event_id).first()
        if ev is None:
            raise HTTPException(status_code=404, detail="scraped event not found")
        session.delete(ev)
    return {"ok": True, "deleted": event_id}
