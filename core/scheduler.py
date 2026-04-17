"""Central APScheduler for all background tasks.

Provides get_jobs_info(), update_job_interval(), and run_job_now()
for the Tasks UI — matching manifold's pattern.
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None

# Display names for the Tasks UI
TASK_NAMES = {
    "stats_collector": "Stats Collector",
    "stream_cleanup": "Stream Idle Cleanup",
    "event_cleanup": "Event Cleanup",
    "yt_cache_worker": "YouTube Pre-Cache",
    "vpn_sampler": "VPN Latency Sampler",
    "vpn_auto_rotate": "VPN Auto-Rotate",
}


def get_scheduler() -> BackgroundScheduler:
    """Return the global scheduler, creating it if needed."""
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler(daemon=True)
        _scheduler.start()
        logger.info("[SCHEDULER] Started global scheduler")
    return _scheduler


def add_job(job_id: str, func, seconds: int, **kwargs):
    """Add an interval job to the scheduler."""
    sched = get_scheduler()
    sched.add_job(
        func, "interval", seconds=seconds,
        id=job_id, name=TASK_NAMES.get(job_id, job_id),
        replace_existing=True, **kwargs,
    )
    logger.info("[SCHEDULER] Added job %s (every %ds)", job_id, seconds)


def get_jobs_info() -> list[dict]:
    """Return job details for the Tasks API."""
    if not _scheduler or not _scheduler.running:
        return []
    jobs = []
    for job in _scheduler.get_jobs():
        trigger = job.trigger
        interval = None
        if isinstance(trigger, IntervalTrigger):
            interval = int(trigger.interval.total_seconds())
        jobs.append({
            "id": job.id,
            "name": TASK_NAMES.get(job.id, job.name or job.id),
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "interval_seconds": interval,
        })
    return jobs


def update_job_interval(job_id: str, seconds: int) -> bool:
    """Update a job's interval."""
    if not _scheduler:
        return False
    job = _scheduler.get_job(job_id)
    if not job:
        return False
    _scheduler.reschedule_job(job_id, trigger="interval", seconds=seconds)
    logger.info("[SCHEDULER] Rescheduled %s to every %ds", job_id, seconds)
    return True


def run_job_now(job_id: str) -> bool:
    """Trigger a job to run immediately (in addition to its schedule)."""
    if not _scheduler:
        return False
    job = _scheduler.get_job(job_id)
    if not job:
        return False
    job.func(*job.args, **job.kwargs)
    return True
