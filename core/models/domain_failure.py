"""Persisted per-domain failure history backing core/block_detector.py's
sustained-failure tracking and core/source_registry.py's auto-hold.

Was in-memory only until 2026-09-15 -- confirmed real problem: every
channelarr process restart (deploys, crashes, whatever) wiped this
tracking, so a domain that had already proven itself sustained-bad got a
fresh ~5-minute grace window every single restart before auto-hold could
kick in again, during which the refresh cycle and fallback-race mechanism
would freely re-hammer it. Restarts happen often enough in this project's
normal operation (multiple per session some nights) that this was a real,
recurring gap, not a theoretical one.

Deliberately does NOT persist block_detector's OTHER piece of state
(_recent / the short 180s cross-domain IP-block correlation window) --
that signal is inherently about "right now," a restart mid-window loses at
most a few seconds of context before fresh failures repopulate it anyway,
and persisting it would add write volume for no real benefit.
"""

from sqlalchemy import Column, String, Integer, Float

from core.models.base import Base


class DomainFailure(Base):
    __tablename__ = "domain_failures"

    # Plain epoch floats (time.time()), not DateTime -- matches
    # block_detector.py's existing convention throughout this module, and
    # keeps every downstream consumer (core/source_registry.py's duration
    # math, web/static/ui.js's relTimeAgo) unchanged.
    domain = Column(String, primary_key=True)
    first_failure_at = Column(Float, nullable=False)
    last_failure_at = Column(Float, nullable=False)
    consecutive_failures = Column(Integer, nullable=False, default=0)
    last_error = Column(String, nullable=True)
