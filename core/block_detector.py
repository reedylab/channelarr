"""Lightweight, in-memory cross-domain IP-block detection signal.

Real motivation: a real session found our own VPN exit IP specifically
blocked by more than one source's anti-bot/anti-DDoS defenses, independently,
within the same short window -- confirmed directly by rotating and watching
the exact same URL go from a hard connection refusal to a real 200 with
nothing else changed. A SINGLE domain refusing us could be that one domain
having a bad day, unrelated to our own network identity. Two or more
INDEPENDENT domains refusing us within a short window is a much stronger,
specific signal that it's genuinely our own exit IP that's the problem --
worth auto-rotating for.

Deliberately narrow: callers should only record a genuine connection-level
failure (refused/reset/DNS-failed), never a generic 403/404/500/timeout --
those are much more likely to be real, unrelated content-side issues and
would make this fire on ordinary flakiness rather than an actual IP block.

The short-window cross-domain correlation (_recent / distinct_recent_
blocked_domains / looks_like_ip_block) stays in-memory only, no I/O --
it's inherently about "right now," cheap to rebuild from fresh failures
after a restart, and called from hot paths where added DB latency isn't
worth it for a few seconds of lost context.

The per-domain sustained-failure history (record_possible_block/
record_success/get_all_domain_status) IS persisted to Postgres as of
2026-09-15 -- confirmed real problem: it used to be in-memory only, so
every channelarr process restart wiped a domain's proven-bad track record
and reopened the ~5min grace window core/source_registry.py's auto-hold
needs before it kicks in again, during which the refresh cycle would
freely re-hammer a domain that had already demonstrated it was bad before
the restart. DB writes are best-effort (wrapped, logged, never raised) so
a Postgres hiccup can't break a caller in a hot exception-handler path.
"""

import logging
import threading
import time

logger = logging.getLogger(__name__)

# How far back "recent" reaches when deciding if multiple domains are
# currently blocking us at the same time, vs. two unrelated blocks hours
# apart that don't really indicate anything about right now.
WINDOW_SECONDS = 180

# This many independently-blocked domains within the window is treated as
# "our own exit IP is blocked," not coincidence. 2 is deliberately low --
# false positives here just cost one extra VPN rotation (cheap, self-
# correcting), while false negatives mean sitting blocked for longer than
# necessary.
MIN_DISTINCT_DOMAINS = 2

_lock = threading.Lock()
_recent: dict[str, float] = {}  # domain -> last-seen-blocked-at (monotonic-ish, time.time())

# Per-domain sustained-failure history is now the Postgres domain_failures
# table (core/models/domain_failure.py) -- source of truth, not a cache, so
# it survives process restarts. The table stays tiny (a handful of rows at
# any time, pruned same as before) so a full read per call is cheap; this
# module doesn't keep its own in-memory copy to avoid any cache-
# invalidation complexity. Deliberately NOT auto-pruned on the same short
# WINDOW_SECONDS as _recent -- a domain that's been failing for an hour
# should still show that full streak, not just whatever's left in the last
# 180s. Pruned instead by DOMAIN_HISTORY_STALE_SECONDS (a success, or just
# enough quiet time, retires a row).
DOMAIN_HISTORY_STALE_SECONDS = 3600  # 1h with no new failure -> drop it


def record_possible_block(domain: str, error: str | None = None) -> None:
    """Call this from a genuine connection-level failure only (refused/
    reset/DNS-failed) -- see module docstring for why generic HTTP errors
    don't belong here. `error` is optional (most call sites already have
    the exception message in hand) -- purely informational, shown on the
    Diagnostics Sources panel, never used for any decision-making here.

    The Postgres write is best-effort: wrapped, logged, never raised, so a
    DB hiccup can't break whatever hot exception-handler path called this."""
    if not domain:
        return
    now = time.time()
    with _lock:
        _recent[domain] = now
    try:
        from core.database import get_session
        from core.models.domain_failure import DomainFailure
        with get_session() as session:
            row = session.query(DomainFailure).filter_by(domain=domain).first()
            if row is None:
                session.add(DomainFailure(domain=domain, first_failure_at=now,
                                          last_failure_at=now, consecutive_failures=1,
                                          last_error=error))
            else:
                row.last_failure_at = now
                row.consecutive_failures += 1
                row.last_error = error
    except Exception as e:
        logger.warning("[BLOCK_DETECTOR] Failed to persist failure for %s: %s", domain, e)


def record_success(domain: str) -> None:
    """Call this once a request to `domain` genuinely succeeds -- ends
    whatever failure streak was tracked for it. Separate from _recent (the
    cross-domain block-signal window) deliberately -- a single success
    doesn't retroactively un-flag an IP block that was real a moment ago,
    it just means THIS domain specifically is reachable again right now."""
    if not domain:
        return
    try:
        from core.database import get_session
        from core.models.domain_failure import DomainFailure
        with get_session() as session:
            session.query(DomainFailure).filter_by(domain=domain).delete()
    except Exception as e:
        logger.warning("[BLOCK_DETECTOR] Failed to clear failure history for %s: %s", domain, e)


def get_all_domain_status() -> list[dict]:
    """Snapshot of every domain with a recent failure streak, for the
    Diagnostics Sources panel and core/source_registry.py's auto-hold.
    Prunes anything stale (no new failure in DOMAIN_HISTORY_STALE_SECONDS)
    while it's at it. Returns dicts with domain, first_failure_at,
    last_failure_at, consecutive_failures, last_error -- deliberately NOT a
    "status" verdict (down/blocked/ok) -- see this module's own docstring
    on why that distinction can't be determined from our own network's
    failures alone; the UI shows the raw signal and leaves the judgment
    call to a human. Returns [] (not a raised exception) on a DB error --
    callers (including the resolve-time enforcement path) must never break
    because this read failed."""
    try:
        from core.database import get_session
        from core.models.domain_failure import DomainFailure
        cutoff = time.time() - DOMAIN_HISTORY_STALE_SECONDS
        with get_session() as session:
            session.query(DomainFailure).filter(DomainFailure.last_failure_at < cutoff).delete()
            rows = session.query(DomainFailure).all()
            return [dict(domain=r.domain, first_failure_at=r.first_failure_at,
                         last_failure_at=r.last_failure_at,
                         consecutive_failures=r.consecutive_failures,
                         last_error=r.last_error) for r in rows]
    except Exception as e:
        logger.warning("[BLOCK_DETECTOR] Failed to read failure history: %s", e)
        return []


def distinct_recent_blocked_domains() -> list[str]:
    """Domains that recorded a block signal within WINDOW_SECONDS, right
    now. Also prunes anything older than that while it's at it, so this
    dict can't grow unbounded over a long uptime."""
    cutoff = time.time() - WINDOW_SECONDS
    with _lock:
        stale = [d for d, ts in _recent.items() if ts < cutoff]
        for d in stale:
            _recent.pop(d, None)
        return list(_recent.keys())


def looks_like_ip_block() -> bool:
    return len(distinct_recent_blocked_domains()) >= MIN_DISTINCT_DOMAINS
