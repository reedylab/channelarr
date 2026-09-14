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

No I/O, no locks held across calls to anything else -- this is meant to be
called from hot paths (a resolver's own exception handler) without adding
any real cost or risk of its own.
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

# Richer per-domain history, kept alongside _recent rather than replacing
# it -- _recent/WINDOW_SECONDS stays exactly as-is (the cross-domain IP-
# block signal this module was built for), this is additive state for the
# Diagnostics "Sources" panel: how long has THIS domain specifically been
# failing, how many times in a row, what was the last error. Deliberately
# NOT auto-pruned on the same short WINDOW_SECONDS -- a domain that's been
# failing for an hour should still show that full streak, not just
# whatever's left in the last 180s. Pruned instead by DOMAIN_HISTORY_
# STALE_SECONDS (a success, or just enough quiet time, retires an entry).
DOMAIN_HISTORY_STALE_SECONDS = 3600  # 1h with no new failure -> drop it
_domain_history: dict[str, dict] = {}
# domain -> {"first_failure_at": float, "last_failure_at": float,
#            "consecutive_failures": int, "last_error": str|None}


def record_possible_block(domain: str, error: str | None = None) -> None:
    """Call this from a genuine connection-level failure only (refused/
    reset/DNS-failed) -- see module docstring for why generic HTTP errors
    don't belong here. `error` is optional (most call sites already have
    the exception message in hand) -- purely informational, shown on the
    Diagnostics Sources panel, never used for any decision-making here."""
    if not domain:
        return
    now = time.time()
    with _lock:
        _recent[domain] = now
        h = _domain_history.get(domain)
        if h is None:
            h = {"first_failure_at": now, "consecutive_failures": 0}
            _domain_history[domain] = h
        h["last_failure_at"] = now
        h["consecutive_failures"] += 1
        h["last_error"] = error


def record_success(domain: str) -> None:
    """Call this once a request to `domain` genuinely succeeds -- ends
    whatever failure streak _domain_history was tracking for it. Separate
    from _recent (the cross-domain block-signal window) deliberately --
    a single success doesn't retroactively un-flag an IP block that was
    real a moment ago, it just means THIS domain specifically is reachable
    again right now."""
    if not domain:
        return
    with _lock:
        _domain_history.pop(domain, None)


def get_all_domain_status() -> list[dict]:
    """Snapshot of every domain with a recent failure streak, for the
    Diagnostics Sources panel. Prunes anything stale (no new failure in
    DOMAIN_HISTORY_STALE_SECONDS) while it's at it. Returns dicts with
    domain, first_failure_at, last_failure_at, consecutive_failures,
    last_error -- deliberately NOT a "status" verdict (down/blocked/ok) --
    see this module's own docstring on why that distinction can't be
    determined from our own network's failures alone; the UI shows the
    raw signal and leaves the judgment call to a human."""
    cutoff = time.time() - DOMAIN_HISTORY_STALE_SECONDS
    with _lock:
        stale = [d for d, h in _domain_history.items() if h["last_failure_at"] < cutoff]
        for d in stale:
            _domain_history.pop(d, None)
        return [dict(domain=d, **h) for d, h in _domain_history.items()]


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
