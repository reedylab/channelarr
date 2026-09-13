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


def record_possible_block(domain: str) -> None:
    """Call this from a genuine connection-level failure only (refused/
    reset/DNS-failed) -- see module docstring for why generic HTTP errors
    don't belong here."""
    if not domain:
        return
    with _lock:
        _recent[domain] = time.time()


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
