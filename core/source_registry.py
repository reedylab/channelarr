"""Plugin-declared Source registry + manual/automated enable state.

Pass 2 of the Diagnostics "Sources" panel (see Pass 1's
core/block_detector.py). Plugins in scrapers/
(gitignored, site-specific -- see CLAUDE.md's public-repo-hygiene rule) may
declare a module-level SOURCE_INFO advertising what they resolve:

    SOURCE_INFO = {
        "source_id": "some_plugin_247",    # stable, used as the toggle key
        "display_name": "Some Plugin 24/7",  # shown in the Sources panel
        "categories": ["247"],             # "247" | "live_events" (informational)
        "domains": ["example.com"],        # substring-matched, see _domain_matches
    }

A module can also export a LIST of such dicts if it has more than one
independently-toggleable backend (e.g. a 24/7 CDN and a separate live-event
CDN behind the same site) -- domains/categories should line up 1:1 per
entry so a toggle only ever gates the backend it actually names. No source
names live in this file itself -- it only ever iterates over whatever
scrapers/ happens to declare, discovered the same way core/scraper_runner.py
already discovers scrape()-exporting modules.

Enable state has two independent layers, ANDed together at read time:
  - manual_enabled: a human's own on/off call for a source_id, persisted in
    core.models.source_toggle.SourceToggle, defaults True, NEVER written by
    anything in this module -- only set_manual_enabled(), which only the
    Sources panel's toggle UI calls.
  - auto_held: NOT persisted anywhere. Computed live, every call, from
    core.block_detector's existing per-domain failure history -- so it
    self-heals the instant a real success is recorded (block_detector.
    record_success() simply drops the domain from its history, which drops
    it from auto-held here too, no separate write path to keep in sync).
    This deliberately does NOT try to classify "down" vs "blocked" -- see
    block_detector's own module docstring for why that distinction can't be
    made from our own network's failures alone. auto_held only ever means
    "sustained recent failures, stop hammering this for now" -- a temporary
    back-off signal, not a verdict. It can never be permanent and never
    needs a human to clear it.
"""

import importlib.util
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/app/scrapers")

# Sustained-failure threshold for auto_held -- deliberately both a count AND
# a duration, so a single noisy blip (a few fast failures in the first
# second) doesn't trip it, but it also doesn't wait forever on a source
# that's failing steadily. Mirrors the same "don't overreact to one bad
# moment" caution as block_detector's own MIN_DISTINCT_DOMAINS.
AUTO_HOLD_MIN_CONSECUTIVE_FAILURES = 5
AUTO_HOLD_MIN_DURATION_SECONDS = 300  # 5 minutes of continuous failure

# Once held, is_domain_enabled() skips the caller's network call entirely --
# which means block_detector's failure/success history for that domain
# stops updating too (nothing left to record), freezing it "held" for a
# full DOMAIN_HISTORY_STALE_SECONDS (1h) even if the underlying source
# would have worked on the very next attempt. Confirmed real 2026-09-14:
# a channel resolved successfully MINUTES before its own domain tripped
# back into auto_held on the next failure streak -- proving a real,
# non-trivial hit rate, not "hard down." A pure hold can't tell
# "mostly down" from "fully down" apart, so it lets exactly one probe
# through per cooldown window instead of a blind hour-long blackout --
# still far less frequent than an unthrottled retry storm, but detects
# real recovery in minutes instead of up to an hour.
AUTO_HOLD_PROBE_COOLDOWN_SECONDS = 120

_lock = threading.Lock()
_declared_cache: list[dict] | None = None
_last_probe_allowed_at: dict[str, float] = {}


def _load_module(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _normalize(raw, module_name: str) -> list[dict]:
    entries = raw if isinstance(raw, list) else [raw]
    out = []
    for e in entries:
        if not isinstance(e, dict) or not e.get("source_id"):
            logger.warning("[SOURCES] %s: SOURCE_INFO entry missing source_id, skipping", module_name)
            continue
        out.append({
            "source_id": e["source_id"],
            "display_name": e.get("display_name") or e["source_id"],
            "categories": list(e.get("categories") or []),
            "domains": list(e.get("domains") or []),
        })
    return out


def _load_declared_sources(force: bool = False) -> list[dict]:
    """Scan every *.py file in scrapers/ (including `_`-prefixed config-only
    modules -- that's exactly where some of the native-resolver-family
    plugins declare themselves) for a module-level SOURCE_INFO. Cached
    after first load, same convention as manifest_resolver.py's own
    _native_resolver() caching -- scrapers/ doesn't change without a
    redeploy, so this doesn't need to re-scan disk on every resolve call.
    Pass force=True (from the Sources panel's own refresh, if ever added)
    to re-scan without a restart.
    """
    global _declared_cache
    with _lock:
        if _declared_cache is not None and not force:
            return _declared_cache
        found: list[dict] = []
        seen_ids: dict[str, str] = {}
        if os.path.isdir(SCRAPERS_DIR):
            for fname in sorted(os.listdir(SCRAPERS_DIR)):
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(SCRAPERS_DIR, fname)
                try:
                    mod = _load_module(path, f"source_registry_{fname[:-3]}")
                except Exception as e:
                    logger.warning("[SOURCES] Failed to load %s for SOURCE_INFO: %s", fname, e)
                    continue
                raw = getattr(mod, "SOURCE_INFO", None)
                if raw is None:
                    continue
                for entry in _normalize(raw, fname):
                    prior = seen_ids.get(entry["source_id"])
                    if prior:
                        logger.warning("[SOURCES] source_id %r declared in both %s and %s -- "
                                       "keeping the first, skipping the duplicate",
                                       entry["source_id"], prior, fname)
                        continue
                    seen_ids[entry["source_id"]] = fname
                    found.append(entry)
        _declared_cache = found
        logger.info("[SOURCES] Discovered %d declared source(s) from %s", len(found), SCRAPERS_DIR)
        return found


def _domain_matches(candidate: str, declared: str) -> bool:
    """Plain substring, either direction -- same convention
    _native_resolvers.py's _ENTRY_HOSTS already uses (`h in host`), which
    is deliberately what lets a declared prefix pattern like "example."
    match any TLD ("example.pk", "example.st", ...) the same way
    _ENTRY_HOSTS does, as well as a declared apex ("example.com") matching
    a subdomain-bearing observed host ("www.example.com") without keeping
    exact domain lists in lockstep."""
    if not candidate or not declared:
        return False
    candidate, declared = candidate.lower(), declared.lower()
    return declared in candidate or candidate in declared


def find_sources_for_domain(domain: str) -> list[dict]:
    """Every declared source with at least one domain matching `domain`."""
    if not domain:
        return []
    return [s for s in _load_declared_sources()
            if any(_domain_matches(domain, d) for d in s["domains"])]


def _get_toggle_map() -> dict[str, bool]:
    from core.database import get_session
    from core.models.source_toggle import SourceToggle
    try:
        with get_session() as session:
            rows = session.query(SourceToggle.source_id, SourceToggle.manual_enabled).all()
            return {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.warning("[SOURCES] Failed to read toggle state, defaulting everything enabled: %s", e)
        return {}


def set_manual_enabled(source_id: str, enabled: bool) -> None:
    """The only write path into SourceToggle -- called exclusively by the
    Sources panel's own toggle action (a human clicking it), never by
    automation. See this module's docstring for why automated back-off is
    deliberately kept separate (auto_held, computed live) instead of also
    writing here."""
    from core.database import get_session
    from core.models.source_toggle import SourceToggle
    with get_session() as session:
        row = session.query(SourceToggle).filter_by(source_id=source_id).first()
        if row:
            row.manual_enabled = enabled
        else:
            session.add(SourceToggle(source_id=source_id, manual_enabled=enabled))
    logger.info("[SOURCES] %s manually %s", source_id, "enabled" if enabled else "disabled")


def _auto_held_domains() -> dict[str, dict]:
    """Domains currently meeting the sustained-failure threshold, per
    block_detector's live in-memory history. Self-healing by construction:
    the moment block_detector.record_success() fires for a domain, it drops
    out of get_all_domain_status() entirely, so it drops out of this set on
    the very next call too -- no separate reset path needed."""
    from core.block_detector import get_all_domain_status
    held = {}
    for status in get_all_domain_status():
        if status["consecutive_failures"] < AUTO_HOLD_MIN_CONSECUTIVE_FAILURES:
            continue
        duration = status["last_failure_at"] - status["first_failure_at"]
        if duration < AUTO_HOLD_MIN_DURATION_SECONDS:
            continue
        held[status["domain"]] = status
    return held


def _probe_allowed(domain: str) -> bool:
    """Rate-limited escape hatch for an auto_held domain: lets exactly one
    caller through per AUTO_HOLD_PROBE_COOLDOWN_SECONDS so real recovery
    gets detected instead of a domain sitting fully blacked out until it
    goes stale. See AUTO_HOLD_PROBE_COOLDOWN_SECONDS's own comment for why
    this exists. Keyed under _lock so a burst of concurrent callers (e.g.
    several race candidates checking at once) only ever lets ONE through
    per window, not all of them at once."""
    now = time.time()
    with _lock:
        last = _last_probe_allowed_at.get(domain, 0.0)
        if now - last < AUTO_HOLD_PROBE_COOLDOWN_SECONDS:
            return False
        _last_probe_allowed_at[domain] = now
        return True


def is_domain_enabled(domain: str) -> tuple:
    """(enabled, reason) for a domain about to be resolved/served. Reason is
    None when enabled, else a short human-readable string. A domain with no
    declared source at all always passes through enabled -- this can only
    gate sources that actually declared themselves, never an unknown host.

    An auto_held domain isn't a hard block -- see _probe_allowed's own
    docstring. If the caller's attempt succeeds, block_detector.
    record_success() clears the domain's failure history and it drops out
    of _auto_held_domains() on the very next check; if it fails, the normal
    record_possible_block() call refreshes last_failure_at and the domain
    stays held for another cooldown window."""
    if not domain:
        return True, None
    sources = find_sources_for_domain(domain)
    if not sources:
        return True, None
    toggles = _get_toggle_map()
    for s in sources:
        if toggles.get(s["source_id"], True) is False:
            return False, f"manually disabled: {s['display_name']}"
    held = _auto_held_domains()
    for held_domain in held:
        if _domain_matches(domain, held_domain):
            if _probe_allowed(held_domain):
                logger.info("[SOURCES] %s: auto_held but cooldown elapsed -- letting one probe through", held_domain)
                return True, None
            return False, f"auto-held: sustained failures on {held_domain}"
    return True, None


def promote_source_to_primary(source_id: str) -> dict:
    """Bulk version of the existing per-channel "Make Primary" action: for
    every channel that currently carries a fallback manifest from this
    declared source, promote that fallback to primary. A human's explicit
    "I know this source is good" call, not something automation decides --
    e.g. after a fresh, verified enrichment pass adds a new independent
    path to a bunch of channels and the human wants to lead with it
    everywhere at once instead of clicking "Make Primary" one channel at
    a time.

    Reuses ChannelManager.set_primary_manifest() as-is -- same safety
    property applies here: the old primary isn't dropped, it gets pushed to
    the front of the fallback chain, so this is a cheap, reversible bulk
    experiment (call promote on whatever source WAS primary to revert any
    one channel), not a one-way door.

    Returns {"promoted": [{"channel_id", "name"}], "already_primary": int,
    "no_match": int} -- no_match counts declared-source channels where
    resolution found no matching fallback at all (shouldn't normally
    happen if list_source_status() shows channels using this source, but
    tracked rather than silently skipped)."""
    sources = [s for s in _load_declared_sources() if s["source_id"] == source_id]
    if not sources:
        return {"promoted": [], "already_primary": 0, "no_match": 0, "error": f"unknown source_id: {source_id}"}
    domains = sources[0]["domains"]

    from core.database import get_session
    from core.models.channel import Channel
    from core.models.manifest import Manifest, Capture
    from core.channels import ChannelManager

    channel_mgr = ChannelManager()
    promoted = []
    already_primary = 0
    no_match = 0

    with get_session() as session:
        manifest_domains = dict(
            session.query(Manifest.id, Capture.page_url)
            .join(Capture, Manifest.capture_id == Capture.id)
            .all()
        )
        rows = session.query(Channel.id, Channel.name, Channel.manifest_id,
                             Channel.fallback_manifest_ids).filter(
            Channel.type == "resolved").all()

    def _matches_source(manifest_id):
        page_url = manifest_domains.get(manifest_id)
        if not page_url:
            return False
        from urllib.parse import urlparse
        domain = urlparse(page_url).netloc
        return any(_domain_matches(domain, d) for d in domains)

    for channel_id, name, primary_id, fb_ids in rows:
        if primary_id and _matches_source(primary_id):
            already_primary += 1
            continue
        match = next((mid for mid in (fb_ids or []) if _matches_source(mid)), None)
        if not match:
            continue
        channel_mgr.set_primary_manifest(channel_id, match)
        promoted.append({"channel_id": channel_id, "name": name})

    logger.info("[SOURCES] Promoted %d channel(s) to primary for source %s (already_primary=%d)",
                len(promoted), source_id, already_primary)
    return {"promoted": promoted, "already_primary": already_primary, "no_match": no_match}


def list_source_status() -> list[dict]:
    """Full Sources panel view: every declared source, its manual + live
    auto-held state, and the effective (ANDed) result -- for the Diagnostics
    UI's toggle table."""
    toggles = _get_toggle_map()
    held = _auto_held_domains()
    out = []
    for s in _load_declared_sources():
        manual_enabled = toggles.get(s["source_id"], True)
        held_hit = next((d for d in s["domains"]
                          if any(_domain_matches(d, hd) for hd in held)), None)
        out.append({
            **s,
            "manual_enabled": manual_enabled,
            "auto_held": held_hit is not None,
            "auto_hold_domain": held_hit,
            "effective_enabled": manual_enabled and held_hit is None,
        })
    out.sort(key=lambda s: s["display_name"].lower())
    return out
