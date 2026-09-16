"""Resolve m3u8 manifests via the selenium-uc sidecar.

The actual browser work happens in the sidecar (Chrome + undetected_chromedriver).
This service is a thin HTTP client that calls the sidecar and stores results
in channelarr's resolver Postgres tables (parallel to existing JSON storage).
"""

import hashlib
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urljoin

import requests as http_requests

from core.config import get_setting
from core.database import get_session
from core.models.manifest import Capture, Manifest, Variant, HeaderProfile
from core.resolver.expiry_parser import parse_expiry, parse_body_expiry

logger = logging.getLogger(__name__)

# Fallback expiry when a manifest's own body/URL carries no parseable expiry
# (parse_body_expiry returns None) -- used to schedule this manifest's next
# background refresh. Confirmed live 2026-09-15: real, direct "Connection
# refused" against a native-resolver entry apex was still happening even after
# fixing a per-request pacing/misattribution bug, with real fleet channels
# (dozens on this one apex) all defaulting to the same flat 30min window --
# meaning many channels' refreshes cluster into near-lockstep cycles
# regardless of how well-paced any ONE resolve's own requests are. Two
# independent levers: raised the base window (fewer refreshes overall,
# directly cutting total request volume against the apex) and added
# jitter (spreads WHEN different channels' windows land, so they don't
# resync into periodic mini-bursts the way a shared flat interval would).
_DEFAULT_EXPIRY_MINUTES = 60
_DEFAULT_EXPIRY_JITTER_MINUTES = 15


def _default_expiry(now: datetime) -> datetime:
    minutes = _DEFAULT_EXPIRY_MINUTES + random.uniform(
        -_DEFAULT_EXPIRY_JITTER_MINUTES, _DEFAULT_EXPIRY_JITTER_MINUTES)
    return now + timedelta(minutes=minutes)


# Status tracking for async resolve jobs
_status = {"running": False, "last_url": None, "last_error": None, "last_manifest_id": None}

# Batch state
_batch = {"running": False, "total": 0, "completed": 0, "current_url": None, "results": []}

# ── In-flight dedup ────────────────────────────────────────────────────
# If a resolve/refresh is already in progress for a page_url, subsequent
# callers wait for it instead of queuing another sidecar capture. This
# is still useful: it coalesces duplicate refresh triggers from the
# 403 safety net and the scheduled refresh worker colliding on the same
# URL. Without it they'd each kick off independent sidecar captures.
_inflight: dict[str, tuple] = {}  # page_url -> (Event, started_at_monotonic)
_inflight_results: dict[str, dict] = {}     # page_url -> result dict
_inflight_lock = threading.Lock()

# A per-URL in-flight entry held far longer than any legitimate resolve
# could take means the thread that created it is genuinely stuck (hung
# inside _call_sidecar with no client-side timeout ever firing -- confirmed
# live, 2026-09-13: one entry sat in-flight for over an hour, permanently
# pinning one of only RESOLVER_HIGH_SLOTS+RESOLVER_LOW_SLOTS pool permits,
# since pool.release() only runs after _call_sidecar returns, which it
# never did). Recurring under heavier load the next night: several entries
# stuck for minutes at once degraded pool capacity enough that fresh
# on-demand channel starts began failing outright. Generous multiple of
# the normal ~90-105s resolve/wait budget -- long enough that no
# legitimate resolve should ever hit it, short enough to actually reclaim
# a stuck slot instead of leaving it stuck forever.
_INFLIGHT_STALE_SECONDS = 180.0

# ── Pipeline lock ──────────────────────────────────────────────────────
# Acquired non-blocking by scheduled ticks (manifest_refresh, event_resolver)
# so only one of them is pumping work into the single-threaded sidecar at
# a time. User-driven resolves bypass this lock — they still serialize at
# the sidecar but shouldn't be blocked by an in-progress sweep.
#
# This whole model assumes a single-threaded sidecar (v1's one-browser-
# one-lock reality) -- see _HIGH_POOL/_LOW_POOL below for the concurrent
# alternative, used only when RESOLVER_CONCURRENCY_MODE="multi". In
# "single" mode (the default, and v1's only mode) pipeline_lock is used
# completely unchanged -- zero behavior difference from before this was
# added.
pipeline_lock = threading.Lock()

# ── Concurrency mode + priority-reserved slot pools ────────────────────
# "single" (default): every dispatch site below uses pipeline_lock exactly
# as before -- byte-identical to pre-existing behavior, the safe default
# and the only sane mode against v1 (one browser, one lock; concurrent
# callers would just queue there anyway with no benefit and added
# complexity). "multi": opt in once pointed at a sidecar that can actually
# run concurrent captures (sidecar 2.0) -- replaces pipeline_lock with two
# HARD-partitioned semaphores so background/keep-warm work can never
# starve live-viewer/JIT work by saturating a shared pool. Read fresh each
# call (get_setting is not cached, matches the existing SELENIUM_URL
# pattern) so flipping it takes effect live, no restart needed.
#
# Defaults (3 high / 5 low = 8 total) match sidecar 2.0's own validated
# SIDECAR_MAX_TABS=8 default exactly -- re-tune both together if that ever
# changes.
_HIGH_POOL = threading.Semaphore(int(get_setting("RESOLVER_HIGH_SLOTS", "3")))
_LOW_POOL = threading.Semaphore(int(get_setting("RESOLVER_LOW_SLOTS", "5")))


def _concurrency_mode() -> str:
    mode = (get_setting("RESOLVER_CONCURRENCY_MODE", "single") or "single").lower()
    return mode if mode in ("single", "multi") else "single"


def _pool_for_priority(priority: str):
    return _LOW_POOL if priority == "low" else _HIGH_POOL

# Cap on heavy sidecar refreshes per refresh tick. Light refreshes are cheap
# (HTTP only) and uncapped, but heavy refreshes launch Chrome and chew RAM.
# After a VPN rotation every session goes stale at once — without this cap a
# single tick would queue 10+ Chrome captures back-to-back and OOM the box.
# Kept well under that danger zone even after the bump (was 3) — the real
# fix for the backlog is the ORDER BY below (fair rotation through the
# candidate pool), this just raises throughput a bit on top of that.
#
# This is the SINGLE-mode budget only, unchanged — single mode's sequential
# per-item pipeline_lock loop means a bigger batch here directly multiplies
# one tick's own wall-clock (batch x ~60-105s each), which would delay every
# subsequent tick (max_instances=1) far longer than is safe. See
# HEAVY_REFRESH_BUDGET_PER_TICK_MULTI below for the mode-gated, much higher
# multi-mode equivalent -- raising THIS constant would still be unsafe even
# with multiplex's real concurrency, since single mode never uses the pool
# that concurrency depends on.
HEAVY_REFRESH_BUDGET_PER_TICK = 5

# Multi-mode-only budget, deliberately much higher than the single-mode one
# above. Safe to raise well past the pool size: submitting more than
# RESOLVER_HIGH_SLOTS+RESOLVER_LOW_SLOTS items to the tick's
# ThreadPoolExecutor doesn't cause extra concurrency (resolve()'s own pool
# acquire still throttles actual concurrent sidecar dispatch) -- it just
# means the excess queues on the pool WITHIN this same tick instead of
# waiting 3-4+ future ticks for its turn, which is exactly the "we're only
# ever reacting" gap this raises. Read live (get_setting, not a module-level
# constant) so it can be tuned without a restart, unlike the pool sizes
# below (which size actual Semaphore objects at import time).
def _heavy_refresh_budget_multi() -> int:
    return int(get_setting("HEAVY_REFRESH_BUDGET_PER_TICK_MULTI", "15"))


def _default_resolved_name(title: str | None, manifest_url: str, source_domain: str | None) -> str:
    """Pick a sensible display name for a resolved channel when the user
    didn't provide a title. Tries: explicit title → manifest URL hostname
    → source_domain → constant fallback."""
    if title and title.strip():
        return title.strip()
    try:
        host = urlparse(manifest_url).hostname or ""
        if host:
            return host
    except Exception:
        pass
    if source_domain:
        return source_domain
    return "Unnamed Resolved"


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _sha256(text: str | None) -> str | None:
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()


def _sanitize_body(text: str | None) -> str | None:
    """Strip control characters, keep #EXT lines intact."""
    if not text:
        return None
    sanitized = re.sub(r'[\x00-\x1F\x7F]', '', text)
    lines = sanitized.splitlines()
    clean = []
    for line in lines:
        if line.startswith('#EXT'):
            clean.append(line)
        else:
            clean.append(re.sub(r'[\x00-\x1F\x7F\x80-\xFF]', '', line))
    return '\n'.join(clean) or None


def _parse_master_variants(body_text: str, manifest_url: str) -> list[dict]:
    """Parse EXT-X-STREAM-INF entries from a master playlist."""
    if not body_text:
        return []
    lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]
    out = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        if ln.startswith("#EXT-X-STREAM-INF"):
            attrs = {}
            for kv in re.split(r',(?=[A-Z0-9\-]+=)', ln.split(":", 1)[1]):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    attrs[k] = v.strip('"')
            uri = lines[i + 1] if i + 1 < len(lines) else ""
            abs_url = urljoin(manifest_url, uri)
            res = attrs.get("RESOLUTION")
            w = h = None
            if res and "x" in res:
                try:
                    w, h = map(int, res.split("x"))
                except Exception:
                    pass
            out.append({
                "uri": uri,
                "abs_url": abs_url,
                "bandwidth": int(attrs.get("BANDWIDTH", "0") or 0),
                "resolution": res,
                "frame_rate": float(attrs.get("FRAME-RATE", "0") or 0),
                "codecs": attrs.get("CODECS"),
                "audio_group": attrs.get("AUDIO"),
                "width": w,
                "height": h,
            })
            i += 2
        else:
            i += 1
    return out


def refresh_due_manifests():
    """One refresh tick — selects manifests near expiry and refreshes them
    through the same sidecar pipeline used by user-driven resolves and the
    JIT event resolver.

    Three pools per tick:
      1. Demand-driven — manifests whose channel was accessed in the last
         10 min. Standard live-stream freshness window.
      2. 24/7 channels — resolved channels with no event_start/event_end
         (always-on intent). Refreshed regardless of access because the
         user expects them to be tunable at any time.
      3. Always-on channels' FALLBACK chain — same always-on channels as
         #2, but their fallback_manifest_ids too, not just the primary.
         Without this, a fallback that isn't the active source just rots
         until the one moment the chain actually reaches for it — which
         defeats the point of having a fallback at all.

    Each manifest is first tried via light_refresh_manifest() (HTTP only,
    cheap, uncapped). Light refreshes that fail get queued for the heavy
    sidecar path — but only HEAVY_REFRESH_BUDGET_PER_TICK of them per tick.
    This prevents the post-VPN-rotation thundering herd that would
    otherwise queue 10+ back-to-back Chrome captures and OOM the box.
    Remaining failures stay eligible for the next tick (their
    last_refreshed_at didn't advance), so 18 stale channels recover
    serially over ~6 ticks (~6 min) instead of crushing the host.

    Acquires the module-level pipeline_lock non-blocking so it can't
    overlap with the JIT event resolver — both pump work into the single
    selenium sidecar and must take turns. Held only around each individual
    sidecar call in the heavy-refresh loop below, not the whole tick — a
    batch of 5 heavy refreshes at ~30-100s each used to hold the lock
    continuously for minutes, which starved the JIT event resolver (its own
    acquire is non-blocking, so it just backs off every 2 min tick and never
    gets a turn during a refresh storm, e.g. an upstream source going down
    for several always-on channels at once). Releasing between items gives
    JIT real windows to grab the lock in between.
    """
    if not pipeline_lock.acquire(blocking=False):
        logger.info("[RESOLVER] Refresh tick skipped — pipeline busy (JIT or prior tick)")
        return
    needs_heavy: list[str] = []
    priority_by_id: dict[str, str] = {}
    try:
        from core.models.channel import Channel
        now = datetime.now(timezone.utc)
        soon = now + timedelta(minutes=5)
        cooldown = now - timedelta(minutes=3)
        watching_window = now - timedelta(minutes=10)
        with get_session() as session:
            relay_manifest_ids = session.query(Channel.manifest_id).filter(
                Channel.source_kind == "relay", Channel.manifest_id.isnot(None),
            )
            demand_rows = (
                session.query(Manifest.id, Manifest.last_refreshed_at)
                .filter(Manifest.tags.contains(["resolved"]))
                .filter(Manifest.active == True)
                .filter(Manifest.last_accessed_at.isnot(None))
                .filter(Manifest.last_accessed_at > watching_window)
                # see always_on_rows below — relay manifests have no playlist
                # to refresh, ContinuousRelaySource handles its own tokens.
                .filter(~Manifest.id.in_(relay_manifest_ids))
                .filter(
                    (Manifest.expires_at.is_(None)) |
                    (Manifest.expires_at < soon)
                )
                .filter(
                    (Manifest.last_refreshed_at.is_(None)) |
                    (Manifest.last_refreshed_at < cooldown)
                )
                .order_by(Manifest.last_refreshed_at.asc().nulls_first())
                .limit(5)
                .all()
            )
            always_on_rows = (
                session.query(Manifest.id, Manifest.last_refreshed_at)
                .join(Channel, Channel.manifest_id == Manifest.id)
                .filter(Manifest.active == True)
                .filter(Channel.type == "resolved")
                .filter(Channel.event_start.is_(None))
                .filter(Channel.event_end.is_(None))
                # relay-sourced channels have no playlist to refresh — the
                # manifest row is just a stable player-page URL, and its
                # ContinuousRelaySource does its own token refresh live on
                # every read. Sending it through here would just burn heavy-
                # refresh budget on a sidecar capture that can never succeed.
                .filter(Channel.source_kind != "relay")
                .filter(
                    (Manifest.expires_at.is_(None)) |
                    (Manifest.expires_at < soon)
                )
                .filter(
                    (Manifest.last_refreshed_at.is_(None)) |
                    (Manifest.last_refreshed_at < cooldown)
                )
                .order_by(Manifest.last_refreshed_at.asc().nulls_first())
                .limit(10)
                .all()
            )

            # Fallback-chain manifests get ZERO proactive refreshing above —
            # the join is Channel.manifest_id == Manifest.id, primary only.
            # A fallback that hasn't been the active source recently just
            # silently rots until the one moment it's actually needed, which
            # defeats the point of having a fallback chain at all (found
            # while investigating a channel whose fallback manifest had been
            # expired for days and failed even a light refresh the one time
            # the chain reached for it). Sweep always-on channels' fallback
            # lists into the same warm-keeping pool, skipping any candidate
            # explicitly overridden to source_kind=relay for that channel
            # (same reasoning as the primary-side relay exclusion above).
            always_on_channels = (
                session.query(Channel.fallback_manifest_ids, Channel.fallback_source_kinds)
                .filter(Channel.type == "resolved")
                .filter(Channel.event_start.is_(None))
                .filter(Channel.event_end.is_(None))
                .all()
            )
            fallback_ids = set()
            for fb_ids, fb_kinds in always_on_channels:
                fb_kinds = fb_kinds or {}
                for fid in (fb_ids or []):
                    if fb_kinds.get(fid) != "relay":
                        fallback_ids.add(fid)
            fallback_ids -= {r[0] for r in always_on_rows}  # already covered above

            always_on_fallback_rows = []
            if fallback_ids:
                always_on_fallback_rows = (
                    session.query(Manifest.id, Manifest.last_refreshed_at)
                    .filter(Manifest.id.in_(fallback_ids))
                    .filter(Manifest.active == True)
                    .filter(
                        (Manifest.expires_at.is_(None)) |
                        (Manifest.expires_at < soon)
                    )
                    .filter(
                        (Manifest.last_refreshed_at.is_(None)) |
                        (Manifest.last_refreshed_at < cooldown)
                    )
                    .order_by(Manifest.last_refreshed_at.asc().nulls_first())
                    .limit(10)
                    .all()
                )

            # Dedupe while preserving a genuine staleness order — a plain
            # set-comprehension dedup here threw away each query's own
            # order_by(last_refreshed_at), and since needs_heavy later
            # takes a fixed-size slice ([:budget]), whichever manifest_ids
            # happened to land past the cutoff in Python's (effectively
            # arbitrary, but stable within one process) set-iteration order
            # would NEVER get their turn for as long as the same due-pool
            # kept recurring — a real starvation bug that got much more
            # visible once the fallback-warming pool above made "due" pools
            # regularly exceed the 5/tick heavy budget. Sorting by
            # last_refreshed_at (nulls first = never-refreshed goes first)
            # makes every item's turn depend on genuine staleness, not hash
            # luck, so the budget rotates fairly across ticks.
            combined = demand_rows + always_on_rows + always_on_fallback_rows
            seen = set()
            deduped = []
            for mid, last_refreshed in combined:
                if mid not in seen:
                    seen.add(mid)
                    deduped.append((mid, last_refreshed))
            deduped.sort(key=lambda row: (row[1] is not None, row[1]))
            ids = [mid for mid, _ in deduped]

            # Skip candidates whose underlying source is currently disabled
            # (manual toggle or auto-held on sustained failures — see
            # core/source_registry.py) BEFORE spending any refresh effort on
            # them, not just relying on _call_sidecar's own downstream gate.
            # Real motivation, confirmed 2026-09-14: a large pool of stored
            # multi-player-family fallback manifests all share the same
            # ~30min TTL (all discovered together by the same fallback-race
            # event),
            # so they naturally re-expire in synchronized clusters and kept
            # re-entering this tick's "due" pool every cycle regardless of
            # how consistently they'd already failed — burning heavy-refresh
            # budget slots on known-doomed candidates every single tick and
            # crowding out genuinely-recoverable ones, on top of the raw
            # hammering itself. is_domain_enabled()'s own probe-cooldown
            # still lets exactly one candidate per held domain through per
            # window (whichever is checked first in this loop) — this isn't
            # a permanent exclusion, just stops re-trying the same known-bad
            # pool on every tick regardless of track record.
            if ids:
                from urllib.parse import urlparse
                from core.source_registry import is_domain_enabled
                page_urls = dict(
                    session.query(Manifest.id, Capture.page_url)
                    .join(Capture, Manifest.capture_id == Capture.id)
                    .filter(Manifest.id.in_(ids))
                    .all()
                )
                filtered_ids = []
                skipped_disabled = 0
                for mid in ids:
                    page_url = page_urls.get(mid)
                    domain = urlparse(page_url).netloc if page_url else None
                    enabled, reason = is_domain_enabled(domain) if domain else (True, None)
                    if enabled:
                        filtered_ids.append(mid)
                    else:
                        skipped_disabled += 1
                if skipped_disabled:
                    logger.info("[RESOLVER] Refresh tick: skipping %d due candidate(s) — source disabled",
                                skipped_disabled)
                ids = filtered_ids

            # Tag each manifest with the priority it should carry into a heavy
            # sidecar refresh — fallback-warming-only manifests are pure
            # background work and get "low" (defers on the sidecar's single
            # browser to any pending live/on-demand request); anything that's
            # a demand-driven or always-on PRIMARY gets "high", even if it
            # also happens to show up in someone else's fallback list.
            priority_by_id = {mid: "low" for mid, _ in always_on_fallback_rows}
            for mid, _ in demand_rows + always_on_rows:
                priority_by_id[mid] = "high"

        if not ids:
            return

        logger.info("[RESOLVER] Refresh tick: %d manifests due (demand=%d always-on=%d always-on-fallback=%d)",
                    len(ids), len(demand_rows), len(always_on_rows), len(always_on_fallback_rows))
        for mid in ids:
            try:
                light = ManifestResolverService.light_refresh_manifest(mid)
                if light.get("ok"):
                    logger.info("[RESOLVER] Light-refreshed %s (next expiry %s)",
                                mid, light.get("expires_at"))
                    continue
                logger.info("[RESOLVER] Light refresh of %s failed (%s) — queuing heavy",
                            mid, light.get("error"))
                needs_heavy.append(mid)
            except Exception as e:
                logger.warning("[RESOLVER] light refresh %s errored: %s", mid, e)
    except Exception as e:
        logger.exception("[RESOLVER] refresh tick error: %s", e)
    finally:
        pipeline_lock.release()

    if not needs_heavy:
        return

    # High-priority (demand-driven / always-on PRIMARY) items go first within
    # the tick's budget — a batch of low-priority fallback-warming refreshes
    # must not crowd out the primaries that are actually keeping a channel on
    # the air right now, on top of what the sidecar-side priority lock
    # already does for items still queued once the capture starts.
    needs_heavy.sort(key=lambda mid: priority_by_id.get(mid, "high") != "high")

    budget = _heavy_refresh_budget_multi() if _concurrency_mode() == "multi" else HEAVY_REFRESH_BUDGET_PER_TICK
    logger.info("[RESOLVER] Heavy refresh queue: %d due, processing up to %d this tick",
                len(needs_heavy), budget)
    batch = needs_heavy[:budget]

    if _concurrency_mode() == "multi":
        # No pipeline_lock at all in this mode -- resolve()'s own
        # _LOW_POOL.acquire() (background/keep-warm work defaults to
        # "high" priority_by_id unless explicitly tagged "low" upstream,
        # same as before) is what actually bounds concurrent sidecar
        # calls. Submitting the whole batch at once and letting the
        # semaphore throttle it is simpler and less error-prone than
        # sizing this executor to match RESOLVER_LOW_SLOTS separately.
        #
        # Real, confirmed anti-bot trigger found the hard way after raising
        # HEAVY_REFRESH_BUDGET_PER_TICK_MULTI: submitting the whole batch at
        # once means every item due for the SAME native-resolver-handled
        # domain (one specific domain family dominates the current due
        # pool) fires within the same second -- confirmed via a real tick
        # where 9 of 15 concurrently-submitted items all targeted the same
        # domain simultaneously, immediately followed by that upstream
        # refusing our exit IP. Native resolves are cheap/fast but this
        # pool has zero per-domain awareness, so raising throughput for the
        # sidecar's sake also raised same-domain burst intensity for
        # pure-HTTP sources that were never a bottleneck and don't want a
        # burst pattern. A small stagger between submissions (not a
        # concurrency cap -- items already running keep running
        # concurrently) spreads same-domain items across a few real
        # seconds instead of one instant, closer to how a human's browser
        # tabs would actually open, without materially hurting throughput
        # (each item's own runtime is 60-105s, dwarfing a sub-second
        # stagger).
        stagger = float(get_setting("HEAVY_REFRESH_STAGGER_SECONDS", "0.75"))
        with ThreadPoolExecutor(max_workers=max(1, len(batch))) as ex:
            futures = {}
            for mid in batch:
                futures[ex.submit(ManifestResolverService.refresh_manifest, mid,
                                   priority=priority_by_id.get(mid, "high"))] = mid
                time.sleep(stagger)
            for fut in as_completed(futures):
                mid = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    logger.warning("[RESOLVER] heavy refresh %s failed: %s", mid, e)
    else:
        for i, mid in enumerate(batch):
            # Non-blocking, per-item — see refresh_due_manifests' docstring
            # for why this isn't just one acquire around the whole loop.
            if not pipeline_lock.acquire(blocking=False):
                logger.info("[RESOLVER] Heavy refresh yielding lock (JIT or another tick got it) "
                            "— %d/%d remaining will retry next tick",
                            len(batch) - i, len(batch))
                break
            try:
                ManifestResolverService.refresh_manifest(mid, priority=priority_by_id.get(mid, "high"))
            except Exception as e:
                logger.warning("[RESOLVER] heavy refresh %s failed: %s", mid, e)
            finally:
                pipeline_lock.release()


_native_mod = False  # False = not yet looked up; None = absent; else module


def _native_resolver():
    """Load the optional user-provided native-resolver module from the scrapers
    dir (gitignored, site-specific) so no target hosts live in core. It exposes
    handles(url)->bool and capture(url, timeout)->dict|None. Cached after first
    load; returns None if absent."""
    global _native_mod
    if _native_mod is not False:
        return _native_mod
    _native_mod = None
    try:
        import importlib.util
        path = os.path.join(os.getenv("SCRAPERS_DIR", "/app/scrapers"),
                            "_native_resolvers.py")
        if os.path.isfile(path):
            spec = importlib.util.spec_from_file_location("_native_resolvers", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            _native_mod = mod
    except Exception as e:
        logger.warning("[RESOLVER] native resolver load failed: %s", e)
    return _native_mod


def _call_sidecar(url: str, timeout: int, priority: str = "high") -> dict:
    """Capture a manifest for a page URL. Some sources expose the HLS URL in
    plain HTML and can be resolved by a pure-HTTP native resolver (no browser);
    everything else goes through the selenium-uc sidecar /capture endpoint.

    priority is forwarded to the sidecar's single-browser lock as-is — "low"
    (background fallback-warming) defers there to any pending "high" (live/
    on-demand) request. See selenium-uc/app.py's _PriorityLock. Anything but
    an explicit "low" behaves like a plain mutex on the sidecar side, so the
    default here preserves prior behavior for every existing caller."""
    from urllib.parse import urlparse
    from core.source_registry import is_domain_enabled
    domain = urlparse(url).netloc
    enabled, reason = is_domain_enabled(domain)
    if not enabled:
        logger.info("[RESOLVER] Skipping %s — source disabled (%s)", url, reason)
        return {"ok": False, "error": f"source disabled: {reason}"}

    native = _native_resolver()
    if native is not None:
        try:
            if native.handles(url):
                cap = native.capture(url, timeout)
                if cap and cap.get("ok"):
                    logger.info("[RESOLVER] Resolved %s via native resolver", url)
                    return cap
                logger.info("[RESOLVER] Native resolver declined %s — using sidecar", url)
        except Exception as e:
            logger.warning("[RESOLVER] native resolver error for %s: %s", url, e)

    sidecar_url = f"{get_setting('SELENIUM_URL', 'http://localhost:4445')}/capture"
    # HTTP timeout = browser timeout + 30s buffer for startup/teardown
    http_timeout = timeout + 30
    logger.info("Calling sidecar %s for %s (priority=%s)", sidecar_url, url, priority)
    resp = http_requests.post(
        sidecar_url,
        json={"url": url, "timeout": timeout, "switch_iframe": True, "priority": priority},
        timeout=http_timeout,
    )
    resp.raise_for_status()
    return resp.json()


class ManifestResolverService:

    @staticmethod
    def get_status():
        return dict(_status)

    @staticmethod
    def check_selenium() -> bool:
        """Check if the selenium-uc sidecar is reachable.

        Returns True if the sidecar responds at all (even 503 busy).
        Only returns False if the sidecar is completely unreachable.
        """
        try:
            r = http_requests.get(f"{get_setting('SELENIUM_URL', 'http://localhost:4445')}/health", timeout=5)
            # Any response means the sidecar is alive — 503 just means busy
            return True
        except Exception:
            return False

    @staticmethod
    def resolve(url: str, title: str | None = None, timeout: int = 60,
                existing_manifest_id: str | None = None,
                tags: list | None = None,
                event_start: str | None = None,
                event_end: str | None = None,
                auto_create: bool = False,
                logo_urls: list | None = None,
                priority: str = "high") -> dict:
        """Capture an m3u8 manifest via the sidecar and store it in DB.

        If existing_manifest_id is provided, the specified row is updated in place
        (used for token refresh — keeps the same manifest ID so streams don't break).
        If auto_create is True and resolve succeeds, a resolved channel is
        created automatically with the given tags and event times.

        priority: "low" for background fallback-warming so it defers to any
        live/on-demand request pending on the sidecar's single browser —
        see _call_sidecar. Everything else should leave this at the default.
        """
        # In-flight dedup — if another thread is already resolving this URL, wait for it
        # (unless that entry is stale enough to be genuinely stuck — see
        # _INFLIGHT_STALE_SECONDS). my_event tracks whether THIS call owns
        # the slot, so its own finally block below only ever clears/signals
        # the entry it actually created — never a newer one that force-
        # cleared it out from under it.
        event = None
        my_event = None
        with _inflight_lock:
            existing = _inflight.get(url)
            if existing:
                existing_event, started_at = existing
                age = time.monotonic() - started_at
                if age < _INFLIGHT_STALE_SECONDS:
                    event = existing_event
                    logger.info("[RESOLVER] Waiting on in-flight resolve for %s", url)
                else:
                    logger.warning("[RESOLVER] In-flight resolve for %s has been stuck for "
                                   "%.0fs — force-clearing and starting fresh", url, age)
                    my_event = threading.Event()
                    _inflight[url] = (my_event, time.monotonic())
            else:
                my_event = threading.Event()
                _inflight[url] = (my_event, time.monotonic())

        if event:
            event.wait(timeout=timeout + 45)
            result = _inflight_results.pop(url, None)
            if result:
                return result
            return {"ok": False, "manifest_id": existing_manifest_id, "manifest_url": None,
                    "error": "In-flight resolve timed out"}

        _status["running"] = True
        _status["last_url"] = url
        _status["last_error"] = None
        result = None

        # Concurrency-mode-gated: single mode (default) does nothing extra
        # here — pipeline_lock, acquired by the CALLING loop (scheduled
        # refresh tick, JIT resolver), is what serializes sidecar access,
        # completely unchanged from before this existed. Multi mode has no
        # equivalent caller-side lock anymore (see refresh_due_manifests/
        # resolve_batch/event_resolver.py) -- this acquire is what actually
        # bounds concurrent sidecar calls to RESOLVER_HIGH_SLOTS/
        # RESOLVER_LOW_SLOTS, hard-partitioned by priority so background
        # work can never starve a live/JIT request by saturating a shared
        # pool. Blocking acquire (not non-blocking like pipeline_lock) is
        # intentional: callers that opted into multi mode WANT to queue for
        # a slot rather than skip the tick, since the whole point is
        # multiple items make real progress concurrently instead of one
        # tick doing one item.
        #
        # Known accepted limitation: _status (module-level, single-slot)
        # was built assuming one resolve() in flight at a time -- under
        # multi mode with several concurrent resolve() calls, "running"/
        # "last_url" become best-effort/racy (last-writer-wins), a
        # reporting-only issue, not a correctness issue in the actual
        # capture/storage logic below.
        mode = _concurrency_mode()
        pool = _pool_for_priority(priority) if mode == "multi" else None
        if pool:
            pool.acquire()
        try:
            capture = _call_sidecar(url, timeout, priority=priority)
        finally:
            if pool:
                pool.release()

        try:
            if not capture.get("ok"):
                err = capture.get("error", "Unknown error from sidecar")
                _status["last_error"] = err
                result = {"ok": False, "manifest_id": None, "manifest_url": None, "error": err}
                return result

            body_text = _sanitize_body(capture.get("body"))
            if not body_text or "#EXTM3U" not in body_text:
                err = "Captured body is not valid HLS"
                _status["last_error"] = err
                result = {"ok": False, "manifest_id": None, "manifest_url": None, "error": err}
                return result

            # Build context with heartbeat info
            context = {}
            heartbeat = capture.get("heartbeat")
            if heartbeat:
                context["heartbeat_url"] = heartbeat.get("heartbeat_url")
                context["heartbeat_interval"] = 30
                context["auth_headers"] = {
                    k: v for k, v in heartbeat.items()
                    if k != "heartbeat_url" and v is not None
                }
                key_match = re.search(r'#EXT-X-KEY:.*?URI="([^"]+)"', body_text)
                if key_match:
                    context["drm_key_url"] = key_match.group(1)

            # Confirmed live 2026-09-15: block_detector.record_success()
            # existed but had no caller anywhere in the codebase, despite
            # source_registry.py's own docstring describing auto_held
            # clearing "the moment record_success() fires" -- a domain
            # that ever tripped auto_held (e.g. a resolve-burst that's
            # since been fixed) had no path back to healthy except the
            # one-probe-per-cooldown trickle in _probe_allowed, forever.
            # This is the one place that sees every successful resolve
            # regardless of whether it came from a native resolver or the
            # sidecar -- confirmed live that the sidecar fallback path is
            # what actually recovers in practice even when native
            # discovery is struggling, so gating this on native-only
            # would have missed the real recovery signal entirely.
            try:
                from core.block_detector import record_success
                record_success(urlparse(url).netloc)
            except Exception:
                pass

            manifest_url = capture["manifest_url"]
            manifest_id = _store_manifest(
                page_url=url,
                user_agent=capture.get("user_agent", ""),
                manifest_url=manifest_url,
                mime=capture.get("mime"),
                resp_headers=capture.get("headers"),
                body_text=body_text,
                title=title,
                context=context,
                heartbeat=heartbeat,
                existing_manifest_id=existing_manifest_id,
                user_tags=tags,
                cookies=capture.get("cookies"),
                referer_url=capture.get("referer"),
            )

            _status["last_manifest_id"] = manifest_id
            now_utc = datetime.now(timezone.utc)
            expires_at = parse_body_expiry(body_text, manifest_url) or _default_expiry(now_utc)
            logger.info("[RESOLVER] Manifest resolved and stored: %s -> %s (expires %s)",
                        url, manifest_id, expires_at.isoformat())
            # No regenerate_m3u() here — every channel publishes one stable
            # /live/{channel_id}/... URL regardless of which manifest is
            # currently behind it, so an ordinary refresh (the overwhelming
            # majority of calls here: background ticks, player-health
            # probes, light/heavy refreshes) never actually changes the
            # M3U's content. The one case that DOES need it — a brand new
            # channel just got created — is handled below, scoped to the
            # auto_create branch. Regenerating unconditionally on every
            # refresh was pure waste and widened the exposure window for
            # the torn-read hazard regenerate_m3u() itself used to have
            # (see its own docstring) for zero benefit.
            result = {
                "ok": True,
                "manifest_id": manifest_id,
                "manifest_url": manifest_url,
                "expires_at": expires_at.isoformat() if expires_at else None,
                "error": None,
                "channel_id": None,
                "channel_name": None,
            }

            # Auto-create a resolved channel if requested
            if auto_create and not existing_manifest_id:
                try:
                    from web import shared_state
                    # Skip if a channel already references this manifest
                    existing_ch = None
                    for ch in shared_state.channel_mgr.list_channels():
                        if ch.get("manifest_id") == manifest_id:
                            existing_ch = ch
                            break
                    if existing_ch:
                        result["channel_id"] = existing_ch["id"]
                        result["channel_name"] = existing_ch["name"]
                        logger.info("[RESOLVER] Channel already exists for manifest %s: %s",
                                    manifest_id, existing_ch["name"])
                    else:
                        ch = shared_state.channel_mgr.create_resolved_channel(
                            manifest_id, title,
                            tags=tags,
                            event_start=event_start,
                            event_end=event_end,
                        )
                        if not ch:
                            # Same "auto_create's success criterion is
                            # getting a channel" reasoning as the except
                            # block below -- create_resolved_channel returning
                            # falsy (no exception) is this same failure mode
                            # via a different path, must not be silently
                            # treated as ok=True either.
                            logger.warning("[RESOLVER] Auto-create channel returned nothing for %s", manifest_id)
                            result["ok"] = False
                            result["error"] = "manifest resolved but create_resolved_channel returned nothing"
                        if ch:
                            result["channel_id"] = ch["id"]
                            result["channel_name"] = ch["name"]
                            logger.info("[RESOLVER] Auto-created channel %s for manifest %s",
                                        ch["name"], manifest_id)
                            # Generate channel logo from scraper-provided URLs
                            # (matchup card stitched from team logos), or fall
                            # back to a SearxNG image search keyed on the
                            # channel name when no scraper-supplied URLs exist
                            # — common for non-event 24/7 channels.
                            if logo_urls:
                                try:
                                    from core.logo_gen import generate_channel_logo
                                    generate_channel_logo(ch["id"], logo_urls)
                                except Exception as e:
                                    logger.warning("[RESOLVER] Logo gen failed for %s: %s",
                                                   ch["id"], e)
                            else:
                                _logo_path = os.path.join(shared_state.LOGO_DIR,
                                                          f"{ch['id']}.png")
                                if not os.path.isfile(_logo_path):
                                    try:
                                        from core import logo_search as _ls
                                        os.makedirs(shared_state.LOGO_DIR, exist_ok=True)
                                        ok, msg = _ls.auto_pick(
                                            ch["id"], ch["name"], _logo_path,
                                        )
                                        if ok:
                                            logger.info("[RESOLVER] Auto-logo for %s: %s",
                                                        ch["name"], msg)
                                        else:
                                            logger.info("[RESOLVER] Auto-logo skipped for %s: %s",
                                                        ch["name"], msg)
                                    except Exception as e:
                                        logger.warning("[RESOLVER] Auto-logo failed for %s: %s",
                                                       ch["id"], e)
                            shared_state.regenerate_m3u()
                except Exception as e:
                    logger.warning("[RESOLVER] Auto-create channel failed for %s: %s", manifest_id, e)
                    # Real bug found 2026-09-16: this except previously only
                    # logged -- result["ok"] stayed True from the manifest
                    # capture succeeding above, so a caller that asked for
                    # auto_create specifically (the whole point of the call)
                    # saw a plain success with channel_id=None and had no way
                    # to tell "fully succeeded" apart from "got a manifest but
                    # never got the channel it actually wanted." event_
                    # resolver.py's reconcile step took that at face value and
                    # marked the event permanently "resolved" -- a status that
                    # never gets retried -- even though nothing was actually
                    # watchable. If auto_create was requested, getting a
                    # channel out of it IS the success criterion.
                    result["ok"] = False
                    result["error"] = f"manifest resolved but channel creation failed: {e}"

            return result

        except http_requests.exceptions.RequestException as e:
            err = f"Sidecar communication failed: {e}"
            logger.exception("Sidecar call failed for %s", url)
            _status["last_error"] = err
            result = {"ok": False, "manifest_id": None, "manifest_url": None, "error": err}
            return result

        except Exception as e:
            logger.exception("Resolve failed for %s", url)
            _status["last_error"] = str(e)
            result = {"ok": False, "manifest_id": None, "manifest_url": None, "error": str(e)}
            return result

        finally:
            _status["running"] = False
            # Signal in-flight waiters — only clear/signal the entry if it's
            # still OURS (identity check on the Event). A later caller may
            # have already force-cleared us as stale (see
            # _INFLIGHT_STALE_SECONDS) and started its own attempt; an
            # unconditional pop here would wipe out THAT newer attempt's
            # own in-flight entry instead of our long-dead one.
            with _inflight_lock:
                current = _inflight.get(url)
                if current is not None and current[0] is my_event:
                    _inflight.pop(url, None)
            if my_event:
                if result:
                    _inflight_results[url] = result
                my_event.set()

    @staticmethod
    def light_refresh_manifest(manifest_id: str) -> dict:
        """Cheap refresh: re-fetch the stored manifest URL with the stored
        cookies + UA + Referer and update last_refreshed_at + expires_at.
        Indistinguishable from a real player polling its playlist URL — no
        browser, no captcha, no iframe drilldown. Returns ok=True with
        path='light' on success.

        Used by the periodic refresh worker so idle 24/7 channels stay alive
        without burning the selenium sidecar (a scarce resource) on every
        cycle. The full sidecar-based refresh_manifest() stays as the
        fallback path for when the upstream session has actually rotated.
        """
        with get_session() as session:
            row = (
                session.query(Manifest.url, Manifest.headers,
                              Manifest.source_domain, Manifest.cookies)
                .filter(Manifest.id == manifest_id)
                .first()
            )
        if not row:
            return {"ok": False, "error": "manifest not found"}
        url, stored_headers, source_domain, cookies_data = row
        if not url:
            return {"ok": False, "error": "no manifest url"}

        # The Manifest model stores response headers, not request headers,
        # so we don't have the original UA — but every modern HLS upstream
        # we've seen accepts a vanilla Chrome UA. The cookies + Referer are
        # what gate access on session-locked sources.
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
             "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        headers = {"User-Agent": ua}
        if source_domain:
            headers["Referer"] = f"https://{source_domain}/"
            headers["Origin"] = f"https://{source_domain}"
        cookie_jar = {}
        for c in (cookies_data or []):
            name = c.get("name")
            val = c.get("value")
            if name and val:
                cookie_jar[name] = val

        try:
            resp = http_requests.get(url, headers=headers, cookies=cookie_jar,
                                     timeout=15, allow_redirects=True)
        except Exception as e:
            return {"ok": False, "error": f"fetch failed: {e}", "path": "light"}
        if resp.status_code != 200:
            return {"ok": False, "error": f"status {resp.status_code}", "path": "light"}
        body = resp.text or ""
        if "#EXTM3U" not in body:
            return {"ok": False, "error": "response is not HLS", "path": "light"}

        # AES-128 sources can keep serving a valid playlist long after their
        # /key/ endpoint has silently rotated to returning fake bytes for our
        # stale session. The playlist alone is not enough to prove the
        # session still works end-to-end. Probe with a single AES block:
        # fetch the key, fetch the first 16 bytes of the first segment, do
        # one AES-CBC block decrypt, check the result starts with 0x47
        # (MPEG-TS sync). One block is all we need — if the session is dead,
        # the wrong key produces random bytes and the sync byte misses ~99%
        # of the time.
        if "#EXT-X-KEY:METHOD=AES-128" in body:
            key_match = re.search(r'#EXT-X-KEY:[^\n]*?URI="([^"]+)"', body)
            iv_match = re.search(r'IV=0x([0-9a-fA-F]+)', body)
            seg_line = next(
                (line.strip() for line in body.splitlines()
                 if line.strip() and not line.startswith("#")),
                None,
            )
            if key_match and iv_match and seg_line:
                from urllib.parse import urljoin
                key_url = urljoin(url, key_match.group(1))
                seg_url = urljoin(url, seg_line)
                hex_iv = iv_match.group(1)
                if len(hex_iv) % 2:
                    hex_iv = "0" + hex_iv
                iv = bytes.fromhex(hex_iv)[-16:].rjust(16, b"\x00")
                try:
                    key_resp = http_requests.get(key_url, headers=headers,
                                                 cookies=cookie_jar, timeout=10)
                    seg_resp = http_requests.get(
                        seg_url,
                        headers={**headers, "Range": "bytes=0-15"},
                        cookies=cookie_jar, timeout=10,
                    )
                except Exception as e:
                    return {"ok": False, "error": f"decrypt probe fetch: {e}",
                            "path": "light"}
                if key_resp.status_code != 200 or len(key_resp.content) != 16:
                    return {"ok": False, "path": "light",
                            "error": f"key probe status={key_resp.status_code} len={len(key_resp.content)}"}
                if seg_resp.status_code not in (200, 206) or len(seg_resp.content) < 16:
                    return {"ok": False, "path": "light",
                            "error": f"seg probe status={seg_resp.status_code} len={len(seg_resp.content)}"}
                try:
                    from Crypto.Cipher import AES
                    cipher = AES.new(key_resp.content, AES.MODE_CBC, iv)
                    plaintext = cipher.decrypt(seg_resp.content[:16])
                except Exception as e:
                    return {"ok": False, "error": f"decrypt failed: {e}",
                            "path": "light"}
                if not plaintext or plaintext[0] != 0x47:
                    return {"ok": False, "path": "light",
                            "error": "decrypt probe: TS sync byte missing (session likely stale)"}

        # Refresh is real. Update expiry from the new body if it carries one,
        # otherwise just push the jittered default window forward (see
        # _default_expiry).
        now = datetime.now(timezone.utc)
        new_expiry = parse_body_expiry(body, url) or _default_expiry(now)
        with get_session() as session:
            m = session.query(Manifest).filter_by(id=manifest_id).first()
            if m:
                m.expires_at = new_expiry
                m.last_refreshed_at = now
                session.commit()
        return {"ok": True, "manifest_id": manifest_id, "manifest_url": url,
                "expires_at": new_expiry.isoformat(), "path": "light"}

    @staticmethod
    def refresh_manifest(manifest_id: str, timeout: int = 60, priority: str = "high") -> dict:
        """Re-resolve an existing manifest using its stored page_url.

        Updates the same row in place (preserves manifest_id) so active streams
        see a seamless URL swap on their next playlist poll. This is the heavy
        path — launches a browser, navigates the watch page, drills iframes,
        captures the manifest. Reserve for cases where light_refresh_manifest()
        has failed (upstream session rotated, manifest URL no longer valid).

        priority: pass "low" only for background fallback-warming (a manifest
        that isn't the active source for anyone right now). Every other
        caller — a live proxy/remux stream refreshing its own primary after a
        401/403, the JIT event resolver, a user-triggered resolve — leaves
        this at the default "high" so it doesn't queue behind background work
        on the sidecar's single browser.
        """
        with get_session() as session:
            row = (
                session.query(Manifest.title, Capture.page_url)
                .outerjoin(Capture, Manifest.capture_id == Capture.id)
                .filter(Manifest.id == manifest_id)
                .first()
            )
        if not row:
            return {"ok": False, "manifest_id": manifest_id, "error": "manifest not found"}
        title, page_url = row
        if not page_url:
            return {"ok": False, "manifest_id": manifest_id, "error": "no page_url for refresh"}

        logger.info("Refreshing manifest %s from %s (full sidecar path, priority=%s)",
                    manifest_id, page_url, priority)
        result = ManifestResolverService.resolve(
            url=page_url, title=title, timeout=timeout,
            existing_manifest_id=manifest_id, priority=priority,
        )
        if result.get("ok"):
            result["path"] = "full"
        return result

    @staticmethod
    def discover_and_store_fallbacks(channel_id: str, primary_manifest_id: str,
                                     timeout: int = 20) -> int:
        """For multi-player sources only (see the native-resolver plugin's
        discover-all hook): walk every other known player path for this
        channel's stream id and store each one that yields a working
        manifest as a fallback source on the channel.

        Meant to piggyback on the moment `_pick_working_manifest` (hls.py)
        already decided the whole fallback chain was exhausted and is
        heavy-refreshing the primary — an already-expensive, infrequent
        event, so tacking on this pure-HTTP discovery sweep (no selenium/
        browser cost) is safe there without its own separate schedule or
        adding to the sidecar's load. The source's own incident messaging
        during a 2026-09-12 DDoS ("check all players 1-6") made clear these
        alternates fail independently — capturing them all up front (not
        just the default Player 1 the resolver has always used) directly
        shortens future downtime windows.

        "Player 1" itself (path="stream") is skipped — that's the primary,
        already handled by the caller. Ranking/ordering these fallbacks by
        reliability is a later project; for now this only makes sure
        they're captured and available at all for the existing fallback-
        chain logic to try in order.

        Returns the number of NEW fallback manifests added (0 if this
        source has no discover-all hook, the channel doesn't exist, or
        nothing new was found).
        """
        from core.resolver import player_health
        discovered = player_health.discover_and_record(channel_id, primary_manifest_id, timeout=timeout)
        if discovered is None:
            return 0

        from web import shared_state
        ch = shared_state.channel_mgr.get_channel(channel_id)
        if not ch:
            return 0
        ch_name = ch.get("name") or channel_id
        existing_titles = {fb.get("title") for fb in (ch.get("fallback_sources") or [])}

        added = 0
        for item in discovered:
            path = item.get("path")
            if path == "stream" or not item.get("ok"):
                continue  # Player 1 / default is the primary, not a fallback; failed paths aren't stored
            manifest_id = _store_discovered_player_manifest(
                channel_id, ch_name, path, item.get("page_url"), item.get("capture") or {}, existing_titles)
            if manifest_id:
                shared_state.channel_mgr.add_fallback_source(channel_id, manifest_id)
                added += 1

        if added:
            logger.info("[RESOLVER] channel %s: discovered and stored %d new player fallback(s)",
                        channel_id, added)
        return added

    @staticmethod
    def resolve_any_working_source(channel_id: str, timeout: int = 30) -> dict | None:
        """Multi-mode-only: when a channel's whole stored chain (primary +
        fallbacks) is exhausted, race EVERY known way to get this channel
        back on the air CONCURRENTLY -- primary's own refresh, every stored
        fallback's own refresh, AND (if this is a recognized multi-player
        source) every native player path -- and serve whichever succeeds
        first.

        Built specifically to answer "assume every source is flaky, how
        fast can we autonomously find ANY working link" rather than lean on
        historical success-rate scoring, which mathematically can't help in
        exactly this moment: a chronically flaky source never builds up
        enough clean history to beat another chronically flaky source by
        player_health's promotion margin. Real-time concurrent probing
        sidesteps that entirely -- it doesn't care about history, only
        about what's actually up right now. Directly motivated by a cold-
        start scenario: primary down after the system's been idle for
        hours, first viewer request should still find a working link fast,
        autonomously, with no manual intervention.

        Single-mode gate: returns None immediately in single mode --
        v1's one-browser reality can't support real racing at all, and
        single mode must stay byte-identical to legacy behavior (caller
        falls back to today's "heavy-refresh primary only" path).

        Returns {"manifest_id", "manifest_url", "encoder_mode",
        "source_kind"} for whichever candidate won, or None if nothing
        succeeded within the budget."""
        if _concurrency_mode() != "multi":
            return None

        from web import shared_state
        ch = shared_state.channel_mgr.get_channel(channel_id)
        if not ch:
            return None

        primary_id = ch.get("manifest_id")
        default_mode = ch.get("encoder_mode", "proxy")
        default_kind = ch.get("source_kind", "hls")
        fb_modes = ch.get("fallback_encoder_modes") or {}
        fb_kinds = ch.get("fallback_source_kinds") or {}
        ch_name = ch.get("name") or channel_id
        existing_titles = {fb.get("title") for fb in (ch.get("fallback_sources") or [])}

        # Every already-known candidate -- race a REAL refresh for EACH one,
        # not just primary. Today's exhausted-chain path only ever retries
        # primary; a fallback whose own underlying source is fine but whose
        # stored URL is merely stale (e.g. after a long cold period) never
        # got a chance under the old logic at all.
        stored = []
        if primary_id:
            stored.append((primary_id, default_mode, default_kind))
        for fb in (ch.get("fallback_sources") or []):
            mid = fb.get("manifest_id")
            if mid:
                stored.append((mid, fb_modes.get(mid, default_mode), fb_kinds.get(mid, default_kind)))

        primary_page_url = None
        if primary_id:
            with get_session() as session:
                row = (session.query(Capture.page_url)
                       .join(Manifest, Manifest.capture_id == Capture.id)
                       .filter(Manifest.id == primary_id).first())
            primary_page_url = row[0] if row else None

        native = _native_resolver()
        native_candidates = []
        if native is not None and primary_page_url and hasattr(native, "player_candidate_urls"):
            try:
                if native.handles(primary_page_url):
                    # Every native candidate for a channel shares the same
                    # front-door apex as primary_page_url (same family/
                    # channel id, different player path) -- one check here
                    # gates the whole set. _try_native_path below calls
                    # native.probe_one_player() directly, which (unlike
                    # _try_stored's refresh_manifest()) never passes through
                    # _call_sidecar's own enforcement check -- this is the
                    # only gate that path gets.
                    from urllib.parse import urlparse
                    from core.source_registry import is_domain_enabled
                    enabled, reason = is_domain_enabled(urlparse(primary_page_url).netloc)
                    if not enabled:
                        logger.info("[RESOLVER] channel %s: skipping native player-path race — "
                                    "source disabled (%s)", channel_id, reason)
                    else:
                        native_candidates = native.player_candidate_urls(primary_page_url) or []
            except Exception:
                native_candidates = []

        if not stored and not native_candidates:
            return None

        def _try_stored(mid, mode, kind):
            result = ManifestResolverService.refresh_manifest(mid, timeout=timeout, priority="high")
            if result.get("ok"):
                label = "primary" if mid == primary_id else "fallback (stored)"
                return {"manifest_id": mid, "manifest_url": result.get("manifest_url"),
                        "encoder_mode": mode, "source_kind": kind, "label": label}
            return None

        def _try_native_path(cand):
            if cand["path"] == "stream":
                return None  # that's the default/primary path, already covered by _try_stored, not a fallback
            item = native.probe_one_player(cand["url"], cand["path"], timeout=min(timeout, 15))
            if not item or not item.get("ok"):
                return None
            manifest_id = _store_discovered_player_manifest(
                channel_id, ch_name, cand["path"], primary_page_url, item.get("capture") or {}, existing_titles)
            if not manifest_id:
                return None
            shared_state.channel_mgr.add_fallback_source(channel_id, manifest_id)
            return {"manifest_id": manifest_id, "manifest_url": item["capture"]["manifest_url"],
                    "encoder_mode": default_mode, "source_kind": default_kind,
                    "label": f"fallback (player: {cand['path']})"}

        total_candidates = len(stored) + len(native_candidates)
        logger.info("[RESOLVER] channel %s: racing %d candidate(s) (%d stored + %d native path) "
                    "for any working source", channel_id, total_candidates, len(stored), len(native_candidates))

        from core.diagnostics import record_event, set_meta
        race_start = time.monotonic()
        record_event(channel_id, "race_started",
                     {"candidates": total_candidates, "stored": len(stored), "native": len(native_candidates)})

        # Deliberately NOT a `with ThreadPoolExecutor(...) as ex:` block --
        # that form calls shutdown(wait=True) on exit, which would block
        # this function's return until EVERY candidate finishes (including
        # the slowest, up to the full sidecar deadline), completely
        # defeating the point of racing. Created bare so returning as soon
        # as a winner is found doesn't wait on the stragglers; they keep
        # running against this same executor object (kept alive by the
        # running futures themselves) and their results (new manifests/
        # fallbacks stored) still land for next time even though nothing
        # is listening for them anymore.
        ex = ThreadPoolExecutor(max_workers=max(1, total_candidates))
        futures = [ex.submit(_try_stored, mid, mode, kind) for mid, mode, kind in stored]
        futures += [ex.submit(_try_native_path, cand) for cand in native_candidates]
        ex.shutdown(wait=False)  # stop accepting new work; already-submitted futures run to completion regardless

        winner = None
        try:
            for fut in as_completed(futures, timeout=timeout + 45):
                try:
                    result = fut.result()
                except Exception as e:
                    logger.warning("[RESOLVER] channel %s: a race candidate errored: %s", channel_id, e)
                    continue
                if result:
                    winner = result
                    break
        except TimeoutError:
            # as_completed's OWN overall-wait timeout (distinct from any
            # individual future's exception) -- means nothing finished
            # within budget at all, not that something errored. Treat
            # exactly like "no candidate succeeded."
            logger.warning("[RESOLVER] channel %s: race hit its overall %ds budget with nothing "
                            "back yet", channel_id, timeout + 45)

        race_elapsed_ms = (time.monotonic() - race_start) * 1000
        if winner:
            logger.info("[RESOLVER] channel %s: race won by manifest %s (%.0fs budget)",
                        channel_id, winner["manifest_id"], timeout)
            record_event(channel_id, "race_won", {
                "manifest_id": winner["manifest_id"], "label": winner.get("label"),
                "candidates": total_candidates, "elapsed_ms": round(race_elapsed_ms),
            })
            set_meta(channel_id, active_source_label=winner.get("label"),
                     fallback_active=(winner.get("label") != "primary"))
        else:
            logger.warning("[RESOLVER] channel %s: no race candidate succeeded within budget", channel_id)
            record_event(channel_id, "race_exhausted",
                         {"candidates": total_candidates, "elapsed_ms": round(race_elapsed_ms)})
        return winner

    @staticmethod
    def get_batch_status():
        return dict(_batch)

    @staticmethod
    def resolve_batch(urls: list[dict], timeout: int = 60, auto_create: bool = False):
        """Resolve a list of URLs -- sequentially in "single" mode (default,
        byte-identical to original behavior), concurrently (bounded by
        resolve()'s own _HIGH_POOL) in "multi" mode. See
        _concurrency_mode()/RESOLVER_CONCURRENCY_MODE.

        Each entry in urls can include: url, title, tags, event_start, event_end.
        If auto_create is True, a resolved channel is created for each successful
        manifest (with tags and event times if provided).
        """
        _batch["running"] = True
        _batch["total"] = len(urls)
        _batch["completed"] = 0
        _batch["results"] = [
            {"url": u["url"], "title": u.get("title"), "status": "pending",
             "manifest_id": None, "manifest_url": None, "expires_at": None,
             "channel_id": None, "channel_name": None, "error": None}
            for u in urls
        ]

        def _apply_result(i, result):
            if result.get("ok"):
                _batch["results"][i]["status"] = "done"
                _batch["results"][i]["manifest_id"] = result["manifest_id"]
                _batch["results"][i]["manifest_url"] = result["manifest_url"]
                _batch["results"][i]["expires_at"] = result.get("expires_at")
                _batch["results"][i]["channel_id"] = result.get("channel_id")
                _batch["results"][i]["channel_name"] = result.get("channel_name")
            else:
                _batch["results"][i]["status"] = "failed"
                _batch["results"][i]["error"] = result.get("error")

        if _concurrency_mode() == "multi":
            # Batch-resolve is interactive/admin-triggered (bulk auto-
            # channel-creation from scraper output), not background work --
            # defaults to "high" priority same as resolve()'s own default,
            # so these calls contend for _HIGH_POOL. Fire the whole list at
            # once and let resolve()'s own semaphore acquire throttle actual
            # concurrent sidecar calls, same reasoning as the heavy-refresh
            # loop above -- no separate executor-sizing decision needed.
            # _batch["current_url"] isn't meaningful with several URLs
            # in flight at once; left None here (same "status reporting is
            # best-effort under multi mode" tradeoff as resolve()'s own
            # docstring).
            for i, entry in enumerate(urls):
                _batch["results"][i]["status"] = "resolving"
            _batch["current_url"] = None
            with ThreadPoolExecutor(max_workers=max(1, len(urls))) as ex:
                futures = {
                    ex.submit(ManifestResolverService.resolve,
                              url=entry["url"], title=entry.get("title"), timeout=timeout,
                              tags=entry.get("tags"), event_start=entry.get("event_start"),
                              event_end=entry.get("event_end"), auto_create=auto_create,
                              logo_urls=entry.get("logo_urls")): i
                    for i, entry in enumerate(urls)
                }
                completed = 0
                for fut in as_completed(futures):
                    i = futures[fut]
                    try:
                        result = fut.result()
                    except Exception as e:
                        result = {"ok": False, "error": str(e)}
                    _apply_result(i, result)
                    completed += 1
                    _batch["completed"] = completed
        else:
            for i, entry in enumerate(urls):
                _batch["current_url"] = entry["url"]
                _batch["results"][i]["status"] = "resolving"

                result = ManifestResolverService.resolve(
                    url=entry["url"],
                    title=entry.get("title"),
                    timeout=timeout,
                    tags=entry.get("tags"),
                    event_start=entry.get("event_start"),
                    event_end=entry.get("event_end"),
                    auto_create=auto_create,
                    logo_urls=entry.get("logo_urls"),
                )
                _apply_result(i, result)
                _batch["completed"] = i + 1

        _batch["running"] = False
        _batch["current_url"] = None
        logger.info("Batch resolve complete: %d/%d succeeded",
                     sum(1 for r in _batch["results"] if r["status"] == "done"),
                     _batch["total"])

    @staticmethod
    def retry_batch_item(index: int, timeout: int = 60):
        """Retry a single failed item in the batch."""
        if index < 0 or index >= len(_batch["results"]):
            return {"ok": False, "error": "Invalid index"}

        item = _batch["results"][index]
        if item["status"] != "failed":
            return {"ok": False, "error": "Item is not in failed state"}

        _batch["running"] = True
        _batch["current_url"] = item["url"]
        item["status"] = "resolving"
        item["error"] = None

        result = ManifestResolverService.resolve(
            url=item["url"], title=item.get("title"), timeout=timeout
        )

        if result["ok"]:
            item["status"] = "done"
            item["manifest_id"] = result["manifest_id"]
            item["manifest_url"] = result["manifest_url"]
            item["expires_at"] = result.get("expires_at")
        else:
            item["status"] = "failed"
            item["error"] = result["error"]

        _batch["running"] = False
        _batch["current_url"] = None
        return result


def _store_discovered_player_manifest(channel_id: str, ch_name: str, path: str, page_url: str,
                                       capture: dict, existing_titles: set) -> str | None:
    """Shared storage logic for a single successfully-discovered player
    path's capture result -- used by both discover_and_store_fallbacks
    (the reactive, chain-exhaustion-triggered sweep) and
    resolve_any_working_source (the real-time race). Factored out so both
    callers store a discovered player identically rather than duplicating
    the title-tagging/sanitize/_store_manifest sequence. Returns the new
    manifest_id, or None if this path is already tracked or its body isn't
    a real playlist."""
    title = f"{ch_name} (player: {path})"
    if title in existing_titles:
        return None
    body_text = _sanitize_body(capture.get("body"))
    if not body_text or "#EXTM3U" not in body_text:
        return None
    try:
        return _store_manifest(
            page_url=page_url,
            user_agent=capture.get("user_agent", ""),
            manifest_url=capture["manifest_url"],
            mime=capture.get("mime"),
            resp_headers=capture.get("headers"),
            body_text=body_text,
            title=title,
            context={},
            heartbeat=capture.get("heartbeat"),
            cookies=capture.get("cookies"),
            referer_url=capture.get("referer"),
        )
    except Exception as e:
        logger.warning("[RESOLVER] failed to store discovered player %s for channel %s: %s",
                      path, channel_id, e)
        return None


def _store_manifest(
    *,
    page_url: str,
    user_agent: str,
    manifest_url: str,
    mime: str | None,
    resp_headers: dict | None,
    body_text: str,
    title: str | None,
    context: dict,
    heartbeat: dict | None,
    existing_manifest_id: str | None = None,
    user_tags: list | None = None,
    cookies: list | None = None,
    referer_url: str | None = None,
) -> str:
    """Insert or update a manifest in Manifold's DB. Returns manifest ID.

    If existing_manifest_id is given, the specified row is updated in place
    regardless of hash changes (used for token refresh).
    """
    # Source domain drives the Referer/Origin used on segment, key, and
    # nested-playlist fetches. The Referer Chrome actually sent for the
    # captured manifest is the most accurate signal — some sources serve
    # different bytes (e.g. fake AES keys) when the Referer doesn't match
    # the player iframe's origin. Fall back to the page URL host (the
    # embedding site) for sources whose capture path doesn't surface a
    # Referer, then to the manifest/CDN host as a last resort.
    referer_host = urlparse(referer_url).netloc if referer_url else ""
    source_domain = (
        referer_host
        or urlparse(page_url).netloc
        or urlparse(manifest_url).netloc
    )
    # Filter cookies to CDN/source domains only
    cdn_domain = urlparse(manifest_url).netloc
    filtered_cookies = []
    for c in (cookies or []):
        cd = (c.get("domain") or "").lstrip(".")
        if cd and (cdn_domain.endswith(cd) or source_domain.endswith(cd)
                   or cd.endswith(cdn_domain) or cd.endswith(source_domain)):
            filtered_cookies.append(c)
    kind = "master" if "#EXT-X-STREAM-INF" in body_text else "media"
    url_hash = _md5(manifest_url)
    body_hash = _sha256(body_text)
    now = datetime.now(timezone.utc)
    # Try to parse real expiry from URL/body; fall back to the jittered
    # default window (see _default_expiry) so the scheduler refreshes all
    # resolved channels periodically regardless of token format.
    expires_at = parse_body_expiry(body_text, manifest_url) or _default_expiry(now)

    # DRM detection
    drm_method = None
    is_drm = False
    if "#EXT-X-KEY" in body_text:
        if "METHOD=SAMPLE-AES" in body_text:
            drm_method, is_drm = "SAMPLE-AES", True
        elif "METHOD=AES-128" in body_text:
            drm_method, is_drm = "AES-128", False

    with get_session() as session:
        cap = Capture(page_url=page_url, user_agent=user_agent, context=context)
        session.add(cap)
        session.flush()

        header_profile_id = None
        if heartbeat:
            profile_name = f"resolved-{source_domain}"
            hp = session.query(HeaderProfile).filter_by(name=profile_name).first()
            auth_headers = {k: v for k, v in heartbeat.items()
                           if k != "heartbeat_url" and v is not None}
            if hp:
                hp.headers = auth_headers
            else:
                hp = HeaderProfile(name=profile_name, headers=auth_headers)
                session.add(hp)
                session.flush()
            header_profile_id = hp.id

        # Find existing row for in-place update:
        # 1. Explicit manifest_id (refresh path) takes precedence
        # 2. Otherwise match by active resolved title (re-resolving same channel)
        # 3. Otherwise fall back to hash dedup (generic content)
        manifest = None
        if existing_manifest_id:
            manifest = session.query(Manifest).filter_by(id=existing_manifest_id).first()
        if not manifest and title:
            manifest = session.query(Manifest).filter(
                Manifest.title == title,
                Manifest.active == True,
                Manifest.tags.contains(["resolved"]),
            ).first()
        if not manifest:
            manifest = session.query(Manifest).filter(
                Manifest.url_hash == url_hash,
                Manifest.sha256 == body_hash,
            ).first()

        if manifest:
            # Update in place — preserves manifest_id across token refreshes
            manifest.capture_id = cap.id
            if header_profile_id:
                manifest.header_profile_id = header_profile_id
                manifest.requires_headers = True
            manifest.url = manifest_url
            manifest.url_hash = url_hash
            manifest.source_domain = source_domain
            manifest.mime = mime
            manifest.kind = kind
            manifest.headers = resp_headers or {}
            manifest.body = body_text
            manifest.sha256 = body_hash
            manifest.drm_method = drm_method
            manifest.is_drm = is_drm
            manifest.expires_at = expires_at
            manifest.last_refreshed_at = now
            manifest.cookies = filtered_cookies
            # A successful re-resolve means the stream is live again — restore
            # the active flag (mirrors the create branch). Without this, a
            # manifest that ever went active=False stays excluded from the
            # background refresh worker forever, so the channel only works on
            # manual access and never gets kept warm.
            manifest.active = True
            # Refresh variants for master playlists (drop old, insert new)
            if kind == "master":
                session.query(Variant).filter_by(manifest_id=manifest.id).delete()
                session.flush()
                for v in _parse_master_variants(body_text, manifest_url):
                    session.add(Variant(manifest_id=manifest.id, **v))
        else:
            manifest = Manifest(
                capture_id=cap.id,
                header_profile_id=header_profile_id,
                url=manifest_url,
                url_hash=url_hash,
                source_domain=source_domain,
                mime=mime,
                kind=kind,
                headers=resp_headers or {},
                requires_headers=bool(header_profile_id),
                body=body_text,
                sha256=body_hash,
                drm_method=drm_method,
                is_drm=is_drm,
                title=title,
                tags=["resolved"] + (user_tags or []),
                active=True,
                expires_at=expires_at,
                last_refreshed_at=now,
                cookies=filtered_cookies,
            )
            session.add(manifest)
            session.flush()

            if kind == "master":
                for v in _parse_master_variants(body_text, manifest_url):
                    session.add(Variant(manifest_id=manifest.id, **v))

            # B3: Resolves create manifests only — they no longer auto-create
            # a Channel row. The library is the manifest collection; channels
            # are created explicitly from a library entry via /api/channels
            # with type=resolved + manifest_id.

        return manifest.id
