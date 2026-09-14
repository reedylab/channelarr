"""Continuous, live-channel-scoped evaluator for multi-player sources.

Two fully decoupled loops, sharing only the player_health_scores table as a
handoff point (never gating each other):

- The warming rotation (Loop A): a persistent background thread pool (2
  workers), continuously cycling through every currently-*playing*
  multi-player channel's non-primary candidate paths, each given a real
  30-second sample (see core/resolver/segment_sampler.py) rather than a
  single point-in-time check — deep enough to catch intermittent flakiness
  a one-shot probe misses (confirmed live: a path that worked, then got
  flagged by anti-bot defenses seconds later on an identical request).
  Deliberately scoped to *live* channels only, not the whole catalog — this
  site is already defensive (anti-bot decoys, rate-limiting, a recent
  DDoS), and there's no payoff to continuously sampling channels nobody's
  watching. Idle channels stay covered by the existing reactive-on-chain-
  exhaustion path (discover_and_store_fallbacks) and the slower 120s
  player_health_probe scheduler tick, both unchanged by this module.

- The switch watchdog (Loop B): a small, fast thread (~10s interval) that
  only ever reads what the warming rotation has already recorded plus the
  live diagnostics dashboard (core/diagnostics.py) — no network calls, no
  waiting on a probe. If a currently-playing channel is struggling
  (source_stall / bad quality — the accurate, already-trusted signal),
  it promotes + hot-restarts immediately using whatever score data already
  exists. This is what makes the actual switch feel seamless: the decision
  itself is instant, because the warming rotation already did the slow
  part in the background ahead of time.

Both loops are pure HTTP (native resolver + plain segment fetches) — no
selenium/browser cost, so this never competes with pipeline_lock/
manifest_refresh for the sidecar's single browser.

Modeled on core/youtube.py's start_yt_cache_worker: a module-level start
guard, plain threading.Thread(daemon=True), while True + try/except +
sleep, no stop path.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from core.resolver.player_health import (
    _channel_is_struggling,
    _restart_channel,
    discover_and_record,
    get_primary_player_path,
    get_scores,
    maybe_promote_best_player,
    record_probe,
)
from core.resolver.segment_sampler import sample_manifest

logger = logging.getLogger(__name__)

_started = False

_MAX_WORKERS = 2
_SAMPLE_SECONDS = 30
_PROBE_TIMEOUT = 15
_WATCHDOG_INTERVAL_SECONDS = 10
_STARTUP_DELAY_SECONDS = 20  # let the rest of app startup settle first
_EMPTY_QUEUE_BACKOFF_SECONDS = 15  # nothing live/multi-player right now

# Real, confirmed bug (found live): a resolve failure returns almost
# instantly (a connection actively refused, not a slow timeout) while
# _evaluate_one's own 30s sample only ever runs on the SUCCESS path -- so
# when a source is blocking us (or genuinely down site-wide), every probe
# fails in well under a second and the loop just re-queues the next batch
# immediately. Confirmed live: this rotation cycled through a blocked
# source's candidate paths roughly once per second, actively hammering it
# harder while blocked than while healthy -- the opposite of the backoff
# a real client would apply, and plausibly self-perpetuating the block.
# A whole batch finishing this fast is a strong, cheap-to-check signal
# that something is being refused/blocked rather than genuinely probed --
# a real per-item check (even one that legitimately times out) takes
# meaningfully longer than this.
_FAST_BATCH_THRESHOLD_SECONDS = 5
_FAST_BATCH_MAX_BACKOFF_SECONDS = 300


def _native_resolver():
    from core.resolver.manifest_resolver import _native_resolver as _load
    return _load()


def _live_channels() -> list:
    """[{channel_id, page_url, primary_manifest_id, fallback_sources}] for
    every currently-running resolved channel — the shared enumeration both
    the warming rotation and switch watchdog build their work-lists from.
    Deliberately not filtered to "primary is multi-player-capable" here:
    a channel can have foreign fallbacks (a different site/plugin entirely,
    manually added or independently discovered) worth tracking even when
    its own primary has no internal picker at all — see
    _foreign_fallback_targets."""
    from web import shared_state
    from core.diagnostics import get_live_snapshot
    from core.database import get_session
    from core.models import Capture, Manifest

    try:
        statuses = shared_state.streamer_mgr.get_all_status()
        live_ids = [row["channel_id"] for row in get_live_snapshot(statuses)]
    except Exception as e:
        logger.warning("[PLAYER-EVAL] live-channel scan failed: %s", e)
        return []

    out = []
    for channel_id in live_ids:
        ch = shared_state.channel_mgr.get_channel(channel_id)
        if not ch or ch.get("type") != "resolved" or not ch.get("manifest_id"):
            continue
        with get_session() as session:
            row = (
                session.query(Capture.page_url)
                .join(Manifest, Manifest.capture_id == Capture.id)
                .filter(Manifest.id == ch["manifest_id"])
                .first()
            )
        out.append({"channel_id": channel_id, "page_url": row[0] if row else None,
                    "primary_manifest_id": ch["manifest_id"],
                    "fallback_sources": ch.get("fallback_sources") or []})
    return out


def _foreign_fallback_targets(ch: dict) -> list:
    """This channel's configured fallback manifests that are NOT sub-paths
    of the primary's own multi-player picker — i.e. a genuinely separate
    source, whatever plugin or manual addition it came from. See
    player_health._reorder_fallbacks_by_score for the same "(player: X)"
    vs. manifest_id-keyed distinction this mirrors. Label comes from
    source_domain (generic, already-stored data — never a hardcoded site
    name) or the manifest's own title as a last resort."""
    out = []
    for fb in ch.get("fallback_sources") or []:
        title = fb.get("title") or ""
        if "(player: " in title:
            continue
        mid = fb.get("manifest_id")
        if not mid:
            continue
        out.append({"manifest_id": mid,
                    "label": fb.get("source_domain") or title or mid})
    return out


def _evaluate_one(channel_id: str, page_url: str, path: str) -> None:
    """One warming-rotation item: resolve this specific path, then run a
    real 30s sample against it if it resolves. Always ends by recording an
    outcome — record_probe is what the switch watchdog reads later, so a
    resolve failure here is just as important to record as a sample
    failure (both mean "don't switch to this path right now")."""
    native = _native_resolver()
    if native is None or not hasattr(native, "probe_one_player"):
        return
    from urllib.parse import urlparse
    from core.source_registry import is_domain_enabled
    enabled, reason = is_domain_enabled(urlparse(page_url).netloc)
    if not enabled:
        logger.info("[PLAYER-EVAL] Skipping %s/%s probe — source disabled (%s)", channel_id, path, reason)
        return
    try:
        result = native.probe_one_player(page_url, path, _PROBE_TIMEOUT)
    except Exception as e:
        logger.warning("[PLAYER-EVAL] %s/%s probe failed: %s", channel_id, path, e)
        record_probe(channel_id, path, False, error=str(e))
        return
    if not result:
        return
    if not result.get("ok") or not result.get("capture"):
        record_probe(channel_id, path, False, latency_ms=result.get("latency_ms"),
                     error=result.get("error"), label=result.get("label"))
        return
    try:
        sample = sample_manifest(result["capture"], duration_seconds=_SAMPLE_SECONDS)
    except Exception as e:
        logger.warning("[PLAYER-EVAL] %s/%s sample failed: %s", channel_id, path, e)
        record_probe(channel_id, path, False, latency_ms=result.get("latency_ms"),
                     error=str(e), label=result.get("label"))
        return
    record_probe(channel_id, path, sample["ok"], latency_ms=result.get("latency_ms"),
                 error=None if sample["ok"] else f"stall_gap={sample['max_stall_gap']}s",
                 label=result.get("label"))
    logger.info("[PLAYER-EVAL] %s/%s sampled: ok=%s segments_ok=%d segments_failed=%d max_stall_gap=%.1fs",
                channel_id, path, sample["ok"], sample["segments_ok"],
                sample["segments_failed"], sample["max_stall_gap"])


def _evaluate_foreign_fallback(channel_id: str, manifest_id: str, label: str) -> None:
    """One warming-rotation item for a fallback that's a genuinely separate
    source (not a sub-path of the primary's own picker) — already a
    resolved manifest sitting in the DB, so this skips discovery entirely
    and goes straight to the same 30s real sample any other candidate
    gets. Tracked under manifest_id as its player_path key (see
    _foreign_fallback_targets) since it has no "path" concept of its own."""
    from core.database import get_session
    from core.models import Manifest

    with get_session() as session:
        m = session.query(Manifest).filter_by(id=manifest_id).first()
        if m is None:
            return
        capture = {
            "manifest_url": m.url,
            "body": m.body,
            "referer": f"https://{m.source_domain}/" if m.source_domain else None,
        }
    try:
        sample = sample_manifest(capture, duration_seconds=_SAMPLE_SECONDS)
    except Exception as e:
        logger.warning("[PLAYER-EVAL] fallback %s/%s sample failed: %s", channel_id, manifest_id, e)
        record_probe(channel_id, manifest_id, False, error=str(e), label=label)
        return
    record_probe(channel_id, manifest_id, sample["ok"],
                 error=None if sample["ok"] else f"stall_gap={sample['max_stall_gap']}s",
                 label=label)
    logger.info("[PLAYER-EVAL] fallback %s/%s (%s) sampled: ok=%s segments_ok=%d segments_failed=%d max_stall_gap=%.1fs",
                channel_id, manifest_id, label, sample["ok"], sample["segments_ok"],
                sample["segments_failed"], sample["max_stall_gap"])


def _build_warming_queue() -> list:
    """One item per non-primary tracked candidate of every currently-live
    channel, oldest-probed (or never-probed) first — both flavors folded
    into the same queue/priority order: "kind": "player_path" (a sub-path
    of the primary's own multi-player picker, needs the plugin to
    resolve it) and "kind": "foreign_fallback" (an already-resolved
    manifest from a separate source entirely, sampled directly). Seeds a
    multi-player channel's candidate pool with a one-time discover_and_
    record the first time this rotation sees it live with no tracked
    paths yet."""
    native = _native_resolver()
    supports_discovery = native is not None and hasattr(native, "discover_all_players")

    items = []
    for ch in _live_channels():
        channel_id = ch["channel_id"]

        is_multiplayer = False
        if supports_discovery and ch["page_url"]:
            try:
                is_multiplayer = native.handles(ch["page_url"])
            except Exception:
                is_multiplayer = False

        if is_multiplayer:
            scores = get_scores(channel_id)
            if not scores:
                try:
                    discover_and_record(channel_id, ch["primary_manifest_id"], timeout=_PROBE_TIMEOUT)
                    scores = get_scores(channel_id)
                except Exception as e:
                    logger.warning("[PLAYER-EVAL] seed discovery failed for %s: %s", channel_id, e)
                    scores = {}
            primary_path = get_primary_player_path(ch["primary_manifest_id"])
            for path, row in scores.items():
                if path == primary_path:
                    continue
                items.append({"kind": "player_path", "channel_id": channel_id,
                              "page_url": ch["page_url"], "path": path,
                              "last_probed_at": row.last_probed_at})

        foreign = _foreign_fallback_targets(ch)
        if foreign:
            scores = get_scores(channel_id)
            for fb in foreign:
                row = scores.get(fb["manifest_id"])
                items.append({"kind": "foreign_fallback", "channel_id": channel_id,
                              "manifest_id": fb["manifest_id"], "label": fb["label"],
                              "last_probed_at": row.last_probed_at if row else None})

    items.sort(key=lambda it: it["last_probed_at"] or datetime.min.replace(tzinfo=timezone.utc))
    return items


def _dispatch_warming_item(item: dict) -> None:
    if item["kind"] == "foreign_fallback":
        _evaluate_foreign_fallback(item["channel_id"], item["manifest_id"], item["label"])
    else:
        _evaluate_one(item["channel_id"], item["page_url"], item["path"])


def _warming_loop() -> None:
    time.sleep(_STARTUP_DELAY_SECONDS)
    consecutive_fast_batches = 0
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        while True:
            try:
                queue = _build_warming_queue()
                if not queue:
                    time.sleep(_EMPTY_QUEUE_BACKOFF_SECONDS)
                    consecutive_fast_batches = 0
                    continue
                batch = queue[:_MAX_WORKERS]
                batch_start = time.monotonic()
                futures = [pool.submit(_dispatch_warming_item, it) for it in batch]
                for f in futures:
                    f.result()
                elapsed = time.monotonic() - batch_start
                if elapsed < _FAST_BATCH_THRESHOLD_SECONDS:
                    consecutive_fast_batches += 1
                    backoff = min(_FAST_BATCH_MAX_BACKOFF_SECONDS,
                                  _EMPTY_QUEUE_BACKOFF_SECONDS * (2 ** min(consecutive_fast_batches, 6)))
                    logger.warning(
                        "[PLAYER-EVAL] batch finished in %.1fs (< %ds) -- likely a source-wide "
                        "block/outage rather than real per-item checks; backing off %ds "
                        "(%d consecutive fast batch(es))",
                        elapsed, _FAST_BATCH_THRESHOLD_SECONDS, backoff, consecutive_fast_batches)
                    time.sleep(backoff)
                else:
                    consecutive_fast_batches = 0
            except Exception as e:
                logger.error("[PLAYER-EVAL] warming loop error: %s", e)
                time.sleep(_EMPTY_QUEUE_BACKOFF_SECONDS)


def _watchdog_loop() -> None:
    time.sleep(_STARTUP_DELAY_SECONDS)
    while True:
        try:
            for ch in _live_channels():
                channel_id = ch["channel_id"]
                if not _channel_is_struggling(channel_id):
                    continue
                try:
                    if maybe_promote_best_player(channel_id):
                        logger.info("[PLAYER-EVAL] channel %s: struggling, promoted a healthier "
                                    "player and restarting now", channel_id)
                        _restart_channel(channel_id)
                except Exception as e:
                    logger.warning("[PLAYER-EVAL] watchdog promote failed for %s: %s", channel_id, e)
        except Exception as e:
            logger.error("[PLAYER-EVAL] watchdog loop error: %s", e)
        time.sleep(_WATCHDOG_INTERVAL_SECONDS)


def start_player_evaluator() -> None:
    global _started
    if _started:
        return
    _started = True
    threading.Thread(target=_warming_loop, daemon=True, name="player-eval-warming").start()
    threading.Thread(target=_watchdog_loop, daemon=True, name="player-eval-watchdog").start()
    logging.info("[PLAYER-EVAL] Started continuous evaluator (warming rotation + switch watchdog)")
