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


def _native_resolver():
    from core.resolver.manifest_resolver import _native_resolver as _load
    return _load()


def _live_multiplayer_channels() -> list:
    """[{channel_id, page_url, primary_manifest_id}] for every currently-
    running channel the native resolver's discover-all hook applies to."""
    from web import shared_state
    from core.diagnostics import get_live_snapshot
    from core.database import get_session
    from core.models import Capture, Manifest

    native = _native_resolver()
    if native is None or not hasattr(native, "discover_all_players"):
        return []

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
        page_url = row[0] if row else None
        if not page_url:
            continue
        try:
            if not native.handles(page_url):
                continue
        except Exception:
            continue
        out.append({"channel_id": channel_id, "page_url": page_url,
                    "primary_manifest_id": ch["manifest_id"]})
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
                     error=result.get("error"))
        return
    try:
        sample = sample_manifest(result["capture"], duration_seconds=_SAMPLE_SECONDS)
    except Exception as e:
        logger.warning("[PLAYER-EVAL] %s/%s sample failed: %s", channel_id, path, e)
        record_probe(channel_id, path, False, latency_ms=result.get("latency_ms"), error=str(e))
        return
    record_probe(channel_id, path, sample["ok"], latency_ms=result.get("latency_ms"),
                 error=None if sample["ok"] else f"stall_gap={sample['max_stall_gap']}s")
    logger.info("[PLAYER-EVAL] %s/%s sampled: ok=%s segments_ok=%d segments_failed=%d max_stall_gap=%.1fs",
                channel_id, path, sample["ok"], sample["segments_ok"],
                sample["segments_failed"], sample["max_stall_gap"])


def _build_warming_queue() -> list:
    """One item per non-primary tracked path of every currently-live
    multi-player channel, oldest-probed (or never-probed) first. Seeds a
    channel's candidate pool with a one-time discover_and_record the first
    time this rotation sees it live and it has no tracked paths yet."""
    items = []
    for ch in _live_multiplayer_channels():
        channel_id = ch["channel_id"]
        scores = get_scores(channel_id)
        if not scores:
            try:
                discover_and_record(channel_id, ch["primary_manifest_id"], timeout=_PROBE_TIMEOUT)
                scores = get_scores(channel_id)
            except Exception as e:
                logger.warning("[PLAYER-EVAL] seed discovery failed for %s: %s", channel_id, e)
                continue
        primary_path = get_primary_player_path(ch["primary_manifest_id"])
        for path, row in scores.items():
            if path == primary_path:
                continue
            items.append({"channel_id": channel_id, "page_url": ch["page_url"],
                          "path": path, "last_probed_at": row.last_probed_at})
    items.sort(key=lambda it: it["last_probed_at"] or datetime.min.replace(tzinfo=timezone.utc))
    return items


def _warming_loop() -> None:
    time.sleep(_STARTUP_DELAY_SECONDS)
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        while True:
            try:
                queue = _build_warming_queue()
                if not queue:
                    time.sleep(_EMPTY_QUEUE_BACKOFF_SECONDS)
                    continue
                batch = queue[:_MAX_WORKERS]
                futures = [pool.submit(_evaluate_one, it["channel_id"], it["page_url"], it["path"])
                          for it in batch]
                for f in futures:
                    f.result()
            except Exception as e:
                logger.error("[PLAYER-EVAL] warming loop error: %s", e)
                time.sleep(_EMPTY_QUEUE_BACKOFF_SECONDS)


def _watchdog_loop() -> None:
    time.sleep(_STARTUP_DELAY_SECONDS)
    while True:
        try:
            for ch in _live_multiplayer_channels():
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
