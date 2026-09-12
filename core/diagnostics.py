"""Real-time per-stream diagnostics.

Captures timing/health telemetry for streams as they run — encode speed vs
realtime, segment/chunk fetch latency, relay reconnect gaps, consecutive-
failure counters, playlist-serve wait time, fallback-chain activations —
independent of StreamerManager's stream-instance lifetime. That matters
because StreamerManager.cleanup_idle() (core/streamer.py) deletes the
streamer instance outright on a 5-min idle timeout, and any per-instance
counters (consecutive_errors, etc.) die with it. This module is a sibling
store streamer classes write into and never read back from, so the data
outlives any one run of a stream.

Mirrors core/vpn_monitor.py's shape: module-level dict/deque state guarded
by a single lock, plain read/write functions, a periodic Postgres rollup
(rollup_tick, registered the same way vpn_monitor.sample_latency is).
"""

import logging
import queue
import threading
import time
from collections import deque

logger = logging.getLogger(__name__)

_MAX_SAMPLES_PER_METRIC = 720
_MAX_EVENTS_PER_CHANNEL = 200

_lock = threading.Lock()
_samples: dict = {}   # {channel_id: {metric: deque[{"ts": float, "value": float}]}}
_events: dict = {}    # {channel_id: deque[{"ts": float, "type": str, "detail": dict}]}
_counters: dict = {}  # {channel_id: {counter_name: int}}
_meta: dict = {}      # {channel_id: {"source_kind":, "encoder_mode":, "fallback_active":, "started_at":}}

# ── SSE pub/sub ──
# Separate lock from _lock above — subscriber bookkeeping must never add
# contention to the hot record_*() path called from every ffmpeg feeder/
# poller thread. Subscribers are single-slot queues: a "ping" just means
# "something changed for this channel, go re-read current state" — the SSE
# generator always re-fetches via get_summary()/get_live_snapshot() on
# wake, so there's no need to queue every individual sample (that's what
# the ring buffers in _samples are for).
_sub_lock = threading.Lock()
_subscribers_all: set = set()
_subscribers_by_channel: dict = {}


def subscribe(channel_id: str = None) -> "queue.Queue":
    """Register a new SSE listener. channel_id=None subscribes to every
    channel's changes (the dashboard-wide feed); a specific channel_id only
    wakes on that channel's changes (drill-down modal / wall tile)."""
    q = queue.Queue(maxsize=1)
    with _sub_lock:
        if channel_id is None:
            _subscribers_all.add(q)
        else:
            _subscribers_by_channel.setdefault(channel_id, set()).add(q)
    return q


def unsubscribe(channel_id: str, q: "queue.Queue") -> None:
    with _sub_lock:
        if channel_id is None:
            _subscribers_all.discard(q)
        else:
            subs = _subscribers_by_channel.get(channel_id)
            if subs is not None:
                subs.discard(q)
                if not subs:
                    _subscribers_by_channel.pop(channel_id, None)


def _notify(channel_id: str) -> None:
    with _sub_lock:
        targets = list(_subscribers_all) + list(_subscribers_by_channel.get(channel_id, ()))
    for q in targets:
        try:
            q.put_nowait(True)
        except queue.Full:
            pass  # a ping is already pending — the eventual re-read covers this one too

# ── Quality scoring thresholds (v1 — naive, first-guess values; expect to
# retune once real distributions are visible on the dashboard). Kept as a
# flat block of module constants so a future pass can retune without
# spelunking through the scoring function itself. ──
SPEED_RATIO_EXCELLENT = 0.98
SPEED_RATIO_GOOD = 0.90         # commit 94d0c1e measured ~0.915 under CPU
                                # contention, which visibly buffered — that's
                                # the empirical anchor for this floor

RECONNECT_GAP_EXCELLENT_MS = 500
RECONNECT_GAP_GOOD_MS = 2000

ERROR_RATE_EXCELLENT_PER_5MIN = 0
ERROR_RATE_GOOD_PER_5MIN = 2

FETCH_LATENCY_EXCELLENT_MS = 500
FETCH_LATENCY_GOOD_MS = 2000

# Which metric to show as a dashboard-card sparkline / default drill-down
# tab, in priority order. NOT every metric applies to every stream mode —
# encode_speed_ratio only exists where a re-encode actually happens (relay,
# local/schedule, multi-mode resolved); proxy/remux are pure `-c copy` and
# will NEVER produce it. Picking the first metric that actually has samples
# (rather than hardcoding encode_speed_ratio) is what keeps proxy/remux
# streams from showing "collecting data" forever for a metric that
# structurally cannot exist for their mode.
SPARK_METRIC_PRIORITY = [
    "encode_speed_ratio", "production_speed_ratio", "relay_read_latency_ms",
    "fetch_latency_ms", "reconnect_gap_ms", "playlist_cold_wait_ms",
    "playlist_warm_wait_ms",
]


# ── Write API — called from streamer classes / segment_sources / hls.py ──

def record_sample(channel_id: str, metric: str, value) -> None:
    if channel_id is None or value is None:
        return
    now = time.time()
    with _lock:
        dq = _samples.setdefault(channel_id, {}).setdefault(
            metric, deque(maxlen=_MAX_SAMPLES_PER_METRIC))
        dq.append({"ts": now, "value": float(value)})
    _notify(channel_id)


def record_event(channel_id: str, event_type: str, detail: dict = None) -> None:
    if channel_id is None:
        return
    now = time.time()
    with _lock:
        dq = _events.setdefault(channel_id, deque(maxlen=_MAX_EVENTS_PER_CHANNEL))
        dq.append({"ts": now, "type": event_type, "detail": detail or {}})
    _notify(channel_id)


# Client-reported playback events — the video element itself telling us it
# stalled or jumped, as opposed to every other metric here which infers
# player experience from server-side proxies. Allowlisted event types only:
# this endpoint is reachable from any browser hitting the API, so it
# shouldn't be able to write arbitrary event/counter names into the store.
CLIENT_EVENT_TYPES = {"client_stall", "client_seek_jump"}


def record_client_event(channel_id: str, event_type: str, detail: dict = None) -> bool:
    """Returns False (and records nothing) if event_type isn't allowlisted."""
    if event_type not in CLIENT_EVENT_TYPES:
        return False
    record_event(channel_id, event_type, detail)
    return True


def incr_counter(channel_id: str, counter: str, by: int = 1) -> None:
    if channel_id is None:
        return
    with _lock:
        c = _counters.setdefault(channel_id, {})
        c[counter] = c.get(counter, 0) + by
    _notify(channel_id)


def set_meta(channel_id: str, **kwargs) -> None:
    if channel_id is None:
        return
    with _lock:
        m = _meta.setdefault(channel_id, {"started_at": time.time()})
        m.update(kwargs)
    _notify(channel_id)


def clear_channel(channel_id: str) -> None:
    """Reset a channel's diagnostics state. Call on a fresh stream start so
    a restarted stream doesn't inherit a dead run's error counts/events."""
    with _lock:
        _samples.pop(channel_id, None)
        _events.pop(channel_id, None)
        _counters.pop(channel_id, None)
        _meta.pop(channel_id, None)
    _notify(channel_id)


# ── Scoring ──

def score_quality(*, encode_speed_ratio=None, reconnect_gap_ms_max=None,
                   error_count_5m=None, fetch_latency_ms_avg=None,
                   fallback_active=False) -> str:
    """Returns "excellent" | "good" | "bad" | "unknown". A metric that's None
    (doesn't apply to this stream's mode, e.g. encode_speed_ratio for a pure
    copy/remux/proxy stream) is skipped rather than penalized. Running on a
    fallback source at all is an automatic "bad" — the primary is broken."""
    if fallback_active:
        return "bad"

    scores = []
    if encode_speed_ratio is not None:
        scores.append(
            "excellent" if encode_speed_ratio >= SPEED_RATIO_EXCELLENT else
            "good" if encode_speed_ratio >= SPEED_RATIO_GOOD else "bad")
    if reconnect_gap_ms_max is not None:
        scores.append(
            "excellent" if reconnect_gap_ms_max <= RECONNECT_GAP_EXCELLENT_MS else
            "good" if reconnect_gap_ms_max <= RECONNECT_GAP_GOOD_MS else "bad")
    if error_count_5m is not None:
        scores.append(
            "excellent" if error_count_5m <= ERROR_RATE_EXCELLENT_PER_5MIN else
            "good" if error_count_5m <= ERROR_RATE_GOOD_PER_5MIN else "bad")
    if fetch_latency_ms_avg is not None:
        scores.append(
            "excellent" if fetch_latency_ms_avg <= FETCH_LATENCY_EXCELLENT_MS else
            "good" if fetch_latency_ms_avg <= FETCH_LATENCY_GOOD_MS else "bad")

    if not scores:
        return "unknown"
    if "bad" in scores:
        return "bad"
    if "good" in scores:
        return "good"
    return "excellent"


# ── Read API ──

def _snapshot_channel(channel_id: str):
    """Copy out everything needed for one channel while holding the lock, so
    all downstream math runs against plain lists, never the live deques."""
    with _lock:
        samples = {m: list(dq) for m, dq in _samples.get(channel_id, {}).items()}
        events = list(_events.get(channel_id, ()))
        counters = dict(_counters.get(channel_id, {}))
        meta = dict(_meta.get(channel_id, {}))
    return samples, events, counters, meta


def _window_stats(values_with_ts: list, window_seconds: float):
    """avg/max of sample values with ts within the trailing window. Returns
    (avg, max) or (None, None) if nothing falls in the window."""
    if not values_with_ts:
        return None, None
    cutoff = time.time() - window_seconds
    vals = [s["value"] for s in values_with_ts if s["ts"] >= cutoff]
    if not vals:
        return None, None
    return sum(vals) / len(vals), max(vals)


def get_summary(channel_id: str) -> dict:
    samples, events, counters, meta = _snapshot_channel(channel_id)

    # Averaged over a trailing 60s window rather than trusting the single
    # latest sample — both metrics genuinely oscillate sample-to-sample
    # (a burst of segments landing back-to-back reads as "ahead," the gap
    # right after reads as "behind"), so the last value alone is noisy
    # enough to flip the quality badge on an unlucky sample even when the
    # real trend is healthy. Averaging is what the window is for.
    encode_speed_ratio, _ = _window_stats(samples.get("encode_speed_ratio", []), 60)
    # production_speed_ratio (remux/proxy: content-seconds emitted per
    # wall-second) is the equivalent "are we keeping up with realtime"
    # signal for modes that never re-encode, so encode_speed_ratio is
    # always None for them. Falls back to it for scoring only when
    # encode_speed_ratio itself doesn't apply — never both at once.
    production_speed_ratio, _ = _window_stats(samples.get("production_speed_ratio", []), 60)
    speed_ratio_for_scoring = (encode_speed_ratio if encode_speed_ratio is not None
                               else production_speed_ratio)

    # Pick whichever metric actually has data, in priority order, instead of
    # hardcoding encode_speed_ratio — see SPARK_METRIC_PRIORITY's docstring.
    spark_metric = next((m for m in SPARK_METRIC_PRIORITY if samples.get(m)), None)
    spark_values = ([round(s["value"], 3) for s in samples[spark_metric][-30:]]
                    if spark_metric else [])

    fetch_avg, fetch_max = _window_stats(samples.get("fetch_latency_ms", []), 300)
    _, reconnect_gap_max = _window_stats(samples.get("reconnect_gap_ms", []), 300)
    _, cold_wait_max = _window_stats(samples.get("playlist_cold_wait_ms", []), 300)
    _, warm_wait_max = _window_stats(samples.get("playlist_warm_wait_ms", []), 300)
    playlist_wait_max = max([v for v in (cold_wait_max, warm_wait_max) if v is not None], default=None)

    cutoff_5m = time.time() - 300
    reconnects_5m = sum(1 for e in events if e["type"] == "relay_reconnect" and e["ts"] >= cutoff_5m)
    resyncs_5m = sum(1 for e in events if e["type"] == "resync_skip" and e["ts"] >= cutoff_5m)
    # source_stall is proxy mode's equivalent of resync_skip — the upstream
    # playlist stopped advancing even though fetches keep returning 200. See
    # ProxyStream._poller_loop_inner's SOURCE_STALL_SECONDS.
    source_stalls_5m = sum(1 for e in events if e["type"] == "source_stall" and e["ts"] >= cutoff_5m)
    # Client-reported playback events are the most direct signal there is —
    # everything else here is a proxy for "is the viewer actually seeing a
    # problem"; these ARE that problem, reported by the video element
    # itself (web player only — see record_client_event()).
    client_stalls_5m = sum(1 for e in events if e["type"] == "client_stall" and e["ts"] >= cutoff_5m)
    client_seeks_5m = sum(1 for e in events if e["type"] == "client_seek_jump" and e["ts"] >= cutoff_5m)
    error_events_5m = sum(1 for e in events
                          if e["type"] in ("give_up", "fallback_chain_exhausted",
                                            "playlist_wait_timeout", "resync_skip",
                                            "source_stall",
                                            "client_stall", "client_seek_jump")
                          and e["ts"] >= cutoff_5m)

    fallback_active = bool(meta.get("fallback_active", False))

    quality = score_quality(
        encode_speed_ratio=speed_ratio_for_scoring,
        reconnect_gap_ms_max=reconnect_gap_max,
        error_count_5m=error_events_5m,
        fetch_latency_ms_avg=fetch_avg,
        fallback_active=fallback_active,
    )

    return {
        "source_kind": meta.get("source_kind"),
        "encoder_mode": meta.get("encoder_mode"),
        "started_at": meta.get("started_at"),
        "encode_speed_ratio": encode_speed_ratio,
        "production_speed_ratio": production_speed_ratio,
        "spark_metric": spark_metric,
        "spark_values": spark_values,
        "fetch_latency_ms_avg": round(fetch_avg, 1) if fetch_avg is not None else None,
        "fetch_latency_ms_max": round(fetch_max, 1) if fetch_max is not None else None,
        "reconnect_gap_ms_max": round(reconnect_gap_max, 1) if reconnect_gap_max is not None else None,
        "resyncs_last_5m": resyncs_5m,
        "source_stalls_last_5m": source_stalls_5m,
        "client_stalls_last_5m": client_stalls_5m,
        "client_seeks_last_5m": client_seeks_5m,
        "playlist_wait_ms_max": round(playlist_wait_max, 1) if playlist_wait_max is not None else None,
        "reconnects_last_5m": reconnects_5m,
        "errors_last_5m": error_events_5m,
        "fallback_active": fallback_active,
        "quality": quality,
    }


def get_history(channel_id: str, minutes: int = 60) -> dict:
    samples, events, counters, meta = _snapshot_channel(channel_id)
    cutoff = time.time() - (minutes * 60)
    return {
        "samples": {
            metric: [s for s in vals if s["ts"] >= cutoff]
            for metric, vals in samples.items()
        },
        "events": [e for e in events if e["ts"] >= cutoff],
        "counters": counters,
        "meta": meta,
    }


def get_live_snapshot(active_statuses: dict) -> list:
    """One row per currently-running stream, joined against the caller's own
    view of StreamerManager.get_all_status() — diagnostics never tracks
    "what's active" independently, StreamerManager already does that
    correctly (including idle-cleanup)."""
    out = []
    for channel_id, status in (active_statuses or {}).items():
        if not status.get("running"):
            continue
        row = {
            "channel_id": channel_id,
            "uptime": status.get("uptime", 0),
            "now_playing": status.get("now_playing", ""),
        }
        row.update(get_summary(channel_id))
        out.append(row)
    return out


# ── Postgres rollup ──

def rollup_tick(active_statuses: dict = None) -> None:
    """Runs every ~60s. Writes one StreamDiagnosticSnapshot row per channel
    that's either currently running or was tracked since the last tick (so a
    channel that stopped between ticks still gets one final row instead of
    silently vanishing)."""
    with _lock:
        tracked_ids = set(_meta.keys())
    active_ids = {cid for cid, st in (active_statuses or {}).items() if st.get("running")}
    channel_ids = tracked_ids | active_ids
    if not channel_ids:
        return

    from core.database import get_session
    from core.models.diagnostics import StreamDiagnosticSnapshot

    with get_session() as session:
        for channel_id in channel_ids:
            try:
                summary = get_summary(channel_id)
                _, events, _, meta = _snapshot_channel(channel_id)
                cutoff = time.time() - 60
                reconnect_count = sum(1 for e in events
                                      if e["type"] == "relay_reconnect" and e["ts"] >= cutoff)
                row = StreamDiagnosticSnapshot(
                    channel_id=channel_id,
                    source_kind=summary.get("source_kind"),
                    encoder_mode=summary.get("encoder_mode"),
                    encode_speed_ratio=summary.get("encode_speed_ratio"),
                    fetch_latency_ms_avg=summary.get("fetch_latency_ms_avg"),
                    fetch_latency_ms_max=summary.get("fetch_latency_ms_max"),
                    reconnect_count=reconnect_count,
                    reconnect_gap_ms_max=summary.get("reconnect_gap_ms_max"),
                    error_count=summary.get("errors_last_5m", 0),
                    fallback_active=summary.get("fallback_active", False),
                    playlist_wait_ms_max=summary.get("playlist_wait_ms_max"),
                    quality_badge=summary.get("quality", "unknown"),
                )
                session.add(row)
            except Exception as e:
                logger.warning("[DIAGNOSTICS] rollup failed for %s: %s", channel_id, e)

            # A channel that's no longer running and has no fresh activity
            # doesn't need to keep being rolled up forever — drop it from
            # _meta once we've written its final row so rollup_tick's
            # working set doesn't grow unbounded across many short-lived
            # event channels.
            if channel_id not in active_ids:
                with _lock:
                    _meta.pop(channel_id, None)
