"""Auto-ranking of multi-player-source path candidates via rolling health
scores — replaces a static, manually-set primary/fallback choice for these
sources with one driven by actual observed reliability (shallow probes +
history). Some source families expose the same stream through several
alternate player paths that fail independently of one another; which
sources this applies to and how a path is discovered/resolved lives
entirely in the gitignored native-resolver plugin (scrapers/), never here.

Three entry points:
- discover_and_record(): reactive — probes every known player path for one
  channel right now and persists the outcome. Called from
  ManifestResolverService.discover_and_store_fallbacks (piggybacks on the
  already-expensive "whole fallback chain exhausted" moment) and directly
  by the periodic tick below.
- probe_due_channels(): the periodic background tick — re-probes any
  tracked multi-player channel whose scores have gone stale, keeping data
  warm independent of whether a chain-exhaustion event happens to fire.
  Registered with the scheduler in web/app.py.
- maybe_promote_best_player(): after recording fresh scores for a channel,
  checks whether some other tracked path is now clearly outperforming the
  current primary and, if so, promotes it via channel_mgr.set_primary_
  manifest — this is what makes primary/fallback choice non-manual.

All of this is pure HTTP (native resolver only) — no selenium/browser cost,
so unlike manifest_refresh/event_resolver it does NOT need pipeline_lock
(that lock exists specifically to gate the sidecar's single browser).
"""

import logging
from collections import namedtuple
from datetime import datetime, timedelta, timezone

from core.database import get_session
from core.models import PlayerHealthScore

logger = logging.getLogger(__name__)

# get_session()'s commit() expires every attribute on its ORM objects, and
# the session itself is gone by the time a caller outside the `with` block
# would touch one — any attribute access at that point raises
# DetachedInstanceError. get_scores() hands snapshots like this out instead
# of live rows so callers can use them freely after the session closes.
_ScoreSnapshot = namedtuple(
    "_ScoreSnapshot",
    "player_path success_count failure_count consecutive_successes "
    "consecutive_failures last_probed_at last_ok last_latency_ms last_error",
)

# A tracked path's score is trusted as current for this long before the
# periodic tick bothers re-probing it. Short enough to catch a path going
# bad within one tick's reach, long enough that steady-state healthy
# channels aren't re-probed needlessly every cycle.
STALE_AFTER_SECONDS = 600

# Promotion guardrails — avoid flapping between two paths that are both
# "fine" or promoting on a single lucky/unlucky probe:
PROMOTE_MIN_SAMPLES = 2      # challenger needs at least this many probes
PROMOTE_MIN_MARGIN = 0.25    # ...and must beat the current primary's score
                             # by at least this much to be worth switching for


def _native_resolver():
    from core.resolver.manifest_resolver import _native_resolver as _load
    return _load()


def score_of(row: "_ScoreSnapshot | None") -> float:
    """Higher is better, roughly in [0, 1]. A never-probed path scores 0.5
    (neutral) so it gets a fair chance rather than being ranked as "bad" by
    default. Recent behavior is weighted harder than lifetime average via
    the streak penalty — a path mid-failing-streak should rank below one
    with a similar overall success rate but currently healthy."""
    if row is None:
        return 0.5
    total = row.success_count + row.failure_count
    if total == 0:
        return 0.5
    success_rate = row.success_count / total
    streak_penalty = min(row.consecutive_failures, 5) * 0.15
    return max(0.0, success_rate - streak_penalty)


def get_scores(channel_id: str) -> dict:
    """{player_path: _ScoreSnapshot} for one channel."""
    with get_session() as session:
        rows = session.query(PlayerHealthScore).filter_by(channel_id=channel_id).all()
        return {
            r.player_path: _ScoreSnapshot(
                player_path=r.player_path,
                success_count=r.success_count,
                failure_count=r.failure_count,
                consecutive_successes=r.consecutive_successes,
                consecutive_failures=r.consecutive_failures,
                last_probed_at=r.last_probed_at,
                last_ok=r.last_ok,
                last_latency_ms=r.last_latency_ms,
                last_error=r.last_error,
            )
            for r in rows
        }


def _is_stale(row: "_ScoreSnapshot | None") -> bool:
    if row is None or row.last_probed_at is None:
        return True
    return datetime.now(timezone.utc) - row.last_probed_at > timedelta(seconds=STALE_AFTER_SECONDS)


def record_probe(channel_id: str, player_path: str, ok: bool,
                  latency_ms: float | None = None, error: str | None = None) -> None:
    with get_session() as session:
        row = (session.query(PlayerHealthScore)
               .filter_by(channel_id=channel_id, player_path=player_path).first())
        if row is None:
            row = PlayerHealthScore(channel_id=channel_id, player_path=player_path,
                                    success_count=0, failure_count=0,
                                    consecutive_successes=0, consecutive_failures=0)
            session.add(row)
        if ok:
            row.success_count += 1
            row.consecutive_successes += 1
            row.consecutive_failures = 0
        else:
            row.failure_count += 1
            row.consecutive_failures += 1
            row.consecutive_successes = 0
        row.last_probed_at = datetime.now(timezone.utc)
        row.last_ok = ok
        row.last_latency_ms = latency_ms
        row.last_error = error


def discover_and_record(channel_id: str, primary_manifest_id: str, timeout: int = 20):
    """Probe every known player path for this channel's stream id and
    persist the outcome of each. Returns the raw per-path result list (each
    item also carries "page_url", needed by callers that go on to store a
    new fallback manifest), or None if this channel isn't a multi-player
    source at all (nothing to do)."""
    from core.database import get_session as _get_session
    from core.models import Capture, Manifest

    native = _native_resolver()
    if native is None or not hasattr(native, "discover_all_players"):
        return None

    with _get_session() as session:
        row = (
            session.query(Capture.page_url)
            .join(Manifest, Manifest.capture_id == Capture.id)
            .filter(Manifest.id == primary_manifest_id)
            .first()
        )
    page_url = row[0] if row else None
    if not page_url:
        return None

    try:
        if not native.handles(page_url):
            return None
    except Exception:
        return None

    try:
        results = native.discover_all_players(page_url, timeout)
    except Exception as e:
        logger.warning("[PLAYER-HEALTH] discovery failed for channel %s: %s", channel_id, e)
        return None

    for item in results:
        record_probe(channel_id, item["path"], item["ok"],
                     latency_ms=item.get("latency_ms"), error=item.get("error"))
        item["page_url"] = page_url

    logger.info("[PLAYER-HEALTH] channel %s: probed %d player path(s), %d healthy",
                channel_id, len(results), sum(1 for r in results if r["ok"]))
    return results


def maybe_promote_best_player(channel_id: str) -> bool:
    """After discover_and_record has run for this channel, check whether a
    tracked fallback is now clearly outperforming the current primary and
    promote it if so (channel_mgr.set_primary_manifest — the old primary
    drops into the fallback chain rather than being discarded). Returns
    True if a promotion happened.

    Only ever compares against fallbacks this app already knows about
    (has a stored Manifest for) — a path scoring well in player_health but
    with no corresponding fallback manifest yet (e.g. discovery hasn't
    stored it, or it failed the probe this round) can't be promoted to,
    there'd be nothing to actually stream.
    """
    from web import shared_state

    ch = shared_state.channel_mgr.get_channel(channel_id)
    if not ch or ch.get("type") != "resolved":
        return False

    scores = get_scores(channel_id)
    primary_score = score_of(scores.get("stream"))

    # Map each stored fallback manifest back to its player_path via the
    # title convention discover_and_store_fallbacks uses ("... (player: X)").
    best_path, best_manifest_id, best_score = None, None, primary_score
    for fb in (ch.get("fallback_sources") or []):
        title = fb.get("title") or ""
        if "(player: " not in title:
            continue
        path = title.rsplit("(player: ", 1)[1].rstrip(")")
        row = scores.get(path)
        if row is None or (row.success_count + row.failure_count) < PROMOTE_MIN_SAMPLES:
            continue
        s = score_of(row)
        if s > best_score:
            best_path, best_manifest_id, best_score = path, fb.get("manifest_id"), s

    if best_manifest_id is None or best_score - primary_score < PROMOTE_MIN_MARGIN:
        return False

    logger.info("[PLAYER-HEALTH] channel %s: promoting player %r (score %.2f) over current "
                "primary (score %.2f)", channel_id, best_path, best_score, primary_score)
    shared_state.channel_mgr.set_primary_manifest(channel_id, best_manifest_id)
    return True



# A struggling channel is probed immediately regardless of score staleness
# -- these thresholds intentionally reuse the existing diagnostics verdicts
# (core/diagnostics.py) rather than inventing a separate signal: quality
# already summarizes several metrics into one badge, and source_stalls is
# the exact counter that first caught this whole failure class (a channel
# sitting stalled for extended stretches while every other diagnostic read
# clean) but that nothing previously acted on -- it only ever reached a log
# line and a dashboard badge.
_STRUGGLING_SOURCE_STALLS_5M = 2


def _channel_is_struggling(channel_id: str) -> bool:
    from core.diagnostics import get_summary
    try:
        summary = get_summary(channel_id)
    except Exception:
        return False
    return (summary.get("quality") == "bad"
            or (summary.get("source_stalls_last_5m") or 0) >= _STRUGGLING_SOURCE_STALLS_5M)


def _restart_channel(channel_id: str) -> None:
    """Force a currently-running channel to re-resolve and restart right
    now, rather than waiting for the player's next request to notice it's
    down. Same stop+restart pattern as the diagnostics reload button
    (web/routers/diagnostics.py) -- needed here because promoting a new
    primary (set_primary_manifest) only updates the DB row; a stream that's
    already running keeps polling its old (struggling) manifest until
    something makes it re-fetch from schedule."""
    from web import shared_state
    from web.routers.hls import _start_from_schedule
    shared_state.streamer_mgr.stop_channel(channel_id)
    _start_from_schedule(channel_id)


def probe_due_channels(timeout: int = 15) -> None:
    """Periodic tick (registered in web/app.py): re-probe any tracked
    multi-player channel whose scores have gone stale, then re-evaluate
    primary/fallback ranking for it. Also immediately (regardless of
    staleness) re-probes any currently-running channel the live diagnostics
    dashboard already considers to be struggling -- see
    _channel_is_struggling -- and hot-restarts it if that turns up a
    healthier player, since promotion alone doesn't affect an
    already-running stream. Pure HTTP throughout -- does not touch the
    selenium sidecar, so it runs on its own schedule independent of
    pipeline_lock/manifest_refresh."""
    from web import shared_state
    from core.diagnostics import get_live_snapshot

    with get_session() as session:
        tracked_ids = {r[0] for r in session.query(PlayerHealthScore.channel_id).distinct()}

    struggling_ids = set()
    try:
        statuses = shared_state.streamer_mgr.get_all_status()
        for row in get_live_snapshot(statuses):
            cid = row.get("channel_id")
            if cid and _channel_is_struggling(cid):
                struggling_ids.add(cid)
    except Exception as e:
        logger.warning("[PLAYER-HEALTH] live-diagnostics scan failed: %s", e)

    for channel_id in tracked_ids | struggling_ids:
        is_struggling = channel_id in struggling_ids
        if not is_struggling:
            scores = get_scores(channel_id)
            if not any(_is_stale(row) for row in scores.values()):
                continue
        ch = shared_state.channel_mgr.get_channel(channel_id)
        if not ch or not ch.get("manifest_id"):
            continue
        try:
            discovered = discover_and_record(channel_id, ch["manifest_id"], timeout=timeout)
            if discovered is None:
                continue
            promoted = maybe_promote_best_player(channel_id)
            if promoted and is_struggling:
                logger.info("[PLAYER-HEALTH] channel %s: live diagnostics flagged trouble, "
                            "promoted a healthier player and restarting now", channel_id)
                _restart_channel(channel_id)
        except Exception as e:
            logger.warning("[PLAYER-HEALTH] periodic probe failed for channel %s: %s", channel_id, e)
