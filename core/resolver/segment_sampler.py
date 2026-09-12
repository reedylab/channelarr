"""Bounded-duration, in-memory-only playlist/segment sampler.

The "30-second sample" building block for core/resolver/player_evaluator.py's
warming rotation: rather than trusting a single point-in-time check (one
playlist fetch + one segment), this actually polls a candidate manifest and
downloads its segments for a sustained window, the same way ProxyStream's
own poller would for a live channel -- catching intermittent flakiness a
one-shot probe can't see (confirmed live: a source that resolved fine, then
got flagged by anti-bot defenses seconds later on an identical request).
Nothing is written to disk; segments are fetched, validated, and discarded.
"""

import logging
import re
import time
from urllib.parse import urljoin

import requests

from core.resolver.proxy_stream import (
    POLL_INTERVAL,
    SOURCE_STALL_SECONDS,
    _fetch_segment_bytes,
    _parse_key_directive,
    _pick_best_variant,
)

logger = logging.getLogger(__name__)


def _parse_segments(playlist_text: str, base_url: str) -> list:
    """Minimal standalone playlist parser covering the same directives
    ProxyStream's own poller understands (#EXT-X-KEY, #EXTINF) -- not
    shared with it directly since that loop carries live-stream state this
    bounded, one-off sample has no use for."""
    segments = []
    current_duration = 0.0
    current_key_info = None
    for line in playlist_text.splitlines():
        line = line.strip()
        if line.startswith("#EXT-X-KEY:"):
            info = _parse_key_directive(line)
            if info and info.get("uri"):
                info["uri"] = urljoin(base_url, info["uri"])
            current_key_info = info
        elif line.startswith("#EXTINF:"):
            try:
                current_duration = float(line.split(":")[1].split(",")[0])
            except (ValueError, IndexError):
                current_duration = 6.0
        elif line and not line.startswith("#"):
            seq_match = re.search(r"(\d+)\.ts", line)
            seq = int(seq_match.group(1)) if seq_match else hash(line)
            segments.append({
                "uri": urljoin(base_url, line),
                "seq": seq,
                "duration": current_duration,
                "key_info": current_key_info,
            })
    return segments


def sample_manifest(capture: dict, duration_seconds: int = 30) -> dict:
    """Poll `capture`'s manifest for up to `duration_seconds` real time,
    downloading (and discarding) each new segment via the same fetch/
    decrypt/decoy-strip logic ProxyStream uses, to observe actual sustained
    behavior. Returns {"ok": bool, "segments_ok": int, "segments_failed":
    int, "max_stall_gap": float}.

    "ok" requires at least one segment to have actually landed and no gap
    between new segments exceeding SOURCE_STALL_SECONDS -- the same bar
    ProxyStream's own source_stall detector uses for a live channel, so
    "healthy" means the same thing here as it does in production. Isolated
    segment failures are recorded but don't fail the sample on their own
    (matches how the live poller treats them -- logged, not immediately
    fatal; only a sustained stall or total silence does).
    """
    session = requests.Session()
    headers = {"User-Agent": capture.get("user_agent") or "Mozilla/5.0"}
    if capture.get("referer"):
        headers["Referer"] = capture["referer"]
        headers["Origin"] = capture["referer"].rstrip("/")

    key_cache: dict = {}

    def _get_key(key_url: str):
        if key_url in key_cache:
            return key_cache[key_url]
        try:
            resp = session.get(key_url, headers=headers, timeout=10)
        except Exception:
            return None
        if resp.status_code != 200 or len(resp.content) != 16:
            return None
        key_cache[key_url] = resp.content
        return resp.content

    # The captured manifest_url can point at a master playlist (variant
    # streams, no actual segments) rather than a media playlist directly —
    # resolve it the same way ProxyStream does before polling for segments.
    # No extra request needed: capture["body"] is already that fetch.
    manifest_url = _pick_best_variant(capture.get("body") or "", capture["manifest_url"])
    seen_uris = set()
    segments_ok = 0
    segments_failed = 0
    last_new_segment_at = time.time()
    max_stall_gap = 0.0
    deadline = time.time() + duration_seconds

    # Seed from the already-captured body so the first poll doesn't
    # re-download everything already listed at capture time -- this sample
    # is about "does new content keep landing," not re-verifying the
    # backlog that was already implicitly checked when the manifest was
    # resolved in the first place. A no-op (empty) if manifest_url was
    # itself a master (its body has no #EXTINF/segment lines to seed from).
    try:
        for seg in _parse_segments(capture.get("body") or "", manifest_url):
            seen_uris.add(seg["uri"])
    except Exception:
        pass

    while time.time() < deadline:
        try:
            resp = session.get(manifest_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                segments_failed += 1
                time.sleep(POLL_INTERVAL)
                continue
            segments = _parse_segments(resp.text, manifest_url)
        except Exception:
            segments_failed += 1
            time.sleep(POLL_INTERVAL)
            continue

        new_found = False
        for seg in segments:
            if seg["uri"] in seen_uris:
                continue
            seen_uris.add(seg["uri"])
            new_found = True
            try:
                _fetch_segment_bytes(session, seg, headers, _get_key, "sample")
                segments_ok += 1
                last_new_segment_at = time.time()
            except Exception as e:
                segments_failed += 1
                logger.debug("[SAMPLER] segment fetch failed: %s", e)

        if not new_found:
            max_stall_gap = max(max_stall_gap, time.time() - last_new_segment_at)

        time.sleep(POLL_INTERVAL)

    ok = segments_ok > 0 and max_stall_gap < SOURCE_STALL_SECONDS
    return {
        "ok": ok,
        "segments_ok": segments_ok,
        "segments_failed": segments_failed,
        "max_stall_gap": round(max_stall_gap, 1),
    }
