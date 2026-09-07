"""Segment sources for transcode-mediated resolved channels.

A SegmentSource is however a channel's upstream actually delivers content,
turned into QueueItems landed on the shared encoder queue. One implementation
per delivery mechanism — the queue/encoder/HLS-writer in transcoder.py never
know which one is in play; they only ever see QueueItems.

This mirrors profiles.py's shape (a small, swappable, mostly-pure-behavior
object) but sits one layer lower: StreamProfile assumes the hard part — an
already-fetchable HLS playlist with individually-addressable segment URIs —
is solved, and only classifies ad-break dialect within it. A SegmentSource
is what actually solves that hard part, however a given upstream requires.

HlsPlaylistSource is today's (and so far, only) mechanism — poll a variant
playlist, classify segments via a StreamProfile, download+queue. Extracted
here unchanged from transcoder.py's original ResolvedChannelStream so this
commit is a pure refactor with no behavior change.
"""

import logging
import os
import random
import re
import subprocess
import threading
from dataclasses import dataclass
from typing import Callable, Optional
from urllib.parse import urljoin

import requests as http_requests

from core.resolver.profiles import (
    StreamProfile,
    UpstreamSegment,
    get_profile,
    detect_profile,
    CLASS_SHOW,
    CLASS_REPLACE,
)

POLL_INTERVAL_SECONDS = 2.0
INITIAL_BACKFILL_SEGMENTS = 3


@dataclass
class QueueItem:
    """Something the encoder loop should encode and pipe through. Produced
    by a SegmentSource, consumed by transcoder.py's feeder/encoder."""
    kind: str            # "upstream" | "bump"
    source_path: str     # local file path
    duration: float      # seconds; for bumps may be shorter than file's actual length
    label: str           # for logging
    cue_remaining_at_start: Optional[float] = None  # for bumps: total seconds left in cue at this bump's start


def build_bump_sequence(bump_paths: list, bump_durations: dict, target_seconds: float) -> list:
    """Fill `target_seconds` with bumps from `bump_paths`.

    Strategy: shuffle, then cycle through. The last bump is truncated to hit
    the exact target duration. Returns a list of (path, duration) tuples.

    bump_durations: {path: duration_seconds}
    """
    if not bump_paths or target_seconds <= 0:
        return []

    pool = list(bump_paths)
    random.shuffle(pool)
    sequence = []
    elapsed = 0.0
    pool_idx = 0

    while elapsed < target_seconds:
        path = pool[pool_idx % len(pool)]
        pool_idx += 1
        full_dur = bump_durations.get(path, 0)
        if full_dur <= 0:
            continue  # skip bumps with unknown durations
        remaining = target_seconds - elapsed
        if full_dur <= remaining:
            sequence.append((path, full_dur))
            elapsed += full_dur
        else:
            # Truncate the last bump to fit exactly
            sequence.append((path, remaining))
            elapsed = target_seconds
            break
        # Defensive: avoid infinite loop if all bumps have 0 duration
        if pool_idx > len(pool) * 1000:
            break

    return sequence


def default_headers(source_domain: str = "") -> dict:
    """Shared UA/Referer header builder — some CDNs reject requests without
    a valid Referer from the originating site."""
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
         "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
    if source_domain:
        h["Referer"] = f"https://{source_domain}/"
        h["Origin"] = f"https://{source_domain}"
    return h


class SegmentSource:
    """However a resolved channel's upstream actually delivers content, turn
    it into QueueItems on the shared encoder queue."""

    def run(self, enqueue: Callable[[QueueItem], None], stop_event: threading.Event) -> None:
        """Blocking. Runs until stop_event is set. Calls `enqueue` for every
        segment (show content or bump) it wants encoded, in order."""
        raise NotImplementedError


class HlsPlaylistSource(SegmentSource):
    """Poll an upstream HLS variant playlist, classify segments via a
    StreamProfile (ad-break dialect), download+decrypt, enqueue.

    Extracted verbatim from ResolvedChannelStream — same behavior, just
    given a name and a seam so a non-HLS source can sit next to it.
    """

    def __init__(self, *, channel_id: str, manifest_id: str, manifest_url: str,
                 profile_name: str = "auto", bump_paths: list | None = None,
                 bump_durations: dict | None = None, source_domain: str = "",
                 download_dir: str = "/tmp"):
        self.channel_id = channel_id
        self.manifest_id = manifest_id
        self.manifest_url = manifest_url
        self.profile_name = profile_name
        self.bump_paths = list(bump_paths or [])
        self.bump_durations = dict(bump_durations or {})
        self.source_domain = source_domain
        self._download_dir = download_dir
        self.profile: Optional[StreamProfile] = None  # resolved on first poll

    def _upstream_headers(self) -> dict:
        return default_headers(self.source_domain)

    def _refresh_manifest_url(self) -> Optional[str]:
        """Trigger a synchronous re-resolve of the manifest via selenium.
        Used when the upstream variant URL 403s and the cached token has
        expired. Returns the fresh manifest URL or None on failure."""
        try:
            from core.resolver.manifest_resolver import ManifestResolverService
            from core.database import get_session
            from core.models.manifest import Manifest

            result = ManifestResolverService.refresh_manifest(self.manifest_id)
            if not result.get("ok"):
                logging.warning(
                    "[RESOLVED-XCODE] %s manifest refresh failed: %s",
                    self.channel_id, result.get("error"),
                )
                return None
            with get_session() as session:
                row = session.query(Manifest.url).filter(Manifest.id == self.manifest_id).first()
            return row[0] if row else None
        except Exception as e:
            logging.warning("[RESOLVED-XCODE] %s manifest refresh error: %s",
                            self.channel_id, e)
            return None

    def _resolve_variant_url(self, url: str) -> str:
        """If `url` is a master playlist, pick the highest-bandwidth variant.
        If it's already a variant (no #EXT-X-STREAM-INF), return as-is."""
        try:
            resp = http_requests.get(url, headers=self._upstream_headers(), timeout=10)
            text = resp.text
        except Exception as e:
            logging.warning("[RESOLVED-XCODE] couldn't fetch master, using as-is: %s", e)
            return url
        if "#EXT-X-STREAM-INF" not in text:
            return url
        best_bw = -1
        best_uri = None
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF"):
                bw_match = re.search(r'BANDWIDTH=(\d+)', line)
                if bw_match and i + 1 < len(lines):
                    bw = int(bw_match.group(1))
                    uri = lines[i + 1].strip()
                    if bw > best_bw and uri and not uri.startswith("#"):
                        best_bw = bw
                        best_uri = uri
        if best_uri:
            return urljoin(url, best_uri)
        return url

    def _download_segment(self, seg: UpstreamSegment) -> str:
        """Download a segment to local disk via ffmpeg.

        Both AES-encrypted and plain segments are routed through ffmpeg with
        `-c copy -f mpegts`. This serves two purposes:

          1. AES-128 segments need decryption — ffmpeg handles the key fetch
             via a synthesized one-segment playlist with the EXT-X-KEY tag.
          2. Plain segments get a remux pass that normalizes their MPEG-TS
             container structure to ffmpeg's defaults (PMT pid 4096, single
             video + single audio stream, dropping any extras like alternate
             audio tracks or closed-caption streams). This makes WSPA's
             Lura-native segments look the same as ffmpeg-produced bump
             segments so a downstream long-running encoder can concatenate
             them cleanly without seeing them as separate streams.

        Returns the local file path. Caller is responsible for cleanup
        (the encoder loop deletes after encoding).
        """
        local_path = os.path.join(self._download_dir, f"seg_{seg.seq}.ts")

        ffmpeg_headers = []
        if self.source_domain:
            ffmpeg_headers = [
                "-headers",
                f"Referer: https://{self.source_domain}/\r\n"
                f"Origin: https://{self.source_domain}\r\n",
            ]

        if seg.key_method == "AES-128" and seg.key_uri:
            mini = (
                "#EXTM3U\n"
                "#EXT-X-VERSION:3\n"
                "#EXT-X-TARGETDURATION:11\n"
                f"#EXT-X-KEY:METHOD=AES-128,URI=\"{seg.key_uri}\""
                + (f",IV={seg.key_iv}" if seg.key_iv else "") + "\n"
                f"#EXTINF:{seg.duration:.3f},\n"
                f"{seg.uri}\n"
                "#EXT-X-ENDLIST\n"
            )
            mini_path = os.path.join(self._download_dir, f"seg_{seg.seq}.m3u8")
            with open(mini_path, "w") as f:
                f.write(mini)
            cmd = [
                "ffmpeg", "-y",
                "-loglevel", "error",
                "-allowed_extensions", "ALL",
                "-protocol_whitelist", "file,http,https,tcp,tls,crypto,data",
                "-i", mini_path,
                "-c", "copy",
                "-f", "mpegts",
                local_path,
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=30)
            try:
                os.remove(mini_path)
            except OSError:
                pass
            if result.returncode != 0:
                err = result.stderr.decode("utf-8", errors="replace")[-300:]
                raise RuntimeError(f"decrypt failed: {err}")
        else:
            cmd = [
                "ffmpeg", "-y",
                "-loglevel", "error",
                *ffmpeg_headers,
                "-protocol_whitelist", "file,http,https,tcp,tls,crypto,data",
                "-i", seg.uri,
                "-c", "copy",
                "-f", "mpegts",
                local_path,
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=30)
            if result.returncode != 0:
                err = result.stderr.decode("utf-8", errors="replace")[-300:]
                raise RuntimeError(f"download failed: {err}")
        return local_path

    def _enqueue_upstream(self, seg: UpstreamSegment, enqueue: Callable[[QueueItem], None]):
        try:
            local_path = self._download_segment(seg)
            enqueue(QueueItem(
                kind="upstream",
                source_path=local_path,
                duration=seg.duration,
                label=f"upstream:{seg.seq}",
            ))
        except Exception as e:
            logging.warning("[RESOLVED-XCODE] %s enqueue failed for seg %d: %s",
                            self.channel_id, seg.seq, e)

    def run(self, enqueue: Callable[[QueueItem], None], stop_event: threading.Event) -> None:
        """Poll the upstream variant playlist, classify segments via the
        profile, enqueue upstream segments or replacement bumps.

        Profile-agnostic — the SCTE-35 details for Adult Swim and the
        Anvato/Lura details for WSPA both flow through the same loop, just
        with different profile.parse() / profile.classify() behavior.
        """
        seen_seqs: set[int] = set()
        profile_state: dict = {}
        in_break: bool = False
        break_coverage_remaining: float = 0.0
        backfilled = False
        consecutive_403s = 0

        variant_url = self._resolve_variant_url(self.manifest_url)
        logging.info("[RESOLVED-XCODE] %s polling variant: %s",
                     self.channel_id, variant_url[:120])

        while not stop_event.is_set():
            try:
                resp = http_requests.get(variant_url, headers=self._upstream_headers(), timeout=10)
                if resp.status_code in (401, 403):
                    consecutive_403s += 1
                    if consecutive_403s > 3:
                        logging.error(
                            "[RESOLVED-XCODE] %s giving up after %d consecutive auth failures",
                            self.channel_id, consecutive_403s,
                        )
                        stop_event.set()
                        break
                    logging.warning(
                        "[RESOLVED-XCODE] %s variant HTTP %d (#%d) — refreshing manifest",
                        self.channel_id, resp.status_code, consecutive_403s,
                    )
                    fresh_master = self._refresh_manifest_url()
                    if fresh_master:
                        self.manifest_url = fresh_master
                        variant_url = self._resolve_variant_url(fresh_master)
                        logging.info("[RESOLVED-XCODE] %s new variant: %s",
                                     self.channel_id, variant_url[:120])
                        consecutive_403s = 0
                    else:
                        stop_event.wait(min(POLL_INTERVAL_SECONDS * 5, 30))
                    continue
                if resp.status_code != 200:
                    logging.warning("[RESOLVED-XCODE] %s playlist HTTP %d",
                                    self.channel_id, resp.status_code)
                    stop_event.wait(POLL_INTERVAL_SECONDS)
                    continue
                consecutive_403s = 0
                if self.profile is None:
                    if self.profile_name and self.profile_name != "auto":
                        self.profile = get_profile(self.profile_name)
                    else:
                        self.profile = detect_profile(resp.text)
                    logging.info("[RESOLVED-XCODE] %s using profile: %s",
                                 self.channel_id, self.profile.name)
                _, segments = self.profile.parse(resp.text, variant_url)
            except Exception as e:
                logging.warning("[RESOLVED-XCODE] %s playlist fetch failed: %s",
                                self.channel_id, e)
                stop_event.wait(POLL_INTERVAL_SECONDS)
                continue

            if not backfilled:
                for old_seg in segments[:-INITIAL_BACKFILL_SEGMENTS]:
                    seen_seqs.add(old_seg.seq)
                live_edge = (
                    segments[-INITIAL_BACKFILL_SEGMENTS:]
                    if len(segments) > INITIAL_BACKFILL_SEGMENTS
                    else segments
                )
                for s in live_edge:
                    cls, _ = self.profile.classify(s, profile_state)
                    if cls == CLASS_REPLACE:
                        in_break = True
                        logging.info(
                            "[RESOLVED-XCODE] %s joined mid-break", self.channel_id,
                        )
                        break
                    profile_state = {}
                backfilled = True

            for seg in segments:
                if seg.seq in seen_seqs:
                    continue
                seen_seqs.add(seg.seq)

                cls, pod_hint = self.profile.classify(seg, profile_state)

                if cls == CLASS_SHOW:
                    if in_break:
                        in_break = False
                        break_coverage_remaining = 0.0
                        logging.info("[RESOLVED-XCODE] %s break ended, master resumed",
                                     self.channel_id)
                    self._enqueue_upstream(seg, enqueue)
                    continue

                if not in_break:
                    in_break = True
                    logging.info("[RESOLVED-XCODE] %s break started (type=%s)",
                                 self.channel_id, seg.anvato_type or "scte35")

                if pod_hint and pod_hint > 0:
                    bump_seq = build_bump_sequence(
                        self.bump_paths, self.bump_durations, pod_hint,
                    )
                    if bump_seq:
                        cue_left = pod_hint
                        for bump_path, bump_dur in bump_seq:
                            enqueue(QueueItem(
                                kind="bump",
                                source_path=bump_path,
                                duration=bump_dur,
                                label=os.path.basename(bump_path),
                                cue_remaining_at_start=cue_left,
                            ))
                            cue_left -= bump_dur
                        break_coverage_remaining = pod_hint
                        logging.info(
                            "[RESOLVED-XCODE] %s queued %d bumps for %.1fs pod (coverage set)",
                            self.channel_id, len(bump_seq), pod_hint,
                        )
                    else:
                        logging.warning(
                            "[RESOLVED-XCODE] %s no bumps configured — break content will pass through",
                            self.channel_id,
                        )
                        self._enqueue_upstream(seg, enqueue)
                    continue

                if break_coverage_remaining > 0:
                    break_coverage_remaining = max(0.0, break_coverage_remaining - seg.duration)
                    continue

                bump_seq = build_bump_sequence(
                    self.bump_paths, self.bump_durations, seg.duration,
                )
                if bump_seq:
                    for bump_path, bump_dur in bump_seq:
                        enqueue(QueueItem(
                            kind="bump",
                            source_path=bump_path,
                            duration=bump_dur,
                            label=os.path.basename(bump_path),
                        ))
                else:
                    logging.warning(
                        "[RESOLVED-XCODE] %s no bumps configured — passing through",
                        self.channel_id,
                    )
                    self._enqueue_upstream(seg, enqueue)

            stop_event.wait(POLL_INTERVAL_SECONDS)
