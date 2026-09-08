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

ContinuousRelaySource is the second mechanism — for sources that aren't HLS
at all: a token-gated endpoint hands out a short-lived CDN URL serving one
continuous WebM (VP8/Opus) byte stream, no discrete segment URIs anywhere.
Python does all the network I/O (token refresh, the continuous read) and
self-segments at keyframe boundaries on-disk; ffmpeg only ever touches
finite already-downloaded chunks, same "gentle mirror" rule HlsPlaylistSource
and remux_stream.py both follow. No ad-break concept here — everything is
CLASS_SHOW, so there's no StreamProfile involved.
"""

import logging
import os
import random
import re
import subprocess
import threading
import time
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


# Per-domain config for ContinuousRelaySource — the class itself is generic
# ("hit a token endpoint, build a CDN URL, read continuously"); these are the
# only bits that differ per site. Keyed by Manifest.source_domain. A second
# site with the same token-API shape is a new entry here, not a new class.
#
# Site-specific values live in the optional gitignored
# scrapers/_relay_source_configs.py (same pattern as _native_resolvers.py in
# manifest_resolver.py) so no target hosts live in tracked code. Loaded once,
# cached; absent file just means no relay sources are configured.
_relay_configs_mod = False  # False = not yet looked up; None = absent; else module


def _relay_source_configs() -> dict:
    global _relay_configs_mod
    if _relay_configs_mod is False:
        _relay_configs_mod = None
        try:
            import importlib.util
            path = os.path.join(os.getenv("SCRAPERS_DIR", "/app/scrapers"),
                                "_relay_source_configs.py")
            if os.path.isfile(path):
                spec = importlib.util.spec_from_file_location("_relay_source_configs", path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                _relay_configs_mod = mod
        except Exception as e:
            logging.warning("[RELAY] site config load failed: %s", e)
    return getattr(_relay_configs_mod, "RELAY_SOURCE_CONFIGS", {}) if _relay_configs_mod else {}


def get_relay_source_config(source_domain: str) -> Optional[dict]:
    return _relay_source_configs().get(source_domain)


# WebM Cluster element ID (EBML) — marks the start of the first Cluster,
# i.e. the end of the "init" prefix (EBML header + Segment/Info/Tracks).
_WEBM_CLUSTER_ID = bytes.fromhex("1F43B675")


class ContinuousRelaySource(SegmentSource):
    """Token-gated continuous WebM relay — for sources with no HLS playlist
    at all.

    A decode endpoint hands out a short-lived (token, code) pair, which
    plugs into a CDN URL template serving one uninterrupted VP8/Opus WebM
    byte stream. The token expires well before any single HTTP connection
    would naturally end, so this reconnects with a fresh token on its own
    clock, comfortably inside that window.

    WebM isn't self-synchronizing the way MPEG-TS is — an arbitrary byte
    slice mid-stream isn't independently parseable, only the EBML
    header+Segment+Tracks prefix followed by a run of Cluster elements is.
    So the same init-prefix pattern remux_stream.py uses for fMP4/CMAF
    applies here: capture that prefix once per connection, prepend it to
    every chunk cut from the Cluster stream that follows.

    Config (decode-endpoint path, CDN URL template) is all field-driven —
    not hardcoded to any one site, so a second site with the same "token
    API -> continuous stream" shape is a config difference, not a new
    class. See get_relay_source_config() above for where per-site values
    live.
    """

    MIN_CHUNK_BYTES = 32_768       # don't even try cutting below this
    MAX_PEND_BYTES = 24_000_000    # force-cut safety cap if no keyframe turns up
    INIT_CAPTURE_CAP_BYTES = 262_144  # give up looking for the Cluster marker past this
    READ_CHUNK_BYTES = 65_536

    def __init__(self, *, channel_id: str, player_page_url: str,
                 decode_url_template: str, referer_template: str,
                 cdn_url_template: str, source_domain: str = "",
                 refresh_interval_seconds: float = 250.0,
                 chunk_target_seconds: float = 6.0,
                 download_dir: str = "/tmp"):
        self.channel_id = channel_id
        self.player_page_url = player_page_url
        self.decode_url_template = decode_url_template
        self.referer_template = referer_template
        self.cdn_url_template = cdn_url_template
        self.source_domain = source_domain
        self.refresh_interval_seconds = refresh_interval_seconds
        self.chunk_target_seconds = chunk_target_seconds
        self._download_dir = download_dir

        m = re.search(r'[?&]id=(\d+)', player_page_url)
        if not m:
            raise ValueError(f"couldn't parse a stream id out of {player_page_url!r}")
        self.stream_id = m.group(1)

    def _headers(self) -> dict:
        h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
        h["Referer"] = self.referer_template.format(stream_id=self.stream_id)
        if self.source_domain:
            h["Origin"] = f"https://{self.source_domain}"
        return h

    def _fetch_token(self) -> Optional[tuple]:
        """Hit the decode endpoint for a fresh (token, code) pair."""
        url = self.decode_url_template.format(stream_id=self.stream_id)
        try:
            resp = http_requests.get(url, headers=self._headers(), timeout=15)
            data = resp.json().get("parsed_data") or {}
        except Exception as e:
            logging.warning("[RELAY] %s decode endpoint failed: %s", self.channel_id, e)
            return None
        if data.get("status") != "OK" or not data.get("token") or not data.get("code"):
            logging.warning("[RELAY] %s decode endpoint returned no usable token: %s",
                            self.channel_id, data)
            return None
        return data["token"], data["code"]

    def _build_stream_url(self, token: str, code: str) -> str:
        return self.cdn_url_template.format(
            token=token, code=code, stream_id=self.stream_id,
            ts_ms=int(time.time() * 1000),
        )

    def _capture_init(self, chunks) -> Optional[tuple]:
        """Read from the response's chunk iterator until the first Cluster
        element shows up. Returns (init_bytes, leftover_bytes) — everything
        before the Cluster marker, and whatever was already read past it.
        None if the marker never turns up within INIT_CAPTURE_CAP_BYTES
        (malformed/unexpected response — caller should give up and retry)."""
        buf = bytearray()
        for chunk in chunks:
            buf.extend(chunk)
            idx = bytes(buf).find(_WEBM_CLUSTER_ID)
            if idx != -1:
                return bytes(buf[:idx]), bytes(buf[idx:])
            if len(buf) > self.INIT_CAPTURE_CAP_BYTES:
                return None
        return None

    def _find_cluster_cut(self, pending: bytes) -> Optional[int]:
        """Byte offset within `pending` of the last Cluster boundary, or
        None if pending doesn't contain at least two (nothing safe to cut
        yet — see below).

        Originally this cut at the exact video keyframe packet position
        (via ffprobe), not the Cluster boundary — WRONG: verified live that
        every Cluster here starts with a keyframe, ~26 bytes before the
        keyframe's own packet position (Cluster ID + size + Timecode
        elements). Cutting at the keyframe's byte offset instead of the
        Cluster's sliced a Cluster element in half: the emitted segment's
        trailing Cluster was left truncated (its declared size promising
        block data that got cut off), and the next segment's leading bytes
        were a bare block fragment with no enclosing Cluster at all —
        broken EBML framing at every single cut, which is what was causing
        the reported buffering/skipping. Cutting at the Cluster boundary
        itself sidesteps needing any ffprobe call here — pure byte-offset
        search, and correct by construction since Matroska/WebM elements
        are self-delimiting at Cluster granularity (unlike MPEG-TS, which
        is self-synchronizing at arbitrary packet offsets and doesn't have
        this problem).

        Cuts before the LAST marker found, never the last marker itself —
        that final Cluster may still be mid-download, so leaving it whole
        in `pending` for the next round is what guarantees `seg_bytes`
        below only ever contains complete Clusters."""
        markers = []
        idx = 0
        while True:
            idx = pending.find(_WEBM_CLUSTER_ID, idx)
            if idx == -1:
                break
            markers.append(idx)
            idx += 1
        if len(markers) < 2:
            return None
        return markers[-1]

    def _probe_source_duration(self, blob: bytes) -> float:
        """Duration of a raw WebM blob per its own Cluster timecodes —
        cheap (container metadata, no decode) and used to keep
        `_transcode_chunk`'s output_ts_offset accurate against the
        source's real timing rather than our chunk_target_seconds
        estimate, which would drift over a long-running connection."""
        probe_path = os.path.join(self._download_dir, f"relay-durprobe-{self.channel_id}.webm")
        try:
            with open(probe_path, "wb") as f:
                f.write(blob)
            r = subprocess.run(
                ["ffprobe", "-v", "error", "-f", "webm",
                 "-show_entries", "format=duration", "-of",
                 "default=noprint_wrappers=1:nokey=1", probe_path],
                capture_output=True, text=True, timeout=10)
            return float(r.stdout.strip())
        except Exception:
            return self.chunk_target_seconds
        finally:
            try:
                os.remove(probe_path)
            except OSError:
                pass

    def _transcode_chunk(self, blob: bytes, seg_index: int, ts_offset: float) -> Optional[str]:
        """VP8/Opus -> H.264/AAC in an MPEG-TS container so this chunk can
        feed into the same shared feeder/encoder every other QueueItem does.
        Quality doesn't matter for "single"/"multi"-mode channels (the
        feeder re-encodes again to TARGET_* anyway) but for "copy" mode
        this IS the final encode — the feeder just repackages it untouched.

        Each invocation is a separate ffmpeg process demuxing its own WebM
        blob, so PTS/DTS naturally restart near zero every time regardless
        of the source's real (ever-increasing) Cluster timecodes — fine
        for "single"/"multi" mode, whose second re-encode pass regenerates
        clean continuous timestamps from scratch anyway, but fatal for
        "copy" mode: concatenating chunks whose timestamps all restart at
        ~0 produces "non-monotonically increasing dts" at every boundary,
        since -c copy has no re-encode pass to paper over it.
        `-output_ts_offset` fixes this at the source — each chunk's
        written timestamps are shifted to continue exactly where the
        previous one left off, using the accumulated real source duration
        (see run()'s cumulative_offset), not a guessed constant."""
        in_path = os.path.join(self._download_dir, f"relay-in-{self.channel_id}-{seg_index}.webm")
        out_path = os.path.join(self._download_dir, f"relay-{self.channel_id}-{seg_index}.ts")
        try:
            with open(in_path, "wb") as f:
                f.write(blob)
            cmd = [
                "ffmpeg", "-y", "-loglevel", "error",
                "-f", "webm", "-i", in_path,
                "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
                "-bf", "0", "-x264-params", "threads=2",
                "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-ar", "48000", "-ac", "2", "-async", "1",
                "-output_ts_offset", f"{ts_offset:.3f}",
                "-f", "mpegts", out_path,
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=30)
            if result.returncode != 0:
                err = result.stderr.decode("utf-8", errors="replace")[-300:]
                logging.warning("[RELAY] %s chunk %d transcode failed: %s",
                                self.channel_id, seg_index, err)
                return None
            return out_path
        finally:
            try:
                os.remove(in_path)
            except OSError:
                pass

    def run(self, enqueue: Callable[[QueueItem], None], stop_event: threading.Event) -> None:
        """Reading (network-bound) and transcoding (CPU-bound) used to run
        strictly sequentially in this loop — reading was fully blocked for
        the whole ~0.6-1x-realtime duration of every transcode. That dead
        time meant this source's actual production cadence was slower and
        much burstier than its declared per-chunk durations, which is what
        surfaced as periodic buffering even after the transcode itself got
        fast enough to individually keep up with realtime. Transcoding one
        chunk now happens on a background thread while reading immediately
        continues accumulating the next one — a single-worker pool so
        completions (and therefore enqueue() calls) stay strictly in
        submission order without needing extra bookkeeping."""
        import concurrent.futures
        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=f"relay-xcode-{self.channel_id}")
        # Caps how far reading can outrun transcoding — steady state should
        # rarely touch this (transcode is faster than realtime, reading is
        # network-paced), it's just a backstop against unbounded memory
        # growth if the CDN ever bursts well ahead of real-time.
        backpressure = threading.Semaphore(3)

        def _on_transcoded(idx, fut):
            backpressure.release()
            try:
                out_path = fut.result()
            except Exception as e:
                logging.warning("[RELAY] %s chunk %d transcode raised: %s",
                                self.channel_id, idx, e)
                return
            if not out_path:
                return
            from core.channels import ffprobe_duration
            duration = ffprobe_duration(out_path)
            enqueue(QueueItem(
                kind="upstream",
                source_path=out_path,
                duration=duration or self.chunk_target_seconds,
                label=f"relay:{idx}",
            ))

        try:
            self._run_connections(stop_event, pool, _on_transcoded, backpressure)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def _run_connections(self, stop_event, pool, on_transcoded, backpressure):
        seg_index = 0
        # Persists across reconnects (not per-connection) — the output HLS
        # timeline must stay continuous across a token-refresh reconnect,
        # same as it already does content-wise.
        cumulative_offset = 0.0

        while not stop_event.is_set():
            pair = self._fetch_token()
            if not pair:
                stop_event.wait(5.0)
                continue
            token, code = pair
            stream_url = self._build_stream_url(token, code)

            try:
                resp = http_requests.get(stream_url, headers=self._headers(),
                                         timeout=(10, self.refresh_interval_seconds + 30),
                                         stream=True)
                resp.raise_for_status()
            except Exception as e:
                logging.warning("[RELAY] %s couldn't open stream: %s", self.channel_id, e)
                stop_event.wait(5.0)
                continue

            logging.info("[RELAY] %s connected: %s", self.channel_id, stream_url.split("?")[0])

            try:
                chunks = resp.iter_content(chunk_size=self.READ_CHUNK_BYTES)
                captured = self._capture_init(chunks)
                if not captured:
                    logging.warning("[RELAY] %s never found a Cluster in the response — "
                                    "reconnecting", self.channel_id)
                    continue
                init_bytes, pending = captured
                pending = bytearray(pending)
                deadline = time.time() + self.refresh_interval_seconds
                last_cut = time.time()

                while not stop_event.is_set() and time.time() < deadline:
                    try:
                        chunk = next(chunks)
                    except StopIteration:
                        logging.info("[RELAY] %s connection ended early", self.channel_id)
                        break
                    except Exception as e:
                        logging.warning("[RELAY] %s read error: %s", self.channel_id, e)
                        break
                    pending.extend(chunk)

                    due = (time.time() - last_cut) >= self.chunk_target_seconds
                    forced = len(pending) > self.MAX_PEND_BYTES
                    if not ((due and len(pending) > self.MIN_CHUNK_BYTES) or forced):
                        continue

                    cut_in_pending = self._find_cluster_cut(bytes(pending))
                    if cut_in_pending is None:
                        if forced:
                            logging.warning(
                                "[RELAY] %s fewer than 2 Cluster boundaries in %d "
                                "buffered bytes — force-cutting (will likely glitch)",
                                self.channel_id, len(pending),
                            )
                            cut_in_pending = len(pending)
                        else:
                            continue

                    seg_bytes = bytes(pending[:cut_in_pending])
                    pending = pending[cut_in_pending:]
                    idx = seg_index
                    seg_index += 1
                    blob = init_bytes + seg_bytes
                    ts_offset = cumulative_offset
                    cumulative_offset += self._probe_source_duration(blob)
                    backpressure.acquire()
                    future = pool.submit(self._transcode_chunk, blob, idx, ts_offset)
                    future.add_done_callback(
                        lambda fut, idx=idx: on_transcoded(idx, fut))
                    last_cut = time.time()

                if pending:
                    logging.debug("[RELAY] %s discarding %d trailing bytes at reconnect",
                                 self.channel_id, len(pending))
            finally:
                resp.close()
