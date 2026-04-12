"""Transcode-mediated streaming for resolved channels.

When a resolved channel has `transcode_mediated=True`, instead of proxying the
upstream HLS bytes through unchanged, we run a full transcode pipeline like
scheduled channels do. The orchestrator polls the upstream variant playlist,
classifies each segment as show or break using a per-channel profile (Adult
Swim's SCTE-35 dialect, Anvato/Lura's type-tagged segments, etc.), and feeds
them through a long-running FFmpeg encoder. Break segments get replaced with
bump files (configurable per channel).

The output is a single coherent HLS stream with one consistent codec profile,
no encryption-method changes, no discontinuities, and no CDN-path mismatches —
solving the four root causes that make Adult Swim and similar SCTE-35 sources
choke Jellyfin's player.

Architecture (mirrors core/streamer.py's ChannelStream):
  [poller] ─────→ [download queue] ─────┐
  [bump fitter] ──→                      ├─→ [encoder per file] → [HLS pipe] → [segmenter]
                                         │                                            ↓
                                                                              /hls/{ch}/stream.m3u8

Each item the encoder processes is a regular file (a downloaded+decrypted
upstream segment, or a bump file). The encoder normalizes everything to
the same MPEG-TS params, so the segmenter sees one continuous bitstream.
"""

import logging
import os
import queue
import random
import re
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests as http_requests

from core.resolver.profiles import (
    UpstreamSegment,
    StreamProfile,
    get_profile,
    detect_profile,
    CLASS_SHOW,
    CLASS_REPLACE,
)


# ── Encoder target params ───────────────────────────────────────────────────
# These are the unified output params. Every input source — upstream segments
# AND bump files — gets re-encoded to these so the HLS segmenter sees one
# coherent stream with no codec/profile/encryption mismatches.

TARGET_WIDTH = 1280
TARGET_HEIGHT = 720
TARGET_FPS = 30
TARGET_VIDEO_PROFILE = "main"
TARGET_AUDIO_BITRATE = "192k"
TARGET_AUDIO_RATE = 48000
TARGET_AUDIO_CHANNELS = 2

# Polling cadence for the upstream playlist watcher
POLL_INTERVAL_SECONDS = 2.0

# How far back to look in the playlist on first poll (segments before this
# already played and shouldn't be enqueued).
INITIAL_BACKFILL_SEGMENTS = 3

# When a cue ends, treat bare discontinuities within this window as bumpers
# (Adult Swim convention — see project memory).
BUMPER_WINDOW_SECONDS = 60


# ── Splice plan items ───────────────────────────────────────────────────────

@dataclass
class QueueItem:
    """Something the encoder loop should encode and pipe through."""
    kind: str            # "upstream" | "bump"
    source_path: str     # local file path
    duration: float      # seconds; for bumps may be shorter than file's actual length
    label: str           # for logging
    cue_remaining_at_start: Optional[float] = None  # for bumps: total seconds left in cue at this bump's start


# ── Bump fitting ────────────────────────────────────────────────────────────

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


# ── Resolved channel stream ─────────────────────────────────────────────────

class ResolvedChannelStream:
    """Transcode-mediated streamer for one resolved channel.

    Mirrors the lifecycle of core.streamer.ChannelStream but pulls items from
    a dynamic queue that's populated by the upstream playlist poller.
    """

    def __init__(
        self,
        channel_id: str,
        manifest_id: str,
        manifest_url: str,
        bump_paths: list,
        bump_durations: dict,
        hls_dir: str,
        *,
        channel_name: str = "",
        logo_path: str = "",
        show_next: bool = False,
        profile_name: str = "auto",
        branding_logo_path: str = "",
        hls_time: int = 6,
        hls_list_size: int = 10,
        loglevel: str = "warning",
        video_preset: str = "veryfast",
        crf: str = "",
        ffmpeg_threads: str = "1",
        x264_threads: str = "4",
        audio_bitrate: str = TARGET_AUDIO_BITRATE,
    ):
        self.channel_id = channel_id
        self.manifest_id = manifest_id
        self.manifest_url = manifest_url
        self.bump_paths = list(bump_paths or [])
        self.bump_durations = dict(bump_durations or {})
        self.hls_dir = hls_dir
        self.channel_name = channel_name or "Live"
        self.logo_path = logo_path
        self.show_next = show_next
        self.profile_name = profile_name
        self.branding_logo_path = branding_logo_path
        self.profile: Optional[StreamProfile] = None  # resolved on first poll
        self.hls_time = hls_time
        self.hls_list_size = hls_list_size
        self.loglevel = loglevel
        self.video_preset = video_preset
        self.crf = crf
        self.ffmpeg_threads = ffmpeg_threads
        self.x264_threads = x264_threads
        self.audio_bitrate = audio_bitrate

        self._enc_proc: Optional[subprocess.Popen] = None
        self._holding_proc: Optional[subprocess.Popen] = None
        self._poller_thread: Optional[threading.Thread] = None
        self._feeder_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._segment_queue: queue.Queue[QueueItem] = queue.Queue(maxsize=200)
        self._download_dir = tempfile.mkdtemp(prefix=f"channelarr-res-{channel_id}-")
        self._started_at: Optional[float] = None
        self._last_access = time.time()

    # ── Public lifecycle ────────────────────────────────────────────────────

    def touch(self):
        self._last_access = time.time()

    @property
    def last_access(self) -> float:
        return self._last_access

    def start_holding(self):
        """Start only the holding pattern — no poller, no encoder. Used to
        pre-warm the channel on container startup so clients get instant
        video. Call upgrade_to_live() when a client actually requests it."""
        os.makedirs(self.hls_dir, exist_ok=True)
        for f in os.listdir(self.hls_dir):
            if f.endswith(".ts") or f.endswith(".m3u8"):
                try:
                    os.remove(os.path.join(self.hls_dir, f))
                except OSError:
                    pass
        self._stop_event.clear()
        self._started_at = time.time()
        self._last_access = time.time()
        self._start_holding_pattern()

    def upgrade_to_live(self):
        """Start the real poller+feeder pipeline. The feeder will kill the
        holding pattern when the first real segment is ready."""
        if self._feeder_thread and self._feeder_thread.is_alive():
            return
        self._poller_thread = threading.Thread(
            target=self._poller_loop, daemon=True, name=f"resolved-poller-{self.channel_id}",
        )
        self._feeder_thread = threading.Thread(
            target=self._feeder_loop, daemon=True, name=f"resolved-feeder-{self.channel_id}",
        )
        self._poller_thread.start()
        self._feeder_thread.start()
        logging.info("[RESOLVED-XCODE] Started channel %s (manifest=%s)",
                     self.channel_id, self.manifest_id)

    def start(self):
        """Full start — holding pattern + poller + feeder."""
        self.start_holding()
        self.upgrade_to_live()

    def status(self) -> dict:
        feeder_alive = (
            self._feeder_thread is not None
            and self._feeder_thread.is_alive()
        )
        holding_alive = self._holding_proc is not None and self._holding_proc.poll() is None
        alive = feeder_alive or holding_alive
        uptime = 0
        if self._started_at and alive:
            uptime = int(time.time() - self._started_at)
        return {
            "running": alive,
            "uptime": uptime,
            "holding": holding_alive and not feeder_alive,
            "now_playing": "Live (transcode-mediated)" if alive else "",
        }

    def stop(self):
        self._stop_event.set()
        if self._holding_proc:
            try:
                self._holding_proc.kill()
            except Exception:
                pass
            self._holding_proc = None
        if self._enc_proc:
            try:
                self._enc_proc.terminate()
                self._enc_proc.wait(timeout=5)
            except Exception:
                try:
                    self._enc_proc.kill()
                except Exception:
                    pass
        self._enc_proc = None
        self._clean_hls_dir()
        try:
            shutil.rmtree(self._download_dir, ignore_errors=True)
        except Exception:
            pass
        logging.info("[RESOLVED-XCODE] Stopped channel %s", self.channel_id)

    def _clean_hls_dir(self):
        if not os.path.isdir(self.hls_dir):
            return
        for f in os.listdir(self.hls_dir):
            if f.endswith(".ts") or f.endswith(".m3u8"):
                try:
                    os.remove(os.path.join(self.hls_dir, f))
                except OSError:
                    pass

    # ── Holding pattern ─────────────────────────────────────────────────────

    def _start_holding_pattern(self):
        """Start a lightweight ffmpeg that loops a cached bump into the HLS
        directory. Produces segments instantly (-c copy, no encoding) so the
        client gets video within ~1 second of requesting the stream. The real
        encoder kills this process when it's ready to take over."""
        cache_files = [
            p + ".cache.ts" for p in self.bump_paths
            if os.path.isfile(p + ".cache.ts")
        ]
        if not cache_files:
            return
        hold_src = random.choice(cache_files)
        playlist = os.path.join(self.hls_dir, "stream.m3u8")
        segment_pattern = os.path.join(self.hls_dir, "seg_%05d.ts")
        use_wm = self.branding_logo_path and os.path.isfile(self.branding_logo_path)
        cmd = [
            "ffmpeg", "-y",
            "-loglevel", "error",
            "-stream_loop", "-1",
            "-re",
            "-i", hold_src,
        ]
        if use_wm:
            cmd.extend(["-loop", "1", "-i", self.branding_logo_path])
            vf = (
                f"[1:v]scale=80:-1,format=rgba,colorchannelmixer=aa=0.6[wm];"
                f"[0:v]null[main];"
                f"[main][wm]overlay=W-w-20:H-h-20:shortest=1"
            )
            cmd.extend([
                "-filter_complex", vf,
                "-map", "0:a:0?",
                "-c:v", "libx264", "-preset", "ultrafast",
                "-c:a", "aac",
            ])
        else:
            cmd.extend(["-c", "copy"])
        cmd.extend([
            "-f", "hls",
            "-hls_time", str(self.hls_time),
            "-hls_list_size", "3",
            "-hls_flags", "delete_segments+omit_endlist",
            "-hls_segment_filename", segment_pattern,
            playlist,
        ])
        self._holding_proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        logging.info("[RESOLVED-XCODE] %s holding pattern started (%s)",
                     self.channel_id, os.path.basename(hold_src))

    def _stop_holding_pattern(self):
        """Kill the holding pattern ffmpeg. Called by the feeder loop just
        before the real encoder starts writing. Leaves the HLS files on disk
        so clients can keep playing them until the real encoder overwrites."""
        if not self._holding_proc:
            return
        try:
            self._holding_proc.terminate()
            self._holding_proc.wait(timeout=3)
        except Exception:
            try:
                self._holding_proc.kill()
            except Exception:
                pass
        self._holding_proc = None
        logging.info("[RESOLVED-XCODE] %s holding pattern stopped, real encoder taking over",
                     self.channel_id)

    # ── Playlist poller ────────────────────────────────────────────────────

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

    def _poller_loop(self):
        """Poll the upstream variant playlist, classify segments via the
        profile, enqueue upstream segments or replacement bumps.

        Profile-agnostic — the SCTE-35 details for Adult Swim and the
        Anvato/Lura details for WSPA both flow through the same loop, just
        with different profile.parse() / profile.classify() behavior.
        """
        seen_seqs: set[int] = set()
        profile_state: dict = {}      # opaque per-profile classifier state
        in_break: bool = False        # tracks whether we're currently replacing
        # When a pod_hint queues a perfect-fit bump sequence, this tracks how
        # many seconds of that pod we've already covered. Subsequent in-break
        # segments arriving in the playlist decrement this; while it's > 0
        # they're SKIPPED (not queued as additional continuous bumps).
        # Prevents the "extra no-overlay bumps tail" pattern after pod_hint
        # bumps finish playing.
        break_coverage_remaining: float = 0.0
        backfilled = False
        consecutive_403s = 0

        variant_url = self._resolve_variant_url(self.manifest_url)
        logging.info("[RESOLVED-XCODE] %s polling variant: %s",
                     self.channel_id, variant_url[:120])

        while not self._stop_event.is_set():
            try:
                resp = http_requests.get(variant_url, timeout=10)
                if resp.status_code in (401, 403):
                    consecutive_403s += 1
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
                        # Backoff to avoid hammering selenium
                        self._stop_event.wait(min(POLL_INTERVAL_SECONDS * 5, 30))
                    continue
                if resp.status_code != 200:
                    logging.warning("[RESOLVED-XCODE] %s playlist HTTP %d",
                                    self.channel_id, resp.status_code)
                    self._stop_event.wait(POLL_INTERVAL_SECONDS)
                    continue
                consecutive_403s = 0
                # Resolve the profile on first poll if set to "auto"
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
                self._stop_event.wait(POLL_INTERVAL_SECONDS)
                continue

            # On first poll, mark every old segment as already-seen so we
            # only process segments at the live edge. Prevents replaying
            # the whole rolling window (which often includes historical
            # ad breaks).
            if not backfilled:
                for old_seg in segments[:-INITIAL_BACKFILL_SEGMENTS]:
                    seen_seqs.add(old_seg.seq)
                # If we joined mid-break, set in_break so the first
                # live-edge segment of the break gets replaced.
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
                    # Reset profile state to avoid double-counting on the
                    # real loop below
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
                    self._enqueue_upstream(seg)
                    continue

                # CLASS_REPLACE — break content, queue a bump (or skip if covered)
                if not in_break:
                    in_break = True
                    logging.info("[RESOLVED-XCODE] %s break started (type=%s)",
                                 self.channel_id, seg.anvato_type or "scte35")

                # When the profile reports a pod_hint (Lura's pod-duration at
                # ad-index=0, or Adult Swim's CUE-OUT duration), build a full
                # perfect-fit bump sequence right now and mark coverage.
                if pod_hint and pod_hint > 0:
                    bump_seq = build_bump_sequence(
                        self.bump_paths, self.bump_durations, pod_hint,
                    )
                    if bump_seq:
                        cue_left = pod_hint
                        for bump_path, bump_dur in bump_seq:
                            self._segment_queue.put(QueueItem(
                                kind="bump",
                                source_path=bump_path,
                                duration=bump_dur,
                                label=os.path.basename(bump_path),
                                cue_remaining_at_start=cue_left,
                            ))
                            cue_left -= bump_dur
                        # Mark coverage so subsequent in-break segments (the
                        # rest of the pod) get SKIPPED instead of queueing
                        # additional no-overlay continuous bumps on top.
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
                        self._enqueue_upstream(seg)
                    continue

                # Already covered by an earlier pod_hint sequence — skip this
                # segment entirely (the perfect-fit bumps already queued cover
                # the time this segment would occupy in the encoder).
                if break_coverage_remaining > 0:
                    break_coverage_remaining = max(0.0, break_coverage_remaining - seg.duration)
                    continue

                # Continuous mode: queue one bump per replaced segment. Used
                # when there's no upfront pod duration (e.g. Lura SLATE before
                # the ad pod starts, or joining mid-cue without continuation
                # markers).
                bump_seq = build_bump_sequence(
                    self.bump_paths, self.bump_durations, seg.duration,
                )
                if bump_seq:
                    for bump_path, bump_dur in bump_seq:
                        self._segment_queue.put(QueueItem(
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
                    self._enqueue_upstream(seg)

            self._stop_event.wait(POLL_INTERVAL_SECONDS)

    def _resolve_variant_url(self, url: str) -> str:
        """If `url` is a master playlist, pick the highest-bandwidth variant.
        If it's already a variant (no #EXT-X-STREAM-INF), return as-is."""
        try:
            resp = http_requests.get(url, timeout=10)
            text = resp.text
        except Exception as e:
            logging.warning("[RESOLVED-XCODE] couldn't fetch master, using as-is: %s", e)
            return url
        if "#EXT-X-STREAM-INF" not in text:
            return url
        # Parse master, pick highest bandwidth
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

    def _enqueue_upstream(self, seg: UpstreamSegment):
        """Download (and decrypt) one upstream segment, queue it for the encoder."""
        try:
            local_path = self._download_segment(seg)
            self._segment_queue.put(QueueItem(
                kind="upstream",
                source_path=local_path,
                duration=seg.duration,
                label=f"upstream:{seg.seq}",
            ))
        except Exception as e:
            logging.warning("[RESOLVED-XCODE] %s enqueue failed for seg %d: %s",
                            self.channel_id, seg.seq, e)

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

        if seg.key_method == "AES-128" and seg.key_uri:
            # Build a tiny one-segment playlist so ffmpeg's HLS demuxer
            # picks up the key URI and decrypts the segment.
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
            # Plain segment — fetch + remux through ffmpeg to normalize the
            # mpegts container. The video/audio bytes themselves are copied
            # (no re-encode), only the container metadata is rewritten.
            cmd = [
                "ffmpeg", "-y",
                "-loglevel", "error",
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

    # ── Feeder loop ─────────────────────────────────────────────────────────
    # ONE long-running encoder reads MPEG-TS from stdin, decodes, re-encodes
    # to target params, and segments to HLS. The feeder thread drains the
    # segment queue and writes source bytes into the encoder's stdin.

    def _feeder_loop(self):
        try:
            self._feeder_loop_inner()
        finally:
            self._clean_hls_dir()

    def _feeder_loop_inner(self):
        # Wait for the first item before starting the real encoder.
        # The holding pattern serves video to clients in the meantime.
        first_item = None
        while not self._stop_event.is_set():
            try:
                first_item = self._segment_queue.get(timeout=5)
                break
            except queue.Empty:
                continue
        if self._stop_event.is_set() or not first_item:
            self._stop_holding_pattern()
            return

        self._stop_holding_pattern()
        enc_cmd = self._build_combined_encoder_cmd()
        logging.info("[RESOLVED-XCODE] %s combined encoder: %s",
                     self.channel_id, " ".join(enc_cmd))
        self._enc_proc = subprocess.Popen(
            enc_cmd, stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )

        def _drain_stderr():
            try:
                while True:
                    line = self._enc_proc.stderr.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").rstrip()
                    if text and ("error" in text.lower() or "fatal" in text.lower()):
                        logging.warning("[RESOLVED-XCODE] %s encoder: %s",
                                        self.channel_id, text[-200:])
            except Exception:
                pass

        threading.Thread(target=_drain_stderr, daemon=True,
                          name=f"resolved-encoder-stderr-{self.channel_id}").start()

        # Re-queue the first item that triggered the handoff
        self._segment_queue.put(first_item)

        try:
            while not self._stop_event.is_set():
                try:
                    item = self._segment_queue.get(timeout=10)
                except queue.Empty:
                    if self._enc_proc.poll() is not None:
                        logging.error("[RESOLVED-XCODE] %s encoder died unexpectedly",
                                      self.channel_id)
                        break
                    continue

                logging.info("[RESOLVED-XCODE] %s feed: %s (%s, %.1fs)",
                             self.channel_id, item.label, item.kind, item.duration)

                try:
                    if item.kind == "upstream":
                        self._feed_upstream_file(item)
                    elif item.kind == "bump":
                        cache_path = item.source_path + ".cache.ts"
                        has_cache = os.path.isfile(cache_path)
                        needs_overlay = self.show_next
                        if has_cache and not needs_overlay:
                            self._feed_cached_bump(item, cache_path)
                        elif has_cache and needs_overlay:
                            self._feed_bump_file(item, cache_path=cache_path)
                        else:
                            self._feed_bump_file(item)
                except (BrokenPipeError, OSError) as e:
                    logging.error("[RESOLVED-XCODE] %s encoder pipe broke: %s",
                                  self.channel_id, e)
                    self._stop_event.set()
                    break
                except Exception as e:
                    logging.warning("[RESOLVED-XCODE] %s feed failed for %s: %s",
                                    self.channel_id, item.label, e)
                finally:
                    if item.kind == "upstream":
                        try:
                            os.remove(item.source_path)
                        except OSError:
                            pass
        finally:
            if self._enc_proc and self._enc_proc.stdin:
                try:
                    self._enc_proc.stdin.close()
                except Exception:
                    pass
            if self._enc_proc:
                try:
                    self._enc_proc.wait(timeout=5)
                except Exception:
                    try:
                        self._enc_proc.kill()
                    except Exception:
                        pass
                self._enc_proc = None

    def _feed_upstream_file(self, item: QueueItem):
        """Copy bytes from a downloaded upstream segment file directly into
        the encoder's stdin."""
        with open(item.source_path, "rb") as f:
            while not self._stop_event.is_set():
                chunk = f.read(65536)
                if not chunk:
                    break
                self._enc_proc.stdin.write(chunk)
        try:
            self._enc_proc.stdin.flush()
        except (BrokenPipeError, OSError):
            raise

    def _feed_cached_bump(self, item: QueueItem, cache_path: str):
        """Feed a pre-encoded cached bump into the encoder's stdin via a
        lightweight copy-trim ffmpeg. No decode/encode — just copies TS
        bytes with accurate time trimming for truncated bumps."""
        cmd = [
            "ffmpeg", "-y",
            "-loglevel", "error",
            "-re",
            "-i", cache_path,
            "-c", "copy",
        ]
        if item.duration > 0:
            cmd.extend(["-t", f"{item.duration:.3f}"])
        cmd.extend(["-f", "mpegts", "pipe:1"])
        sub = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            while not self._stop_event.is_set():
                chunk = sub.stdout.read(65536)
                if not chunk:
                    break
                self._enc_proc.stdin.write(chunk)
        finally:
            try:
                sub.stdout.close()
            except Exception:
                pass
            try:
                sub.wait(timeout=5)
            except Exception:
                try:
                    sub.kill()
                except Exception:
                    pass
            try:
                sub.stderr.close()
            except Exception:
                pass

    def _feed_bump_file(self, item: QueueItem, cache_path: str = None):
        """Run a per-bump ffmpeg that decodes the bump, applies overlay
        filters, and outputs MPEG-TS bytes into the main encoder's stdin."""
        sub_cmd = self._build_bump_subffmpeg_cmd(item, cache_path=cache_path)
        sub = subprocess.Popen(
            sub_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        try:
            while not self._stop_event.is_set():
                chunk = sub.stdout.read(65536)
                if not chunk:
                    break
                self._enc_proc.stdin.write(chunk)
        finally:
            try:
                sub.stdout.close()
            except Exception:
                pass
            try:
                sub.wait(timeout=5)
            except Exception:
                try:
                    sub.kill()
                except Exception:
                    pass
            if sub.returncode and sub.returncode != 0 and not self._stop_event.is_set():
                try:
                    err = sub.stderr.read().decode("utf-8", errors="replace")[-200:]
                    logging.warning("[RESOLVED-XCODE] %s bump rc=%d for %s: %s",
                                    self.channel_id, sub.returncode, item.label, err)
                except Exception:
                    pass
            try:
                sub.stderr.close()
            except Exception:
                pass

    def _build_combined_encoder_cmd(self) -> list:
        playlist = os.path.join(self.hls_dir, "stream.m3u8")
        segment_pattern = os.path.join(self.hls_dir, "seg_%05d.ts")
        base_vf = (
            f"scale=w={TARGET_WIDTH}:h={TARGET_HEIGHT}:force_original_aspect_ratio=decrease,"
            f"pad={TARGET_WIDTH}:{TARGET_HEIGHT}:(ow-iw)/2:(oh-ih)/2,setsar=1"
        )
        use_watermark = (
            self.branding_logo_path
            and os.path.isfile(self.branding_logo_path)
        )
        cmd = [
            "ffmpeg", "-y",
            "-threads", self.ffmpeg_threads,
            "-loglevel", self.loglevel,
            "-fflags", "+genpts+discardcorrupt",
            "-f", "mpegts",
            "-i", "pipe:0",
        ]
        if use_watermark:
            cmd.extend(["-loop", "1", "-i", self.branding_logo_path])
            wm_filter = (
                f"[1:v]scale=80:-1,format=rgba,colorchannelmixer=aa=0.6[wm];"
                f"[0:v]{base_vf}[main];"
                f"[main][wm]overlay=W-w-20:H-h-20:shortest=1"
            )
            cmd.extend([
                "-filter_complex", wm_filter,
                "-map", "0:a:0?",
            ])
        else:
            cmd.extend([
                "-map", "0:v:0?", "-map", "0:a:0?",
                "-vf", base_vf,
            ])
        cmd.extend([
            "-r", str(TARGET_FPS),
            "-c:v", "libx264",
            "-x264-params", f"threads={self.x264_threads}",
            "-preset", self.video_preset,
            "-profile:v", TARGET_VIDEO_PROFILE,
            "-pix_fmt", "yuv420p",
            "-force_key_frames", f"expr:gte(t,n_forced*{self.hls_time})",
            "-c:a", "aac",
            "-b:a", self.audio_bitrate,
            "-ar", str(TARGET_AUDIO_RATE),
            "-ac", str(TARGET_AUDIO_CHANNELS),
            "-async", "1",
            "-f", "hls",
            "-hls_time", str(self.hls_time),
            "-hls_list_size", str(self.hls_list_size),
            "-hls_flags", "delete_segments+omit_endlist",
            "-hls_segment_filename", segment_pattern,
            playlist,
        ])
        if self.crf:
            idx = cmd.index("-profile:v")
            cmd.insert(idx, self.crf)
            cmd.insert(idx, "-crf")
        return cmd

    @staticmethod
    def _wrap_title(text: str, max_chars: int = 28) -> list:
        """Word-wrap a title to fit a fixed-width overlay box."""
        words = text.split()
        lines = []
        current = ""
        for word in words:
            if current and len(current) + 1 + len(word) > max_chars:
                lines.append(current)
                current = word
            else:
                current = f"{current} {word}" if current else word
        if current:
            lines.append(current)
        return lines

    def _build_overlay_vf(self, item: QueueItem, base_vf: str) -> str:
        """Build the video filter chain with overlay drawtext for a bump.

        Mirrors the scheduled-channel overlay (RESUMING IN H:MM countdown +
        UP NEXT box) but the countdown counts down toward the end of the
        WHOLE cue, not just the current bump. So a 6-bump 180s cue shows
        '3:00' on the first bump, '2:30' on the second, etc., reaching
        '0:00' exactly when the upstream show resumes.
        """
        if item.kind != "bump" or not self.show_next:
            return base_vf

        font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        font_bold = font

        # Countdown is only shown when we know the full break duration.
        # Pod-hint bumps have cue_remaining_at_start; continuous-mode bumps
        # don't, so they get the UP NEXT overlay without the timer.
        if item.cue_remaining_at_start and item.cue_remaining_at_start > 0:
            cue_left = f"{item.cue_remaining_at_start:.2f}"
            countdown = (
                f"drawbox=x=(w-400)/2:y=h-80:w=400:h=50:color=black@0.6:t=fill,"
                f"drawtext=fontfile={font}:"
                f"text='RESUMING IN "
                f"%{{eif\\:trunc(max(0\\,{cue_left}-t)/60)\\:d}}"
                f"\\:"
                f"%{{eif\\:mod(max(0\\,{cue_left}-t)\\,60)\\:d\\:2}}':"
                f"fontsize=28:fontcolor=white@0.9:"
                f"x=(w-text_w)/2:y=h-70"
            )
        else:
            countdown = (
                f"drawbox=x=(w-400)/2:y=h-80:w=400:h=50:color=black@0.6:t=fill,"
                f"drawtext=fontfile={font}:"
                f"text='RETURNING SHORTLY':"
                f"fontsize=28:fontcolor=white@0.9:"
                f"x=(w-text_w)/2:y=h-70"
            )

        # UP NEXT box: title is the channel name; poster is the channel logo
        # if one exists on disk.
        next_title = self.channel_name
        use_poster = bool(self.logo_path and os.path.isfile(self.logo_path))
        safe_title = (
            next_title.replace("'", "\u2019")
            .replace(":", "\\:")
            .replace("\\", "\\\\")
        )

        if use_poster:
            text_x = 280
            box_w = 640
            box_h = 220
            text_lines = self._wrap_title(safe_title, max_chars=28)
            title_draws = ""
            for li, line in enumerate(text_lines):
                y = 100 + li * 30
                title_draws += (
                    f",drawtext=fontfile={font}:"
                    f"text='{line}':"
                    f"fontsize=22:fontcolor=white@0.95:"
                    f"x={text_x}:y={y}"
                )
            next_overlay = (
                f",drawbox=x=40:y=35:w={box_w}:h={box_h}:color=black@0.65:t=fill,"
                f"drawtext=fontfile={font_bold}:"
                f"text='UP NEXT':"
                f"fontsize=22:fontcolor=0x5aa9ff:"
                f"x={text_x}:y=60"
                f"{title_draws}"
            )
        else:
            text_lines = self._wrap_title(safe_title, max_chars=40)
            box_h = 60 + len(text_lines) * 30
            title_draws = ""
            for li, line in enumerate(text_lines):
                y = 80 + li * 30
                title_draws += (
                    f",drawtext=fontfile={font}:"
                    f"text='{line}':"
                    f"fontsize=22:fontcolor=white@0.95:"
                    f"x=(w-text_w)/2:y={y}"
                )
            next_overlay = (
                f",drawbox=x=(w-600)/2:y=40:w=600:h={box_h}:color=black@0.65:t=fill,"
                f"drawtext=fontfile={font_bold}:"
                f"text='UP NEXT':"
                f"fontsize=18:fontcolor=0x5aa9ff:"
                f"x=(w-text_w)/2:y=50"
                f"{title_draws}"
            )

        parts = [base_vf]
        if countdown:
            parts.append(countdown)
        return ",".join(parts) + next_overlay

    def _build_bump_subffmpeg_cmd(self, item: QueueItem, cache_path: str = None) -> list:
        """Build the per-bump sub-ffmpeg command. When a cache_path is
        provided, uses the pre-encoded TS (already at target resolution)
        as input — skipping the mp4 decode and scale step."""
        input_path = cache_path or item.source_path
        if cache_path:
            base_vf = "null"
        else:
            base_vf = (
                f"scale=w={TARGET_WIDTH}:h={TARGET_HEIGHT}:force_original_aspect_ratio=decrease,"
                f"pad={TARGET_WIDTH}:{TARGET_HEIGHT}:(ow-iw)/2:(oh-ih)/2,setsar=1"
            )
        use_poster = (
            self.show_next
            and self.logo_path
            and os.path.isfile(self.logo_path)
        )
        vf = self._build_overlay_vf(item, base_vf)

        cmd = [
            "ffmpeg", "-y",
            "-threads", self.ffmpeg_threads,
            "-loglevel", "error",
            "-re",
        ]
        cmd.extend(["-i", input_path])
        if use_poster:
            cmd.extend(["-i", self.logo_path])
        if item.duration > 0:
            cmd.extend(["-t", f"{item.duration:.3f}"])

        if use_poster:
            poster_filter = (
                f"[1:v]scale=200:200[poster];"
                f"[0:v]{vf}[main];"
                f"[main][poster]overlay=55:50"
            )
            cmd.extend([
                "-filter_complex", poster_filter,
                "-map", "0:a:0?",
            ])
        else:
            cmd.extend([
                "-map", "0:v:0?", "-map", "0:a:0?",
                "-vf", vf,
            ])

        cmd.extend([
            "-r", str(TARGET_FPS),
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-profile:v", TARGET_VIDEO_PROFILE,
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", self.audio_bitrate,
            "-ar", str(TARGET_AUDIO_RATE),
            "-ac", str(TARGET_AUDIO_CHANNELS),
            "-f", "mpegts",
            "pipe:1",
        ])
        return cmd
