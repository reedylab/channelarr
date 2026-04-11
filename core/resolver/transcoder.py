"""Transcode-mediated streaming for resolved channels.

When a resolved channel has `transcode_mediated=True`, instead of proxying the
upstream HLS bytes through unchanged, we run a full transcode pipeline like
scheduled channels do. The orchestrator polls the upstream variant playlist,
classifies each segment as show / ad / bumper using the SCTE-35 cue markers,
and feeds them through a long-running FFmpeg encoder. Ad segments get
replaced with bump files (configurable per channel).

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


# ── Upstream playlist parsing ───────────────────────────────────────────────

@dataclass
class UpstreamSegment:
    seq: int
    uri: str               # absolute URL (resolved against playlist base)
    duration: float
    program_date_time: Optional[str]
    discontinuity: bool    # this segment starts after a discontinuity
    cue_out_duration: Optional[float]  # if this segment starts a cue, total cue duration
    cue_out_cont_remaining: Optional[float]  # if mid-cue, seconds remaining (Duration - ElapsedTime)
    cue_in: bool           # this segment is the cue-in marker
    key_method: Optional[str]
    key_uri: Optional[str]
    key_iv: Optional[str]


def parse_variant_playlist(text: str, base_url: str) -> tuple[int, list[UpstreamSegment]]:
    """Parse a variant playlist into structured segments.

    Returns (media_sequence_start, segments). Each segment carries enough
    state to classify it as show/ad/bumper later.
    """
    lines = text.splitlines()
    media_seq = 0
    segments: list[UpstreamSegment] = []

    current_duration = 0.0
    current_pdt = None
    pending_disc = False
    pending_cue_out_dur = None
    pending_cue_out_cont_remaining = None
    pending_cue_in = False
    current_key_method = None
    current_key_uri = None
    current_key_iv = None

    for line in lines:
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                media_seq = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("#EXTINF:"):
            try:
                current_duration = float(line.split(":", 1)[1].split(",", 1)[0])
            except (ValueError, IndexError):
                current_duration = 0.0
        elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            current_pdt = line.split(":", 1)[1].strip()
        elif line.startswith("#EXT-X-DISCONTINUITY"):
            pending_disc = True
        elif line.startswith("#EXT-X-CUE-OUT-CONT"):
            # Continuation marker — extract Duration and ElapsedTime to know
            # how much cue is left when joining mid-break
            dur_m = re.search(r'Duration=([\d.]+)', line)
            elapsed_m = re.search(r'ElapsedTime=([\d.]+)', line)
            if dur_m and elapsed_m:
                try:
                    total = float(dur_m.group(1))
                    elapsed = float(elapsed_m.group(1))
                    pending_cue_out_cont_remaining = max(0.0, total - elapsed)
                except ValueError:
                    pass
        elif line.startswith("#EXT-X-CUE-OUT:"):
            try:
                pending_cue_out_dur = float(line.split(":", 1)[1].strip())
            except ValueError:
                pending_cue_out_dur = None
        elif line.startswith("#EXT-X-CUE-IN"):
            pending_cue_in = True
        elif line.startswith("#EXT-X-KEY:"):
            attrs = line.split(":", 1)[1]
            method_match = re.search(r'METHOD=([^,]+)', attrs)
            uri_match = re.search(r'URI="([^"]+)"', attrs)
            iv_match = re.search(r'IV=(0x[0-9a-fA-F]+)', attrs)
            current_key_method = method_match.group(1) if method_match else None
            current_key_uri = uri_match.group(1) if uri_match else None
            current_key_iv = iv_match.group(1) if iv_match else None
        elif line and not line.startswith("#"):
            # Segment URI line
            seq_num = media_seq + len(segments)
            abs_uri = urljoin(base_url, line)
            segments.append(UpstreamSegment(
                seq=seq_num,
                uri=abs_uri,
                duration=current_duration,
                program_date_time=current_pdt,
                discontinuity=pending_disc,
                cue_out_duration=pending_cue_out_dur,
                cue_out_cont_remaining=pending_cue_out_cont_remaining,
                cue_in=pending_cue_in,
                key_method=current_key_method,
                key_uri=current_key_uri,
                key_iv=current_key_iv,
            ))
            current_duration = 0.0
            current_pdt = None
            pending_disc = False
            pending_cue_out_dur = None
            pending_cue_out_cont_remaining = None
            pending_cue_in = False

    return media_seq, segments


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
        hls_time: int = 6,
        hls_list_size: int = 10,
        loglevel: str = "warning",
        video_preset: str = "fast",
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
        self.hls_time = hls_time
        self.hls_list_size = hls_list_size
        self.loglevel = loglevel
        self.video_preset = video_preset
        self.crf = crf
        self.ffmpeg_threads = ffmpeg_threads
        self.x264_threads = x264_threads
        self.audio_bitrate = audio_bitrate

        self._enc_proc: Optional[subprocess.Popen] = None
        self._hls_proc: Optional[subprocess.Popen] = None
        self._poller_thread: Optional[threading.Thread] = None
        self._encoder_thread: Optional[threading.Thread] = None
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

    def start(self):
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
        self._poller_thread = threading.Thread(
            target=self._poller_loop, daemon=True, name=f"resolved-poller-{self.channel_id}",
        )
        self._encoder_thread = threading.Thread(
            target=self._encoder_loop, daemon=True, name=f"resolved-encoder-{self.channel_id}",
        )
        self._poller_thread.start()
        self._encoder_thread.start()
        logging.info("[RESOLVED-XCODE] Started channel %s (manifest=%s)",
                     self.channel_id, self.manifest_id)

    def status(self) -> dict:
        alive = (
            self._encoder_thread is not None
            and self._encoder_thread.is_alive()
        )
        uptime = 0
        if self._started_at and alive:
            uptime = int(time.time() - self._started_at)
        return {
            "running": alive,
            "uptime": uptime,
            "now_playing": "Live (transcode-mediated)" if alive else "",
        }

    def stop(self):
        self._stop_event.set()
        for proc in (self._enc_proc, self._hls_proc):
            if proc:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        self._enc_proc = None
        self._hls_proc = None
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

    # ── Playlist poller ────────────────────────────────────────────────────

    def _poller_loop(self):
        """Poll the upstream variant playlist, classify segments, enqueue work.

        Maintains state about the current cue (if any) so consecutive ad
        segments get replaced by a single bump sequence rather than one bump
        per ad segment.
        """
        seen_seqs: set[int] = set()
        cue_state = "IDLE"            # IDLE | IN_CUE | POST_CUE_BUMPER_WINDOW
        cue_remaining = 0.0
        bumper_window_until = 0.0     # epoch when post-cue bumper window expires
        backfilled = False

        # Need a variant URL — if the manifest_url is a master, pick the first variant
        variant_url = self._resolve_variant_url(self.manifest_url)
        logging.info("[RESOLVED-XCODE] Polling variant: %s", variant_url[:120])

        while not self._stop_event.is_set():
            try:
                resp = http_requests.get(variant_url, timeout=10)
                if resp.status_code != 200:
                    logging.warning("[RESOLVED-XCODE] %s playlist HTTP %d", self.channel_id, resp.status_code)
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue
                _, segments = parse_variant_playlist(resp.text, variant_url)
            except Exception as e:
                logging.warning("[RESOLVED-XCODE] %s playlist fetch failed: %s", self.channel_id, e)
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            # On first poll, mark every old segment as already-seen so we
            # only process segments at the live edge. This prevents
            # replaying the whole rolling window (which would include
            # historical ad breaks).
            if not backfilled:
                for old_seg in segments[:-INITIAL_BACKFILL_SEGMENTS]:
                    seen_seqs.add(old_seg.seq)
                # Also: if any of the live-edge segments are mid-cue, set
                # state appropriately so we know we joined inside a break.
                live_edge = segments[-INITIAL_BACKFILL_SEGMENTS:] if len(segments) > INITIAL_BACKFILL_SEGMENTS else segments
                for s in live_edge:
                    if s.cue_out_cont_remaining is not None and not s.cue_in:
                        cue_state = "IN_CUE"
                        cue_remaining = s.cue_out_cont_remaining
                        logging.info(
                            "[RESOLVED-XCODE] %s joined mid-cue, %.1fs remaining",
                            self.channel_id, cue_remaining,
                        )
                        bump_seq = build_bump_sequence(
                            self.bump_paths, self.bump_durations, cue_remaining,
                        )
                        cue_left = cue_remaining
                        for bump_path, bump_dur in bump_seq:
                            self._segment_queue.put(QueueItem(
                                kind="bump",
                                source_path=bump_path,
                                duration=bump_dur,
                                label=os.path.basename(bump_path),
                                cue_remaining_at_start=cue_left,
                            ))
                            cue_left -= bump_dur
                        if not bump_seq:
                            logging.warning(
                                "[RESOLVED-XCODE] %s no bumps configured — joining mid-cue with no replacement",
                                self.channel_id,
                            )
                        break
                backfilled = True

            for seg in segments:
                if seg.seq in seen_seqs:
                    continue
                seen_seqs.add(seg.seq)

                # Cue start: a segment with cue_out_duration starts an ad break
                if seg.cue_out_duration:
                    cue_state = "IN_CUE"
                    cue_remaining = seg.cue_out_duration
                    logging.info("[RESOLVED-XCODE] %s CUE-OUT detected: %.1fs",
                                 self.channel_id, cue_remaining)
                    bump_seq = build_bump_sequence(self.bump_paths, self.bump_durations, cue_remaining)
                    if not bump_seq:
                        logging.warning(
                            "[RESOLVED-XCODE] %s no bumps configured — falling back to upstream during cue",
                            self.channel_id,
                        )
                        self._enqueue_upstream(seg)
                        continue
                    # Discard the upstream ad segments and queue bumps instead
                    cue_left = cue_remaining
                    for bump_path, bump_dur in bump_seq:
                        self._segment_queue.put(QueueItem(
                            kind="bump",
                            source_path=bump_path,
                            duration=bump_dur,
                            label=os.path.basename(bump_path),
                            cue_remaining_at_start=cue_left,
                        ))
                        cue_left -= bump_dur
                    continue

                # Inside an active cue: skip the upstream ad segments
                if cue_state == "IN_CUE":
                    cue_remaining -= seg.duration
                    if seg.cue_in or cue_remaining <= 0:
                        cue_state = "POST_CUE_BUMPER_WINDOW"
                        bumper_window_until = time.time() + BUMPER_WINDOW_SECONDS
                        logging.info("[RESOLVED-XCODE] %s CUE-IN — bumper window opens", self.channel_id)
                    continue

                # Post-cue: bare discontinuities within the window are bumpers
                # (Adult Swim convention) — pass through unchanged.
                if cue_state == "POST_CUE_BUMPER_WINDOW":
                    if time.time() > bumper_window_until:
                        cue_state = "IDLE"

                # Normal show or bumper segment — enqueue
                self._enqueue_upstream(seg)

            # Sleep before next poll
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
        """Download a segment to local disk. If AES-128 encrypted, decrypt
        in-place using ffmpeg as the demuxer (it handles the key fetch).

        Returns the local file path. Caller is responsible for cleanup
        (the encoder loop deletes after encoding).
        """
        local_path = os.path.join(self._download_dir, f"seg_{seg.seq}.ts")
        # Use ffmpeg to fetch + decrypt + remux to local .ts. ffmpeg follows
        # the EXT-X-KEY URI we synthesize via -allowed_extensions.
        if seg.key_method == "AES-128" and seg.key_uri:
            # Build a tiny one-segment playlist that ffmpeg can parse with the key
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
            # Plain segment — just download
            r = http_requests.get(seg.uri, timeout=15, stream=True)
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
        return local_path

    # ── Encoder loop ───────────────────────────────────────────────────────

    def _encoder_loop(self):
        try:
            self._encoder_loop_inner()
        finally:
            self._clean_hls_dir()

    def _encoder_loop_inner(self):
        hls_cmd = self._build_hls_cmd()
        logging.info("[RESOLVED-XCODE] %s HLS segmenter: %s", self.channel_id, " ".join(hls_cmd))
        self._hls_proc = subprocess.Popen(
            hls_cmd, stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )

        ts_offset = 0.0
        while not self._stop_event.is_set():
            try:
                item = self._segment_queue.get(timeout=10)
            except queue.Empty:
                continue

            logging.info("[RESOLVED-XCODE] %s encode: %s (%s, %.1fs)",
                         self.channel_id, item.label, item.kind, item.duration)
            enc_cmd = self._build_encoder_cmd(item, ts_offset)
            file_start = time.time()

            try:
                self._enc_proc = subprocess.Popen(
                    enc_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                while not self._stop_event.is_set():
                    chunk = self._enc_proc.stdout.read(65536)
                    if not chunk:
                        break
                    try:
                        self._hls_proc.stdin.write(chunk)
                    except (BrokenPipeError, OSError):
                        logging.error("[RESOLVED-XCODE] %s HLS pipe broke", self.channel_id)
                        self._stop_event.set()
                        break
                self._enc_proc.stdout.close()
                self._enc_proc.wait()
                rc = self._enc_proc.returncode
                file_elapsed = time.time() - file_start
                if rc != 0 and not self._stop_event.is_set():
                    err = self._enc_proc.stderr.read().decode("utf-8", errors="replace")[-300:]
                    logging.warning("[RESOLVED-XCODE] %s encoder rc=%d for %s: %s",
                                    self.channel_id, rc, item.label, err)
                else:
                    ts_offset += file_elapsed
                if self._enc_proc and self._enc_proc.stderr:
                    self._enc_proc.stderr.close()
            except Exception as e:
                logging.error("[RESOLVED-XCODE] %s encoder failed for %s: %s",
                              self.channel_id, item.label, e)
            finally:
                self._enc_proc = None
                # Delete the temp upstream file once encoded
                if item.kind == "upstream":
                    try:
                        os.remove(item.source_path)
                    except OSError:
                        pass

        if self._hls_proc and self._hls_proc.stdin:
            try:
                self._hls_proc.stdin.close()
            except Exception:
                pass
        if self._hls_proc:
            try:
                self._hls_proc.wait(timeout=5)
            except Exception:
                pass
            self._hls_proc = None

    def _build_hls_cmd(self) -> list:
        playlist = os.path.join(self.hls_dir, "stream.m3u8")
        segment_pattern = os.path.join(self.hls_dir, "seg_%05d.ts")
        return [
            "ffmpeg", "-y",
            "-loglevel", self.loglevel,
            "-f", "mpegts",
            "-i", "pipe:0",
            "-c", "copy",
            "-f", "hls",
            "-hls_time", str(self.hls_time),
            "-hls_list_size", str(self.hls_list_size),
            "-hls_flags", "delete_segments+omit_endlist",
            "-hls_segment_filename", segment_pattern,
            playlist,
        ]

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
        if not item.cue_remaining_at_start or item.cue_remaining_at_start <= 0:
            return base_vf

        font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        font_bold = font

        # Countdown is from cue_remaining_at_start, decrementing as bump time
        # progresses (ffmpeg's `t` is current playback time in this bump).
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

        return f"{base_vf},{countdown}{next_overlay}"

    def _build_encoder_cmd(self, item: QueueItem, ts_offset: float) -> list:
        """Build the per-source encoder command. Re-encodes everything to
        identical MPEG-TS params so the segmenter sees one coherent bitstream
        across source switches.

        For bump items with show_next enabled, draws the same RESUMING IN
        countdown + UP NEXT overlay as scheduled channels do.
        """
        base_vf = (
            f"scale=w={TARGET_WIDTH}:h={TARGET_HEIGHT}:force_original_aspect_ratio=decrease,"
            f"pad={TARGET_WIDTH}:{TARGET_HEIGHT}:(ow-iw)/2:(oh-ih)/2,setsar=1"
        )
        use_poster = (
            item.kind == "bump"
            and self.show_next
            and item.cue_remaining_at_start
            and self.logo_path
            and os.path.isfile(self.logo_path)
        )
        vf = self._build_overlay_vf(item, base_vf)

        cmd = [
            "ffmpeg", "-y",
            "-threads", self.ffmpeg_threads,
            "-loglevel", self.loglevel,
            "-re",
        ]
        cmd.extend(["-i", item.source_path])
        # Logo as a second input for the poster overlay
        if use_poster:
            cmd.extend(["-i", self.logo_path])
        if item.kind == "bump" and item.duration > 0:
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
            "-x264-params", f"threads={self.x264_threads}",
            "-preset", self.video_preset,
            "-profile:v", TARGET_VIDEO_PROFILE,
            "-pix_fmt", "yuv420p",
            "-force_key_frames", f"expr:gte(t,n_forced*{self.hls_time})",
            "-c:a", "aac",
            "-b:a", self.audio_bitrate,
            "-ar", str(TARGET_AUDIO_RATE),
            "-ac", str(TARGET_AUDIO_CHANNELS),
            "-output_ts_offset", f"{ts_offset:.3f}",
            "-f", "mpegts",
            "pipe:1",
        ])
        if self.crf:
            idx = cmd.index("-profile:v")
            cmd.insert(idx, self.crf)
            cmd.insert(idx, "-crf")
        return cmd
