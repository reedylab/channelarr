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

# Polling cadence / backfill depth for playlist-based sources now live in
# segment_sources.py alongside HlsPlaylistSource, which is what uses them.

# When a cue ends, treat bare discontinuities within this window as bumpers
# (Adult Swim convention — see project memory).
BUMPER_WINDOW_SECONDS = 60


# QueueItem and build_bump_sequence live in segment_sources.py now — a
# SegmentSource is the thing that produces QueueItems, so that's where the
# type belongs. Re-exported here so existing `from core.resolver.transcoder
# import QueueItem` call sites (if any) keep working.
from core.resolver.segment_sources import (  # noqa: E402
    QueueItem, build_bump_sequence, SegmentSource, HlsPlaylistSource,
    ContinuousRelaySource, get_relay_source_config,
)

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
        encoder_mode: str = "single",  # "single" | "multi" | "copy"
        source_kind: str = "hls",
        source: Optional[SegmentSource] = None,
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
        self.source_kind = source_kind
        self.hls_time = hls_time
        self.hls_list_size = hls_list_size
        self.loglevel = loglevel
        self.video_preset = video_preset
        self.crf = crf
        self.ffmpeg_threads = ffmpeg_threads
        self.x264_threads = x264_threads
        self.audio_bitrate = audio_bitrate
        self.encoder_mode = encoder_mode

        # Look up source_domain for Referer headers on upstream requests
        self.source_domain = ""
        try:
            from core.database import get_session
            from core.models.manifest import Manifest as _M
            with get_session() as _s:
                _row = _s.query(_M.source_domain).filter_by(id=manifest_id).first()
                self.source_domain = (_row[0] if _row and _row[0] else "") or ""
        except Exception:
            pass

        self._enc_proc: Optional[subprocess.Popen] = None
        self._hls_proc: Optional[subprocess.Popen] = None  # multi mode only
        self._poller_thread: Optional[threading.Thread] = None
        self._feeder_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._segment_queue: queue.Queue[QueueItem] = queue.Queue(maxsize=200)
        self._download_dir = tempfile.mkdtemp(prefix=f"channelarr-res-{channel_id}-")
        self._started_at: Optional[float] = None
        self._last_access = time.time()

        # The SegmentSource does the actual content discovery/download; this
        # class only knows about QueueItems from here on. `source_kind` picks
        # which one — "hls" (the only kind that existed before this became
        # pluggable) polls a playlist via a StreamProfile, same as always.
        # A caller that already built a source (e.g. a relay-based channel)
        # passes it directly instead.
        if source is not None:
            self.source = source
        elif self.source_kind == "hls":
            self.source = HlsPlaylistSource(
                channel_id=channel_id,
                manifest_id=manifest_id,
                manifest_url=manifest_url,
                profile_name=profile_name,
                bump_paths=self.bump_paths,
                bump_durations=self.bump_durations,
                source_domain=self.source_domain,
                download_dir=self._download_dir,
            )
        elif self.source_kind == "relay":
            # No HLS playlist involved — manifest_url IS the stable
            # player-page URL for this source.
            relay_cfg = get_relay_source_config(self.source_domain)
            if not relay_cfg:
                raise ValueError(
                    f"source_kind=relay but no relay source config for "
                    f"domain {self.source_domain!r}"
                )
            self.source = ContinuousRelaySource(
                channel_id=channel_id,
                player_page_url=manifest_url,
                source_domain=self.source_domain,
                download_dir=self._download_dir,
                **relay_cfg,
            )
        else:
            raise ValueError(f"Unknown source_kind {source_kind!r} and no source given")

    # ── Public lifecycle ────────────────────────────────────────────────────

    def touch(self):
        self._last_access = time.time()

    @property
    def last_access(self) -> float:
        return self._last_access

    def start(self):
        """Start the full resolved channel pipeline: poller + feeder + encoder."""
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
            target=self.source.run, args=(self._segment_queue.put, self._stop_event),
            daemon=True, name=f"resolved-poller-{self.channel_id}",
        )
        self._feeder_thread = threading.Thread(
            target=self._feeder_loop, daemon=True, name=f"resolved-feeder-{self.channel_id}",
        )
        self._poller_thread.start()
        self._feeder_thread.start()
        logging.info("[RESOLVED-XCODE] Started channel %s (manifest=%s)",
                     self.channel_id, self.manifest_id)

    def status(self) -> dict:
        feeder_alive = (
            self._feeder_thread is not None
            and self._feeder_thread.is_alive()
        )
        uptime = 0
        if self._started_at and feeder_alive:
            uptime = int(time.time() - self._started_at)
        return {
            "running": feeder_alive,
            "uptime": uptime,
            "now_playing": "Live (transcode-mediated)" if feeder_alive else "",
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

    # Playlist-polling logic (poll variant, classify via StreamProfile,
    # download+decrypt segments) moved to segment_sources.py::HlsPlaylistSource
    # — this class only deals with QueueItems from here on.

    # ── Feeder loop ─────────────────────────────────────────────────────────
    # ONE long-running encoder reads MPEG-TS from stdin, decodes, re-encodes
    # to target params, and segments to HLS. The feeder thread drains the
    # segment queue and writes source bytes into the encoder's stdin.

    def _feeder_loop(self):
        try:
            if self.encoder_mode == "multi":
                self._feeder_loop_multi()
            else:
                self._feeder_loop_single()
        finally:
            self._clean_hls_dir()

    def _feeder_loop_single(self):
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
            return

        enc_cmd = (self._build_copy_encoder_cmd() if self.encoder_mode == "copy"
                   else self._build_combined_encoder_cmd())
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
                    # stop() (a different thread) can null this out between
                    # our stop_event check above and here.
                    enc_proc = self._enc_proc
                    if enc_proc is None or enc_proc.poll() is not None:
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

    # ── Multi-encoder feeder loop ──────────────────────────────────────────
    # Per-item encoder + separate HLS segmenter. Each source file gets its
    # own short-lived ffmpeg encoder that re-encodes to MPEG-TS on stdout,
    # piped into a long-running HLS segmenter. This avoids the PSI/PMT
    # mismatch issues of single-encoder mode at the cost of brief seams
    # at segment boundaries.

    def _feeder_loop_multi(self):
        hls_cmd = self._build_hls_cmd()
        logging.info("[RESOLVED-XCODE] %s HLS segmenter (multi mode): %s",
                     self.channel_id, " ".join(hls_cmd))
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
            enc_cmd = self._build_per_item_encoder_cmd(item, ts_offset)
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
                    # Use content duration for offset (not wall-clock, since
                    # -re is removed and encoding is faster than realtime)
                    ts_offset += item.duration if item.duration > 0 else file_elapsed
                    if item.duration > 0 and file_elapsed > 0:
                        from core.diagnostics import record_sample
                        record_sample(self.channel_id, "encode_speed_ratio",
                                     item.duration / file_elapsed)
                if self._enc_proc and self._enc_proc.stderr:
                    self._enc_proc.stderr.close()
            except Exception as e:
                logging.error("[RESOLVED-XCODE] %s encoder failed for %s: %s",
                              self.channel_id, item.label, e)
            finally:
                self._enc_proc = None
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

    def _build_copy_encoder_cmd(self) -> list:
        """"copy" mode — same combined single-process shape as
        _build_combined_encoder_cmd (one long-running ffmpeg fed via
        stdin), but does no re-encoding at all. For sources whose
        SegmentSource already normalized content to a directly-HLS-
        compatible codec before enqueueing it (relay sources transcode
        VP8/Opus -> H.264/AAC themselves, since that conversion is
        mandatory and can't happen anywhere else) — re-encoding it AGAIN
        here would just be redundant CPU cost and a second lossy
        generation for no benefit.

        No watermark/scale/framerate-forcing support: all of those need a
        decode+filter+re-encode pass, which is exactly what this mode
        exists to skip. Video comes through at whatever resolution/fps the
        upstream SegmentSource already produced. Fine for today's only
        caller (relay channels, no bump/show-next UI to overlay anyway);
        a source needing those would use "single" or "multi" instead."""
        playlist = os.path.join(self.hls_dir, "stream.m3u8")
        segment_pattern = os.path.join(self.hls_dir, "seg_%05d.ts")
        return [
            "ffmpeg", "-y",
            "-loglevel", self.loglevel,
            "-fflags", "+genpts+discardcorrupt",
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

    def _build_hls_cmd(self) -> list:
        """HLS segmenter for multi mode — reads MPEG-TS from stdin, copies
        to HLS segment files."""
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

    def _build_per_item_encoder_cmd(self, item: QueueItem, ts_offset: float) -> list:
        """Per-source encoder for multi mode — re-encodes one file to MPEG-TS
        on stdout for the HLS segmenter to consume."""
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
        use_watermark = (
            not use_poster
            and self.branding_logo_path
            and os.path.isfile(self.branding_logo_path)
        )

        cmd = [
            "ffmpeg", "-y",
            "-threads", self.ffmpeg_threads,
            "-loglevel", self.loglevel,
            "-i", item.source_path,
        ]
        if use_poster:
            cmd.extend(["-i", self.logo_path])
        if item.kind == "bump" and item.duration > 0:
            cmd.extend(["-t", f"{item.duration:.3f}"])

        if use_poster:
            overlay_vf = self._build_overlay_vf(item, base_vf)
            cmd.extend([
                "-filter_complex",
                f"[0:v]{overlay_vf}[vout];[1:v]scale=200:-1[poster];"
                f"[vout][poster]overlay=W-230:30",
                "-map", "0:a:0?",
            ])
        elif use_watermark:
            cmd.extend(["-loop", "1", "-i", self.branding_logo_path])
            wm_filter = (
                f"[1:v]scale=80:-1,format=rgba,colorchannelmixer=aa=0.6[wm];"
                f"[0:v]{base_vf}[main];"
                f"[main][wm]overlay=W-w-20:H-h-20:shortest=1"
            )
            cmd.extend(["-filter_complex", wm_filter, "-map", "0:a:0?"])
        else:
            vf = base_vf
            if item.kind == "bump" and self.show_next:
                vf = self._build_overlay_vf(item, base_vf)
            cmd.extend(["-map", "0:v:0?", "-map", "0:a:0?", "-vf", vf])

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
