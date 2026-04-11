"""FFmpeg HLS process manager — schedule-driven with seek support.

Architecture:
  [File 1 @ seek] → FFmpeg encoder → MPEG-TS stdout ─┐
  [File 2]        → FFmpeg encoder → MPEG-TS stdout ──┼──→ pipe ──→ FFmpeg HLS segmenter → .m3u8 + .ts
  [File 3]        → FFmpeg encoder → MPEG-TS stdout ──┘

Each file is independently encoded to identical MPEG-TS params.
The pipe never breaks between files, so the HLS stream is continuous.
The first file may be seeked into (via -ss) to match the schedule position.
"""

import json
import os
import subprocess
import threading
import time
import logging

from core.nfo import read_nfo_title, find_poster


class ChannelStream:
    def __init__(self, channel_id: str, schedule: list, start_index: int = 0,
                 start_seek: float = 0.0, hls_dir: str = "",
                 hls_time: int = 6, hls_list_size: int = 10, loglevel: str = "warning",
                 loop: bool = True, channel_mgr=None,
                 video_preset: str = "fast", crf: str = "",
                 ffmpeg_threads: str = "1", x264_threads: str = "4",
                 audio_bitrate: str = "192k", show_next: bool = False):
        self.channel_id = channel_id
        self.schedule = schedule
        self.start_index = start_index
        self.start_seek = start_seek
        self.hls_dir = hls_dir
        self.hls_time = hls_time
        self.hls_list_size = hls_list_size
        self.loglevel = loglevel
        self.loop = loop
        self.channel_mgr = channel_mgr
        self.video_preset = video_preset
        self.crf = crf
        self.ffmpeg_threads = ffmpeg_threads
        self.x264_threads = x264_threads
        self.audio_bitrate = audio_bitrate
        self.show_next = show_next

        self._enc_proc = None
        self._hls_proc = None
        self._thread = None
        self._stop_event = threading.Event()
        self._started_at = None
        self._current_title = ""
        self._last_access = time.time()

        # YouTube pre-fetch tracking
        self._yt_downloads = {}   # yt_id -> threading.Event
        self._yt_failures = set() # yt_ids that failed to download

    def touch(self):
        """Update last access time — called when clients request segments."""
        self._last_access = time.time()

    @property
    def last_access(self) -> float:
        return self._last_access

    def start(self):
        os.makedirs(self.hls_dir, exist_ok=True)
        for f in os.listdir(self.hls_dir):
            if f.endswith(".ts") or f.endswith(".m3u8"):
                os.remove(os.path.join(self.hls_dir, f))

        self._stop_event.clear()
        self._started_at = time.time()
        self._last_access = time.time()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logging.info("[STREAM] Started channel %s at index %d, seek %.1fs",
                     self.channel_id, self.start_index, self.start_seek)

    @staticmethod
    def _find_next_content(entries: list, current_idx: int) -> dict | None:
        """Find the next non-bump entry after current_idx."""
        for j in range(current_idx + 1, len(entries)):
            if entries[j]["type"] != "bump":
                return entries[j]
        return None

    @staticmethod
    def _wrap_title(text: str, max_chars: int = 28) -> list:
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
        return lines[:4]

    def _build_encoder_cmd(self, filepath: str, ts_offset: float = 0.0,
                           seek_seconds: float = 0.0,
                           is_bump: bool = False, bump_duration: float = 0.0,
                           next_title: str = "", next_poster: str = "",
                           is_youtube: bool = False) -> list:
        """Build FFmpeg command to encode a single file to MPEG-TS on stdout.

        seek_seconds: if > 0, seek into the file before encoding (for schedule catch-up).
        ts_offset: chains PTS timestamps across files for seamless HLS.
        """
        font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        font_bold = font
        base_vf = (
            "scale=1920:1080:force_original_aspect_ratio=decrease,"
            "pad=1920:1080:(ow-iw)/2:(oh-ih)/2,"
            "format=yuv420p"
        )
        # YouTube content uses a faster preset to reduce CPU load
        preset = "veryfast" if is_youtube else self.video_preset

        use_poster = bool(is_bump and next_title and next_poster and os.path.isfile(next_poster))

        if is_bump and bump_duration > 0:
            dur = f"{bump_duration:.2f}"
            countdown = (
                f"drawbox=x=(w-400)/2:y=h-80:w=400:h=50:color=black@0.6:t=fill,"
                f"drawtext=fontfile={font}:"
                f"text='RESUMING IN "
                f"%{{eif\\:trunc(max(0\\,{dur}-t)/60)\\:d}}"
                f"\\:"
                f"%{{eif\\:mod(max(0\\,{dur}-t)\\,60)\\:d\\:2}}':"
                f"fontsize=28:fontcolor=white@0.9:"
                f"x=(w-text_w)/2:y=h-70"
            )

            if next_title:
                safe_title = next_title.replace("'", "\u2019").replace(":", "\\:").replace("\\", "\\\\")
                if use_poster:
                    text_x = 280
                    box_w = 640
                    box_h = 340
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
            else:
                next_overlay = ""

            vf = f"{base_vf},{countdown}{next_overlay}"
        else:
            vf = base_vf

        if use_poster:
            poster_filter = (
                f"[1:v]scale=200:300[poster];"
                f"[0:v]{vf}[main];"
                f"[main][poster]overlay=55:50"
            )
            cmd = [
                "ffmpeg", "-y",
                "-threads", self.ffmpeg_threads,
                "-loglevel", self.loglevel,
                "-re",
            ]
            if seek_seconds > 0:
                cmd.extend(["-ss", f"{seek_seconds:.3f}"])
            cmd.extend([
                "-i", filepath,
                "-i", next_poster,
                "-filter_complex", poster_filter,
                "-map", "0:a:0",
                "-r", "30",
                "-c:v", "libx264",
                "-x264-params", f"threads={self.x264_threads}",
                "-preset", preset,
                "-profile:v", "high",
                "-force_key_frames", f"expr:gte(t,n_forced*{self.hls_time})",
                "-c:a", "aac",
                "-b:a", self.audio_bitrate,
                "-ac", "2",
                "-ar", "48000",
                "-output_ts_offset", f"{ts_offset:.3f}",
                "-f", "mpegts",
                "pipe:1",
            ])
        else:
            cmd = [
                "ffmpeg", "-y",
                "-threads", self.ffmpeg_threads,
                "-loglevel", self.loglevel,
                "-re",
            ]
            if seek_seconds > 0:
                cmd.extend(["-ss", f"{seek_seconds:.3f}"])
            cmd.extend([
                "-i", filepath,
                "-map", "0:v:0", "-map", "0:a:0",
                "-vf", vf,
                "-r", "30",
                "-c:v", "libx264",
                "-x264-params", f"threads={self.x264_threads}",
                "-preset", preset,
                "-profile:v", "high",
                "-force_key_frames", f"expr:gte(t,n_forced*{self.hls_time})",
                "-c:a", "aac",
                "-b:a", self.audio_bitrate,
                "-ac", "2",
                "-ar", "48000",
                "-output_ts_offset", f"{ts_offset:.3f}",
                "-f", "mpegts",
                "pipe:1",
            ])
        if self.crf:
            idx = cmd.index("-profile:v")
            cmd.insert(idx, self.crf)
            cmd.insert(idx, "-crf")
        return cmd

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

    def _prefetch_yt(self, schedule: list, current_idx: int, lookahead: int = 2):
        """Start background downloads for the next N YouTube entries."""
        from core.youtube import yt_download
        for offset in range(1, lookahead + 1):
            next_idx = current_idx + offset
            if next_idx >= len(schedule):
                if self.loop:
                    next_idx = next_idx % len(schedule)
                else:
                    break
            entry = schedule[next_idx]
            if entry.get("type") != "youtube":
                continue
            yt_id = entry.get("yt_id", "")
            if not yt_id or yt_id in self._yt_downloads or yt_id in self._yt_failures:
                continue
            if os.path.isfile(entry["path"]):
                continue
            event = threading.Event()
            self._yt_downloads[yt_id] = event

            def _dl(e=entry, ev=event, vid=yt_id):
                ok = yt_download(e.get("url", ""), e["path"])
                if not ok:
                    self._yt_failures.add(vid)
                ev.set()

            threading.Thread(target=_dl, daemon=True).start()
            logging.info("[YT] Pre-fetching %s", yt_id)

    def _ensure_yt_ready(self, entry: dict, timeout: float = 900) -> bool:
        """Wait for a YouTube video to be downloaded. Returns True if ready."""
        from core.youtube import yt_download
        yt_id = entry.get("yt_id", "")
        path = entry["path"]

        if os.path.isfile(path):
            return True
        if yt_id in self._yt_failures:
            return False

        # Start download if not already in progress
        if yt_id not in self._yt_downloads:
            event = threading.Event()
            self._yt_downloads[yt_id] = event

            def _dl():
                ok = yt_download(entry.get("url", ""), path)
                if not ok:
                    self._yt_failures.add(yt_id)
                event.set()

            threading.Thread(target=_dl, daemon=True).start()

        event = self._yt_downloads.get(yt_id)
        if event:
            event.wait(timeout=timeout)
        return os.path.isfile(path)

    def _run_loop(self):
        try:
            self._run_loop_inner()
        finally:
            # Always clean up HLS files when the loop exits for any reason
            self._clean_hls_dir()
            logging.info("[STREAM] Cleaned HLS files for channel %s", self.channel_id)

    def _run_loop_inner(self):
        while not self._stop_event.is_set():
            schedule = self.schedule
            if not schedule:
                logging.error("[STREAM] No schedule for channel %s", self.channel_id)
                break

            hls_cmd = self._build_hls_cmd()
            logging.info("[STREAM] HLS segmenter: %s", " ".join(hls_cmd))
            self._hls_proc = subprocess.Popen(
                hls_cmd, stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            )

            ts_offset = 0.0
            is_first_file = True

            for i in range(self.start_index, len(schedule)):
                if self._stop_event.is_set():
                    break
                entry = schedule[i]
                filepath = entry["path"]
                is_bump = entry["type"] == "bump"
                is_youtube = entry.get("type") == "youtube"
                self._current_title = entry.get("title", os.path.basename(filepath))
                seek = self.start_seek if is_first_file else 0.0

                # YouTube: pre-fetch upcoming and wait for current
                if is_youtube:
                    self._prefetch_yt(schedule, i)
                    if not self._ensure_yt_ready(entry):
                        logging.warning("[STREAM] Skipping YouTube item %s — download failed",
                                        entry.get("yt_id", ""))
                        is_first_file = False
                        continue

                logging.info("[STREAM] Channel %s [%d/%d] (offset=%.1fs, seek=%.1fs%s%s): %s",
                             self.channel_id, i + 1, len(schedule), ts_offset, seek,
                             ", bump" if is_bump else "",
                             ", yt" if is_youtube else "",
                             self._current_title)

                bump_duration = 0.0
                next_title = ""
                next_poster = ""
                if is_bump:
                    bump_duration = entry.get("duration", 0)
                    if seek > 0:
                        bump_duration = max(0, bump_duration - seek)
                    if self.show_next:
                        next_content = self._find_next_content(schedule, i)
                        if next_content:
                            next_title = read_nfo_title(next_content["path"])
                            next_poster = find_poster(next_content["path"]) or ""

                enc_cmd = self._build_encoder_cmd(
                    filepath, ts_offset,
                    seek_seconds=seek,
                    is_bump=is_bump,
                    bump_duration=bump_duration,
                    next_title=next_title,
                    next_poster=next_poster,
                    is_youtube=is_youtube,
                )
                file_start = time.time()
                is_first_file = False

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
                            logging.error("[STREAM] HLS pipe broke for %s", self.channel_id)
                            self._stop_event.set()
                            break

                    self._enc_proc.stdout.close()
                    self._enc_proc.wait()
                    rc = self._enc_proc.returncode
                    file_elapsed = time.time() - file_start
                    if rc != 0 and not self._stop_event.is_set():
                        err = self._enc_proc.stderr.read().decode("utf-8", errors="replace")[-500:]
                        logging.error("[STREAM] Encoder exited %d for [%s]: %s",
                                      rc, self._current_title, err)
                    else:
                        ts_offset += file_elapsed
                        logging.info("[STREAM] Finished [%s] in %.1fs, next offset=%.1fs",
                                     self._current_title, file_elapsed, ts_offset)
                        # Clean up YouTube file after successful encoding
                        if is_youtube:
                            yt_id = entry.get("yt_id", "")
                            try:
                                if os.path.isfile(filepath):
                                    os.remove(filepath)
                                    logging.info("[YT] Cleaned up after playback: %s", yt_id)
                            except OSError as e:
                                logging.warning("[YT] Cleanup failed for %s: %s", yt_id, e)
                            self._yt_downloads.pop(yt_id, None)
                    if self._enc_proc and self._enc_proc.stderr:
                        self._enc_proc.stderr.close()
                except Exception as e:
                    logging.error("[STREAM] Encoder failed for %s: %s", self._current_title, e)
                finally:
                    self._enc_proc = None

            # Close HLS pipe
            if self._hls_proc and self._hls_proc.stdin:
                try:
                    self._hls_proc.stdin.close()
                except Exception:
                    pass
            if self._hls_proc:
                self._hls_proc.wait()
                self._hls_proc = None

            if self._stop_event.is_set():
                break

            if self.loop:
                logging.info("[STREAM] Channel %s playlist ended, looping...", self.channel_id)
                self._yt_downloads.clear()
                self._yt_failures.clear()
                # On loop, recalculate position from schedule
                if self.channel_mgr:
                    from core.channels import find_schedule_position
                    ch = self.channel_mgr.get_channel(self.channel_id)
                    if ch and ch.get("materialized_schedule"):
                        self.schedule = ch["materialized_schedule"]
                        idx, seek = find_schedule_position(ch)
                        if idx is not None:
                            self.start_index = idx
                            self.start_seek = seek
                        else:
                            self.start_index = 0
                            self.start_seek = 0.0
                    else:
                        self.start_index = 0
                        self.start_seek = 0.0
                else:
                    self.start_index = 0
                    self.start_seek = 0.0
            else:
                logging.info("[STREAM] Channel %s finished (no loop)", self.channel_id)
                break

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
        # Clean up HLS files so stale segments don't get served
        self._clean_hls_dir()
        logging.info("[STREAM] Stopped channel %s", self.channel_id)

    def _clean_hls_dir(self):
        """Remove all .ts, .m3u8, and concat.txt files from this channel's HLS dir."""
        if not os.path.isdir(self.hls_dir):
            return
        for f in os.listdir(self.hls_dir):
            if f.endswith(".ts") or f.endswith(".m3u8") or f == "concat.txt":
                try:
                    os.remove(os.path.join(self.hls_dir, f))
                except OSError:
                    pass

    def status(self) -> dict:
        alive = self._thread is not None and self._thread.is_alive()
        uptime = 0
        if self._started_at and alive:
            uptime = int(time.time() - self._started_at)
        return {
            "running": alive,
            "uptime": uptime,
            "now_playing": self._current_title if alive else "",
        }


class StreamerManager:
    def __init__(self, get_setting_fn):
        self._get = get_setting_fn
        # Holds either ChannelStream (scheduled) or ResolvedChannelStream
        # (resolved+transcode_mediated). Both expose the same lifecycle:
        # start(), stop(), touch(), last_access, status().
        self._streams: dict = {}
        self._cleanup_thread = None
        self._cleanup_stop = threading.Event()

    def start_channel(self, channel_id: str, schedule: list,
                      start_index: int = 0, start_seek: float = 0.0,
                      loop: bool = True, show_next: bool = False,
                      channel_mgr=None) -> bool:
        if channel_id in self._streams:
            if self._streams[channel_id].status()["running"]:
                return False

        hls_base = self._get("HLS_OUTPUT_PATH", "/app/data/hls")
        hls_dir = os.path.join(hls_base, channel_id)

        stream = ChannelStream(
            channel_id=channel_id,
            schedule=schedule,
            start_index=start_index,
            start_seek=start_seek,
            hls_dir=hls_dir,
            hls_time=int(self._get("HLS_TIME", "6")),
            hls_list_size=int(self._get("HLS_LIST_SIZE", "10")),
            loglevel=self._get("FFMPEG_LOGLEVEL", "warning"),
            loop=loop,
            channel_mgr=channel_mgr,
            video_preset=self._get("VIDEO_PRESET", "fast"),
            crf=self._get("VIDEO_CRF", ""),
            ffmpeg_threads=self._get("FFMPEG_THREADS", "1"),
            x264_threads=self._get("X264_THREADS", "4"),
            audio_bitrate=self._get("AUDIO_BITRATE", "192k"),
            show_next=show_next,
        )
        stream.start()
        self._streams[channel_id] = stream
        return True

    def start_resolved_channel(self, channel_id: str, manifest_id: str,
                                manifest_url: str, bump_config: dict,
                                bump_manager) -> bool:
        """Start a transcode-mediated resolved channel.

        Builds a ResolvedChannelStream that polls the upstream playlist,
        downloads/decrypts segments, and inserts bumps at SCTE-35 cue
        boundaries. Output goes to the same /app/data/hls/{channel_id}/
        directory as scheduled channels — the existing HLS endpoints serve
        it without any changes.
        """
        from core.resolver.transcoder import ResolvedChannelStream
        from core.channels import ffprobe_duration

        if channel_id in self._streams:
            existing = self._streams[channel_id]
            try:
                if existing.status()["running"]:
                    return False
            except Exception:
                pass
            try:
                existing.stop()
            except Exception:
                pass
            del self._streams[channel_id]

        # Resolve bump folders into a flat list of files with durations
        folders = (bump_config or {}).get("folders") or []
        if not folders and (bump_config or {}).get("folder"):
            folders = [bump_config["folder"]]
        bump_paths: list = []
        bump_durations: dict = {}
        for folder in folders:
            try:
                clips = bump_manager.get_clips(folder)
            except Exception as e:
                logging.warning("[STREAM] couldn't get bumps for %s: %s", folder, e)
                continue
            for clip_path in clips:
                bump_paths.append(clip_path)
                bump_durations[clip_path] = ffprobe_duration(clip_path)

        hls_base = self._get("HLS_OUTPUT_PATH", "/app/data/hls")
        hls_dir = os.path.join(hls_base, channel_id)

        stream = ResolvedChannelStream(
            channel_id=channel_id,
            manifest_id=manifest_id,
            manifest_url=manifest_url,
            bump_paths=bump_paths,
            bump_durations=bump_durations,
            hls_dir=hls_dir,
            hls_time=int(self._get("HLS_TIME", "6")),
            hls_list_size=int(self._get("HLS_LIST_SIZE", "10")),
            loglevel=self._get("FFMPEG_LOGLEVEL", "warning"),
            video_preset=self._get("VIDEO_PRESET", "fast"),
            crf=self._get("VIDEO_CRF", ""),
            ffmpeg_threads=self._get("FFMPEG_THREADS", "1"),
            x264_threads=self._get("X264_THREADS", "4"),
            audio_bitrate=self._get("AUDIO_BITRATE", "192k"),
        )
        stream.start()
        self._streams[channel_id] = stream
        return True

    def stop_channel(self, channel_id: str) -> bool:
        stream = self._streams.get(channel_id)
        if not stream:
            return False
        stream.stop()
        del self._streams[channel_id]
        return True

    def stop_all(self):
        for cid in list(self._streams.keys()):
            self.stop_channel(cid)

    def touch(self, channel_id: str):
        """Update last access time for a channel stream."""
        stream = self._streams.get(channel_id)
        if stream:
            stream.touch()

    def get_status(self, channel_id: str) -> dict:
        stream = self._streams.get(channel_id)
        if not stream:
            return {"running": False, "uptime": 0}
        return stream.status()

    def get_all_status(self) -> dict:
        return {cid: s.status() for cid, s in self._streams.items()}

    def running_count(self) -> int:
        return sum(1 for s in self._streams.values() if s.status()["running"])

    def cleanup_idle(self, timeout_seconds: int = 300):
        """Stop channels that haven't been accessed recently."""
        now = time.time()
        to_stop = []
        for cid, stream in self._streams.items():
            if stream.status()["running"] and (now - stream.last_access) > timeout_seconds:
                to_stop.append(cid)
        for cid in to_stop:
            logging.info("[STREAM] Idle timeout — stopping channel %s", cid)
            self.stop_channel(cid)

    def start_idle_cleanup(self, interval: int = 60, timeout: int = 300):
        """Start background thread to periodically clean up idle streams."""
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            return

        def _cleanup_loop():
            while not self._cleanup_stop.is_set():
                self.cleanup_idle(timeout)
                self._cleanup_stop.wait(interval)

        self._cleanup_stop.clear()
        self._cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True,
                                                 name="stream-idle-cleanup")
        self._cleanup_thread.start()
        logging.info("[STREAM] Idle cleanup started (interval=%ds, timeout=%ds)",
                     interval, timeout)
