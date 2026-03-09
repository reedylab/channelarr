"""FFmpeg HLS process manager — pipe-based architecture for seamless playback.

Architecture:
  [File 1] → FFmpeg encoder → MPEG-TS stdout ─┐
  [File 2] → FFmpeg encoder → MPEG-TS stdout ──┼──→ pipe ──→ FFmpeg HLS segmenter → .m3u8 + .ts
  [File 3] → FFmpeg encoder → MPEG-TS stdout ──┘

Each file is independently encoded to identical MPEG-TS params.
The pipe never breaks between files, so the HLS stream is continuous.
"""

import json
import os
import subprocess
import threading
import time
import logging

from core.nfo import read_nfo_title, find_poster


class ChannelStream:
    def __init__(self, channel_id: str, concat_path: str, hls_dir: str,
                 hls_time: int = 6, hls_list_size: int = 10, loglevel: str = "warning",
                 loop: bool = True, on_finished=None,
                 video_preset: str = "fast", crf: str = "",
                 ffmpeg_threads: str = "1", x264_threads: str = "4",
                 audio_bitrate: str = "192k", show_next: bool = False):
        self.channel_id = channel_id
        self.concat_path = concat_path
        self.hls_dir = hls_dir
        self.hls_time = hls_time
        self.hls_list_size = hls_list_size
        self.loglevel = loglevel
        self.loop = loop
        self.on_finished = on_finished
        self.video_preset = video_preset
        self.crf = crf
        self.ffmpeg_threads = ffmpeg_threads
        self.x264_threads = x264_threads
        self.audio_bitrate = audio_bitrate
        self.show_next = show_next

        self._enc_proc = None   # current encoder process
        self._hls_proc = None   # HLS segmenter process
        self._thread = None
        self._stop_event = threading.Event()
        self._started_at = None
        self._current_title = ""

    def start(self):
        os.makedirs(self.hls_dir, exist_ok=True)
        for f in os.listdir(self.hls_dir):
            if f.endswith(".ts") or f.endswith(".m3u8"):
                os.remove(os.path.join(self.hls_dir, f))

        self._stop_event.clear()
        self._started_at = time.time()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logging.info("[STREAM] Started channel %s", self.channel_id)

    def _read_file_list(self) -> list:
        """Parse concat file with type metadata. Returns list of dicts."""
        entries = []
        current_type = "content"
        try:
            with open(self.concat_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("# type="):
                        current_type = line[7:]
                    elif line.startswith("file '"):
                        path = line[6:-1].replace("'\\''", "'")
                        entries.append({"path": path, "type": current_type})
                        current_type = "content"
        except Exception as e:
            logging.error("[STREAM] Failed to read concat file: %s", e)
        return entries

    def _get_duration(self, filepath: str) -> float:
        """Probe file duration in seconds via ffprobe."""
        try:
            result = subprocess.run(
                ["ffprobe", "-v", "quiet", "-print_format", "json",
                 "-show_format", filepath],
                capture_output=True, text=True, timeout=15,
            )
            info = json.loads(result.stdout)
            return float(info["format"]["duration"])
        except Exception as e:
            logging.warning("[STREAM] ffprobe failed for %s: %s", filepath, e)
            return 0.0

    @staticmethod
    def _find_next_content(entries: list, current_idx: int) -> dict | None:
        """Find the next non-bump entry after current_idx."""
        for j in range(current_idx + 1, len(entries)):
            if entries[j]["type"] != "bump":
                return entries[j]
        return None

    @staticmethod
    def _wrap_title(text: str, max_chars: int = 28) -> list:
        """Word-wrap text into lines of max_chars for FFmpeg drawtext."""
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
        return lines[:4]  # max 4 lines

    def _build_encoder_cmd(self, filepath: str, ts_offset: float = 0.0,
                           is_bump: bool = False, bump_duration: float = 0.0,
                           next_title: str = "", next_poster: str = "") -> list:
        """Build FFmpeg command to encode a single file to MPEG-TS on stdout.

        ts_offset chains timestamps across files so the HLS segmenter sees
        a continuous, monotonically increasing PTS stream.
        When is_bump=True, a countdown overlay is burned into the video.
        When next_title is set, an "Up Next" overlay is shown during bumps.
        When next_poster is set, the poster image is shown alongside.
        """
        font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        font_bold = font
        base_vf = (
            "scale=1920:1080:force_original_aspect_ratio=decrease,"
            "pad=1920:1080:(ow-iw)/2:(oh-ih)/2,"
            "format=yuv420p"
        )

        use_poster = bool(is_bump and next_title and next_poster and os.path.isfile(next_poster))

        if is_bump and bump_duration > 0:
            dur = f"{bump_duration:.2f}"
            # Countdown timer
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

            # "Up Next" text overlay
            if next_title:
                # Escape special chars for FFmpeg drawtext
                safe_title = next_title.replace("'", "\u2019").replace(":", "\\:").replace("\\", "\\\\")
                if use_poster:
                    # With poster: text to the right of poster (200x300 poster at x=55,y=50)
                    # Text area starts at x=280, box is 600px wide total
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
                    # No poster: centered text with wrapping
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
            # Two inputs: bump video + poster image
            # filter_complex: scale bump, overlay scaled poster (200x300)
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
                "-i", filepath,
                "-i", next_poster,
                "-filter_complex", poster_filter,
                "-map", "0:a:0",
                "-r", "30",
                "-c:v", "libx264",
                "-x264-params", f"threads={self.x264_threads}",
                "-preset", self.video_preset,
                "-profile:v", "high",
                "-force_key_frames", f"expr:gte(t,n_forced*{self.hls_time})",
                "-c:a", "aac",
                "-b:a", self.audio_bitrate,
                "-ac", "2",
                "-ar", "48000",
                "-output_ts_offset", f"{ts_offset:.3f}",
                "-f", "mpegts",
                "pipe:1",
            ]
        else:
            cmd = [
                "ffmpeg", "-y",
                "-threads", self.ffmpeg_threads,
                "-loglevel", self.loglevel,
                "-re",
                "-i", filepath,
                "-map", "0:v:0", "-map", "0:a:0",
                "-vf", vf,
                "-r", "30",
                "-c:v", "libx264",
                "-x264-params", f"threads={self.x264_threads}",
                "-preset", self.video_preset,
                "-profile:v", "high",
                "-force_key_frames", f"expr:gte(t,n_forced*{self.hls_time})",
                "-c:a", "aac",
                "-b:a", self.audio_bitrate,
                "-ac", "2",
                "-ar", "48000",
                "-output_ts_offset", f"{ts_offset:.3f}",
                "-f", "mpegts",
                "pipe:1",
            ]
        if self.crf:
            idx = cmd.index("-profile:v")
            cmd.insert(idx, self.crf)
            cmd.insert(idx, "-crf")
        return cmd

    def _build_hls_cmd(self) -> list:
        """Build FFmpeg command to segment MPEG-TS from stdin into HLS."""
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

    def _run_loop(self):
        while not self._stop_event.is_set():
            entries = self._read_file_list()
            if not entries:
                logging.error("[STREAM] No files for channel %s", self.channel_id)
                break

            # Start HLS segmenter — reads from stdin, writes .ts + .m3u8
            hls_cmd = self._build_hls_cmd()
            logging.info("[STREAM] HLS segmenter: %s", " ".join(hls_cmd))
            self._hls_proc = subprocess.Popen(
                hls_cmd, stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            )

            # Feed each file through an encoder into the segmenter pipe.
            # Track elapsed wall-clock time to chain PTS timestamps across files.
            ts_offset = 0.0
            for i, entry in enumerate(entries):
                if self._stop_event.is_set():
                    break
                filepath = entry["path"]
                is_bump = entry["type"] == "bump"
                self._current_title = os.path.basename(filepath)
                logging.info("[STREAM] Channel %s [%d/%d] (offset=%.1fs%s): %s",
                             self.channel_id, i + 1, len(entries), ts_offset,
                             ", bump" if is_bump else "",
                             self._current_title)

                bump_duration = 0.0
                next_title = ""
                next_poster = ""
                if is_bump:
                    bump_duration = self._get_duration(filepath)
                    logging.info("[STREAM] Bump duration: %.1fs", bump_duration)
                    # Look ahead for "Up Next" overlay
                    if self.show_next:
                        next_content = self._find_next_content(entries, i)
                        if next_content:
                            next_title = read_nfo_title(next_content["path"])
                            next_poster = find_poster(next_content["path"]) or ""
                            logging.info("[STREAM] Up next: %s (poster: %s)",
                                         next_title, "yes" if next_poster else "no")

                enc_cmd = self._build_encoder_cmd(filepath, ts_offset,
                                                  is_bump=is_bump,
                                                  bump_duration=bump_duration,
                                                  next_title=next_title,
                                                  next_poster=next_poster)
                file_start = time.time()
                try:
                    self._enc_proc = subprocess.Popen(
                        enc_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    )
                    # Pump encoded TS data into the HLS segmenter
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
                        # Advance offset by actual elapsed time (≈ media duration with -re)
                        ts_offset += file_elapsed
                        logging.info("[STREAM] Finished [%s] in %.1fs, next offset=%.1fs",
                                     self._current_title, file_elapsed, ts_offset)
                    self._enc_proc.stderr.close()
                except Exception as e:
                    logging.error("[STREAM] Encoder failed for %s: %s", self._current_title, e)
                finally:
                    self._enc_proc = None

            # Close pipe to end the HLS segmenter
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
                if self.on_finished:
                    new_concat = self.on_finished(self.channel_id)
                    if new_concat:
                        self.concat_path = new_concat
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
        logging.info("[STREAM] Stopped channel %s", self.channel_id)

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
        self._streams = {}

    def start_channel(self, channel_id: str, concat_path: str, loop: bool = True,
                      on_finished=None, show_next: bool = False) -> bool:
        if channel_id in self._streams:
            if self._streams[channel_id].status()["running"]:
                return False

        hls_base = self._get("HLS_OUTPUT_PATH", "/app/data/hls")
        hls_dir = os.path.join(hls_base, channel_id)

        stream = ChannelStream(
            channel_id=channel_id,
            concat_path=concat_path,
            hls_dir=hls_dir,
            hls_time=int(self._get("HLS_TIME", "6")),
            hls_list_size=int(self._get("HLS_LIST_SIZE", "10")),
            loglevel=self._get("FFMPEG_LOGLEVEL", "warning"),
            loop=loop,
            on_finished=on_finished,
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

    def get_status(self, channel_id: str) -> dict:
        stream = self._streams.get(channel_id)
        if not stream:
            return {"running": False, "uptime": 0}
        return stream.status()

    def get_all_status(self) -> dict:
        return {cid: s.status() for cid, s in self._streams.items()}

    def running_count(self) -> int:
        return sum(1 for s in self._streams.values() if s.status()["running"])
