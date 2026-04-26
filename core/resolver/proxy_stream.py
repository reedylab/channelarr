"""Proxy streaming mode for resolved channels.

Downloads upstream HLS segments with proper auth headers (Referer, etc)
and serves them from channelarr's local HLS directory. No re-encode —
segments are copied byte-for-byte. The client sees clean local URLs that
ffprobe and any player can handle without CDN auth issues.

Architecture:
  [poller thread] → polls upstream variant playlist
    → for each new segment: download to /app/data/hls/{channel_id}/
    → rewrite playlist with local segment filenames
    → serve via the existing /live/{channel_id}/ HLS endpoint
"""

import logging
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

import requests as http_requests

logger = logging.getLogger(__name__)

POLL_INTERVAL = 2  # seconds between playlist polls
MAX_SEGMENTS_ON_DISK = 10  # rolling window of segment files to keep


class ProxyStream:
    """Proxy streamer for one resolved channel. Polls upstream, downloads
    segments, writes a local HLS playlist. No encoding."""

    def __init__(
        self,
        channel_id: str,
        manifest_id: str,
        manifest_url: str,
        hls_dir: str,
        *,
        hls_time: int = 6,
        hls_list_size: int = 10,
    ):
        self.channel_id = channel_id
        self.manifest_id = manifest_id
        self.manifest_url = manifest_url
        self.hls_dir = hls_dir
        self.hls_time = hls_time
        self.hls_list_size = hls_list_size

        self._poller_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started_at: Optional[float] = None
        self._last_access = time.time()

        # Look up source_domain for Referer headers
        self.source_domain = ""
        try:
            from core.database import get_session
            from core.models.manifest import Manifest as _M
            with get_session() as _s:
                _row = _s.query(_M.source_domain).filter_by(id=manifest_id).first()
                self.source_domain = (_row[0] if _row and _row[0] else "") or ""
        except Exception:
            pass

    def _upstream_headers(self) -> dict:
        h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
        if self.source_domain:
            h["Referer"] = f"https://{self.source_domain}/"
            h["Origin"] = f"https://{self.source_domain}"
        return h

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
            target=self._poller_loop, daemon=True,
            name=f"proxy-poller-{self.channel_id}",
        )
        self._poller_thread.start()
        logging.info("[PROXY] Started channel %s (manifest=%s)",
                     self.channel_id, self.manifest_id)

    def status(self) -> dict:
        alive = self._poller_thread is not None and self._poller_thread.is_alive()
        uptime = 0
        if self._started_at and alive:
            uptime = int(time.time() - self._started_at)
        return {
            "running": alive,
            "uptime": uptime,
            "now_playing": "Live (proxy)" if alive else "",
        }

    def stop(self):
        self._stop_event.set()
        if self._poller_thread:
            self._poller_thread.join(timeout=10)
        self._clean_hls_dir()
        logging.info("[PROXY] Stopped channel %s", self.channel_id)

    def _clean_hls_dir(self):
        if not os.path.isdir(self.hls_dir):
            return
        for f in os.listdir(self.hls_dir):
            if f.endswith(".ts") or f.endswith(".m3u8"):
                try:
                    os.remove(os.path.join(self.hls_dir, f))
                except OSError:
                    pass

    # ── Variant resolution ─────────────────────────────────────────────────

    def _resolve_variant_url(self, url: str) -> str:
        try:
            resp = http_requests.get(url, headers=self._upstream_headers(), timeout=10)
            text = resp.text
        except Exception:
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

    # ── Manifest refresh ───────────────────────────────────────────────────

    def _refresh_manifest_url(self) -> Optional[str]:
        try:
            from core.resolver.manifest_resolver import ManifestResolverService
            from core.database import get_session
            from core.models.manifest import Manifest
            result = ManifestResolverService.refresh_manifest(self.manifest_id)
            if not result.get("ok"):
                return None
            with get_session() as session:
                row = session.query(Manifest.url).filter(Manifest.id == self.manifest_id).first()
            return row[0] if row else None
        except Exception:
            return None

    # ── Poller loop ────────────────────────────────────────────────────────

    def _poller_loop(self):
        try:
            self._poller_loop_inner()
        finally:
            self._clean_hls_dir()

    def _poller_loop_inner(self):
        seen_uris: set[str] = set()
        consecutive_errors = 0
        local_seq = 0  # our own sequence counter for the local playlist
        segment_files: list[tuple[int, str, float]] = []  # (local_seq, filename, duration)

        variant_url = self._resolve_variant_url(self.manifest_url)
        logging.info("[PROXY] %s polling variant: %s",
                     self.channel_id, variant_url[:120])

        while not self._stop_event.is_set():
            try:
                resp = http_requests.get(
                    variant_url, headers=self._upstream_headers(), timeout=10
                )
                if resp.status_code in (401, 403, 404):
                    consecutive_errors += 1
                    if consecutive_errors > 3:
                        logging.error("[PROXY] %s giving up after %d consecutive errors",
                                      self.channel_id, consecutive_errors)
                        self._stop_event.set()
                        break
                    logging.warning("[PROXY] %s variant HTTP %d (#%d) — refreshing",
                                    self.channel_id, resp.status_code, consecutive_errors)
                    fresh = self._refresh_manifest_url()
                    if fresh:
                        self.manifest_url = fresh
                        variant_url = self._resolve_variant_url(fresh)
                        consecutive_errors = 0
                    else:
                        self._stop_event.wait(10)
                    continue
                if resp.status_code != 200:
                    self._stop_event.wait(POLL_INTERVAL)
                    continue
                consecutive_errors = 0
            except Exception as e:
                logging.warning("[PROXY] %s playlist fetch failed: %s",
                                self.channel_id, e)
                self._stop_event.wait(POLL_INTERVAL)
                continue

            # Parse segments from the playlist
            lines = resp.text.splitlines()
            segments = []
            current_duration = 0.0
            current_key = None
            pending_discontinuity = False
            for line in lines:
                line = line.strip()
                if line.startswith("#EXT-X-KEY:"):
                    current_key = line
                elif line == "#EXT-X-DISCONTINUITY":
                    # Attach to the next segment we see. DAI inserts this
                    # between live content and ad pods and between pods;
                    # dropping it causes downstream decoders to carry PCR/
                    # PTS/codec state across the boundary and stall or die.
                    pending_discontinuity = True
                elif line.startswith("#EXTINF:"):
                    try:
                        current_duration = float(line.split(":")[1].split(",")[0])
                    except (ValueError, IndexError):
                        current_duration = self.hls_time
                elif line and not line.startswith("#"):
                    uri = urljoin(resp.url, line)
                    seq_match = re.search(r'(\d+)\.ts', line)
                    seq = int(seq_match.group(1)) if seq_match else hash(line)
                    segments.append({
                        "uri": uri,
                        "seq": seq,
                        "duration": current_duration,
                        "key_line": current_key,
                        "discontinuity": pending_discontinuity,
                    })
                    pending_discontinuity = False

            # First poll optimization: DAI/HLS live playlists can advertise
            # hours of DVR backlog (thousands of segments). We only need the
            # live edge. Seed seen_uris with every segment EXCEPT the last N
            # positions so the download loop processes at most hls_list_size
            # segments and writes the first playlist within seconds instead
            # of tens of minutes. Using full URI as the dedup key (not the
            # sequence number) avoids collisions — DAI ad pods/slates reuse
            # small seq numbers like 0,1,2,3 across pods, which a seq-based
            # dedup would collapse into a single entry.
            if not seen_uris and len(segments) > self.hls_list_size:
                cutoff = len(segments) - self.hls_list_size
                for seg in segments[:cutoff]:
                    seen_uris.add(seg["uri"])
                logging.info("[PROXY] %s seeded past %d backlog segments; will grab the last %d",
                             self.channel_id, cutoff, self.hls_list_size)

            # Download new segments
            new_count = 0
            for seg in segments:
                uri = seg["uri"]
                if uri in seen_uris:
                    continue
                seen_uris.add(uri)

                try:
                    local_filename = f"seg_{local_seq:05d}.ts"
                    local_path = os.path.join(self.hls_dir, local_filename)
                    self._download_segment(seg, local_path)
                    segment_files.append((local_seq, local_filename, seg["duration"], seg.get("discontinuity", False)))
                    local_seq += 1
                    new_count += 1
                except Exception as e:
                    logging.warning("[PROXY] %s download failed for %s: %s",
                                    self.channel_id, uri[:80], e)

            # Trim old segments and write playlist
            if segment_files:
                # Keep only the last N segments on disk
                while len(segment_files) > MAX_SEGMENTS_ON_DISK:
                    _, old_file, _, _ = segment_files.pop(0)
                    old_path = os.path.join(self.hls_dir, old_file)
                    try:
                        os.remove(old_path)
                    except OSError:
                        pass

                self._write_playlist(segment_files)

            if new_count:
                logging.info("[PROXY] %s downloaded %d segment(s), total on disk: %d",
                             self.channel_id, new_count, len(segment_files))

            # Prune seen_uris to avoid unbounded growth — keep only URIs that
            # are still advertised in the current playlist, plus a small buffer.
            if len(seen_uris) > 4000:
                current_uris = {s["uri"] for s in segments}
                seen_uris = seen_uris & current_uris

            self._stop_event.wait(POLL_INTERVAL)

    def _download_segment(self, seg: dict, local_path: str):
        """Download a single segment to local disk."""
        headers = self._upstream_headers()
        resp = http_requests.get(seg["uri"], headers=headers, timeout=15, stream=True)
        resp.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)

    def _write_playlist(self, segment_files: list[tuple[int, str, float, bool]]):
        """Write a local HLS playlist from the current segment list.

        Each entry is (local_seq, filename, duration, discontinuity_before).
        Emits #EXT-X-DISCONTINUITY before any segment where the upstream
        playlist had one — DAI places these between content and ad pods,
        and between pods, because codec/PCR/PTS state doesn't carry across.
        """
        playlist_path = os.path.join(self.hls_dir, "stream.m3u8")
        first_seq = segment_files[0][0]
        max_dur = max(d for _, _, d, _ in segment_files)

        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{int(max_dur) + 1}",
            f"#EXT-X-MEDIA-SEQUENCE:{first_seq}",
        ]
        # First segment never gets a leading discontinuity — the playlist
        # itself is already a fresh load boundary for the player.
        for idx, (_, filename, duration, disc) in enumerate(segment_files):
            if disc and idx > 0:
                lines.append("#EXT-X-DISCONTINUITY")
            lines.append(f"#EXTINF:{duration:.3f},")
            lines.append(filename)

        with open(playlist_path, "w") as f:
            f.write("\n".join(lines) + "\n")
