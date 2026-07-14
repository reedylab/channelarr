"""Remux-mode resolved channel: gentle Python mirror + per-segment ffmpeg mux.

For upstream HLS the lightweight ProxyStream can't serve — fMP4/CMAF with a
separate ``#EXT-X-MAP`` init and/or demuxed audio in its own rendition.

Two problems this design solves:
  1. Some CDNs return *deliberately-scrambled* bytes to bot-like requests.
     Fetched like a real player — full browser + Sec-Fetch headers, and NO
     bursts (one segment per rendition per poll, paced ~1x realtime), and after
     hitting the stream's entry redirector to establish a blessed session — the
     same CDN serves clean, decodable bytes. So Python does all the fetching;
     ffmpeg never touches the network.
  2. ffmpeg reading a *live growing* HLS as input is fragile (lag, window
     slide). Instead we mux ONE aligned (video, audio) segment pair at a time
     with a short finite ffmpeg ``-c copy`` — the exact finite operation that
     decodes clean — and write the output HLS playlist ourselves. No
     long-running network-facing ffmpeg at all.

Output at ``/live/{id}/`` (same dir the other modes use). Same lifecycle
surface: start/stop/touch/last_access/status.
"""

import logging
import os
import re
import shutil
import subprocess
import threading
import time
from collections import deque
from typing import Optional
from urllib.parse import urljoin

import requests as _requests

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


class RemuxStream:
    def __init__(self, channel_id, manifest_id, manifest_url, hls_dir, *,
                 hls_time=6, hls_list_size=10, loglevel="warning"):
        self.channel_id = channel_id
        self.manifest_id = manifest_id
        self.manifest_url = manifest_url
        self.hls_dir = hls_dir
        self.src_dir = os.path.join(hls_dir, "src")
        self.hls_list_size = hls_list_size
        self.loglevel = loglevel

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started_at: Optional[float] = None
        self._last_access = time.time()
        self._producing = False

        self.session = _requests.Session()
        self.source_domain = ""
        self.page_url = ""
        self._load_context()
        # Some CDNs only serve clean (unscrambled) segment bytes to a session
        # that first hit the stream's *entry* URL (the short redirector the
        # player loads) — hitting the resolved master directly reads as a
        # scraper. Read that entry out of the capture page so we can "bless" the
        # session at startup and periodically.
        self.entry_url = self._resolve_entry()

    def _load_context(self):
        try:
            from core.database import get_session
            from core.models.manifest import Manifest, Capture
            with get_session() as s:
                row = (s.query(Manifest.source_domain, Capture.page_url)
                       .outerjoin(Capture, Capture.id == Manifest.capture_id)
                       .filter(Manifest.id == self.manifest_id).first())
                if row:
                    self.source_domain = row[0] or ""
                    self.page_url = row[1] or ""
        except Exception:
            pass

    def _resolve_entry(self):
        """Find the stream ENTRY redirector the player hits before the master —
        hitting it blesses the session (hitting the master directly reads as a
        scraper → scrambled bytes). Generic: reads the entry template out of the
        capture page's own JS (24/7 pages and live-event pages point at
        different entry hosts), following ONE hop through an event page's stream
        source. Returns None if the page carries no such entry."""
        if not self.page_url:
            return None
        try:
            r = self.session.get(self.page_url, headers=self._headers(), timeout=12)
            entry = self._extract_entry(r.text, r.url)
            if entry:
                return entry
            # Event pages carry a streams array whose first source is itself a
            # player page (which holds the entry). Hop to it once.
            for m in re.finditer(r'''source:\s*["']([^"']+)["']''', r.text):
                src = m.group(1)
                if "streamid=" in src or "stream_id=" in src:
                    try:
                        r2 = self.session.get(urljoin(r.url, src), headers=self._headers(), timeout=12)
                        e2 = self._extract_entry(r2.text, r2.url)
                        if e2:
                            return e2
                    except Exception:
                        pass
        except Exception as e:
            logging.warning("[REMUX] %s entry resolve failed: %s", self.channel_id, e)
        return None

    @staticmethod
    def _extract_entry(html, base):
        """Pull the entry URL out of a player page. Its JS holds a template like
        ``https://<entry-host>/?stream_id=${..}&pro_id=${..}&index.m3u8``; the
        stream_id/pro_id come from THIS page's own query string."""
        from urllib.parse import urlparse, parse_qs
        m = re.search(r'''(https?://[^"'`\s?]+/)\?stream_id=\$\{''', html)
        if not m:
            return None
        q = parse_qs(urlparse(base).query)
        sid = (q.get("streamid") or q.get("stream_id") or [None])[0]
        pid = (q.get("proid") or q.get("pro_id") or [""])[0]
        if not sid:
            return None
        return f"{m.group(1)}?stream_id={sid}&pro_id={pid}&index.m3u8"

    def _headers(self) -> dict:
        # Sec-Fetch-* is the browser-authenticity signal the CDN checks — without
        # it (plus the paced, burst-free fetch below) it serves scrambled bytes.
        h = {
            "User-Agent": _UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9",
            "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-site",
        }
        if self.source_domain:
            h["Referer"] = f"https://{self.source_domain}/"
            h["Origin"] = f"https://{self.source_domain}"
        return h

    # ── lifecycle interface ─────────────────────────────────────────────────
    def touch(self):
        self._last_access = time.time()

    @property
    def last_access(self) -> float:
        return self._last_access

    def status(self) -> dict:
        alive = (self._thread is not None and self._thread.is_alive()
                 and not self._stop_event.is_set())
        uptime = int(time.time() - self._started_at) if (self._started_at and alive) else 0
        return {"running": alive, "uptime": uptime,
                "now_playing": "Live (remux)" if alive else ""}

    def start(self):
        os.makedirs(self.hls_dir, exist_ok=True)
        self._clean_all()
        os.makedirs(self.src_dir, exist_ok=True)
        self._stop_event.clear()
        self._started_at = time.time()
        self._last_access = time.time()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"remux-{self.channel_id}")
        self._thread.start()
        logging.info("[REMUX] Started channel %s (manifest=%s)", self.channel_id, self.manifest_id)

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=15)
        self._clean_all()
        logging.info("[REMUX] Stopped channel %s", self.channel_id)

    # ── internals ────────────────────────────────────────────────────────────
    def _clean_all(self):
        for f in (os.listdir(self.hls_dir) if os.path.isdir(self.hls_dir) else []):
            if f.endswith((".ts", ".m3u8")):
                try:
                    os.remove(os.path.join(self.hls_dir, f))
                except OSError:
                    pass
        shutil.rmtree(self.src_dir, ignore_errors=True)

    def _resolve_inputs(self):
        """(video_playlist_url, audio_playlist_url_or_None). Fetches through the
        entry URL when known (which blesses the session AND 302-redirects to a
        fresh master), else the stored master directly."""
        url = self.entry_url or self.manifest_url
        try:
            r = self.session.get(url, headers=self._headers(), timeout=12, allow_redirects=True)
            body, base = r.text, r.url
        except Exception as e:
            logging.warning("[REMUX] %s master fetch failed: %s", self.channel_id, e)
            return self.manifest_url, None
        if "#EXT-X-STREAM-INF" not in body:
            return base, None
        lines = body.splitlines()
        best_uri, best_bw = None, -1
        for i, l in enumerate(lines):
            if l.startswith("#EXT-X-STREAM-INF"):
                mbw = re.search(r"BANDWIDTH=(\d+)", l)
                bw = int(mbw.group(1)) if mbw else 0
                uri = next((x.strip() for x in lines[i + 1:i + 4]
                            if x.strip() and not x.startswith("#")), None)
                if uri and bw > best_bw:
                    best_bw, best_uri = bw, uri
        audio = None
        for l in lines:
            if l.startswith("#EXT-X-MEDIA") and "TYPE=AUDIO" in l:
                mm = re.search(r'URI="([^"]+)"', l)
                if mm:
                    audio = urljoin(base, mm.group(1))
                    if "DEFAULT=YES" in l:
                        break
        return (urljoin(base, best_uri) if best_uri else base), audio

    def _refresh_manifest(self):
        try:
            from core.resolver.manifest_resolver import ManifestResolverService
            ManifestResolverService.refresh_manifest(self.manifest_id)
        except Exception as e:
            logging.warning("[REMUX] %s manifest refresh failed: %s", self.channel_id, e)
        try:
            from core.database import get_session
            from core.models.manifest import Manifest
            with get_session() as s:
                row = s.query(Manifest.url, Manifest.source_domain).filter_by(
                    id=self.manifest_id).first()
                if row:
                    if row[0]:
                        self.manifest_url = row[0]
                    if row[1]:
                        self.source_domain = row[1]
        except Exception:
            pass

    def _get_playlist(self, url):
        """Return (status, base, media_seq, init_uri|None, [(seg_uri, dur)], targetdur).

        targetdur is EXT-X-TARGETDURATION — the player-standard playlist RELOAD
        interval. Reloading faster than this (e.g. at the shorter EXTINF) reads
        as a bot and re-trips the CDN scramble; reloading at targetdur stays
        clean and still keeps up (each reload reveals the new segments)."""
        try:
            r = self.session.get(url, headers=self._headers(), timeout=12)
        except Exception as e:
            logging.warning("[REMUX] %s playlist fetch error: %s", self.channel_id, e)
            return 0, None, 0, None, [], 3.0
        if r.status_code != 200:
            return r.status_code, None, 0, None, [], 3.0
        base, text = r.url, r.text
        msq = 0
        m = re.search(r"#EXT-X-MEDIA-SEQUENCE:(\d+)", text)
        if m:
            msq = int(m.group(1))
        targetdur = 3.0
        mt = re.search(r"#EXT-X-TARGETDURATION:(\d+)", text)
        if mt:
            targetdur = max(1.0, float(mt.group(1)))
        init = None
        mm = re.search(r'#EXT-X-MAP:URI="([^"]+)"', text)
        if mm:
            init = urljoin(base, mm.group(1))
        segs = []
        dur = 6.0
        for line in text.splitlines():
            s = line.strip()
            if s.startswith("#EXTINF:"):
                try:
                    dur = float(s[8:].split(",")[0])
                except ValueError:
                    dur = 6.0
            elif s and not s.startswith("#"):
                segs.append((urljoin(base, s), dur))
        return 200, base, msq, init, segs, targetdur

    def _dl(self, url) -> bytes:
        return self.session.get(url, headers=self._headers(), timeout=15).content

    def _write_output_playlist(self, out_segs, seq0, target):
        lines = ["#EXTM3U", "#EXT-X-VERSION:3", f"#EXT-X-TARGETDURATION:{int(target) + 1}",
                 f"#EXT-X-MEDIA-SEQUENCE:{seq0}", "#EXT-X-ALLOW-CACHE:NO"]
        for name, dur in out_segs:
            lines.append(f"#EXTINF:{dur:.3f},")
            lines.append(name)
        tmp = os.path.join(self.hls_dir, "stream.m3u8.tmp")
        with open(tmp, "w") as f:
            f.write("\n".join(lines) + "\n")
        os.replace(tmp, os.path.join(self.hls_dir, "stream.m3u8"))

    def _mux_pair(self, v_init, v_data, a_init, a_data, out_path) -> bool:
        """Mux one aligned (video, audio) fMP4 pair to one MPEG-TS output
        segment with -c copy (the proven-clean finite operation)."""
        vp = os.path.join(self.src_dir, "v.mp4")
        with open(vp, "wb") as f:
            f.write(v_init + v_data)
        cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", vp]
        if a_init is not None and a_data is not None:
            ap = os.path.join(self.src_dir, "a.mp4")
            with open(ap, "wb") as f:
                f.write(a_init + a_data)
            cmd += ["-i", ap, "-map", "0:v:0", "-map", "1:a:0"]
        else:
            cmd += ["-map", "0"]
        # -copyts keeps the original (continuous) PTS so output segments
        # concatenate smoothly for the player.
        cmd += ["-c", "copy", "-sn", "-dn", "-copyts", "-muxpreload", "0", "-muxdelay", "0",
                "-f", "mpegts", out_path]
        try:
            r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=20)
            return r.returncode == 0 and os.path.isfile(out_path) and os.path.getsize(out_path) > 0
        except Exception as e:
            logging.warning("[REMUX] %s mux failed: %s", self.channel_id, e)
            return False

    def _run(self):
        vurl, aurl = self._resolve_inputs()
        v_init = a_init = None
        vbuf: dict = {}                       # media-seq -> video bytes
        abuf: dict = {}                       # media-seq -> audio bytes
        adur: dict = {}                       # media-seq -> duration
        emit = None                           # next media-seq to mux/emit
        out_segs: deque = deque()             # (name, dur) output window
        out_seq = 0
        target = 3.0

        def _download_new(url, buf, dur_map, is_video):
            """Reload one rendition's playlist and download every NEW segment in
            it (media-seq >= emit and not already buffered). Returns status."""
            nonlocal v_init, a_init
            st, base, msq, init_u, segs, _tgt = self._get_playlist(url)
            if st != 200:
                return st
            if is_video and v_init is None and init_u:
                try:
                    v_init = self._dl(init_u)
                except Exception:
                    pass
            if (not is_video) and a_init is None and init_u:
                try:
                    a_init = self._dl(init_u)
                except Exception:
                    pass
            for i, (seg_url, dur) in enumerate(segs):
                seq = msq + i
                if emit is not None and seq < emit:
                    continue
                if seq in buf:
                    continue
                try:
                    buf[seq] = self._dl(seg_url)          # fetch every new one, like a player
                    if dur_map is not None:
                        dur_map[seq] = dur
                except Exception as e:
                    logging.warning("[REMUX] %s seg dl failed: %s", self.channel_id, e)
            return 200

        last_bless = time.time()
        while not self._stop_event.is_set():
            cycle_start = time.time()

            # Re-hit the entry URL periodically to keep the session blessed. Just
            # a bless GET — do NOT re-resolve/swap the variant URLs (that jumps
            # the media-sequence and strands the emit pointer). Blessing lapses
            # after ~30s.
            if self.entry_url and cycle_start - last_bless > 20:
                try:
                    self.session.get(self.entry_url, headers=self._headers(), timeout=10)
                except Exception:
                    pass
                last_bless = cycle_start

            sv, vbase, vmsq, vinit_u, vsegs, reload_int = self._get_playlist(vurl)
            if sv in (403, 404, 410):
                logging.info("[REMUX] %s token expired — refreshing", self.channel_id)
                self._refresh_manifest()
                vurl, aurl = self._resolve_inputs()
                v_init = a_init = None
                vbuf.clear(); abuf.clear(); emit = None
                if self._stop_event.wait(2):
                    break
                continue
            if sv != 200 or not vsegs:
                if self._stop_event.wait(3):
                    break
                continue

            if vsegs[0][1]:
                target = vsegs[0][1] if vsegs[0][1] >= 1 else 3.0
            # Start a few segments behind the live edge for a small buffer.
            if emit is None:
                emit = vmsq + max(0, len(vsegs) - 4)
            elif emit < vmsq:
                # Fell behind the live window (a slow cycle / token refresh gap):
                # the segment we want has rolled off. Resync to what's available
                # so we resume instead of stalling forever.
                logging.info("[REMUX] %s resync emit %d -> %d", self.channel_id, emit, vmsq)
                emit = vmsq
                vbuf.clear(); abuf.clear(); adur.clear()

            # Fetch every NEW segment this reload reveals (keeps up with live),
            # both renditions — paced to the reload interval below.
            if v_init is None and vinit_u:
                try:
                    v_init = self._dl(vinit_u)
                except Exception:
                    pass
            for i, (seg_url, dur) in enumerate(vsegs):
                seq = vmsq + i
                if seq < emit or seq in vbuf:
                    continue
                try:
                    vbuf[seq] = self._dl(seg_url)
                    adur[seq] = dur
                except Exception as e:
                    logging.warning("[REMUX] %s video dl failed: %s", self.channel_id, e)
            if aurl:
                sta = _download_new(aurl, abuf, None, is_video=False)
                if sta in (403, 404, 410):
                    self._refresh_manifest()
                    vurl, aurl = self._resolve_inputs()
                    v_init = a_init = None
                    vbuf.clear(); abuf.clear(); emit = None
                    if self._stop_event.wait(2):
                        break
                    continue

            # Emit every buffered sequence whose (video, audio) pair is ready.
            while emit in vbuf and (aurl is None or emit in abuf) and not self._stop_event.is_set():
                out_name = f"seg_{out_seq:05d}.ts"
                out_path = os.path.join(self.hls_dir, out_name)
                ok = self._mux_pair(v_init, vbuf.pop(emit),
                                    a_init if aurl else None,
                                    abuf.pop(emit, None) if aurl else None, out_path)
                dur = adur.pop(emit, target)
                emit += 1
                if not ok:
                    continue
                out_segs.append((out_name, dur))
                out_seq += 1
                seq0 = out_seq - len(out_segs)
                while len(out_segs) > self.hls_list_size:
                    old, _ = out_segs.popleft()
                    try:
                        os.remove(os.path.join(self.hls_dir, old))
                    except OSError:
                        pass
                    seq0 += 1
                self._write_output_playlist(out_segs, seq0, target)
                self._producing = True

            # Drop anything stale we somehow skipped.
            for d in (vbuf, abuf, adur):
                for k in [k for k in d if k < emit]:
                    d.pop(k, None)

            # Reload at the playlist TARGETDURATION (a player's cadence) — this
            # paced cadence + Sec-Fetch headers is what keeps the CDN unscrambled.
            elapsed = time.time() - cycle_start
            if self._stop_event.wait(max(1.0, reload_int - elapsed)):
                break
