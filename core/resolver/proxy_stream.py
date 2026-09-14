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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests as http_requests

from core.diagnostics import record_sample, record_event, incr_counter

logger = logging.getLogger(__name__)

POLL_INTERVAL = 2  # seconds between playlist polls
MAX_SEGMENTS_ON_DISK = 10  # rolling window of segment files to keep

# How many segments behind the upstream's live edge to seed on the very
# first poll, deliberately independent of hls_list_size (tunable per
# deployment for unrelated reasons, e.g. serving-window depth). Cold-start
# always downloads every segment newer than the seed point, serially,
# before the local playlist exists at all — so this needs to stay small
# regardless of hls_list_size, or a source with a deep live window (ad-
# insertion CDNs commonly keep 15+ segments vs. a plain live feed's 3-6)
# leaves the player with nothing for a minute or more. Tying this to
# hls_list_size used to mean the fast-path never engaged at all for such a
# source unless its window happened to exceed that (unrelated, often much
# larger) setting.
COLD_START_SEED_SEGMENTS = 4

# Even with a small, fast initial download batch, exposing a playlist the
# moment that batch lands leaves the player almost no runway to absorb this
# poller's inherently bursty landing cadence — POLL_INTERVAL (2s) is shorter
# than a typical segment duration (~4-6s), so roughly half of all polls find
# nothing new and the rest land 1-2 segments at once. With only
# COLD_START_SEED_SEGMENTS on hand that shows up as repeated buffer-underrun
# stalls and live-edge reseeks in the player during the first ~20-30s
# (2026-09-12). Holding the very first playlist write until this many
# segments have accumulated — which costs one or two extra poll cycles
# beyond the initial seed batch, not a return to the old full-backlog
# wait — trades a little more upfront latency for a much smoother start.
MIN_STARTUP_SEGMENTS = 6

# Trigger a manifest refresh after N consecutive segment-decrypt failures.
# Mid-stream decrypt failures mean the upstream session went stale (typically
# a VPN exit-IP rotation invalidated the IP-bound AES key endpoint) — the
# playlist URL still serves valid HLS, but every key fetched returns garbage.
# Without this, the proxy would log "Padding is incorrect" indefinitely until
# expires_at rolled around (up to 30 min). 3 failures × 2-3s per segment
# means ~6-10s of broken playback before we self-heal.
DECRYPT_FAILURES_BEFORE_REFRESH = 3
# Don't kick another refresh more often than this — gives the new manifest
# time to land + the player time to re-fetch the playlist.
DECRYPT_REFRESH_DEBOUNCE_SECONDS = 60

# Same idea, for plain segment-fetch HTTP errors (403/503/connection errors,
# not a decrypt failure) — a real, confirmed gap found live: these were only
# ever logged (the bare `except Exception` below), with NO escalation path
# at all. A CDN blocking/rate-limiting segment fetches (confirmed live: a
# backend returning 503 on every segment while the playlist itself
# kept resolving fine) meant the stream could sit at zero downloaded
# segments indefinitely -- never reaching MIN_STARTUP_SEGMENTS, never
# writing a playlist, the client just times out after 90s with no
# self-healing attempt in between. Mirrors the decrypt-failure pattern
# exactly: after N consecutive segment errors, try a fresh manifest (a new
# resolve may land on a different, currently-unblocked backend/token).
SEGMENT_ERRORS_BEFORE_REFRESH = 3
SEGMENT_REFRESH_DEBOUNCE_SECONDS = 60

# production_speed_ratio window — same concept as RemuxStream's (see there
# for the full rationale): sum(new segment duration)/wall-clock elapsed over
# a rolling window, not per-poll, so a burst of several segments landing at
# once doesn't produce a meaningless spike. Unlike remux, this loop already
# runs on a fixed POLL_INTERVAL regardless of whether anything new showed up,
# so the window is checked unconditionally every iteration — that's what
# lets a stall show up as a live, decaying ratio *during* the stall, not
# just as a low reading the moment it resolves.
PRODUCTION_WINDOW_SECONDS = 3.0
# No new segment for this long -> the upstream playlist itself has stopped
# advancing even though our fetches keep returning 200. fetch_latency_ms
# alone can't see this (each individual request is fine); it's exactly the
# blind spot that let a proxy-mode channel sit dark for ~57s while every
# diagnostic counter read clean (2026-09-12).
SOURCE_STALL_SECONDS = 15.0

# Segment fetches are deliberately sequential in steady state (see the
# poller loop below) — going through a defensive source's anti-bot
# checks one request at a time is what keeps this deployment welcome
# there at all. But that same seriality costs real time during a
# catch-up burst (cold start, or recovering from a stall), when several
# already-published segments need fetching before the local playlist can
# move again. Selective threading: only escalate to a small bounded pool
# when a single poll finds more than CATCHUP_THREAD_THRESHOLD new
# segments at once, AND the deployment has opted into "multi" concurrency
# mode (RESOLVER_CONCURRENCY_MODE — see manifest_resolver._concurrency_
# mode). Reusing that same flag means one setting controls the whole
# deployment's overall concurrency posture instead of two independent
# toggles to keep in sync — flip it once when pointed at a sidecar/
# source mix that can tolerate it. In the ordinary case (0-1 new segment
# per poll, or "single" mode) this changes nothing: same request count,
# same pacing, same steady-state traffic pattern as before.
CATCHUP_THREAD_THRESHOLD = 2
CATCHUP_MAX_WORKERS = 3


def _resolver_concurrency_mode() -> str:
    """Local import to avoid a module-load-order cycle between the
    resolver and streaming layers — see CATCHUP_THREAD_THRESHOLD."""
    try:
        from core.resolver.manifest_resolver import _concurrency_mode
        return _concurrency_mode()
    except Exception:
        return "single"


# Some CDNs prepend a fake image header (observed: a 36-byte RIFF/WebP
# header) before the real MPEG-TS sync bytes on an otherwise-plain segment —
# the same trick RemuxStream's forced `-f mpegts` demux already works around
# for its own download path (see remux_stream.py's _mux_pair). Proxy mode
# serves raw bytes straight through with no re-mux step, so a player that
# trusts the leading magic bytes over content (Jellyfin/ffprobe) sees a 1x1
# "webp image" instead of video and fails outright, even though hls.js in a
# browser tolerates it fine by resyncing past the garbage — exactly the
# "works in browser, fails completely in Jellyfin" split this produces.
_TS_PACKET_SIZE = 188
_TS_SYNC_BYTE = 0x47
_TS_DECOY_SCAN_WINDOW = 512  # observed decoy headers are tiny (~36B); this
                             # leaves generous room for other CDNs' variants
_TS_SYNC_CONFIRM_PACKETS = 8  # consecutive 188-byte-periodic sync bytes
                               # required before trusting a candidate offset


def _find_ts_sync_offset(data: bytes) -> Optional[int]:
    """Byte offset of the first confirmed MPEG-TS sync point, or None if the
    segment is too short to confirm one or genuinely isn't MPEG-TS."""
    span = _TS_SYNC_CONFIRM_PACKETS * _TS_PACKET_SIZE
    if len(data) < span:
        return None
    limit = min(len(data) - span, _TS_DECOY_SCAN_WINDOW)
    for pos in range(limit + 1):
        if data[pos] != _TS_SYNC_BYTE:
            continue
        if all(data[pos + i * _TS_PACKET_SIZE] == _TS_SYNC_BYTE
               for i in range(_TS_SYNC_CONFIRM_PACKETS)):
            return pos
    return None


def _strip_ts_decoy_prefix(channel_id: str, data: bytes) -> bytes:
    """Return `data` with any decoy header before the real MPEG-TS sync
    point removed. A segment that already starts clean (the common case) is
    returned untouched at negligible cost."""
    offset = _find_ts_sync_offset(data)
    if not offset:
        return data
    logging.info("[PROXY] %s stripped %d-byte decoy header before real MPEG-TS sync",
                 channel_id, offset)
    return data[offset:]


class _DecryptError(Exception):
    """Raised by ProxyStream._fetch_one_segment when AES decryption fails
    (wrong key / stale session). Distinguished from generic download errors
    so the poller loop can react with a manifest refresh instead of just
    retrying."""


def _pick_best_variant(text: str, base_url: str) -> str:
    """Given a fetched playlist's text, return the highest-bandwidth media
    playlist URL if `text` is a master playlist (#EXT-X-STREAM-INF present),
    or `base_url` unchanged if it's already a media playlist. Pure parsing,
    no network — factored out of ProxyStream._resolve_variant_url so
    core/resolver/segment_sampler.py can resolve a master playlist it
    already has the body for without re-fetching it."""
    if "#EXT-X-STREAM-INF" not in text:
        return base_url
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
        return urljoin(base_url, best_uri)
    return base_url


def _fetch_segment_bytes(session, seg: dict, headers: dict, get_key_fn, label: str) -> bytes:
    """Fetch (and, if AES-128 encrypted, decrypt) one segment's raw bytes,
    with the decoy-header strip already applied — everything
    ProxyStream._fetch_one_segment needs before writing to disk, factored
    out as a module-level function so core/resolver/segment_sampler.py can
    run the exact same fetch/decrypt/decoy-strip logic without a live
    ProxyStream instance (it just measures whether this succeeds and
    discards the result).

    get_key_fn(key_url) -> bytes|None is injected rather than assuming a
    ProxyStream instance — the live stream passes its own cached
    self._get_key; the sampler passes a throwaway one-off lookup. `label`
    is only used for the decoy-strip log line (channel_id for a live
    stream, something generic for a sample).
    """
    info = seg.get("key_info")
    # Encrypted: must buffer the full segment, then AES-CBC decrypt.
    # AES-CBC isn't streamable across an unknown total length without
    # also tracking padding; the segment is small enough (~2 MB) that
    # buffering is fine and avoids partial-write corruption.
    if info and info.get("method") == "AES-128" and info.get("uri"):
        resp = session.get(seg["uri"], headers=headers, timeout=15)
        resp.raise_for_status()
        ciphertext = resp.content
        key = get_key_fn(info["uri"])
        if key is None:
            raise RuntimeError(f"key fetch failed for {info['uri']}")
        iv = info.get("iv")
        if iv is None:
            # Fallback per HLS spec: media-sequence number padded to 128b
            iv = seg["seq"].to_bytes(16, "big", signed=False)
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import unpad
        cipher = AES.new(key, AES.MODE_CBC, iv)
        try:
            plaintext = unpad(cipher.decrypt(ciphertext), AES.block_size)
        except (ValueError, Exception) as e:
            # Padding / unpad errors here mean the ciphertext didn't
            # match this key — almost always a stale session (VPN
            # rotation invalidated the IP-bound key endpoint while
            # we were mid-stream). Surface as a typed error so the
            # poller loop can trigger a manifest refresh.
            raise _DecryptError(str(e)) from e
        return _strip_ts_decoy_prefix(label, plaintext)

    # Plaintext (or unknown method we'd rather pass through). Buffered
    # rather than streamed straight to disk — needs the full segment in
    # hand to check for a decoy header prefix (see _strip_ts_decoy_prefix),
    # and segments are small enough (~2-4 MB) that this costs nothing
    # meaningful.
    resp = session.get(seg["uri"], headers=headers, timeout=15)
    resp.raise_for_status()
    return _strip_ts_decoy_prefix(label, resp.content)


def _parse_key_directive(line: str) -> Optional[dict]:
    """Parse an #EXT-X-KEY: directive into {method, uri, iv}.

    Returns None if METHOD=NONE or the line can't be parsed. IV is returned
    as raw 16 bytes when present; the spec defaults it to the segment's
    media sequence number padded to 128 bits when omitted, but every source
    we've seen that uses encryption supplies one explicitly.
    """
    method = re.search(r'METHOD=([A-Z0-9-]+)', line)
    uri = re.search(r'URI="([^"]+)"', line)
    if not method or method.group(1) == "NONE" or not uri:
        return None
    iv_match = re.search(r'IV=0x([0-9a-fA-F]+)', line)
    iv_bytes = None
    if iv_match:
        hex_iv = iv_match.group(1)
        if len(hex_iv) % 2:
            hex_iv = "0" + hex_iv
        iv_bytes = bytes.fromhex(hex_iv)[-16:].rjust(16, b"\x00")
    return {"method": method.group(1), "uri": uri.group(1), "iv": iv_bytes}


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

        # Stale-session detection: count consecutive segment decrypt failures,
        # debounce refresh attempts so we don't hammer the resolver.
        self._consecutive_decrypt_failures = 0
        self._last_decrypt_refresh_at = 0.0

        # Same idea for plain segment-fetch HTTP errors -- see
        # SEGMENT_ERRORS_BEFORE_REFRESH's docstring.
        self._consecutive_segment_errors = 0
        self._last_segment_refresh_at = 0.0

        # AES key cache, keyed by absolute key URL — populated lazily as
        # encrypted playlists are seen. Key URLs rotate as the upstream
        # MEDIA-SEQUENCE advances; old keys are evicted when no segment
        # in the current playlist still references them.
        self._key_cache: dict[str, bytes] = {}
        # Guards _key_cache only — concurrent catch-up segment fetches
        # (see CATCHUP_THREAD_THRESHOLD) commonly share the same
        # still-current key and would otherwise race on the dict.
        self._key_cache_lock = threading.Lock()

        # Single Session so cookies persist across manifest poll, segment,
        # and key fetches. Sources whose stream auth lives on a different
        # subdomain than the page need the captured cross-domain jar
        # attached or the upstream returns 401/403.
        self.session = http_requests.Session()
        self.source_domain = ""
        try:
            from core.database import get_session
            from core.models.manifest import Manifest as _M
            with get_session() as _s:
                _row = (_s.query(_M.source_domain, _M.cookies)
                        .filter_by(id=manifest_id).first())
                if _row:
                    self.source_domain = (_row[0] or "") or ""
                    for c in (_row[1] or []):
                        name = c.get("name")
                        value = c.get("value")
                        domain = c.get("domain")
                        if not (name and value and domain):
                            continue
                        self.session.cookies.set(
                            name, value,
                            domain=domain,
                            path=c.get("path") or "/",
                            secure=bool(c.get("secure")),
                        )
        except Exception:
            pass

    def _upstream_headers(self) -> dict:
        h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
        if self.source_domain:
            h["Referer"] = f"https://{self.source_domain}/"
            h["Origin"] = f"https://{self.source_domain}"
        return h

    def _get_key(self, key_url: str) -> Optional[bytes]:
        """Fetch + cache the AES-128 key bytes for a given URL. Locked
        around the cache dict only — see _key_cache_lock — the network
        fetch itself runs unlocked, so a rare simultaneous miss on the
        same key from two catch-up workers just costs one redundant
        fetch, never a corrupted cache."""
        with self._key_cache_lock:
            if key_url in self._key_cache:
                return self._key_cache[key_url]
        try:
            resp = self.session.get(key_url, headers=self._upstream_headers(), timeout=10)
        except Exception as e:
            logging.warning("[PROXY] %s key fetch failed for %s: %s",
                            self.channel_id, key_url[:80], e)
            return None
        if resp.status_code != 200 or len(resp.content) != 16:
            logging.warning("[PROXY] %s key fetch %s: status=%d len=%d",
                            self.channel_id, key_url[:80],
                            resp.status_code, len(resp.content))
            return None
        with self._key_cache_lock:
            self._key_cache[key_url] = resp.content
        return resp.content

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
            resp = self.session.get(url, headers=self._upstream_headers(), timeout=10)
            text = resp.text
        except Exception:
            return url
        return _pick_best_variant(text, url)

    # ── Manifest refresh ───────────────────────────────────────────────────

    def _refresh_manifest_url(self) -> Optional[str]:
        try:
            from core.resolver.manifest_resolver import ManifestResolverService
            from core.database import get_session
            from core.models.manifest import Manifest
            # priority="high" (the default): a real viewer is watching this
            # channel right now, so this must not queue behind background
            # fallback-warming refreshes on the sidecar's single browser.
            result = ManifestResolverService.refresh_manifest(self.manifest_id, priority="high")
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
        has_written_playlist = False  # see MIN_STARTUP_SEGMENTS

        # production_speed_ratio / source_stall state — see the constants'
        # docstrings above for why this is checked every loop iteration
        # rather than only when a new segment actually lands.
        prod_window_start = time.time()
        prod_window_dur = 0.0
        last_new_segment_at = time.time()
        source_stalled = False
        stall_started_at = None

        variant_url = self._resolve_variant_url(self.manifest_url)
        logging.info("[PROXY] %s polling variant: %s",
                     self.channel_id, variant_url[:120])

        while not self._stop_event.is_set():
            try:
                resp = self.session.get(
                    variant_url, headers=self._upstream_headers(), timeout=10
                )
                # Any non-200 (not just 401/403/404) gets the same
                # log+count+refresh+give-up treatment. This used to only
                # apply to that one status tuple — anything else (502/503
                # from an origin outage, 429, etc.) fell into a silent
                # infinite retry with no logging and, because of the
                # `continue` below, no chance for the source-stall detector
                # to ever see it either. A channel wedged on a persistent
                # upstream error looked indistinguishable from a healthy
                # one in diagnostics — "running", no stalls, no errors —
                # right up until a human noticed playback was actually dead
                # (2026-09-12, an upstream origin returning 503 mid-stream).
                if resp.status_code != 200:
                    consecutive_errors += 1
                    incr_counter(self.channel_id, "playlist_errors")
                    if consecutive_errors > 3:
                        record_event(self.channel_id, "give_up",
                                    {"reason": "consecutive_playlist_errors",
                                     "count": consecutive_errors,
                                     "status": resp.status_code})
                        logging.error("[PROXY] %s giving up after %d consecutive errors (last status %d)",
                                      self.channel_id, consecutive_errors, resp.status_code)
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
            current_key = None       # raw directive line, kept for legacy callers
            current_key_info = None  # parsed {method, uri, iv} or None for plaintext
            pending_discontinuity = False
            for line in lines:
                line = line.strip()
                if line.startswith("#EXT-X-KEY:"):
                    current_key = line
                    info = _parse_key_directive(line)
                    if info and info.get("uri"):
                        info["uri"] = urljoin(resp.url, info["uri"])
                    current_key_info = info
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
                        "key_info": current_key_info,
                        "discontinuity": pending_discontinuity,
                    })
                    pending_discontinuity = False

            # Drop unused keys from the cache so it can't grow unbounded
            # across long uptimes (key URLs rotate every few minutes).
            active_key_urls = {
                s["key_info"]["uri"] for s in segments
                if s.get("key_info") and s["key_info"].get("uri")
            }
            if active_key_urls:
                for stale in [u for u in self._key_cache if u not in active_key_urls]:
                    self._key_cache.pop(stale, None)

            # First poll optimization: DAI/HLS live playlists can advertise
            # anywhere from a few segments (plain live) to a deep DVR-style
            # backlog (ad-insertion CDNs commonly keep 15+ segments for pod
            # buffering). We only need the live edge. Seed seen_uris with
            # every segment EXCEPT the last COLD_START_SEED_SEGMENTS
            # positions so the download loop writes the first playlist
            # within seconds regardless of how deep that window is — see
            # COLD_START_SEED_SEGMENTS's docstring for why this is
            # deliberately not tied to hls_list_size. Using full URI as the
            # dedup key (not the sequence number) avoids collisions — DAI ad
            # pods/slates reuse small seq numbers like 0,1,2,3 across pods,
            # which a seq-based dedup would collapse into a single entry.
            if not seen_uris and len(segments) > COLD_START_SEED_SEGMENTS:
                cutoff = len(segments) - COLD_START_SEED_SEGMENTS
                for seg in segments[:cutoff]:
                    seen_uris.add(seg["uri"])
                logging.info("[PROXY] %s seeded past %d backlog segments; will grab the last %d",
                             self.channel_id, cutoff, COLD_START_SEED_SEGMENTS)

            # Download new segments — fetch first (sequential by default,
            # selectively threaded for a catch-up burst; see
            # CATCHUP_THREAD_THRESHOLD), then apply all the counter/
            # escalation/playlist bookkeeping below single-threaded and in
            # original segment order, exactly as before this existed.
            new_segs = []
            for seg in segments:
                if seg["uri"] in seen_uris:
                    continue
                seen_uris.add(seg["uri"])
                new_segs.append(seg)

            new_count = 0
            for seg, data, err in self._fetch_new_segments(new_segs):
                uri = seg["uri"]
                if err is None:
                    try:
                        local_filename = f"seg_{local_seq:05d}.ts"
                        local_path = os.path.join(self.hls_dir, local_filename)
                        with open(local_path, "wb") as f:
                            f.write(data)
                        segment_files.append((local_seq, local_filename, seg["duration"], seg.get("discontinuity", False)))
                        local_seq += 1
                        new_count += 1
                        prod_window_dur += seg["duration"]
                        self._consecutive_decrypt_failures = 0
                        self._consecutive_segment_errors = 0
                    except Exception as e:
                        err = e  # local disk write failed — fall through to the same error handling below

                if err is not None:
                    if isinstance(err, _DecryptError):
                        self._consecutive_decrypt_failures += 1
                        incr_counter(self.channel_id, "decrypt_failures")
                        logging.warning("[PROXY] %s decrypt failed for %s: %s (#%d consecutive)",
                                        self.channel_id, uri[:80], err,
                                        self._consecutive_decrypt_failures)
                    else:
                        self._consecutive_segment_errors += 1
                        logging.warning("[PROXY] %s download failed for %s: %s (#%d consecutive)",
                                        self.channel_id, uri[:80], err,
                                        self._consecutive_segment_errors)
                        if isinstance(err, http_requests.exceptions.ConnectionError):
                            try:
                                from core.block_detector import record_possible_block
                                record_possible_block(urlparse(uri).netloc, error=str(err))
                            except Exception:
                                pass

            # If decrypt has been failing for several segments in a row, the
            # upstream session is stale — kick the resolver to re-establish
            # it. Debounced so we don't queue refreshes faster than they can
            # take effect.
            if self._consecutive_decrypt_failures >= DECRYPT_FAILURES_BEFORE_REFRESH:
                now_mono = time.monotonic()
                since_last = now_mono - self._last_decrypt_refresh_at
                if since_last >= DECRYPT_REFRESH_DEBOUNCE_SECONDS:
                    record_event(self.channel_id, "session_refresh",
                                {"reason": "consecutive_decrypt_failures",
                                 "count": self._consecutive_decrypt_failures})
                    logging.warning("[PROXY] %s triggering manifest refresh after %d "
                                    "consecutive decrypt failures (stale session?)",
                                    self.channel_id, self._consecutive_decrypt_failures)
                    self._last_decrypt_refresh_at = now_mono
                    fresh = self._refresh_manifest_url()
                    if fresh:
                        self.manifest_url = fresh
                        variant_url = self._resolve_variant_url(fresh)
                        # Old keys were tied to the dead session — drop them
                        # so the new playlist's key URLs get fetched fresh.
                        self._key_cache.clear()
                        self._consecutive_decrypt_failures = 0
                        logging.info("[PROXY] %s manifest refreshed, switched to new variant",
                                     self.channel_id)

            # Same escalation for plain segment HTTP errors -- see
            # SEGMENT_ERRORS_BEFORE_REFRESH's docstring. A fresh resolve may
            # land on a different backend/token that isn't currently
            # blocked/rate-limited, self-healing without ever needing the
            # 90s cold-start timeout to fire client-side.
            if self._consecutive_segment_errors >= SEGMENT_ERRORS_BEFORE_REFRESH:
                now_mono = time.monotonic()
                since_last = now_mono - self._last_segment_refresh_at
                if since_last >= SEGMENT_REFRESH_DEBOUNCE_SECONDS:
                    record_event(self.channel_id, "session_refresh",
                                {"reason": "consecutive_segment_errors",
                                 "count": self._consecutive_segment_errors})
                    logging.warning("[PROXY] %s triggering manifest refresh after %d "
                                    "consecutive segment errors (blocked/rate-limited CDN?)",
                                    self.channel_id, self._consecutive_segment_errors)
                    self._last_segment_refresh_at = now_mono
                    fresh = self._refresh_manifest_url()
                    if fresh:
                        self.manifest_url = fresh
                        variant_url = self._resolve_variant_url(fresh)
                        self._key_cache.clear()
                        self._consecutive_segment_errors = 0
                        logging.info("[PROXY] %s manifest refreshed after segment errors, "
                                     "switched to new variant", self.channel_id)

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

                if has_written_playlist or len(segment_files) >= MIN_STARTUP_SEGMENTS:
                    has_written_playlist = True
                    self._write_playlist(segment_files)

            if new_count:
                logging.info("[PROXY] %s downloaded %d segment(s), total on disk: %d",
                             self.channel_id, new_count, len(segment_files))
                last_new_segment_at = time.time()
                if source_stalled:
                    stalled_for = time.time() - stall_started_at
                    record_event(self.channel_id, "source_stall_recovered",
                                {"stalled_seconds": round(stalled_for, 1)})
                    logging.info("[PROXY] %s source stall recovered after %.0fs",
                                 self.channel_id, stalled_for)
                    source_stalled = False
                    stall_started_at = None

            # production_speed_ratio: checked every iteration (not just when
            # new_count > 0) so a genuine stall shows up as a live, decaying
            # ratio while it's happening — see PRODUCTION_WINDOW_SECONDS.
            window_elapsed = time.time() - prod_window_start
            if window_elapsed >= PRODUCTION_WINDOW_SECONDS:
                record_sample(self.channel_id, "production_speed_ratio",
                             prod_window_dur / window_elapsed)
                prod_window_dur = 0.0
                prod_window_start = time.time()

            # source_stall: the playlist itself has stopped advancing even
            # though our fetches keep returning 200 — invisible to
            # fetch_latency_ms/playlist_errors, which only see individual
            # request health, not whether new content is actually arriving.
            stall_gap = time.time() - last_new_segment_at
            if stall_gap >= SOURCE_STALL_SECONDS and not source_stalled:
                source_stalled = True
                stall_started_at = last_new_segment_at
                record_event(self.channel_id, "source_stall", {"gap_seconds": round(stall_gap, 1)})
                incr_counter(self.channel_id, "source_stalls")
                logging.warning("[PROXY] %s source stall — no new segments in %.0fs",
                                self.channel_id, stall_gap)

            # Prune seen_uris to avoid unbounded growth — keep only URIs that
            # are still advertised in the current playlist, plus a small buffer.
            if len(seen_uris) > 4000:
                current_uris = {s["uri"] for s in segments}
                seen_uris = seen_uris & current_uris

            self._stop_event.wait(POLL_INTERVAL)

    def _fetch_one_segment(self, seg: dict) -> tuple:
        """Fetch (and decrypt/decoy-strip) one segment's bytes — see
        _fetch_segment_bytes (module-level) for the actual logic, shared
        with core/resolver/segment_sampler.py. Deliberately pure: no disk
        write, no counter/state mutation, so it's safe to call from
        multiple threads at once during a catch-up burst (see
        _fetch_new_segments). Records its own fetch latency here (rather
        than in the caller) so the metric reflects this segment's real
        duration even when several run concurrently. Returns
        (seg, data, error) — never raises."""
        _fetch_start = time.time()
        try:
            data = _fetch_segment_bytes(self.session, seg, self._upstream_headers(),
                                        self._get_key, self.channel_id)
        except Exception as e:
            return (seg, None, e)
        record_sample(self.channel_id, "fetch_latency_ms",
                     (time.time() - _fetch_start) * 1000)
        return (seg, data, None)

    def _fetch_new_segments(self, new_segs: list) -> list:
        """Fetch every segment in new_segs and return (seg, data, error)
        tuples in the SAME order as new_segs, regardless of which path
        ran — the caller's counter/escalation/playlist bookkeeping stays
        single-threaded either way. Sequential by default (see
        CATCHUP_THREAD_THRESHOLD); escalates to a small bounded pool only
        for a genuine catch-up burst under "multi" concurrency mode."""
        if len(new_segs) < CATCHUP_THREAD_THRESHOLD or _resolver_concurrency_mode() != "multi":
            return [self._fetch_one_segment(seg) for seg in new_segs]

        logging.info("[PROXY] %s catch-up: fetching %d new segments with up to %d workers",
                     self.channel_id, len(new_segs), CATCHUP_MAX_WORKERS)
        with ThreadPoolExecutor(max_workers=CATCHUP_MAX_WORKERS) as pool:
            # map() preserves input order in its results regardless of
            # completion order — no manual reordering needed.
            return list(pool.map(self._fetch_one_segment, new_segs))

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
