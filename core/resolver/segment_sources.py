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

from core.diagnostics import record_sample, record_event, incr_counter
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

            # priority="high" (the default): a real viewer is watching this
            # channel right now, so this must not queue behind background
            # fallback-warming refreshes on the sidecar's single browser.
            result = ManifestResolverService.refresh_manifest(self.manifest_id, priority="high")
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
        _fetch_start = time.time()

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
        record_sample(self.channel_id, "fetch_latency_ms", (time.time() - _fetch_start) * 1000)
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
                    incr_counter(self.channel_id, "auth_failures")
                    if consecutive_403s > 3:
                        record_event(self.channel_id, "give_up",
                                    {"reason": "consecutive_auth_failures",
                                     "count": consecutive_403s})
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


class ContinuousRelaySource(SegmentSource):
    """Token-gated continuous WebM relay — for sources with no HLS playlist
    at all.

    A decode endpoint hands out a short-lived (token, code) pair, which
    plugs into a CDN URL template serving one uninterrupted VP8/Opus WebM
    byte stream. The token expires well before any single HTTP connection
    would naturally end, so this reconnects with a fresh token on its own
    clock, comfortably inside that window.

    ONE persistent ffmpeg transcodes the whole connection's worth of
    content (VP8/Opus -> H.264/AAC), restarted only on reconnects — not
    per-chunk. This used to run a fresh short-lived ffmpeg process every
    ~6-8s; every restart forced an encoder flush, and that flush boundary
    was the source of a small but persistent audio/video corruption
    artifact that no amount of downstream timestamp massaging
    (output_ts_offset, resampling filters, forced CFR, pre-roll priming)
    fully eliminated — the restart itself was the cause, not anything
    those were aimed at. A persistent process removes the restart
    entirely: one real encoder flush per ~250s connection instead of one
    every ~6-8s.

    Still fully "gentle mirror": Python owns 100% of the network I/O
    (token refresh, the continuous CDN read) and explicitly controls every
    byte handed to ffmpeg's stdin; ffmpeg never touches the network,
    only a local pipe Python is feeding pre-downloaded bytes into — the
    same pattern transcoder.py's own feeder already uses for every other
    source. The only thing that changed here is the encoder process's
    lifetime: per-connection instead of per-chunk.

    That also makes WebM's non-self-synchronizing framing a non-issue —
    ffmpeg's own WebM demuxer sees one genuinely continuous stream (same
    as it would over a real network connection) and handles Cluster
    boundaries internally, no cut-finding needed on this end anymore.
    Output-side chunking (for QueueItem compatibility with the shared
    feeder) is a plain byte-count/time cut of already-encoded MPEG-TS,
    which unlike WebM *is* self-synchronizing at arbitrary offsets — no
    boundary-finding logic needed there either.

    Config (decode-endpoint path, CDN URL template) is all field-driven —
    not hardcoded to any one site, so a second site with the same "token
    API -> continuous stream" shape is a config difference, not a new
    class. See get_relay_source_config() above for where per-site values
    live.
    """

    MIN_CHUNK_BYTES = 32_768       # don't flush an output slice below this
    MAX_PEND_BYTES = 24_000_000    # force-flush safety cap
    READ_CHUNK_BYTES = 65_536

    # If the token/decode endpoint (or the CDN itself) is down upstream,
    # _open_connection() keeps returning None and run() used to retry
    # forever with a 5s backoff — the poller thread never exits, so
    # ResolvedChannelStream.status() (which only checks the feeder
    # thread) kept reporting running:true indefinitely, and the fallback
    # chain in web/routers/hls.py never got a chance to re-pick and try
    # the next candidate. Same bug class RemuxStream had (see
    # MAX_CONSECUTIVE_FAILURES there) — found via a real upstream outage
    # (a relay source's decode endpoint returning a broken result
    # site-wide) that left a fallback stuck "running" for 20+ minutes
    # while its own working fallback sat unused behind it.
    MAX_CONSECUTIVE_CONNECT_FAILURES = 5

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

    def _build_transcode_cmd(self, ts_offset: float) -> list:
        """One of these runs for the lifetime of a single CDN connection
        (restarted only at the next reconnect, never per-chunk). Reads a
        continuous raw WebM byte stream via stdin, transcodes VP8/Opus ->
        H.264/AAC, writes continuous MPEG-TS to stdout.

        `ts_offset`: each fresh connection's encoder starts its own
        PTS/DTS numbering back near zero (independent -i pipe:0 demux
        session) — harmless within a connection (nothing to be
        discontinuous against), but the downstream "copy" mode stitches
        every connection's output into one continuous HLS timeline, so
        without this, connection 2 starting back at ~0 right after
        connection 1 ended around ~250s is a huge backward jump — players
        see that as the stream rewinding, not a tiny per-chunk artifact.
        Shifts this connection's output to continue exactly where the
        previous one left off."""
        return [
            "ffmpeg", "-y", "-loglevel", "error",
            "-fflags", "+genpts",
            "-f", "webm", "-i", "pipe:0",
            "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
            "-bf", "0", "-x264-params", "threads=2", "-pix_fmt", "yuv420p",
            # Real cut points for the downstream "copy" mode's HLS muxer
            # every 2s of output — otherwise it's stuck waiting for
            # whatever the encoder's own (long, variable) natural GOP
            # boundary happens to be.
            "-force_key_frames", "expr:gte(t,n_forced*2)",
            "-c:a", "aac", "-ar", "48000", "-ac", "2",
            "-output_ts_offset", f"{ts_offset:.3f}",
            "-f", "mpegts", "pipe:1",
        ]

    def _open_connection(self):
        """Fetch a fresh token and open the CDN stream. Used both for a
        connection's normal startup and for background-prefetching the
        next one ahead of the current one's deadline (see run()) — pure
        w.r.t. instance state, safe to call from either the main loop or
        a prefetch thread."""
        pair = self._fetch_token()
        if not pair:
            return None
        token, code = pair
        stream_url = self._build_stream_url(token, code)
        try:
            resp = http_requests.get(stream_url, headers=self._headers(),
                                     timeout=(10, self.refresh_interval_seconds + 30),
                                     stream=True)
            resp.raise_for_status()
        except Exception as e:
            logging.warning("[RELAY] %s couldn't open stream: %s", self.channel_id, e)
            return None
        resp._relay_log_url = stream_url.split("?")[0]  # stash for the connect log line
        return resp

    def run(self, enqueue: Callable[[QueueItem], None], stop_event: threading.Event) -> None:
        seg_index = 0
        # Persists across reconnects — see _build_transcode_cmd's
        # ts_offset docstring. Incremented after each connection by the
        # real total duration it actually produced (summed from
        # ffprobe'd QueueItem durations in _read_stdout below), not a
        # guessed constant.
        cumulative_offset = 0.0
        # Set at the end of a connection's finally-block if a background
        # prefetch (below) had already opened the next one — lets the next
        # loop iteration skip straight to encoder startup instead of
        # paying for token-fetch + CDN-connect on the critical path.
        prefetched_resp = None
        consecutive_failures = 0
        # Set at the end of a connection's finally-block (below); read at the
        # next connection's first emitted chunk to turn "how long between
        # connections" into a measured, chartable blip instead of something
        # only visible by eyeballing docker logs.
        last_disconnect_ts = None

        while not stop_event.is_set():
            used_prefetch = prefetched_resp is not None
            if prefetched_resp is not None:
                resp = prefetched_resp
                prefetched_resp = None
            else:
                resp = self._open_connection()
                if resp is None:
                    consecutive_failures += 1
                    incr_counter(self.channel_id, "connect_failures")
                    if consecutive_failures >= self.MAX_CONSECUTIVE_CONNECT_FAILURES:
                        record_event(self.channel_id, "give_up",
                                    {"reason": "consecutive_connect_failures",
                                     "count": consecutive_failures})
                        logging.error(
                            "[RELAY] %s giving up after %d consecutive connect "
                            "failures — upstream token/CDN endpoint looks down",
                            self.channel_id, consecutive_failures)
                        stop_event.set()
                        return
                    stop_event.wait(5.0)
                    continue

            consecutive_failures = 0
            logging.info("[RELAY] %s connected: %s", self.channel_id, resp._relay_log_url)

            def _raise_priority():
                # This is the one piece of the relay pipeline with real,
                # continuous CPU cost (proxy/remux modes are pure I/O,
                # which is why they don't have this problem). Measured
                # this box at load average ~16 on 6 cores — under that
                # contention the transcode was structurally losing to the
                # OS scheduler and running below realtime (measured
                # ~91.5% over a 6min window), which is what surfaces as
                # persistent buffering distinct from the reconnect stalls
                # fixed separately. Raising its scheduling priority means
                # it wins contention against lower-priority background
                # work (YT downloads, scraper ticks) instead of being
                # treated the same as them.
                #
                # Best-effort: swallow the failure instead of raising —
                # os.nice(negative) needs CAP_SYS_NICE, which this
                # container doesn't have by default even running as root
                # (Docker drops it unless granted via cap_add). Letting
                # this raise inside preexec_fn kills the whole Popen call,
                # which would silently break every relay channel start if
                # the capability isn't (or stops being) granted — the
                # actual priority boost is a real but separate ask (see
                # docker-compose.yml's cap_add), this must never be a
                # hard dependency for the encoder to start at all.
                try:
                    os.nice(-10)
                except OSError:
                    pass

            try:
                enc_proc = subprocess.Popen(
                    self._build_transcode_cmd(cumulative_offset),
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    preexec_fn=_raise_priority,
                )
            except Exception as e:
                logging.warning("[RELAY] %s couldn't start encoder: %s", self.channel_id, e)
                resp.close()
                stop_event.wait(5.0)
                continue

            def _drain_stderr(proc=enc_proc):
                try:
                    while True:
                        line = proc.stderr.readline()
                        if not line:
                            break
                        text = line.decode("utf-8", errors="replace").rstrip()
                        if text:
                            logging.warning("[RELAY] %s encoder: %s", self.channel_id, text[-200:])
                except Exception:
                    pass

            connection_duration = [0.0]  # summed real duration this connection produced

            def _read_stdout(proc=enc_proc):
                """Chunk the encoder's continuous MPEG-TS output into
                QueueItem files. Plain byte/time cuts — MPEG-TS is
                self-synchronizing at arbitrary offsets, no boundary-
                finding needed the way WebM required on the input side."""
                nonlocal seg_index, last_disconnect_ts
                buf = bytearray()
                last_flush = time.time()
                buf_wall_start = last_flush
                first_chunk = True
                try:
                    while True:
                        chunk = proc.stdout.read(65536)
                        if not chunk:
                            break
                        buf.extend(chunk)
                        due = (time.time() - last_flush) >= self.chunk_target_seconds
                        forced = len(buf) > self.MAX_PEND_BYTES
                        if not ((due and len(buf) > self.MIN_CHUNK_BYTES) or forced):
                            continue
                        idx = seg_index
                        seg_index += 1
                        out_path = os.path.join(self._download_dir,
                                                f"relay-{self.channel_id}-{idx}.ts")
                        with open(out_path, "wb") as f:
                            f.write(bytes(buf))
                        buf = bytearray()
                        wall_elapsed = time.time() - buf_wall_start
                        last_flush = time.time()
                        buf_wall_start = last_flush
                        from core.channels import ffprobe_duration
                        duration = ffprobe_duration(out_path)
                        duration = duration or self.chunk_target_seconds
                        connection_duration[0] += duration
                        if wall_elapsed > 0:
                            record_sample(self.channel_id, "encode_speed_ratio",
                                         duration / wall_elapsed)
                        if first_chunk:
                            first_chunk = False
                            if last_disconnect_ts is not None:
                                gap_ms = (time.time() - last_disconnect_ts) * 1000
                                record_sample(self.channel_id, "reconnect_gap_ms", gap_ms)
                                record_event(self.channel_id, "relay_reconnect",
                                            {"gap_ms": round(gap_ms, 1), "prefetched": used_prefetch})
                        enqueue(QueueItem(
                            kind="upstream", source_path=out_path,
                            duration=duration,
                            label=f"relay:{idx}",
                        ))
                except Exception as e:
                    logging.warning("[RELAY] %s stdout reader failed: %s", self.channel_id, e)
                # Trailing partial buffer at connection end is small and
                # discarded, same as the old design's boundary behavior —
                # not worth emitting a short QueueItem for (its bit of
                # duration is lost from cumulative_offset too, but that's
                # the same small, bounded loss the old design already had
                # at every reconnect).

            threading.Thread(target=_drain_stderr, daemon=True,
                             name=f"relay-stderr-{self.channel_id}").start()
            stdout_thread = threading.Thread(target=_read_stdout, daemon=True,
                                             name=f"relay-stdout-{self.channel_id}")
            stdout_thread.start()

            deadline = time.time() + self.refresh_interval_seconds
            # Opening the next connection was entirely sequential with
            # tearing down this one — every reconnect paused output for
            # token-fetch + CDN-connect on top of the new encoder spawn,
            # a real multi-second stall every ~250s. Starting the next
            # connection in the background well before this one's
            # scheduled deadline means it's already sitting open by the
            # time we need it — the only gap left at handoff is spawning
            # the new encoder process, not two more network round trips.
            # Doesn't help a connection the CDN cuts early with no
            # warning (nothing to prefetch ahead of), only the scheduled,
            # predictable reconnect — but that's the one firing every
            # ~250s like clockwork.
            PREFETCH_LEAD_SECONDS = 12.0
            prefetch_thread: Optional[threading.Thread] = None
            prefetch_holder: dict = {}

            def _prefetch_next():
                prefetch_holder["resp"] = self._open_connection()

            try:
                chunks = resp.iter_content(chunk_size=self.READ_CHUNK_BYTES)
                while not stop_event.is_set() and time.time() < deadline:
                    if (prefetch_thread is None
                            and (deadline - time.time()) <= PREFETCH_LEAD_SECONDS):
                        prefetch_thread = threading.Thread(
                            target=_prefetch_next, daemon=True,
                            name=f"relay-prefetch-{self.channel_id}")
                        prefetch_thread.start()
                    try:
                        _read_start = time.time()
                        chunk = next(chunks)
                        _read_ms = (time.time() - _read_start) * 1000
                        if _read_ms > 50:
                            # A plain read under normal conditions is
                            # sub-millisecond — anything over this floor is
                            # the CDN read stalling, likely the first
                            # symptom of a "micro timing delay" before it
                            # becomes a visible stall. Filtering below the
                            # floor keeps the ring buffer from filling with
                            # noise on every single 64KB read.
                            record_sample(self.channel_id, "relay_read_latency_ms", _read_ms)
                    except StopIteration:
                        logging.info("[RELAY] %s connection ended early", self.channel_id)
                        break
                    except Exception as e:
                        logging.warning("[RELAY] %s read error: %s", self.channel_id, e)
                        break
                    try:
                        enc_proc.stdin.write(chunk)
                    except (BrokenPipeError, OSError) as e:
                        logging.warning("[RELAY] %s encoder pipe broke: %s", self.channel_id, e)
                        break
            finally:
                last_disconnect_ts = time.time()
                resp.close()
                try:
                    enc_proc.stdin.close()
                except Exception:
                    pass
                try:
                    enc_proc.wait(timeout=10)
                except Exception:
                    try:
                        enc_proc.kill()
                    except Exception:
                        pass
                stdout_thread.join(timeout=5)
                cumulative_offset += connection_duration[0]
                if prefetch_thread is not None:
                    prefetch_thread.join(timeout=10)
                    prefetched_resp = prefetch_holder.get("resp")

        # stop_event fired between connections — if a prefetch had already
        # opened the next one, it never gets used; close it rather than
        # leaking the socket.
        if prefetched_resp is not None:
            prefetched_resp.close()
