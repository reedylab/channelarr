"""Stream profiles for resolved-channel transcode-mediated streaming.

Each upstream provider has its own dialect for marking ad breaks. Adult Swim
uses SCTE-35 with `#EXT-X-CUE-OUT`/`#EXT-X-CUE-IN` and continuation markers.
Anvato/Lura (used by WSPA News and other local affiliates) uses
`#ANVATO-SEGMENT-INFO: type=master|slate|ad` plus per-segment
`#EXT-X-DATERANGE` blocks with base64-encoded `X-LURA-DATA` carrying
ad-pod metadata.

A profile names the dialect, parses it, and answers one question per
segment: should this segment be queued as upstream content, or replaced
by a bump? It also reports a "pod duration" hint when available so the
orchestrator can pre-queue a perfect-fit bump sequence instead of
queueing one bump per segment continuously.

The orchestrator stays profile-agnostic — it just calls the profile's
classify() and parse() methods.
"""

import base64
import logging
import re
from dataclasses import dataclass, field
from typing import Callable, Optional
from urllib.parse import urljoin


# ── Segment shape (kept independent of any profile) ─────────────────────────

@dataclass
class UpstreamSegment:
    """Single segment from an upstream variant playlist, normalized.

    Profile-specific metadata (cue duration, anvato type, pod info) is
    folded into the typed fields below or carried in `extras`.
    """
    seq: int
    uri: str               # absolute URL
    duration: float
    program_date_time: Optional[str] = None
    discontinuity: bool = False

    # SCTE-35 / Adult Swim style
    cue_out_duration: Optional[float] = None
    cue_out_cont_remaining: Optional[float] = None
    cue_in: bool = False

    # Anvato / Lura style
    anvato_type: Optional[str] = None  # "master" | "slate" | "ad" | None
    lura_pod_duration: Optional[float] = None
    lura_ad_index: Optional[int] = None

    # AES decryption (for upstreams that use it)
    key_method: Optional[str] = None
    key_uri: Optional[str] = None
    key_iv: Optional[str] = None

    extras: dict = field(default_factory=dict)


# ── Classification result ────────────────────────────────────────────────────

# What the orchestrator should do with a segment:
#   "show"     — queue as upstream content
#   "replace"  — replace with bump(s)
CLASS_SHOW = "show"
CLASS_REPLACE = "replace"


# ── Adult Swim / Turner profile ──────────────────────────────────────────────

def parse_adultswim_playlist(text: str, base_url: str) -> tuple[int, list[UpstreamSegment]]:
    """Parse a SCTE-35 style variant playlist (Adult Swim and similar)."""
    lines = text.splitlines()
    media_seq = 0
    segments: list[UpstreamSegment] = []

    cur_dur = 0.0
    cur_pdt = None
    pending_disc = False
    pending_cue_out = None
    pending_cue_cont = None
    pending_cue_in = False
    cur_key_method = None
    cur_key_uri = None
    cur_key_iv = None

    for line in lines:
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                media_seq = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("#EXTINF:"):
            try:
                cur_dur = float(line.split(":", 1)[1].split(",", 1)[0])
            except (ValueError, IndexError):
                cur_dur = 0.0
        elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            cur_pdt = line.split(":", 1)[1].strip()
        elif line.startswith("#EXT-X-DISCONTINUITY"):
            pending_disc = True
        elif line.startswith("#EXT-X-CUE-OUT-CONT"):
            dur_m = re.search(r"Duration=([\d.]+)", line)
            elapsed_m = re.search(r"ElapsedTime=([\d.]+)", line)
            if dur_m and elapsed_m:
                try:
                    pending_cue_cont = max(0.0, float(dur_m.group(1)) - float(elapsed_m.group(1)))
                except ValueError:
                    pass
        elif line.startswith("#EXT-X-CUE-OUT:"):
            try:
                pending_cue_out = float(line.split(":", 1)[1].strip())
            except ValueError:
                pending_cue_out = None
        elif line.startswith("#EXT-X-CUE-IN"):
            pending_cue_in = True
        elif line.startswith("#EXT-X-KEY:"):
            attrs = line.split(":", 1)[1]
            method_m = re.search(r"METHOD=([^,]+)", attrs)
            uri_m = re.search(r'URI="([^"]+)"', attrs)
            iv_m = re.search(r"IV=(0x[0-9a-fA-F]+)", attrs)
            cur_key_method = method_m.group(1) if method_m else None
            cur_key_uri = uri_m.group(1) if uri_m else None
            cur_key_iv = iv_m.group(1) if iv_m else None
        elif line and not line.startswith("#"):
            seq_num = media_seq + len(segments)
            segments.append(UpstreamSegment(
                seq=seq_num,
                uri=urljoin(base_url, line),
                duration=cur_dur,
                program_date_time=cur_pdt,
                discontinuity=pending_disc,
                cue_out_duration=pending_cue_out,
                cue_out_cont_remaining=pending_cue_cont,
                cue_in=pending_cue_in,
                key_method=cur_key_method,
                key_uri=cur_key_uri,
                key_iv=cur_key_iv,
            ))
            cur_dur = 0.0
            cur_pdt = None
            pending_disc = False
            pending_cue_out = None
            pending_cue_cont = None
            pending_cue_in = False
    return media_seq, segments


def classify_adultswim(seg: UpstreamSegment, state: dict) -> tuple[str, Optional[float]]:
    """Classify an Adult Swim segment.

    state is a dict the profile owns: {'in_cue': bool, 'cue_remaining': float}
    Returns (class, pod_duration_hint).
    """
    if seg.cue_out_duration:
        state["in_cue"] = True
        state["cue_remaining"] = seg.cue_out_duration
        return CLASS_REPLACE, seg.cue_out_duration

    if state.get("in_cue"):
        state["cue_remaining"] = state.get("cue_remaining", 0) - seg.duration
        if seg.cue_in or state["cue_remaining"] <= 0:
            state["in_cue"] = False
        return CLASS_REPLACE, None

    return CLASS_SHOW, None


# ── Anvato / Lura profile (WSPA News and other local affiliates) ────────────

def _decode_lura_data(b64: str) -> dict:
    """Decode an X-LURA-DATA base64 blob into a key=value dict."""
    try:
        decoded = base64.b64decode(b64).decode("utf-8", errors="replace")
    except Exception:
        return {}
    out = {}
    # Format: X-LURA-TYPE="MASTER",X-LURA-AD-DURATION="15",...
    for match in re.finditer(r'(X-[A-Z0-9-]+)=("([^"]*)"|([^,]+))', decoded):
        key = match.group(1)
        value = match.group(3) if match.group(3) is not None else match.group(4)
        out[key] = value
    return out


def parse_anvato_playlist(text: str, base_url: str) -> tuple[int, list[UpstreamSegment]]:
    """Parse an Anvato/Lura variant playlist.

    Recognizes #ANVATO-SEGMENT-INFO type tags and the X-LURA-DATA base64
    blob inside #EXT-X-DATERANGE for richer ad-pod metadata.
    """
    lines = text.splitlines()
    media_seq = 0
    segments: list[UpstreamSegment] = []

    cur_dur = 0.0
    cur_pdt = None
    pending_disc = False
    pending_anvato_type = None
    pending_lura_pod_dur = None
    pending_lura_ad_index = None
    cur_key_method = None
    cur_key_uri = None
    cur_key_iv = None

    for line in lines:
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                media_seq = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("#EXTINF:"):
            try:
                cur_dur = float(line.split(":", 1)[1].split(",", 1)[0])
            except (ValueError, IndexError):
                cur_dur = 0.0
        elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            cur_pdt = line.split(":", 1)[1].strip()
        elif line.startswith("#EXT-X-DISCONTINUITY"):
            pending_disc = True
        elif line.startswith("#ANVATO-SEGMENT-INFO"):
            type_m = re.search(r"type=(\w+)", line)
            if type_m:
                pending_anvato_type = type_m.group(1).lower()
        elif line.startswith("#EXT-X-DATERANGE:"):
            lura_m = re.search(r'X-LURA-DATA="([^"]+)"', line)
            if lura_m:
                lura = _decode_lura_data(lura_m.group(1))
                # If type wasn't set by ANVATO-SEGMENT-INFO, fall back to LURA
                if not pending_anvato_type and "X-LURA-TYPE" in lura:
                    pending_anvato_type = lura["X-LURA-TYPE"].lower()
                if "X-LURA-POD-DURATION" in lura:
                    try:
                        pending_lura_pod_dur = float(lura["X-LURA-POD-DURATION"])
                    except ValueError:
                        pass
                if "X-LURA-AD-INDEX" in lura:
                    try:
                        pending_lura_ad_index = int(lura["X-LURA-AD-INDEX"])
                    except ValueError:
                        pass
        elif line.startswith("#EXT-X-KEY:"):
            attrs = line.split(":", 1)[1]
            method_m = re.search(r"METHOD=([^,]+)", attrs)
            uri_m = re.search(r'URI="([^"]+)"', attrs)
            iv_m = re.search(r"IV=(0x[0-9a-fA-F]+)", attrs)
            cur_key_method = method_m.group(1) if method_m else None
            cur_key_uri = uri_m.group(1) if uri_m else None
            cur_key_iv = iv_m.group(1) if iv_m else None
        elif line and not line.startswith("#"):
            seq_num = media_seq + len(segments)
            segments.append(UpstreamSegment(
                seq=seq_num,
                uri=urljoin(base_url, line),
                duration=cur_dur,
                program_date_time=cur_pdt,
                discontinuity=pending_disc,
                anvato_type=pending_anvato_type,
                lura_pod_duration=pending_lura_pod_dur,
                lura_ad_index=pending_lura_ad_index,
                key_method=cur_key_method,
                key_uri=cur_key_uri,
                key_iv=cur_key_iv,
            ))
            cur_dur = 0.0
            cur_pdt = None
            pending_disc = False
            pending_anvato_type = None
            pending_lura_pod_dur = None
            pending_lura_ad_index = None
    return media_seq, segments


def classify_anvato(seg: UpstreamSegment, state: dict) -> tuple[str, Optional[float]]:
    """Classify an Anvato/Lura segment.

    Treats anything that isn't `master` (slate, ad, or unknown) as a break
    that should be replaced with a bump. When the first ad of a pod arrives
    (X-LURA-AD-INDEX=0), reports the pod duration so the orchestrator can
    pre-queue a perfect-fit bump sequence.
    """
    seg_type = seg.anvato_type or "master"

    if seg_type == "master":
        state["in_break"] = False
        return CLASS_SHOW, None

    # Non-master → replace
    if not state.get("in_break"):
        state["in_break"] = True

    # When the first ad of a pod arrives, return the full pod duration
    # so the orchestrator can build a perfect-fit bump sequence.
    pod_hint = None
    if seg_type == "ad" and seg.lura_ad_index == 0 and seg.lura_pod_duration:
        pod_hint = seg.lura_pod_duration

    return CLASS_REPLACE, pod_hint


# ── Profile registry ────────────────────────────────────────────────────────

@dataclass
class StreamProfile:
    name: str
    description: str
    parse: Callable[[str, str], tuple[int, list[UpstreamSegment]]]
    classify: Callable[[UpstreamSegment, dict], tuple[str, Optional[float]]]


ADULTSWIM = StreamProfile(
    name="adultswim",
    description="Turner / Adult Swim CDN — SCTE-35 with CUE-OUT/CUE-IN markers",
    parse=parse_adultswim_playlist,
    classify=classify_adultswim,
)

ANVATO_LURA = StreamProfile(
    name="anvato_lura",
    description="Anvato / Lura Live CDN — type-tagged segments (master/slate/ad) with X-LURA pod metadata",
    parse=parse_anvato_playlist,
    classify=classify_anvato,
)

PROFILES: dict[str, StreamProfile] = {
    ADULTSWIM.name: ADULTSWIM,
    ANVATO_LURA.name: ANVATO_LURA,
}


def detect_profile(text: str) -> StreamProfile:
    """Heuristic profile detection for a variant playlist body.

    Used when a channel's profile is set to "auto". Looks for marker
    fingerprints unique to each known dialect. Falls back to Adult Swim
    (SCTE-35 is the most widely supported standard).
    """
    if "#ANVATO-SEGMENT-INFO" in text or "X-LURA-DATA" in text:
        return ANVATO_LURA
    return ADULTSWIM


def get_profile(name: Optional[str]) -> StreamProfile:
    """Look up a profile by name. Returns Adult Swim as the safe default."""
    if not name or name == "auto":
        return ADULTSWIM
    profile = PROFILES.get(name.lower())
    if profile is None:
        logging.warning("[PROFILE] unknown profile %r, falling back to %s", name, ADULTSWIM.name)
        return ADULTSWIM
    return profile
