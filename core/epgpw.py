"""epg.pw integration — free public XMLTV guide data for 24-7 resolved channels.

Resolved channels are live streams with no schedule of their own, so the XMLTV
export gives them placeholder blocks. For real TV channels (CNN, AMC, ...) the
actual guide is available for free from epg.pw, keyed by their channel id.

Two halves:
  * auto_map()  — match our channel names to epg.pw's US catalog. Deterministic
    (normalisation + curated aliases + exact key match). NO fuzzy guessing: an
    uncertain match is reported as no-match rather than pointing a channel at
    the wrong guide. Unmapped channels keep their placeholder blocks.
  * refresh_cache() / programmes_for() — fetch and cache each mapped channel's
    XMLTV, then hand parsed programmes to the exporter.

Only 24-7 resolved channels are eligible; live-event channels are excluded (they
carry their own event_start/event_end programme).
"""

import difflib
import logging
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import requests

from core.config import get_setting

logger = logging.getLogger(__name__)

CATALOG_URL = "https://epg.pw/areas/us.html?lang=en"
EPG_URL = "https://epg.pw/api/epg.xml?lang=en&channel_id={cid}"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
_HDRS = {"User-Agent": UA}

# Words that describe picture quality / region / feed type, never identity.
_DECOR = {"hd", "sd", "fhd", "uhd", "4k", "hdtv", "national", "feed", "stream",
          "east", "west", "pacific", "the"}
# Generic suffixes stripped only from the END, and never down to nothing:
# "History Channel" == "History", "TLC HD (US)" == "TLC", but "USA Network"
# must stay "usa" rather than collapse to "network".
_TRAILING = {"channel", "network", "tv", "us", "usa"}
# Prefer the East feed for dual-feed channels.
_WEST_MARKERS = ("west", "pacific")

# Acronyms / callsigns / renames no string metric can bridge. Maps our
# normalised channel name -> epg.pw's normalised name.
_ALIAS = {
    "hgtv": "home and garden television",
    "mtv": "mtv music television",
    "mtv2": "mtv2 music television",
    "nbatv": "nba",
    "bbc news": "bbc news north america",
    "disney jr": "disney junior",
    "wspa 7news": "wspa dt",
}


def _base_norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").lower()
    s = re.sub(r"\[[^\]]*\]", " ", s)      # drop "[Country]" suffixes
    s = re.sub(r"[()]", " ", s)            # unwrap "(HD)" / "(Pacific)"
    s = s.replace("&", " and ").replace("+", " plus ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = [t for t in s.split() if t not in _DECOR]
    while len(toks) > 1 and toks[-1] in _TRAILING:
        toks.pop()
    return " ".join(toks) if toks else s


def normalize(s: str) -> str:
    n = _base_norm(s)
    return _ALIAS.get(n, n)


def _key(s: str) -> str:
    return normalize(s).replace(" ", "")


def fetch_catalog(timeout: int = 90) -> list[dict]:
    """Scrape epg.pw's US channel table -> [{id, name, key, norm, west}]."""
    html = requests.get(CATALOG_URL, headers=_HDRS, timeout=timeout).text
    rows = re.findall(r'href="/last/(\d+)\.html\?lang=en"[^>]*>([^<]+)</a>', html)
    out = []
    for cid, name in rows:
        name = name.strip()
        if not name:
            continue
        out.append({
            "id": cid,
            "name": name,
            "key": _key(name),
            "norm": normalize(name),
            "west": any(w in name.lower() for w in _WEST_MARKERS),
        })
    logger.info("[EPGPW] Catalog: %d US channels", len(out))
    return out


def match_one(catalog: list[dict], channel_name: str, cutoff: float = 0.90):
    """Best epg.pw candidate for a channel name, or None.

    Tier 1 is an exact match on the normalised key (East feed wins ties). Tier 2
    is a difflib ratio, deliberately WITHOUT any subset/containment bonus —
    that bonus makes "FOX" swallow "Fox News Channel", which is exactly the kind
    of confidently-wrong mapping we must never produce.
    """
    k, n = _key(channel_name), normalize(channel_name)
    exact = [c for c in catalog if c["key"] == k]
    if exact:
        exact.sort(key=lambda c: (c["west"], len(c["name"])))
        return exact[0], 1.0, "exact"
    scored = [(difflib.SequenceMatcher(None, n, c["norm"]).ratio(), c) for c in catalog]
    scored.sort(key=lambda x: (-x[0], x[1]["west"], len(x[1]["norm"])))
    if scored and scored[0][0] >= cutoff:
        return scored[0][1], round(scored[0][0], 3), "fuzzy"
    return None, (round(scored[0][0], 3) if scored else 0.0), "none"


def eligible_channels(session):
    """24-7 resolved channels only — live-event channels are excluded."""
    from core.models import Channel
    return (session.query(Channel)
            .filter(Channel.type == "resolved",
                    Channel.event_start.is_(None),
                    Channel.event_end.is_(None))
            .order_by(Channel.name)
            .all())


def auto_map(dry_run: bool = True) -> dict:
    """Match eligible channels to epg.pw ids. Returns a report; writes only when
    dry_run is False. Never overwrites an id that's already set."""
    from core.database import get_session
    catalog = fetch_catalog()
    matched, unmatched, skipped = [], [], []
    with get_session() as session:
        for ch in eligible_channels(session):
            if ch.epg_pw_id:
                skipped.append({"channel": ch.name, "epg_pw_id": ch.epg_pw_id})
                continue
            cand, score, how = match_one(catalog, ch.name)
            if cand:
                matched.append({"channel": ch.name, "epg_pw_id": cand["id"],
                                "epg_pw_name": cand["name"], "score": score, "how": how})
                if not dry_run:
                    ch.epg_pw_id = cand["id"]
            else:
                unmatched.append({"channel": ch.name, "best_score": score})
    logger.info("[EPGPW] auto_map(dry_run=%s): %d matched, %d unmatched, %d already mapped",
                dry_run, len(matched), len(unmatched), len(skipped))
    return {"dry_run": dry_run, "matched": matched, "unmatched": unmatched,
            "already_mapped": skipped,
            "counts": {"matched": len(matched), "unmatched": len(unmatched),
                       "already_mapped": len(skipped)}}


def _fetch_one(cid: str, timeout: int = 30) -> str | None:
    try:
        r = requests.get(EPG_URL.format(cid=cid), headers=_HDRS, timeout=timeout)
    except Exception as e:
        logger.warning("[EPGPW] fetch %s failed: %s", cid, e)
        return None
    if r.status_code != 200 or "<tv" not in r.text:
        logger.warning("[EPGPW] fetch %s: HTTP %s (len %d)", cid, r.status_code, len(r.text))
        return None
    return r.text


def refresh_cache(force: bool = False) -> dict:
    """Fetch XMLTV for every mapped channel and cache it. Skips ids refreshed
    within the configured interval unless force."""
    from core.database import get_session
    from core.models import Channel, EpgPwCache
    hours = float(get_setting("EPGPW_REFRESH_HOURS", "12") or 12)
    now = datetime.now(timezone.utc)
    ok = skipped = failed = 0
    with get_session() as session:
        ids = {c.epg_pw_id for c in eligible_channels(session) if c.epg_pw_id}
        for cid in sorted(ids):
            row = session.query(EpgPwCache).filter_by(epg_pw_id=cid).first()
            if row and not force and row.fetched_at:
                age = now - row.fetched_at.replace(tzinfo=row.fetched_at.tzinfo or timezone.utc)
                if age < timedelta(hours=hours):
                    skipped += 1
                    continue
            xml = _fetch_one(cid)
            if not xml:
                failed += 1
                continue
            if row:
                row.xml, row.fetched_at = xml, now
            else:
                session.add(EpgPwCache(epg_pw_id=cid, xml=xml, fetched_at=now))
            ok += 1
    logger.info("[EPGPW] refresh_cache: %d fetched, %d fresh, %d failed", ok, skipped, failed)
    return {"fetched": ok, "still_fresh": skipped, "failed": failed}


def load_cache() -> dict:
    """{epg_pw_id: xml} for all cached guides."""
    try:
        from core.database import get_session
        from core.models import EpgPwCache
        with get_session() as session:
            return {r.epg_pw_id: r.xml for r in session.query(EpgPwCache).all()}
    except Exception as e:
        logger.warning("[EPGPW] load_cache failed: %s", e)
        return {}


def programmes_for(xml: str, window_start: datetime, window_end: datetime) -> list[dict]:
    """Parse cached epg.pw XMLTV -> [{start, stop, title, desc, category}] within
    the window. Returns [] on anything unparseable so the exporter falls back to
    placeholders rather than emitting a broken guide."""
    out = []
    try:
        root = ET.fromstring(xml)
    except Exception as e:
        logger.warning("[EPGPW] unparseable cached XML: %s", e)
        return out
    for prog in root.findall("programme"):
        start = _parse_xmltv_ts(prog.get("start"))
        stop = _parse_xmltv_ts(prog.get("stop"))
        if not start or not stop or stop <= window_start or start >= window_end:
            continue
        title = (prog.findtext("title") or "").strip()
        if not title:
            continue
        out.append({
            "start": start,
            "stop": stop,
            "title": title,
            "desc": (prog.findtext("desc") or "").strip(),
            "category": (prog.findtext("category") or "").strip(),
        })
    out.sort(key=lambda p: p["start"])
    return out


def _parse_xmltv_ts(s: str | None):
    """XMLTV timestamps: 'YYYYMMDDHHMMSS +0000' (offset optional)."""
    if not s:
        return None
    s = s.strip()
    m = re.match(r"^(\d{14})(?:\s*([+-]\d{4}))?$", s)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
    except ValueError:
        return None
    off = m.group(2)
    if off:
        sign = 1 if off[0] == "+" else -1
        delta = timedelta(hours=int(off[1:3]), minutes=int(off[3:5])) * sign
        return dt.replace(tzinfo=timezone.utc) - delta
    return dt.replace(tzinfo=timezone.utc)
