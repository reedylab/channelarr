"""Parse CDN token expiry timestamps from signed URLs.

Resolved m3u8 URLs are usually signed with short-lived tokens. This module
extracts the expiry timestamp from known CDN URL patterns so the scheduler
can refresh them proactively before they expire.
"""

import base64
import json
import logging
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)


def _from_unix(ts: int | str) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _try_anvato_lura(query: dict) -> datetime | None:
    """Anvato / Lura: te=UNIX_TS (WSPA, Nexstar, Sinclair)."""
    vals = query.get("te") or query.get("anvauth")
    if not vals:
        return None
    # te= is a direct unix timestamp
    if query.get("te"):
        return _from_unix(query["te"][0])
    # anvauth=tb=0~te=UNIX_TS~sgn=...
    m = re.search(r"te=(\d+)", vals[0])
    if m:
        return _from_unix(m.group(1))
    return None


def _try_cloudfront(query: dict) -> datetime | None:
    """CloudFront / generic: Expires=UNIX_TS (Turner, many CDNs)."""
    for key in ("Expires", "expires"):
        if key in query:
            return _from_unix(query[key][0])
    return None


def _try_s3_presigned(query: dict) -> datetime | None:
    """S3 presigned: X-Amz-Date=ISO8601 + X-Amz-Expires=SECONDS."""
    date = query.get("X-Amz-Date") or query.get("x-amz-date")
    expires = query.get("X-Amz-Expires") or query.get("x-amz-expires")
    if not date or not expires:
        return None
    try:
        # Format: 20260410T153000Z
        dt = datetime.strptime(date[0], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return dt + timedelta(seconds=int(expires[0]))
    except (ValueError, TypeError):
        return None


def _try_akamai_hdnts(query: dict) -> datetime | None:
    """Akamai token: hdnts=...exp=UNIX_TS~..."""
    token = query.get("hdnts") or query.get("token")
    if not token:
        return None
    m = re.search(r"exp=(\d+)", token[0])
    if m:
        return _from_unix(m.group(1))
    return None


def _try_jwt(query: dict) -> datetime | None:
    """JWT in any query param: look for eyJ-prefixed base64 segments."""
    for vals in query.values():
        for v in vals:
            if not v or "eyJ" not in v:
                continue
            # Find the JWT-looking substring
            m = re.search(r"(eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+)", v)
            if not m:
                continue
            try:
                payload_b64 = m.group(1).split(".")[1]
                # Add padding
                payload_b64 += "=" * (-len(payload_b64) % 4)
                payload = json.loads(base64.urlsafe_b64decode(payload_b64))
                exp = payload.get("exp")
                if exp:
                    return _from_unix(exp)
            except Exception:
                continue
    return None


_PARSERS = (
    _try_anvato_lura,
    _try_cloudfront,
    _try_s3_presigned,
    _try_akamai_hdnts,
    _try_jwt,
)


# Tokens that parse to less than this many seconds from now are treated as
# rolling/refresh tokens, not real CDN expiry. WSPA's Anvato master URL has a
# 90-second te= that is NOT the actual stream lifetime — fetching it again
# gets a fresh token. Real CDN expiries start at ~15 minutes.
MIN_REASONABLE_LIFETIME_SEC = 10 * 60  # 10 minutes


def parse_expiry(url: str, *, filter_short: bool = True) -> datetime | None:
    """Extract a token expiry timestamp from a signed URL.

    Returns a timezone-aware UTC datetime, or None if no known pattern matches.

    When filter_short=True (default), parsed expiries less than 10 minutes in
    the future are treated as rolling tokens and return None — the 403 safety
    net will handle these streams instead of wasteful constant refreshing.
    """
    if not url:
        return None
    try:
        query = parse_qs(urlparse(url).query, keep_blank_values=True)
    except Exception:
        return None

    for parser in _PARSERS:
        try:
            result = parser(query)
            if result:
                if filter_short:
                    delta = (result - datetime.now(timezone.utc)).total_seconds()
                    if delta < MIN_REASONABLE_LIFETIME_SEC:
                        logger.debug("Ignoring short-lived token (%ds) from %s", int(delta), parser.__name__)
                        continue
                logger.debug("Parsed expiry from %s: %s", parser.__name__, result.isoformat())
                return result
        except Exception as e:
            logger.debug("Parser %s raised %s", parser.__name__, e)

    return None


def parse_body_expiry(body_text: str, manifest_url: str) -> datetime | None:
    """Find the earliest expiry among URLs embedded in an HLS master playlist.

    Master playlists often contain rendition URLs with longer-lived tokens than
    the master URL itself — the master token may be 90s but renditions can be
    an hour out. For predictive refresh we want the *effective* expiry of the
    stream which is the earliest token we depend on.

    Also considers the manifest URL itself. Returns the min expiry, or None.
    """
    expiries = []

    # Start with the master URL's own expiry
    master_exp = parse_expiry(manifest_url)
    if master_exp:
        expiries.append(master_exp)

    # Scan URLs in the body (rendition playlists + segment URLs)
    if body_text:
        for line in body_text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            exp = parse_expiry(line)
            if exp:
                expiries.append(exp)

    if not expiries:
        return None

    # Return the SECOND-shortest expiry if we have many, to avoid being dominated
    # by an outlier 90s master token. If we only have a few, take the min.
    expiries.sort()
    if len(expiries) >= 3:
        # Skip the master-URL micro-token, use the rendition/segment expiry
        return expiries[1]
    return expiries[0]
