#!/usr/bin/env python3
"""
Verifies that a v2-captured cookie jar actually enables real downstream
playback -- not just that /capture's response contains plausible-looking
cookies. Replicates the EXACT mechanism production already relies on:

  core/resolver/manifest_resolver.py::_store_manifest (~line 1004-1016)
    -- filters raw captured cookies down to only those whose domain
    relates (substring match, either direction) to the CDN domain (from
    the manifest URL) or the source domain (referer, falling back to
    page URL, falling back to manifest URL).

  core/resolver/proxy_stream.py::ProxyStream.__init__ (~line 265-315)
    -- loads the filtered cookies into a requests.Session honoring
    domain/path/secure, reused across manifest/segment/key fetches, with
    a fixed UA + Referer/Origin derived from source_domain
    (_upstream_headers, same file).

No hardcoded site names -- the target URL is a required CLI arg, matching
the convention set by shadow_compare.py. Read-only against the DB (one
SELECT, for the positive-control cross-check) -- never writes/mutates any
Channel/Manifest row, never touches the live resolver pipeline.

Usage:
  python3 verify_cookie_auth.py --url "https://example.com/channel"
"""
import argparse
import json
import subprocess
import sys
from urllib.parse import urljoin, urlparse

import requests

V2_CONTAINER = "channelarr-selenium-uc-v2"
V2_PORT = 4446
APP_CONTAINER = "channelarr"

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


def capture_via_v2(url: str, timeout: int) -> dict:
    script = f"""
import requests, json
r = requests.post("http://localhost:{V2_PORT}/capture",
                   json={{"url": {json.dumps(url)}, "timeout": {timeout}, "priority": "high"}},
                   timeout={timeout + 60})
print(json.dumps(r.json()))
"""
    result = subprocess.run(["docker", "exec", V2_CONTAINER, "python3", "-c", script],
                             capture_output=True, text=True, timeout=timeout + 90)
    if result.returncode != 0:
        raise RuntimeError(f"v2 capture failed: {result.stderr}")
    return json.loads(result.stdout.strip())


def filter_like_store_manifest(cookies: list, manifest_url: str, referer: str, page_url: str) -> tuple[list, str]:
    """Verbatim port of _store_manifest's domain-filter rule -- see module
    docstring for the exact source location. Returns (filtered_cookies,
    source_domain) since source_domain is also needed for header/session
    construction below, same as the real pipeline."""
    referer_host = urlparse(referer).netloc if referer else ""
    source_domain = referer_host or urlparse(page_url).netloc or urlparse(manifest_url).netloc
    cdn_domain = urlparse(manifest_url).netloc

    filtered = []
    for c in (cookies or []):
        cd = (c.get("domain") or "").lstrip(".")
        if cd and (cdn_domain.endswith(cd) or source_domain.endswith(cd)
                   or cd.endswith(cdn_domain) or cd.endswith(source_domain)):
            filtered.append(c)
    return filtered, source_domain


def build_session_and_headers(cookies: list, source_domain: str):
    """Verbatim port of ProxyStream.__init__ + _upstream_headers -- see
    module docstring for the exact source location."""
    session = requests.Session()
    for c in cookies:
        name, value, domain = c.get("name"), c.get("value"), c.get("domain")
        if not (name and value and domain):
            continue
        session.cookies.set(name, value, domain=domain, path=c.get("path") or "/",
                             secure=bool(c.get("secure")))
    headers = {"User-Agent": _UA}
    if source_domain:
        headers["Referer"] = f"https://{source_domain}/"
        headers["Origin"] = f"https://{source_domain}"
    return session, headers


def find_segment_url(master_body: str, master_url: str):
    """Master playlist -> media playlist -> a real segment URL. Plain HLS
    text parsing, no library needed. Returns (segment_url, media_playlist_url)
    -- media_playlist_url is None if master_body was already a media
    playlist (no #EXT-X-STREAM-INF variants)."""
    lines = [l.strip() for l in master_body.splitlines() if l.strip()]
    media_url = None
    for i, l in enumerate(lines):
        if l.startswith("#EXT-X-STREAM-INF"):
            if i + 1 < len(lines) and not lines[i + 1].startswith("#"):
                media_url = urljoin(master_url, lines[i + 1])
                break

    if media_url is None:
        # master_body may already be a media playlist -- look for a segment directly
        for l in lines:
            if l and not l.startswith("#"):
                return urljoin(master_url, l), None
        return None, None

    r = requests.get(media_url, timeout=15)
    r.raise_for_status()
    media_lines = [l.strip() for l in r.text.splitlines() if l.strip()]
    for l in media_lines:
        if l and not l.startswith("#"):
            return urljoin(media_url, l), media_url
    return None, media_url


def fetch_and_report(label: str, session, headers: dict, url: str):
    try:
        client = session if session is not None else requests
        r = client.get(url, headers=headers, timeout=15)
        size = len(r.content)
        verdict = "OK" if r.status_code == 200 and size > 1000 else "SUSPECT"
        print(f"  [{label:20s}] status={r.status_code} size={size:>8d} verdict={verdict}")
        return r.status_code, size
    except Exception as e:
        print(f"  [{label:20s}] EXC: {e}")
        return None, None


def fetch_v1_historical_cookies(source_domain: str) -> list:
    script = f"""
from core.database import get_session
from core.models.manifest import Manifest
import json
with get_session() as s:
    row = s.query(Manifest.cookies).filter(
        Manifest.source_domain == {json.dumps(source_domain)}
    ).order_by(Manifest.updated_at.desc()).first()
    print(json.dumps(row[0] if row else []))
"""
    result = subprocess.run(["docker", "exec", APP_CONTAINER, "python3", "-c", script],
                             capture_output=True, text=True, timeout=20)
    if result.returncode != 0:
        return []
    try:
        return json.loads(result.stdout.strip())
    except Exception:
        return []


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", required=True, help="real watch-page URL to capture and test")
    ap.add_argument("--timeout", type=int, default=40)
    args = ap.parse_args()

    print(f"Capturing via v2: {args.url}")
    cap = capture_via_v2(args.url, args.timeout)
    if not cap.get("ok"):
        print(f"v2 capture failed: {cap.get('error')}")
        return 1

    manifest_url = cap["manifest_url"]
    body = cap["body"]
    referer = cap.get("referer") or ""
    raw_cookies = cap.get("cookies") or []
    print(f"manifest_url: {manifest_url[:100]}")
    print(f"raw cookies from v2 (profile-wide, unfiltered): {len(raw_cookies)}")

    filtered, source_domain = filter_like_store_manifest(raw_cookies, manifest_url, referer, args.url)
    print(f"cookies after _store_manifest-style domain filter: {len(filtered)}  (source_domain={source_domain})")
    for c in filtered:
        print(f"    {c.get('domain'):40s} {c.get('name')}")

    seg_url, media_url = find_segment_url(body, manifest_url)
    if not seg_url:
        print("Could not resolve a real segment URL from the manifest -- inconclusive.")
        return 1
    print(f"Real segment URL to test against: {seg_url[:100]}")
    fetch_target = media_url or manifest_url

    with_session, headers = build_session_and_headers(filtered, source_domain)
    print("\n--- WITH v2 cookies (as ProxyStream would receive them) ---")
    fetch_and_report("manifest/media", with_session, headers, fetch_target)
    fetch_and_report("segment", with_session, headers, seg_url)

    print("\n--- WITHOUT cookies (bare session, same headers) ---")
    fetch_and_report("manifest/media", None, headers, fetch_target)
    fetch_and_report("segment", None, headers, seg_url)

    print("\n--- v1 historical cookies (positive control) ---")
    v1_cookies = fetch_v1_historical_cookies(source_domain)
    print(f"v1 historical cookies found for {source_domain}: {len(v1_cookies)}")
    if v1_cookies:
        v1_session, _ = build_session_and_headers(v1_cookies, source_domain)
        fetch_and_report("manifest/media", v1_session, headers, fetch_target)
        fetch_and_report("segment", v1_session, headers, seg_url)
    else:
        print("  (no v1 historical row found for this domain -- skipping positive control)")

    print("\n=== VERDICT ===")
    print("Compare status/size across WITH / WITHOUT / v1-historical above.")
    print("WITH succeeding (200, real size) while WITHOUT fails/differs (401/403/")
    print("redirect/tiny body) is conclusive proof cookies gate this access point.")
    print("Both succeeding identically means this specific fetch isn't cookie-gated")
    print("-- still useful: narrows where cookies actually matter for this source.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
