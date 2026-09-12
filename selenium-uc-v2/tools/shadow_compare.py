#!/usr/bin/env python3
"""
Shadow/parity comparison harness for sidecar 2.0 vs. v1 (selenium-uc), per
the sidecar-2.0 plan's Sec3/Sec8 verification plan.

Host-side orchestration script (not shipped inside either sidecar's image --
needs the `docker` CLI, which containers don't have). Pulls a fresh sample
of real, recently-active Capture.page_url values from Postgres via the
already-running `channelarr` app container (read-only), then fires /capture
at v1 (sequentially, matching its one-browser-one-lock reality) and v2
(concurrently, its actual design) against the SAME urls, reporting success
rate, #EXTM3U presence, and latency for each side.

Deliberately no hardcoded site names anywhere in this file -- URLs are only
ever pulled live from the DB or passed in by the caller, matching the same
plugin/scraper source-name-out-of-tracked-code convention the rest of the
project follows.

Usage:
  python3 shadow_compare.py --count 10                 # fresh random sample from DB
  python3 shadow_compare.py --count 10 --stagger 30     # wait 30s between v1 and v2 runs
  python3 shadow_compare.py --v1-only --count 10        # skip v2 (e.g. v2 container down)
  python3 shadow_compare.py --v2-only --count 10

Requires: v1 running as `channelarr-selenium-uc` (port 4445), v2 running as
`channelarr-selenium-uc-v2` (port 4446) -- both reached via `docker exec`
against localhost inside each container, never a published host port.
"""
import argparse
import json
import subprocess
import sys
import time


V1_CONTAINER = "channelarr-selenium-uc"
V1_PORT = 4445
V2_CONTAINER = "channelarr-selenium-uc-v2"
V2_PORT = 4446
APP_CONTAINER = "channelarr"


def fetch_sample_urls(count: int, pool: int = 500) -> list[str]:
    """Pull `count` distinct, real, recently-active page_urls from Postgres
    via the already-running app container -- read-only, no writes, no
    schema/data changes. `pool` bounds how far back in updated_at order we
    look before sampling, so the sample stays representative of currently-
    active sources rather than ancient/stale ones."""
    script = f"""
from core.database import get_session
from core.models.manifest import Capture
import random, json

with get_session() as s:
    rows = s.query(Capture.page_url).filter(Capture.page_url.isnot(None)).order_by(Capture.updated_at.desc()).limit({pool}).all()
    urls = list({{r[0] for r in rows}})
    sample = random.sample(urls, min({count}, len(urls)))
    print(json.dumps(sample))
"""
    result = subprocess.run(
        ["docker", "exec", APP_CONTAINER, "python3", "-c", script],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to fetch sample URLs: {result.stderr}")
    return json.loads(result.stdout.strip())


def _run_v1(container: str, port: int, urls: list[str], timeout: int) -> dict:
    """Sequential, matching v1's real one-browser-one-lock behavior --
    running these concurrently against v1 would just queue on its own lock
    anyway, so sequential here measures the same thing it'd do in practice."""
    script = f"""
import requests, time, json
urls = {json.dumps(urls)}
results = []
for i, url in enumerate(urls):
    t0 = time.time()
    try:
        r = requests.post("http://localhost:{port}/capture", json={{"url": url, "timeout": {timeout}, "switch_iframe": True}}, timeout={timeout + 60})
        d = r.json()
        body = d.get("body") or ""
        results.append({{"url": url, "elapsed": time.time()-t0, "ok": d.get("ok"), "has_extm3u": "#EXTM3U" in body, "error": d.get("error")}})
    except Exception as e:
        results.append({{"url": url, "elapsed": time.time()-t0, "ok": False, "has_extm3u": False, "error": str(e)}})
print(json.dumps(results))
"""
    t0 = time.time()
    result = subprocess.run(
        ["docker", "exec", container, "python3", "-c", script],
        capture_output=True, text=True, timeout=(timeout + 60) * len(urls) + 60,
    )
    wall_clock = time.time() - t0
    if result.returncode != 0:
        raise RuntimeError(f"v1 run failed: {result.stderr}")
    return {"results": json.loads(result.stdout.strip()), "wall_clock": wall_clock}


def _run_v2(container: str, port: int, urls: list[str], timeout: int, tab_cap: int = 3) -> dict:
    """Concurrent -- this is the actual point of Phase 2's design, so the
    harness exercises it that way rather than artificially serializing.

    Client-side per-request timeout is scaled by expected queue depth
    (len(urls) / tab_cap waves, each up to timeout+60s) -- a batch larger
    than tab_cap WILL queue some requests behind others (correct, intended
    behavior of a bounded-concurrency system, not a bug), and a client
    timeout sized for a single wave would misreport "queued a while, then
    genuinely succeeded" as a failure. Real, observed regression in an
    earlier run of this harness before this fix."""
    import math
    queue_waves = max(1, math.ceil(len(urls) / max(1, tab_cap)))
    client_timeout = queue_waves * (timeout + 60)
    script = f"""
import concurrent.futures, requests, time, json
urls = {json.dumps(urls)}
def one(url):
    t0 = time.time()
    try:
        r = requests.post("http://localhost:{port}/capture", json={{"url": url, "timeout": {timeout}, "priority": "high"}}, timeout={client_timeout})
        d = r.json()
        body = d.get("body") or ""
        return {{"url": url, "elapsed": time.time()-t0, "ok": d.get("ok"), "has_extm3u": "#EXTM3U" in body, "error": d.get("error")}}
    except Exception as e:
        return {{"url": url, "elapsed": time.time()-t0, "ok": False, "has_extm3u": False, "error": str(e)}}
with concurrent.futures.ThreadPoolExecutor(max_workers=len(urls)) as ex:
    results = list(ex.map(one, urls))
print(json.dumps(results))
"""
    t0 = time.time()
    result = subprocess.run(
        ["docker", "exec", container, "python3", "-c", script],
        capture_output=True, text=True, timeout=client_timeout + 60,
    )
    wall_clock = time.time() - t0
    if result.returncode != 0:
        raise RuntimeError(f"v2 run failed: {result.stderr}")
    return {"results": json.loads(result.stdout.strip()), "wall_clock": wall_clock}


def _report(label: str, run: dict):
    results = run["results"]
    ok_count = sum(1 for r in results if r["ok"])
    print(f"\n--- {label} ---")
    for r in results:
        status = "OK  " if r["ok"] else "FAIL"
        print(f"  [{status}] {r['elapsed']:6.1f}s  {r['url'][:70]}"
              + (f"  ({r['error']})" if r["error"] else ""))
    print(f"  => {ok_count}/{len(results)} succeeded, wall-clock {run['wall_clock']:.1f}s")
    return ok_count, len(results)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--count", type=int, default=10, help="number of real URLs to sample (default 10)")
    ap.add_argument("--timeout", type=int, default=40, help="per-capture timeout seconds passed to each sidecar (default 40)")
    ap.add_argument("--stagger", type=int, default=0, help="seconds to wait between v1 and v2 runs (default 0 = run v1 then immediately v2)")
    ap.add_argument("--v1-only", action="store_true")
    ap.add_argument("--v2-only", action="store_true")
    ap.add_argument("--urls", type=str, default=None, help="comma-separated URLs instead of sampling from the DB")
    ap.add_argument("--tab-cap", type=int, default=3, help="expected SIDECAR_MAX_TABS on the v2 side, for client-timeout scaling (default 3)")
    args = ap.parse_args()

    if args.urls:
        urls = [u.strip() for u in args.urls.split(",") if u.strip()]
    else:
        print(f"Sampling {args.count} real, recently-active page_urls from Postgres...")
        urls = fetch_sample_urls(args.count)
    print(f"Testing {len(urls)} URLs:")
    for u in urls:
        print(f"  {u}")

    v1_ok = v1_total = v2_ok = v2_total = None

    if not args.v2_only:
        run = _run_v1(V1_CONTAINER, V1_PORT, urls, args.timeout)
        v1_ok, v1_total = _report("v1 (sequential)", run)

    if args.stagger and not args.v1_only and not args.v2_only:
        print(f"\nStaggering {args.stagger}s before v2 run...")
        time.sleep(args.stagger)

    if not args.v1_only:
        run = _run_v2(V2_CONTAINER, V2_PORT, urls, args.timeout, tab_cap=args.tab_cap)
        v2_ok, v2_total = _report("v2 (concurrent)", run)

    print("\n=== SUMMARY ===")
    if v1_total is not None:
        print(f"v1: {v1_ok}/{v1_total} ({100*v1_ok/v1_total:.0f}%)")
    if v2_total is not None:
        print(f"v2: {v2_ok}/{v2_total} ({100*v2_ok/v2_total:.0f}%)")
    print("Note: real content-availability variance minute-to-minute is a")
    print("genuine confound -- treat any single run as directional, not precise.")


if __name__ == "__main__":
    sys.exit(main())
