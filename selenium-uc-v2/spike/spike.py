"""
Sidecar 2.0 -- Phase 0 spike. Throwaway, disposable (see the sidecar-2.0 plan,
section 4). Answers four empirical questions before any pool/dispatcher code
is written:

  (a) Does nodriver's event-driven Network.RequestWillBeSent/ResponseReceived
      capture see an m3u8 manifest request as reliably as the current
      selenium-uc's 80ms-poll-of-get_log("performance") approach did?
  (b) Does the "CDP getResponseBody hangs on a cross-origin iframe's target"
      workaround (used 3x in selenium-uc/app.py) reproduce under nodriver?
  (c) How does nodriver actually expose/enumerate iframe targets in practice?
  (d) Crash isolation: if one tab in a shared instance gets wedged/crashed,
      do OTHER tabs sharing that instance keep working? This is the central
      bet of the whole "multi-tab in a shared instance" architecture choice
      (see the plan's section 1) -- if this fails, escalate back to the
      process-pool alternative before writing any more code.

Run standalone: docker build -t sidecar2-spike . && docker run --rm
  -v /mnt/projects/apps/channelarr/scrapers:/app/scrapers:ro \
  --network container:channelarr-vpn -e DISPLAY_NUM=98 sidecar2-spike
No compose integration, no ports, no channelarr dependency beyond that
mount -- fleet test URLs are loaded at runtime from the gitignored plugin
file below (same SCRAPERS_DIR pattern core/resolver/manifest_resolver.py's
_native_resolver() uses), so this file itself never names a real source.
"""

import asyncio
import base64
import importlib.util
import os
import time
import traceback

import nodriver as uc
import requests

from session_attach import attach_to_target

SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/app/scrapers")


def _load_fleet_config():
    """Load the gitignored, site-specific test-fleet module -- same pattern
    as manifest_resolver.py's _native_resolver(), so this spike's own source
    never names a real target site. Fails loudly (not gracefully) since,
    unlike the production native-resolver plugin, this spike has no reason
    to run at all without real fleet data to test against."""
    path = os.path.join(SCRAPERS_DIR, "_sidecar2_fleet_config.py")
    if not os.path.isfile(path):
        raise SystemExit(
            f"No fleet config at {path} -- mount the scrapers/ dir "
            f"(see this file's module docstring for the docker run invocation)."
        )
    spec = importlib.util.spec_from_file_location("_sidecar2_fleet_config", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_fleet = _load_fleet_config()
FLEET_URLS = _fleet.FLEET_URLS
CRASH_TEST_URL = _fleet.CRASH_TEST_URL
BENIGN_TEST_URLS = _fleet.BENIGN_TEST_URLS

# -- Ported from selenium-uc/app.py -- pure Python, no selenium/nodriver dependency --
MATCH_PATTERNS = ("m3u8", "application/x-mpegurl", "application/vnd.apple.mpegurl")
JSON_STREAM_PATTERNS = ("ngtv.io", "/api/", "/media/", "/stream", "anvato", "uplynk", "/proxy/")

# Ported from selenium-uc/app.py's iframe-candidate selection -- skip known
# ad/chat/analytics iframe srcs so the "best" candidate isn't a decoy.
IFRAME_SKIP_SUBSTRINGS = (
    "chatango.com", "adbanner", "/ads/", "/ad-", "google.com/recaptcha",
    "doubleclick.net", "googletagmanager.com", "googlesyndication", "googleadservices",
)
IFRAME_SKIP_SCHEMES = ("javascript:", "about:", "data:", "blob:")


def _matches(url: str, mime: str = "") -> bool:
    hay = f"{url} {mime}".lower()
    return any(p in hay for p in MATCH_PATTERNS)


def _looks_like_json_stream(url: str) -> bool:
    return any(p in url.lower() for p in JSON_STREAM_PATTERNS)


class CaptureResult:
    def __init__(self, url):
        self.url = url
        self.found = False
        self.manifest_url = None
        self.method = None  # "cdp_body" | "short_circuit_http"
        self.via = None  # "top" | "iframe" -- which target the success came from
        self.body_ok = False
        self.elapsed_s = None
        self.error = None
        self.candidates_seen = 0


# Ported from selenium-uc/app.py::_try_click_play -- most fleet sites are
# click-to-play, not autoplay (confirmed by run #1 of this spike: everything
# except one autoplaying site MISSed with candidates_seen>0 but no m3u8 ever
# fired -- the page loaded, the player never started). Same selector list,
# same order, same JS fallback.
PLAY_SELECTORS = (
    ".play-button", ".vjs-big-play-button", ".jw-icon-display", "[class*='play']",
    "button[aria-label*='play' i]", ".btn-play", "#play-btn",
    ".plyr__control--overlaid", "video", ".video-player", ".player", "#player",
    ".jw-wrapper",
)


async def _find_iframe_targets(browser):
    """(c) -- how nodriver actually exposes iframe targets.

    Runs #1-4 used `browser.tabs`, which nodriver's own browser.py defines as
    `filter(lambda item: item.type_ == "page", self.targets)` -- i.e. .tabs
    DELIBERATELY filters iframe-type entries OUT. The real, UNFILTERED list
    is `browser.targets`, populated by update_targets() from a plain
    `Target.getTargets()` call (discovery was already enabled via
    `Target.setDiscoverTargets(discover=True)` at browser startup, per
    browser.py) -- so iframe/OOPIF targets should already be present there
    without needing any extra setAutoAttach wiring, IF Chrome actually
    creates a distinct target for that iframe (true for cross-origin/OOPIF
    iframes, not for same-origin ones, which stay in the parent's own
    execution context and don't need this at all).

    Non-page entries in browser.targets are raw `Connection` objects (per
    browser.py's update_targets(), which literally does
    `self.targets.append(Connection(ws_url, target=t, browser=self))` for
    anything new) -- NOT full `Tab` objects. `Connection` has `.send()` and
    `.add_handler()` (both defined on the Connection base class Tab
    subclasses), so network capture works identically either way. It does
    NOT have Tab-only convenience methods (`.select()`/`.evaluate()`), which
    is why click-play below goes through raw `Runtime.evaluate` instead --
    that works uniformly on both a full Tab and a bare Connection."""
    await browser.update_targets()
    all_targets = [(getattr(t.target, "type_", None), str(getattr(t.target, "url", "") or ""))
                   for t in browser.targets]
    print(f"    all targets seen ({len(all_targets)}, unfiltered): "
          f"{[(ty, u[:60]) for ty, u in all_targets]}")

    iframe_targets = [(t, u) for t, (ty, u) in zip(browser.targets, all_targets) if ty == "iframe"]
    if not iframe_targets:
        return None

    def _skip(url_):
        low = url_.lower()
        if any(low.startswith(s) for s in IFRAME_SKIP_SCHEMES):
            return True
        return any(s in low for s in IFRAME_SKIP_SUBSTRINGS)

    good = [t for t, u in iframe_targets if not _skip(u)]
    chosen = good[0] if good else iframe_targets[0][0]
    print(f"    picked iframe target: {str(getattr(chosen.target, 'url', ''))[:100]}")
    # Return the target_id, not the Connection object nodriver appended into
    # browser.targets -- that object's direct per-target websocket 404s for
    # non-page targets (confirmed empirically, run #6). Real access is via
    # Target.attachToTarget(flatten=True) -- see session_attach.py.
    return chosen.target.target_id


_CLICK_PLAY_JS = """
(function(){
  var sels = %s;
  for (var i=0;i<sels.length;i++){
    try {
      var el = document.querySelector(sels[i]);
      if (el) { el.click(); if (el.play) { el.play().catch(function(){}); } return sels[i]; }
    } catch(e) {}
  }
  var v = document.querySelector('video');
  if (v) { v.click(); if (v.play) v.play().catch(function(){}); return 'video-fallback'; }
  return null;
})()
""" % (list(PLAY_SELECTORS),)


async def _try_click_play(connection):
    """Works uniformly on a full Tab OR a bare Connection (iframe target) --
    see _find_iframe_targets' docstring for why .select()/.evaluate() (Tab-
    only) aren't used here."""
    try:
        result = await connection.send(
            uc.cdp.runtime.evaluate(expression=_CLICK_PLAY_JS, return_by_value=True,
                                     await_promise=True, user_gesture=True)
        )
        remote_obj = result[0] if isinstance(result, tuple) else result
        clicked = getattr(remote_obj, "value", None)
        if clicked:
            print(f"    clicked play candidate: {clicked}")
            await asyncio.sleep(2)
            return True
        return False
    except Exception as e:
        print(f"    click-play evaluate failed: {e}")
        return False


def _try_short_circuit_fetch(url, headers, timeout=10):
    """Mirrors app.py's plain-HTTP-refetch workaround -- fetch the manifest
    directly with the captured request headers instead of asking CDP for the
    body, since CDP's getResponseBody hangs when the request lives on a
    different Target (cross-origin iframe) than the one we're attached to."""
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        return resp.status_code, resp.text
    except Exception as e:
        return None, str(e)


def _make_capture_handlers(capture_tab, result, found_event, label):
    """Factory so the same on_request/on_response logic can be attached to
    EITHER the main page tab or a drilled-into iframe target -- each needs
    its own closure over `capture_tab` because get_response_body must be
    called against whichever Target the matching response actually arrived
    on, not always the top-level tab."""
    req_meta = {}

    async def on_request(event):
        try:
            req_meta[event.request_id] = dict(getattr(event.request, "headers", {}) or {})
        except Exception:
            pass

    async def on_response(event):
        if found_event.is_set():
            return
        try:
            rid = event.request_id
            resp = event.response
            url_ = resp.url
            mime = getattr(resp, "mime_type", "") or ""
            if not (_matches(url_, mime) or _looks_like_json_stream(url_)):
                return
            result.candidates_seen += 1
            headers = req_meta.get(rid, {})

            if ".m3u8" in url_.lower():
                cdp_start = time.time()
                cdp_body = None
                try:
                    cdp_result = await asyncio.wait_for(
                        capture_tab.send(uc.cdp.network.get_response_body(request_id=rid)),
                        timeout=5.0,
                    )
                    if cdp_result:
                        body_str, b64 = cdp_result
                        cdp_body = base64.b64decode(body_str) if b64 else body_str.encode()
                except asyncio.TimeoutError:
                    print(f"    [{label}][CDP] get_response_body TIMED OUT after 5s for {url_[:80]}")
                except Exception as e:
                    print(f"    [{label}][CDP] get_response_body errored: {e}")
                cdp_elapsed = time.time() - cdp_start

                if cdp_body and b"#EXTM3U" in cdp_body:
                    result.method = "cdp_body"
                    result.body_ok = True
                    result.manifest_url = url_
                    print(f"    [{label}][CDP] body fetch OK in {cdp_elapsed:.2f}s")
                else:
                    status, body = _try_short_circuit_fetch(url_, headers)
                    if body and "#EXTM3U" in body:
                        result.method = "short_circuit_http"
                        result.body_ok = True
                        result.manifest_url = url_
                        slow = "timed out" if cdp_elapsed >= 4.9 else "failed"
                        print(f"    [{label}][HTTP fallback] worked (CDP {slow} after {cdp_elapsed:.2f}s)")

                if result.body_ok:
                    result.found = True
                    result.via = label
                    found_event.set()
        except Exception as e:
            print(f"    [{label}] on_response error: {e}")

    return on_request, on_response


async def _attach_capture(capture_tab, result, found_event, label):
    on_request, on_response = _make_capture_handlers(capture_tab, result, found_event, label)
    await capture_tab.send(uc.cdp.network.enable())
    capture_tab.add_handler(uc.cdp.network.RequestWillBeSent, lambda e: asyncio.create_task(on_request(e)))
    capture_tab.add_handler(uc.cdp.network.ResponseReceived, lambda e: asyncio.create_task(on_response(e)))


async def spike_single_capture(url: str, timeout: int = 30) -> CaptureResult:
    """(a) event-driven capture reliability, (b) cross-origin body-fetch hang,
    (c) iframe target enumeration/drilling."""
    result = CaptureResult(url)
    t0 = time.time()
    found_event = asyncio.Event()
    browser = None

    try:
        browser = await uc.start(headless=False)
        tab = browser.main_tab
        await _attach_capture(tab, result, found_event, "top")

        await tab.get(url)
        await asyncio.sleep(3)  # let the page settle before hunting for iframes/play controls

        # (c) -- drill into an iframe if one looks like the real player, same
        # skip-substring selection app.py uses. Capture handlers get attached
        # to the iframe's OWN session (via Target.attachToTarget(flatten=True),
        # see session_attach.py) since that's where its network traffic
        # actually lands under CDP's per-target model, and nodriver's naive
        # direct-connection approach 404s for non-page targets.
        iframe_tab = None
        if not found_event.is_set():
            iframe_target_id = await _find_iframe_targets(browser)
            if iframe_target_id is not None:
                try:
                    iframe_tab = await attach_to_target(tab, iframe_target_id)
                    await _attach_capture(iframe_tab, result, found_event, "iframe")
                except Exception as e:
                    print(f"    session-attach to iframe failed: {e}")

        if not found_event.is_set():
            clicked = await _try_click_play(iframe_tab or tab)
            if not clicked and iframe_tab is not None:
                await _try_click_play(tab)  # try the top page too, cheap fallback

        try:
            await asyncio.wait_for(found_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            result.error = f"no manifest found within {timeout}s"

    except Exception as e:
        result.error = str(e)
        traceback.print_exc()
    finally:
        result.elapsed_s = time.time() - t0
        if browser is not None:
            try:
                browser.stop()
            except Exception:
                pass

    return result


async def spike_crash_isolation():
    """(d) -- the central bet of the multi-tab-in-shared-instance architecture.
    Open 3 tabs in ONE instance: 2 benign, 1 deliberately pointed at a known
    flaky/crash-correlated site. Confirm the 2 benign tabs' captures still
    succeed while/after the flaky one runs, in the SAME browser process."""
    print("\n=== (d) crash-isolation test: 1 instance, 3 tabs, 1 deliberately bad ===")
    browser = await uc.start(headless=False)
    outcomes = {}
    try:
        async def capture_on_tab(tab, url, label):
            result = CaptureResult(url)
            found_event = asyncio.Event()
            await _attach_capture(tab, result, found_event, label)

            try:
                await tab.get(url)
                await asyncio.sleep(3)
                # Deliberately NOT doing iframe drilling here (unlike
                # spike_single_capture) -- _find_iframe_targets enumerates
                # browser.tabs *browser-wide*, not scoped to this one tab's
                # own iframes, which would be ambiguous with 3 tabs
                # navigating concurrently. Test URLs below are chosen to be
                # ones that already succeed without drilling (see run #4),
                # so this isolation test isn't confounded by that gap.
                if not found_event.is_set():
                    await _try_click_play(tab)
                await asyncio.wait_for(found_event.wait(), timeout=25)
            except asyncio.TimeoutError:
                result.error = "timeout"
            except Exception as e:
                result.error = str(e)

            outcomes[label] = {"found": result.found, "error": result.error}
            print(f"    [{label}] found={result.found} error={result.error}")

        main_tab = browser.main_tab
        bad_tab = await browser.get("about:blank", new_tab=True)
        good_tab_2 = await browser.get("about:blank", new_tab=True)

        # Fire all three concurrently -- the bad one is deliberately the flaky
        # crash-correlated site; if it wedges/crashes its OWN tab/target, the
        # other two should be unaffected if per-tab isolation actually holds.
        await asyncio.gather(
            capture_on_tab(main_tab, BENIGN_TEST_URLS[0], "benign-1 (main_tab)"),
            capture_on_tab(bad_tab, CRASH_TEST_URL, "BAD (crash_test_url, expected flaky)"),
            capture_on_tab(good_tab_2, BENIGN_TEST_URLS[1], "benign-2 (new_tab)"),
            return_exceptions=True,
        )

        benign_ok = (outcomes.get("benign-1 (main_tab)", {}).get("found")
                     and outcomes.get("benign-2 (new_tab)", {}).get("found"))
        print(f"\n    VERDICT: both benign tabs survived the bad tab = {benign_ok}")
        if not benign_ok:
            print("    >>> ISOLATION FAILURE -- a wedge/crash in one tab took others down too.")
            print("    >>> Per the plan's go/no-go gate: escalate back to the process-pool")
            print("    >>> alternative before writing any more pool/dispatcher code.")
    finally:
        try:
            browser.stop()
        except Exception:
            pass

    return outcomes


async def main():
    print("=" * 70)
    print("SIDECAR 2.0 -- PHASE 0 SPIKE")
    print("=" * 70)

    print("\n=== (a)+(b): single-capture reliability + cross-origin body-fetch hang ===")
    results = []
    for label, entry in FLEET_URLS.items():
        url = entry["url"]
        print(f"\n--- {label}: {url}")
        r = await spike_single_capture(url, timeout=30)
        results.append((label, entry, r))
        print(f"    found={r.found} method={r.method} candidates_seen={r.candidates_seen} "
              f"elapsed={r.elapsed_s:.1f}s error={r.error}")

    print("\n\n" + "=" * 70)
    print("SUMMARY -- (a)/(b)")
    print("=" * 70)
    for label, entry, r in results:
        status = "OK" if r.found else "MISS"
        err = f" error={r.error}" if r.error else ""
        expected = entry.get("needs_iframe")
        via_mismatch = ""
        if r.found and expected is not None:
            actually_needed_iframe = (r.via == "iframe")
            if actually_needed_iframe != expected:
                via_mismatch = f" [expected needs_iframe={expected}, actually via={r.via}]"
        print(f"  [{status}] {label:20s} method={r.method or '-':20s} via={r.via or '-':7s} "
              f"candidates={r.candidates_seen} elapsed={r.elapsed_s:.1f}s{err}{via_mismatch}")

    await spike_crash_isolation()

    print("\nDone.")


if __name__ == "__main__":
    loop_factory = getattr(uc, "loop", None)
    if callable(loop_factory):
        loop_factory().run_until_complete(main())
    else:
        asyncio.run(main())
