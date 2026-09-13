"""multiplex — browser-tab video relay.

For sources whose real manifest/stream request is never visible to network-
level capture at all (WASM-obfuscated players, client-side-only decryption,
etc. — confirmed live this session against a real source: normal /capture
found only ad/tracker noise, and a blind play-button click never actually
started playback). Instead of trying to find a manifest URL, this drives a
real tab to REAL playback and captures the DECODED output straight off the
<video> element via captureStream()+MediaRecorder — exactly what a real
viewer's browser produces. core.resolver.segment_sources.TabRelaySource
polls this session's accumulated chunks and feeds them to its own ffmpeg,
the same "gentle mirror" shape as ContinuousRelaySource.

The click-sequence vocabulary and JS below are ported from the dead
selenium-uc/tab_proxy.py engine (built for exactly this problem, on the
old selenium/chromedriver sidecar, never actually landed because that
engine's own reliability was the blocker — not this config). Real,
previously-reasoned-through site knowledge (nested same-origin iframe
players, "2-click popunder-bait" patterns, modal/popup dismissal) is worth
carrying forward rather than re-deriving from scratch; see the gitignored
per-site scraper config the old engine's TAB_PROXY_CONFIG lived in for
the concrete site this vocabulary was designed against.

Deliberately source-name-free (see project public-repo hygiene convention)
— per-site click sequences live in the gitignored
scrapers/_tab_relay_configs.py, not here.

A relay session is long-lived (the tab stays open, playing, for the whole
channel's runtime) — a fundamentally different lifecycle than every other
capture in this service (open, single-shot, close). app.py tracks these
separately from the ephemeral-capture tab-slot pool.
"""

import asyncio
import base64
import json
import logging

import nodriver as uc

from network_capture import _find_iframe_candidates
from session_attach import attach_to_target

logger = logging.getLogger(__name__)

# How long to wait for the page to settle before starting the click
# sequence — matches network_capture.py's own post-navigate settle sleep.
PAGE_SETTLE_SECONDS = 3

# Ported from selenium-uc/tab_proxy.py's _CAPTURE_STREAM_JS, adapted to
# accumulate base64 chunks in a page-global array for polling (this
# service pulls chunks via Runtime.evaluate rather than having the page's
# own JS push them over the network — sidesteps target-site CSP/CORS
# restrictions on outbound fetch entirely, since nothing here crosses
# origins from the page's own script). Superior to a one-shot top-level-
# only attempt in two ways worth keeping: (1) searches same-origin
# iframes' <video> elements too (via contentDocument — cross-origin
# iframes throw, caught and skipped, same as the old engine); (2) retries
# for up to 120s rather than giving up after one check, since the player
# may take a while to actually start rendering after the click sequence.
_RELAY_START_JS = r"""
(function() {
  if (window.__relay) return JSON.stringify({ok: true, already: true});

  function findPlayingVideo() {
    var list = [];
    try { list = list.concat(Array.prototype.slice.call(document.querySelectorAll('video'))); } catch(e) {}
    try {
      var ifs = document.querySelectorAll('iframe');
      for (var i = 0; i < ifs.length; i++) {
        try {
          var d = ifs[i].contentDocument;
          if (d) list = list.concat(Array.prototype.slice.call(d.querySelectorAll('video')));
        } catch(e) {}
      }
    } catch(e) {}
    for (var i = 0; i < list.length; i++) {
      var v = list[i];
      if (v.readyState >= 2 && !v.paused && v.videoWidth > 0 && v.videoHeight > 0) return v;
    }
    return null;
  }

  function attach(video) {
    var stream;
    try {
      stream = video.captureStream ? video.captureStream()
             : (video.mozCaptureStream ? video.mozCaptureStream() : null);
    } catch (e) {
      window.__relay.state.error = 'captureStream() threw: ' + e;
      return false;
    }
    if (!stream) { window.__relay.state.error = 'captureStream() unsupported'; return false; }
    var mimes = ['video/webm;codecs=vp9,opus', 'video/webm;codecs=vp8,opus', 'video/webm'];
    var mimeType = '';
    for (var i = 0; i < mimes.length; i++) {
      if (window.MediaRecorder && MediaRecorder.isTypeSupported(mimes[i])) { mimeType = mimes[i]; break; }
    }
    if (!mimeType) { window.__relay.state.error = 'no supported webm mimeType'; return false; }
    var recorder;
    try {
      recorder = new MediaRecorder(stream, {mimeType: mimeType});
    } catch (e) {
      window.__relay.state.error = 'MediaRecorder() threw: ' + e;
      return false;
    }
    recorder.ondataavailable = function(e) {
      if (e.data && e.data.size > 0) {
        var reader = new FileReader();
        reader.onload = function() {
          // NOT reader.result.split(',')[1], and NOT indexOf(',') either
          // -- the data: URL's own MIME type can itself contain a comma
          // (e.g. "codecs=vp9,opus"), which comes BEFORE the real
          // ";base64," separator. Both of those naive splits land on
          // that inner comma, leaving "opus;base64," prepended to the
          // real payload -- invalid base64 characters (the semicolon)
          // that Python's lenient b64decode silently drops rather than
          // rejecting, shifting the length just enough to break padding
          // on every single chunk. The base64 payload itself can NEVER
          // contain a comma (comma isn't in the base64 alphabet), so the
          // LAST comma in the string is always the real, unambiguous
          // separator, regardless of how many commas the MIME type has.
          var r = reader.result;
          window.__relay.state.chunks.push(r.substring(r.lastIndexOf(',') + 1));
        };
        reader.readAsDataURL(e.data);
      }
    };
    recorder.onstop = function() { window.__relay.state.ended = true; };
    recorder.onerror = function(e) { window.__relay.state.ended = true; window.__relay.state.error = String(e); };
    // 1s timeslice -- confirmed via direct testing that Runtime.evaluate
    // round-trips a 500KB+ string cleanly; the "large chunks corrupt"
    // theory (which briefly lived here as 250ms + a faster poll
    // interval) was wrong. The real bug was the data: URL comma-split
    // above -- fixed at the source, no need to trade quality/overhead
    // for smaller chunks.
    recorder.start(1000);
    window.__relay.recorder = recorder;
    window.__relay.state.mimeType = mimeType;
    return true;
  }

  window.__relay = {recorder: null, state: {chunks: [], ended: false, error: null, mimeType: null}};

  var attempts = 0;
  var MAX_ATTEMPTS = 240;  // 120s -- player may take a while to start rendering
  window.__relayTimer = setInterval(function() {
    attempts++;
    var v = findPlayingVideo();
    if (v && attach(v)) { clearInterval(window.__relayTimer); return; }
    if (attempts >= MAX_ATTEMPTS) {
      clearInterval(window.__relayTimer);
      if (!window.__relay.state.error) window.__relay.state.error = 'timed out finding a playing <video>';
      // No video ever found in THIS context -- nothing more will ever
      // happen here. Distinct from a recorder starting then later
      // stopping/erroring, but the same "this context is done" signal
      // poll_relay needs to eventually treat the whole session as over
      // once every context has given up.
      window.__relay.state.ended = true;
    }
  }, 500);

  return JSON.stringify({ok: true, pending: true});
})()
"""

# Drains and clears accumulated chunks -- each poll naturally returns only
# what's arrived since the previous poll, no sequence numbers needed.
_RELAY_POLL_JS = """
(function() {
  if (!window.__relay) return JSON.stringify({ok: false, chunks: [], ended: true});
  var out = window.__relay.state.chunks;
  window.__relay.state.chunks = [];
  return JSON.stringify({ok: true, chunks: out, ended: window.__relay.state.ended,
                          error: window.__relay.state.error,
                          recording: !!window.__relay.recorder});
})()
"""

_RELAY_STOP_JS = """
(function() {
  if (window.__relayTimer) clearInterval(window.__relayTimer);
  if (window.__relay && window.__relay.recorder && window.__relay.recorder.state !== 'inactive') {
    try { window.__relay.recorder.stop(); } catch (e) {}
  }
  return JSON.stringify({ok: true});
})()
"""

# ── Click-sequence action JS, ported verbatim (behavior-equivalent) from
# selenium-uc/tab_proxy.py's _IFRAME_CLICK_JS / _MODAL_DISMISS_JS ────────

_IFRAME_CLICK_JS = r"""
(function() {
    var out = [];
    var ifs = document.querySelectorAll('iframe');
    var sels = [
        ".vjs-big-play-button", ".jw-icon-display", ".plyr__control--overlaid",
        "button[aria-label*='play' i]", ".play-button", "#play-btn",
        ".btn-play", ".player-container", ".video-player", ".player",
        "#player", ".jw-wrapper", "video"
    ];
    for (var i = 0; i < ifs.length; i++) {
        var d = null;
        try { d = ifs[i].contentDocument; } catch(e) { continue; }
        if (!d) continue;
        for (var j = 0; j < sels.length; j++) {
            var el = d.querySelector(sels[j]);
            if (el) {
                var r = el.getBoundingClientRect();
                if (r.width > 0 && r.height > 0) {
                    try {
                        el.click();
                        if (el.tagName === 'VIDEO' && el.play) el.play().catch(function(){});
                        out.push('iframe:' + sels[j]);
                    } catch(e) {}
                }
            }
        }
    }
    return JSON.stringify(out);
})()
"""

_MODAL_DISMISS_JS = r"""
(function() {
    var clicked = [];
    var sels = [
        "button[aria-label*='close' i]", "button[aria-label*='dismiss' i]",
        "[aria-label='Close ad' i]",
        ".ad-popup-close", ".modal__close", ".modal-close", ".btn-close",
        ".close-button", ".overlay__close", ".popup-close", ".dialog-close",
        ".close-icon", ".icon-close", ".fa-times", ".fa-xmark",
        "[data-dismiss='modal']", "[data-close='true']",
        "#adPopupClose", "#closeBtn", "#ad-close", ".ad-close",
        "button.close", "a.close"
    ];
    for (var i = 0; i < sels.length; i++) {
        var nodes = document.querySelectorAll(sels[i]);
        for (var j = 0; j < nodes.length; j++) {
            var el = nodes[j];
            var r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0) {
                try { el.click(); clicked.push(sels[i]); } catch(e) {}
            }
        }
    }
    var nodes = document.querySelectorAll("[aria-label], [title]");
    for (var i = 0; i < nodes.length; i++) {
        var el = nodes[i];
        var label = ((el.getAttribute('aria-label') || '') + ' ' +
                     (el.getAttribute('title') || '')).toLowerCase();
        if (/\b(close|dismiss)\b/.test(label)) {
            var r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0 && r.width < 100 && r.height < 100) {
                try { el.click(); clicked.push('label:'+label.slice(0,30)); } catch(e) {}
            }
        }
    }
    return JSON.stringify(clicked);
})()
"""

_IFRAME_CENTER_RECT_JS = r"""
(function() {
    var f = document.querySelector('iframe');
    if (!f) return null;
    var r = f.getBoundingClientRect();
    if (r.width <= 0 || r.height <= 0) return null;
    return JSON.stringify({x: r.left + r.width / 2, y: r.top + r.height / 2});
})()
"""

# Same default this problem shape has always used: click inside the
# (same-origin-reachable) player DOM, then a trusted top-level click at
# the iframe's screen position twice (the "2-click popunder-bait" pattern
# a real site this was reasoned through against uses), closing any
# popunder tabs that spawn from either click before it can steal focus/
# resources.
DEFAULT_CLICK_SEQUENCE = [
    {"action": "iframe_dom_click"},
    {"action": "click_iframe_center"},
    {"action": "delay", "seconds": 1.0},
    {"action": "close_popups"},
    {"action": "click_iframe_center"},
    {"action": "delay", "seconds": 0.5},
    {"action": "close_popups"},
]


class _IsolatedWorldContext:
    """A CDP execution context scoped to one specific frame, created via
    Page.createIsolatedWorld(grant_univeral_access=True).

    Real, confirmed-live reason this exists (not a hypothetical): this
    browser runs with Site Isolation disabled (see app.py's uc.start()
    browser_args -- deliberately, so network_capture.py's top-level tab
    capture can see cross-origin iframes' network traffic without per-
    iframe session-attach). That means a same-process cross-origin
    iframe has NO separate CDP Target at all -- _find_iframe_candidates
    (browser.targets) returns nothing for it, so Target.attachToTarget
    (session_attach.py) has nothing to attach to. But the JS-level
    same-origin policy blocking contentDocument access is enforced
    independent of process placement -- confirmed live: a real source's
    actual <video> lived in a genuinely cross-origin iframe, reachable by
    neither contentDocument (blocked, same-origin policy) NOR
    attach_to_target (no separate target to attach to). An isolated
    world with grant_univeral_access=True is CDP's own answer to exactly
    this gap -- it runs JS inside that specific frame's real DOM/window,
    bypassing the origin check, without needing a separate target."""

    def __init__(self, tab, context_id, frame_url: str = ""):
        self.tab = tab
        self.context_id = context_id
        self.frame_url = frame_url


async def _eval_json(ctx, expression: str):
    """Same tuple-vs-object unwrap dance as network_capture.py's own
    helper of the same name -- duplicated here rather than imported so
    this module stays independently readable (it's a small helper).
    Accepts a Tab, an AttachedSession (session_attach.py), or an
    _IsolatedWorldContext -- all end up calling Runtime.evaluate, just
    scoped differently."""
    if isinstance(ctx, _IsolatedWorldContext):
        result = await ctx.tab.send(uc.cdp.runtime.evaluate(
            expression=expression, return_by_value=True, context_id=ctx.context_id))
    else:
        result = await ctx.send(uc.cdp.runtime.evaluate(expression=expression, return_by_value=True))
    remote_obj = result[0] if isinstance(result, tuple) else result
    raw = getattr(remote_obj, "value", None)
    if not raw:
        return None
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return raw
    return raw


def _flatten_frame_tree(node) -> list:
    """Page.getFrameTree() returns a recursive FrameTree; flatten to a
    plain list of Frame objects, root (the top-level page) included."""
    out = [node.frame]
    for child in (node.child_frames or []):
        out.extend(_flatten_frame_tree(child))
    return out


async def _find_child_frame_contexts(tab) -> list:
    """Every non-root, real http(s) frame gets its own isolated world --
    covers same-process cross-origin iframes (see _IsolatedWorldContext's
    docstring) as well as same-origin ones (an isolated world works
    there too, it's just not the only way to reach them)."""
    try:
        tree = await tab.send(uc.cdp.page.get_frame_tree())
    except Exception as e:
        logger.warning("[RELAY] get_frame_tree failed: %s", e)
        return []
    frames = _flatten_frame_tree(tree)
    root_id = tree.frame.id_
    contexts = []
    for frame in frames:
        if frame.id_ == root_id:
            continue
        url = frame.url or ""
        # Real, confirmed-live case this was too strict for: a sandboxed
        # srcdoc iframe (common for isolating a 3rd-party player library)
        # reports as "about:srcdoc" or empty in the frame tree, not a real
        # http(s) URL -- but DOES have its own real, isolatable execution
        # context, and can be exactly where the actual player lives. Only
        # exclude genuinely inert frames (blank/decoy), not anything with
        # real document content.
        if url in ("", "about:blank", "chrome://newtab/"):
            continue
        try:
            context_id = await tab.send(uc.cdp.page.create_isolated_world(
                frame_id=frame.id_, world_name="relay", grant_univeral_access=True))
            contexts.append(_IsolatedWorldContext(tab, context_id, url))
        except Exception as e:
            logger.debug("[RELAY] create_isolated_world failed for %s: %s", url[:80], e)
    return contexts


async def _dispatch_click(tab, x: float, y: float):
    await tab.send(uc.cdp.input_.dispatch_mouse_event(type_="mouseMoved", x=x, y=y))
    await asyncio.sleep(0.05)
    await tab.send(uc.cdp.input_.dispatch_mouse_event(
        type_="mousePressed", x=x, y=y,
        button=uc.cdp.input_.MouseButton.LEFT, click_count=1,
    ))
    await asyncio.sleep(0.05)
    await tab.send(uc.cdp.input_.dispatch_mouse_event(
        type_="mouseReleased", x=x, y=y,
        button=uc.cdp.input_.MouseButton.LEFT, click_count=1,
    ))


async def _click_iframe_center(tab) -> bool:
    """Ported from tab_proxy.py's _cdp_click_iframe_center: a trusted
    CDP click at the first <iframe> element's center, in the TOP-level
    page's own coordinate space -- this is the "popunder bait" click,
    distinct from iframe_dom_click's same-origin-DOM click below."""
    try:
        rect = await _eval_json(tab, _IFRAME_CENTER_RECT_JS)
        if not rect:
            return False
        await _dispatch_click(tab, float(rect["x"]), float(rect["y"]))
        return True
    except Exception as e:
        logger.debug("[RELAY] click_iframe_center failed: %s", e)
        return False


async def _iframe_dom_click(tab) -> list:
    """Ported from tab_proxy.py's _IFRAME_CLICK_JS: clicks known
    play-button selectors inside any SAME-ORIGIN iframe (cross-origin
    throws on contentDocument access, caught and skipped per-iframe)."""
    try:
        result = await _eval_json(tab, _IFRAME_CLICK_JS)
        return result or []
    except Exception as e:
        logger.debug("[RELAY] iframe_dom_click failed: %s", e)
        return []


async def _dismiss_modals(tab):
    """Ported from tab_proxy.py's _dismiss_modals: Escape key first (fast
    path for well-behaved modals), then a generic JS sweep for common
    close-button selectors / aria-label hints."""
    try:
        await tab.send(uc.cdp.input_.dispatch_key_event(
            type_="keyDown", key="Escape", code="Escape",
            windows_virtual_key_code=27, native_virtual_key_code=27,
        ))
        await tab.send(uc.cdp.input_.dispatch_key_event(
            type_="keyUp", key="Escape", code="Escape",
            windows_virtual_key_code=27, native_virtual_key_code=27,
        ))
        await asyncio.sleep(0.2)
    except Exception as e:
        logger.debug("[RELAY] escape dispatch failed: %s", e)
    try:
        result = await _eval_json(tab, _MODAL_DISMISS_JS)
        if result:
            logger.info("[RELAY] dismissed modals: %s", result)
    except Exception as e:
        logger.debug("[RELAY] modal dismiss eval failed: %s", e)


async def _close_popup_tabs(browser, keep_target_id: str):
    """Ported from tab_proxy.py's _close_popup_tabs: some streaming
    players open popunder windows on first click; close anything that
    isn't our own relay tab so it doesn't steal focus or burn resources.
    Gives the popup a moment to register as a real target first."""
    await asyncio.sleep(0.3)
    closed = 0
    try:
        for t in list(browser.tabs):
            try:
                tid = t.target.target_id
            except Exception:
                continue
            if tid == keep_target_id:
                continue
            url = getattr(t, "url", "") or ""
            if url in ("", "about:blank", "chrome://newtab/"):
                continue
            try:
                await t.close()
                closed += 1
            except Exception:
                pass
    except Exception as e:
        logger.debug("[RELAY] popup cleanup failed: %s", e)
    if closed:
        logger.info("[RELAY] closed %d popup tab(s)", closed)


async def run_click_sequence(tab, browser, keep_target_id: str, click_sequence: list,
                              contexts: list | None = None) -> list:
    """Walks a per-site config list of steps (the same 'action'-keyed
    vocabulary selenium-uc/tab_proxy.py's dead engine used -- see this
    module's docstring for why that's worth carrying forward):
      {"action": "delay", "seconds": N}
      {"action": "close_popups"}
      {"action": "dismiss_modals"}
      {"action": "click_iframe_center"}
      {"action": "iframe_dom_click"}
      {"action": "evaluate", "js": "..."}
      {"action": "start_capture_stream"}  -- no-op; capture is a separate
                                             explicit step in start_relay,
                                             kept valid here only so a
                                             config ported verbatim from
                                             TAB_PROXY_CONFIG doesn't need
                                             editing.
    Also accepts simpler, generic step shapes:
      {"action": "click_selector", "selector": "...", "seconds": N}
      {"action": "click_coords", "x": N, "y": N, "seconds": N}
      {"action": "click_selector_any_context", "selector": "...", "seconds": N}
        -- searches every context in `contexts` (top tab AND every
        attached/isolated-world frame) for the selector and clicks
        whichever one has it, via a JS-level .click() (not a trusted CDP
        click -- confirmed live this session that a player library's OWN
        play button doesn't require one; only the browser's autoplay gate
        on a bare <video>.play() does). Real, confirmed-live motivation:
        a source whose actual player control (e.g. a JW Player-style
        .jw-icon-display button) only exists inside a same-process
        cross-origin frame reachable solely via an isolated world -- the
        outer click_iframe_center/iframe_dom_click steps only ever
        reached the popunder-bait layer, never this control.
    Returns a per-step log -- this is expected to need real tuning per
    site, and a silent failure here is the hardest thing to diagnose from
    outside (see /relay/{id}/debug-eval for interactive tuning)."""
    log = []
    for step in (click_sequence or []):
        action = (step.get("action") or "").strip()
        try:
            if action == "delay":
                await asyncio.sleep(float(step.get("seconds", 0.5)))
                log.append({"step": step, "ok": True})
            elif action == "close_popups":
                await _close_popup_tabs(browser, keep_target_id)
                log.append({"step": step, "ok": True})
            elif action == "dismiss_modals":
                await _dismiss_modals(tab)
                log.append({"step": step, "ok": True})
            elif action == "click_iframe_center":
                ok = await _click_iframe_center(tab)
                log.append({"step": step, "ok": ok})
            elif action == "iframe_dom_click":
                result = await _iframe_dom_click(tab)
                log.append({"step": step, "ok": bool(result), "result": result})
            elif action == "evaluate":
                js = step.get("js", "")
                if js:
                    await tab.send(uc.cdp.runtime.evaluate(expression=js))
                log.append({"step": step, "ok": True})
            elif action == "start_capture_stream":
                log.append({"step": step, "ok": True, "note": "no-op; see start_relay"})
            elif action == "click_selector":
                rect = await _eval_json(
                    tab, "(function(){var el=document.querySelector(%s);if(!el)return null;"
                         "var r=el.getBoundingClientRect();if(r.width<=0||r.height<=0)return null;"
                         "return JSON.stringify({x:r.x+r.width/2,y:r.y+r.height/2});})()"
                         % json.dumps(step["selector"]))
                if rect:
                    await _dispatch_click(tab, rect["x"], rect["y"])
                    log.append({"step": step, "ok": True})
                else:
                    log.append({"step": step, "ok": False, "error": "selector not found or zero-size"})
            elif action == "click_coords":
                await _dispatch_click(tab, step["x"], step["y"])
                log.append({"step": step, "ok": True})
            elif action == "click_selector_any_context":
                selector = step["selector"]
                click_js = (
                    "(function(){var el=document.querySelector(%s);"
                    "if(!el)return null;var r=el.getBoundingClientRect();"
                    "if(r.width<=0||r.height<=0)return null;"
                    "el.click();return JSON.stringify({w:r.width,h:r.height});})()"
                    % json.dumps(selector)
                )
                hit = None
                for i, ctx in enumerate(contexts or []):
                    try:
                        result = await _eval_json(ctx, click_js)
                    except Exception:
                        continue
                    if result:
                        hit = i
                        break
                log.append({"step": step, "ok": hit is not None, "context_index": hit})
            else:
                log.append({"step": step, "ok": False, "error": f"unknown action {action!r}"})
        except Exception as e:
            log.append({"step": step, "ok": False, "error": str(e)})
    return log


async def _build_contexts(tab, browser) -> tuple:
    """Enumerates every plausible execution context for a page: the
    top-level tab, every genuinely separate cross-origin iframe TARGET
    (via network_capture.py's iframe-candidate/session-attach machinery),
    and every same-process cross-origin frame that has NO separate target
    at all (Site Isolation disabled -- see _IsolatedWorldContext's
    docstring; confirmed live this session to be exactly where a real
    source's actual player lived). Returns (contexts, attached_count,
    isolated_count) for logging."""
    contexts = [tab]
    try:
        candidates = await _find_iframe_candidates(browser)
    except Exception as e:
        logger.warning("[RELAY] iframe candidate lookup failed: %s", e)
        candidates = None
    attached_count = 0
    for cand in (candidates or []):
        try:
            session = await attach_to_target(tab, cand.target.target_id)
            contexts.append(session)
            attached_count += 1
        except Exception as e:
            logger.debug("[RELAY] attach_to_target failed for a candidate: %s", e)

    isolated_count = 0
    try:
        isolated_contexts = await _find_child_frame_contexts(tab)
        contexts.extend(isolated_contexts)
        isolated_count = len(isolated_contexts)
    except Exception as e:
        logger.warning("[RELAY] isolated-world frame lookup failed: %s", e)

    return contexts, attached_count, isolated_count


async def start_relay(tab, browser, keep_target_id: str, page_url: str, click_sequence: list) -> dict:
    """Navigate, build every plausible execution context (see
    _build_contexts), run the site's click sequence to reach real
    playback -- steps can act on the top tab (iframe_dom_click,
    click_iframe_center, popup/modal handling) OR on ANY specific found
    context (click_selector_any_context, for a control -- e.g. a player
    library's own play button -- that only exists inside a same-process
    cross-origin frame reachable solely via an isolated world) -- then
    inject the captureStream()+MediaRecorder JS into every context.
    Real, confirmed-live motivation for context-aware clicks: a source
    whose <video> loaded real segment data and set duration/seekable
    metadata but never actually started decoding (readyState stuck at 0,
    zero segment fetches) until the PLAYER LIBRARY'S OWN play button
    (inside the real cross-origin frame, not the outer iframe/popunder
    layer click_iframe_center handles) was clicked -- calling video.play()
    directly bypassed the player's own internal state machine entirely.

    Each context's own _RELAY_START_JS retries independently for up to
    120s; only whichever one actually has the real <video> will ever
    start recording, but there's no reliable way to know which one ahead
    of time without site-specific knowledge, so every candidate gets a
    shot. Returns {"ok", "error", "click_log", "contexts": [...]} --
    TabRelaySource's caller stores the whole context list and polls all
    of them every tick."""
    await tab.get(page_url)
    await asyncio.sleep(PAGE_SETTLE_SECONDS)

    contexts, attached_count, isolated_count = await _build_contexts(tab, browser)
    logger.info("[RELAY] built %d context(s): top tab + %d attached iframe target(s) "
                "+ %d isolated-world frame context(s)", len(contexts), attached_count, isolated_count)

    click_log = await run_click_sequence(tab, browser, keep_target_id,
                                          click_sequence if click_sequence is not None
                                          else DEFAULT_CLICK_SEQUENCE,
                                          contexts=contexts)

    any_ok = False
    last_error = "no context accepted the capture JS"
    for ctx in contexts:
        try:
            result = await _eval_json(ctx, _RELAY_START_JS)
        except Exception as e:
            last_error = f"relay-start JS failed: {e}"
            continue
        if result and result.get("ok"):
            any_ok = True
        elif result:
            last_error = result.get("error", "unknown")

    if not any_ok:
        return {"ok": False, "error": last_error, "click_log": click_log, "contexts": []}
    return {"ok": True, "error": None, "click_log": click_log, "contexts": contexts}


_POLL_PER_CONTEXT_TIMEOUT_SECONDS = 5.0


async def poll_relay(contexts: list) -> dict:
    """Polls every context from start_relay's return, merging results --
    in practice only one context ever actually has the real <video> and
    starts producing chunks; the rest just harmlessly report
    recording=False (or eventually error out once their own 120s search
    times out, which is not itself fatal -- only ALL contexts erroring/
    ending counts as the session being over). Returns {"ok", "chunks",
    "ended", "error", "recording"}.

    Each context gets its OWN short timeout -- real, confirmed-live bug
    found in production: with a bare per-context await and no timeout, a
    SINGLE wedged context (a CDP connection whose listener died -- the
    same nodriver failure class fixed elsewhere this session) blocked
    this whole function past the caller's own 15s deadline, even though
    the OTHER contexts (including whichever one actually held the real,
    still-healthy <video>) were perfectly fine and had real chunks
    waiting. The caller (app.py's /relay/{id}/chunks) then returned a
    blank-error failure every single poll -- and since that's a single,
    generic failure, not a real "this session is dead" signal,
    TabRelaySource kept retrying indefinitely without ever giving up and
    restarting, which is what a genuinely dead session needs to recover.
    A per-context timeout means one wedged context is skipped (its
    chunks lost for that poll, no different from a slow response) rather
    than silently stalling every context's chunks forever."""
    async def _poll_one(ctx):
        return await asyncio.wait_for(_eval_json(ctx, _RELAY_POLL_JS),
                                       timeout=_POLL_PER_CONTEXT_TIMEOUT_SECONDS)

    # Concurrent, not sequential -- with a per-context timeout but a
    # sequential await, N contexts could still stack up to N times that
    # timeout in the worst case (multiple wedged at once), which can
    # itself exceed the caller's own outer deadline. Running them all at
    # once bounds total wall time to the single slowest context's own
    # timeout, not the sum.
    raw_results = await asyncio.gather(*(_poll_one(ctx) for ctx in contexts),
                                        return_exceptions=True)

    chunks: list = []
    any_recording = False
    all_ended = True
    last_error = None
    any_ok = False
    for result in raw_results:
        if isinstance(result, Exception):
            # Context unreachable/unresponsive (target closed, or wedged
            # past its own timeout above) -- doesn't override all_ended's
            # running AND; a genuinely dead context is consistent with
            # "this one's done" either way.
            last_error = str(result) or type(result).__name__
            continue
        if not result:
            continue
        any_ok = True
        chunks.extend(result.get("chunks") or [])
        if result.get("recording"):
            any_recording = True
        if not result.get("ended"):
            all_ended = False
        if result.get("error"):
            last_error = result["error"]
    if not any_ok:
        return {"ok": False, "chunks": [], "ended": True, "error": last_error or "no context reachable"}
    return {"ok": True, "chunks": chunks, "ended": all_ended,
            "error": last_error, "recording": any_recording}


async def stop_relay(contexts: list):
    for ctx in contexts:
        try:
            await asyncio.wait_for(_eval_json(ctx, _RELAY_STOP_JS), timeout=5.0)
        except Exception:
            pass


async def sniff_network(tab, browser, keep_target_id: str, page_url: str, click_sequence: list,
                         url_substrings: list, settle_seconds: float = 8.0) -> list:
    """Diagnostic-only, not called by channelarr: captures every response
    whose URL contains any of url_substrings, body included -- unlike
    network_capture.py's real /capture pipeline, which only ever inspects
    a response's body if its URL/mime already matched MATCH_PATTERNS or
    JSON_STREAM_PATTERNS first. Real, confirmed-live gap this exists to
    investigate: a small XHR/fetch endpoint (e.g. a real source's own
    "/fetch" call) can carry a hidden key/token/disguised-manifest payload
    that never gets its body checked at all today, simply because its URL
    doesn't happen to contain any of the handful of known substrings.
    Existing disguised-body-sniff logic (network_capture.py's #EXTM3U-in-
    first-4096-chars check) already handles a body once it's fetched --
    this tool is for finding responses that check needs to be reached
    for at all, not a replacement for it."""
    seen = []
    seen_lock = asyncio.Lock()
    req_headers: dict = {}

    async def on_request(event):
        try:
            req_headers[event.request_id] = {
                "headers": dict(getattr(event.request, "headers", {}) or {}),
                "url": event.request.url,
            }
        except Exception:
            pass

    async def on_response(event):
        try:
            url_ = event.response.url
            if not any(s in url_ for s in url_substrings):
                return
            rid = event.request_id
            mime = getattr(event.response, "mime_type", "") or ""
            status = getattr(event.response, "status", None)
            body_b64 = None
            try:
                cdp_result = await asyncio.wait_for(
                    tab.send(uc.cdp.network.get_response_body(request_id=rid)), timeout=5.0)
                if cdp_result:
                    body_str, already_b64 = cdp_result
                    # Always hand back real base64 of the raw bytes -- callers
                    # that need to inspect binary/obfuscated payloads (the
                    # whole point of this tool) can't do that through a
                    # decode(errors="replace") text string, which silently
                    # corrupts exactly the bytes worth looking at.
                    body_b64 = body_str if already_b64 else base64.b64encode(
                        body_str.encode("utf-8", errors="surrogateescape")).decode("ascii")
            except Exception as e:
                body_b64 = None
            async with seen_lock:
                seen.append({
                    "url": url_, "status": status, "mime": mime,
                    "request_headers": (req_headers.get(rid) or {}).get("headers"),
                    "body_b64": body_b64,
                })
        except Exception as e:
            logger.debug("[SNIFF] on_response error: %s", e)

    await tab.send(uc.cdp.network.enable())
    tab.add_handler(uc.cdp.network.RequestWillBeSent, lambda e: asyncio.create_task(on_request(e)))
    tab.add_handler(uc.cdp.network.ResponseReceived, lambda e: asyncio.create_task(on_response(e)))

    await tab.get(page_url)
    await asyncio.sleep(PAGE_SETTLE_SECONDS)
    await run_click_sequence(tab, browser, keep_target_id, click_sequence or [])
    await asyncio.sleep(settle_seconds)

    for evt in (uc.cdp.network.RequestWillBeSent, uc.cdp.network.ResponseReceived):
        try:
            tab.remove_handler(evt)
        except Exception:
            pass

    return seen
