"""Plugin-driven tab-proxy configuration.

Scraper plugins in /app/scrapers/ can optionally declare a module-level
TAB_PROXY_CONFIG dict describing how their source's pages should be
driven by the nodriver sidecar — which domains to recognize, which
click sequence to execute, any source-specific JS to run.

The sidecar itself is fully source-agnostic. Channelarr discovers the
matching plugin config by page_url domain and includes it in the
/tab/open request body; the sidecar just executes the declared steps.

Schema
------
    TAB_PROXY_CONFIG = {
        # Hostnames (exact or suffix-match) this plugin handles.
        "domains": ["example.com", "*.cdn.example.com"],

        # Whether the sidecar should run its generic modal-dismiss sweep
        # before the click sequence. Default: True.
        "dismiss_modals": True,

        # Ordered list of actions to drive playback start. Actions:
        #   {"action": "delay", "seconds": float}
        #   {"action": "close_popups"}          # close non-main tabs
        #   {"action": "dismiss_modals"}        # generic modal close pass
        #   {"action": "click_iframe_center"}   # CDP mousePressed/Released
        #   {"action": "iframe_dom_click"}      # click player selectors via contentDocument
        #   {"action": "evaluate", "js": str}   # run arbitrary JS in main tab
        "click_sequence": [...],

        # Hosts whose traffic is relevant for diagnostic logging.
        "stream_hosts": ["cdn.example.com"],

        # Opt-in: have the sidecar enable CDP Fetch-domain interception
        # for this source. Fetch catches requests that the Network domain
        # misses — WASM fetches, dedicated worker fetches, some exotic
        # media pipelines. Each pattern is a dict {url_pattern,
        # resource_type}. Leave unset to rely on Network events only.
        "fetch_intercept": [
            {"url_pattern": "*", "resource_type": "Media"},
            {"url_pattern": "*", "resource_type": "XHR"},
            {"url_pattern": "*", "resource_type": "Fetch"},
        ],

        # Stream delivery model. "hls" (default) = sidecar captures the
        # upstream m3u8 + .ts and channelarr rewrites the playlist to
        # point back through itself. "webm" = source's bytes are opaque
        # (encrypted, WASM-driven, etc); instead drive the in-tab
        # MediaRecorder via captureStream() and serve the resulting
        # WebM live feed. Requires "start_capture_stream" in
        # click_sequence. Trades CPU (in-tab re-encode) for
        # source-agnostic capture.
        "stream_type": "webm",
    }

If a plugin doesn't declare TAB_PROXY_CONFIG, or its domains don't
match the page URL, the sidecar runs with its built-in defaults.
"""

import importlib.util
import logging
import os
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/app/scrapers")


def _load_plugin(path: str):
    """Import a scraper file as a one-off module. Returns the module
    or None on failure."""
    try:
        name = os.path.splitext(os.path.basename(path))[0]
        spec = importlib.util.spec_from_file_location(f"plugin_{name}", path)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        logger.debug("[TAB-PROXY-CFG] failed to load %s: %s", path, e)
        return None


def _domain_matches(host: str, patterns: list) -> bool:
    """Exact or suffix match: 'example.com' matches 'example.com' and
    'www.example.com'; '*.cdn.com' matches 'a.cdn.com' and 'b.cdn.com'."""
    h = (host or "").lower()
    for p in patterns or []:
        p = str(p).lower().strip()
        if not p:
            continue
        if p.startswith("*."):
            if h.endswith(p[1:]):  # '.cdn.com'
                return True
        elif h == p or h.endswith("." + p):
            return True
    return False


def get_tab_proxy_config(page_url: str) -> dict | None:
    """Find the first scraper plugin whose TAB_PROXY_CONFIG domains
    match this page URL. Returns the config dict (with the domains key
    stripped) or None."""
    if not page_url:
        return None
    try:
        host = urlparse(page_url).hostname or ""
    except Exception:
        return None
    if not host:
        return None
    if not os.path.isdir(SCRAPERS_DIR):
        return None
    try:
        entries = sorted(os.listdir(SCRAPERS_DIR))
    except OSError:
        return None
    for name in entries:
        if not name.endswith(".py") or name.startswith("_"):
            continue
        path = os.path.join(SCRAPERS_DIR, name)
        mod = _load_plugin(path)
        if mod is None:
            continue
        cfg = getattr(mod, "TAB_PROXY_CONFIG", None)
        if not isinstance(cfg, dict):
            continue
        if _domain_matches(host, cfg.get("domains", [])):
            # Return a shallow copy so callers can mutate without leaking
            # back into the cached module's constant
            out = dict(cfg)
            out.pop("domains", None)
            out["_plugin"] = os.path.splitext(name)[0]
            return out
    return None
