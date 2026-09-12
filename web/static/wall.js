/* Channelarr — Multi-stream diagnostics wall.
 *
 * A standalone pop-out page (window.open'd from the main Diagnostics view,
 * not part of the SPA in ui.js). Up to 4 channels, each with a real HLS
 * <video> (via Hls.js, same pattern as channelarr.watchChannel in ui.js)
 * plus a live diagnostics overlay fed by the per-channel SSE stream. The
 * video tiles pulling real segments is itself what keeps StreamerManager
 * from idling those channels out — no separate "test mode" backend needed.
 */
(function () {
"use strict";

const API = window.API_BASE || "/api";
const MAX_SLOTS = 4;
const STORAGE_KEY = "channelarr_wall_state";

let channels = [];
let slotCount = 1;
const slots = []; // {channelId, hls, es}

function escHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
}

function qualityClass(q) {
  return ["excellent", "good", "bad"].includes(q) ? q : "unknown";
}

// Client-reported playback events (stall/live-catch-up-jump) — same
// mechanism as ui.js's attachPlaybackDiagnostics, duplicated here since
// this is a standalone page. Each slot's video is a stable DOM node for
// the slot's lifetime (only channel picks change, not the element), so
// listeners are attached once per tile in buildTile(), reading the
// channelId off the shared `slots[i]` object so they stay correct across
// channel changes without re-attaching.
function postClientPlaybackEvent(channelId, eventType, detail) {
  if (!channelId) return;
  fetch(`${API}/diagnostics/${encodeURIComponent(channelId)}/client-event`, {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({event_type: eventType, detail}),
  }).catch(() => {});
}

function attachPlaybackDiagnostics(video, slot) {
  video.addEventListener("timeupdate", () => { slot.lastKnownTime = video.currentTime; });
  video.addEventListener("waiting", () => {
    if (slot.stallStartWall == null) slot.stallStartWall = performance.now();
  });
  video.addEventListener("playing", () => {
    if (slot.stallStartWall != null) {
      const durationMs = performance.now() - slot.stallStartWall;
      slot.stallStartWall = null;
      if (durationMs > 500) {
        postClientPlaybackEvent(slot.channelId, "client_stall", {duration_ms: Math.round(durationMs)});
      }
    }
  });
  video.addEventListener("seeking", () => {
    const delta = video.currentTime - (slot.lastKnownTime || 0);
    if (Math.abs(delta) > 2) {
      postClientPlaybackEvent(slot.channelId, "client_seek_jump", {delta_s: Math.round(delta * 100) / 100});
    }
  });
}

function loadState() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY)) || {};
  } catch (e) { return {}; }
}

function saveState() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      slotCount,
      channelIds: slots.map(s => (s && s.channelId) || ""),
    }));
  } catch (e) {}
}

async function loadChannels() {
  const r = await fetch(`${API}/channels`);
  channels = await r.json();
}

function optionsHtml(selectedId) {
  return '<option value="">— pick a channel —</option>' + channels.map(ch =>
    `<option value="${ch.id}" ${ch.id === selectedId ? "selected" : ""}>${escHtml(ch.name)}</option>`
  ).join("");
}

function destroySlot(i) {
  const s = slots[i];
  if (!s) return;
  if (s.hls) { try { s.hls.destroy(); } catch (e) {} s.hls = null; }
  if (s.es) { s.es.close(); s.es = null; }
}

function renderOverlay(overlay, s) {
  const q = qualityClass(s.quality);
  const bits = [];
  if (s.encode_speed_ratio != null) bits.push(`${Math.round(s.encode_speed_ratio * 100)}% speed`);
  if (s.production_speed_ratio != null) bits.push(`${Math.round(s.production_speed_ratio * 100)}% production`);
  if (s.fetch_latency_ms_avg != null) bits.push(`${Math.round(s.fetch_latency_ms_avg)}ms fetch`);
  bits.push(`${s.reconnects_last_5m || 0} rc/5m`);
  if (s.client_stalls_last_5m) bits.push(`<span style="color:var(--danger)">${s.client_stalls_last_5m} stall</span>`);
  if (s.client_seeks_last_5m) bits.push(`<span style="color:var(--danger)">${s.client_seeks_last_5m} jump</span>`);
  if (s.fallback_active) bits.push('<span style="color:var(--danger);font-weight:600">fallback</span>');
  overlay.innerHTML = `<span class="diag-badge diag-badge-${q}">${q}</span> ${bits.join(" &middot; ")}`;
}

function playSlot(i, channelId) {
  destroySlot(i);
  slots[i].channelId = channelId || null;
  saveState();

  const tile = document.getElementById(`wall-tile-${i}`);
  if (!tile) return;
  const video = tile.querySelector("video");
  const overlay = tile.querySelector(".wall-overlay");

  if (!channelId) {
    video.removeAttribute("src");
    if (video.load) video.load();
    overlay.innerHTML = "";
    return;
  }

  const ch = channels.find(c => c.id === channelId);
  const url = (ch && ch.stream_url) || `/live/${channelId}/stream.m3u8`;

  // Same Hls.js config/error-recovery pattern as channelarr.watchChannel in
  // ui.js — deliberately not reinvented.
  if (window.Hls && Hls.isSupported()) {
    const hls = new Hls({
      // liveMaxLatencyDurationCount loosened from 10 — see ui.js's watchChannel
      // for why (server resync_skip events were tripping Hls.js's forced
      // catch-up-to-live jump, causing the fast-forward-then-stall pattern).
      liveSyncDurationCount: 3, liveMaxLatencyDurationCount: 30, liveDurationInfinity: true,
      enableWorker: true, lowLatencyMode: false, backBufferLength: 0,
      maxBufferLength: 30, maxMaxBufferLength: 60,
    });
    hls.loadSource(url);
    hls.attachMedia(video);
    hls.on(Hls.Events.MANIFEST_PARSED, () => { video.play().catch(() => {}); });
    hls.on(Hls.Events.ERROR, (_, data) => {
      if (data.fatal) {
        if (data.type === Hls.ErrorTypes.NETWORK_ERROR) {
          setTimeout(() => hls.startLoad(), 3000);
        } else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
          hls.recoverMediaError();
        } else {
          hls.destroy();
        }
      }
    });
    slots[i].hls = hls;
  } else if (video.canPlayType("application/vnd.apple.mpegurl")) {
    video.src = url;
    video.play().catch(() => {});
  }

  const es = new EventSource(`${API}/diagnostics/${encodeURIComponent(channelId)}/stream`);
  es.onmessage = (e) => {
    try {
      renderOverlay(overlay, (JSON.parse(e.data).summary) || {});
    } catch (err) {}
  };
  slots[i].es = es;
}

function buildTile(i) {
  const tile = document.createElement("div");
  tile.className = "wall-tile";
  tile.id = `wall-tile-${i}`;
  tile.innerHTML = `
    <div class="wall-tile-controls">
      <select class="wall-picker">${optionsHtml(slots[i].channelId)}</select>
      <button class="wall-mute-btn" title="Toggle mute">&#128264;</button>
    </div>
    <video muted playsinline></video>
    <div class="wall-overlay"></div>
  `;
  const select = tile.querySelector(".wall-picker");
  select.addEventListener("change", () => playSlot(i, select.value || null));
  const muteBtn = tile.querySelector(".wall-mute-btn");
  const video = tile.querySelector("video");
  muteBtn.addEventListener("click", () => {
    video.muted = !video.muted;
    muteBtn.innerHTML = video.muted ? "&#128264;" : "&#128266;";
  });
  attachPlaybackDiagnostics(video, slots[i]);
  return tile;
}

function renderGrid() {
  // Tear down every currently-playing slot before touching the DOM — Hls/
  // EventSource cleanup must happen while we still hold live references,
  // not after the video elements have already been discarded.
  for (let i = 0; i < slots.length; i++) destroySlot(i);

  const grid = document.getElementById("wall-grid");
  grid.dataset.slots = String(slotCount);
  grid.innerHTML = "";

  const prior = slots.slice();
  slots.length = 0;
  for (let i = 0; i < slotCount; i++) {
    slots[i] = {channelId: (prior[i] && prior[i].channelId) || null, hls: null, es: null, stallStartWall: null, lastKnownTime: 0};
    grid.appendChild(buildTile(i));
    if (slots[i].channelId) playSlot(i, slots[i].channelId);
  }
  saveState();
}

function setSlotCount(n) {
  slotCount = Math.min(MAX_SLOTS, Math.max(1, n));
  document.querySelectorAll("[data-slots-btn]").forEach(b =>
    b.classList.toggle("active", Number(b.dataset.slotsBtn) === slotCount));
  renderGrid();
}

document.querySelectorAll("[data-slots-btn]").forEach(btn => {
  btn.addEventListener("click", () => setSlotCount(Number(btn.dataset.slotsBtn)));
});

window.addEventListener("beforeunload", () => {
  for (let i = 0; i < slots.length; i++) destroySlot(i);
});

(async function init() {
  await loadChannels();
  const state = loadState();
  slotCount = Math.min(MAX_SLOTS, Math.max(1, state.slotCount || 1));
  document.querySelectorAll("[data-slots-btn]").forEach(b =>
    b.classList.toggle("active", Number(b.dataset.slotsBtn) === slotCount));
  for (let i = 0; i < slotCount; i++) {
    slots[i] = {channelId: (state.channelIds && state.channelIds[i]) || null, hls: null, es: null, stallStartWall: null, lastKnownTime: 0};
  }
  renderGrid();
})();

})();
