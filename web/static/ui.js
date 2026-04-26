/* Channelarr — Single Page Application */
(function(){
"use strict";

const API = window.API_BASE || "/api";
const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);

// ─── State ───
let channels = [];
let movies = [];
let tvShows = [];
let bumpData = {};
let editingChannel = null;
let editorItems = [];
let editorWeights = {};
let pickerTab = "movies";
let pickerShowEpisodes = null;
let settingsSchema = {};
let settingsOriginal = {};
let settingsModified = {};
let activeSettingsSection = "";
let guideHours = 3;

// Log state
let tailPos = 0, tailInode = null;
const logOut = $("#log-output");

// ─── Navigation ───
let currentSettingsSub = "general";
let tasksTimer = null;

$$(".nav-item").forEach(btn => {
  btn.addEventListener("click", () => {
    if (btn.classList.contains("nav-item-parent")) {
      btn.classList.toggle("expanded");
      const sub = $("#settings-subnav");
      if (sub) sub.classList.toggle("expanded");
    }
    switchView(btn.dataset.view);
  });
});

$$(".nav-subitem").forEach(btn => {
  btn.addEventListener("click", () => {
    currentSettingsSub = btn.dataset.sub;
    $$(".nav-subitem").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    switchSettingsSub(currentSettingsSub);
  });
});

function switchSettingsSub(sub) {
  $$(".settings-sub").forEach(el => el.style.display = "none");
  const target = $(`#settings-sub-${sub}`);
  if (target) target.style.display = "";

  // Show/hide save button based on sub-tab
  const saveBtn = $("#save-settings");
  if (saveBtn) saveBtn.style.display = (sub === "general") ? "" : "none";

  const title = $("#settings-section-title");
  if (title) {
    const titles = {general: "Settings", tasks: "Background Tasks", integrations: "Integrations", branding: "Branding Logos"};
    title.textContent = titles[sub] || "Settings";
  }

  if (sub === "general") loadSettings();
  if (sub === "tasks") loadTasks();
  if (sub === "integrations") loadIntegrations();
  if (sub === "branding") loadBrandingGrid();
  if (sub !== "tasks") { clearInterval(tasksTimer); tasksTimer = null; }
}

function switchView(view) {
  $$(".nav-item").forEach(n => {
    if (n.dataset.view === view || (view === "settings" && n.dataset.view === "settings")) {
      n.classList.add("active");
    } else {
      n.classList.remove("active");
    }
  });
  $$(".view").forEach(v => v.classList.toggle("visible", v.id === `view-${view}`));

  if (view === "channels") loadChannels();
  if (view === "guide") loadGuide();
  if (view === "media") loadMediaView();
  if (view === "bumps") loadBumps();
  if (view === "system") updateSystemStats();
  if (view === "resolver") loadResolver();
  if (view === "settings") {
    // Auto-expand subnav and load active sub-tab
    const parentBtn = document.querySelector('[data-view="settings"]');
    const sub = $("#settings-subnav");
    if (parentBtn && !parentBtn.classList.contains("expanded")) {
      parentBtn.classList.add("expanded");
      if (sub) sub.classList.add("expanded");
    }
    switchSettingsSub(currentSettingsSub);
  }
  if (view !== "settings") { clearInterval(tasksTimer); tasksTimer = null; }
  if (view !== "resolver") {
    clearInterval(resolverTimer);
    resolverTimer = null;
  }
  if (view === "scrapers") { loadScrapers(); loadEventQueue(); }
  if (view !== "scrapers") {
    clearInterval(scraperTimer);
    scraperTimer = null;
    clearInterval(eqTimer);
    eqTimer = null;
  }
}

// ─── Toast ───
function toast(type, msg) {
  const old = $(".toast");
  if (old) old.remove();
  const t = document.createElement("div");
  t.className = `toast ${type}`;
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => { t.classList.add("fade-out"); setTimeout(() => t.remove(), 300); }, 3000);
}

// ─── Status polling ───
async function updateStatus() {
  try {
    const r = await fetch(`${API}/status`);
    const d = await r.json();
    const streaming = d.channels_streaming || 0;
    const total = d.channels_total || 0;
    $("#sidebar-total").textContent = total;
    $("#sidebar-streaming").textContent = streaming;
    const badge = $("#streaming-badge");
    if (streaming > 0) {
      badge.textContent = `${streaming} streaming`;
      badge.classList.remove("hidden");
    } else {
      badge.classList.add("hidden");
    }
  } catch(e) {}
  loadVpnStatus();
}

// ─── Header buttons ───
$("#soft-refresh-btn").addEventListener("click", async () => {
  try {
    const r = await fetch(`${API}/schedule/refresh`, {method: "POST"});
    const d = await r.json();
    toast("success", d.message || "Refreshed");
  } catch(e) { toast("error", "Refresh failed"); }
});

$("#hard-regen-btn").addEventListener("click", async () => {
  if (!confirm("Regenerate all channel schedules? This stops any running streams.")) return;
  const btn = $("#hard-regen-btn");
  try {
    btn.disabled = true;
    btn.textContent = "Working...";
    const r = await fetch(`${API}/schedule/regenerate`, {method: "POST"});
    const d = await r.json();
    toast("success", d.message || "Regenerated");
    loadChannels();
    if ($("#view-guide").classList.contains("visible")) loadGuide();
  } catch(e) {
    toast("error", "Regeneration failed");
  } finally {
    btn.disabled = false;
    btn.textContent = "Regenerate";
  }
});

// Copy URL buttons (matches manifold behavior)
function copyToClipboard(text) {
  const ta = document.createElement("textarea");
  ta.value = text;
  ta.style.position = "fixed";
  ta.style.left = "-9999px";
  document.body.appendChild(ta);
  ta.focus();
  ta.select();
  try { document.execCommand("copy"); } catch(e) {}
  document.body.removeChild(ta);
}

$$("[data-copy-path]").forEach(btn => {
  btn.addEventListener("click", () => {
    const path = btn.dataset.copyPath;
    const url = window.location.protocol + "//" + window.location.host + path;
    copyToClipboard(url);
    btn.classList.add("copied");
    btn.textContent = "\u2713";
    setTimeout(() => { btn.classList.remove("copied"); btn.innerHTML = "\u2398"; }, 1500);
    toast("success", "Copied: " + url);
  });
});

// ─── Channels ───
async function loadChannels() {
  try {
    const r = await fetch(`${API}/channels`);
    channels = await r.json();
    renderChannels();
  } catch(e) { toast("error", "Failed to load channels"); }
}

function renderChannels() {
  const grid = $("#channel-grid");
  if (!channels.length) {
    grid.innerHTML = '<div class="empty-state"><div class="empty-icon">&#128250;</div>No channels yet. Create one to get started.</div>';
    return;
  }
  grid.innerHTML = channels.map(ch => {
    const isResolved = ch.type === "resolved";
    const logoUrl = `${API}/logo/${ch.id}`;

    if (isResolved) {
      // Resolved channels are pure live streams. The API stamps a deterministic
      // 30-minute placeholder block in now_playing so the tile shows the same
      // visual treatment as scheduled channels (title + remaining + progress).
      const domain = ch.source_domain || (ch.manifest_url ? new URL(ch.manifest_url).hostname : "");
      const meta = [];
      if (domain) meta.push(domain);
      if (ch.expires_at) {
        const exp = new Date(ch.expires_at);
        const minsLeft = Math.round((exp - Date.now()) / 60000);
        if (minsLeft > 0) meta.push(`token: ${minsLeft}m`);
        else meta.push("token: refreshing");
      }

      const np = ch.now_playing;
      let nowPlayingHtml;
      if (np && np.entry) {
        const pct = Math.round((np.progress || 0) * 100);
        const remaining = np.entry.duration ? formatDuration(np.entry.duration - np.seek_offset) : "";
        nowPlayingHtml = `
          <div class="ch-now-playing">
            <div class="ch-now-label">LIVE NOW</div>
            <div class="ch-now-title">${esc(np.entry.title || ch.name)}</div>
            ${remaining ? `<div class="ch-now-remaining">${remaining} until next block</div>` : ""}
            <div class="ch-progress-bar"><div class="ch-progress-fill" style="width:${pct}%"></div></div>
          </div>`;
      } else {
        nowPlayingHtml = `<div class="ch-now-playing"><div class="ch-now-label">LIVE</div></div>`;
      }

      return `
        <div class="channel-card" data-id="${ch.id}" data-type="resolved">
          <div class="channel-card-logo-row">
            <img class="channel-card-logo" src="${logoUrl}" alt="" onerror="this.classList.add('no-logo')" onload="this.classList.remove('no-logo')" />
          </div>
          <div class="channel-card-body">
            <div class="channel-card-header">
              <h3>${esc(ch.name)}</h3>
              <span class="badge badge-schedule">LIVE</span>
            </div>
            ${nowPlayingHtml}
            <div class="channel-card-meta">${meta.map(m => `<span>${esc(m)}</span>`).join("")}</div>
            <div class="channel-card-actions">
              <button class="btn btn-sm" onclick="channelarr.watchChannel('${ch.id}', '${esc(ch.name)}')">Watch</button>
              <button class="btn btn-sm" onclick="channelarr.editChannel('${ch.id}')">Edit</button>
              <button class="btn btn-sm" onclick="channelarr.generateLogo('${ch.id}')">Logo</button>
              <button class="btn btn-sm btn-danger" onclick="channelarr.deleteChannel('${ch.id}')">Delete</button>
            </div>
          </div>
        </div>`;
    }

    // Scheduled channel
    const np = ch.now_playing;
    const items = ch.items || [];
    const bc = ch.bump_config || {};
    const meta = [];
    meta.push(`${items.length} item${items.length !== 1 ? "s" : ""}`);
    const bFolders = bc.folders || (bc.folder ? [bc.folder] : []);
    if (bc.enabled && bFolders.length) meta.push(`Bumps: ${bFolders.join(", ")}`);
    const sc = ch.shuffle_config;
    if (sc && sc.mode && sc.mode !== "none") {
      const modeLabels = {random: "Shuffle", round_robin: "Round Robin", weighted: "Weighted"};
      meta.push(modeLabels[sc.mode] || sc.mode);
    } else if (ch.shuffle) {
      meta.push("Shuffle");
    }
    if (ch.loop) meta.push("Loop");

    // Now playing info
    let nowPlayingHtml = "";
    if (np && np.entry) {
      const pct = Math.round((np.progress || 0) * 100);
      const title = np.entry.title || "Unknown";
      const remaining = np.entry.duration ? formatDuration(np.entry.duration - np.seek_offset) : "";
      nowPlayingHtml = `
        <div class="ch-now-playing">
          <div class="ch-now-label">NOW PLAYING</div>
          <div class="ch-now-title">${esc(title)}</div>
          ${remaining ? `<div class="ch-now-remaining">${remaining} remaining</div>` : ""}
          <div class="ch-progress-bar"><div class="ch-progress-fill" style="width:${pct}%"></div></div>
        </div>`;
    } else {
      nowPlayingHtml = `<div class="ch-now-playing"><div class="ch-now-label">NO SCHEDULE</div></div>`;
    }

    // Schedule info
    const cycleDur = ch.schedule_cycle_duration || 0;
    const schedInfo = cycleDur > 0
      ? `Cycle: ${formatDuration(cycleDur)}`
      : "";
    const schedEntries = (ch.materialized_schedule || []).length;
    if (schedEntries > 0) meta.push(`${schedEntries} scheduled`);

    return `
      <div class="channel-card" data-id="${ch.id}" data-type="scheduled">
        <div class="channel-card-logo-row">
          <img class="channel-card-logo" src="${logoUrl}" alt="" onerror="this.classList.add('no-logo')" onload="this.classList.remove('no-logo')" />
        </div>
        <div class="channel-card-body">
          <div class="channel-card-header">
            <h3>${esc(ch.name)}</h3>
            ${schedInfo ? `<span class="badge badge-schedule">${schedInfo}</span>` : ""}
          </div>
          ${nowPlayingHtml}
          <div class="channel-card-meta">${meta.map(m => `<span>${esc(m)}</span>`).join("")}</div>
          <div class="channel-card-actions">
            <button class="btn btn-sm" onclick="channelarr.watchChannel('${ch.id}', '${esc(ch.name)}')">Watch</button>
            <button class="btn btn-sm" onclick="channelarr.editChannel('${ch.id}')">Edit</button>
            <button class="btn btn-sm" onclick="channelarr.deleteChannel('${ch.id}')">Delete</button>
          </div>
        </div>
      </div>`;
  }).join("");
}

function formatUptime(s) {
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s/60)}m`;
  return `${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`;
}

function formatDuration(s) {
  s = Math.round(s);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s/60)}m ${s%60}s`;
  const h = Math.floor(s/3600);
  const m = Math.floor((s%3600)/60);
  return `${h}h ${m}m`;
}

window.channelarr = {};

channelarr.deleteChannel = async function(id) {
  if (!confirm("Delete this channel?")) return;
  try {
    await fetch(`${API}/channels/${id}`, {method:"DELETE"});
    toast("success", "Channel deleted");
    loadChannels();
    updateStatus();
  } catch(e) { toast("error", "Failed to delete channel"); }
};

channelarr.editChannel = function(id) {
  const ch = channels.find(c => c.id === id);
  if (!ch) return;
  openEditor(ch);
};

// ─── Channel Editor Modal ───
const overlay = $("#modal-overlay");

$("#new-channel-btn").addEventListener("click", () => openTypePicker());
$("#modal-close").addEventListener("click", closeEditor);
$("#modal-cancel").addEventListener("click", closeEditor);
$("#modal-save").addEventListener("click", saveChannel);

// ─── Channel Type Picker ───
function openTypePicker() {
  $("#type-picker-overlay").classList.remove("hidden");
}
function closeTypePicker() {
  $("#type-picker-overlay").classList.add("hidden");
}
$("#type-picker-close").addEventListener("click", closeTypePicker);
$("#type-picker-overlay").addEventListener("click", e => {
  if (e.target.id === "type-picker-overlay") closeTypePicker();
});
$("#type-pick-scheduled").addEventListener("click", () => {
  closeTypePicker();
  openEditor(null);
});
$("#type-pick-resolved").addEventListener("click", () => {
  closeTypePicker();
  openCreateResolved(null);
});

// ─── Create Resolved Channel Modal ───
let crSelectedManifest = null;

function openCreateResolved(preselectedManifest) {
  crSelectedManifest = null;
  $("#cr-channel-name").value = "";
  $("#create-resolved-submit").disabled = true;
  $("#create-resolved-overlay").classList.remove("hidden");

  if (preselectedManifest) {
    // Skip picker — go straight to form
    crShowForm(preselectedManifest);
  } else {
    crShowPicker();
  }
}

function closeCreateResolved() {
  $("#create-resolved-overlay").classList.add("hidden");
  crSelectedManifest = null;
}

function crShowPicker() {
  $("#cr-picker-section").style.display = "";
  $("#cr-form-section").style.display = "none";
  $("#create-resolved-back").style.display = "none";
  $("#create-resolved-submit").disabled = true;
  loadCrManifestList();
}

function crShowForm(manifest) {
  crSelectedManifest = manifest;
  $("#cr-picker-section").style.display = "none";
  $("#cr-form-section").style.display = "";
  // "Back" only visible when we came from the picker (i.e. no preselect)
  $("#create-resolved-back").style.display = manifest && manifest._fromPicker ? "" : "none";

  const display = `<div class="cr-selected-name">${esc(manifest.title || "(no title)")}</div>` +
    `<div class="cr-selected-url">${esc(manifest.manifest_url || manifest.url || "")}</div>`;
  $("#cr-selected-manifest").innerHTML = display;

  // Default name: title → hostname → domain → fallback
  let defaultName = manifest.title || "";
  if (!defaultName && manifest.manifest_url) {
    try { defaultName = new URL(manifest.manifest_url).hostname; } catch (e) {}
  }
  if (!defaultName) defaultName = "Resolved Channel";
  $("#cr-channel-name").value = defaultName;
  $("#create-resolved-submit").disabled = false;
  setTimeout(() => $("#cr-channel-name").focus(), 50);
}

async function loadCrManifestList() {
  const list = $("#cr-manifest-list");
  list.innerHTML = '<div class="empty-state">Loading library...</div>';
  try {
    const r = await fetch(`${API}/resolve/channels`);
    const j = await r.json();
    const results = j.results || [];
    if (!results.length) {
      list.innerHTML = '<div class="empty-state">No manifests in the library yet. Open the Manifest Library tab and resolve a URL first.</div>';
      return;
    }
    list.innerHTML = results.map(m => {
      const title = m.title || "(no title)";
      const url = m.manifest_url || m.url || "";
      const truncUrl = url.length > 70 ? url.substring(0, 70) + "..." : url;
      const usedBadge = m.channel_count > 0
        ? `<span class="cr-used-badge">in ${m.channel_count} channel${m.channel_count !== 1 ? "s" : ""}</span>`
        : `<span class="cr-used-badge cr-used-zero">unused</span>`;
      return `<div class="cr-manifest-item" data-mid="${esc(m.manifest_id)}">
        <div class="cr-manifest-title">${esc(title)} ${usedBadge}</div>
        <div class="cr-manifest-url" title="${esc(url)}">${esc(truncUrl)}</div>
      </div>`;
    }).join("");
    list.querySelectorAll(".cr-manifest-item").forEach(el => {
      el.addEventListener("click", () => {
        const mid = el.dataset.mid;
        const m = results.find(x => x.manifest_id === mid);
        if (m) crShowForm({ ...m, _fromPicker: true });
      });
    });
  } catch (e) {
    list.innerHTML = '<div class="empty-state">Failed to load library.</div>';
  }
}

$("#create-resolved-close").addEventListener("click", closeCreateResolved);
$("#create-resolved-cancel").addEventListener("click", closeCreateResolved);
$("#create-resolved-overlay").addEventListener("click", e => {
  if (e.target.id === "create-resolved-overlay") closeCreateResolved();
});
$("#create-resolved-back").addEventListener("click", () => {
  crShowPicker();
});
$("#create-resolved-submit").addEventListener("click", async () => {
  if (!crSelectedManifest) return;
  const name = $("#cr-channel-name").value.trim();
  if (!name) { toast("error", "Channel name required"); return; }
  const btn = $("#create-resolved-submit");
  btn.disabled = true;
  try {
    const r = await fetch(`${API}/channels`, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        type: "resolved",
        manifest_id: crSelectedManifest.manifest_id,
        name,
      }),
    });
    const j = await r.json();
    if (r.ok) {
      toast("success", `Created channel "${j.name}"`);
      closeCreateResolved();
      loadChannels();
      loadResolver();
    } else {
      toast("error", j.error || "Create failed");
      btn.disabled = false;
    }
  } catch (e) {
    toast("error", "Create failed");
    btn.disabled = false;
  }
});

function openEditor(ch) {
  editingChannel = ch;
  const isResolved = !!(ch && ch.type === "resolved");
  editorItems = (ch && !isResolved) ? JSON.parse(JSON.stringify(ch.items || [])) : [];
  $("#modal-title").textContent = ch ? (isResolved ? "Edit Resolved Channel" : "Edit Channel") : "New Channel";
  $("#ch-name").value = ch ? ch.name : "";
  $("#ch-tags").value = ch && ch.tags ? ch.tags.join(", ") : "";

  // Toggle scheduled-only vs resolved-only sections
  $("#ch-scheduled-only").style.display = isResolved ? "none" : "";
  $("#ch-resolved-only").style.display = isResolved ? "" : "none";
  if (isResolved) {
    $("#ch-resolved-url").value = ch.manifest_url || "";
    const mode = ch.encoder_mode || "proxy";
    $("#ch-resolved-encoder-mode").value = mode;
    const isTranscode = (mode === "single" || mode === "multi");
    $("#ch-transcode-options").style.display = isTranscode ? "" : "none";
    $("#ch-resolved-bump-section").style.display = isTranscode ? "" : "none";
    const bc = ch.bump_config || {};
    const selected = bc.folders || (bc.folder ? [bc.folder] : []);
    loadResolvedBumpFolders(selected);
    $("#ch-resolved-shownext").checked = !!bc.show_next;
    $("#ch-resolved-profile").value = ch.profile_name || "auto";
    loadBrandingDropdown("ch-resolved-branding", ch.branding_logo || "");
    // Event times — convert ISO to datetime-local format (YYYY-MM-DDTHH:MM)
    $("#ch-event-start").value = ch.event_start ? ch.event_start.slice(0, 16) : "";
    $("#ch-event-end").value = ch.event_end ? ch.event_end.slice(0, 16) : "";
  }

  const bc = ch ? (ch.bump_config || {}) : {};
  $("#ch-bump-enabled").checked = !!bc.enabled;
  $("#ch-bump-start").checked = !!bc.start_bumps;
  $("#ch-bump-next").checked = !!bc.show_next;
  $("#ch-bump-freq").value = bc.frequency || "between";
  $("#ch-bump-count").value = bc.count || 1;
  // Shuffle config
  const sc = ch ? ch.shuffle_config : null;
  if (sc && sc.mode) {
    $("#ch-shuffle-mode").value = sc.mode;
  } else if (ch && ch.shuffle) {
    $("#ch-shuffle-mode").value = "random";
  } else {
    $("#ch-shuffle-mode").value = "none";
  }
  editorWeights = (sc && sc.weights) ? Object.assign({}, sc.weights) : {};
  updateWeightsUI();
  $("#ch-loop").checked = ch ? !!ch.loop : true;
  loadBrandingDropdown("ch-branding", ch ? (ch.branding_logo || "") : "");

  const logoPreview = $("#ch-logo-preview");
  const logoInput = $("#ch-logo-input");
  const logoDelete = $("#ch-logo-delete");
  logoInput.value = "";
  if (ch) {
    const logoUrl = `${API}/logo/${ch.id}?t=${Date.now()}`;
    logoPreview.src = logoUrl;
    logoPreview.style.display = "none";
    logoPreview.onload = () => { logoPreview.style.display = "block"; logoDelete.style.display = "inline-block"; };
    logoPreview.onerror = () => { logoPreview.style.display = "none"; logoDelete.style.display = "none"; };
  } else {
    logoPreview.style.display = "none";
    logoDelete.style.display = "none";
  }

  if (!isResolved) {
    const selectedFolders = bc.folders || (bc.folder ? [bc.folder] : []);
    loadBumpFolders(selectedFolders);
    pickerTab = "movies";
    pickerShowEpisodes = null;
    renderEditorItems();
    renderPicker();
    updateSchedulePreview();
  }
  overlay.classList.remove("hidden");
}

function closeEditor() {
  overlay.classList.add("hidden");
  editingChannel = null;
  editorItems = [];
}

// Logo upload handler
$("#ch-logo-input").addEventListener("change", async (e) => {
  if (!editingChannel) { toast("error", "Save the channel first before uploading a logo"); return; }
  const file = e.target.files[0];
  if (!file) return;
  const fd = new FormData();
  fd.append("file", file);
  try {
    const r = await fetch(`${API}/logo/${editingChannel.id}`, { method: "POST", body: fd });
    const d = await r.json();
    if (r.ok) {
      toast("success", "Logo uploaded");
      const preview = $("#ch-logo-preview");
      preview.src = `${API}/logo/${editingChannel.id}?t=${Date.now()}`;
      preview.style.display = "block";
      $("#ch-logo-delete").style.display = "inline-block";
    } else {
      toast("error", d.error || "Upload failed");
    }
  } catch(ex) { toast("error", "Logo upload failed"); }
});

channelarr.deleteLogo = async function() {
  if (!editingChannel) return;
  try {
    await fetch(`${API}/logo/${editingChannel.id}`, { method: "DELETE" });
    $("#ch-logo-preview").style.display = "none";
    $("#ch-logo-delete").style.display = "none";
    toast("success", "Logo removed");
  } catch(ex) { toast("error", "Failed to remove logo"); }
};

// ── Logo search (SearxNG-backed) ────────────────────────────────────
channelarr.openLogoSearch = function() {
  if (!editingChannel) {
    toast("error", "Save the channel first before searching for a logo");
    return;
  }
  const panel = $("#ch-logo-search-panel");
  if (panel.style.display === "none") {
    panel.style.display = "block";
    $("#ch-logo-search-q").value = "";
    $("#ch-logo-search-results").innerHTML = "";
    $("#ch-logo-search-status").textContent = "";
    channelarr.runLogoSearch();
  } else {
    channelarr.closeLogoSearch();
  }
};

channelarr.closeLogoSearch = function() {
  $("#ch-logo-search-panel").style.display = "none";
};

channelarr.runLogoSearch = async function() {
  if (!editingChannel) return;
  const grid = $("#ch-logo-search-results");
  const status = $("#ch-logo-search-status");
  const q = $("#ch-logo-search-q").value.trim();
  grid.innerHTML = "";
  status.textContent = "Searching…";
  try {
    const url = q
      ? `${API}/channels/${editingChannel.id}/logo-search?q=${encodeURIComponent(q)}`
      : `${API}/channels/${editingChannel.id}/logo-search`;
    const r = await fetch(url);
    const d = await r.json();
    if (!r.ok) {
      status.textContent = `Search failed: ${d.detail || r.status}`;
      return;
    }
    const cands = d.candidates || [];
    if (!cands.length) {
      status.textContent = `No results for ${esc(d.query || "")}.`;
      return;
    }
    status.textContent = `${cands.length} candidates for ${esc(d.query)}. Click to apply.`;
    grid.innerHTML = cands.map((c, i) => `
      <div class="ch-logo-candidate" data-idx="${i}" title="${esc(c.title || "")} — ${esc(c.domain || "")} (score ${c.score})">
        <img src="${esc(c.thumbnail || c.url)}" alt="" loading="lazy" onerror="this.style.opacity=0.2"/>
        <div class="ch-logo-candidate-meta">${esc(c.domain || "")}</div>
      </div>
    `).join("");
    grid.querySelectorAll(".ch-logo-candidate").forEach(el => {
      el.addEventListener("click", () => channelarr.pickLogo(cands[Number(el.dataset.idx)].url, el));
    });
  } catch(ex) {
    status.textContent = `Search failed: ${ex}`;
  }
};

// Refresh just the visible logo for a single channel without reloading
// the whole channel list — append a cache-buster so the browser pulls
// the new bytes.
channelarr.refreshLogoFor = function(channelId) {
  document.querySelectorAll(`.channel-card[data-id="${channelId}"] .channel-card-logo`).forEach(img => {
    img.src = `${API}/logo/${channelId}?t=${Date.now()}`;
  });
};

channelarr.generateLogo = async function(channelId) {
  try {
    const r = await fetch(`${API}/channels/${channelId}/logo-auto`, { method: "POST" });
    const d = await r.json();
    if (!r.ok) {
      toast("error", `Generate failed: ${d.detail || r.status}`);
      return;
    }
    if (d.applied) {
      channelarr.refreshLogoFor(channelId);
      toast("success", `Logo generated (${d.message || "ok"})`);
    } else {
      // Soft skip — top hit was below the auto-pick threshold. Open the
      // manual picker so the user can choose from candidates.
      toast("info", `${d.message}. Opening picker…`);
      channelarr.editChannel(channelId);
      setTimeout(() => channelarr.openLogoSearch(), 350);
    }
  } catch(ex) {
    toast("error", `Generate failed: ${ex}`);
  }
};

channelarr.backfillLogos = async function() {
  if (!confirm("Search SearxNG and apply a logo to every channel currently missing one. Continue?")) return;
  toast("info", "Backfill running — this may take a minute…");
  try {
    const r = await fetch(`${API}/logo-search/backfill`, { method: "POST" });
    const d = await r.json();
    if (!r.ok) {
      toast("error", `Backfill failed: ${d.detail || r.status}`);
      return;
    }
    toast("success", `Filled ${d.filled}, skipped ${d.skipped_existing} (already had logos), failed ${d.failed}`);
    if (typeof loadChannels === "function") loadChannels();
  } catch(ex) {
    toast("error", `Backfill failed: ${ex}`);
  }
};

channelarr.pickLogo = async function(url, el) {
  if (!editingChannel) return;
  if (el) el.classList.add("applying");
  try {
    const r = await fetch(`${API}/channels/${editingChannel.id}/logo-pick`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });
    const d = await r.json();
    if (!r.ok) {
      toast("error", `Failed to apply logo: ${d.detail || r.status}`);
      if (el) el.classList.remove("applying");
      return;
    }
    const preview = $("#ch-logo-preview");
    preview.src = `${API}/logo/${editingChannel.id}?t=${Date.now()}`;
    preview.style.display = "block";
    $("#ch-logo-delete").style.display = "inline-block";
    toast("success", `Logo applied (${d.message || "ok"})`);
    channelarr.closeLogoSearch();
  } catch(ex) {
    toast("error", `Failed to apply logo: ${ex}`);
    if (el) el.classList.remove("applying");
  }
};

async function loadResolvedBumpFolders(selectedFolders) {
  try {
    const r = await fetch(`${API}/bumps`);
    const d = await r.json();
    const container = $("#ch-resolved-bump-folders");
    if (!container) return;
    const folders = Object.keys(d.folders || {});
    if (!folders.length) {
      container.innerHTML = '<span class="muted">No bump folders found</span>';
      return;
    }
    container.innerHTML = folders.map(f => {
      const checked = (selectedFolders || []).includes(f) ? " checked" : "";
      return `<label class="bump-folder-check"><input type="checkbox" value="${esc(f)}" class="ch-res-bump-folder-cb"${checked}/> ${esc(f)} <span class="muted">(${d.folders[f]})</span></label>`;
    }).join("");
  } catch (e) {}
}

function getSelectedResolvedBumpFolders() {
  return Array.from($$(".ch-res-bump-folder-cb:checked")).map(cb => cb.value);
}

// Toggle transcode options based on stream mode dropdown
document.addEventListener("DOMContentLoaded", () => {
  const sel = document.getElementById("ch-resolved-encoder-mode");
  if (sel) {
    sel.addEventListener("change", () => {
      const isTranscode = (sel.value === "single" || sel.value === "multi");
      $("#ch-transcode-options").style.display = isTranscode ? "" : "none";
      $("#ch-resolved-bump-section").style.display = isTranscode ? "" : "none";
    });
  }
});

async function loadBumpFolders(selectedFolders) {
  try {
    const r = await fetch(`${API}/bumps`);
    const d = await r.json();
    const container = $("#ch-bump-folders");
    const folders = Object.keys(d.folders || {});
    if (!folders.length) {
      container.innerHTML = '<span class="muted">No bump folders found</span>';
      return;
    }
    container.innerHTML = folders.map(f => {
      const checked = (selectedFolders || []).includes(f) ? " checked" : "";
      return `<label class="bump-folder-check"><input type="checkbox" value="${esc(f)}" class="ch-bump-folder-cb"${checked}/> ${esc(f)} <span class="muted">(${d.folders[f]})</span></label>`;
    }).join("");
    container.querySelectorAll(".ch-bump-folder-cb").forEach(cb => {
      cb.addEventListener("change", updateSchedulePreview);
    });
  } catch(e) {}
}

function getSelectedBumpFolders() {
  return Array.from($$(".ch-bump-folder-cb:checked")).map(cb => cb.value);
}

// ─── Shuffle weights UI ───
$("#ch-shuffle-mode").addEventListener("change", () => {
  updateWeightsUI();
  updateSchedulePreview();
});

function updateWeightsUI() {
  const mode = $("#ch-shuffle-mode").value;
  const section = $("#ch-weights-section");
  if (mode !== "weighted") {
    section.style.display = "none";
    return;
  }
  section.style.display = "";
  renderWeightRows();
}

function renderWeightRows() {
  const list = $("#ch-weights-list");
  const shows = editorItems.filter(it => it.type === "show");
  if (!shows.length) {
    list.innerHTML = '<div class="muted">Add TV shows to set weights</div>';
    $("#ch-weights-total").textContent = "";
    return;
  }
  // Initialize missing weights with even split
  const evenPct = Math.floor(100 / shows.length);
  shows.forEach((s, i) => {
    if (editorWeights[s.path] === undefined) {
      editorWeights[s.path] = i === shows.length - 1 ? 100 - evenPct * (shows.length - 1) : evenPct;
    }
  });
  // Remove stale paths
  const showPaths = new Set(shows.map(s => s.path));
  Object.keys(editorWeights).forEach(k => { if (!showPaths.has(k)) delete editorWeights[k]; });

  list.innerHTML = shows.map(s => `
    <div class="ch-weight-row">
      <span class="ch-weight-name" title="${esc(s.path)}">${esc(s.title || s.path)}</span>
      <input type="number" min="0" max="100" value="${editorWeights[s.path] || 0}"
        data-weight-path="${esc(s.path)}" class="ch-weight-input" />
      <span class="ch-weight-pct">%</span>
    </div>
  `).join("");

  list.querySelectorAll(".ch-weight-input").forEach(inp => {
    inp.addEventListener("input", () => {
      editorWeights[inp.dataset.weightPath] = parseInt(inp.value) || 0;
      updateWeightsTotal();
    });
  });
  updateWeightsTotal();
}

function updateWeightsTotal() {
  const total = Object.values(editorWeights).reduce((a, b) => a + b, 0);
  const el = $("#ch-weights-total");
  el.textContent = `Total: ${total}%`;
  el.className = "ch-weights-total " + (total === 100 ? "valid" : "invalid");
}

function renderEditorItems() {
  const list = $("#ch-items");
  $("#ch-item-count").textContent = `(${editorItems.length})`;
  if (!editorItems.length) {
    list.innerHTML = '<div class="empty-state">No content added</div>';
    return;
  }
  list.innerHTML = editorItems.map((item, i) => {
    const badge = item.type === "show" ? "show" : item.type === "youtube" ? "yt" : (item.type || "?");
    const titleAttr = item.type === "youtube" ? item.url : item.path;
    return `
    <div class="ch-item">
      <span class="ch-item-type ${item.type === "show" ? "ch-item-show" : item.type === "youtube" ? "ch-item-yt" : ""}">${esc(badge)}</span>
      <span class="ch-item-title" title="${esc(titleAttr || "")}">${esc(item.title || item.path || item.url)}</span>
      <button class="btn-remove" onclick="channelarr.removeItem(${i})">&times;</button>
    </div>`;
  }).join("");
}

channelarr.removeItem = function(i) {
  editorItems.splice(i, 1);
  renderEditorItems();
  if ($("#ch-shuffle-mode").value === "weighted") renderWeightRows();
  updateSchedulePreview();
};

channelarr.addToChannel = function(item) {
  editorItems.push(item);
  renderEditorItems();
  if ($("#ch-shuffle-mode").value === "weighted") renderWeightRows();
  updateSchedulePreview();
  toast("info", `Added: ${item.title}`);
};

// Picker
$$(".picker-tab").forEach(btn => {
  btn.addEventListener("click", () => {
    pickerTab = btn.dataset.ptab;
    pickerShowEpisodes = null;
    $$(".picker-tab").forEach(b => b.classList.toggle("active", b === btn));
    renderPicker();
  });
});

$("#picker-search").addEventListener("input", () => renderPicker());

async function renderPicker() {
  const list = $("#picker-list");
  const q = ($("#picker-search").value || "").toLowerCase();

  if (pickerShowEpisodes) {
    let eps = pickerShowEpisodes.episodes;
    if (q) eps = eps.filter(e => (e.label || "").toLowerCase().includes(q));
    list.innerHTML = `<button class="picker-back" onclick="channelarr.pickerBack()">&#8592; Back to shows</button>` +
      eps.map(ep => `
        <div class="picker-item">
          <div class="picker-item-info">
            <div class="picker-item-title">${esc(ep.label)}</div>
            <div class="picker-item-sub">${ep.runtime ? ep.runtime + " min" : ""}</div>
          </div>
          <button class="btn-add" onclick='channelarr.addToChannel(${JSON.stringify({type:"episode",path:ep.path,title:pickerShowEpisodes.title+" "+ep.label,runtime:ep.runtime||0}).replace(/'/g,"\\'")})'">Add</button>
        </div>
      `).join("");
    return;
  }

  if (pickerTab === "movies") {
    if (!movies.length) await loadMovies();
    let filtered = movies;
    if (q) filtered = movies.filter(m => m.title.toLowerCase().includes(q));
    list.innerHTML = filtered.slice(0, 100).map(m => `
      <div class="picker-item">
        <img class="picker-poster" src="${API}/media/poster?path=${encodeURIComponent(m.path)}" alt="" onerror="this.classList.add('no-poster')" />
        <div class="picker-item-info">
          <div class="picker-item-title">${esc(m.title)}</div>
          <div class="picker-item-sub">${m.year || ""} ${m.runtime ? "| " + m.runtime + " min" : ""}</div>
        </div>
        <button class="btn-add" onclick='channelarr.addToChannel(${JSON.stringify({type:"movie",path:m.path,title:m.title,runtime:m.runtime||0}).replace(/'/g,"\\'")})'">Add</button>
      </div>
    `).join("") || '<div class="empty-state">No movies found</div>';
  } else if (pickerTab === "youtube") {
    list.innerHTML = `
      <div class="yt-browse-form">
        <input type="text" id="yt-browse-url" class="media-search" placeholder="Paste YouTube channel or playlist URL..." />
        <button class="btn btn-primary" id="yt-browse-btn">Browse</button>
      </div>
      <div id="yt-results">${ytResults.length ? "" : '<div class="empty-state">Paste a URL and click Browse</div>'}</div>
    `;
    $("#yt-browse-btn").addEventListener("click", browseYouTube);
    $("#yt-browse-url").addEventListener("keydown", e => { if (e.key === "Enter") browseYouTube(); });
    if (ytResults.length) renderYTResults();
  } else {
    if (!tvShows.length) await loadTVShows();
    let filtered = tvShows;
    if (q) filtered = tvShows.filter(s => s.title.toLowerCase().includes(q));
    list.innerHTML = filtered.slice(0, 100).map((s, idx) => `
      <div class="picker-item picker-show" style="cursor:pointer" data-idx="${idx}">
        <img class="picker-poster" src="${API}/media/poster?path=${encodeURIComponent(s.path)}" alt="" onerror="this.classList.add('no-poster')" />
        <div class="picker-item-info">
          <div class="picker-item-title">${esc(s.title)}</div>
          <div class="picker-item-sub">${s.episodeCount || 0} episodes</div>
        </div>
        <button class="btn-add picker-addshow-btn" data-idx="${idx}">Add Show</button>
        <button class="btn-add picker-show-btn" data-idx="${idx}">Episodes &rarr;</button>
      </div>
    `).join("") || '<div class="empty-state">No shows found</div>';
    list.querySelectorAll(".picker-show").forEach(el => {
      const idx = parseInt(el.dataset.idx);
      const s = filtered[idx];
      const drillHandler = (e) => { e.stopPropagation(); channelarr.drillShow(s.path, s.title); };
      el.querySelector(".picker-show-btn").addEventListener("click", drillHandler);
      el.querySelector(".picker-addshow-btn").addEventListener("click", (e) => {
        e.stopPropagation();
        channelarr.addToChannel({type:"show", path:s.path, title:s.title});
      });
      el.addEventListener("click", drillHandler);
    });
  }
}

channelarr.pickerBack = function() {
  pickerShowEpisodes = null;
  renderPicker();
};

channelarr.drillShow = async function(showPath, title) {
  try {
    const r = await fetch(`${API}/media/tv/episodes?path=${encodeURIComponent(showPath)}`);
    const d = await r.json();
    if (!r.ok || !Array.isArray(d)) {
      toast("error", d.error || "Failed to load episodes");
      return;
    }
    pickerShowEpisodes = { path: showPath, title: title, episodes: d };
    renderPicker();
  } catch(e) { toast("error", "Failed to load episodes"); }
};

// ─── YouTube picker ───
let ytResults = [];

async function browseYouTube() {
  const url = ($("#yt-browse-url") || {}).value;
  if (!url || !url.trim()) return;
  const resultsDiv = $("#yt-results");
  if (resultsDiv) resultsDiv.innerHTML = '<div class="empty-state">Loading...</div>';
  try {
    const r = await fetch(`${API}/youtube/browse`, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({url: url.trim()}),
    });
    const d = await r.json();
    if (!r.ok) { toast("error", d.error || "Browse failed"); return; }
    ytResults = d.videos || [];
    renderYTResults();
    toast("info", `Found ${ytResults.length} videos`);
  } catch(e) { toast("error", "Failed to browse YouTube"); }
}

function renderYTResults() {
  const resultsDiv = $("#yt-results");
  if (!resultsDiv) return;
  const q = ($("#picker-search").value || "").toLowerCase();
  let filtered = ytResults;
  if (q) filtered = ytResults.filter(v => (v.title || "").toLowerCase().includes(q));

  let html = "";
  if (filtered.length > 1) {
    html += `<div class="yt-add-all"><button class="btn btn-primary" id="yt-add-all-btn">Add All (${filtered.length})</button></div>`;
  }
  html += filtered.slice(0, 50).map((v, i) => `
    <div class="picker-item">
      <img class="picker-poster" src="${esc(v.thumbnail || "")}" alt="" onerror="this.classList.add('no-poster')" />
      <div class="picker-item-info">
        <div class="picker-item-title">${esc(v.title)}</div>
        <div class="picker-item-sub">${v.duration ? formatDuration(v.duration) : "?"}</div>
      </div>
      <button class="btn-add yt-add-one" data-idx="${i}">Add</button>
    </div>
  `).join("") || '<div class="empty-state">No videos found</div>';

  resultsDiv.innerHTML = html;

  // Bind individual add buttons
  resultsDiv.querySelectorAll(".yt-add-one").forEach(btn => {
    btn.addEventListener("click", () => {
      const v = filtered[parseInt(btn.dataset.idx)];
      channelarr.addToChannel({
        type: "youtube", url: v.url, yt_id: v.yt_id,
        title: v.title, duration: v.duration || 0, thumbnail: v.thumbnail || "",
      });
    });
  });

  // Bind add-all button
  const addAllBtn = document.getElementById("yt-add-all-btn");
  if (addAllBtn) {
    addAllBtn.addEventListener("click", () => {
      filtered.forEach(v => {
        editorItems.push({
          type: "youtube", url: v.url, yt_id: v.yt_id,
          title: v.title, duration: v.duration || 0, thumbnail: v.thumbnail || "",
        });
      });
      renderEditorItems();
      if ($("#ch-shuffle-mode").value === "weighted") renderWeightRows();
      updateSchedulePreview();
      toast("info", `Added ${filtered.length} YouTube videos`);
    });
  }
}

function updateSchedulePreview() {
  const container = $("#ch-schedule");
  if (!editorItems.length) {
    container.innerHTML = '<div class="empty-state">Add content to see schedule</div>';
    return;
  }
  const bumpEnabled = $("#ch-bump-enabled").checked;
  const bumpFolders = getSelectedBumpFolders();
  const bumpFreq = $("#ch-bump-freq").value;
  const bumpCount = parseInt($("#ch-bump-count").value) || 1;
  const startBumps = $("#ch-bump-start").checked;
  const shuffleMode = $("#ch-shuffle-mode").value;

  // Reorder items based on shuffle mode for preview
  let previewItems = editorItems.slice();
  if (shuffleMode === "round_robin") {
    // Show interleaved preview: rotate through shows in order
    const groups = [];
    const groupMap = {};
    previewItems.forEach(item => {
      const key = item.type === "show" ? item.path : "__standalone__" + item.path;
      if (!groupMap[key]) { groupMap[key] = []; groups.push(groupMap[key]); }
      groupMap[key].push(item);
    });
    previewItems = [];
    let more = true;
    let idx = 0;
    while (more) {
      more = false;
      for (const g of groups) {
        if (idx < g.length) { previewItems.push(g[idx]); more = true; }
      }
      idx++;
    }
  }

  let sched = [];
  // Shuffle mode label
  if (shuffleMode !== "none") {
    const labels = {random: "Random shuffle", round_robin: "Round-robin interleave", weighted: "Weighted random"};
    sched.push({ type: "info", title: labels[shuffleMode] || shuffleMode });
  }

  if (bumpEnabled && bumpFolders.length && startBumps) {
    for (let b = 0; b < bumpCount; b++) sched.push({ type: "bump", title: `[bump]` });
  }

  previewItems.forEach((item, i) => {
    if (bumpEnabled && bumpFolders.length && i > 0) {
      if (bumpFreq === "between" || (parseInt(bumpFreq) && i % parseInt(bumpFreq) === 0)) {
        for (let b = 0; b < bumpCount; b++) sched.push({ type: "bump", title: `[bump]` });
      }
    }
    if (item.type === "show") {
      sched.push({ type: "show", title: (item.title || item.path) + " (all episodes)" });
    } else if (item.type === "youtube") {
      sched.push({ type: "youtube", title: item.title || item.url });
    } else {
      sched.push({ type: item.type, title: item.title || item.path });
    }
  });

  container.innerHTML = sched.map((s, i) => {
    if (s.type === "info") {
      return `<div class="sched-item" style="background:rgba(88,166,255,.1);border:1px solid rgba(88,166,255,.25);color:var(--accent);font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;justify-content:center">${esc(s.title)}</div>`;
    }
    const cls = s.type === "bump" ? "sched-bump" : s.type === "show" ? "sched-show" : s.type === "youtube" ? "sched-yt" : "sched-content";
    return `
    <div class="sched-item ${cls}">
      <span class="sched-num">${i}</span>
      <span>${esc(s.title)}</span>
    </div>`;
  }).join("");
}

["ch-bump-enabled", "ch-bump-freq", "ch-bump-count", "ch-bump-start", "ch-bump-next"].forEach(id => {
  $(`#${id}`).addEventListener("change", updateSchedulePreview);
});
// Note: ch-shuffle-mode change listener is set up in the shuffle weights UI section above

function parseTags() {
  return ($("#ch-tags").value || "").split(",").map(s => s.trim()).filter(Boolean);
}

async function saveChannel() {
  const saveBtn = $("#modal-save");
  // Guard against double-click: schedule generation for big YouTube
  // playlists takes seconds, and an enabled button + visible modal during
  // that window invites repeat clicks that each create a duplicate channel.
  if (saveBtn && saveBtn.disabled) return;
  const name = $("#ch-name").value.trim();
  if (!name) { toast("error", "Channel name required"); return; }

  const isResolved = !!(editingChannel && editingChannel.type === "resolved");

  let data;
  if (isResolved) {
    // Resolved channels
    const encoderMode = $("#ch-resolved-encoder-mode").value || "proxy";
    const isTranscode = (encoderMode === "single" || encoderMode === "multi");
    const evStart = $("#ch-event-start").value;
    const evEnd = $("#ch-event-end").value;
    data = {
      name,
      tags: parseTags(),
      transcode_mediated: isTranscode,
      profile_name: $("#ch-resolved-profile").value || "auto",
      encoder_mode: encoderMode,
      branding_logo: $("#ch-resolved-branding").value || null,
      event_start: evStart ? new Date(evStart).toISOString() : null,
      event_end: evEnd ? new Date(evEnd).toISOString() : null,
    };
    if (isTranscode) {
      data.bump_config = {
        enabled: true,
        folders: getSelectedResolvedBumpFolders(),
        show_next: $("#ch-resolved-shownext").checked,
      };
    }
  } else {
    const shuffleMode = $("#ch-shuffle-mode").value;
    const shuffleConfig = { mode: shuffleMode };
    if (shuffleMode === "weighted") {
      const total = Object.values(editorWeights).reduce((a, b) => a + b, 0);
      if (total !== 100) { toast("error", "Weights must total 100%"); return; }
      shuffleConfig.weights = Object.assign({}, editorWeights);
    }
    data = {
      name,
      items: editorItems,
      bump_config: {
        enabled: $("#ch-bump-enabled").checked,
        folders: getSelectedBumpFolders(),
        frequency: $("#ch-bump-freq").value,
        count: parseInt($("#ch-bump-count").value) || 1,
        start_bumps: $("#ch-bump-start").checked,
        show_next: $("#ch-bump-next").checked,
      },
      shuffle: shuffleMode === "random",
      shuffle_config: shuffleConfig,
      loop: $("#ch-loop").checked,
      branding_logo: $("#ch-branding").value || null,
      tags: parseTags(),
    };
  }

  if (saveBtn) {
    saveBtn.disabled = true;
    saveBtn.dataset.origLabel = saveBtn.textContent;
    saveBtn.textContent = "Saving…";
  }
  try {
    let r;
    if (editingChannel) {
      r = await fetch(`${API}/channels/${editingChannel.id}`, {
        method: "PUT",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(data),
      });
    } else {
      r = await fetch(`${API}/channels`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(data),
      });
    }
    if (!r.ok) {
      let detail = `${r.status}`;
      try { detail = (await r.json()).detail || detail; } catch(_) {}
      toast("error", `Failed to save channel: ${detail}`);
      return;
    }
    toast("success", editingChannel ? "Channel updated — schedule regenerated"
                                    : "Channel created — schedule generated");
    closeEditor();
    loadChannels();
    updateStatus();
  } catch(e) {
    toast("error", `Failed to save channel: ${e}`);
  } finally {
    if (saveBtn) {
      saveBtn.disabled = false;
      if (saveBtn.dataset.origLabel) {
        saveBtn.textContent = saveBtn.dataset.origLabel;
        delete saveBtn.dataset.origLabel;
      }
    }
  }
}

// ─── Guide View ───
$$(".guide-range").forEach(btn => {
  btn.addEventListener("click", () => {
    guideHours = parseInt(btn.dataset.hours);
    $$(".guide-range").forEach(b => b.classList.toggle("active", b === btn));
    loadGuide();
  });
});

async function loadGuide() {
  try {
    const r = await fetch(`${API}/epg/guide?hours=${guideHours}`);
    const data = await r.json();
    renderGuide(data);
  } catch(e) { toast("error", "Failed to load guide"); }
}

function renderGuide(data) {
  const grid = $("#guide-grid");

  if (!data.channels || !data.channels.length) {
    grid.innerHTML = '<div class="guide-empty">No guide data. Create channels and click Regenerate.</div>';
    return;
  }

  const wStart = new Date(data.start).getTime();
  const wEnd = new Date(data.end).getTime();
  const wDur = wEnd - wStart;
  const pxPerMs = 4000 / wDur;
  const now = Date.now();

  // Channel sidebar column
  let chHtml = '<div class="guide-channels">';
  chHtml += '<div class="guide-ch-row" style="height:36px"></div>'; // spacer for time header
  data.channels.forEach(ch => {
    const logoUrl = `${API}/logo/${ch.id}`;
    chHtml += `<div class="guide-ch-row">
      <img class="guide-ch-logo" src="${logoUrl}" alt="" onerror="this.classList.add('no-logo')" />
      <span class="guide-ch-name" title="${esc(ch.name)}">${esc(ch.name)}</span>
    </div>`;
  });
  chHtml += '</div>';

  // Timeline column
  let tlHtml = '<div class="guide-timeline" id="guide-tl"><div class="guide-time-header">';
  const hourMs = 3600000;
  const firstHour = new Date(Math.ceil(wStart / hourMs) * hourMs);
  for (let t = firstHour.getTime(); t < wEnd; t += hourMs) {
    const w = Math.min(hourMs, wEnd - t) * pxPerMs;
    const label = new Date(t).toLocaleTimeString([], {hour: "numeric", minute: "2-digit"});
    tlHtml += `<div class="guide-time-mark" style="width:${w}px">${label}</div>`;
  }
  tlHtml += '</div><div class="guide-rows" id="guide-rows" style="position:relative">';

  data.channels.forEach(ch => {
    tlHtml += '<div class="guide-row">';
    (ch.entries || []).forEach(entry => {
      const pStart = Math.max(new Date(entry.start).getTime(), wStart);
      const pStop = Math.min(new Date(entry.stop).getTime(), wEnd);
      const left = (pStart - wStart) * pxPerMs;
      const width = Math.max(2, (pStop - pStart) * pxPerMs);
      const isNow = now >= pStart && now < pStop;
      const cls = entry.type === "movie" ? "gp-movie" : entry.type === "episode" ? "gp-episode" : "gp-default";

      const startT = new Date(entry.start).toLocaleTimeString([], {hour: "numeric", minute: "2-digit"});
      const stopT = new Date(entry.stop).toLocaleTimeString([], {hour: "numeric", minute: "2-digit"});
      const posterUrl = entry.thumbnail ? entry.thumbnail : entry.path ? `${API}/media/poster?path=${encodeURIComponent(entry.path)}` : "";
      const imgTag = posterUrl && width > 44 ? `<img class="gp-icon" src="${posterUrl}" onerror="this.remove()">` : "";
      const titleSpan = width > (posterUrl && width > 44 ? 80 : 60) ? `<span class="gp-title">${esc(entry.title)}</span>` : "";

      const safeDesc = (entry.desc || "").replace(/"/g, "&quot;");
      const safeTitle = (entry.title || "").replace(/"/g, "&quot;");
      const safePoster = posterUrl.replace(/"/g, "&quot;");

      tlHtml += `<div class="guide-prog ${cls} ${isNow ? "gp-now" : ""}"
        style="position:absolute;left:${left}px;width:${width}px"
        data-prog-title="${safeTitle}"
        data-desc="${safeDesc}"
        data-time="${startT} - ${stopT}"
        data-poster="${safePoster}"
        data-ch-id="${ch.id}" data-ch-name="${esc(ch.name)}"
        >${imgTag}${titleSpan}</div>`;
    });
    tlHtml += '</div>';
  });

  // Now line
  const nowPos = (now - wStart) * pxPerMs;
  tlHtml += `<div class="guide-now-line" id="guide-now" style="left:${nowPos}px"></div>`;
  tlHtml += '</div></div>';

  grid.innerHTML = chHtml + tlHtml;

  // Click popover on programme blocks
  document.querySelectorAll(".guide-prog").forEach(el => {
    el.addEventListener("click", e => {
      e.stopPropagation();
      const old = document.getElementById("guide-prog-detail");
      if (old) old.remove();
      const t = el.getAttribute("data-prog-title") || "";
      if (!t) return;
      const d = el.getAttribute("data-desc") || "";
      const tm = el.getAttribute("data-time") || "";
      const poster = el.getAttribute("data-poster") || "";
      const chId = el.getAttribute("data-ch-id") || "";
      const chName = el.getAttribute("data-ch-name") || "";

      const pop = document.createElement("div");
      pop.id = "guide-prog-detail";
      pop.innerHTML = `<div class="gpd-inner">`
        + (poster ? `<img class="gpd-poster" src="${poster}" onerror="this.remove()">` : "")
        + `<div class="gpd-info"><div class="gpd-title">${t}</div>`
        + `<div class="gpd-time">${tm}</div>`
        + (d ? `<div class="gpd-desc">${d}</div>` : `<div class="gpd-desc gpd-nodesc">No description</div>`)
        + `<button class="btn btn-sm" style="margin-top:8px" onclick="channelarr.watchChannel('${chId}','${chName.replace(/'/g,"\\'")}')">Watch</button>`
        + `</div></div>`;
      document.body.appendChild(pop);

      const rect = el.getBoundingClientRect();
      pop.style.top = Math.min(rect.bottom + 6, window.innerHeight - pop.offsetHeight - 10) + "px";
      pop.style.left = Math.min(rect.left, window.innerWidth - pop.offsetWidth - 10) + "px";
    });
  });
  document.addEventListener("click", () => {
    const p = document.getElementById("guide-prog-detail");
    if (p) p.remove();
  });

  // Scroll to now
  const tl = $("#guide-tl");
  if (tl) tl.scrollLeft = Math.max(0, nowPos - 200);
}

function formatTime(d) {
  if (typeof d === "number") d = new Date(d);
  return d.toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"});
}

// ─── Media Browse View ───
let mediaTab = "movies";

$$(".media-tab").forEach(btn => {
  btn.addEventListener("click", () => {
    mediaTab = btn.dataset.tab;
    $$(".media-tab").forEach(b => b.classList.toggle("active", b === btn));
    loadMediaView();
  });
});

$("#media-search").addEventListener("input", () => renderMediaList());

async function loadMediaView() {
  if (mediaTab === "movies") {
    if (!movies.length) await loadMovies();
  } else {
    if (!tvShows.length) await loadTVShows();
  }
  renderMediaList();
}

async function loadMovies() {
  try {
    const r = await fetch(`${API}/media/movies`);
    movies = await r.json();
  } catch(e) { toast("error", "Failed to load movies"); }
}

async function loadTVShows() {
  try {
    const r = await fetch(`${API}/media/tv`);
    tvShows = await r.json();
  } catch(e) { toast("error", "Failed to load TV shows"); }
}

function renderMediaList() {
  const list = $("#media-list");
  const q = ($("#media-search").value || "").toLowerCase();

  if (mediaTab === "movies") {
    let filtered = movies;
    if (q) filtered = movies.filter(m => m.title.toLowerCase().includes(q));
    list.innerHTML = filtered.map(m => `
      <div class="media-item">
        <img class="media-poster" src="${API}/media/poster?path=${encodeURIComponent(m.path)}" alt="" onerror="this.classList.add('no-poster')" />
        <div class="media-item-info">
          <div class="media-item-title">${esc(m.title)}</div>
          <div class="media-item-sub">${m.year || ""} ${m.runtime ? "| " + m.runtime + " min" : ""} ${(m.genres||[]).join(", ")}</div>
        </div>
      </div>
    `).join("") || '<div class="empty-state">No movies found</div>';
  } else {
    let filtered = tvShows;
    if (q) filtered = tvShows.filter(s => s.title.toLowerCase().includes(q));
    list.innerHTML = filtered.map(s => `
      <div class="media-item">
        <img class="media-poster" src="${API}/media/poster?path=${encodeURIComponent(s.path)}" alt="" onerror="this.classList.add('no-poster')" />
        <div class="media-item-info">
          <div class="media-item-title">${esc(s.title)}</div>
          <div class="media-item-sub">${s.year || ""} | ${s.episodeCount || 0} episodes | ${(s.genres||[]).join(", ")}</div>
        </div>
      </div>
    `).join("") || '<div class="empty-state">No shows found</div>';
  }
}

// ─── Bumps ───
async function loadBumps() {
  try {
    const r = await fetch(`${API}/bumps`);
    bumpData = await r.json();
    renderBumps();
  } catch(e) { toast("error", "Failed to load bumps"); }
}

function renderBumps() {
  const grid = $("#bumps-grid");
  const clips = bumpData.clips || {};
  const folders = bumpData.folders || {};
  if (!Object.keys(folders).length) {
    grid.innerHTML = '<div class="empty-state"><div class="empty-icon">&#127917;</div>No bump clips found. Add videos to the bumps folder.</div>';
    return;
  }
  grid.innerHTML = Object.entries(clips).map(([name, items]) => `
    <div class="bump-folder" onclick="channelarr.toggleBumpFolder(this)">
      <h4>${esc(name)}</h4>
      <div class="bump-count">${items.length} clip${items.length !== 1 ? "s" : ""}</div>
      <div class="bump-clips">
        ${items.map(clip => `
          <div class="bump-clip">
            <img class="bump-thumb" src="${API}/bumps/thumbnail?path=${encodeURIComponent(clip.path)}" alt="" onerror="this.classList.add('no-thumb')" />
            <span class="bump-clip-name" title="${esc(clip.path)}">${esc(clip.name)}</span>
            <button class="btn-sm" onclick="event.stopPropagation(); channelarr.previewBump('${esc(clip.path).replace(/'/g,"\\'")}', '${esc(clip.name).replace(/'/g,"\\'")}')">Preview</button>
            <button class="btn-sm btn-danger" onclick="event.stopPropagation(); channelarr.deleteBump('${esc(clip.path).replace(/'/g,"\\'")}', '${esc(clip.name).replace(/'/g,"\\'")}')">Delete</button>
          </div>
        `).join("")}
      </div>
    </div>
  `).join("");
}

channelarr.toggleBumpFolder = function(el) {
  const clips = el.querySelector(".bump-clips");
  if (clips) clips.classList.toggle("hidden");
};

channelarr.deleteBump = async function(path, name) {
  if (!confirm(`Delete bump clip "${name}"?`)) return;
  try {
    const r = await fetch(`${API}/bumps/clip`, {
      method: "DELETE",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({path}),
    });
    const d = await r.json();
    if (r.ok) {
      toast("success", d.message);
      loadBumps();
    } else {
      toast("error", d.error || "Delete failed");
    }
  } catch(e) { toast("error", "Failed to delete bump"); }
};

channelarr.previewBump = function(path, name) {
  const url = `/preview/bump?path=${encodeURIComponent(path)}`;
  $("#player-title").textContent = name || "Preview";
  const playerOverlay = $("#player-overlay");
  const playerVideo = $("#player-video");
  playerOverlay.classList.remove("hidden");
  if (activeHls) { activeHls.destroy(); activeHls = null; }
  playerVideo.src = url;
  playerVideo.play().catch(() => {});
};

$("#rescan-bumps").addEventListener("click", async () => {
  try {
    const r = await fetch(`${API}/bumps/scan`, {method:"POST"});
    bumpData = await r.json();
    renderBumps();
    toast("success", `Scan complete: ${bumpData.total || 0} clips`);
  } catch(e) { toast("error", "Rescan failed"); }
});

$("#bump-dl-btn").addEventListener("click", async () => {
  const url = $("#bump-dl-url").value.trim();
  const folder = $("#bump-dl-folder").value.trim();
  const resolution = $("#bump-dl-res").value;
  if (!url) { toast("error", "Enter a YouTube URL"); return; }
  if (!folder) { toast("error", "Enter a folder name"); return; }
  try {
    const r = await fetch(`${API}/bumps/download`, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({url, folder, resolution}),
    });
    const d = await r.json();
    if (r.ok) {
      toast("success", d.message);
      $("#bump-dl-url").value = "";
    } else {
      toast("error", d.error || "Download failed");
    }
  } catch(e) { toast("error", "Download request failed"); }
});

// ─── Logs ───
async function pollLogs() {
  try {
    const url = new URL(`${API}/logs/tail`, window.location.origin);
    url.searchParams.set("pos", String(tailPos));
    if (tailInode) url.searchParams.set("inode", tailInode);
    const r = await fetch(url);
    const d = await r.json();
    if (d.reset || (tailInode && d.inode && d.inode !== tailInode)) {
      logOut.textContent = "";
    }
    if (d.text && d.text.length) {
      logOut.textContent += d.text;
      const c = logOut.parentElement;
      c.scrollTop = c.scrollHeight;
    }
    tailPos = d.pos;
    tailInode = d.inode;
  } catch(e) {}
}

$("#clear-log").addEventListener("click", () => { logOut.textContent = ""; });

// ─── Settings ───
async function loadSettings() {
  try {
    const r = await fetch(`${API}/settings`);
    const data = await r.json();
    settingsSchema = data.schema;
    settingsOriginal = {...data.values};
    settingsModified = {...data.values};
    renderAllSettings();
    // Show/hide export links based on strategy
    _updateExportLinks(data.values);
  } catch(e) {}
}

function _updateExportLinks(vals) {
  const links = $("#output-links");
  if (!links) return;
  if ((vals || settingsOriginal).EXPORT_STRATEGY === "local") {
    const path = (vals || settingsOriginal).EXPORT_LOCAL_PATH || "/output/m3u";
    links.innerHTML = `<span class="output-link" style="cursor:default;font-size:11px" title="Local path exports">${esc(path)}/channelarr.m3u</span>`;
  } else {
    links.innerHTML = `
      <a href="/api/export/m3u" target="_blank" class="output-link" title="M3U Playlist URL">M3U</a><button class="copy-btn" data-copy-path="/api/export/m3u" title="Copy M3U URL">&#x2398;</button>
      <a href="/api/export/xmltv" target="_blank" class="output-link" title="XMLTV EPG URL">EPG</a><button class="copy-btn" data-copy-path="/api/export/xmltv" title="Copy EPG URL">&#x2398;</button>`;
  }
}

function renderAllSettings() {
  const container = $("#settings-container");
  container.innerHTML = "";
  const grid = document.createElement("div");
  grid.className = "settings-grid";

  for (const [sectionKey, section] of Object.entries(settingsSchema)) {
    const group = document.createElement("div");
    group.className = "settings-group";
    const heading = document.createElement("h4");
    heading.textContent = section.label;
    group.appendChild(heading);

    for (const [fieldKey, field] of Object.entries(section.fields)) {
      const isModified = settingsModified[fieldKey] !== settingsOriginal[fieldKey];
      const div = document.createElement("div");
      div.className = `setting-field${isModified ? " modified" : ""}`;

      const label = document.createElement("label");
      label.textContent = field.label;
      div.appendChild(label);

      if (field.type === "select") {
        const sel = document.createElement("select");
        (field.options || []).forEach(opt => {
          const o = document.createElement("option");
          o.value = opt.value;
          o.textContent = opt.label;
          sel.appendChild(o);
        });
        sel.value = settingsModified[fieldKey] || "";
        sel.addEventListener("change", () => {
          settingsModified[fieldKey] = sel.value;
          div.classList.toggle("modified", settingsModified[fieldKey] !== settingsOriginal[fieldKey]);
        });
        div.appendChild(sel);
      } else {
        const wrap = document.createElement("div");
        wrap.className = "input-wrap";
        const inp = document.createElement("input");
        inp.type = field.type || "text";
        inp.placeholder = field.placeholder || "";
        inp.value = settingsModified[fieldKey] || "";
        inp.addEventListener("input", () => {
          settingsModified[fieldKey] = inp.value;
          div.classList.toggle("modified", settingsModified[fieldKey] !== settingsOriginal[fieldKey]);
        });
        wrap.appendChild(inp);
        div.appendChild(wrap);
      }

      group.appendChild(div);
    }
    grid.appendChild(group);
  }
  container.appendChild(grid);
}

$("#save-settings").addEventListener("click", async () => {
  const status = $("#settings-status");
  try {
    const r = await fetch(`${API}/settings`, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(settingsModified),
    });
    const d = await r.json();
    status.textContent = d.message || "Saved";
    status.className = "settings-status success";
    settingsOriginal = {...settingsModified};
    renderAllSettings();
    _updateExportLinks(settingsModified);
    toast("success", "Settings saved");
  } catch(e) {
    status.textContent = "Save failed";
    status.className = "settings-status error";
    toast("error", "Failed to save settings");
  }
  setTimeout(() => { status.textContent = ""; status.className = "settings-status"; }, 3000);
});

// ─── Tasks ───
const TASK_INTERVAL_OPTIONS = {
  stream_cleanup: [
    {label: "30 seconds", seconds: 30},
    {label: "1 minute", seconds: 60},
    {label: "2 minutes", seconds: 120},
    {label: "5 minutes", seconds: 300},
  ],
  event_cleanup: [
    {label: "30 seconds", seconds: 30},
    {label: "1 minute", seconds: 60},
    {label: "5 minutes", seconds: 300},
    {label: "15 minutes", seconds: 900},
  ],
  vpn_sampler: [
    {label: "30 seconds", seconds: 30},
    {label: "1 minute", seconds: 60},
    {label: "2 minutes", seconds: 120},
    {label: "5 minutes", seconds: 300},
  ],
  vpn_auto_rotate: [
    {label: "1 minute", seconds: 60},
    {label: "5 minutes", seconds: 300},
    {label: "15 minutes", seconds: 900},
    {label: "30 minutes", seconds: 1800},
    {label: "1 hour", seconds: 3600},
    {label: "2 hours", seconds: 7200},
    {label: "4 hours", seconds: 14400},
    {label: "8 hours", seconds: 28800},
    {label: "12 hours", seconds: 43200},
    {label: "24 hours", seconds: 86400},
  ],
};

function _fmtInterval(secs) {
  if (!secs) return "unknown";
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)} min`;
  return `${Math.floor(secs / 3600)}h`;
}

async function loadTasks() {
  const container = $("#tasks-container");
  if (!container) return;
  try {
    const r = await fetch(`${API}/tasks/status`);
    const data = await r.json();
    renderTasks(data.tasks || []);
    if (!tasksTimer) {
      tasksTimer = setInterval(loadTasks, 10000);
    }
  } catch (e) {
    container.innerHTML = '<div class="empty-state">Failed to load tasks</div>';
  }
}

function renderTasks(tasks) {
  const container = $("#tasks-container");
  if (!container) return;

  if (!tasks.length) {
    container.innerHTML = '<div class="empty-state">No background tasks registered.</div>';
    return;
  }

  container.innerHTML = tasks.map(t => {
    const options = TASK_INTERVAL_OPTIONS[t.id] || [];
    let selectHtml = "";
    if (options.length) {
      selectHtml = `<select class="task-select" data-task-interval="${esc(t.id)}">` +
        options.map(o =>
          `<option value="${o.seconds}" ${t.interval_seconds && Math.abs(t.interval_seconds - o.seconds) < 5 ? "selected" : ""}>${o.label}</option>`
        ).join("") + `</select>`;
    } else if (t.interval_seconds) {
      selectHtml = `<span>${_fmtInterval(t.interval_seconds)}</span>`;
    }
    const nextRun = t.next_run_time ? _timeUntil(t.next_run_time) : "";

    return `<div class="task-card" data-task-id="${esc(t.id)}">
      <div class="task-header">
        <span class="task-name">${esc(t.name)}</span>
        <div class="task-actions">
          <button class="btn-sm" data-task-run="${esc(t.id)}">Run Now</button>
        </div>
      </div>
      <div class="task-meta">
        ${selectHtml ? `<span>Every: ${selectHtml}</span>` : ""}
        ${nextRun ? `<span class="task-next">Next: ${nextRun}</span>` : ""}
      </div>
    </div>`;
  }).join("");

  // Bind interval change handlers
  container.querySelectorAll("[data-task-interval]").forEach(sel => {
    sel.addEventListener("change", async () => {
      const jobId = sel.dataset.taskInterval;
      const seconds = parseInt(sel.value);
      try {
        const r = await fetch(`${API}/tasks/${encodeURIComponent(jobId)}`, {
          method: "PUT",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({interval_seconds: seconds}),
        });
        if (r.ok) {
          toast("success", "Interval updated");
          setTimeout(loadTasks, 500);
        } else {
          toast("error", "Failed to update interval");
        }
      } catch (e) {
        toast("error", "Failed to update interval");
      }
    });
  });

  // Bind run now handlers
  container.querySelectorAll("[data-task-run]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const jobId = btn.dataset.taskRun;
      btn.disabled = true;
      btn.textContent = "Running...";
      try {
        const r = await fetch(`${API}/tasks/${encodeURIComponent(jobId)}/run`, {method: "POST"});
        if (r.ok) {
          toast("success", "Task triggered");
        } else {
          toast("error", "Failed to trigger task");
        }
      } catch (e) {
        toast("error", "Failed to trigger task");
      }
      btn.disabled = false;
      btn.textContent = "Run Now";
      setTimeout(loadTasks, 1000);
    });
  });
}

// ─── Integrations ───
let _integData = {};

async function loadIntegrations() {
  try {
    const r = await fetch(`${API}/integrations/status`);
    _integData = await r.json();
    renderIntegrations();
  } catch (e) {
    $("#integrations-container").innerHTML = '<div class="empty-state">Failed to load integrations</div>';
  }
}

function renderIntegrations() {
  const c = $("#integrations-container");
  if (!c) return;
  const jf = _integData.jellyfin || {};
  const mf = _integData.manifold || {};

  function badge(configured, label) {
    if (!configured) return `<span class="integ-badge integ-not-configured">Not Configured</span>`;
    return `<span class="integ-badge integ-configured">${label || "Configured"}</span>`;
  }

  c.innerHTML = `<div class="integ-grid">
    <div class="integ-card" onclick="channelarr.openIntegModal('jellyfin')">
      <div class="integ-card-header">
        <span class="integ-card-name">Jellyfin</span>
        ${badge(jf.configured)}
      </div>
      <div class="integ-card-desc">Push M3U/XMLTV updates directly with cache-busting refresh.</div>
    </div>
    <div class="integ-card" onclick="channelarr.openIntegModal('manifold')">
      <div class="integ-card-header">
        <span class="integ-card-name">Manifold</span>
        ${badge(mf.configured)}
      </div>
      <div class="integ-card-desc">Sync channelarr's M3U + EPG into manifold and trigger republish. Cascades to Jellyfin if manifold has auto-refresh enabled.</div>
    </div>
  </div>`;
}

channelarr.openIntegModal = function(type) {
  // Reuse the existing modal overlay
  const overlay = $("#modal-overlay");
  const modal = overlay.querySelector(".modal");
  overlay.classList.remove("hidden");

  if (type === "jellyfin") {
    const jf = _integData.jellyfin || {};
    modal.innerHTML = `
      <div class="modal-header"><h3>Jellyfin Integration</h3><button class="btn-close" onclick="document.getElementById('modal-overlay').classList.add('hidden')">&times;</button></div>
      <div class="modal-body" style="padding:16px">
        <div class="integ-modal-fields">
          <label>Server URL<input type="text" id="integ-jf-url" value="${esc(jf.url || "")}" placeholder="http://192.168.20.34:8096"></label>
          <label>API Key<input type="text" id="integ-jf-key" value="${esc(jf.api_key || "")}" placeholder="Jellyfin API key"></label>
          <div class="integ-toggle-row">
            <label class="scraper-toggle"><input type="checkbox" id="integ-jf-auto" ${jf.auto_refresh ? "checked" : ""}><span class="slider"></span></label>
            <span>Auto-refresh after M3U regeneration</span>
          </div>
          <div class="integ-toggle-row">
            <label class="scraper-toggle"><input type="checkbox" id="integ-jf-rebind" ${jf.rebind_mode ? "checked" : ""}><span class="slider"></span></label>
            <span>Force rebind on every refresh (drops stale channel bindings)</span>
          </div>
        </div>
        <p style="font-size:11px;color:var(--text-muted);margin-bottom:12px">Refresh triggers Jellyfin's guide data task. Rebind additionally deletes + re-adds the XMLTV listings provider so Jellyfin rediscovers every channel from scratch.</p>
        <div class="integ-modal-actions">
          <button class="btn-sm" id="integ-jf-save">Save</button>
          <button class="btn-sm" id="integ-jf-test">Test Connection</button>
          <button class="btn-sm" id="integ-jf-refresh">Force Refresh</button>
        </div>
        <div id="integ-jf-result" style="margin-top:12px;font-size:12px"></div>
      </div>`;

    // Save
    $("#integ-jf-save").addEventListener("click", async () => {
      try {
        const r = await fetch(`${API}/integrations/jellyfin/config`, {
          method: "PUT", headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            url: $("#integ-jf-url").value, api_key: $("#integ-jf-key").value,
            auto_refresh: $("#integ-jf-auto").checked, rebind_mode: $("#integ-jf-rebind").checked,
          }),
        });
        if ((await r.json()).ok) { toast("success", "Jellyfin config saved"); loadIntegrations(); }
        else toast("error", "Save failed");
      } catch (e) { toast("error", "Save failed"); }
    });

    // Test
    $("#integ-jf-test").addEventListener("click", async () => {
      const res = $("#integ-jf-result");
      res.textContent = "Testing...";
      await fetch(`${API}/integrations/jellyfin/config`, {
        method: "PUT", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          url: $("#integ-jf-url").value, api_key: $("#integ-jf-key").value,
          auto_refresh: $("#integ-jf-auto").checked,
        }),
      });
      try {
        const r = await fetch(`${API}/integrations/jellyfin/test`, {method: "POST"});
        const d = await r.json();
        if (d.ok) {
          res.innerHTML = `<span style="color:var(--ok)">Connected: ${esc(d.server_name)} v${esc(d.version)}</span>`;
          loadIntegrations();
        } else {
          res.innerHTML = `<span style="color:var(--danger)">${esc(d.error)}</span>`;
        }
      } catch (e) { res.innerHTML = '<span style="color:var(--danger)">Connection failed</span>'; }
    });

    // Refresh
    $("#integ-jf-refresh").addEventListener("click", async () => {
      const res = $("#integ-jf-result");
      const willRebind = $("#integ-jf-rebind").checked;
      // Save first so refresh uses latest rebind_mode
      await fetch(`${API}/integrations/jellyfin/config`, {
        method: "PUT", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          url: $("#integ-jf-url").value, api_key: $("#integ-jf-key").value,
          auto_refresh: $("#integ-jf-auto").checked, rebind_mode: willRebind,
        }),
      });
      res.textContent = willRebind ? "Rebinding provider + refreshing guide..." : "Triggering guide refresh...";
      try {
        const r = await fetch(`${API}/integrations/jellyfin/refresh`, {method: "POST"});
        const d = await r.json();
        if (d.ok) {
          const msg = d.mode === "rebind" ? "Rebind + guide refresh triggered" : "Guide refresh triggered";
          res.innerHTML = `<span style="color:var(--ok)">${msg}</span>`;
        } else res.innerHTML = `<span style="color:var(--danger)">${esc(d.error)}</span>`;
      } catch (e) { res.innerHTML = '<span style="color:var(--danger)">Refresh failed</span>'; }
    });

  } else if (type === "manifold") {
    const mf = _integData.manifold || {};
    modal.innerHTML = `
      <div class="modal-header"><h3>Manifold Integration</h3><button class="btn-close" onclick="document.getElementById('modal-overlay').classList.add('hidden')">&times;</button></div>
      <div class="modal-body" style="padding:16px">
        <div class="integ-modal-fields">
          <label>Manifold URL<input type="text" id="integ-mf-url" value="${esc(mf.url || "")}" placeholder="http://192.168.20.34:40000"></label>
          <label>M3U Source Name in Manifold<input type="text" id="integ-mf-m3u" value="${esc(mf.m3u_source_name || "Channelarr")}" placeholder="Channelarr"></label>
          <label>EPG Source Name in Manifold<input type="text" id="integ-mf-epg" value="${esc(mf.epg_source_name || "Channelarr")}" placeholder="Channelarr"></label>
          <div class="integ-toggle-row">
            <label class="scraper-toggle"><input type="checkbox" id="integ-mf-auto" ${mf.auto_sync ? "checked" : ""}><span class="slider"></span></label>
            <span>Auto-sync manifold after channelarr regenerates</span>
          </div>
        </div>
        <p style="font-size:11px;color:var(--text-muted);margin-bottom:12px">Source names must exactly match the names configured inside manifold's M3U Sources and EPG Sources tables.</p>
        <div class="integ-modal-actions">
          <button class="btn-sm" id="integ-mf-save">Save</button>
          <button class="btn-sm" id="integ-mf-test">Test Connection</button>
          <button class="btn-sm" id="integ-mf-sync">Force Sync</button>
        </div>
        <div id="integ-mf-result" style="margin-top:12px;font-size:12px"></div>
      </div>`;

    function _mfPayload() {
      return {
        url: $("#integ-mf-url").value.trim(),
        m3u_source_name: $("#integ-mf-m3u").value.trim(),
        epg_source_name: $("#integ-mf-epg").value.trim(),
        auto_sync: $("#integ-mf-auto").checked,
      };
    }

    $("#integ-mf-save").addEventListener("click", async () => {
      try {
        const r = await fetch(`${API}/integrations/manifold/config`, {
          method: "PUT", headers: {"Content-Type": "application/json"},
          body: JSON.stringify(_mfPayload()),
        });
        if ((await r.json()).ok) { toast("success", "Manifold config saved"); loadIntegrations(); }
        else toast("error", "Save failed");
      } catch (e) { toast("error", "Save failed"); }
    });

    $("#integ-mf-test").addEventListener("click", async () => {
      const res = $("#integ-mf-result");
      res.textContent = "Testing...";
      await fetch(`${API}/integrations/manifold/config`, {
        method: "PUT", headers: {"Content-Type": "application/json"},
        body: JSON.stringify(_mfPayload()),
      });
      try {
        const r = await fetch(`${API}/integrations/manifold/test`, {method: "POST"});
        const d = await r.json();
        if (d.ok) {
          res.innerHTML = '<span style="color:var(--ok)">Connected to Manifold</span>';
          loadIntegrations();
        } else {
          res.innerHTML = `<span style="color:var(--danger)">${esc(d.error)}</span>`;
        }
      } catch (e) { res.innerHTML = '<span style="color:var(--danger)">Connection failed</span>'; }
    });

    $("#integ-mf-sync").addEventListener("click", async () => {
      const res = $("#integ-mf-result");
      res.textContent = "Syncing channelarr sources in manifold...";
      await fetch(`${API}/integrations/manifold/config`, {
        method: "PUT", headers: {"Content-Type": "application/json"},
        body: JSON.stringify(_mfPayload()),
      });
      try {
        const r = await fetch(`${API}/integrations/manifold/sync`, {method: "POST"});
        const d = await r.json();
        if (d.ok) res.innerHTML = '<span style="color:var(--ok)">Sync triggered — manifold will re-ingest + regenerate</span>';
        else res.innerHTML = `<span style="color:var(--danger)">${esc(d.error)}</span>`;
      } catch (e) { res.innerHTML = '<span style="color:var(--danger)">Sync failed</span>'; }
    });
  }
};

// ─── Branding Logos ───
async function loadBrandingDropdown(selectId, selected) {
  const sel = document.getElementById(selectId);
  if (!sel) return;
  try {
    const r = await fetch(`${API}/branding`);
    const logos = await r.json();
    sel.innerHTML = '<option value="">None</option>';
    for (const logo of logos) {
      const opt = document.createElement("option");
      opt.value = logo.filename;
      opt.textContent = logo.filename.replace(/\.[^.]+$/, "");
      if (logo.filename === selected) opt.selected = true;
      sel.appendChild(opt);
    }
  } catch(e) {}
}

async function loadBrandingGrid() {
  const grid = document.getElementById("branding-grid");
  if (!grid) return;
  try {
    const r = await fetch(`${API}/branding`);
    const logos = await r.json();
    grid.innerHTML = "";
    if (!logos.length) {
      grid.innerHTML = '<div style="color:#666;font-size:13px;">No branding logos uploaded yet.</div>';
      return;
    }
    for (const logo of logos) {
      const card = document.createElement("div");
      card.style.cssText = "position:relative;background:#1a1a2e;border-radius:8px;padding:8px;text-align:center;width:100px;";
      card.innerHTML = `
        <img src="${logo.url}?t=${Date.now()}" style="max-width:80px;max-height:80px;display:block;margin:0 auto 6px;">
        <div style="font-size:11px;color:#aaa;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${logo.filename.replace(/\.[^.]+$/, "")}</div>
        <button onclick="channelarr.deleteBranding('${logo.filename}')" style="position:absolute;top:2px;right:2px;background:none;border:none;color:#f44;cursor:pointer;font-size:14px;">x</button>
      `;
      grid.appendChild(card);
    }
  } catch(e) {}
}

channelarr.deleteBranding = async function(filename) {
  if (!confirm(`Delete branding logo "${filename}"?`)) return;
  try {
    await fetch(`${API}/branding/${filename}`, { method: "DELETE" });
    toast("success", "Branding logo deleted");
    loadBrandingGrid();
  } catch(e) { toast("error", "Delete failed"); }
};

document.getElementById("branding-upload")?.addEventListener("change", async (e) => {
  const file = e.target.files[0];
  if (!file) return;
  const fd = new FormData();
  fd.append("file", file);
  try {
    const r = await fetch(`${API}/branding`, { method: "POST", body: fd });
    if (r.ok) {
      toast("success", "Branding logo uploaded");
      loadBrandingGrid();
    } else {
      const d = await r.json();
      toast("error", d.error || "Upload failed");
    }
  } catch(e) { toast("error", "Upload failed"); }
  e.target.value = "";
});

// ─── Web Player ───
let activeHls = null;
const playerOverlay = $("#player-overlay");
const playerVideo = $("#player-video");

$("#player-close").addEventListener("click", closePlayer);
playerOverlay.addEventListener("click", (e) => {
  if (e.target === playerOverlay) closePlayer();
});

channelarr.watchChannel = function(id, name) {
  // Use the API-provided stream_url so the frontend doesn't have to know
  // which endpoint each channel type maps to. The backend stamps:
  //   - scheduled: /live/{id}/stream.m3u8
  //   - resolved (passthrough): /live-resolved/{manifest_id}.m3u8
  //   - resolved (transcode-mediated): /live/{id}/stream.m3u8 (same as scheduled)
  const ch = (channels || []).find(c => c.id === id);
  let url = ch && ch.stream_url;
  if (!url) {
    if (ch && ch.type === "resolved" && ch.manifest_id) {
      url = `/live-resolved/${ch.manifest_id}.m3u8`;
    } else {
      url = `/live/${id}/stream.m3u8`;
    }
  }
  $("#player-title").textContent = name || "Watch";
  playerOverlay.classList.remove("hidden");

  if (activeHls) { activeHls.destroy(); activeHls = null; }

  if (Hls.isSupported()) {
    const hls = new Hls({
      liveSyncDurationCount: 3,
      liveMaxLatencyDurationCount: 10,
      liveDurationInfinity: true,
      enableWorker: true,
      lowLatencyMode: false,
      backBufferLength: 0,
      maxBufferLength: 30,
      maxMaxBufferLength: 60,
    });
    hls.loadSource(url);
    hls.attachMedia(playerVideo);
    hls.on(Hls.Events.MANIFEST_PARSED, () => { playerVideo.play(); });
    hls.on(Hls.Events.ERROR, (_, data) => {
      if (data.fatal) {
        if (data.type === Hls.ErrorTypes.NETWORK_ERROR) {
          toast("error", "Stream starting — please wait...");
          setTimeout(() => hls.startLoad(), 3000);
        } else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
          hls.recoverMediaError();
        } else {
          toast("error", "Playback error");
          hls.destroy();
        }
      }
    });
    activeHls = hls;
  } else if (playerVideo.canPlayType("application/vnd.apple.mpegurl")) {
    playerVideo.src = url;
    playerVideo.play();
  } else {
    toast("error", "HLS not supported in this browser");
  }
};

function closePlayer() {
  playerOverlay.classList.add("hidden");
  if (activeHls) { activeHls.destroy(); activeHls = null; }
  playerVideo.pause();
  playerVideo.removeAttribute("src");
  playerVideo.load();
}

// ─── Helpers ───
function esc(s) {
  if (!s) return "";
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

// ─── System Stats ───
let statsData = null;
let chartModalType = "";
let chartModalRange = "1h";

async function updateSystemStats() {
  try {
    const r = await fetch(`${API}/system/stats`);
    statsData = await r.json();
    renderStatGauges();
    renderLineChart("cpu-chart", statsData.history.timestamps, statsData.history.cpu, {color:"var(--accent)", label:"CPU"});
    renderLineChart("ram-chart", statsData.history.timestamps, statsData.history.ram, {color:"var(--accent)", label:"RAM"});
    renderDiskGauge();
    renderYTCacheChart();
  } catch(e) {}
  loadVpnChart();
}

function renderStatGauges() {
  if (!statsData) return;
  const c = statsData.current;
  $("#cpu-live").textContent = `${Math.round(c.cpu_percent)}%`;
  $("#ram-live").textContent = `${Math.round(c.ram_percent)}%`;
  if (c.disk) {
    const used = fmtBytes(c.disk.used);
    const total = fmtBytes(c.disk.total);
    $("#disk-live").textContent = `${used} / ${total}`;
  } else {
    $("#disk-live").textContent = "--";
  }
  $("#yt-cache-live").textContent = c.yt_cache_bytes != null ? fmtBytes(c.yt_cache_bytes) : "--";
}

function renderDiskGauge() {
  const container = $("#disk-chart");
  if (!statsData || !statsData.current.disk) {
    container.innerHTML = '<div class="disk-bar-wrap"><div class="disk-pct">N/A</div></div>';
    return;
  }
  const d = statsData.current.disk;
  const pct = d.percent.toFixed(1);
  const color = d.percent > 90 ? "var(--danger)" : d.percent > 75 ? "var(--warn)" : "var(--ok)";
  container.innerHTML = `
    <div class="disk-bar-wrap">
      <div class="disk-pct" style="color:${color}">${pct}% used</div>
      <div class="disk-bar">
        <div class="disk-bar-fill" style="width:${pct}%;background:${color}"></div>
      </div>
      <div class="disk-info">
        <span>Free: ${fmtBytes(d.free)}</span>
        <span>Total: ${fmtBytes(d.total)}</span>
      </div>
    </div>`;
}

function renderYTCacheChart() {
  const container = $("#yt-cache-chart");
  if (!statsData || !statsData.history.yt_cache) {
    container.innerHTML = '<div class="empty-state">Collecting data...</div>';
    return;
  }
  const timestamps = statsData.history.timestamps;
  const values = statsData.history.yt_cache;
  if (!values.length) {
    container.innerHTML = '<div class="empty-state">Collecting data...</div>';
    return;
  }
  const maxBytes = Math.max(...values, 1);
  const pctValues = values.map(v => (v / maxBytes) * 100);
  const maxLabel = fmtBytes(maxBytes);
  renderLineChart(container.id, timestamps, pctValues, {
    color: "var(--danger)", label: "YT Cache", maxLabel,
    formatTip: (val, idx) => `${fmtBytes(values[idx])}`
  });
}

function renderLineChart(containerId, timestamps, values, opts) {
  const container = $(`#${containerId}`);
  if (!timestamps || !timestamps.length) {
    container.innerHTML = '<div class="empty-state">Collecting data...</div>';
    return;
  }

  const W = 600, H = 160;
  const pad = {l:35, r:10, t:10, b:22};
  const cw = W - pad.l - pad.r;
  const ch = H - pad.t - pad.b;
  const color = opts.color || "var(--accent)";
  const maxVal = 100;

  let svg = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:100%">`;
  for (let g = 0; g <= 4; g++) {
    const y = pad.t + (ch * (1 - g / 4));
    svg += `<line x1="${pad.l}" y1="${y}" x2="${W - pad.r}" y2="${y}" class="chart-grid"/>`;
    svg += `<text x="${pad.l - 4}" y="${y + 3}" class="chart-label" text-anchor="end">${g * 25}%</text>`;
  }

  const n = values.length;
  const pts = values.map((v, i) => {
    const x = pad.l + (i / Math.max(1, n - 1)) * cw;
    const y = pad.t + ch * (1 - Math.min(v, maxVal) / maxVal);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  const firstX = pad.l;
  const lastX = (pad.l + ((n - 1) / Math.max(1, n - 1)) * cw).toFixed(1);
  const bottom = (pad.t + ch).toFixed(1);
  svg += `<polygon class="chart-area" fill="${color}" points="${firstX},${bottom} ${pts.join(" ")} ${lastX},${bottom}"/>`;
  svg += `<polyline class="chart-line" stroke="${color}" points="${pts.join(" ")}"/>`;

  const labelCount = Math.min(6, n);
  for (let li = 0; li < labelCount; li++) {
    const idx = Math.round(li * (n - 1) / Math.max(1, labelCount - 1));
    const x = pad.l + (idx / Math.max(1, n - 1)) * cw;
    const d = new Date(timestamps[idx] * 1000);
    const label = `${d.getHours().toString().padStart(2,"0")}:${d.getMinutes().toString().padStart(2,"0")}`;
    svg += `<text x="${x.toFixed(1)}" y="${H - 4}" class="chart-label" text-anchor="middle">${label}</text>`;
  }

  svg += "</svg>";
  svg += `<div class="chart-tooltip" style="display:none" id="${containerId}-tip"></div>`;
  container.innerHTML = svg;

  const svgEl = container.querySelector("svg");
  const tip = container.querySelector(`#${containerId}-tip`);
  svgEl.addEventListener("mousemove", (e) => {
    const rect = svgEl.getBoundingClientRect();
    const frac = (e.clientX - rect.left) / rect.width;
    const adjFrac = (frac * W - pad.l) / cw;
    const idx = Math.max(0, Math.min(n - 1, Math.round(adjFrac * (n - 1))));
    const val = values[idx];
    const d = new Date(timestamps[idx] * 1000);
    const time = `${d.getHours().toString().padStart(2,"0")}:${d.getMinutes().toString().padStart(2,"0")}`;
    tip.textContent = opts.formatTip ? `${opts.formatTip(val, idx)} at ${time}` : `${opts.label}: ${val.toFixed(1)}% at ${time}`;
    tip.style.display = "block";
    tip.style.left = `${e.clientX - rect.left + 12}px`;
    tip.style.top = `${e.clientY - rect.top - 30}px`;
  });
  svgEl.addEventListener("mouseleave", () => { tip.style.display = "none"; });
}

// Chart modal
["cpu-card", "ram-card"].forEach(id => {
  $(`#${id}`).addEventListener("click", () => {
    chartModalType = id.replace("-card", "");
    chartModalRange = "1h";
    openChartModal();
  });
});

function openChartModal() {
  const overlay = $("#chart-modal-overlay");
  $("#chart-modal-title").textContent = chartModalType === "cpu" ? "CPU Usage" : "Memory Usage";
  const ranges = $("#chart-modal-ranges");
  ranges.innerHTML = ["1h","2h","6h","12h","24h"].map(r =>
    `<button class="${r === chartModalRange ? "active" : ""}" data-range="${r}">${r}</button>`
  ).join("");
  ranges.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", () => {
      chartModalRange = btn.dataset.range;
      ranges.querySelectorAll("button").forEach(b => b.classList.toggle("active", b === btn));
      renderModalChart();
    });
  });
  overlay.classList.remove("hidden");
  renderModalChart();
}

$("#chart-modal-close").addEventListener("click", () => { $("#chart-modal-overlay").classList.add("hidden"); });
$("#chart-modal-overlay").addEventListener("click", (e) => {
  if (e.target === $("#chart-modal-overlay")) $("#chart-modal-overlay").classList.add("hidden");
});

function renderModalChart() {
  if (!statsData) return;
  const h = statsData.history;
  const rangeSeconds = {"1h":3600,"2h":7200,"6h":21600,"12h":43200,"24h":86400}[chartModalRange] || 3600;
  const now = Date.now() / 1000;
  const cutoff = now - rangeSeconds;
  const idxStart = h.timestamps.findIndex(t => t >= cutoff);
  if (idxStart < 0) return;

  const ts = h.timestamps.slice(idxStart);
  const vals = chartModalType === "cpu" ? h.cpu.slice(idxStart) : h.ram.slice(idxStart);
  const color = chartModalType === "cpu" ? "var(--accent)" : "var(--accent)";
  const label = chartModalType === "cpu" ? "CPU" : "RAM";

  const container = $("#chart-modal-chart");
  const W = 900, H = 380;
  const pad = {l:40, r:10, t:10, b:28};
  const cw = W - pad.l - pad.r;
  const ch = H - pad.t - pad.b;
  const n = vals.length;

  let svg = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:100%">`;
  for (let g = 0; g <= 4; g++) {
    const y = pad.t + (ch * (1 - g / 4));
    svg += `<line x1="${pad.l}" y1="${y}" x2="${W - pad.r}" y2="${y}" class="chart-grid"/>`;
    svg += `<text x="${pad.l - 4}" y="${y + 3}" class="chart-label" text-anchor="end">${g * 25}%</text>`;
  }

  const pts = vals.map((v, i) => {
    const x = pad.l + (i / Math.max(1, n - 1)) * cw;
    const y = pad.t + ch * (1 - Math.min(v, 100) / 100);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const firstX = pad.l;
  const lastX = (pad.l + ((n - 1) / Math.max(1, n - 1)) * cw).toFixed(1);
  const bottom = (pad.t + ch).toFixed(1);
  svg += `<polygon class="chart-area" fill="${color}" points="${firstX},${bottom} ${pts.join(" ")} ${lastX},${bottom}"/>`;
  svg += `<polyline class="chart-line" stroke="${color}" points="${pts.join(" ")}"/>`;

  const labelCount = Math.min(8, n);
  for (let li = 0; li < labelCount; li++) {
    const idx = Math.round(li * (n - 1) / Math.max(1, labelCount - 1));
    const x = pad.l + (idx / Math.max(1, n - 1)) * cw;
    const d = new Date(ts[idx] * 1000);
    const lbl = `${d.getHours().toString().padStart(2,"0")}:${d.getMinutes().toString().padStart(2,"0")}`;
    svg += `<text x="${x.toFixed(1)}" y="${H - 4}" class="chart-label" text-anchor="middle">${lbl}</text>`;
  }
  svg += "</svg>";
  svg += `<div class="chart-tooltip" style="display:none" id="modal-chart-tip"></div>`;
  container.innerHTML = svg;

  const svgEl = container.querySelector("svg");
  const tip = container.querySelector("#modal-chart-tip");
  svgEl.addEventListener("mousemove", (e) => {
    const rect = svgEl.getBoundingClientRect();
    const frac = (e.clientX - rect.left) / rect.width;
    const adjFrac = (frac * W - pad.l) / cw;
    const idx = Math.max(0, Math.min(n - 1, Math.round(adjFrac * (n - 1))));
    const val = vals[idx];
    const d = new Date(ts[idx] * 1000);
    const time = `${d.getHours().toString().padStart(2,"0")}:${d.getMinutes().toString().padStart(2,"0")}`;
    tip.textContent = `${label}: ${val.toFixed(1)}% at ${time}`;
    tip.style.display = "block";
    tip.style.left = `${e.clientX - rect.left + 12}px`;
    tip.style.top = `${e.clientY - rect.top - 30}px`;
  });
  svgEl.addEventListener("mouseleave", () => { tip.style.display = "none"; });
}

function fmtBytes(b) {
  if (b >= 1e12) return (b / 1e12).toFixed(1) + " TB";
  if (b >= 1e9) return (b / 1e9).toFixed(1) + " GB";
  if (b >= 1e6) return (b / 1e6).toFixed(1) + " MB";
  return (b / 1e3).toFixed(0) + " KB";
}

// ─── Polling Loop ───
async function tick() {
  await updateStatus();
  if ($("#view-logs").classList.contains("visible")) {
    await pollLogs();
  }
  if ($("#view-system").classList.contains("visible")) {
    await updateSystemStats();
  }
}

// ─── Resolver ─────────────────────────────────────────────────────────────
let resolverTimer = null;
let resolverHls = null;

async function loadResolver() {
  // Selenium sidecar status
  try {
    const r = await fetch(`${API}/resolve/selenium-status`);
    const s = await r.json();
    const badge = $("#selenium-badge");
    if (s.ready) {
      badge.textContent = "Selenium: Ready";
      badge.className = "badge selenium-ready";
    } else {
      badge.textContent = "Selenium: Offline";
      badge.className = "badge selenium-offline";
    }
  } catch (e) {
    const badge = $("#selenium-badge");
    badge.textContent = "Selenium: Offline";
    badge.className = "badge selenium-offline";
  }
  // Always load persisted channels from DB first (survives restarts)
  let persisted = [];
  try {
    const r = await fetch(`${API}/resolve/channels`);
    const j = await r.json();
    persisted = j.results || [];
  } catch (e) {}

  // Layer any in-flight batch state on top of persisted manifests
  try {
    const r = await fetch(`${API}/resolve/batch/status`);
    const b = await r.json();
    if (b.running || (b.results && b.results.some(x => x.status === "resolving" || x.status === "pending"))) {
      // Merge: batch items first, then persisted items not already in the batch
      const batchUrls = new Set((b.results || []).map(x => x.url));
      const merged = [...(b.results || []), ...persisted.filter(p => !batchUrls.has(p.url))];
      renderResolverQueue({ ...b, results: merged });
      if (b.running) startResolverPolling();
    } else {
      renderResolverQueue({ running: false, results: persisted });
    }
  } catch (e) {
    renderResolverQueue({ running: false, results: persisted });
  }
}

function renderResolverQueue(batch) {
  const body = $("#resolver-queue-body");
  const prog = $("#resolver-progress");
  const goBtn = $("#resolver-go");

  if (!batch.results || batch.results.length === 0) {
    body.innerHTML = '<tr><td colspan="4" class="empty-state">No manifests captured yet. Paste one or more URLs above and click "Resolve All" to add your first manifest to the library.</td></tr>';
    prog.classList.add("hidden");
    goBtn.disabled = false;
    return;
  }

  if (batch.running) {
    prog.classList.remove("hidden");
    const pct = batch.total > 0 ? Math.round((batch.completed / batch.total) * 100) : 0;
    $("#resolver-progress-fill").style.width = pct + "%";
    $("#resolver-progress-label").textContent = `Resolving ${batch.completed + 1} of ${batch.total}...`;
    goBtn.disabled = true;
  } else {
    const done = batch.results.filter(r => r.status === "done").length;
    const failed = batch.results.filter(r => r.status === "failed").length;
    if (done > 0 || failed > 0) {
      prog.classList.remove("hidden");
      $("#resolver-progress-fill").style.width = "100%";
      $("#resolver-progress-label").textContent = `Complete: ${done} resolved, ${failed} failed`;
    } else {
      prog.classList.add("hidden");
    }
    goBtn.disabled = false;
  }

  body.innerHTML = batch.results.map((r, i) => {
    const statusClass = `res-status-${r.status}`;
    const statusTitle = r.status === "failed" ? (r.error || "Failed") : r.status;
    const title = r.title || "(untitled)";
    const url = r.url || "";
    const truncUrl = url.length > 80 ? url.substring(0, 80) + "..." : url;

    let expiryHint = "";
    if (r.status === "done" && r.expires_at) {
      const exp = new Date(r.expires_at).getTime();
      const mins = Math.round((exp - Date.now()) / 60000);
      if (mins > 5) expiryHint = `<span class="res-expiry" title="Auto-refresh scheduled">refreshes in ${mins - 5}m</span>`;
      else if (mins > 0) expiryHint = `<span class="res-expiry res-expiry-soon" title="Refresh due">refreshing soon</span>`;
      else expiryHint = `<span class="res-expiry res-expiry-stale" title="Token expired">refresh overdue</span>`;
    } else if (r.status === "done") {
      expiryHint = `<span class="res-expiry res-expiry-unknown" title="Unknown expiry — will auto-refresh on failure">token unknown</span>`;
    }

    // Channel reference badge: how many Channel rows point at this manifest
    let channelBadge = "";
    if (r.status === "done" && typeof r.channel_count === "number") {
      if (r.channel_count > 0) {
        const names = (r.channels || []).map(c => c.name).join(", ");
        channelBadge = `<span class="res-channel-count" title="${esc(names)}">in ${r.channel_count} channel${r.channel_count !== 1 ? "s" : ""}</span>`;
      } else {
        channelBadge = `<span class="res-channel-count res-channel-count-zero" title="Not yet used as a channel">unused</span>`;
      }
    }

    // Persisted (done) rows get a DB delete; failed/pending rows just get a DOM remove
    let removeBtn;
    if (r.status === "done" && r.manifest_id) {
      const delTitle = r.channel_count > 0
        ? `Delete library entry — also removes ${r.channel_count} channel${r.channel_count !== 1 ? "s" : ""}`
        : "Delete library entry";
      removeBtn = `<button class="btn btn-sm-danger" data-res-delete="${r.manifest_id}" data-res-delete-title="${title}" data-res-delete-count="${r.channel_count || 0}" title="${delTitle}">&times;</button>`;
    } else {
      removeBtn = `<button class="btn btn-sm-danger" data-res-remove="${i}" title="Remove from queue">&times;</button>`;
    }
    let actions = removeBtn;
    if (r.status === "done" && r.manifest_id) {
      const createBtn = `<button class="btn btn-sm" data-res-create="${r.manifest_id}" data-res-create-title="${title}" title="Create a channel from this manifest">+ Channel</button>`;
      actions = `<button class="btn btn-sm-watch" data-res-play="${r.manifest_id}" data-res-play-title="${title}" data-res-play-url="${url}" title="Test Stream">&#9654;</button> <button class="btn btn-sm" data-res-refresh="${r.manifest_id}" title="Refresh token now">&#8635;</button> ${createBtn} ` + actions;
    }
    if (r.status === "failed") {
      actions = `<button class="btn btn-sm" data-res-retry="${i}" title="Retry">&#8635;</button> ` + actions;
    }

    return `<tr>
      <td><span class="res-status ${statusClass}" title="${statusTitle}"></span></td>
      <td class="res-title">${title}${expiryHint ? " " + expiryHint : ""}${channelBadge ? " " + channelBadge : ""}</td>
      <td class="res-url" title="${url}">${truncUrl}</td>
      <td class="res-actions" style="text-align:right">${actions}</td>
    </tr>`;
  }).join("");

  body.querySelectorAll("[data-res-play]").forEach(btn => {
    btn.addEventListener("click", () => resolverPlay(btn.dataset.resPlay, btn.dataset.resPlayTitle, btn.dataset.resPlayUrl));
  });
  body.querySelectorAll("[data-res-retry]").forEach(btn => {
    btn.addEventListener("click", async () => {
      await fetch(`${API}/resolve/retry/${btn.dataset.resRetry}`, { method: "POST" });
      startResolverPolling();
    });
  });
  body.querySelectorAll("[data-res-refresh]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const mid = btn.dataset.resRefresh;
      btn.disabled = true;
      try {
        const r = await fetch(`${API}/resolve/refresh/${mid}`, { method: "POST" });
        const j = await r.json();
        if (j.ok) toast("info", "Refresh queued");
        else toast("error", j.error || "Refresh failed");
      } catch (e) { toast("error", "Refresh failed"); }
      setTimeout(() => { btn.disabled = false; loadResolver(); }, 3000);
    });
  });
  body.querySelectorAll("[data-res-remove]").forEach(btn => {
    btn.addEventListener("click", () => { btn.closest("tr").remove(); });
  });
  body.querySelectorAll("[data-res-delete]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const mid = btn.dataset.resDelete;
      const title = btn.dataset.resDeleteTitle || "library entry";
      const count = parseInt(btn.dataset.resDeleteCount || "0");
      let prompt = `Delete "${title}" from the library?`;
      if (count > 0) {
        prompt += `\n\nThis will also remove ${count} channel${count !== 1 ? "s" : ""} that reference this manifest.`;
      }
      if (!confirm(prompt)) return;
      btn.disabled = true;
      try {
        const r = await fetch(`${API}/resolve/channels/${mid}`, { method: "DELETE" });
        const j = await r.json();
        if (r.ok && j.ok) {
          toast("success", `Deleted "${title}"`);
          btn.closest("tr").remove();
          setTimeout(() => { loadResolver(); loadChannels(); }, 500);
        } else {
          toast("error", j.error || "Delete failed");
          btn.disabled = false;
        }
      } catch (e) {
        toast("error", "Delete failed");
        btn.disabled = false;
      }
    });
  });
  body.querySelectorAll("[data-res-create]").forEach(btn => {
    btn.addEventListener("click", () => {
      const mid = btn.dataset.resCreate;
      // Look up the full manifest record from the rendered batch results
      const m = (batch.results || []).find(r => r.manifest_id === mid);
      if (!m) { toast("error", "Manifest not found"); return; }
      openCreateResolved({
        manifest_id: m.manifest_id,
        title: m.title,
        manifest_url: m.manifest_url || m.url,
      });
    });
  });
}

function startResolverPolling() {
  clearInterval(resolverTimer);
  resolverTimer = setInterval(async () => {
    try {
      const [batchRes, persistRes] = await Promise.all([
        fetch(`${API}/resolve/batch/status`),
        fetch(`${API}/resolve/channels`),
      ]);
      const b = await batchRes.json();
      const pj = await persistRes.json();
      const persisted = pj.results || [];

      // Merge batch results with persisted manifests
      const batchUrls = new Set((b.results || []).map(x => x.url));
      const merged = [...(b.results || []), ...persisted.filter(p => !batchUrls.has(p.url))];
      renderResolverQueue({ ...b, results: merged });

      if (!b.running) {
        clearInterval(resolverTimer);
        resolverTimer = null;
        const doneCount = (b.results || []).filter(x => x.status === "done").length;
        toast("success", `Batch complete: ${doneCount} resolved`);
        setTimeout(() => loadResolver(), 500);
      }
    } catch (e) {
      clearInterval(resolverTimer);
      resolverTimer = null;
    }
  }, 2000);
}

function resolverPlay(manifestId, title, pageUrl) {
  const player = $("#resolver-player");
  const video = $("#resolver-video");
  player.classList.remove("hidden");
  $("#resolver-player-title").textContent = title || "Test Stream";
  $("#resolver-player-url").textContent = pageUrl || "";

  const url = `/live-resolved/${manifestId}.m3u8`;
  if (resolverHls) { resolverHls.destroy(); resolverHls = null; }

  if (typeof Hls !== "undefined" && Hls.isSupported()) {
    const hls = new Hls({
      liveSyncDurationCount: 3,
      liveMaxLatencyDurationCount: 10,
      liveDurationInfinity: true,
      enableWorker: true,
      maxBufferLength: 30,
    });
    hls.loadSource(url);
    hls.attachMedia(video);
    hls.on(Hls.Events.MANIFEST_PARSED, () => video.play().catch(() => {}));
    hls.on(Hls.Events.ERROR, (ev, data) => {
      if (data.fatal) toast("error", "Stream playback error");
    });
    resolverHls = hls;
  } else if (video.canPlayType("application/vnd.apple.mpegurl")) {
    video.src = url;
    video.play().catch(() => {});
  } else {
    toast("error", "HLS not supported in this browser");
  }
}

function closeResolverPlayer() {
  $("#resolver-player").classList.add("hidden");
  if (resolverHls) { resolverHls.destroy(); resolverHls = null; }
  const video = $("#resolver-video");
  video.pause();
  video.removeAttribute("src");
  video.load();
}

// Wire resolver buttons (outside SPA switching loop so bound once)
document.addEventListener("DOMContentLoaded", () => {
  const goBtn = $("#resolver-go");
  if (goBtn) {
    goBtn.addEventListener("click", async () => {
      const raw = $("#resolver-urls").value.trim();
      if (!raw) { toast("error", "Enter at least one URL"); return; }
      let urls = [];
      const title = $("#resolver-title").value.trim();
      const timeout = parseInt($("#resolver-timeout").value);
      const rTags = ($("#resolver-tags").value || "").split(",").map(s => s.trim()).filter(Boolean);
      const rEvStart = $("#resolver-event-start").value;
      const rEvEnd = $("#resolver-event-end").value;
      const autoCreate = $("#resolver-auto-create").checked;
      const entryMeta = {};
      if (rTags.length) entryMeta.tags = rTags;
      if (rEvStart) entryMeta.event_start = new Date(rEvStart).toISOString();
      if (rEvEnd) entryMeta.event_end = new Date(rEvEnd).toISOString();
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) urls = parsed.map(e => typeof e === "string" ? {url: e, ...entryMeta} : {...entryMeta, ...e});
      } catch (e) {
        urls = raw.split("\n").map(l => l.trim()).filter(l => l && l.startsWith("http")).map(u => ({url: u, title: title || null, ...entryMeta}));
      }
      if (urls.length === 0) { toast("error", "No valid URLs found"); return; }
      try {
        const r = await fetch(`${API}/resolve/batch`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ urls, timeout, auto_create: autoCreate }),
        });
        const j = await r.json();
        if (j.ok) {
          toast("info", `Resolving ${urls.length} URL${urls.length > 1 ? "s" : ""}...`);
          $("#resolver-urls").value = "";
          $("#resolver-title").value = "";
          $("#resolver-tags").value = "";
          $("#resolver-event-start").value = "";
          $("#resolver-event-end").value = "";
          startResolverPolling();
          setTimeout(() => loadResolver(), 500);
        } else {
          toast("error", j.error || "Failed to start batch");
        }
      } catch (e) { toast("error", "Failed to start resolve"); }
    });
  }
  const clearBtn = $("#resolver-clear");
  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      $("#resolver-queue-body").innerHTML = "";
      $("#resolver-progress").classList.add("hidden");
      $("#resolver-urls").value = "";
      $("#resolver-title").value = "";
      $("#resolver-tags").value = "";
      $("#resolver-event-start").value = "";
      $("#resolver-event-end").value = "";
      closeResolverPlayer();
    });
  }
  const closeBtn = $("#resolver-player-close");
  if (closeBtn) closeBtn.addEventListener("click", closeResolverPlayer);
});

// ─── VPN ───
let vpnHistory = [];
const VPN_MAX_POINTS = 60;

async function loadVpnStatus() {
  try {
    const r = await fetch(`${API}/vpn/status`);
    const d = await r.json();
    const badge = $("#vpn-badge");
    if (!d.enabled) {
      badge.classList.add("hidden");
      return;
    }
    badge.classList.remove("hidden");
    badge.classList.remove("vpn-connected", "vpn-disconnected", "vpn-unconfigured");
    if (d.status === "running") {
      badge.classList.add("vpn-connected");
      const loc = [d.city, d.country].filter(Boolean).join(", ");
      badge.textContent = `VPN: ${d.ip}${loc ? " (" + loc + ")" : ""}`;
      badge.title = `VPN Connected — ${d.ip} ${loc}`;
    } else if (d.status === "not configured") {
      badge.classList.add("vpn-unconfigured");
      badge.textContent = "VPN: offline";
      badge.title = "Gluetun unreachable";
    } else {
      badge.classList.add("vpn-disconnected");
      badge.textContent = `VPN: ${d.status}`;
      badge.title = `VPN ${d.status}`;
    }
  } catch(e) {
    const badge = $("#vpn-badge");
    badge.classList.add("hidden");
  }
}

async function loadVpnChart() {
  try {
    const r = await fetch(`${API}/vpn/history?minutes=60`);
    const v = await r.json();
    const series = (v.samples || [])
      .map(x => x.rtt_ms)
      .filter(x => x !== null && x !== undefined);
    vpnHistory.length = 0;
    vpnHistory.push(...series.slice(-VPN_MAX_POINTS));
    const sum = v.summary || {};
    const mode = sum.mode || "vpn";
    const isVpn = mode === "vpn";

    $("#vpn-card-title").textContent = isVpn ? "VPN Latency" : "Network Latency";
    $("#vpn-live").textContent = sum.current_rtt_ms != null ? sum.current_rtt_ms.toFixed(1) + "ms" : "--ms";
    if (isVpn) {
      $("#vpn-exit").textContent = sum.current_city ? `${sum.current_city} \u00b7 ${sum.current_ip || ""}` : "--";
    } else {
      $("#vpn-exit").textContent = "Direct connection";
    }
    $("#vpn-rotate-btn").style.display = isVpn ? "" : "none";
    $("#vpn-history-section").style.display = isVpn ? "" : "none";

    // Auto-scale Y to ~120% of observed max (min 100ms)
    if (vpnHistory.length) {
      const observed = Math.max(...vpnHistory);
      const maxMs = Math.max(observed * 1.2, 100);
      renderVpnChart(vpnHistory, maxMs);
    } else {
      const container = $("#vpn-chart");
      container.innerHTML = '<div class="empty-state">Collecting data...</div>';
    }

    if (isVpn) loadVpnServers();
  } catch(e) {}
}

function renderVpnChart(values, maxVal) {
  const container = $("#vpn-chart");
  const W = 600, H = 170;
  const pad = {l:40, r:10, t:10, b:22};
  const cw = W - pad.l - pad.r;
  const ch = H - pad.t - pad.b;
  const n = values.length;
  const color = "var(--ok)";

  let svg = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:100%">`;
  const steps = 4;
  for (let g = 0; g <= steps; g++) {
    const y = pad.t + (ch * (1 - g / steps));
    const label = Math.round(maxVal * g / steps);
    svg += `<line x1="${pad.l}" y1="${y}" x2="${W - pad.r}" y2="${y}" class="chart-grid"/>`;
    svg += `<text x="${pad.l - 4}" y="${y + 3}" class="chart-label" text-anchor="end">${label}ms</text>`;
  }

  const pts = values.map((v, i) => {
    const x = pad.l + (i / Math.max(1, n - 1)) * cw;
    const y = pad.t + ch * (1 - Math.min(v, maxVal) / maxVal);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  const firstX = pad.l;
  const lastX = (pad.l + ((n - 1) / Math.max(1, n - 1)) * cw).toFixed(1);
  const bottom = (pad.t + ch).toFixed(1);
  svg += `<polygon class="chart-area" fill="${color}" points="${firstX},${bottom} ${pts.join(" ")} ${lastX},${bottom}"/>`;
  svg += `<polyline class="chart-line" stroke="${color}" points="${pts.join(" ")}"/>`;
  svg += "</svg>";

  svg += `<div class="chart-tooltip" style="display:none" id="vpn-chart-tip"></div>`;
  container.innerHTML = svg;

  const svgEl = container.querySelector("svg");
  const tip = container.querySelector("#vpn-chart-tip");
  svgEl.addEventListener("mousemove", (e) => {
    const rect = svgEl.getBoundingClientRect();
    const frac = (e.clientX - rect.left) / rect.width;
    const adjFrac = (frac * W - pad.l) / cw;
    const idx = Math.max(0, Math.min(n - 1, Math.round(adjFrac * (n - 1))));
    tip.textContent = `${values[idx].toFixed(1)}ms`;
    tip.style.display = "block";
    tip.style.left = `${e.clientX - rect.left + 12}px`;
    tip.style.top = `${e.clientY - rect.top - 30}px`;
  });
  svgEl.addEventListener("mouseleave", () => { tip.style.display = "none"; });
}

function vpnFmtDuration(seconds) {
  if (!seconds || seconds < 60) return (seconds || 0) + "s";
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (h > 24) {
    const d = Math.floor(h / 24);
    return `${d}d ${h % 24}h`;
  }
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

function vpnFmtRelative(iso) {
  if (!iso) return "--";
  const then = new Date(iso).getTime();
  const sec = Math.floor((Date.now() - then) / 1000);
  if (sec < 60) return sec + "s ago";
  if (sec < 3600) return Math.floor(sec / 60) + "m ago";
  if (sec < 86400) return Math.floor(sec / 3600) + "h ago";
  return Math.floor(sec / 86400) + "d ago";
}

async function loadVpnServers(sort) {
  sort = sort || ($("#vpn-history-sort") ? $("#vpn-history-sort").value : "avg_rtt");
  try {
    const r = await fetch(`${API}/vpn/servers?sort=${sort}&limit=100`);
    const data = await r.json();
    const servers = data.servers || [];
    $("#vpn-history-count").textContent = `${servers.length} server${servers.length === 1 ? "" : "s"}`;
    const body = $("#vpn-history-body");
    if (servers.length === 0) {
      body.innerHTML = `<tr><td colspan="10" style="text-align:center;color:var(--text-muted);padding:24px">No server history yet — samples accumulate every minute.</td></tr>`;
      return;
    }
    body.innerHTML = servers.map((s, i) => {
      const cls = [];
      if (s.is_current) cls.push("is-current");
      if (sort === "avg_rtt" && i < 3 && s.avg_rtt_ms != null) cls.push("top-rank");
      const rankMarker = s.is_current
        ? '<span class="rank-marker rank-current" title="Current">\u25cf</span>'
        : (sort === "avg_rtt" && i < 3 && s.avg_rtt_ms != null)
          ? `<span class="rank-marker rank-gold" title="Top ${i+1}">${["\u2605","\u2461","\u2462"][i]}</span>`
          : '<span class="rank-marker"></span>';
      return `<tr class="${cls.join(" ")}">
        <td>${rankMarker}${esc(s.city || "?")}${s.country ? ", " + esc(s.country) : ""}</td>
        <td class="ip-cell">${esc(s.ip)}</td>
        <td>${esc(s.org || "--")}</td>
        <td class="num">${s.avg_rtt_ms != null ? s.avg_rtt_ms.toFixed(1) : "--"}</td>
        <td class="num">${s.min_rtt_ms != null ? s.min_rtt_ms.toFixed(1) : "--"}</td>
        <td class="num">${s.max_rtt_ms != null ? s.max_rtt_ms.toFixed(1) : "--"}</td>
        <td class="num">${(s.success_rate * 100).toFixed(1)}%</td>
        <td class="num">${s.total_samples}</td>
        <td class="num">${vpnFmtDuration(s.total_seconds_connected)}</td>
        <td>${vpnFmtRelative(s.last_seen_at)}</td>
      </tr>`;
    }).join("");
  } catch(e) {
    $("#vpn-history-body").innerHTML = `<tr><td colspan="10" style="color:var(--danger);padding:24px">Failed to load: ${e}</td></tr>`;
  }
}

// VPN history sort change
(function() {
  const histSort = document.getElementById("vpn-history-sort");
  if (histSort) histSort.addEventListener("change", () => loadVpnServers());
})();

// VPN rotate button
(function() {
  const btn = document.getElementById("vpn-rotate-btn");
  if (!btn) return;
  btn.addEventListener("click", async () => {
    if (!confirm("Rotate VPN — picks a new server from your SERVER_CITIES list. Brief connectivity blip while the tunnel reconnects.")) return;
    btn.disabled = true;
    const orig = btn.innerHTML;
    btn.textContent = "Rotating...";
    try {
      const r = await fetch(`${API}/vpn/rotate`, {method: "POST"});
      const j = await r.json();
      if (j.ok) {
        toast("success", `Rotated to ${j.to.city || j.to.ip || "new exit"}`);
        loadVpnChart();
        loadVpnStatus();
      } else {
        toast("error", j.error || "Rotate failed");
      }
    } catch(e) {
      toast("error", "Rotate request failed");
    } finally {
      btn.disabled = false;
      btn.innerHTML = orig;
    }
  });
})();

// Initial
updateStatus();
loadChannels();
loadSettings();

setInterval(tick, 3000);
setInterval(() => {
  if ($("#view-channels").classList.contains("visible")) loadChannels();
  if ($("#view-guide").classList.contains("visible")) loadGuide();
}, 10000);

// ─── Scrapers ───
let scraperTimer = null;
let scraperSourceOpen = {};  // name -> bool

function _timeAgo(iso) {
  if (!iso) return "--";
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (diff < 0) return _timeUntil(iso);
  if (diff < 60) return `${Math.round(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function _timeUntil(iso) {
  if (!iso) return "--";
  const diff = (new Date(iso).getTime() - Date.now()) / 1000;
  if (diff < 0) return _timeAgo(iso);
  if (diff < 60) return `in ${Math.round(diff)}s`;
  if (diff < 3600) return `in ${Math.floor(diff / 60)}m`;
  if (diff < 86400) return `in ${Math.floor(diff / 3600)}h`;
  return `in ${Math.floor(diff / 86400)}d`;
}

async function loadScrapers() {
  try {
    const [statusR, summaryR] = await Promise.all([
      fetch(`${API}/scrapers/status`),
      fetch(`${API}/scraped-events/summary`),
    ]);
    const data = await statusR.json();
    const summary = summaryR.ok ? await summaryR.json() : {scrapers: {}};
    renderScraperCards(data, summary);
    const anyRunning = Object.values(data.scrapers || {}).some(s => s.running);
    if (anyRunning && !scraperTimer) {
      scraperTimer = setInterval(loadScrapers, 3000);
    } else if (!anyRunning && scraperTimer) {
      clearInterval(scraperTimer);
      scraperTimer = null;
    }
  } catch (e) {
    toast("error", "Failed to load scrapers");
  }
}

function renderScraperCards(data, summary) {
  const grid = $("#scraper-grid");
  const entries = Object.entries(data.scrapers || {});
  const badge = $("#scraper-count");
  if (badge) badge.textContent = `${entries.length} plugin${entries.length !== 1 ? "s" : ""}`;

  if (!entries.length) {
    grid.innerHTML = '<div class="empty-state">No scraper plugins found.<br><small class="text-muted">Drop .py scripts into the scrapers/ directory to get started.</small></div>';
    return;
  }

  const summaryByScraper = (summary && summary.scrapers) || {};

  grid.innerHTML = entries.map(([name, s]) => {
    const dotClass = s.running ? "scraper-dot-running"
      : (s.last_run && s.last_run.error) ? "scraper-dot-error"
      : s.last_run ? "scraper-dot-ok"
      : "scraper-dot-idle";
    const checked = s.enabled ? "checked" : "";
    const disabledClass = s.enabled ? "" : " disabled";
    const tags = (s.default_tags || []).join(", ");
    const lastTime = s.last_run ? _timeAgo(s.last_run.time) : "never";
    const lastEvents = s.last_run ? s.last_run.events : "--";
    const lastErr = s.last_run && s.last_run.error
      ? `<span class="stat-err" title="${esc(s.last_run.error)}">${esc(s.last_run.error).substring(0, 40)}</span>`
      : "none";
    const nextRun = s.next_run_time ? _timeUntil(s.next_run_time) : (s.enabled ? "pending" : "--");
    const hasScript = s.has_script;
    const sourceOpen = scraperSourceOpen[name];
    const useQueue = s.use_event_queue !== false;
    const queueChecked = useQueue ? "checked" : "";
    const legacyBadge = useQueue ? "" : '<span class="badge" style="font-size:10px;color:var(--warn);border-color:var(--warn)">legacy mode</span>';

    const q = summaryByScraper[name];
    let queueLine = "";
    if (useQueue) {
      if (q) {
        const parts = [];
        parts.push(`<span class="stat-val">${q.pending || 0}</span> pending`);
        parts.push(`<span class="stat-val">${q.resolved_24h || 0}</span> resolved (24h)`);
        if (q.failed_final) parts.push(`<span class="stat-val">${q.failed_final}</span> dismissed`);
        if (q.resolving) parts.push(`<span class="stat-val" style="color:var(--accent)">${q.resolving}</span> resolving`);
        queueLine = `<div class="scraper-queue-line" onclick="channelarr.scraperFilterQueue('${esc(name)}')" title="Filter Event Queue to this scraper">Queue: ${parts.join(" • ")}</div>`;
      } else {
        queueLine = '<div class="scraper-queue-line text-muted">Queue: <span class="stat-val">0</span> pending</div>';
      }
    }

    return `<div class="scraper-card${disabledClass}" data-scraper="${esc(name)}">
      <div class="scraper-card-header">
        <div class="scraper-card-name">
          <span class="scraper-dot ${dotClass}"></span>
          ${esc(name)}
          ${!hasScript ? '<span class="badge" style="font-size:10px;color:var(--warn)">no script</span>' : ''}
          ${legacyBadge}
        </div>
        <label class="scraper-toggle">
          <input type="checkbox" ${checked} onchange="channelarr.scraperToggle('${esc(name)}', this)">
          <span class="slider"></span>
        </label>
      </div>
      <div class="scraper-config">
        <div class="scraper-config-row">
          <label>Interval</label>
          <input type="number" min="0.5" step="0.5" value="${s.interval_hours}" data-field="interval_hours" style="width:70px">
          <span style="color:var(--text-muted)">hours</span>
          <label style="width:auto;margin-left:12px">Timeout</label>
          <input type="number" min="10" step="10" value="${s.timeout}" data-field="timeout" style="width:60px">
          <span style="color:var(--text-muted)">s</span>
        </div>
        <div class="scraper-config-row">
          <label>Tags</label>
          <input type="text" value="${esc(tags)}" data-field="default_tags" placeholder="comma-separated">
        </div>
        <div class="scraper-config-row">
          <label>Filter</label>
          <input type="text" value="${esc(s.title_filter || "")}" data-field="title_filter" placeholder="title/tag substrings, comma-separated" title="Case-insensitive match against title and tags. Empty = pass all." style="flex:1">
          <label class="toggle-label" style="font-size:12px;white-space:nowrap;gap:4px" title="Invert: drop events that match instead of keeping them"><input type="checkbox" ${(s.title_filter_invert ? "checked" : "")} data-field="title_filter_invert"> Invert</label>
        </div>
        <div class="scraper-config-row">
          <label class="toggle-label" style="font-size:12px;white-space:nowrap;gap:6px"><input type="checkbox" ${queueChecked} data-field="use_event_queue"> Event queue (JIT resolve)</label>
        </div>
      </div>
      <div class="scraper-stats">
        <span>Last run: <span class="stat-val">${lastTime}</span></span>
        <span>Events: <span class="stat-val">${lastEvents}</span></span>
        <span>Errors: ${lastErr}</span>
        <span>Next: <span class="stat-val">${nextRun}</span></span>
      </div>
      ${queueLine}
      <div class="scraper-card-actions">
        <div style="display:flex;gap:6px">
          <button class="btn-sm" onclick="channelarr.scraperRun('${esc(name)}')" ${!hasScript ? "disabled" : ""}>Run Now</button>
          ${hasScript ? `<button class="btn-sm" onclick="channelarr.scraperViewSource('${esc(name)}', this)">${sourceOpen ? "Hide Source" : "View Source"}</button>` : ""}
        </div>
        <button class="btn-sm" onclick="channelarr.scraperSave('${esc(name)}')">Save Config</button>
      </div>
      <div class="scraper-source-wrap" id="scraper-source-${esc(name)}" style="display:${sourceOpen ? "block" : "none"}"></div>
    </div>`;
  }).join("");
}

channelarr.scraperToggle = async function(name, checkbox) {
  const card = checkbox.closest(".scraper-card");
  const cfg = _readScraperConfig(card, checkbox.checked);
  try {
    const r = await fetch(`${API}/scrapers/${encodeURIComponent(name)}/config`, {
      method: "PUT",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(cfg),
    });
    const d = await r.json();
    if (r.ok && d.ok) {
      toast("success", `${name} ${cfg.enabled ? "enabled" : "disabled"}`);
      loadScrapers();
    } else {
      toast("error", d.detail || d.error || "Toggle failed");
      checkbox.checked = !checkbox.checked;
    }
  } catch (e) {
    toast("error", "Toggle failed");
    checkbox.checked = !checkbox.checked;
  }
};

channelarr.scraperSave = async function(name) {
  const card = $(`[data-scraper="${name}"]`);
  if (!card) return;
  const toggle = card.querySelector(".scraper-toggle input");
  const cfg = _readScraperConfig(card, toggle ? toggle.checked : false);
  try {
    const r = await fetch(`${API}/scrapers/${encodeURIComponent(name)}/config`, {
      method: "PUT",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(cfg),
    });
    const d = await r.json();
    if (r.ok && d.ok) {
      toast("success", `Config saved for ${name}`);
      loadScrapers();
    } else {
      toast("error", d.detail || d.error || "Save failed");
    }
  } catch (e) {
    toast("error", "Save failed");
  }
};

channelarr.scraperRun = async function(name) {
  try {
    const r = await fetch(`${API}/scrapers/${encodeURIComponent(name)}/run`, {method: "POST"});
    const d = await r.json();
    if (r.ok && d.ok) {
      toast("success", d.message || `Running ${name}`);
      setTimeout(loadScrapers, 500);
    } else {
      toast("error", d.detail || d.error || "Run failed");
    }
  } catch (e) {
    toast("error", "Run failed");
  }
};

channelarr.scraperViewSource = async function(name, btn) {
  const wrap = $(`#scraper-source-${name}`);
  if (!wrap) return;
  if (scraperSourceOpen[name]) {
    scraperSourceOpen[name] = false;
    wrap.style.display = "none";
    if (btn) btn.textContent = "View Source";
    return;
  }
  try {
    const r = await fetch(`${API}/scrapers/${encodeURIComponent(name)}/source`);
    if (!r.ok) { toast("error", "Failed to load source"); return; }
    const text = await r.text();
    const lines = text.split("\n").map(l =>
      `<span class="line">${esc(l)}</span>`
    ).join("");
    wrap.innerHTML = `<pre class="scraper-source">${lines}</pre>`;
    wrap.style.display = "block";
    scraperSourceOpen[name] = true;
    if (btn) btn.textContent = "Hide Source";
  } catch (e) {
    toast("error", "Failed to load source");
  }
};

function _readScraperConfig(card, enabled) {
  const interval = parseFloat(card.querySelector('[data-field="interval_hours"]').value) || 6;
  const timeout = parseInt(card.querySelector('[data-field="timeout"]').value) || 90;
  const tagsRaw = card.querySelector('[data-field="default_tags"]').value;
  const tags = tagsRaw.split(",").map(t => t.trim()).filter(Boolean);
  const queueToggle = card.querySelector('[data-field="use_event_queue"]');
  const useQueue = queueToggle ? queueToggle.checked : true;
  const filterInput = card.querySelector('[data-field="title_filter"]');
  const titleFilter = filterInput ? filterInput.value.trim() : "";
  const invertInput = card.querySelector('[data-field="title_filter_invert"]');
  const titleFilterInvert = invertInput ? invertInput.checked : false;
  return {enabled, interval_hours: interval, default_tags: tags, timeout, use_event_queue: useQueue, title_filter: titleFilter, title_filter_invert: titleFilterInvert};
}

// ─── Event Queue ───
const EQ_STATUSES = ["pending", "resolving", "resolved", "failed", "failed_final", "expired"];
const EQ_DEFAULT_ACTIVE = new Set(["pending", "resolving", "failed"]);
let eqState = {
  statuses: new Set(EQ_DEFAULT_ACTIVE),
  rowsById: {},   // id -> row data (so action handlers can re-read after click)
  selected: new Set(),
};
let eqTimer = null;
let eqStatusPillsInit = false;

function _renderEqStatusPills() {
  const host = $("#eq-status-pills");
  if (!host) return;
  host.innerHTML = EQ_STATUSES.map(s => {
    const active = eqState.statuses.has(s) ? " active" : "";
    return `<label class="eq-pill eq-pill-${s}${active}">
      <input type="checkbox" ${eqState.statuses.has(s) ? "checked" : ""} onchange="channelarr.eqTogglePill('${s}', this)">${s}
    </label>`;
  }).join("");
  eqStatusPillsInit = true;
}

function _eqUrlParams() {
  const p = new URLSearchParams();
  const scr = $("#eq-filter-scraper");
  if (scr && scr.value) p.set("scraper", scr.value);
  const win = $("#eq-filter-window");
  p.set("window", win ? win.value : "upcoming");
  for (const s of eqState.statuses) p.append("status", s);
  p.set("limit", "500");
  return p.toString();
}

function _eqStatusBadge(st) {
  return `<span class="eq-status eq-status-${esc(st)}">${esc(st)}</span>`;
}

function _eqTagList(tags) {
  if (!tags || !tags.length) return '<span class="text-muted">—</span>';
  return tags.slice(0, 3).map(t =>
    `<span class="eq-tag">${esc(t)}</span>`
  ).join("") + (tags.length > 3 ? `<span class="text-muted" title="${esc(tags.slice(3).join(", "))}">+${tags.length - 3}</span>` : "");
}

function _eqEventStart(iso) {
  if (!iso) return '<span class="text-muted">—</span>';
  const abs = new Date(iso);
  const relative = _timeUntil(iso);
  const absStr = abs.toLocaleString(undefined, {month: "short", day: "numeric", hour: "2-digit", minute: "2-digit"});
  return `<div class="eq-start-primary">${esc(relative)}</div><div class="eq-start-abs">${esc(absStr)}</div>`;
}

function _eqChannelCell(row) {
  if (!row.channel_id) return '<span class="text-muted">—</span>';
  return `<a href="#" onclick="channelarr.editChannel('${esc(row.channel_id)}');return false" title="Open channel">${esc(row.channel_id.slice(0, 8))}…</a>`;
}

function _eqErrorCell(err) {
  if (!err) return '<span class="text-muted">—</span>';
  const short = err.length > 60 ? err.slice(0, 57) + "…" : err;
  return `<span class="eq-err" title="${esc(err)}">${esc(short)}</span>`;
}

async function loadEventQueue() {
  if (!eqStatusPillsInit) _renderEqStatusPills();
  try {
    const [rowsR, summaryR, batchR, jitR] = await Promise.all([
      fetch(`${API}/scraped-events?${_eqUrlParams()}`),
      fetch(`${API}/scraped-events/summary`),
      fetch(`${API}/resolve/batch/status`),
      fetch(`${API}/scraped-events/jit-status`),
    ]);
    const rowsData = await rowsR.json();
    const summaryData = summaryR.ok ? await summaryR.json() : {scrapers: {}};
    const batch = batchR.ok ? await batchR.json() : {running: false};
    const jit = jitR.ok ? await jitR.json() : null;
    _renderEventQueue(rowsData.results || [], summaryData.scrapers || {}, batch);
    _renderEqProgress(batch);
    _renderJitBadge(jit, batch);

    // Auto-refresh when something is in motion
    const anyResolving = (rowsData.results || []).some(r => r.status === "resolving");
    const needsPoll = anyResolving || batch.running;
    if (needsPoll && !eqTimer) {
      eqTimer = setInterval(loadEventQueue, 3000);
    } else if (!needsPoll && eqTimer) {
      clearInterval(eqTimer);
      eqTimer = null;
    }
  } catch (e) {
    const body = $("#event-queue-body");
    if (body) body.innerHTML = `<tr><td colspan="10" class="empty-state" style="color:var(--danger)">Failed to load: ${esc(String(e))}</td></tr>`;
  }
}

function _renderEventQueue(rows, summaryByScraper, batch) {
  // Scraper dropdown options — union of current filter + scrapers seen in summary
  const scrSel = $("#eq-filter-scraper");
  if (scrSel) {
    const current = scrSel.value;
    const names = Object.keys(summaryByScraper).sort();
    scrSel.innerHTML = '<option value="">All scrapers</option>' +
      names.map(n => `<option value="${esc(n)}"${n === current ? " selected" : ""}>${esc(n)}</option>`).join("");
  }

  // Stash rows for action callbacks
  eqState.rowsById = {};
  for (const r of rows) eqState.rowsById[r.id] = r;

  $("#event-queue-count").textContent = `${rows.length}`;
  const body = $("#event-queue-body");
  if (!rows.length) {
    body.innerHTML = '<tr><td colspan="9" class="empty-state" style="padding:24px">No events match the current filters.</td></tr>';
    _refreshEqBulkBar();
    return;
  }

  const activeUrl = batch && batch.running ? batch.current_url : null;

  body.innerHTML = rows.map(r => {
    const canResolve = !["resolved", "resolving"].includes(r.status);
    const canDismiss = !["resolved", "failed_final", "expired"].includes(r.status);
    const selected = eqState.selected.has(r.id) ? "checked" : "";
    const isActive = activeUrl && r.url === activeUrl;
    const rowClass = isActive ? " class=\"eq-row-active\"" : "";
    const activeMark = isActive ? '<span class="eq-dot-live" title="Currently capturing"></span>' : '';
    return `<tr data-id="${esc(r.id)}"${rowClass}>
      <td><input type="checkbox" class="eq-row-check" ${selected} onchange="channelarr.eqToggleRow('${esc(r.id)}', this)"></td>
      <td class="eq-scraper-cell" title="${esc(r.scraper_name)}">${esc(r.scraper_name)}</td>
      <td class="eq-title-cell" title="${esc(r.title || "")}">${activeMark}${esc(r.title || "—")}</td>
      <td>${_eqTagList(r.tags)}</td>
      <td>${_eqEventStart(r.event_start)}</td>
      <td>${_eqStatusBadge(r.status)}</td>
      <td style="text-align:right;font-variant-numeric:tabular-nums">${r.attempt_count || 0}</td>
      <td>${_eqErrorCell(r.last_error)}</td>
      <td style="text-align:right">
        <span class="eq-actions">
          <button class="btn-sm-ok" onclick="channelarr.eqResolveNow('${esc(r.id)}')" ${canResolve ? "" : "disabled"}>Resolve</button>
          <button class="btn-sm" onclick="channelarr.eqDismiss('${esc(r.id)}')" ${canDismiss ? "" : "disabled"}>Dismiss</button>
          <button class="btn-sm-danger" onclick="channelarr.eqDelete('${esc(r.id)}')">Delete</button>
        </span>
      </td>
    </tr>`;
  }).join("");
  _refreshEqBulkBar();
}

function _renderJitBadge(jit, batch) {
  const badge = $("#jit-status-badge");
  if (!badge) return;
  badge.classList.remove("badge-running", "badge-stopped");
  if (!jit || !jit.enabled) {
    badge.textContent = "JIT: disabled";
    badge.classList.add("badge-stopped");
    badge.title = "JIT resolver job not registered on the scheduler";
    return;
  }
  const parts = [];
  if (batch && batch.running) {
    parts.push(`resolving ${batch.completed || 0}/${batch.total || 0}`);
  } else if (jit.next_run_time) {
    parts.push(`next ${_timeUntil(jit.next_run_time)}`);
  }
  const lt = jit.last_tick || {};
  if (lt.time) {
    const res = lt.resolved || 0;
    const fail = lt.failed || 0;
    if (res || fail) parts.push(`last +${res}/−${fail}`);
    else if (lt.picked) parts.push(`last ${lt.picked} picked`);
  }
  badge.textContent = "JIT: " + (parts.join(" · ") || "active");
  badge.classList.add("badge-running");
  badge.title = lt.time ? `Last tick: ${new Date(lt.time).toLocaleString()}` : "JIT active";
}

function _renderEqProgress(batch) {
  const host = $("#eq-progress");
  const label = $("#eq-progress-label");
  const fill = $("#eq-progress-fill");
  if (!host) return;
  if (batch && batch.running) {
    const total = batch.total || 0;
    const done = batch.completed || 0;
    const pct = total ? Math.round((done / total) * 100) : 0;
    const cur = batch.current_url ? ` — ${batch.current_url}` : "";
    if (label) label.textContent = `Resolving ${done}/${total} (${pct}%)${cur}`;
    if (fill) fill.style.width = `${pct}%`;
    host.style.display = "block";
  } else {
    host.style.display = "none";
  }
}

function _refreshEqBulkBar() {
  const bar = $("#eq-bulk-bar");
  const count = $("#eq-bulk-count");
  if (!bar) return;
  // Prune selected IDs that aren't in the currently-rendered set
  for (const id of Array.from(eqState.selected)) {
    if (!eqState.rowsById[id]) eqState.selected.delete(id);
  }
  if (eqState.selected.size) {
    bar.style.display = "";
    if (count) count.textContent = `${eqState.selected.size} selected`;
  } else {
    bar.style.display = "none";
  }
  const selAll = $("#eq-select-all");
  if (selAll) {
    const visible = Object.keys(eqState.rowsById).length;
    selAll.checked = visible > 0 && eqState.selected.size >= visible;
  }
}

channelarr.eqReload = function() { loadEventQueue(); };

channelarr.eqTogglePill = function(status, input) {
  if (input.checked) eqState.statuses.add(status);
  else eqState.statuses.delete(status);
  const label = input.closest(".eq-pill");
  if (label) label.classList.toggle("active", input.checked);
  loadEventQueue();
};

channelarr.eqToggleRow = function(id, input) {
  if (input.checked) eqState.selected.add(id);
  else eqState.selected.delete(id);
  _refreshEqBulkBar();
};

channelarr.eqSelectAll = function(input) {
  if (input.checked) {
    for (const id of Object.keys(eqState.rowsById)) eqState.selected.add(id);
  } else {
    eqState.selected.clear();
  }
  // Update row checkboxes
  document.querySelectorAll(".eq-row-check").forEach(cb => { cb.checked = input.checked; });
  _refreshEqBulkBar();
};

channelarr.eqResolveNow = async function(id) {
  try {
    const r = await fetch(`${API}/scraped-events/${encodeURIComponent(id)}/resolve`, {method: "POST"});
    const d = await r.json();
    if (r.ok && d.ok) toast("success", "Queued for next tick");
    else toast("error", d.detail || d.error || "Failed");
  } catch (e) { toast("error", "Failed"); }
  loadEventQueue();
};

channelarr.eqDismiss = async function(id) {
  try {
    const r = await fetch(`${API}/scraped-events/${encodeURIComponent(id)}/dismiss`, {method: "POST"});
    const d = await r.json();
    if (r.ok && d.ok) toast("success", "Dismissed");
    else toast("error", d.detail || d.error || "Failed");
  } catch (e) { toast("error", "Failed"); }
  loadEventQueue();
};

channelarr.eqDelete = async function(id) {
  const row = eqState.rowsById[id];
  const label = row ? (row.title || id) : id;
  if (!confirm(`Delete "${label}" from the event queue?`)) return;
  try {
    const r = await fetch(`${API}/scraped-events/${encodeURIComponent(id)}`, {method: "DELETE"});
    const d = await r.json();
    if (r.ok && d.ok) toast("success", "Deleted");
    else toast("error", d.detail || d.error || "Failed");
  } catch (e) { toast("error", "Failed"); }
  eqState.selected.delete(id);
  loadEventQueue();
};

async function _eqBulkAction(verb, fetcher) {
  const ids = Array.from(eqState.selected);
  if (!ids.length) return;
  if (!confirm(`${verb} ${ids.length} event${ids.length === 1 ? "" : "s"}?`)) return;
  let ok = 0, fail = 0;
  for (const id of ids) {
    try {
      const r = await fetcher(id);
      if (r.ok) ok++; else fail++;
    } catch (e) { fail++; }
  }
  toast(fail ? "error" : "success", `${verb} — ${ok} ok${fail ? `, ${fail} failed` : ""}`);
  eqState.selected.clear();
  loadEventQueue();
}

channelarr.eqBulkDismiss = function() {
  _eqBulkAction("Dismissed", (id) =>
    fetch(`${API}/scraped-events/${encodeURIComponent(id)}/dismiss`, {method: "POST"}));
};

channelarr.eqBulkDelete = function() {
  _eqBulkAction("Deleted", (id) =>
    fetch(`${API}/scraped-events/${encodeURIComponent(id)}`, {method: "DELETE"}));
};

channelarr.scraperFilterQueue = function(name) {
  const sel = $("#eq-filter-scraper");
  if (sel) sel.value = name;
  // Ensure at least one status pill is active so rows actually render
  if (!eqState.statuses.size) {
    eqState.statuses = new Set(EQ_DEFAULT_ACTIVE);
    _renderEqStatusPills();
  }
  loadEventQueue();
  const panel = $("#event-queue-panel");
  if (panel) panel.scrollIntoView({behavior: "smooth", block: "start"});
};

})();
