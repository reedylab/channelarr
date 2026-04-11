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
$$(".nav-item").forEach(btn => {
  btn.addEventListener("click", () => {
    switchView(btn.dataset.view);
  });
});

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
  if (view !== "resolver") {
    clearInterval(resolverTimer);
    resolverTimer = null;
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
              <button class="btn btn-sm" onclick="channelarr.deleteChannel('${ch.id}')">Delete</button>
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

  // Toggle scheduled-only vs resolved-only sections
  $("#ch-scheduled-only").style.display = isResolved ? "none" : "";
  $("#ch-resolved-only").style.display = isResolved ? "" : "none";
  if (isResolved) {
    $("#ch-resolved-url").value = ch.manifest_url || "";
    const transcodeOn = !!ch.transcode_mediated;
    $("#ch-resolved-transcode").checked = transcodeOn;
    $("#ch-resolved-bump-section").style.display = transcodeOn ? "" : "none";
    const bc = ch.bump_config || {};
    const selected = bc.folders || (bc.folder ? [bc.folder] : []);
    loadResolvedBumpFolders(selected);
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

// Toggle the bump section when transcode-mediated is checked
document.addEventListener("DOMContentLoaded", () => {
  const cb = document.getElementById("ch-resolved-transcode");
  if (cb) {
    cb.addEventListener("change", () => {
      $("#ch-resolved-bump-section").style.display = cb.checked ? "" : "none";
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

async function saveChannel() {
  const name = $("#ch-name").value.trim();
  if (!name) { toast("error", "Channel name required"); return; }

  const isResolved = !!(editingChannel && editingChannel.type === "resolved");

  let data;
  if (isResolved) {
    // Resolved channels: name + transcode_mediated toggle + bump folders.
    const transcodeOn = $("#ch-resolved-transcode").checked;
    data = {
      name,
      transcode_mediated: transcodeOn,
    };
    if (transcodeOn) {
      data.bump_config = {
        enabled: true,
        folders: getSelectedResolvedBumpFolders(),
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
    };
  }

  try {
    if (editingChannel) {
      await fetch(`${API}/channels/${editingChannel.id}`, {
        method: "PUT",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(data),
      });
      toast("success", "Channel updated — schedule regenerated");
    } else {
      await fetch(`${API}/channels`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(data),
      });
      toast("success", "Channel created — schedule generated");
    }
    closeEditor();
    loadChannels();
    updateStatus();
  } catch(e) { toast("error", "Failed to save channel"); }
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
  } catch(e) {}
}

function renderAllSettings() {
  $("#settings-section-title").textContent = "Settings";
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
    toast("success", "Settings saved");
  } catch(e) {
    status.textContent = "Save failed";
    status.className = "settings-status error";
    toast("error", "Failed to save settings");
  }
  setTimeout(() => { status.textContent = ""; status.className = "settings-status"; }, 3000);
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

  // Layer any in-flight batch state on top
  try {
    const r = await fetch(`${API}/resolve/batch/status`);
    const b = await r.json();
    if (b.running || (b.results && b.results.some(x => x.status === "resolving" || x.status === "pending"))) {
      // A batch is active — show batch state (it includes in-progress + recently done)
      renderResolverQueue(b);
      if (b.running) startResolverPolling();
    } else {
      // No active batch — show persisted channels
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
      <td>${title}${expiryHint ? "<br>" + expiryHint : ""}${channelBadge ? "<br>" + channelBadge : ""}</td>
      <td class="res-url" title="${url}">${truncUrl}</td>
      <td>${actions}</td>
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
      const r = await fetch(`${API}/resolve/batch/status`);
      const b = await r.json();
      renderResolverQueue(b);
      if (!b.running) {
        clearInterval(resolverTimer);
        resolverTimer = null;
        const doneCount = b.results.filter(x => x.status === "done").length;
        toast("success", `Batch complete: ${doneCount} resolved`);
        // Reload full persisted list so previously-resolved channels reappear
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
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) urls = parsed.map(e => typeof e === "string" ? {url: e} : e);
      } catch (e) {
        urls = raw.split("\n").map(l => l.trim()).filter(l => l && l.startsWith("http")).map(u => ({url: u, title: title || null}));
      }
      if (urls.length === 0) { toast("error", "No valid URLs found"); return; }
      try {
        const r = await fetch(`${API}/resolve/batch`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ urls, timeout }),
        });
        const j = await r.json();
        if (j.ok) {
          toast("info", `Resolving ${urls.length} URL${urls.length > 1 ? "s" : ""}...`);
          $("#resolver-urls").value = "";
          $("#resolver-title").value = "";
          startResolverPolling();
          setTimeout(async () => {
            const res = await fetch(`${API}/resolve/batch/status`);
            renderResolverQueue(await res.json());
          }, 500);
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
      closeResolverPlayer();
    });
  }
  const closeBtn = $("#resolver-player-close");
  if (closeBtn) closeBtn.addEventListener("click", closeResolverPlayer);
});

// Initial
updateStatus();
loadChannels();
loadSettings();

setInterval(tick, 3000);
setInterval(() => {
  if ($("#view-channels").classList.contains("visible")) loadChannels();
  if ($("#view-guide").classList.contains("visible")) loadGuide();
}, 10000);

})();
