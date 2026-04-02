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
const settingsSubnav = $("#settings-subnav");

$$(".nav-item").forEach(btn => {
  btn.addEventListener("click", () => {
    const view = btn.dataset.view;
    if (view === "settings") {
      const par = btn;
      const isExp = settingsSubnav.classList.contains("expanded");
      if (isExp) {
        settingsSubnav.classList.remove("expanded");
        par.classList.remove("expanded");
        switchView("channels");
      } else {
        settingsSubnav.classList.add("expanded");
        par.classList.add("expanded");
        switchView("settings");
        if (!activeSettingsSection && Object.keys(settingsSchema).length) {
          showSettingsSection(Object.keys(settingsSchema)[0]);
        }
      }
      return;
    }
    settingsSubnav.classList.remove("expanded");
    $$(".nav-item-parent").forEach(p => p.classList.remove("expanded"));
    switchView(view);
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
    const np = ch.now_playing;
    const items = ch.items || [];
    const bc = ch.bump_config || {};
    const meta = [];
    meta.push(`${items.length} item${items.length !== 1 ? "s" : ""}`);
    const bFolders = bc.folders || (bc.folder ? [bc.folder] : []);
    if (bc.enabled && bFolders.length) meta.push(`Bumps: ${bFolders.join(", ")}`);
    if (ch.shuffle) meta.push("Shuffle");
    if (ch.loop) meta.push("Loop");
    const logoUrl = `${API}/logo/${ch.id}`;

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
      <div class="channel-card" data-id="${ch.id}">
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

$("#new-channel-btn").addEventListener("click", () => openEditor(null));
$("#modal-close").addEventListener("click", closeEditor);
$("#modal-cancel").addEventListener("click", closeEditor);
$("#modal-save").addEventListener("click", saveChannel);

function openEditor(ch) {
  editingChannel = ch;
  editorItems = ch ? JSON.parse(JSON.stringify(ch.items || [])) : [];
  $("#modal-title").textContent = ch ? "Edit Channel" : "New Channel";
  $("#ch-name").value = ch ? ch.name : "";
  const bc = ch ? (ch.bump_config || {}) : {};
  $("#ch-bump-enabled").checked = !!bc.enabled;
  $("#ch-bump-start").checked = !!bc.start_bumps;
  $("#ch-bump-next").checked = !!bc.show_next;
  $("#ch-bump-freq").value = bc.frequency || "between";
  $("#ch-bump-count").value = bc.count || 1;
  $("#ch-shuffle").checked = ch ? !!ch.shuffle : false;
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

  const selectedFolders = bc.folders || (bc.folder ? [bc.folder] : []);
  loadBumpFolders(selectedFolders);

  pickerTab = "movies";
  pickerShowEpisodes = null;
  renderEditorItems();
  renderPicker();
  updateSchedulePreview();
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

function renderEditorItems() {
  const list = $("#ch-items");
  $("#ch-item-count").textContent = `(${editorItems.length})`;
  if (!editorItems.length) {
    list.innerHTML = '<div class="empty-state">No content added</div>';
    return;
  }
  list.innerHTML = editorItems.map((item, i) => {
    const badge = item.type === "show" ? "show" : (item.type || "?");
    return `
    <div class="ch-item">
      <span class="ch-item-type ${item.type === "show" ? "ch-item-show" : ""}">${esc(badge)}</span>
      <span class="ch-item-title" title="${esc(item.path)}">${esc(item.title || item.path)}</span>
      <button class="btn-remove" onclick="channelarr.removeItem(${i})">&times;</button>
    </div>`;
  }).join("");
}

channelarr.removeItem = function(i) {
  editorItems.splice(i, 1);
  renderEditorItems();
  updateSchedulePreview();
};

channelarr.addToChannel = function(item) {
  editorItems.push(item);
  renderEditorItems();
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

  let sched = [];
  if (bumpEnabled && bumpFolders.length && startBumps) {
    for (let b = 0; b < bumpCount; b++) sched.push({ type: "bump", title: `[bump]` });
  }

  editorItems.forEach((item, i) => {
    if (bumpEnabled && bumpFolders.length && i > 0) {
      if (bumpFreq === "between" || (parseInt(bumpFreq) && i % parseInt(bumpFreq) === 0)) {
        for (let b = 0; b < bumpCount; b++) sched.push({ type: "bump", title: `[bump]` });
      }
    }
    if (item.type === "show") {
      sched.push({ type: "show", title: (item.title || item.path) + " (all episodes)" });
    } else {
      sched.push({ type: item.type, title: item.title || item.path });
    }
  });

  container.innerHTML = sched.map((s, i) => {
    const cls = s.type === "bump" ? "sched-bump" : s.type === "show" ? "sched-show" : "sched-content";
    return `
    <div class="sched-item ${cls}">
      <span class="sched-num">${i + 1}</span>
      <span>${esc(s.title)}</span>
    </div>`;
  }).join("");
}

["ch-bump-enabled", "ch-bump-freq", "ch-bump-count", "ch-bump-start", "ch-bump-next"].forEach(id => {
  $(`#${id}`).addEventListener("change", updateSchedulePreview);
});

async function saveChannel() {
  const name = $("#ch-name").value.trim();
  if (!name) { toast("error", "Channel name required"); return; }

  const data = {
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
    shuffle: $("#ch-shuffle").checked,
    loop: $("#ch-loop").checked,
  };

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
      const posterUrl = entry.path ? `${API}/media/poster?path=${encodeURIComponent(entry.path)}` : "";
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
      <div class="bump-clips hidden">
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
    renderSettingsNav();
    if (Object.keys(settingsSchema).length) {
      showSettingsSection(Object.keys(settingsSchema)[0]);
    }
  } catch(e) {}
}

function renderSettingsNav() {
  settingsSubnav.innerHTML = "";
  for (const [key, section] of Object.entries(settingsSchema)) {
    const btn = document.createElement("button");
    btn.className = "nav-subitem";
    btn.textContent = section.label;
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      showSettingsSection(key);
    });
    settingsSubnav.appendChild(btn);
  }
}

function showSettingsSection(sectionKey) {
  activeSettingsSection = sectionKey;
  const section = settingsSchema[sectionKey];
  if (!section) return;
  $("#settings-section-title").textContent = section.label;

  $$(".nav-subitem").forEach((btn, i) => {
    btn.classList.toggle("active", Object.keys(settingsSchema)[i] === sectionKey);
  });

  const container = $("#settings-container");
  container.innerHTML = "";
  const fields = document.createElement("div");
  fields.className = "settings-fields";

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

      if (field.type === "password") {
        const reveal = document.createElement("button");
        reveal.className = "btn-reveal";
        reveal.textContent = "show";
        reveal.addEventListener("click", () => {
          inp.type = inp.type === "password" ? "text" : "password";
          reveal.textContent = inp.type === "password" ? "show" : "hide";
        });
        wrap.appendChild(reveal);
      }

      div.appendChild(wrap);
    }

    fields.appendChild(div);
  }
  container.appendChild(fields);
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
    showSettingsSection(activeSettingsSection);
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
  const url = `/live/${id}/stream.m3u8`;
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
    tip.textContent = `${opts.label}: ${val.toFixed(1)}% at ${time}`;
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
