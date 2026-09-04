/**
 * components.js - UI 组件模块（原型框架版）
 * 包含：Toast、Modal(schema 驱动)、Drawer、分页、认证状态、主题、自定义下拉与筛选 chips
 */

import { escapeHtml, setText } from './utils.js';

// ===== Toast 提示 =====

export function showToast(message, isError = false) {
  const wrap = document.getElementById("toastWrap");
  if (!wrap) return;
  const t = document.createElement("div");
  t.className = "toast" + (isError ? " error" : "");
  t.textContent = message;
  wrap.appendChild(t);
  window.setTimeout(() => {
    t.style.opacity = "0";
    t.style.transition = "opacity .3s";
    window.setTimeout(() => t.remove(), 300);
  }, 2200);
}

// ===== URL Tooltip 和复制功能 =====

let urlTooltip = null;

export function showUrlTooltip(e, url) {
  if (!url || url === "-") return;
  hideUrlTooltip();
  urlTooltip = document.createElement("div");
  urlTooltip.className = "url-tooltip";
  urlTooltip.textContent = url;
  document.body.appendChild(urlTooltip);
  const rect = e.target.getBoundingClientRect();
  let top = rect.bottom + 8;
  let left = rect.left;
  if (left + 500 > window.innerWidth) left = window.innerWidth - 510;
  if (top + 200 > window.innerHeight) top = rect.top - 208;
  urlTooltip.style.top = top + "px";
  urlTooltip.style.left = left + "px";
}

export function hideUrlTooltip() {
  if (urlTooltip) { urlTooltip.remove(); urlTooltip = null; }
}

export function copyToClipboard(text) {
  if (!text || text === "-") return;
  const done = () => showToast("已复制到剪贴板");
  const fail = () => showToast("复制失败", true);
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(done).catch(() => fallbackCopy(text, done, fail));
  } else {
    fallbackCopy(text, done, fail);
  }
}

function fallbackCopy(text, done, fail) {
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  try { document.execCommand("copy"); done(); } catch (e) { fail(); }
  document.body.removeChild(textarea);
}

// ===== 认证状态 =====

export function setAuthError(message = "") {
  const el = document.getElementById("auth-error");
  if (!el) return;
  const normalized = String(message || "").trim();
  el.hidden = !normalized;
  el.textContent = normalized;
}

export function applyAuthState(auth, state) {
  if (!state) return;
  state.auth = {
    enabled: Boolean(auth && auth.enabled),
    authenticated: Boolean(auth && auth.authenticated),
    username: (auth && auth.username) || "",
  };
  const locked = state.auth.enabled && !state.auth.authenticated;
  document.body.classList.toggle("auth-locked", locked);
  const overlay = document.getElementById("auth-overlay");
  if (overlay) overlay.classList.toggle("is-active", locked);
  const logoutBtn = document.getElementById("auth-logout-btn");
  if (logoutBtn) logoutBtn.hidden = !state.auth.enabled || !state.auth.authenticated;
  const uname = document.getElementById("sideUsername");
  if (uname) uname.textContent = state.auth.username || "admin";
  const avatar = document.getElementById("sideAvatar");
  if (avatar) avatar.textContent = (state.auth.username || "A").charAt(0).toUpperCase();
}

// ===== 主题管理 =====

const THEME_STORAGE_KEY = "proxy_admin_theme";

export function initTheme() {
  const saved = localStorage.getItem(THEME_STORAGE_KEY);
  const theme = saved === "dark" ? "dark" : "light";
  applyTheme(theme, false);
}

export function applyTheme(theme, persist = true) {
  const next = theme === "dark" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", next);
  if (persist) localStorage.setItem(THEME_STORAGE_KEY, next);
}

// ===== 分页组件 =====

export function renderPagination(currentPage, totalPages, containerId, onPageChange) {
  const container = document.getElementById(containerId);
  if (!container) return;
  container.innerHTML = "";
  container.classList.remove("is-hidden");

  const effectiveTotal = Math.max(1, totalPages);
  const maxVisible = 7;
  let pages = [];
  if (effectiveTotal <= maxVisible + 2) {
    for (let i = 1; i <= effectiveTotal; i++) pages.push(i);
  } else {
    pages.push(1);
    let start = Math.max(2, currentPage - 2);
    let end = Math.min(effectiveTotal - 1, currentPage + 2);
    if (start > 2) pages.push("...");
    for (let i = start; i <= end; i++) pages.push(i);
    if (end < effectiveTotal - 1) pages.push("...");
    pages.push(effectiveTotal);
  }

  const prevBtn = document.createElement("button");
  prevBtn.type = "button";
  prevBtn.className = "page-btn";
  prevBtn.textContent = "‹ 上一页";
  prevBtn.disabled = currentPage <= 1;
  prevBtn.addEventListener("click", () => onPageChange(currentPage - 1, effectiveTotal));
  container.appendChild(prevBtn);

  pages.forEach((p) => {
    if (p === "...") {
      const ellipsis = document.createElement("span");
      ellipsis.className = "page-ellipsis";
      ellipsis.textContent = "...";
      container.appendChild(ellipsis);
      return;
    }
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = `page-btn ${p === currentPage ? "is-active" : ""}`;
    btn.textContent = String(p);
    btn.addEventListener("click", () => onPageChange(p, effectiveTotal));
    container.appendChild(btn);
  });

  const nextBtn = document.createElement("button");
  nextBtn.type = "button";
  nextBtn.className = "page-btn";
  nextBtn.textContent = "下一页 ›";
  nextBtn.disabled = currentPage >= effectiveTotal;
  nextBtn.addEventListener("click", () => onPageChange(currentPage + 1, effectiveTotal));
  container.appendChild(nextBtn);

  const jumpWrap = document.createElement("span");
  jumpWrap.className = "page-jump";
  jumpWrap.innerHTML = `跳至 <input type="number" class="page-jump-input" min="1" max="${effectiveTotal}" value="${currentPage}" /> / ${effectiveTotal} 页`;
  container.appendChild(jumpWrap);

  const jumpInput = jumpWrap.querySelector(".page-jump-input");
  const doJump = () => {
    const v = parseInt(jumpInput.value, 10);
    if (Number.isFinite(v) && v >= 1 && v <= effectiveTotal) onPageChange(v, effectiveTotal);
    else jumpInput.value = String(currentPage);
  };
  jumpInput.addEventListener("keydown", (e) => { if (e.key === "Enter") doJump(); });
}

// ===== Modal 框架（schema 驱动） =====

function fieldRow(f, values) {
  const raw = (values && values[f.key] != null) ? values[f.key] : (f.default != null ? f.default : "");
  const esc = (s) => escapeHtml(s == null ? "" : String(s));
  if (f.type === "switch") {
    return `<div class="form-field switch-row"><div class="fr-text"><label>${esc(f.label)}</label>${f.hint ? `<div class="hint">${esc(f.hint)}</div>` : ""}</div><div class="switch ${raw ? "on" : ""}" data-fkey="${esc(f.key)}" role="switch" tabindex="0"></div></div>`;
  }
  const lab = `<label>${esc(f.label)}${f.required ? '<span class="req">*</span>' : ""}</label>`;
  let ctrl;
  if (f.type === "textarea" || f.type === "json") {
    ctrl = `<textarea class="input" data-fkey="${esc(f.key)}" rows="${f.rows || 3}" placeholder="${esc(f.placeholder || "")}">${esc(raw)}</textarea>`;
  } else if (f.type === "select") {
    ctrl = `<select class="input" data-fkey="${esc(f.key)}">${(f.options || []).map((o) => `<option value="${esc(o.value)}" ${String(o.value) === String(raw) ? "selected" : ""}>${esc(o.label)}</option>`).join("")}</select>`;
  } else {
    ctrl = `<input class="input" type="${esc(f.type || "text")}" data-fkey="${esc(f.key)}" value="${esc(raw)}" placeholder="${esc(f.placeholder || "")}">`;
  }
  const fieldId = `form-field-${esc(f.key)}`;
  return `<div class="form-field" id="${fieldId}">${lab}${ctrl}${f.hint ? `<div class="hint">${esc(f.hint)}</div>` : ""}</div>`;
}

export function openFormModal(opts) {
  const { title, sub, schema, values = {}, onSave, size, validate } = opts;
  const modalEl = document.getElementById("modal");
  const mask = document.getElementById("modalMask");
  if (!modalEl || !mask) return;
  modalEl.style.width = size ? `min(${size}px,100%)` : "";
  modalEl.innerHTML = `
    <div class="modal-head"><div><div class="modal-title">${escapeHtml(title)}</div>${sub ? `<div class="modal-sub">${escapeHtml(sub)}</div>` : ""}</div><button class="icon-btn" id="modalClose">✕</button></div>
    <div class="modal-body">${schema.map((f) => fieldRow(f, values)).join("")}</div>
    <div class="modal-foot"><button class="btn" id="modalCancel">取消</button><button class="btn btn-primary" id="modalSave">保存</button></div>`;
  // 防累积：innerHTML 重建后，上一轮弹窗的下拉弹层已成孤儿（其 select 已断连），立即清掉
  document.querySelectorAll(".cs-pop").forEach((p) => {
    if (p._sel && !p._sel.isConnected) p.remove();
  });
  modalEl.querySelectorAll(".switch[data-fkey]").forEach((s) => {
    const toggle = () => s.classList.toggle("on");
    s.addEventListener("click", toggle);
    s.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(); } });
  });
  // 弹窗内 select 统一升级为自定义下拉（展开面板可脱离 modal 裁剪）
  modalEl.querySelectorAll(".modal-body select[data-fkey]").forEach((s) => upgradeSelect(s));

  // select 联动：监听 schema 字段中带 dependsOn 的 select，值变更时显隐其他字段
  const depends = (modalEl.__depends = []);
  schema.forEach((f) => {
    if (f.dependsOn) depends.push(f);
  });
  const applyDeps = () => {
    depends.forEach((f) => {
      const dep = modalEl.querySelector(`[data-fkey="${f.dependsOn.field}"]`);
      if (!dep) return;
      const val = dep.tagName === "SELECT" ? dep.value : dep.value;
      const visible = String(val) === String(f.dependsOn.value);
      const row = document.getElementById(`form-field-${f.key}`);
      if (row) row.style.display = visible ? "" : "none";
    });
  };
  modalEl.querySelectorAll("[data-fkey]").forEach((el) => {
    if (el.tagName === "SELECT" || el.tagName === "INPUT") {
      el.addEventListener("change", applyDeps);
      el.addEventListener("input", applyDeps);
    }
  });
  applyDeps();
  document.getElementById("modalClose").onclick = closeModal;
  document.getElementById("modalCancel").onclick = closeModal;
  document.getElementById("modalSave").onclick = async () => {
    const out = {};
    modalEl.querySelectorAll("[data-fkey]").forEach((x) => {
      const k = x.dataset.fkey;
      if (x.classList.contains("switch")) out[k] = x.classList.contains("on");
      else if (x.tagName === "SELECT") out[k] = x.value;
      else if (x.type === "number") out[k] = x.value === "" ? null : Number(x.value);
      else out[k] = x.value;
    });
    if (validate) { const e = validate(out); if (e) { showToast(e, true); return; } }
    const saveBtn = document.getElementById("modalSave");
    if (saveBtn) { saveBtn.disabled = true; saveBtn.textContent = "保存中…"; }
    try {
      await onSave(out);
      closeModal();
    } catch (e) {
      showToast(e && e.message ? e.message : "保存失败", true);
    } finally {
      if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = "保存"; }
    }
  };
  mask.classList.add("open");
  // 聚焦第一个输入
  window.setTimeout(() => {
    const first = modalEl.querySelector("[data-fkey]");
    if (first && first.tagName !== "DIV") first.focus();
  }, 50);
}

export function openConfirm(opts) {
  const { title, message, onOk, danger = true } = opts;
  const modalEl = document.getElementById("modal");
  const mask = document.getElementById("modalMask");
  if (!modalEl || !mask) return;
  modalEl.style.width = "";
  modalEl.innerHTML = `
    <div class="modal-head"><div class="modal-title">${escapeHtml(title)}</div><button class="icon-btn" id="modalClose">✕</button></div>
    <div class="modal-body"><div style="color:var(--text-2);line-height:1.6">${message}</div></div>
    <div class="modal-foot"><button class="btn" id="modalCancel">取消</button><button class="btn ${danger ? "btn-danger" : "btn-primary"}" id="modalOk">确定</button></div>`;
  document.getElementById("modalClose").onclick = closeModal;
  document.getElementById("modalCancel").onclick = closeModal;
  document.getElementById("modalOk").onclick = () => { closeModal(); if (onOk) onOk(); };
  mask.classList.add("open");
}

export function closeModal() {
  const mask = document.getElementById("modalMask");
  if (mask) mask.classList.remove("open");
  // 清理已随 modal innerHTML 重建而失联的孤儿弹层
  document.querySelectorAll(".cs-pop").forEach((p) => {
    if (p._sel && !p._sel.isConnected) p.remove();
  });
}

// ===== Drawer =====

export function openDrawer() {
  document.getElementById("drawer").classList.add("open");
  document.getElementById("drawerMask").classList.add("open");
}

export function closeDrawer() {
  document.getElementById("drawer").classList.remove("open");
  document.getElementById("drawerMask").classList.remove("open");
}

// ===== 自定义下拉 + 筛选 chips =====

function getDefaultOption(sel) {
  return sel.querySelector('option[value=""]') || sel.options[0];
}

export function refreshTrigger(sel) {
  if (!sel || !sel._csTrig) return;
  const label = sel._csTrig.querySelector(".cs-label");
  if (label) label.textContent = sel.options[sel.selectedIndex].textContent;
  if (sel._csPop) {
    sel._csPop.querySelectorAll(".cs-opt").forEach((o) => o.classList.toggle("sel", o.dataset.val === sel.value));
  }
}

function buildPopOptions(sel, pop) {
  pop.innerHTML = "";
  [...sel.options].forEach((o) => {
    const op = document.createElement("div");
    op.className = "cs-opt" + (o.value === sel.value ? " sel" : "");
    op.dataset.val = o.value;
    // 勾选图标由 .cs-opt.sel::after 按 .sel 状态绘制（见 admin.css），保证与高亮严格同步
    op.innerHTML = `<span>${escapeHtml(o.textContent)}</span>`;
    op.addEventListener("click", (e) => {
      e.stopPropagation();
      sel.value = o.value;
      refreshTrigger(sel);
      closeSelectPops();
      sel.dispatchEvent(new Event("change"));
      renderAllChips();
    });
    pop.appendChild(op);
  });
}

let _popScrollBound = false;

export function upgradeSelect(sel) {
  if (!sel || sel._csTrig) return;
  const item = sel.closest(".filter-item");
  const label = item ? item.querySelector(".fi-label").textContent : "";
  const def = getDefaultOption(sel);
  sel.dataset.default = def ? def.value : "";
  sel.dataset.label = label;
  const wrap = document.createElement("span");
  wrap.className = "cs";
  const trig = document.createElement("button");
  trig.type = "button";
  trig.className = "cs-trigger";
  const caret = '<svg class="cs-caret" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>';
  trig.innerHTML = `<span class="cs-label">${escapeHtml(sel.options[sel.selectedIndex].textContent)}</span>${caret}`;
  const pop = document.createElement("div");
  pop.className = "cs-pop";
  pop._sel = sel;
  buildPopOptions(sel, pop);

  // pop 挂 body + fixed 定位：不受 modal/drawer 的 overflow 裁剪与 transform 影响
  const placePop = () => {
    const r = trig.getBoundingClientRect();
    pop.style.minWidth = r.width + "px";
    const ph = pop.offsetHeight;
    const below = r.bottom + 6 + ph <= window.innerHeight - 8;
    let top = below ? r.bottom + 6 : Math.max(8, r.top - 6 - ph);
    let left = Math.min(r.left, window.innerWidth - pop.offsetWidth - 8);
    pop.style.top = top + "px";
    pop.style.left = Math.max(8, left) + "px";
  };
  trig.addEventListener("click", (e) => {
    e.stopPropagation();
    const open = pop.classList.contains("open");
    closeSelectPops();
    if (!open) {
      pop.classList.add("open");
      trig.classList.add("open");
      placePop();
    }
  });
  wrap.appendChild(trig);
  sel.style.cssText = "position:absolute;width:1px;height:1px;opacity:0;pointer-events:none";
  sel.parentNode.insertBefore(wrap, sel);
  document.body.appendChild(pop);
  sel._csTrig = trig;
  sel._csPop = pop;

  if (!_popScrollBound) {
    _popScrollBound = true;
    document.addEventListener("scroll", closeSelectPops, true);
    window.addEventListener("resize", closeSelectPops);
  }
}

export function syncSelect(sel) {
  if (!sel || !sel._csPop) return;
  buildPopOptions(sel, sel._csPop);
  refreshTrigger(sel);
}

export function closeSelectPops() {
  document.querySelectorAll(".cs-pop.open").forEach((p) => p.classList.remove("open"));
  document.querySelectorAll(".cs-trigger.open").forEach((t) => t.classList.remove("open"));
}

export function renderChips(chipbarEl, toolbarEl) {
  if (!chipbarEl || !toolbarEl) return;
  const selects = [...toolbarEl.querySelectorAll("select")];
  const active = selects.filter((s) => String(s.value) !== String(s.dataset.default));
  chipbarEl.innerHTML = "";
  active.forEach((s) => {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.innerHTML = `<span>${escapeHtml(s.dataset.label || "")}：${escapeHtml(s.options[s.selectedIndex].textContent)}</span><span class="chip-x" title="清除">×</span>`;
    chip.querySelector(".chip-x").addEventListener("click", () => {
      s.value = s.dataset.default;
      refreshTrigger(s);
      s.dispatchEvent(new Event("change"));
      renderChips(chipbarEl, toolbarEl);
    });
    chipbarEl.appendChild(chip);
  });
  if (active.length) {
    const c = document.createElement("span");
    c.className = "chip-clear";
    c.textContent = "清除全部";
    c.addEventListener("click", () => {
      active.forEach((s) => { s.value = s.dataset.default; refreshTrigger(s); s.dispatchEvent(new Event("change")); });
      renderChips(chipbarEl, toolbarEl);
    });
    chipbarEl.appendChild(c);
  }
}

export function renderAllChips() {
  const rt = document.querySelector("#ruleToolbar");
  if (rt) renderChips(document.getElementById("ruleChips"), rt);
  const lt = document.querySelector("#page-logs [data-panel='req'] .toolbar");
  if (lt) renderChips(document.getElementById("logChips"), lt);
}
