/**
 * components.js - UI组件模块
 * 包含：Modal、Toast、分页、认证状态、URL工具等
 */

import { setText } from './utils.js';

// ===== 全局元素引用 =====

export const els = {
  authError: document.getElementById("auth-error"),
  authLogoutBtn: document.getElementById("auth-logout-btn"),
  authOverlay: document.getElementById("auth-overlay"),
  toast: document.getElementById("toast"),
  routeGroupOptions: document.getElementById("route-group-options"),
};

// ===== Modal 操作 =====

export function openModal(modalId) {
  const modal = document.getElementById(modalId);
  if (modal) {
    modal.hidden = false;
    document.body.style.overflow = 'hidden';
  }
}

export function closeModal(modalId) {
  const modal = document.getElementById(modalId);
  if (modal) {
    modal.hidden = true;
    document.body.style.overflow = '';
  }
}

export function closeAllModals() {
  document.querySelectorAll('.modal-overlay').forEach(modal => {
    modal.hidden = true;
  });
  document.body.style.overflow = '';
}

// ===== Toast 提示 =====

export function showToast(message, isError = false) {
  els.toast.textContent = message;
  els.toast.classList.remove("error", "success");
  els.toast.style.background = "";
  if (isError) {
    els.toast.classList.add("error");
  }
  els.toast.classList.add("visible");
  window.clearTimeout(showToast._timer);
  showToast._timer = window.setTimeout(() => {
    els.toast.classList.remove("visible");
  }, 2600);
}

export function showCopyToast(message) {
  const toast = document.createElement("div");
  toast.style.cssText = `
    position: fixed;
    top: 20px;
    left: 50%;
    transform: translateX(-50%);
    padding: 10px 20px;
    background: #10b981;
    color: white;
    border-radius: 8px;
    font-size: 14px;
    z-index: 10001;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  `;
  toast.textContent = message;
  document.body.appendChild(toast);
  
  setTimeout(() => {
    toast.remove();
  }, 2000);
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
  
  if (left + 500 > window.innerWidth) {
    left = window.innerWidth - 510;
  }
  
  if (top + 200 > window.innerHeight) {
    top = rect.top - 208;
  }
  
  urlTooltip.style.top = top + "px";
  urlTooltip.style.left = left + "px";
}

export function hideUrlTooltip() {
  if (urlTooltip) {
    urlTooltip.remove();
    urlTooltip = null;
  }
}

export function copyToClipboard(text) {
  if (!text || text === "-") return;
  
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(() => {
      showCopyToast("已复制到剪贴板");
    }).catch(() => {
      fallbackCopy(text);
    });
  } else {
    fallbackCopy(text);
  }
}

function fallbackCopy(text) {
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  try {
    document.execCommand("copy");
    showCopyToast("已复制到剪贴板");
  } catch (e) {
    showCopyToast("复制失败");
  }
  document.body.removeChild(textarea);
}

// ===== 认证状态 =====

export function setAuthError(message = "") {
  if (!els.authError) return;
  const normalized = String(message || "").trim();
  els.authError.hidden = !normalized;
  els.authError.textContent = normalized;
}

export function applyAuthState(auth, state) {
  if (!state) return;
  state.auth = {
    enabled: Boolean(auth?.enabled),
    authenticated: Boolean(auth?.authenticated),
    username: auth?.username || "",
  };
  const locked = state.auth.enabled && !state.auth.authenticated;
  document.body.classList.toggle("auth-locked", locked);
  if (els.authOverlay) {
    els.authOverlay.classList.toggle("is-active", locked);
  }
  if (els.authLogoutBtn) {
    els.authLogoutBtn.hidden = !state.auth.enabled || !state.auth.authenticated;
  }
}

// ===== 主题管理 =====

const THEME_STORAGE_KEY = "proxy_admin_theme";
const THEME_VALUES = ["light", "dark", "cosmic", "ocean", "amber", "forest", "sakura"];
const THEME_MIGRATIONS = { sunset: "amber" };

export function initTheme() {
  let saved = localStorage.getItem(THEME_STORAGE_KEY);
  if (THEME_MIGRATIONS[saved]) {
    saved = THEME_MIGRATIONS[saved];
  }
  const theme = THEME_VALUES.includes(saved) ? saved : "light";
  applyTheme(theme);
}

export function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem(THEME_STORAGE_KEY, theme);
  document.querySelectorAll(".theme-dot").forEach((dot) => {
    dot.classList.toggle("active", dot.dataset.themeVal === theme);
  });
}

// ===== 仪表板渲染 =====

export function renderSummary(summary) {
  setText("metric-db-path", summary.database_path || "-");
  setText("metric-total-rules", summary.total_rules ?? 0);
  setText("metric-enabled-rules", summary.enabled_rules ?? 0);
  setText("metric-route-groups", summary.route_group_count ?? 0);
  setText("metric-region-groups", summary.region_enabled_group_count ?? 0);
}

export function animateCounter(el, target, duration = 600) {
  if (!el) return;
  const finalValue = Number.isFinite(target) ? Math.max(0, Math.floor(target)) : 0;
  const startTime = performance.now();
  const startValue = 0;
  const range = finalValue - startValue;
  if (range === 0) {
    el.textContent = String(finalValue);
    return;
  }
  function tick(now) {
    const elapsed = now - startTime;
    const progress = Math.min(1, elapsed / duration);
    const eased = 1 - Math.pow(1 - progress, 3);
    const current = Math.round(startValue + range * eased);
    el.textContent = String(current);
    if (progress < 1) {
      window.requestAnimationFrame(tick);
    } else {
      el.textContent = String(finalValue);
    }
  }
  window.requestAnimationFrame(tick);
}

export function renderDashboardMetrics(state, animate = true) {
  const metrics = {
    routes: state.routeGroups.length,
    rules: state.rules.length,
    bans: state.bannedIps.length,
    sources: state.geoSources.length,
    logfiles: (state.logFiles || []).length,
    backups: (state.backups || []).length,
  };
  document.querySelectorAll("[data-metric]").forEach((el) => {
    const key = el.dataset.metric;
    if (Object.prototype.hasOwnProperty.call(metrics, key)) {
      if (animate) {
        animateCounter(el, metrics[key]);
      } else {
        el.textContent = String(metrics[key]);
      }
    }
  });
}

// ===== 分页组件 =====

export function renderPagination(currentPage, totalPages, containerId, onPageChange) {
  const container = document.getElementById(containerId);
  if (!container) return;
  container.innerHTML = "";
  if (!container.offsetParent) return;
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
  prevBtn.textContent = "< 上一页";
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
  nextBtn.textContent = "下一页 >";
  nextBtn.disabled = currentPage >= effectiveTotal;
  nextBtn.addEventListener("click", () => onPageChange(currentPage + 1, effectiveTotal));
  container.appendChild(nextBtn);

  const jumpWrap = document.createElement("span");
  jumpWrap.className = "page-jump";
  jumpWrap.innerHTML = `
    跳至 <input type="number" class="page-jump-input" min="1" max="${effectiveTotal}" value="${currentPage}" /> / ${effectiveTotal} 页
  `;
  container.appendChild(jumpWrap);

  const jumpInput = jumpWrap.querySelector(".page-jump-input");
  const doJump = () => {
    const v = parseInt(jumpInput.value, 10);
    if (Number.isFinite(v) && v >= 1 && v <= effectiveTotal) {
      onPageChange(v, effectiveTotal);
    } else {
      jumpInput.value = String(currentPage);
    }
  };
  jumpInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") doJump();
  });
}
