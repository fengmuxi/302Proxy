/**
 * modules.js - 业务逻辑模块（原型 6 页版）
 * 页面：概览 / 路由 / 安全 / IP定位 / 日志 / 系统
 * 所有弹窗改用 schema 驱动的 openFormModal，抽屉用 openDrawer，全部走真实 API。
 */

import { state } from './state.js';
import {
  setValue, setChecked, getValue, getChecked,
  getNonNegativeIntValue, getPositiveIntValue,
  setText, focusField,
  escapeHtml, formatDateTime, formatRemainTime, formatBytes,
  normalizeRequestHost, formatRequestHostLabel,
  findRouteGroup, toIsoDateTime,
  formatMatchStrategy, formatResultStatus, formatCacheStatus, formatMatchDetail,
  formatRouteLogRequestHost, formatRouteLogRuleRequestHost,
} from './utils.js';
import { apiFetch } from './api.js';
import {
  showToast, renderPagination, openFormModal, openConfirm, closeModal,
  openDrawer, closeDrawer, syncSelect, copyToClipboard,
} from './components.js';

const esc = (v) => escapeHtml(v == null ? "" : String(v));

// 百分号编码（URL encode）解码显示：仅还原 %XX 序列，保留 :/?=&# 等结构性字符；
// 非法 % 序列（如 %ZZ、%E4% 截断）会抛错，此时回退原串，绝不破坏原始值。
// 已解码串（无 %XX 序列）调用后原样返回，故对「已解码 / 未解码」两种存储都安全。
const decodeUrlDisplay = (v) => {
  if (v == null) return "";
  const s = String(v);
  if (!s.includes("%")) return s;
  try {
    return decodeURIComponent(s);
  } catch (_e) {
    return s;
  }
};

// 秒数 → 易读时长（小于 1 分钟走「秒」、小于 1 小时走「分秒」、更大走「小时分钟」）
function formatTtl(sec) {
  sec = Math.max(0, Math.round(Number(sec) || 0));
  if (sec < 60) return `${sec} 秒`;
  if (sec < 3600) {
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    return s ? `${m} 分 ${s} 秒` : `${m} 分钟`;
  }
  const h = Math.floor(sec / 3600);
  const m = Math.round((sec % 3600) / 60);
  return m ? `${h} 小时 ${m} 分` : `${h} 小时`;
}

// ============ 页面激活 / 导航 ============

const VALID_PAGES = ["overview", "routing", "security", "geo", "logs", "audit", "system", "backup", "email", "signing", "apidoc", "apikeys"];

export function setActivePage(page) {
  state.activeModule = page;
  document.querySelectorAll(".nav-item[data-page]").forEach((item) => {
    item.classList.toggle("active", item.dataset.page === page);
  });
  document.querySelectorAll(".page").forEach((p) => {
    p.hidden = p.id !== `page-${page}`;
  });
  const crumb = document.getElementById("breadcrumb");
  const labels = {
    overview: "系统概览", routing: "路由配置", security: "安全与封禁",
    geo: "IP 定位", logs: "日志与审计", audit: "审计日志", system: "系统设置",
    backup: "备份与恢复", email: "邮件提醒", signing: "加签防护", apidoc: "API 文档", apikeys: "API 密钥",
  };
  if (crumb) crumb.innerHTML = `${esc(labels[page] || "")} / <b>${esc(labels[page] || "")}</b>`;
  try {
    const url = new URL(window.location.href);
    url.hash = page;
    window.history.replaceState(null, "", url.toString());
  } catch (_) {}
}

export function activatePage(page) {
  if (!page || !VALID_PAGES.includes(page)) return;
  setActivePage(page);
  stopAutoRefresh();
  stopAppLogAutoRefresh();
  stopBanAutoRefresh();
  stopNetAutoRefresh();

  switch (page) {
    case "overview":
      renderOverview().catch(() => {});
      startNetAutoRefresh();
      startOverviewAutoRefresh();
      break;
    case "routing":
      refreshRouting();
      break;
    case "security":
      loadBannedIpList();
      loadAutoBanSettings();
      loadAutoBanStats();
      if (getChecked("ban_auto_refresh_enabled")) startBanAutoRefresh();
      startTrackedAutoRefresh();
      break;
    case "apikeys":
      loadApiKeys();
      break;
    case "geo":
      refreshGeo();
      break;
    case "logs":
      refreshRouteLogModule().catch((e) => showToast(e.message, true));
      refreshAppLogModule().catch((e) => showToast(e.message, true));
      if (getChecked("log_auto_refresh_enabled")) startAutoRefresh();
      break;
    case "audit":
      loadAuditLogs().catch((e) => showToast(e.message, true));
      break;
    case "system":
      loadIpCacheSettings();
      loadIpCacheStats();
      loadDedupSettings();
      loadDedupStats();
      loadStreamGuardSettings();
      loadLoginProtectionSettings();
      loadRateLimitSettings();
      loadCorsSettings();
      loadSettingsHistory();
      break;
    case "signing":
      // 签名 URL / 302 加签改写两张功能卡已独立为「加签防护」页
      // （此前误挂在 security 分支导致刷新后不加载的教训：页面激活分支必须与卡片所在页一致）
      loadSignedUrlSettings();
      loadRedirectSigningSettings();
      break;
    case "apidoc":
      loadApiDoc();
      break;
    case "backup":
      loadBackups();
      break;
    case "email":
      loadEmailSettings();
      loadNotificationsSettings();
      break;
  }
}

let _hashRoutingInitialized = false;
export function initHashRouting() {
  const hash = window.location.hash.replace(/^#\/?/, "");
  if (hash && VALID_PAGES.includes(hash) && hash !== state.activeModule) {
    activatePage(hash);
  } else if (state.activeModule === "overview") {
    // 无 hash（或 hash 即 overview）的初始加载不经过 activatePage，概览页虽为默认
    // 显示但轮询不会启动，网络吞吐卡会永远停在「正在采样」——此处补启
    startNetAutoRefresh();
    startOverviewAutoRefresh();
  }
  if (!_hashRoutingInitialized) {
    _hashRoutingInitialized = true;
    window.addEventListener("hashchange", () => {
      const h = window.location.hash.replace(/^#\/?/, "");
      if (h && VALID_PAGES.includes(h) && h !== state.activeModule) activatePage(h);
    });
  }
}

// ============ 仪表板加载 ============

export async function loadDashboard() {
  const [data, bansData, logsData, backupsData] = await Promise.all([
    apiFetch("/_admin/api/bootstrap"),
    apiFetch("/_admin/api/banned-ips").catch(() => ({ items: [] })),
    apiFetch("/_admin/api/app-logs").catch(() => ({ items: [] })),
    apiFetch("/_admin/api/backup/list").catch(() => ({ items: [] })),
  ]);
  state.bannedIps = bansData.items || [];
  state.logFiles = logsData.items || [];
  state.backups = backupsData.items || [];
  renderBackupStats(); // 启动时即更新「备份与恢复」侧边栏计数
  state.routeLogSettings = data.route_log_settings || null;

  const groups = data.route_groups || [];
  const rules = data.rules || [];
  setText("navCountRouting", String(groups.length));
  setText("navCountSecurity", String(state.bannedIps.length));

  renderRules(rules);
  renderRouteGroups(groups);
  fillGeoConfig(data.geoip || {});
  renderRouteLogSettings(data.route_log_settings || { retention_days: 30 });
  renderOverview().catch(() => {});
}

// 导航进入时按需重新拉取，保证多端/多会话数据一致性
export async function refreshRouting() {
  try {
    const data = await apiFetch("/_admin/api/bootstrap");
    const groups = data.route_groups || [];
    const rules = data.rules || [];
    setText("navCountRouting", String(groups.length));
    renderRouteGroups(groups);
    renderRules(rules);
  } catch (e) {
    showToast(e.message, true);
  }
}

export async function refreshGeo() {
  try {
    const data = await apiFetch("/_admin/api/geoip");
    fillGeoConfig(data || {});
    renderGeoSources();
  } catch (e) {
    showToast(e.message, true);
  }
}

// ============ 概览 ============

function formatDuration(seconds) {
  seconds = Math.max(0, Math.floor(seconds || 0));
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const parts = [];
  if (d) parts.push(`${d}天`);
  if (h) parts.push(`${h}小时`);
  if (m) parts.push(`${m}分`);
  if (!parts.length) parts.push(`${seconds}秒`);
  return parts.slice(0, 2).join("");
}

function relativeTime(ts) {
  if (!ts) return "未知";
  const diff = Math.max(0, Date.now() / 1000 - ts);
  if (diff < 60) return "刚刚";
  if (diff < 3600) return `${Math.floor(diff / 60)} 分钟前`;
  if (diff < 86400) return `${Math.floor(diff / 3600)} 小时前`;
  return `${Math.floor(diff / 86400)} 天前`;
}

function trendTimeLabel(h, i, n) {
  const ts = h && h[i] && h[i].ts;
  if (ts) {
    // 桶内 ts 为 UTC 整点；按浏览器本地时区呈现为正常时钟时间（HH:00）
    const d = new Date(ts * 1000);
    return String(d.getHours()).padStart(2, "0") + ":00";
  }
  const hoursAgo = n - 1 - i;
  return hoursAgo === 0 ? "现在" : "-" + hoursAgo + "h";
}

function renderTrendSvg(hours) {
  const container = document.getElementById("trendChart");
  if (!container) return;
  const h = Array.isArray(hours) ? hours : [];
  const counts = h.map((x) => x.count || 0);
  const redirects = h.map((x) => x.redirects || 0);
  const failed = h.map((x) => x.failed || 0);
  const streamed = h.map((x) => x.streamed || 0);
  const n = Math.max(counts.length, 1);
  const W = 680, H = 210, padL = 40, padR = 10, padT = 14, padB = 22;
  if (n < 2) {
    container.innerHTML = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:${H}px"><text x="${W / 2}" y="${H / 2 + 4}" fill="var(--text-3)" font-size="12" text-anchor="middle">暂无 24 小时趋势数据</text></svg>`;
    return;
  }
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;
  const stepX = innerW / (n - 1);
  const rawMax = Math.max(1, ...counts, ...redirects, ...failed, ...streamed);
  // 向上取整到 1/2/5×10^k 的整刻度，Y 轴标签更易读
  const pow = Math.pow(10, Math.floor(Math.log10(rawMax)));
  const base = rawMax / pow;
  const maxValue = (base <= 1 ? 1 : base <= 2 ? 2 : base <= 5 ? 5 : 10) * pow;
  // 钳制到 [0, maxValue]：无论数据如何异常，绘制永不越出绘图区/越过零点基线
  const yOf = (v) => padT + innerH - (Math.min(Math.max(v, 0), maxValue) / maxValue) * innerH;
  const fmtY = (v) => (v >= 1000 ? (v % 1000 === 0 ? v / 1000 + "k" : (v / 1000).toFixed(1) + "k") : String(v));
  const pathOf = (arr, closeArea) => {
    if (!arr.length) return { line: "", area: "" };
    let line = "";
    let area = closeArea ? `M ${padL} ${padT + innerH}` : "";
    arr.forEach((v, i) => {
      const x = padL + i * stepX;
      const y = yOf(v);
      line += `${i === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)} `;
      if (closeArea) area += ` L ${x.toFixed(1)} ${y.toFixed(1)}`;
    });
    if (closeArea) area += ` L ${(padL + (arr.length - 1) * stepX).toFixed(1)} ${padT + innerH} Z`;
    return { line, area };
  };
  const { line, area } = pathOf(counts, true);
  const lineRedirect = pathOf(redirects, false).line;
  const lineFailed = pathOf(failed, false).line;
  const lineStreamed = pathOf(streamed, false).line;
  let grid = "";
  for (let g = 1; g <= 3; g++) {
    const y = padT + (innerH * g) / 4;
    grid += `<line x1="${padL}" y1="${y.toFixed(1)}" x2="${W - padR}" y2="${y.toFixed(1)}" stroke="var(--border)" stroke-width="1" stroke-dasharray="3 4"/>`;
  }
  // Y 轴刻度（0 / 半程 / 最大）+ 零点实线基线：明确「水平线」就是 0，小数值不再像掉到线下
  const yLabels = [0, maxValue / 2, maxValue].map((v) => {
    const y = yOf(v);
    return `<text x="${padL - 6}" y="${(y + 3).toFixed(1)}" fill="var(--text-3)" font-size="10" text-anchor="end">${fmtY(v)}</text>`;
  }).join("");
  let labels = "";
  [0, 6, 12, 18, 23].forEach((i) => {
    if (i >= n) return;
    const x = padL + i * stepX;
    const anchor = i === 0 ? "start" : i === n - 1 ? "end" : "middle";
    labels += `<text x="${x.toFixed(1)}" y="${H - 6}" fill="var(--text-3)" font-size="10" text-anchor="${anchor}">${trendTimeLabel(h, i, n)}</text>`;
  });
  const lastX = padL + (n - 1) * stepX;
  const lastY = yOf(counts[n - 1] || 0);
  container.innerHTML = `
    <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:${H}px;display:block">
      <defs>
        <linearGradient id="ovTrendFill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--brand)" stop-opacity="0.22"/>
          <stop offset="100%" stop-color="var(--brand)" stop-opacity="0"/>
        </linearGradient>
      </defs>
      ${grid}
      ${yLabels}
      <line x1="${padL}" y1="${padT + innerH}" x2="${W - padR}" y2="${padT + innerH}" stroke="var(--border)" stroke-width="1"/>
      <path d="${area}" fill="url(#ovTrendFill)"/>
      <path d="${line}" fill="none" stroke="var(--brand)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
      <path d="${lineRedirect}" fill="none" stroke="var(--info)" stroke-width="1.5" stroke-dasharray="4 3" stroke-linejoin="round" stroke-linecap="round"/>
      <path d="${lineStreamed}" fill="none" stroke="var(--ok)" stroke-width="1.5" stroke-dasharray="6 3" stroke-linejoin="round" stroke-linecap="round"/>
      <path d="${lineFailed}" fill="none" stroke="var(--danger)" stroke-width="1.5" stroke-dasharray="2 3" stroke-linejoin="round" stroke-linecap="round"/>
      <circle cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="3.5" fill="var(--brand)" stroke="var(--surface)" stroke-width="1.5"/>
      <g id="trendHover" style="display:none">
        <line id="trendHoverLine" y1="${padT}" y2="${padT + innerH}" stroke="var(--text-3)" stroke-width="1" stroke-dasharray="2 3"/>
        <circle id="trendHoverCount" r="3.5" fill="var(--brand)" stroke="var(--surface)" stroke-width="1.5"/>
        <circle id="trendHoverRedirect" r="3" fill="var(--info)" stroke="var(--surface)" stroke-width="1.5"/>
        <circle id="trendHoverStream" r="3" fill="var(--ok)" stroke="var(--surface)" stroke-width="1.5"/>
        <circle id="trendHoverFailed" r="3" fill="var(--danger)" stroke="var(--surface)" stroke-width="1.5"/>
      </g>
      ${labels}
    </svg>
    <div class="trend-tip" style="display:none"></div>`;

  // 悬停十字线 + 数值气泡：鼠标位置反推最近的小时桶（viewBox 拉伸后需按实际宽换算）
  container.style.position = "relative";
  container._trendData = { h, counts, redirects, failed, streamed, n, stepX, padL, padT, innerH, W, maxValue };
  if (!container._trendBound) {
    container._trendBound = true;
    container.addEventListener("mousemove", (e) => {
      const d = container._trendData;
      if (!d || d.n < 2) return;
      const rect = container.getBoundingClientRect();
      const vx = (e.clientX - rect.left) * (d.W / rect.width);
      const idx = Math.min(d.n - 1, Math.max(0, Math.round((vx - d.padL) / d.stepX)));
      const x = d.padL + idx * d.stepX;
      const g = container.querySelector("#trendHover");
      if (g) {
        g.style.display = "";
        const set = (id, attr, val) => { const el = container.querySelector("#" + id); if (el) el.setAttribute(attr, val); };
        set("trendHoverLine", "x1", x); set("trendHoverLine", "x2", x);
        // 圆点 y 与主渲染同用 nice 化的 maxValue，保证落在曲线上
        const yOfLocal = (v) => d.padT + d.innerH - (Math.min(Math.max(v, 0), d.maxValue) / d.maxValue) * d.innerH;
        set("trendHoverCount", "cx", x); set("trendHoverCount", "cy", yOfLocal(d.counts[idx]));
        set("trendHoverRedirect", "cx", x); set("trendHoverRedirect", "cy", yOfLocal(d.redirects[idx]));
        set("trendHoverStream", "cx", x); set("trendHoverStream", "cy", yOfLocal(d.streamed[idx]));
        set("trendHoverFailed", "cx", x); set("trendHoverFailed", "cy", yOfLocal(d.failed[idx]));
      }
      const tip = container.querySelector(".trend-tip");
      if (tip) {
        tip.innerHTML = `<strong>${trendTimeLabel(d.h, idx, d.n)}</strong>`
          + `<span><i style="background:var(--brand)"></i>请求 ${d.counts[idx]}</span>`
          + `<span><i style="background:var(--info)"></i>302 跳转 ${d.redirects[idx]}</span>`
          + `<span><i style="background:var(--ok)"></i>本地代理 ${d.streamed[idx]}</span>`
          + `<span><i style="background:var(--danger)"></i>失败拦截 ${d.failed[idx]}</span>`;
        tip.style.display = "flex";
        const tipW = tip.offsetWidth || 180;
        const left = Math.min(Math.max(e.clientX - rect.left + 12, 4), rect.width - tipW - 4);
        tip.style.left = left + "px";
        tip.style.top = "6px";
      }
    });
    container.addEventListener("mouseleave", () => {
      const g = container.querySelector("#trendHover");
      if (g) g.style.display = "none";
      const tip = container.querySelector(".trend-tip");
      if (tip) tip.style.display = "none";
    });
  }
}

// ============ 网络吞吐（概览 KPI，2s 轮询差分速率由服务端计算） ============

function formatRate(bytesPerSec) {
  if (bytesPerSec == null || !Number.isFinite(bytesPerSec)) return null;
  if (bytesPerSec >= 1024 * 1024 * 1024) return (bytesPerSec / 1024 / 1024 / 1024).toFixed(2) + " GB/s";
  if (bytesPerSec >= 1024 * 1024) return (bytesPerSec / 1024 / 1024).toFixed(2) + " MB/s";
  if (bytesPerSec >= 1024) return (bytesPerSec / 1024).toFixed(1) + " KB/s";
  return Math.round(bytesPerSec) + " B/s";
}

function formatBytesTotal(bytes) {
  if (bytes == null || !Number.isFinite(bytes)) return null;
  if (bytes >= 1024 * 1024 * 1024) return (bytes / 1024 / 1024 / 1024).toFixed(2) + " GB";
  if (bytes >= 1024 * 1024) return (bytes / 1024 / 1024).toFixed(1) + " MB";
  if (bytes >= 1024) return (bytes / 1024).toFixed(1) + " KB";
  return bytes + " B";
}

export async function loadNetThroughput() {
  const valueEl = document.getElementById("kpiNet");
  const deltaEl = document.getElementById("kpiNetDelta");
  if (!valueEl) return;
  try {
    const d = await apiFetch("/_admin/api/net-throughput");
    if (!d || !d.ok) {
      valueEl.textContent = "—";
      if (deltaEl) deltaEl.textContent = "当前平台不支持网卡统计";
      return;
    }
    const recvText = formatRate(d.recv_rate);
    if (recvText == null) {
      valueEl.textContent = "…";
      if (deltaEl) deltaEl.textContent = "正在采样网络速率…";
      return;
    }
    // 主值取下行速率（代理场景下行为主），上行与累计收发进副标题
    valueEl.innerHTML = `<span style="color:var(--info);font-size:13px;font-weight:600">↓ </span>${esc(recvText)}`;
    if (deltaEl) {
      const sentText = formatRate(d.sent_rate) || "—";
      const recvTotal = formatBytesTotal(d.bytes_recv);
      deltaEl.textContent = `↑ ${sentText} · 累计收 ${recvTotal}`;
    }
  } catch (_) {
    valueEl.textContent = "—";
    if (deltaEl) deltaEl.textContent = "吞吐数据不可用";
  }
}

let _netAutoRefreshTimer = null;
export function stopNetAutoRefresh() {
  if (_netAutoRefreshTimer) { clearInterval(_netAutoRefreshTimer); _netAutoRefreshTimer = null; }
}
export function startNetAutoRefresh() {
  stopNetAutoRefresh();
  _netAutoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "overview") { stopNetAutoRefresh(); return; }
    loadNetThroughput().catch(() => {});
  }, 2000);
}

// 概览页其余数据（KPI/趋势/服务健康/拦截事件/待办风险）15s 定时刷新；
// 网络吞吐单独走 2s 快轮询，此处慢轮询避免频繁 SQL 聚合
let _ovAutoRefreshTimer = null;
export function stopOverviewAutoRefresh() {
  if (_ovAutoRefreshTimer) { clearInterval(_ovAutoRefreshTimer); _ovAutoRefreshTimer = null; }
}
export function startOverviewAutoRefresh() {
  stopOverviewAutoRefresh();
  _ovAutoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "overview") { stopOverviewAutoRefresh(); return; }
    renderOverview().catch(() => {});
  }, 15000);
}

export async function renderOverview() {
  loadNetThroughput().catch(() => {});
  const [stats, overviewResp, cacheResp] = await Promise.all([
    apiFetch("/_admin/api/stats").catch(() => null),
    apiFetch("/_admin/api/overview-stats").catch(() => null),
    apiFetch("/_admin/api/ip-cache/stats").catch(() => null),
  ]);

  const st = stats || {};
  const ov = overviewResp || {};

  // KPI：「今日请求」取聚合接口的今日计数（本地时区 0 点起），累计数进副标题
  const todayRequests = ov.requests_today ?? 0;
  const totalAll = ov.requests_total ?? st.total_requests ?? 0;

  // 302 跳转 / 本地代理 / 拦截：与「今日请求」同窗口（今日 0 点起），分项相加 ≤ 今日请求
  // （此前用 24h 滚动窗口 ⊃ 今日，出现「48+41 > 70」的口径错位；更早前用 ProxyStats
  // 内存计数器，重启归零造成「图表有数据、卡片显示 0」——两类问题同源于数据源不统一）
  const hoursArr = Array.isArray(ov.hours) ? ov.hours : [];
  const sumBucket = (key) => hoursArr.reduce((s, x) => s + (x[key] || 0), 0);
  const redirect = ov.redirects_today ?? ov.redirects_24h ?? sumBucket("redirects");
  const streamed = ov.streamed_today ?? ov.streamed_24h ?? sumBucket("streamed");
  const failed = ov.failed_today ?? ov.failed_24h ?? sumBucket("failed");

  setText("kpiTotal", todayRequests.toLocaleString("en-US"));
  const totalDelta = document.getElementById("kpiTotalDelta");
  if (totalDelta) totalDelta.textContent = `累计 ${totalAll.toLocaleString("en-US")} 次请求`;
  setText("kpiRedirect", redirect.toLocaleString("en-US"));
  setText("kpiStream", streamed.toLocaleString("en-US"));
  setText("kpiFailed", failed.toLocaleString("en-US"));

  // 平均延迟：由 /_admin/api/overview-stats 对近 24h 全量日志 SQL 聚合
  //（此前前端取最近 500 条日志自行均值，覆盖不足且 created_at 为 ISO 字符串无法分桶）
  const avgLatency = ov.avg_latency_ms ?? null;
  const latencyEl = document.getElementById("kpiLatency");
  if (latencyEl) {
    latencyEl.innerHTML = avgLatency == null
      ? "—"
      : `${avgLatency.toFixed(0)}<small style="font-size:13px;font-weight:500;color:var(--text-3)"> ms</small>`;
  }
  const latencyDelta = document.getElementById("kpiLatencyDelta");
  if (latencyDelta) {
    latencyDelta.textContent = avgLatency == null
      ? "近 24 小时暂无请求样本"
      : `近 24 小时 · ${ov.latency_sample_count ?? 0} 个样本`;
  }

  // 趋势三序列（总请求 / 302 跳转 / 失败拦截），24 个整点桶（含 UTC ts，供 x 轴呈现本地时钟时间）
  const hours = Array.isArray(ov.hours) ? ov.hours : [];
  renderTrendSvg(hours);

  // 服务健康
  const healthBody = document.getElementById("healthBody");
  const healthPill = document.getElementById("healthPill");
  if (healthBody) {
    const uptime = formatDuration(st.uptime_seconds || 0);
    const cacheHit = cacheResp ? (cacheResp.hit_rate || "0%") : "—";
    const bans = (state.bannedIps || []).length;
    const geo = state.geoSources || [];
    const geoEnabled = geo.filter((g) => g && g.enabled).length;
    const hitNum = cacheResp ? parseFloat(cacheResp.hit_rate) : 100;
    const rows = [
      { label: "运行时长", val: uptime, status: "ok" },
      { label: "缓存命中率", val: cacheHit, status: hitNum < 50 ? "warn" : "ok" },
      { label: "封禁 IP 数", val: String(bans), status: bans > 0 ? "warn" : "ok" },
      { label: "在线定位源", val: `${geoEnabled}/${geo.length}`, status: geoEnabled > 0 ? "ok" : "bad" },
    ];
    healthBody.innerHTML = rows.map((r) => `
      <div class="kv"><div class="k">${esc(r.label)}</div><div class="val">${esc(r.val)}</div></div>`).join("");
    if (healthPill) {
      const bad = rows.some((r) => r.status === "bad");
      const warn = rows.some((r) => r.status === "warn");
      healthPill.className = "pill " + (bad ? "pill-danger" : warn ? "pill-warn" : "pill-ok");
      healthPill.textContent = bad ? "异常" : warn ? "注意" : "正常";
    }
  }

  // 近期拦截事件
  const blocksBody = document.getElementById("blocksBody");
  if (blocksBody) {
    const bans = (state.bannedIps || []).slice()
      .sort((a, b) => (b.banned_at || 0) - (a.banned_at || 0)).slice(0, 5);
    if (!bans.length) {
      blocksBody.innerHTML = `<li class="empty" style="padding:20px 0">当前没有已封禁的 IP。</li>`;
    } else {
      blocksBody.innerHTML = bans.map((b) => {
        const exp = b.permanent ? "永久" : (b.expire_at ? `至 ${formatDateTime(new Date(b.expire_at * 1000).toISOString())}` : "临时");
        return `<li>
          <div class="src-icon" style="background:var(--danger-soft);color:var(--danger-text)">禁</div>
          <div style="min-width:0">
            <div><strong>${esc(b.ip)}</strong></div>
            <div class="hint">${relativeTime(b.banned_at)} · ${esc(exp)}</div>
            <div class="hint">${esc(b.reason || "未填写原因")}</div>
          </div>
        </li>`;
      }).join("");
    }
  }

  // 待办与风险
  const risksBody = document.getElementById("risksBody");
  if (risksBody) {
    const nowSec = Date.now() / 1000;
    const risks = [];
    const bans = state.bannedIps || [];
    const expiring = bans.filter((b) => !b.permanent && b.expire_at && (b.expire_at - nowSec) < 86400 && (b.expire_at - nowSec) > 0);
    if (expiring.length) risks.push({ level: "warn", text: `<strong>${expiring.length}</strong> 个临时封禁将在 24 小时内到期。` });
    const hitNum = cacheResp ? parseFloat(cacheResp.hit_rate) : 100;
    if (cacheResp && hitNum < 50) risks.push({ level: "warn", text: `请求结果缓存命中率偏低（${cacheResp.hit_rate}）。` });
    if (!state.routeGroups || !state.routeGroups.length) risks.push({ level: "warn", text: `尚未配置任何<strong>路由组</strong>，代理不会转发任何请求。` });
    else if (!state.rules || !state.rules.length) risks.push({ level: "warn", text: `路由组已配置，但<strong>转发规则为空</strong>，请补充规则。` });
    const geo = state.geoSources || [];
    if (!geo.length) risks.push({ level: "warn", text: `未配置任何<strong>在线定位源</strong>，离线库将作为唯一回退。` });
    if (!risks.length) risks.push({ level: "ok", text: `当前未发现显著风险，系统运行正常。` });
    risksBody.innerHTML = risks.map((r) => {
      const ic = r.level === "ok"
        ? `<span class="pill pill-ok">✓</span>`
        : `<span class="pill ${r.level === "warn" ? "pill-warn" : "pill-danger"}">!</span>`;
      return `<li style="align-items:center">${ic}<div style="min-width:0">${r.text}</div></li>`;
    }).join("");
  }
}

// ============ 路由组 ============

export function getRulesForGroup(pathPrefix, requestHost = "") {
  const normalizedHost = normalizeRequestHost(requestHost);
  return state.rules
    .filter((r) => r.path_prefix === pathPrefix && normalizeRequestHost(r.request_host) === normalizedHost)
    .sort((a, b) => (Number(b.is_default) - Number(a.is_default)) || (b.priority - a.priority) || ((a.id || 0) - (b.id || 0)));
}

export function renderRouteGroups(groups) {
  state.routeGroups = groups || [];
  setText("navCountRouting", String(state.routeGroups.length));
  const tbody = document.getElementById("groupBody");
  setText("groupCountPill", `${state.routeGroups.length} 个`);
  if (!tbody) return;
  tbody.innerHTML = "";
  if (!state.routeGroups.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty" style="padding:26px 0">还没有路由组，点击右上角「新建路由组」创建。</td></tr>`;
    return;
  }
  state.routeGroups.forEach((group) => {
    const hostLabel = formatRequestHostLabel(normalizeRequestHost(group.request_host));
    const rules = getRulesForGroup(group.path_prefix, group.request_host);
    const defaultCount = rules.filter((r) => r.is_default).length;
    const enabledCount = rules.filter((r) => r.enabled).length;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><code class="mono">${esc(group.path_prefix)}</code></td>
      <td>${esc(hostLabel)}</td>
      <td>
        <div class="switch ${group.region_matching_enabled ? "on" : ""}" data-action="toggle-group-region" data-path-prefix="${esc(group.path_prefix)}" data-request-host="${esc(normalizeRequestHost(group.request_host))}" role="switch" title="地区匹配开关"></div>
      </td>
      <td>${defaultCount ? `<span class="pill pill-brand">${defaultCount} 默认</span>` : '<span class="text-muted">—</span>'}</td>
      <td>${enabledCount}/${rules.length} 启用</td>
      <td>
        <div style="display:flex;gap:6px;justify-content:flex-end;flex-wrap:wrap">
          <button class="btn btn-sm" data-action="create-rule-for-group" data-path-prefix="${esc(group.path_prefix)}" data-request-host="${esc(normalizeRequestHost(group.request_host))}">新增规则</button>
          <button class="btn btn-sm" data-action="edit-group" data-path-prefix="${esc(group.path_prefix)}" data-request-host="${esc(normalizeRequestHost(group.request_host))}">编辑</button>
          <button class="btn btn-sm btn-danger" data-action="delete-group" data-path-prefix="${esc(group.path_prefix)}" data-request-host="${esc(normalizeRequestHost(group.request_host))}">删除</button>
        </div>
      </td>`;
    tbody.appendChild(tr);
  });
}

const GROUP_SCHEMA = [
  { key: "path_prefix", label: "路径前缀", type: "text", required: true, placeholder: "/play" },
  { key: "request_host", label: "请求主机（域名）", type: "text", placeholder: "example.com（留空匹配所有）" },
  { key: "access_ip_whitelist", label: "访问控制 IP 白名单", type: "text", placeholder: "1.2.3.4, 5.6.7.0/24" },
  { key: "ip_blacklist", label: "访问控制 IP 黑名单", type: "text" },
  { key: "region_whitelist", label: "地区白名单", type: "text", placeholder: "CN, HK" },
  { key: "region_blacklist", label: "地区黑名单", type: "text" },
  { key: "notes", label: "备注", type: "text" },
  { key: "region_matching_enabled", label: "地区匹配", type: "switch", hint: "该前缀下所有规则按地区过滤命中" },
  // 组级规则默认（P2-4.1）：留空/选「不设置」= 组未设默认，规则「继承组」时回落全局默认
  { key: "rd_timeout", label: "默认超时（秒）", type: "number", placeholder: "不设置", group: "组级默认配置（规则可继承）" },
  { key: "rd_max_redirects", label: "默认最大重定向", type: "number", placeholder: "不设置", group: "组级默认配置（规则可继承）" },
  { key: "rd_retry_times", label: "默认重试次数", type: "number", placeholder: "不设置", group: "组级默认配置（规则可继承）" },
  { key: "rd_follow_redirects", label: "默认跟随重定向", type: "seg", options: [
    { value: "", label: "不设置" },
    { value: "1", label: "开" },
    { value: "0", label: "关" },
  ], group: "组级默认配置（规则可继承）" },
  { key: "rd_enable_streaming", label: "默认流式转发", type: "seg", options: [
    { value: "", label: "不设置" },
    { value: "1", label: "开" },
    { value: "0", label: "关" },
  ], group: "组级默认配置（规则可继承）" },
  { key: "rd_strip_prefix", label: "默认去前缀", type: "seg", options: [
    { value: "", label: "不设置" },
    { value: "1", label: "开" },
    { value: "0", label: "关" },
  ], group: "组级默认配置（规则可继承）" },
  { key: "rd_referer_whitelist", label: "默认 Referer 白名单", type: "text", placeholder: "不设置（如 *.example.com）", group: "组级默认配置（规则可继承）" },
  { key: "rd_referer_policy", label: "默认空 Referer 策略", type: "select", options: [
    { value: "", label: "不设置" },
    { value: "allow", label: "允许（本地播放器/直链）" },
    { value: "deny", label: "拒绝（仅白名单网页引用）" },
  ], group: "组级默认配置（规则可继承）" },
  { key: "rd_ua_blacklist", label: "默认 UA 黑名单", type: "text", placeholder: "不设置（如 curl, python-requests）", group: "组级默认配置（规则可继承）" },
  { key: "rd_ua_whitelist", label: "默认 UA 白名单", type: "text", placeholder: "不设置（如 NASKTV, ExoPlayer）", group: "组级默认配置（规则可继承）" },
  { key: "rd_note", type: "note", text: "以上默认值不直接生效：规则中对应字段选择「继承组默认」时才取这里的值。保存后可把组内与默认相同的显式配置一键转为继承。", group: "组级默认配置（规则可继承）" },
];

// 后台/系统保留路径：这些路由由后台控制台或系统内置接口直接处理，不经过代理兜底，
// 若被设置为路由组路径前缀将永远无法命中，故在创建/编辑时拦截。
const RESERVED_BACKEND_PATHS = ["/_admin", "/_health", "/json/version", "/_block"];

function matchReservedPath(pathPrefix) {
  let p = String(pathPrefix || "").trim();
  if (!p) return null;
  if (!p.startsWith("/")) p = "/" + p;
  p = p.replace(/\/+$/, "") || "/";
  return RESERVED_BACKEND_PATHS.find((res) => p === res || p.startsWith(res + "/") || res.startsWith(p + "/")) || null;
}

export function openRouteGroupModal(group) {
  const isEdit = Boolean(group);
  const rd = (group && group.rule_defaults) || {};
  const rdStr = (k) => (rd[k] == null ? "" : String(rd[k]));
  const rdBool = (k) => (rd[k] === true ? "1" : rd[k] === false ? "0" : "");
  const values = group ? {
    path_prefix: group.path_prefix,
    request_host: normalizeRequestHost(group.request_host),
    access_ip_whitelist: group.access_ip_whitelist || "",
    ip_blacklist: group.ip_blacklist || "",
    region_whitelist: group.region_whitelist || "",
    region_blacklist: group.region_blacklist || "",
    notes: group.notes || "",
    region_matching_enabled: Boolean(group.region_matching_enabled),
    // 组级默认（P2-4.1）
    rd_timeout: rdStr("timeout"), rd_max_redirects: rdStr("max_redirects"), rd_retry_times: rdStr("retry_times"),
    rd_follow_redirects: rdBool("follow_redirects"), rd_enable_streaming: rdBool("enable_streaming"), rd_strip_prefix: rdBool("strip_prefix"),
    rd_referer_whitelist: rdStr("referer_whitelist"), rd_referer_policy: rdStr("referer_policy"),
    rd_ua_blacklist: rdStr("ua_blacklist"), rd_ua_whitelist: rdStr("ua_whitelist"),
  } : { region_matching_enabled: true };

  openFormModal({
    title: isEdit ? `编辑路径前缀 ${group.path_prefix}` : "新建路由组",
    sub: isEdit ? `域名 ${formatRequestHostLabel(normalizeRequestHost(group.request_host))}` : "创建一个路径前缀，再为其添加转发规则",
    schema: GROUP_SCHEMA,
    values,
    validate: (out) => {
      const pp = String(out.path_prefix || "").trim();
      if (!pp) return "路径前缀不能为空";
      const reserved = matchReservedPath(pp);
      if (reserved) return `路径前缀不能使用后台保留路径「${reserved}」，该路径由系统内部占用，转发规则不会生效。`;
      for (const k of ["rd_timeout", "rd_max_redirects", "rd_retry_times"]) {
        const v = String(out[k] ?? "").trim();
        if (v !== "" && (!Number.isFinite(Number(v)) || Number(v) <= 0)) return "组级默认中的数值需为正整数（留空表示不设置）";
      }
      return null;
    },
    onSave: async (out) => {
      // 组级默认收集：留空/「不设置」的键不出现在 rule_defaults 里
      const ruleDefaults = {};
      const rdNum = (k) => { const v = String(out[k] ?? "").trim(); if (v !== "") ruleDefaults[k] = Number(v); };
      const rdBool = (k) => { if (out[k] !== "") ruleDefaults[k] = out[k] === "1"; };
      const rdText = (k) => { const v = String(out[k] ?? "").trim(); if (v !== "") ruleDefaults[k] = v; };
      rdNum("timeout"); rdNum("max_redirects"); rdNum("retry_times");
      rdBool("follow_redirects"); rdBool("enable_streaming"); rdBool("strip_prefix");
      rdText("referer_whitelist"); rdText("ua_blacklist"); rdText("ua_whitelist");
      if (out.rd_referer_policy) ruleDefaults.referer_policy = out.rd_referer_policy;

      const payload = {
        old_path_prefix: isEdit ? group.path_prefix : "",
        old_request_host: isEdit ? normalizeRequestHost(group.request_host) : "",
        path_prefix: String(out.path_prefix || "").trim(),
        request_host: normalizeRequestHost(out.request_host),
        access_ip_whitelist: out.access_ip_whitelist || "",
        ip_blacklist: out.ip_blacklist || "",
        region_whitelist: out.region_whitelist || "",
        region_blacklist: out.region_blacklist || "",
        notes: out.notes || "",
        region_matching_enabled: Boolean(out.region_matching_enabled),
        rule_defaults: ruleDefaults,
      };
      if (isEdit) {
        await apiFetch("/_admin/api/route-groups", { method: "PUT", body: JSON.stringify(payload) });
        showToast("路径前缀已更新。");
      } else {
        await apiFetch("/_admin/api/route-groups", { method: "POST", body: JSON.stringify(payload) });
        showToast("路径前缀已创建。");
      }
      // 存量迁移（用户拍板：切换时弹窗询问）：统计组内显式值 == 组默认 的规则，询问是否转继承
      if (isEdit && Object.keys(ruleDefaults).length) {
        const matches = countRulesMatchingGroupDefaults(group, ruleDefaults);
        if (matches > 0) {
          openConfirm({
            title: "转为继承组默认",
            danger: false,
            message: `检测到组内 <strong>${matches}</strong> 条规则的显式配置与组级默认完全相同。<br>是否把这些字段转为「继承组默认」？转换后调整组默认即可对它们整体生效。`,
            onOk: async () => {
              try {
                const res = await apiFetch("/_admin/api/route-groups/inherit-convert", {
                  method: "POST",
                  body: JSON.stringify({ path_prefix: payload.path_prefix, request_host: payload.request_host }),
                });
                showToast(`已转换 ${res.converted_rules || 0} 条规则（${res.converted_fields || 0} 个字段）。`);
              } catch (e) { showToast(e.message, true); }
              await loadDashboard();
            },
          });
        }
      }
      await loadDashboard();
    },
  });
}

// 统计组内「未继承 且 显式值 == 组默认」的规则数（前端预估，后端 inherit-convert 会再精确过滤）
function countRulesMatchingGroupDefaults(group, ruleDefaults) {
  const normalizedHost = normalizeRequestHost(group.request_host);
  const keys = Object.keys(ruleDefaults || {});
  if (!keys.length) return 0;
  return (state.rules || []).filter((r) => {
    if (r.path_prefix !== group.path_prefix || normalizeRequestHost(r.request_host) !== normalizedHost) return false;
    const inherit = new Set(r.inherit_fields || []);
    return keys.some((k) => {
      if (inherit.has(k)) return false;
      const dv = ruleDefaults[k];
      if (typeof dv === "boolean") return Boolean(r[k]) === dv;
      if (typeof dv === "number") return Number(r[k]) === dv;
      return String(r[k] || "").trim() === String(dv).trim();
    });
  }).length;
}

export async function updateGroupRegionSwitch(pathPrefix, requestHost, enabled) {
  const group = findRouteGroup(pathPrefix, requestHost, state);
  if (!group) throw new Error(`未找到路径前缀 ${pathPrefix}`);
  await apiFetch("/_admin/api/route-groups", {
    method: "PUT",
    body: JSON.stringify({
      old_path_prefix: group.path_prefix,
      old_request_host: normalizeRequestHost(group.request_host),
      path_prefix: group.path_prefix,
      request_host: normalizeRequestHost(group.request_host),
      notes: group.notes || "",
      region_matching_enabled: enabled,
    }),
  });
}

// ============ 规则 ============

export function populateRuleHostFilter(rules) {
  const select = document.getElementById("ruleHost");
  if (!select) return;
  const hosts = [];
  (rules || []).forEach((r) => {
    const h = normalizeRequestHost(r.request_host);
    if (h && !hosts.includes(h)) hosts.push(h);
  });
  hosts.sort();
  const current = select.value;
  select.innerHTML = '<option value="">全部主机</option>' +
    hosts.map((h) => `<option value="${esc(h)}">${esc(formatRequestHostLabel(h))}</option>`).join("");
  if (current && hosts.includes(current)) select.value = current;
  syncSelect(select);
}

export function populateRuleGroupFilter() {
  const select = document.getElementById("ruleGroup");
  if (!select) return;
  const options = buildRuleGroupOptions(state.routeGroups);
  const current = select.value;
  select.innerHTML = '<option value="">全部路由组</option>' +
    options.map((o) => `<option value="${esc(o.path_prefix)}|${esc(o.request_host)}">${esc(o.label)}</option>`).join("");
  if (current && options.some((o) => `${o.path_prefix}|${o.request_host}` === current)) select.value = current;
  syncSelect(select);
}

// 多上游 target_urls（JSON 数组字符串）↔ 多行文本互转（P1-2.2）
function ruleUrlsToLines(raw) {
  if (!raw) return "";
  try {
    const arr = typeof raw === "string" ? JSON.parse(raw) : raw;
    if (Array.isArray(arr)) return arr.map((u) => String(u || "").trim()).filter(Boolean).join("\n");
  } catch (_) {}
  return "";
}
function linesToRuleUrls(text) {
  const list = String(text || "").split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  return list.length ? JSON.stringify(list) : "";
}
// 健康徽标：返回各上游小圆点（绿=健康 / 红=不健康）
function healthBadges(health) {
  if (!health || typeof health !== "object") return "";
  const entries = Object.entries(health);
  if (!entries.length) return "";
  const dots = entries.map(([target, ok]) => {
    const color = ok ? "#3fb950" : "#f85149";
    const label = ok ? "健康" : "不健康";
    return `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${color};margin:0 2px;vertical-align:middle" title="${esc(target)} · ${label}"></span>`;
  }).join("");
  return `<span style="margin-left:6px;white-space:nowrap">${dots}</span>`;
}

export function renderRules(rules) {
  state.rules = rules || [];
  populateRuleHostFilter(state.rules);
  populateRuleGroupFilter();
  const tbody = document.getElementById("rulesBody");
  if (!tbody) return;
  tbody.innerHTML = "";

  const keyword = String(getValue("ruleSearch") || "").trim().toLowerCase();
  const status = getValue("ruleFilter") || "";
  const host = getValue("ruleHost") || "";
  const groupValue = getValue("ruleGroup") || "";
  let groupPath = "";
  let groupHost = "";
  if (groupValue) {
    const idx = groupValue.indexOf("|");
    groupPath = idx >= 0 ? groupValue.slice(0, idx) : groupValue;
    groupHost = idx >= 0 ? groupValue.slice(idx + 1) : "";
  }

  const filtered = state.rules.filter((rule) => {
    const requestHost = normalizeRequestHost(rule.request_host);
    const hostLabel = formatRequestHostLabel(requestHost);
    if (keyword) {
      const haystack = [
        rule.name, rule.path_prefix, rule.target_url, rule.notes,
        rule.region_filters, rule.ip_whitelist, rule.access_ip_whitelist,
        rule.path_rewrite_pattern, rule.path_rewrite_replacement, hostLabel,
      ].map((v) => String(v || "")).join(" ").toLowerCase();
      if (!haystack.includes(keyword)) return false;
    }
    if (status === "enabled" && !rule.enabled) return false;
    if (status === "disabled" && rule.enabled) return false;
    if (status === "default" && !rule.is_default) return false;
    if (host && host !== requestHost) return false;
    if (groupValue) {
      if (rule.path_prefix !== groupPath) return false;
      if (groupHost && requestHost !== groupHost) return false;
    }
    return true;
  });

  const summaryEl = document.getElementById("rulesSummary");
  if (summaryEl) {
    const total = state.rules.length;
    summaryEl.textContent = (keyword || status || host || groupValue)
      ? `共 ${total} 条规则，当前匹配 ${filtered.length} 条`
      : `共 ${total} 条转发规则`;
  }

  if (!filtered.length) {
    tbody.innerHTML = `<tr><td colspan="10" class="empty" style="padding:26px 0">${state.rules.length ? "没有匹配当前查询条件的规则。" : "暂无转发规则，请先新增规则。"}</td></tr>`;
    return;
  }

  filtered.forEach((rule) => {
    const requestHost = normalizeRequestHost(rule.request_host);
    const hostLabel = formatRequestHostLabel(requestHost);
    const rewritePattern = rule.path_rewrite_pattern || "";
    const hasRewrite = Boolean(rewritePattern);
    const rewriteCell = hasRewrite
      ? `<code class="mono" title="${esc(rewritePattern)}">${esc(rewritePattern)}</code>`
      : '<span class="text-muted">—</span>';
    const regionText = rule.region_filters || "";
    const tr = document.createElement("tr");
    tr.dataset.ruleId = rule.id;
    tr.className = "clickable";
    tr.innerHTML = `
      <td class="mono">${rule.id}</td>
      <td><strong>${esc(rule.name || "(未命名规则)")}</strong><div class="hint">${esc(rule.path_prefix)} · ${esc(hostLabel)}</div></td>
      <td class="cell-truncate" title="${esc(rule.target_url)}">${esc(rule.target_url)}${healthBadges(rule.health)}</td>
      <td class="cell-truncate">${rewriteCell}</td>
      <td class="cell-truncate" title="${esc(regionText)}">${regionText ? esc(regionText) : '<span class="text-muted">默认</span>'}</td>
      <td>${rule.priority ?? 0}</td>
      <td>${rule.enable_streaming ? '<span class="pill pill-ok">开</span>' : '<span class="pill pill-neutral">关</span>'}</td>
      <td><div class="switch ${rule.enabled ? "on" : ""}" data-action="toggle-rule" data-id="${rule.id}" role="switch" title="点击切换启用状态"></div></td>
      <td>${rule.is_default ? '<span class="pill pill-brand">默认</span>' : '<span class="text-muted">—</span>'}</td>
      <td>
        <div style="display:flex;gap:6px;justify-content:flex-end;flex-wrap:wrap">
          <button class="btn btn-sm" data-action="edit-rule" data-id="${rule.id}">编辑</button>
          <button class="btn btn-sm btn-danger" data-action="delete-rule" data-id="${rule.id}">删除</button>
        </div>
      </td>`;
    tbody.appendChild(tr);
  });
}

// 规则表单 schema：group 字段收进「高级选项」折叠组（components.openFormModal 渲染），
// 明面只留核心四项（路由组/名称/目标/启用），简化主配置路径
const RULE_SCHEMA = [
  { key: "name", label: "规则名称", type: "text", required: true, placeholder: "示例流媒体" },
  { key: "target_url", label: "目标地址（主）", type: "text", required: true, placeholder: "https://target.example.com" },
  { key: "target_urls", label: "额外上游（故障转移）", type: "textarea", placeholder: "https://cdn2.example.com\nhttps://cdn3.example.com\n（每行一个；留空则仅用上方主目标）", group: "故障转移与健康探针" },
  { key: "health_check_enabled", label: "启用健康探针", type: "switch", hint: "周期探测各上游，故障自动从可用列表剔除", group: "故障转移与健康探针" },
  { key: "health_check_path", label: "探针路径", type: "text", placeholder: "/health（留空则 TCP 建连探测）", group: "故障转移与健康探针" },
  { key: "health_check_interval", label: "探针间隔（秒）", type: "number", default: 30, group: "故障转移与健康探针" },
  { key: "health_check_timeout", label: "探针超时（秒）", type: "number", default: 5, group: "故障转移与健康探针" },
  { key: "priority", label: "优先级", type: "number", default: 0, group: "转发行为与重写", groupOpen: true },
  // —— 组级继承三态（P2-4.1）：数值字段用「模式 select + dependsOn 数值输入」，开关字段直接三态 select ——
  { key: "timeout_mode", label: "超时", type: "seg", options: [
    { value: "inherit", label: "继承组默认" },
    { value: "custom", label: "自定义" },
  ], group: "转发行为与重写" },
  { key: "timeout", label: "超时值（秒）", type: "number", default: 30, dependsOn: { field: "timeout_mode", value: "custom" }, group: "转发行为与重写" },
  { key: "max_redirects_mode", label: "最大重定向", type: "seg", options: [
    { value: "inherit", label: "继承组默认" },
    { value: "custom", label: "自定义" },
  ], group: "转发行为与重写" },
  { key: "max_redirects", label: "最大重定向值", type: "number", default: 10, dependsOn: { field: "max_redirects_mode", value: "custom" }, group: "转发行为与重写" },
  { key: "retry_times_mode", label: "重试次数", type: "seg", options: [
    { value: "inherit", label: "继承组默认" },
    { value: "custom", label: "自定义" },
  ], group: "转发行为与重写" },
  { key: "retry_times", label: "重试次数值", type: "number", default: 3, dependsOn: { field: "retry_times_mode", value: "custom" }, group: "转发行为与重写" },
  { key: "strip_prefix", label: "去前缀", type: "seg", options: [
    { value: "inherit", label: "继承组默认" },
    { value: "1", label: "开" },
    { value: "0", label: "关" },
  ], group: "转发行为与重写" },
  { key: "follow_redirects", label: "跟随重定向", type: "seg", options: [
    { value: "inherit", label: "继承组默认" },
    { value: "1", label: "开" },
    { value: "0", label: "关" },
  ], group: "转发行为与重写" },
  { key: "enable_streaming", label: "流式转发", type: "seg", options: [
    { value: "inherit", label: "继承组默认" },
    { value: "1", label: "开" },
    { value: "0", label: "关" },
  ], group: "转发行为与重写" },
  { key: "is_default", label: "默认规则", type: "switch", group: "转发行为与重写" },
  { key: "path_rewrite_pattern", label: "正则重写·匹配", type: "text", placeholder: "^(.*)$", group: "转发行为与重写" },
  { key: "path_rewrite_replacement", label: "正则重写·替换", type: "text", placeholder: "/new$1", group: "转发行为与重写" },
  { key: "ip_whitelist", label: "IP 白名单（路由）", type: "text", hint: "选路维度，不是拦截：命中的客户端 IP 优先由本规则服务（支持 CIDR，如 10.0.0.0/8）；未命中走其他规则。要拒绝访问请用下面的「访问控制」两项", group: "访问控制" },
  { key: "region_filters", label: "地区条件", type: "text", placeholder: "CN,HK", hint: "选路维度：客户端 IP 属地命中才优先选中本规则（不同地区可定向不同上游）；未命中走默认/其他规则", group: "访问控制" },
  { key: "access_ip_whitelist", label: "访问控制 IP 白", type: "text", hint: "拒绝层：配置后名单外的 IP 一律 403（组级先于规则级生效）；留空不启用", group: "访问控制" },
  { key: "ip_blacklist", label: "访问控制 IP 黑", type: "text", hint: "拒绝层：名单内 IP 一律 403，无论是否命中其他条件；留空不启用", group: "访问控制" },
  { key: "region_whitelist", label: "地区白名单", type: "text", group: "访问控制" },
  { key: "region_blacklist", label: "地区黑名单", type: "text", group: "访问控制" },
  { key: "referer_whitelist", label: "Referer 白名单", type: "text", placeholder: "*.dwzynj.top, example.com", hint: "留空=继承组默认（组也未配置=不校验）；填 - = 组有默认时对本规则强制停用", group: "访问控制" },
  { key: "referer_policy", label: "空 Referer 策略", type: "select", options: [
    { value: "", label: "继承组默认" },
    { value: "allow", label: "允许（本地播放器/直链）" },
    { value: "deny", label: "拒绝（仅白名单网页引用）" },
  ], group: "访问控制" },
  { key: "ua_blacklist", label: "UA 黑名单", type: "text", placeholder: "curl, python-requests（子串匹配）", hint: "留空=继承组默认（组也未配置=不校验）；填 - = 组有默认时对本规则强制停用", group: "访问控制" },
  { key: "ua_whitelist", label: "UA 白名单", type: "text", placeholder: "NASKTV, ExoPlayer（子串匹配；启用后无 UA 也拦截）", hint: "留空=继承组默认（组也未配置=不校验）；填 - = 组有默认时对本规则强制停用", group: "访问控制" },
  { key: "cors_origins", label: "CORS 来源覆盖", type: "text", placeholder: "https://app.example.com（留空继承全局 CORS 配置）", group: "上游 TLS 与请求注入" },
  { key: "inject_request_headers", label: "自定义请求头（JSON）", type: "textarea", placeholder: '{"X-Custom-Auth": "token123"}', hint: "JSON 对象；转发前注入，同名头覆盖", group: "上游 TLS 与请求注入" },
  { key: "upstream_verify_ssl", label: "上游 TLS 校验", type: "select", default: -1, options: [
    { value: -1, label: "继承全局配置" },
    { value: 1, label: "强制校验证书" },
    { value: 0, label: "关闭校验（不安全）" },
  ], group: "上游 TLS 与请求注入" },
  { key: "client_cert", label: "mTLS 客户端证书（PEM 路径）", type: "text", placeholder: "/path/client.crt（留空不启用）", group: "上游 TLS 与请求注入" },
  { key: "client_key", label: "mTLS 私钥（PEM 路径）", type: "text", placeholder: "/path/client.key", group: "上游 TLS 与请求注入" },
  { key: "notes", label: "备注", type: "text", group: "其他" },
  { key: "enabled", label: "启用", type: "switch" },
];

function buildRuleGroupOptions(groups) {
  return (groups || []).map((g) => {
    const host = normalizeRequestHost(g.request_host);
    return {
      path_prefix: g.path_prefix,
      request_host: host,
      label: host ? `${g.path_prefix} · ${formatRequestHostLabel(host)}` : `${g.path_prefix} · 全部主机`,
    };
  });
}

export function openRuleModal(rule, presetGroup = null) {
  const isEdit = Boolean(rule);
  const groupOptions = buildRuleGroupOptions(state.routeGroups);
  if (!groupOptions.length) {
    showToast("请先创建路由组，再为其添加转发规则。", true);
    return;
  }

  // 确定默认选中的路由组
  let selectedIndex = 0;
  if (isEdit) {
    const idx = groupOptions.findIndex((o) => o.path_prefix === rule.path_prefix && o.request_host === normalizeRequestHost(rule.request_host));
    if (idx >= 0) selectedIndex = idx;
  } else if (presetGroup) {
    const idx = groupOptions.findIndex((o) => o.path_prefix === presetGroup.path_prefix && o.request_host === normalizeRequestHost(presetGroup.request_host));
    if (idx >= 0) selectedIndex = idx;
  }

  const schema = [
    { key: "route_group", label: "所属路由组", type: "select", required: true, options: groupOptions.map((o, i) => ({ value: String(i), label: o.label })) },
    ...RULE_SCHEMA,
  ];

  // 组级继承三态：编辑时按规则 inherit_fields 还原；新建默认全部「继承组默认」，
  // 组未设默认的字段在运行时回落全局/内建默认（与旧行为一致）
  const inherits = new Set((rule && rule.inherit_fields) || []);
  const tri = (key, explicit) => (inherits.has(key) ? "inherit" : explicit);

  const values = rule ? {
    route_group: String(selectedIndex),
    name: rule.name, target_url: rule.target_url, priority: rule.priority ?? 0,
    timeout_mode: tri("timeout", "custom"), timeout: rule.timeout ?? 30,
    max_redirects_mode: tri("max_redirects", "custom"), max_redirects: rule.max_redirects ?? 10,
    retry_times_mode: tri("retry_times", "custom"), retry_times: rule.retry_times ?? 3,
    strip_prefix: tri("strip_prefix", rule.strip_prefix ? "1" : "0"),
    follow_redirects: tri("follow_redirects", rule.follow_redirects !== false ? "1" : "0"),
    enable_streaming: tri("enable_streaming", rule.enable_streaming ? "1" : "0"),
    path_rewrite_pattern: rule.path_rewrite_pattern || "", path_rewrite_replacement: rule.path_rewrite_replacement || "",
    ip_whitelist: rule.ip_whitelist || "", region_filters: rule.region_filters || "",
    access_ip_whitelist: rule.access_ip_whitelist || "", ip_blacklist: rule.ip_blacklist || "",
    region_whitelist: rule.region_whitelist || "", region_blacklist: rule.region_blacklist || "",
    // 字符串继承哨兵是空串：继承中的字段显示为空（保存空串 = 仍继承），
    // 需要强制停用填 "-"；显式值原样回显
    referer_whitelist: inherits.has("referer_whitelist") ? "" : (rule.referer_whitelist || ""),
    referer_policy: tri("referer_policy", rule.referer_policy || "allow"),
    ua_blacklist: inherits.has("ua_blacklist") ? "" : (rule.ua_blacklist || ""),
    ua_whitelist: inherits.has("ua_whitelist") ? "" : (rule.ua_whitelist || ""),
    // 多上游 + 健康检查（P1-2.2）：target_urls(JSON 数组) ↔ 多行文本互转
    target_urls: ruleUrlsToLines(rule.target_urls),
    health_check_enabled: Boolean(rule.health_check_enabled),
    health_check_path: rule.health_check_path || "",
    health_check_interval: rule.health_check_interval ?? 30,
    health_check_timeout: rule.health_check_timeout ?? 5,
    cors_origins: rule.cors_origins || "",
    inject_request_headers: rule.inject_request_headers || "",
    upstream_verify_ssl: Number(rule.upstream_verify_ssl ?? -1),
    client_cert: rule.client_cert || "",
    client_key: rule.client_key || "",
    notes: rule.notes || "", enabled: Boolean(rule.enabled), is_default: Boolean(rule.is_default),
  } : {
    route_group: String(selectedIndex),
    priority: 0,
    timeout_mode: "inherit", timeout: 30,
    max_redirects_mode: "inherit", max_redirects: 10,
    retry_times_mode: "inherit", retry_times: 3,
    strip_prefix: "inherit", follow_redirects: "inherit", enable_streaming: "inherit",
    referer_whitelist: "", referer_policy: "", ua_blacklist: "", ua_whitelist: "",
    target_urls: "", health_check_enabled: false, health_check_path: "",
    health_check_interval: 30, health_check_timeout: 5, cors_origins: "",
    enabled: true, is_default: false,
  };

  openFormModal({
    title: isEdit ? `编辑规则 #${rule.id}` : "新建规则",
    schema,
    values,
    size: 720,
    validate: (out) => {
      if (!String(out.name || "").trim()) return "规则名称不能为空";
      if (out.route_group === "" || out.route_group == null) return "请选择所属路由组";
      if (!String(out.target_url || "").trim()) return "目标地址不能为空";
      return null;
    },
    onSave: async (out) => {
      const gi = groupOptions[Number(out.route_group)] || groupOptions[0];
      // 组级继承（P2-4.1）：三态控件 → inherit_fields 清单；标记字段后端写哨兵
      const inheritFields = [];
      ["timeout", "max_redirects", "retry_times"].forEach((k) => {
        if (out[`${k}_mode`] === "inherit") inheritFields.push(k);
      });
      ["follow_redirects", "enable_streaming", "strip_prefix"].forEach((k) => {
        if (out[k] === "inherit") inheritFields.push(k);
      });
      // referer_policy 空串选项 = 继承组默认
      if (out.referer_policy === "") inheritFields.push("referer_policy");
      const payload = {
        name: out.name, path_prefix: gi.path_prefix, request_host: gi.request_host,
        target_url: out.target_url, ip_whitelist: out.ip_whitelist || "", region_filters: out.region_filters || "",
        access_ip_whitelist: out.access_ip_whitelist || "", ip_blacklist: out.ip_blacklist || "",
        region_whitelist: out.region_whitelist || "", region_blacklist: out.region_blacklist || "",
        referer_whitelist: out.referer_whitelist || "", referer_policy: out.referer_policy || "",
        ua_blacklist: out.ua_blacklist || "",
        ua_whitelist: out.ua_whitelist || "",
        target_urls: linesToRuleUrls(out.target_urls),
        health_check_enabled: Boolean(out.health_check_enabled),
        health_check_path: out.health_check_path || "",
        health_check_interval: Number(out.health_check_interval ?? 30),
        health_check_timeout: Number(out.health_check_timeout ?? 5),
        cors_origins: out.cors_origins || "",
        inject_request_headers: out.inject_request_headers || "",
        upstream_verify_ssl: Number(out.upstream_verify_ssl ?? -1),
        client_cert: out.client_cert || "",
        client_key: out.client_key || "",
        priority: Number(out.priority ?? 0),
        timeout: Number(out.timeout ?? 30),
        max_redirects: Number(out.max_redirects ?? 10), retry_times: Number(out.retry_times ?? 3),
        notes: out.notes || "", path_rewrite_pattern: out.path_rewrite_pattern || "",
        path_rewrite_replacement: out.path_rewrite_replacement || "",
        enabled: Boolean(out.enabled), is_default: Boolean(out.is_default),
        strip_prefix: out.strip_prefix === "1", follow_redirects: out.follow_redirects !== "0",
        enable_streaming: out.enable_streaming !== "0",
        inherit_fields: inheritFields,
      };
      if (isEdit) {
        await apiFetch(`/_admin/api/rules/${rule.id}`, { method: "PUT", body: JSON.stringify(payload) });
        showToast("规则已更新。");
      } else {
        await apiFetch("/_admin/api/rules", { method: "POST", body: JSON.stringify(payload) });
        showToast("规则已创建。");
      }
      await loadDashboard();
    },
  });
}

export function prepareRuleForGroup(pathPrefix, requestHost = "") {
  openRuleModal(null, { path_prefix: pathPrefix, request_host: requestHost });
}

export async function removeRule(ruleId) {
  openConfirm({
    title: "删除规则",
    message: `确认删除规则 #${ruleId} 吗？此操作不可撤销。`,
    onOk: async () => {
      try {
        await apiFetch(`/_admin/api/rules/${ruleId}`, { method: "DELETE" });
        await loadDashboard();
        showToast("规则已删除。");
      } catch (e) { showToast(e.message, true); }
    },
  });
}

export async function toggleRule(ruleId, enabled) {
  try {
    await apiFetch(`/_admin/api/rules/${ruleId}`, { method: "PUT", body: JSON.stringify({ enabled }) });
    await loadDashboard();
    showToast(enabled ? "规则已启用。" : "规则已禁用。");
  } catch (e) { showToast(e.message, true); }
}

export function openRuleDrawer(ruleId) {
  const rule = state.rules.find((r) => r.id === Number(ruleId));
  if (!rule) return;
  const hostLabel = formatRequestHostLabel(normalizeRequestHost(rule.request_host));
  const bool = (v) => (v ? '<span class="pill pill-ok">开</span>' : '<span class="pill pill-neutral">关</span>');
  const cell = (v) => (v ? esc(String(v)) : '<span class="text-muted">—</span>');
  const kv = (k, v) => `<div class="kv"><div class="k">${esc(k)}</div><div class="val">${v}</div></div>`;
  const title = document.getElementById("drawerTitle");
  const body = document.getElementById("drawerBody");
  // 组级继承（P2-4.1）：继承字段加「继承组默认」徽标，值显示的是解析后的有效值
  const inherits = new Set(rule.inherit_fields || []);
  const inhw = (k) => (inherits.has(k) ? ' <span class="pill pill-brand" title="取自路由组级默认配置">继承组</span>' : "");
  if (title) title.textContent = `规则 #${rule.id}`;
  if (body) {
    body.innerHTML = `
      <div class="section-h">基础信息</div>
      ${kv("规则名称", cell(rule.name))}
      ${kv("备注", cell(rule.notes))}
      <div class="section-h" style="margin-top:16px">路由匹配</div>
      ${kv("路径前缀", `<code class="mono">${esc(rule.path_prefix || "")}</code>`)}
      ${kv("请求域名", cell(hostLabel))}
      ${kv("目标地址", rule.target_url ? `<a class="drawer-link" href="${esc(rule.target_url)}" target="_blank" rel="noopener">${esc(rule.target_url)}</a>` : cell(rule.target_url))}
      ${kv("额外上游", cell(ruleUrlsToLines(rule.target_urls) || "—"))}
      ${kv("上游健康", healthBadges(rule.health) || '<span class="text-muted">未启用探针</span>')}
      ${kv("健康探针", rule.health_check_enabled ? `<span class="pill pill-ok">开</span> ${(rule.health_check_path || "TCP")} · ${(rule.health_check_interval ?? 30)}s` : '<span class="pill pill-neutral">关</span>')}
      ${kv("优先级", cell(rule.priority ?? 0))}
      ${kv("默认规则", bool(rule.is_default))}
      ${kv("启用状态", bool(rule.enabled))}
      <div class="section-h" style="margin-top:16px">请求处理</div>
      ${kv("超时(秒)", cell(rule.timeout ?? 30) + inhw("timeout"))}
      ${kv("最大重定向", cell(rule.max_redirects ?? 10) + inhw("max_redirects"))}
      ${kv("重试次数", cell(rule.retry_times ?? 3) + inhw("retry_times"))}
      ${kv("去前缀", bool(rule.strip_prefix) + inhw("strip_prefix"))}
      ${kv("跟随重定向", bool(rule.follow_redirects !== false) + inhw("follow_redirects"))}
      ${kv("流式转发", bool(rule.enable_streaming) + inhw("enable_streaming"))}
      ${kv("正则模式", cell(rule.path_rewrite_pattern))}
      ${kv("正则替换", cell(rule.path_rewrite_replacement))}
      <div class="section-h" style="margin-top:16px">访问控制</div>
      ${kv("IP 白名单(路由)", cell(rule.ip_whitelist))}
      ${kv("地区条件", cell(rule.region_filters))}
      ${kv("访问控制 IP 白", cell(rule.access_ip_whitelist))}
      ${kv("访问控制 IP 黑", cell(rule.ip_blacklist))}
      ${kv("地区白名单", cell(rule.region_whitelist))}
      ${kv("地区黑名单", cell(rule.region_blacklist))}
      ${kv("Referer 白名单", rule.referer_whitelist ? `<code class="mono">${esc(rule.referer_whitelist)}</code>` : '<span class="text-muted">未启用校验</span>' + inhw("referer_whitelist"))}
      ${kv("空 Referer 策略", esc((rule.referer_policy || "allow") === "deny" ? "拒绝（仅白名单网页引用）" : "允许（本地播放器/直链）") + inhw("referer_policy"))}
      ${kv("UA 黑名单", rule.ua_blacklist ? `<code class="mono">${esc(rule.ua_blacklist)}</code>` : '<span class="text-muted">未启用</span>' + inhw("ua_blacklist"))}
      ${kv("UA 白名单", rule.ua_whitelist ? `<code class="mono">${esc(rule.ua_whitelist)}</code>` : '<span class="text-muted">未启用（放行全部 UA）</span>' + inhw("ua_whitelist"))}
    `;
  }
  openDrawer();
}

// ============ GeoIP ============

let _geoConfig = { enabled: false, online_cache_ttl_seconds: 120, sources: [], offline: {} };

export function bindGeoNumericInputSafety() {
  // 新版改用 schema 弹窗，无需绑定静态 input；保留空实现以兼容旧调用
}

export function fillGeoConfig(geo) {
  _geoConfig = geo || {};
  state.geoSources = Array.isArray(geo.sources)
    ? geo.sources.map((item, index) => ({
        id: item.id ?? `source-${index}`,
        name: item.name || "",
        enabled: Boolean(item.enabled),
        weight: Number(item.weight ?? 1),
        url: item.url || "",
        method: item.method || "GET",
        request_location: item.request_location || "query",
        body_format: item.body_format || "json",
        query_params_json: item.query_params_json || "{}",
        headers_json: item.headers_json || "{}",
        body_template: item.body_template || "",
        ip_param_name: item.ip_param_name || "ip",
        timeout: Number(item.timeout ?? 3),
        country_path: item.country_path || "country",
        region_path: item.region_path || "region",
        city_path: item.city_path || "city",
        full_path: item.full_path || "",
        priority: Number(item.priority ?? 0),
        notes: item.notes || "",
      }))
    : [];
  renderGeoSources();
  renderOfflineStatus(geo.offline || {});
  renderGeoCache();
}

export function renderGeoSources() {
  const body = document.getElementById("geoSourceBody");
  if (!body) return;
  if (!state.geoSources.length) {
    body.innerHTML = `<div class="empty">暂无在线定位源，点击「新增源」添加，或直接使用离线库。</div>`;
    return;
  }
  body.innerHTML = state.geoSources.map((s, i) => `
    <div class="src-row">
      <div class="src-icon"><svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg></div>
      <div style="min-width:0;flex:1">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <strong>${esc(s.name || `source-${i + 1}`)}</strong>
          <span class="pill ${s.enabled ? "pill-ok" : "pill-neutral"}">${s.enabled ? "启用" : "停用"}</span>
          <span class="hint">权重 ${s.weight} · ${esc(s.method)}/${esc(s.request_location)}</span>
        </div>
        <div class="hint" style="word-break:break-all">${esc(s.url)}</div>
      </div>
      <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
        <div class="switch ${s.enabled ? "on" : ""}" data-action="toggle-geo-source" data-index="${i}" role="switch" title="启用/停用"></div>
        <button class="btn btn-sm" data-action="test-geo-source" data-index="${i}">测试</button>
        <button class="btn btn-sm" data-action="edit-geo-source" data-index="${i}">编辑</button>
        <button class="btn btn-sm btn-danger" data-action="delete-geo-source" data-index="${i}">删除</button>
      </div>
    </div>`).join("");
}

const GEO_SOURCE_SCHEMA = [
  { key: "name", label: "名称", type: "text", required: true, placeholder: "示例定位源" },
  { key: "url", label: "接口地址", type: "text", required: true, placeholder: "https://geo.example.com/lookup" },
  { key: "weight", label: "权重", type: "number", default: 1 },
  { key: "priority", label: "优先级", type: "number", default: 0 },
  { key: "method", label: "请求方式", type: "select", options: [{ value: "GET", label: "GET" }, { value: "POST", label: "POST" }] },
  { key: "request_location", label: "参数位置", type: "select", options: [{ value: "query", label: "query" }, { value: "body", label: "body" }] },
  { key: "body_format", label: "请求体格式", type: "select", options: [{ value: "json", label: "json" }, { value: "form", label: "form" }] },
  { key: "ip_param_name", label: "IP 参数名", type: "text", default: "ip" },
  { key: "timeout", label: "超时（秒）", type: "number", default: 3 },
  { key: "country_path", label: "国家字段路径", type: "text", default: "country" },
  { key: "region_path", label: "地区字段路径", type: "text", default: "region" },
  { key: "city_path", label: "城市字段路径", type: "text", default: "city" },
  { key: "full_path", label: "汇总字段路径", type: "text" },
  { key: "query_params_json", label: "附加 query 参数 (JSON)", type: "json", default: "{}" },
  { key: "headers_json", label: "附加请求头 (JSON)", type: "json", default: "{}" },
  { key: "body_template", label: "请求体模板", type: "textarea" },
  { key: "notes", label: "备注", type: "text" },
  { key: "enabled", label: "启用", type: "switch" },
];

export function openGeoSourceModal(source, index) {
  const isEdit = source != null;
  const values = source ? { ...source, enabled: Boolean(source.enabled) } : {
    weight: 1, priority: 0, method: "GET", request_location: "query", body_format: "json",
    ip_param_name: "ip", timeout: 3, country_path: "country", region_path: "region", city_path: "city",
    query_params_json: "{}", headers_json: "{}", enabled: true,
  };
  openFormModal({
    title: isEdit ? `编辑在线源 ${source.name || `#${index + 1}`}` : "新增在线定位源",
    schema: GEO_SOURCE_SCHEMA,
    values,
    size: 720,
    validate: (out) => {
      if (!String(out.name || "").trim()) return "名称不能为空";
      if (!String(out.url || "").trim()) return "接口地址不能为空";
      return null;
    },
    onSave: async (out) => {
      const payload = {
        name: out.name, enabled: Boolean(out.enabled), weight: Number(out.weight ?? 1),
        url: out.url, method: out.method, request_location: out.request_location,
        body_format: out.body_format, query_params_json: out.query_params_json || "{}",
        headers_json: out.headers_json || "{}", body_template: out.body_template || "",
        ip_param_name: out.ip_param_name || "ip", timeout: Number(out.timeout ?? 3),
        country_path: out.country_path, region_path: out.region_path, city_path: out.city_path,
        full_path: out.full_path || "", priority: Number(out.priority ?? 0), notes: out.notes || "",
      };
      const prev = state.geoSources.map((s) => ({ ...s }));
      if (isEdit) state.geoSources[index] = { ...state.geoSources[index], ...payload };
      else state.geoSources.push(payload);
      renderGeoSources();
      try {
        await persistGeoSettings(isEdit ? "在线源已更新。" : "在线源已新增。");
      } catch (e) {
        state.geoSources = prev;
        renderGeoSources();
        throw e;
      }
    },
  });
}

export function renderOfflineStatus(offline) {
  const container = document.getElementById("offlineStatusBody");
  if (!container) return;
  const status = offline.status || {};
  const pillCls = status.file_exists ? "success" : "failed";
  container.innerHTML = `
    <div class="test-result-head">
      <div><h4 style="margin:0;font-size:14px">离线库维护状态</h4>
        <p class="test-result-message">${esc(status.last_sync_message || "尚未执行同步。")}</p></div>
      <span class="status-pill ${pillCls}">${status.file_exists ? "文件可用" : "文件缺失"}</span>
    </div>
    <div class="result-grid">
      <div class="result-item"><strong>本地路径</strong><span>${esc(offline.db_path || "-")}</span></div>
      <div class="result-item"><strong>下载链接</strong><span>${esc(offline.download_url || "-")}</span></div>
      <div class="result-item"><strong>文件大小</strong><span>${esc(formatBytes(status.file_size || 0))}</span></div>
      <div class="result-item"><strong>文件更新时间</strong><span>${esc(formatDateTime(status.file_updated_at || ""))}</span></div>
      <div class="result-item"><strong>最近同步</strong><span>${esc(formatDateTime(status.last_sync_at || ""))}</span></div>
      <div class="result-item"><strong>同步状态</strong><span>${esc(status.last_sync_status || "-")}</span></div>
    </div>`;
}

export function renderGeoCache() {
  const pill = document.getElementById("geoCachePill");
  const body = document.getElementById("geoCacheBody");
  const ttl = _geoConfig.online_cache_ttl_seconds ?? 120;
  if (pill) {
    pill.className = "pill " + (_geoConfig.enabled ? "pill-ok" : "pill-neutral");
    pill.textContent = _geoConfig.enabled ? "启用" : "停用";
  }
  if (body) {
    body.innerHTML = `
      <div class="kv"><div class="k">在线定位缓存</div><div class="val">${_geoConfig.enabled ? "已启用" : "已停用"}</div></div>
      <div class="kv"><div class="k">缓存 TTL</div><div class="val">${esc(String(ttl))} 秒</div></div>
      <div class="kv"><div class="k">在线源数量</div><div class="val">${state.geoSources.length}</div></div>`;
  }
}

export function buildGeoSettingsPayload() {
  return {
    enabled: _geoConfig.enabled,
    online_cache_ttl_seconds: _geoConfig.online_cache_ttl_seconds ?? 120,
    sources: state.geoSources.map((s) => ({
      name: s.name, enabled: s.enabled, weight: s.weight, url: s.url, method: s.method,
      request_location: s.request_location, body_format: s.body_format,
      query_params_json: s.query_params_json, headers_json: s.headers_json,
      body_template: s.body_template, ip_param_name: s.ip_param_name, timeout: s.timeout,
      country_path: s.country_path, region_path: s.region_path, city_path: s.city_path,
      full_path: s.full_path, priority: s.priority, notes: s.notes,
    })),
    offline: {
      enabled: _geoConfig.offline?.enabled ?? false,
      db_path: _geoConfig.offline?.db_path || "",
      locale: _geoConfig.offline?.locale || "zh-CN",
      download_url: _geoConfig.offline?.download_url || "",
      download_headers_json: _geoConfig.offline?.download_headers_json || "{}",
      refresh_interval_hours: _geoConfig.offline?.refresh_interval_hours ?? 24,
    },
  };
}

export async function persistGeoSettings(successMessage = "IP 定位配置已保存。") {
  await apiFetch("/_admin/api/geoip", { method: "PUT", body: JSON.stringify(buildGeoSettingsPayload()) });
  await loadDashboard();
  showToast(successMessage);
}

export function openGeoOnlineSettings() {
  openFormModal({
    title: "在线定位配置",
    schema: [
      { key: "enabled", label: "启用在线定位", type: "switch", hint: "关闭后仅使用离线 MMDB 定位" },
      { key: "online_cache_ttl_seconds", label: "在线定位缓存 TTL（秒）", type: "number", default: 120 },
    ],
    values: { enabled: Boolean(_geoConfig.enabled), online_cache_ttl_seconds: _geoConfig.online_cache_ttl_seconds ?? 120 },
    onSave: async (out) => {
      _geoConfig.enabled = Boolean(out.enabled);
      _geoConfig.online_cache_ttl_seconds = Math.max(0, Number(out.online_cache_ttl_seconds ?? 120));
      renderGeoCache();
      await persistGeoSettings("在线定位配置已保存。");
    },
  });
}

export function openGeoOfflineSettings() {
  const off = _geoConfig.offline || {};
  openFormModal({
    title: "离线 MMDB 配置",
    size: 640,
    schema: [
      { key: "enabled", label: "启用离线定位", type: "switch", hint: "在线定位失败时的兜底方案" },
      { key: "db_path", label: "本地库路径", type: "text", placeholder: "./data/GeoLite2-City.mmdb" },
      { key: "locale", label: "语言区域", type: "text", default: "zh-CN" },
      { key: "download_url", label: "下载链接", type: "text" },
      { key: "download_headers_json", label: "下载请求头 (JSON)", type: "json", default: "{}" },
      { key: "refresh_interval_hours", label: "自动刷新间隔（小时）", type: "number", default: 24 },
    ],
    values: {
      enabled: Boolean(off.enabled), db_path: off.db_path || "", locale: off.locale || "zh-CN",
      download_url: off.download_url || "", download_headers_json: off.download_headers_json || "{}",
      refresh_interval_hours: off.refresh_interval_hours ?? 24,
    },
    onSave: async (out) => {
      _geoConfig.offline = {
        enabled: Boolean(out.enabled), db_path: out.db_path || "", locale: out.locale || "zh-CN",
        download_url: out.download_url || "", download_headers_json: out.download_headers_json || "{}",
        refresh_interval_hours: Math.max(1, Number(out.refresh_interval_hours ?? 24)),
      };
      renderOfflineStatus(_geoConfig.offline);
      await persistGeoSettings("离线定位配置已保存。");
    },
  });
}

function renderGeoTestResult(result, ip) {
  const location = result.location || {};
  const statusText = result.success ? "测试成功" : "测试失败";
  const statusClass = result.success ? "success" : "failed";
  const upstreamResponse = result.upstream_response || {};
  const upstreamPayload = upstreamResponse.payload !== undefined ? upstreamResponse.payload : location.raw;
  let rawJson = "";
  if (upstreamPayload !== undefined && upstreamPayload !== null) {
    const txt = typeof upstreamPayload === "string" ? upstreamPayload : (() => { try { return JSON.stringify(upstreamPayload, null, 2); } catch { return String(upstreamPayload); } })();
    rawJson = `<details class="test-result-raw"><summary>查看接口原始返回</summary><pre>${esc(txt)}</pre></details>`;
  }
  return `
    <div class="test-result-head">
      <div><h4 style="margin:0;font-size:14px">${statusText}</h4>
        <p class="test-result-message">${esc(result.message || "-")}</p></div>
      <span class="status-pill ${statusClass}">${statusText}</span>
    </div>
    <div class="result-grid">
      <div class="result-item"><strong>测试 IP</strong><span>${esc(ip)}</span></div>
      <div class="result-item"><strong>定位来源</strong><span>${esc(result.provider || result.stage || "-")}</span></div>
      <div class="result-item"><strong>国家</strong><span>${esc(location.country || "-")}</span></div>
      <div class="result-item"><strong>地区</strong><span>${esc(location.region || "-")}</span></div>
      <div class="result-item"><strong>城市</strong><span>${esc(location.city || "-")}</span></div>
      <div class="result-item"><strong>区域汇总</strong><span>${esc(location.summary || location.full_text || "-")}</span></div>
    </div>
    ${rawJson}`;
}

function openTestModal({ title, placeholder, run }) {
  const modalEl = document.getElementById("modal");
  const mask = document.getElementById("modalMask");
  if (!modalEl || !mask) return;
  modalEl.style.width = "min(640px,100%)";
  modalEl.innerHTML = `
    <div class="modal-head"><div class="modal-title">${esc(title)}</div><button class="icon-btn" id="modalClose">✕</button></div>
    <div class="modal-body">
      <div class="form-field"><label>测试 IP</label><input class="input" id="testIp" placeholder="${esc(placeholder)}"></div>
      <div id="testResult"></div>
    </div>
    <div class="modal-foot"><button class="btn" id="modalCancel">关闭</button><button class="btn btn-primary" id="modalRun">运行测试</button></div>`;
  mask.classList.add("open");
  document.getElementById("modalClose").onclick = closeModal;
  document.getElementById("modalCancel").onclick = closeModal;
  document.getElementById("modalRun").onclick = async () => {
    const ip = document.getElementById("testIp").value.trim();
    const btn = document.getElementById("modalRun");
    if (!ip) { showToast("请输入测试 IP", true); return; }
    btn.disabled = true; btn.textContent = "测试中…";
    document.getElementById("testResult").innerHTML = `<p class="test-result-placeholder">正在请求定位服务，请稍候…</p>`;
    try {
      const result = await run(ip);
      document.getElementById("testResult").innerHTML = renderGeoTestResult(result, ip);
    } catch (e) {
      document.getElementById("testResult").innerHTML = `<p class="test-result-placeholder">${esc(e.message || "测试失败")}</p>`;
    } finally {
      btn.disabled = false; btn.textContent = "运行测试";
    }
  };
  window.setTimeout(() => document.getElementById("testIp")?.focus(), 50);
}

export function openGeoSourceTest(index) {
  const source = state.geoSources[index];
  if (!source) return;
  openTestModal({
    title: `测试在线源：${source.name || `source-${index + 1}`}`,
    placeholder: "例如 8.8.8.8",
    run: (ip) => apiFetch("/_admin/api/geoip/test", { method: "POST", body: JSON.stringify({ ip, source }) }),
  });
}

export function openOfflineTest() {
  openTestModal({
    title: "离线 MMDB 定位测试",
    placeholder: "例如 8.8.8.8",
    run: (ip) => apiFetch("/_admin/api/geoip/offline/test", { method: "POST", body: JSON.stringify({ ip, geoip: buildGeoSettingsPayload() }) }),
  });
}

export async function syncOffline() {
  const result = await apiFetch("/_admin/api/geoip/offline/sync", { method: "POST", body: JSON.stringify({ geoip: buildGeoSettingsPayload() }) });
  await loadDashboard();
  showToast(result.message || "离线 GeoIP 同步完成。");
}

export async function rollbackOffline() {
  const result = await apiFetch("/_admin/api/geoip/offline/rollback", { method: "POST", body: JSON.stringify({}) });
  await loadDashboard();
  showToast(result.message || "离线 GeoIP 回滚完成。");
}

export async function clearGeoCache() {
  const result = await apiFetch("/_admin/api/geoip/cache/clear", { method: "POST", body: JSON.stringify({}) });
  showToast(result.message || "在线定位缓存已清空。");
}

// ============ 日志（请求日志 + 应用日志） ============

export function renderRouteLogSettings(settings) {
  state.routeLogSettings = settings || {};
  const container = document.getElementById("route-log-settings-status");
  if (container) {
    container.innerHTML = `
      <div class="result-grid">
        <div class="result-item"><strong>日志总数</strong><span>${esc(String(settings.total_logs ?? 0))}</span></div>
        <div class="result-item"><strong>最大保留天数</strong><span>${esc(String(settings.retention_days ?? 30))}</span></div>
        <div class="result-item"><strong>最近清理时间</strong><span>${esc(formatDateTime(settings.last_pruned_at || ""))}</span></div>
      </div>`;
  }
}

function renderRefererField(log) {
  const ref = log.referer || "";
  if (!ref) return '<span>-</span>';
  let host = ref;
  try {
    const u = new URL(ref);
    if (u.hostname) host = u.hostname;
  } catch (_) { /* 非标准 URL，直接展示原始值 */ }
  const reqHost = (log.request_host || "").toLowerCase();
  const isExternal = !!host && host.toLowerCase() !== reqHost;
  // 外部引用 = Referer 域名既非空也非本次请求命中的代理域名，提示可能存在盗链
  const warn = isExternal ? ' <span class="pill pill-warn">外部引用</span>' : '';
  return `<span title="${esc(ref)}">${esc(host)}</span>${warn}`;
}

export function renderRouteLogs(payload) {
  state.routeLogs = Array.isArray(payload.items) ? payload.items : [];
  const total = payload.total ?? state.routeLogs.length;
  const totalPages = payload.total_pages ?? 1;
  const currentPage = payload.page ?? 1;
  const limit = payload.limit ?? 50;
  state.logTotalPages = totalPages;
  state.logCurrentPage = currentPage;

  const startOffset = (currentPage - 1) * limit + 1;
  const endOffset = Math.min(currentPage * limit, total);
  const rangeText = total > 0 ? `${startOffset}-${endOffset} / ${total}` : "0 / 0";
  setText("route-log-total-count", `共 ${total} 条（${rangeText}）`);

  const container = document.getElementById("route-logs-list-body");
  if (!container) return;
  container.innerHTML = "";
  setChecked("route-log-select-all", false);

  if (!state.routeLogs.length) {
    container.innerHTML = '<div class="route-log-empty">当前没有匹配到规则转发日志。</div>';
    renderPagination(1, 1, "log-pagination", goToPage);
    return;
  }

  state.routeLogs.forEach((log) => {
    const banIp = log.client_ip || log.original_client_ip || "-";
    const ipBanned = banIp !== "-" && isIpBanned(banIp, state.bannedIps);
    const banButtonHtml = ipBanned
      ? `<button class="btn btn-sm" data-action="unban-ip-from-log" data-ip="${esc(banIp)}">解禁IP</button>`
      : `<button class="btn btn-sm btn-danger" data-action="ban-ip-from-log" data-ip="${esc(banIp)}" data-path-prefix="${esc(log.path_prefix || "")}">封禁IP</button>`;
    const cacheStatusInfo = formatCacheStatus(log.cache_status);
    // 请求链路（迁移 025）：链路节点随日志落库；含「签名重入」即为 /_signed 端点
    // 处理的请求（A 换302直连 / B 内部代理穿流），与普通代理请求一眼区分。
    const chainText = String(log.chain || "");
    const isSignedReentry = chainText.includes("签名重入");
    // 结果类型徽章：3xx 或有 302地址 = 重定向结果（规则不跟随/缓存命中直接回 302）；
    // 有上游状态且非 3xx = 代理转发（跟随重定向后回传内容）
    const upstream = Number(log.upstream_status || 0);
    const resultKindBadge = [301, 302, 303, 307, 308].includes(upstream) || log.redirect_location
      ? '<span class="pill pill-warn">重定向结果</span>'
      : (upstream > 0 ? '<span class="pill pill-ok">代理转发</span>' : "");
    const card = document.createElement("article");
    card.className = "route-log-item";
    card.innerHTML = `
      <div class="route-log-item-main">
        <div class="route-log-item-check"><input class="route-log-checkbox" data-id="${log.id}" type="checkbox"></div>
        <div class="route-log-item-body">
          <div class="route-log-item-header">
            <div class="route-log-item-time"><strong>${esc(formatDateTime(log.created_at))}</strong><span class="route-log-duration">${esc(`${log.operation_duration_ms || 0} ms`)}</span></div>
            <div class="route-log-item-actions">${banButtonHtml}<button class="btn btn-sm btn-danger" data-action="delete-route-log" data-id="${log.id}">删除</button></div>
          </div>
          <div class="route-log-item-fields">
            <div class="route-log-field"><span class="route-log-field-label">请求</span><div class="route-log-field-value"><strong>${esc(log.request_method || "-")}</strong><span class="route-log-path" title="${esc(decodeUrlDisplay(log.request_path))}">${esc(decodeUrlDisplay(log.request_path || "-"))}</span>${log.request_query_string ? `<span class="route-log-query" title="${esc(decodeUrlDisplay(log.request_query_string))}">?${esc(decodeUrlDisplay(log.request_query_string))}</span>` : ""}</div></div>
            <div class="route-log-field"><span class="route-log-field-label">域名</span><div class="route-log-field-value"><span>${esc(formatRouteLogRequestHost(log.request_host || ""))}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">前缀</span><div class="route-log-field-value"><strong>${esc(log.path_prefix || "-")}</strong></div></div>
            <div class="route-log-field"><span class="route-log-field-label">规则</span><div class="route-log-field-value"><span>${esc(log.rule_name || "-")}</span><span class="hint">命中域名: ${esc(formatRouteLogRuleRequestHost(log.rule_request_host || ""))}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">地区</span><div class="route-log-field-value"><strong>${esc(log.geo_summary || "-")}</strong><span class="hint">命中: ${esc(log.matched_region || "-")}</span><span class="hint">源: ${esc(log.geo_source || "-")}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">匹配</span><div class="route-log-field-value"><strong>${esc(formatMatchStrategy(log.match_strategy))}</strong><span class="hint">${esc(formatMatchDetail(log.match_detail))}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">302地址</span><div class="route-log-field-value"><strong class="route-log-target-url" title="${esc(decodeUrlDisplay(log.redirect_location))}">${esc(decodeUrlDisplay(log.redirect_location || "-"))}</strong></div></div>
            <div class="route-log-field"><span class="route-log-field-label">转发结果</span><div class="route-log-field-value">${isSignedReentry ? '<span class="pill pill-info">签名重入</span>' : ""}${resultKindBadge}<strong class="route-log-target-url" title="${esc(decodeUrlDisplay(log.target_url))}">${esc(decodeUrlDisplay(log.target_url || "-"))}</strong><span class="hint">上游: ${esc(String(log.upstream_status || 0))}</span><span class="cache-status-badge ${cacheStatusInfo.cls}">${esc(cacheStatusInfo.text)}</span><span class="hint">结果: ${esc(formatResultStatus(log.result_status))}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">链路</span><div class="route-log-field-value">${chainText ? `<span class="route-log-chain" title="${esc(chainText)}">${esc(chainText)}</span>` : "-"}</div></div>
            ${log.error_message ? `<div class="route-log-field"><span class="route-log-field-label">错误</span><div class="route-log-field-value"><span class="route-log-error" title="${esc(log.error_message)}">${esc(log.error_message)}</span></div></div>` : ""}
            <div class="route-log-field"><span class="route-log-field-label">IP</span><div class="route-log-field-value"><span>原始: ${esc(log.original_client_ip || "-")}</span><span>匹配: ${esc(log.client_ip || "-")}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">Referer</span><div class="route-log-field-value">${renderRefererField(log)}</div></div>
            <div class="route-log-field"><span class="route-log-field-label">UA</span><div class="route-log-field-value"><span title="${esc(log.user_agent || "")}">${esc((log.user_agent || "-").slice(0, 60))}</span><span class="hint">${log.bytes_transferred ? esc(formatBytes(Number(log.bytes_transferred) || 0)) : ""}</span></div></div>
          </div>
        </div>
      </div>`;
    container.appendChild(card);
  });
  renderPagination(state.logCurrentPage, state.logTotalPages, "log-pagination", goToPage);
}

export function collectRouteLogFilters() {
  return {
    keyword: getValue("log_keyword").trim(),
    path_prefix: getValue("log_path_prefix").trim(),
    rule_request_host: normalizeRequestHost(getValue("log_rule_request_host").trim()),
    match_strategy: getValue("log_match_strategy"),
    result_status: getValue("log_result_status"),
    referer: getValue("log_referer").trim(),
    date_from: toIsoDateTime(getValue("log_date_from")),
    date_to: toIsoDateTime(getValue("log_date_to")),
    limit: state.logPageSize,
    page: state.logCurrentPage,
  };
}

function buildRouteLogQuery(filters) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value === "" || value === null || value === undefined) return;
    params.set(key, String(value));
  });
  return params.toString();
}

export async function loadRouteLogSettings() {
  const data = await apiFetch("/_admin/api/log-settings");
  renderRouteLogSettings(data || {});
}

export async function loadRouteLogs() {
  const query = buildRouteLogQuery(collectRouteLogFilters());
  const [payload, bansData] = await Promise.all([
    apiFetch(`/_admin/api/logs${query ? `?${query}` : ""}`),
    apiFetch("/_admin/api/banned-ips").catch(() => ({ items: [] })),
  ]);
  state.bannedIps = bansData.items || [];
  renderRouteLogs(payload || { items: [], total: 0 });
}

export async function refreshRouteLogModule() {
  // 盗链监控已迁入「设置与监控」弹窗，改为打开弹窗时按需加载，不再随日志页刷新
  await Promise.all([loadRouteLogSettings(), loadRouteLogs()]);
}

// ============ 转发结果 CSV 导出（路径/URL 已解码） ============
// 复用 collectRouteLogFilters 取当前筛选条件，分页拉取全部匹配记录（后端 limit 上限 500），
// 对 request_path / request_query_string / redirect_location / target_url 做 URL 解码后写 CSV。
// 解码与列表显示共用 decodeUrlDisplay，保证「看得到的解码值 = 导出的解码值」。

// CSV 单元格转义：含逗号/引号/换行时用双引号包裹并把内部引号翻倍（RFC 4180）。
function csvCell(v) {
  const s = v == null ? "" : String(v);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

const ROUTE_LOG_CSV_COLUMNS = [
  ["id", "ID"],
  ["created_at", "时间"],
  ["request_method", "方法"],
  ["request_path", "请求路径", true],
  ["request_query_string", "查询串", true],
  ["request_host", "域名"],
  ["path_prefix", "前缀"],
  ["rule_name", "规则"],
  ["rule_request_host", "命中域名"],
  ["geo_summary", "地区"],
  ["matched_region", "命中地区"],
  ["match_strategy", "匹配策略"],
  ["match_detail", "匹配详情"],
  ["redirect_location", "302地址", true],
  ["target_url", "转发目标", true],
  ["upstream_status", "上游状态"],
  ["cache_status", "缓存状态"],
  ["result_status", "结果"],
  ["operation_duration_ms", "耗时(ms)"],
  ["original_client_ip", "原始IP"],
  ["client_ip", "匹配IP"],
  ["referer", "Referer"],
  ["bytes_transferred", "传输字节"],
  ["chain", "链路"],
  ["error_message", "错误"],
];

function routeLogsToCsv(rows) {
  const header = ROUTE_LOG_CSV_COLUMNS.map((c) => csvCell(c[1])).join(",");
  const lines = rows.map((r) =>
    ROUTE_LOG_CSV_COLUMNS.map((c) => {
      const raw = r[c[0]];
      // 第三项为 true 的列为 URL 字段，导出时解码；其余原样输出
      const val = c[2] ? decodeUrlDisplay(raw) : raw;
      return csvCell(val);
    }).join(",")
  );
  return [header, ...lines].join("\r\n");
}

export async function exportRouteLogs(showToast) {
  const baseFilters = collectRouteLogFilters();
  const PAGE = 500; // 后端 list_route_logs 的 limit 上限
  const SAFETY_CAP = 200000; // 单次导出行数上限，避免极端数据量拖垮浏览器
  let page = 1;
  const all = [];
  // 逐页拉取，直到取完当前筛选条件下的全部记录
  while (true) {
    const payload = await apiFetch(
      `/_admin/api/logs?${buildRouteLogQuery({ ...baseFilters, limit: PAGE, page })}`
    );
    const items = (payload && Array.isArray(payload.items)) ? payload.items : [];
    all.push(...items);
    const total = payload ? Number(payload.total || 0) : 0;
    if (items.length < PAGE || all.length >= total || all.length >= SAFETY_CAP) break;
    page += 1;
  }
  if (!all.length) {
    if (showToast) showToast("没有可导出的记录（当前筛选条件下无数据）。");
    return;
  }
  const csv = routeLogsToCsv(all);
  // 前置 BOM 让 Excel 正确识别 UTF-8（含中文）；download 属性触发浏览器下载
  const blob = new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `route_logs_${new Date().toISOString().slice(0, 10)}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
  if (showToast) showToast(`已导出 ${all.length} 条转发记录（CSV，路径/URL 已解码）。`);
}

// ---- 盗链监控看板（HOTLINK_PROTECTION.md 阶段 1） ----

function renderHotlinkStats(data) {
  const refsEl = document.getElementById("hotlink-top-referrers");
  const ipsEl = document.getElementById("hotlink-top-ips");
  const totalEl = document.getElementById("hotlink-external-total");
  if (!refsEl && !ipsEl) return;
  const extTotal = Number(data.external_referer_total || 0);
  if (totalEl) totalEl.textContent = extTotal > 0 ? `（共 ${extTotal} 次外部引用）` : "";

  if (refsEl) {
    const refs = Array.isArray(data.top_referrers) ? data.top_referrers : [];
    refsEl.innerHTML = refs.length
      ? refs.map((r, i) => `
          <div class="hotlink-row">
            <div class="hotlink-row-main"><span class="hint">${i + 1}.</span> <strong title="${esc(r.host)}">${esc(r.host)}</strong></div>
            <div class="hotlink-row-side">
              <span class="hint">${r.count} 次</span>
              ${r.last_client_ip ? `<button class="btn btn-sm btn-danger" data-action="ban-ip-from-log" data-ip="${esc(r.last_client_ip)}">封禁IP</button>` : ""}
            </div>
          </div>`).join("")
      : '<div class="hint">近 24h 无外部 Referer 记录。</div>';
  }

  if (ipsEl) {
    const ips = Array.isArray(data.top_ips_by_bytes) ? data.top_ips_by_bytes : [];
    ipsEl.innerHTML = ips.length
      ? ips.map((item, i) => `
          <div class="hotlink-row">
            <div class="hotlink-row-main"><span class="hint">${i + 1}.</span> <strong>${esc(item.client_ip)}</strong> <span class="hint">${item.request_count} 次</span></div>
            <div class="hotlink-row-side">${esc(formatBytes(Number(item.bytes_transferred) || 0))}</div>
          </div>`).join("")
      : '<div class="hint">近 24h 无流量统计（流式传输后显示）。</div>';
  }
}

export async function loadHotlinkStats() {
  const card = document.getElementById("hotlink-monitor-card");
  if (!card || card.hidden) return;
  const data = await apiFetch("/_admin/api/hotlink/stats?hours=24");
  renderHotlinkStats(data || {});
}

export async function goToPage(page, totalPages) {
  page = Math.max(1, Math.min(totalPages, page));
  state.logCurrentPage = page;
  await loadRouteLogs();
}

export async function saveLogRetention() {
  const days = getNonNegativeIntValue("log_retention_days", 30);
  await apiFetch("/_admin/api/log-settings", { method: "PUT", body: JSON.stringify({ retention_days: days }) });
  await refreshRouteLogModule();
  showToast("日志保留策略已保存。");
}

export async function cleanupLogs() {
  const result = await apiFetch("/_admin/api/log-cleanup", { method: "POST" });
  showToast(`清理完成，删除了 ${result.deleted_count} 条过期日志记录。`);
  await refreshRouteLogModule();
}

// ---- 自动刷新（请求日志） ----

let _autoRefreshTimer = null;
const AUTO_REFRESH_STORAGE_KEY = "log_auto_refresh";

export function getAutoRefreshConfig() {
  try { const raw = localStorage.getItem(AUTO_REFRESH_STORAGE_KEY); if (raw) return JSON.parse(raw); } catch (_) {}
  return { enabled: false, interval: 5 };
}
export function saveAutoRefreshConfig(cfg) { localStorage.setItem(AUTO_REFRESH_STORAGE_KEY, JSON.stringify(cfg)); }

function updateAutoRefreshStatusUI() {
  const el = document.getElementById("log_auto_refresh_status");
  if (!el) return;
  if (_autoRefreshTimer !== null) { el.textContent = "●"; el.className = "auto-refresh-status running"; }
  else { el.textContent = ""; el.className = "auto-refresh-status stopped"; }
}

export function stopAutoRefresh() {
  if (_autoRefreshTimer !== null) { clearInterval(_autoRefreshTimer); _autoRefreshTimer = null; }
  updateAutoRefreshStatusUI();
}

export function startAutoRefresh() {
  stopAutoRefresh();
  if (!getChecked("log_auto_refresh_enabled")) return;
  const interval = Math.max(1, parseInt(getValue("log_auto_refresh_interval") || "5", 10) || 5);
  saveAutoRefreshConfig({ enabled: true, interval });
  _autoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "logs") { stopAutoRefresh(); return; }
    loadRouteLogs().catch((e) => { showToast(e.message, true); stopAutoRefresh(); setChecked("log_auto_refresh_enabled", false); });
  }, interval * 1000);
  updateAutoRefreshStatusUI();
}

// ---- 应用日志 ----

let _appLogAutoRefreshTimer = null;
const APP_LOG_MAX_DOM_NODES = 600;

export function highlightLogLine(line) {
  if (!line) return line;
  let safe = line.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  safe = safe.replace(/^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}[,\.]?\d*)/, '<span class="log-ts">$1</span>');
  safe = safe.replace(/\b(INFO|DEBUG|WARNING|ERROR|CRITICAL)\b/g, '<span class="log-level-$1">$1</span>');
  safe = safe.replace(/\b(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\/\S*)\s+(\d{3})\s+([\d.]+ms)\b/g, '<span class="log-method">$1</span> $2 <span class="log-status-$3">$3</span> <span class="log-duration">$4</span>');
  return safe;
}

export async function loadAppLogFiles() {
  const data = await apiFetch("/_admin/api/app-logs");
  if (!data) return;
  const container = document.getElementById("appLogFiles");
  const nameEl = document.getElementById("appLogName");
  if (!container) return;
  const files = data.items || [];
  if (!files.length) {
    container.innerHTML = `<div class="empty">暂无日志文件</div>`;
    if (nameEl) nameEl.textContent = "—";
    return;
  }
  const prev = state.appLogFile || data.current || files[0].name;
  state.appLogFile = prev;
  if (nameEl) nameEl.textContent = prev;
  container.innerHTML = files.map((f) => `
    <div class="file-item ${f.name === prev ? "current" : ""}" data-action="select-log-file" data-file="${esc(f.name)}" title="${esc(f.name)}">
      <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
      <span style="min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(f.name)}</span>
      <span class="fsize">${esc(formatBytes(f.size))}</span>
    </div>`).join("");
  loadAppLogContent().catch(() => {});
}

export async function loadAppLogContent(isAutoRefresh = false) {
  const params = new URLSearchParams();
  if (state.appLogFile) params.set("file", state.appLogFile);
  const keyword = (getValue("app-log-keyword") || "").trim();
  if (keyword) params.set("keyword", keyword);
  params.set("tail", getValue("app-log-tail-lines") || "100");

  const contentEl = document.getElementById("appLogContent");
  if (!contentEl) return;
  const data = await apiFetch(`/_admin/api/app-logs/content?${params.toString()}`);
  if (!data) return;

  const fileInfoEl = document.getElementById("app-log-file-info");
  const lineInfoEl = document.getElementById("app-log-line-info");
  const raw = data.content || "";

  if (!raw) {
    contentEl.textContent = "(无内容)";
    state.logLastLineCount = 0;
    return;
  }

  const lines = raw.split("\n");
  const newTotal = data.total_lines || lines.length;
  const prevTotal = state.logLastLineCount || 0;

  if (isAutoRefresh && newTotal > prevTotal && prevTotal > 0 && !keyword) {
    const appendCount = Math.min(newTotal - prevTotal, lines.length);
    const newLines = lines.slice(lines.length - appendCount);
    const fragment = document.createDocumentFragment();
    for (const line of newLines) {
      if (!line) continue;
      const span = document.createElement("span");
      span.innerHTML = highlightLogLine(line);
      fragment.appendChild(document.createElement("br"));
      fragment.appendChild(span);
    }
    contentEl.appendChild(fragment);
    state.logLastLineCount = newTotal;
    let childNodes = contentEl.childNodes;
    while (childNodes.length > APP_LOG_MAX_DOM_NODES) contentEl.removeChild(childNodes[0]);
  } else {
    const fragment = document.createDocumentFragment();
    lines.forEach((line, i) => {
      if (!line) return;
      if (fragment.childNodes.length > 0) fragment.appendChild(document.createElement("br"));
      const span = document.createElement("span");
      span.innerHTML = highlightLogLine(line);
      fragment.appendChild(span);
    });
    contentEl.innerHTML = "";
    contentEl.appendChild(fragment);
    state.logLastLineCount = newTotal;
  }

  if (fileInfoEl) fileInfoEl.textContent = `文件: ${data.file || state.appLogFile || "-"}`;
  if (lineInfoEl) {
    const matched = data.matched_lines != null ? data.matched_lines : data.total_lines;
    lineInfoEl.textContent = keyword ? `匹配: ${matched} / 总计: ${data.total_lines} 行` : `共 ${data.total_lines} 行`;
  }
  if (!isAutoRefresh || state.logAutoScroll) contentEl.scrollTop = contentEl.scrollHeight;
}

export function startAppLogAutoRefresh() {
  stopAppLogAutoRefresh();
  if (!getChecked("app-log-auto-refresh")) return;
  _appLogAutoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "logs") { stopAppLogAutoRefresh(); return; }
    if (!state.logAutoScroll) return;
    loadAppLogContent(true).catch(() => {});
  }, 3000);
}
export function stopAppLogAutoRefresh() {
  if (_appLogAutoRefreshTimer !== null) { clearInterval(_appLogAutoRefreshTimer); _appLogAutoRefreshTimer = null; }
}

export async function refreshAppLogModule() {
  await loadAppLogFiles();
  initLogScrollDetection();
  loadLoggingSettings().catch(() => {});
}

export async function loadLoggingSettings() {
  const data = await apiFetch("/_admin/api/logging-settings");
  if (data && typeof data.retention_days === "number") {
    setValue("disk_log_retention_days", String(data.retention_days));
  }
}

export async function saveDiskLogRetention() {
  const days = getNonNegativeIntValue("disk_log_retention_days", 30);
  const result = await apiFetch("/_admin/api/logging-settings", {
    method: "PUT",
    body: JSON.stringify({ retention_days: days }),
  });
  setText("disk-log-retention-status", `已保存：磁盘日志保留 ${result.retention_days} 天。`);
  showToast("磁盘日志保留策略已保存。");
}

export function initLogScrollDetection() {
  const contentEl = document.getElementById("appLogContent");
  if (!contentEl || contentEl._scrollListenerAdded) return;
  contentEl._scrollListenerAdded = true;
  let scrollTimer = null;
  contentEl.addEventListener("scroll", () => {
    if (scrollTimer !== null) return;
    scrollTimer = setTimeout(() => {
      scrollTimer = null;
      const isAtBottom = contentEl.scrollHeight - contentEl.scrollTop - contentEl.clientHeight < 50;
      state.logAutoScroll = isAtBottom;
    }, 100);
  });
}

export async function cleanupAppLogFiles() {
  const result = await apiFetch("/_admin/api/log-file-cleanup", {
    method: "POST",
    body: JSON.stringify({}),
  });
  showToast(`清理完成，删除了 ${result.deleted_count} 个过期日志文件。`);
  await refreshAppLogModule();
}

// ============ 请求结果缓存（ip_result_cache） ============

export async function loadIpCacheSettings() {
  try {
    const data = await apiFetch("/_admin/api/ip-cache-settings");
    if (data) {
      setValue("ip_cache_enabled", data.enabled ? "1" : "0");
      setValue("ip_cache_ttl_seconds", String(data.ttl_seconds || 300));
      setValue("ip_cache_max_entries", String(data.max_entries || 5000));
      renderIpCacheSettings(data);
    }
  } catch (_) {}
}

function renderIpCacheSettings(data) {
  const pill = document.getElementById("ipCachePill");
  if (pill) {
    pill.className = "pill " + (data.enabled ? "pill-ok" : "pill-neutral");
    pill.textContent = data.enabled ? "启用" : "停用";
  }
  const body = document.getElementById("ipCacheSettings");
  if (body) {
    body.innerHTML = `
      <div class="kv"><div class="k">状态</div><div class="val">${data.enabled ? "已启用" : "已禁用"}</div></div>
      <div class="kv"><div class="k">TTL</div><div class="val">${esc(String(data.ttl_seconds || 300))} 秒</div></div>
      <div class="kv"><div class="k">最大条目</div><div class="val">${esc(String(data.max_entries || 5000))}</div></div>`;
  }
}

export async function loadIpCacheStats() {
  try {
    const stats = await apiFetch("/_admin/api/ip-cache/stats");
    const body = document.getElementById("ipCacheStats");
    if (!body || !stats) return;
    // 独立的 ipCacheStats 容器，直接 innerHTML 替换即可，无重复风险
    body.innerHTML = `
      <div class="kv"><div class="k">当前条目</div><div class="val">${esc(String(stats.current_entries))}</div></div>
      <div class="kv"><div class="k">命中 / 未命中</div><div class="val">${esc(String(stats.hits))} / ${esc(String(stats.misses))}</div></div>
      <div class="kv"><div class="k">命中率</div><div class="val">${esc(String(stats.hit_rate))}</div></div>`;
  } catch (_) {}
}

export function openIpCacheSettings() {
  openFormModal({
    title: "请求结果缓存配置",
    schema: [
      { key: "enabled", label: "启用请求结果缓存", type: "switch", hint: "缓存 IP 对应的转发结果，命中后跳过定位与规则匹配" },
      { key: "ttl_seconds", label: "TTL（秒）", type: "number", default: 300 },
      { key: "max_entries", label: "最大条目", type: "number", default: 5000 },
    ],
    values: {
      enabled: getValue("ip_cache_enabled") === "1",
      ttl_seconds: Number(getValue("ip_cache_ttl_seconds") || 300),
      max_entries: Number(getValue("ip_cache_max_entries") || 5000),
    },
    onSave: async (out) => {
      await apiFetch("/_admin/api/ip-cache-settings", {
        method: "PUT",
        body: JSON.stringify({
          enabled: Boolean(out.enabled),
          ttl_seconds: Number(out.ttl_seconds ?? 300),
          max_entries: Number(out.max_entries ?? 5000),
        }),
      });
      await loadIpCacheSettings();
      showToast("请求结果缓存配置已保存。");
    },
  });
}

export async function clearIpCache() {
  openConfirm({
    title: "清空请求结果缓存",
    message: "确认清空所有请求结果缓存吗？",
    onOk: async () => {
      try {
        const data = await apiFetch("/_admin/api/ip-cache/clear", { method: "POST" });
        showToast(data.message || "缓存已清空");
        loadIpCacheStats();
      } catch (e) { showToast(e.message, true); }
    },
  });
}

// ============ 请求去重（request_dedup） ============

export async function loadDedupSettings() {
  try {
    const data = await apiFetch("/_admin/api/dedup-settings");
    if (data) {
      setValue("dedup_enabled", data.enabled ? "1" : "0");
      setValue("dedup_window_seconds", String(data.window_seconds ?? 2.0));
      setValue("dedup_max_cache_entries", String(data.max_cache_entries ?? 10000));
      const pill = document.getElementById("dedupPill");
      if (pill) { pill.className = "pill " + (data.enabled ? "pill-ok" : "pill-neutral"); pill.textContent = data.enabled ? "启用" : "停用"; }
    }
  } catch (_) {}
  loadDedupStats();
}

export async function loadDedupStats() {
  try {
    const stats = await apiFetch("/_admin/api/dedup/stats");
    const body = document.getElementById("dedupBody");
    if (!body || !stats) return;
    body.innerHTML = `
      <div class="kv"><div class="k">状态</div><div class="val">${stats.enabled ? "已启用" : "已禁用"}</div></div>
      <div class="kv"><div class="k">窗口时长</div><div class="val">${esc(String(stats.window_seconds))} 秒</div></div>
      <div class="kv"><div class="k">最大条目</div><div class="val">${esc(String(stats.max_cache_entries))}</div></div>
      <div class="kv"><div class="k">当前条目</div><div class="val">${esc(String(stats.current_entries))}</div></div>
      <div class="kv"><div class="k">累计命中</div><div class="val">${esc(String(stats.total_hits))}</div></div>`;
  } catch (_) {}
}

export function openDedupSettings() {
  openFormModal({
    title: "请求去重配置",
    schema: [
      { key: "enabled", label: "启用请求去重", type: "switch", hint: "短时间窗口内相同的请求直接返回上次结果，降低后端压力" },
      { key: "window_seconds", label: "去重窗口（秒）", type: "number", default: 2.0 },
      { key: "max_cache_entries", label: "最大缓存条目", type: "number", default: 10000 },
    ],
    values: {
      enabled: getValue("dedup_enabled") === "1",
      window_seconds: Number(getValue("dedup_window_seconds") || 2.0),
      max_cache_entries: Number(getValue("dedup_max_cache_entries") || 10000),
    },
    onSave: async (out) => {
      await apiFetch("/_admin/api/dedup-settings", {
        method: "PUT",
        body: JSON.stringify({
          enabled: Boolean(out.enabled),
          window_seconds: Number(out.window_seconds ?? 2.0),
          max_cache_entries: Number(out.max_cache_entries ?? 10000),
        }),
      });
      await loadDedupSettings();
      showToast("请求去重配置已保存。");
    },
  });
}

export async function clearDedupCache() {
  openConfirm({
    title: "清空请求去重缓存",
    message: "确认清空请求去重缓存吗？",
    onOk: async () => {
      try {
        const data = await apiFetch("/_admin/api/dedup/clear", { method: "POST" });
        showToast(data.message || "已清除请求去重缓存");
        loadDedupStats();
      } catch (e) { showToast(e.message, true); }
    },
  });
}

// ============ 自动封禁 ============

export async function loadAutoBanSettings() {
  try {
    const data = await apiFetch("/_admin/api/auto-ban");
    if (data) {
      setValue("auto_ban_enabled", data.enabled ? "1" : "0");
      setValue("auto_ban_window_seconds", String(data.window_seconds || 60));
      setValue("auto_ban_max_requests", String(data.max_requests || 100));
      setValue("auto_ban_ban_duration_seconds", String(data.ban_duration_seconds || 3600));
      setValue("auto_ban_max_404", String(data.max_404 || 20));
      setValue("auto_ban_auto_ban_on_404", data.auto_ban_on_404 ? "1" : "0");
      setValue("auto_ban_whitelist", data.whitelist || "");
      setValue("auto_ban_email_on_ban", data.email_on_ban ? "1" : "0");
      setValue("auto_ban_max_mb", String(Math.round((data.max_bytes || 0) / 1048576)));
      const pill = document.getElementById("autoBanStatusPill");
      if (pill) { pill.className = "pill " + (data.enabled ? "pill-ok" : "pill-neutral"); pill.textContent = data.enabled ? "已启用" : "已停用"; }
    }
  } catch (_) {}
}

export async function loadAutoBanStats() {
  try {
    const [stats, trackedResp] = await Promise.all([
      apiFetch("/_admin/api/auto-ban/stats"),
      apiFetch("/_admin/api/auto-ban/tracked").catch(() => null),
    ]);
    const body = document.getElementById("autoBanStatsBody");
    if (!body || !stats) return;
    const items = (trackedResp && trackedResp.items) || [];
    // 只展示前 20 个（按窗口内请求数降序，服务端已排序），完整异常趋势看日志页
    const rows = items.slice(0, 20).map((t) => `
      <div class="tracked-row">
        <span class="tracked-ip" title="${esc(t.ip)}">${esc(t.ip)}</span>
        <span class="hint tracked-meta">请求 ${t.request_count ?? 0} 次 · 404 ×${t.error_404_count ?? 0} · ${esc(formatBytes(Number(t.bytes_total) || 0))}</span>
        <button class="btn btn-sm btn-danger" data-action="ban-tracked-ip" data-ip="${esc(t.ip)}">封禁</button>
      </div>`).join("");
    body.innerHTML = `
      <div class="kv"><div class="k">跟踪 IP 数</div><div class="val">${esc(String(stats.tracked_ips ?? 0))}</div></div>
      <div class="kv"><div class="k">白名单 IP 数</div><div class="val">${esc(String(stats.whitelisted_ips ?? 0))}</div></div>
      <div class="kv"><div class="k">总请求数</div><div class="val">${esc(String(stats.total_requests ?? 0))}</div></div>
      <div class="kv"><div class="k">累计封禁</div><div class="val">${esc(String(stats.total_bans ?? 0))}</div></div>
      <div class="tracked-box">
        <div class="tracked-title">正在追踪的可疑 IP <span class="hint">统计窗口内异常请求 · 共 ${items.length} 个</span></div>
        ${rows || `<div class="hint" style="padding:6px 0">${stats.enabled ? "暂无可疑 IP，统计窗口内无异常请求" : "自动封禁已停用，仍在记录请求统计"}</div>`}
      </div>`;
  } catch (_) {}
}

// 可疑 IP 列表随安全页定时刷新（5s），切走页面后由 tick 自检停止（同网络吞吐卡模式）
let _trackedAutoRefreshTimer = null;
export function stopTrackedAutoRefresh() {
  if (_trackedAutoRefreshTimer) { clearInterval(_trackedAutoRefreshTimer); _trackedAutoRefreshTimer = null; }
}
export function startTrackedAutoRefresh() {
  stopTrackedAutoRefresh();
  _trackedAutoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "security") { stopTrackedAutoRefresh(); return; }
    loadAutoBanStats().catch(() => {});
  }, 5000);
}

export function openAutoBanSettings() {
  openFormModal({
    title: "自动封禁策略",
    size: 640,
    schema: [
      { key: "enabled", label: "启用自动封禁", type: "switch" },
      { key: "window_seconds", label: "统计窗口（秒）", type: "number", default: 60 },
      { key: "max_requests", label: "窗口内最大请求数", type: "number", default: 100 },
      { key: "ban_duration_seconds", label: "封禁时长（秒）", type: "number", default: 3600 },
      { key: "max_404", label: "窗口内最大 404 数", type: "number", default: 20 },
      { key: "auto_ban_on_404", label: "对 404 自动封禁", type: "switch" },
      { key: "whitelist", label: "白名单（逗号分隔）", type: "text", placeholder: "1.2.3.4, 5.6.7.8" },
      { key: "email_on_ban", label: "封禁时发送邮件提醒", type: "switch" },
      { key: "max_mb", label: "窗口内单 IP 流量上限（MB，0 不启用）", type: "number", default: 0, hint: "按流式传输字节数累计，超过即自动封禁" },
    ],
    values: {
      enabled: getValue("auto_ban_enabled") === "1",
      window_seconds: Number(getValue("auto_ban_window_seconds") || 60),
      max_requests: Number(getValue("auto_ban_max_requests") || 100),
      ban_duration_seconds: Number(getValue("auto_ban_ban_duration_seconds") || 3600),
      max_404: Number(getValue("auto_ban_max_404") || 20),
      auto_ban_on_404: getValue("auto_ban_auto_ban_on_404") === "1",
      whitelist: getValue("auto_ban_whitelist") || "",
      email_on_ban: getValue("auto_ban_email_on_ban") === "1",
      max_mb: Number(getValue("auto_ban_max_mb") || 0),
    },
    onSave: async (out) => {
      await apiFetch("/_admin/api/auto-ban", {
        method: "PUT",
        body: JSON.stringify({
          enabled: Boolean(out.enabled),
          window_seconds: Number(out.window_seconds ?? 60),
          max_requests: Number(out.max_requests ?? 100),
          ban_duration_seconds: Number(out.ban_duration_seconds ?? 3600),
          max_404: Number(out.max_404 ?? 20),
          auto_ban_on_404: Boolean(out.auto_ban_on_404),
          whitelist: out.whitelist || "",
          email_on_ban: Boolean(out.email_on_ban),
          max_bytes: Math.max(0, Number(out.max_mb ?? 0)) * 1048576,
        }),
      });
      await Promise.all([loadAutoBanSettings(), loadAutoBanStats()]);
      showToast("自动封禁配置已保存。");
    },
  });
}

// ============ 单 IP 并发限制（HOTLINK_PROTECTION.md 阶段 3.2） ============

export async function loadStreamGuardSettings() {
  try {
    const data = await apiFetch("/_admin/api/stream-guard");
    const body = document.getElementById("streamGuardBody");
    const pill = document.getElementById("streamGuardPill");
    const limit = Number(data.max_concurrent_per_ip || 0);
    setValue("stream_guard_max", String(limit));
    if (pill) {
      pill.className = "pill " + (limit > 0 ? "pill-ok" : "pill-neutral");
      pill.textContent = limit > 0 ? `上限 ${limit}` : "未启用";
    }
    if (body) {
      body.innerHTML = `<div class="kv"><div class="k">单 IP 并发上限</div><div class="val">${limit > 0 ? esc(String(limit)) + " 路流式连接" : "不限制"}</div></div>` +
        `<div style="font-size:12px;color:var(--text-3);line-height:1.7;margin-top:8px">说明：仅对流式通道（视频 / 大文件）计数，0 = 不限制；同一 IP 在途连接达到上限时新请求返回 429，播完自动释放名额。公司 / 校园等共享出口环境建议调高或关闭。点「编辑配置」查看完整说明。</div>`;
    }
  } catch (_) {}
}

export function openStreamGuardSettings() {
  openFormModal({
    title: "单 IP 并发限制",
    size: 560,
    sub: "限制同一 IP 同时进行的流式传输数量，压制单点滥用与脚本并发拉流。",
    schema: [
      { type: "note", text: "【生效范围】只对流式通道（视频 / 大文件）计数，标准转发与后台请求不受影响。\n【生效行为】同一 IP 在途流式连接达到上限时，新请求立即返回 429（带 Retry-After: 5），已在播放的连接不受影响；传输结束自动释放名额。\n【误伤提示】公司 / 校园 / 部分宽带是多人共享同一出口 IP，此类用户多时可调高上限或设为 0 关闭。\n【日志排查】被 429 拒绝的请求可在「日志与审计」按结果状态筛选查看。" },
      { key: "max_concurrent_per_ip", label: "单 IP 流式并发上限", type: "number", default: 0, hint: "0 = 不限制；家庭 / 个人站点建议 2~3，共享出口环境建议 4~6" },
    ],
    values: {
      max_concurrent_per_ip: Number(getValue("stream_guard_max") || 0),
    },
    validate: (out) => {
      if (Number(out.max_concurrent_per_ip ?? 0) < 0) return "并发上限不能为负数";
      return null;
    },
    onSave: async (out) => {
      const data = await apiFetch("/_admin/api/stream-guard", {
        method: "PUT",
        body: JSON.stringify({ max_concurrent_per_ip: Math.max(0, Number(out.max_concurrent_per_ip ?? 0)) }),
      });
      setValue("stream_guard_max", String(data.max_concurrent_per_ip || 0));
      await loadStreamGuardSettings();
      showToast("单 IP 并发限制已保存。");
    },
  });
}

// ============ 登录防爆破（P0-1.2） ============

export async function loadLoginProtectionSettings() {
  try {
    const data = await apiFetch("/_admin/api/login-protection");
    const body = document.getElementById("loginProtectionBody");
    const pill = document.getElementById("loginProtectionPill");
    const maxAttempts = Number(data.max_attempts || 0);
    const lockoutMinutes = Number(data.lockout_minutes || 0);
    const cooldownSeconds = Number(data.cooldown_seconds || 0);
    setValue("login_max_attempts", String(maxAttempts));
    setValue("login_lockout_minutes", String(lockoutMinutes));
    setValue("login_cooldown_seconds", String(cooldownSeconds));
    if (pill) {
      pill.className = "pill " + (maxAttempts > 0 ? "pill-ok" : "pill-neutral");
      pill.textContent = maxAttempts > 0 ? `${maxAttempts} 次 / 锁 ${lockoutMinutes} 分` : "未配置";
    }
    if (body) {
      body.innerHTML =
        `<div class="kv"><div class="k">失败阈值</div><div class="val">${esc(String(maxAttempts))} 次</div></div>` +
        `<div class="kv"><div class="k">锁定时长</div><div class="val">${esc(String(lockoutMinutes))} 分钟</div></div>` +
        `<div class="kv"><div class="k">重试冷却</div><div class="val">${cooldownSeconds > 0 ? esc(String(cooldownSeconds)) + " 秒" : "不限制"}</div></div>` +
        `<div style="font-size:12px;color:var(--text-3);line-height:1.7;margin-top:8px">说明：同一 IP + 账号连续失败达到阈值即锁定该组合（含正确密码也拒绝），返回 429 并带 Retry-After；登录成功即清空计数。部署在反代后请确保透传 X-Forwarded-For / X-Real-IP，否则所有来源会被视为同一地址而互相牵连。</div>`;
    }
  } catch (_) {}
}

export function openLoginProtectionSettings() {
  openFormModal({
    title: "登录防爆破",
    size: 560,
    sub: "限制后台登录的连续失败次数，抵御暴力破解。",
    schema: [
      { type: "note", text: "【生效范围】按「客户端 IP + 账号」组合分别计数，互不影响。\n【生效行为】连续失败达到阈值后，该组合在锁定时长内一律拒绝登录（即使密码正确），返回 429 并附 Retry-After；期满自动恢复。\n【计数清空】任意一次登录成功即清空该组合计数。\n【反代注意】前置 nginx 需透传 X-Forwarded-For / X-Real-IP，否则所有来源被视作同一地址而互相牵连。\n【审计】登录成功会记入审计日志（对象类型：登录）。" },
      { key: "max_attempts", label: "失败次数阈值", type: "number", default: 5, hint: "达到即锁定；建议 3~10" },
      { key: "lockout_minutes", label: "锁定时长（分钟）", type: "number", default: 15, hint: "锁定期间该组合无法登录" },
      { key: "cooldown_seconds", label: "重试冷却（秒）", type: "number", default: 0, hint: "0 = 不额外冷却（预留）" },
    ],
    values: {
      max_attempts: Number(getValue("login_max_attempts") || 5),
      lockout_minutes: Number(getValue("login_lockout_minutes") || 15),
      cooldown_seconds: Number(getValue("login_cooldown_seconds") || 0),
    },
    validate: (out) => {
      if (Number(out.max_attempts ?? 0) < 1) return "失败次数阈值至少为 1";
      if (Number(out.lockout_minutes ?? 0) < 1) return "锁定时长至少为 1 分钟";
      if (Number(out.cooldown_seconds ?? 0) < 0) return "冷却时间不能为负数";
      return null;
    },
    onSave: async (out) => {
      const data = await apiFetch("/_admin/api/login-protection", {
        method: "PUT",
        body: JSON.stringify({
          max_attempts: Math.max(1, Number(out.max_attempts ?? 5)),
          lockout_minutes: Math.max(1, Number(out.lockout_minutes ?? 15)),
          cooldown_seconds: Math.max(0, Number(out.cooldown_seconds ?? 0)),
        }),
      });
      setValue("login_max_attempts", String(data.max_attempts || 0));
      setValue("login_lockout_minutes", String(data.lockout_minutes || 0));
      setValue("login_cooldown_seconds", String(data.cooldown_seconds || 0));
      await loadLoginProtectionSettings();
      showToast("登录防爆破配置已保存。");
    },
  });
}

// ============ 主动速率限制（P1-2.1） ============

export async function loadRateLimitSettings() {
  try {
    const data = await apiFetch("/_admin/api/rate-limit");
    const body = document.getElementById("rateLimitBody");
    const pill = document.getElementById("rateLimitPill");
    const enabled = Boolean(data.enabled);
    const rps = Number(data.requests_per_second || 0);
    const burst = Number(data.burst || 0);
    const perIp = Boolean(data.per_ip);
    setValue("rate_limit_enabled", enabled ? "1" : "0");
    setValue("rate_limit_rps", String(rps));
    setValue("rate_limit_burst", String(burst));
    setValue("rate_limit_per_ip", perIp ? "1" : "0");
    if (pill) {
      pill.className = "pill " + (enabled ? "pill-ok" : "pill-neutral");
      pill.textContent = enabled ? `${rps} 次/秒 · 突发 ${burst}` : "未启用";
    }
    if (body) {
      body.innerHTML =
        `<div class="kv"><div class="k">状态</div><div class="val">${enabled ? "启用" : "未启用"}</div></div>` +
        `<div class="kv"><div class="k">速率</div><div class="val">${esc(String(rps))} 请求/秒</div></div>` +
        `<div class="kv"><div class="k">突发容量</div><div class="val">${esc(String(burst))} 个令牌</div></div>` +
        `<div class="kv"><div class="k">限流维度</div><div class="val">${perIp ? "按客户端 IP" : "全局共享"}</div></div>` +
        `<div style="font-size:12px;color:var(--text-3);line-height:1.7;margin-top:8px">说明：在封禁检查之后、请求去重之前做柔性节流。令牌桶按「速率」持续补充、容量上限为「突发」，瞬时超过即返回 429 + Retry-After。部署在反代后请确保透传 X-Forwarded-For / X-Real-IP，否则所有来源会被视作同一地址而互相牵连。</div>`;
    }
  } catch (_) {}
}

export function openRateLimitSettings() {
  openFormModal({
    title: "主动速率限制",
    size: 560,
    sub: "在封禁之前柔性限流，避免正常突发被误封。",
    schema: [
      { type: "note", text: "【触发顺序】限流在 IP 封禁之后、请求去重之前执行。\n【限流维度】默认按客户端 IP 独立计数；关掉则全局共享一个令牌桶。\n【封顶】requests_per_second 按 0.1~1000 钳制，burst ≥ 1。\n【出口】超限返回 429 + Retry-After，并在请求日志中记为 rate_limited。\n【反代注意】前置 nginx 需透传 X-Forwarded-For / X-Real-IP。" },
      { key: "enabled", label: "启用限流", type: "switch", default: false },
      { key: "requests_per_second", label: "速率（请求/秒）", type: "number", default: 10, hint: "令牌桶填充速率，建议 5~50" },
      { key: "burst", label: "突发容量", type: "number", default: 20, hint: "允许瞬时可消耗的令牌数" },
      { key: "per_ip", label: "按客户端 IP 限流", type: "switch", default: true, hint: "关闭则全局共享一个桶" },
    ],
    values: {
      enabled: getValue("rate_limit_enabled") === "1",
      requests_per_second: Number(getValue("rate_limit_rps") || 10),
      burst: Number(getValue("rate_limit_burst") || 20),
      per_ip: getValue("rate_limit_per_ip") !== "0",
    },
    validate: (out) => {
      if (out.enabled) {
        if (Number(out.requests_per_second ?? 0) <= 0) return "速率必须大于 0";
        if (Number(out.burst ?? 0) < 1) return "突发容量至少为 1";
      }
      return null;
    },
    onSave: async (out) => {
      const data = await apiFetch("/_admin/api/rate-limit", {
        method: "PUT",
        body: JSON.stringify({
          enabled: Boolean(out.enabled),
          requests_per_second: Math.max(0.1, Number(out.requests_per_second ?? 10)),
          burst: Math.max(1, Number(out.burst ?? 20)),
          per_ip: Boolean(out.per_ip),
        }),
      });
      setValue("rate_limit_enabled", data.enabled ? "1" : "0");
      setValue("rate_limit_rps", String(data.requests_per_second || 0));
      setValue("rate_limit_burst", String(data.burst || 0));
      setValue("rate_limit_per_ip", data.per_ip ? "1" : "0");
      await loadRateLimitSettings();
      showToast("主动速率限制配置已保存。");
    },
  });
}

// ============ 跨域资源共享 CORS（P2-3.2） ============

export async function loadCorsSettings() {
  try {
    const data = await apiFetch("/_admin/api/cors");
    const body = document.getElementById("corsBody");
    const pill = document.getElementById("corsPill");
    const enabled = Boolean(data.enabled);
    const origins = String(data.allowed_origins || "");
    setValue("cors_enabled", enabled ? "1" : "0");
    setValue("cors_allowed_origins", origins);
    setValue("cors_allowed_methods", String(data.allowed_methods || "GET,HEAD,OPTIONS"));
    setValue("cors_allow_credentials", data.allow_credentials ? "1" : "0");
    setValue("cors_max_age", String(data.max_age ?? 600));
    if (pill) {
      pill.className = "pill " + (enabled ? "pill-ok" : "pill-neutral");
      pill.textContent = enabled ? (origins === "*" ? "允许全部来源" : `${origins.split(",").filter(Boolean).length} 个来源`) : "未启用";
    }
    if (body) {
      body.innerHTML =
        `<div class="kv"><div class="k">状态</div><div class="val">${enabled ? "启用" : "未启用"}</div></div>` +
        `<div class="kv"><div class="k">允许来源</div><div class="val" style="word-break:break-all">${esc(origins || "—")}</div></div>` +
        `<div class="kv"><div class="k">允许方法</div><div class="val">${esc(String(data.allowed_methods || ""))}</div></div>` +
        `<div class="kv"><div class="k">携带凭据</div><div class="val">${data.allow_credentials ? "允许" : "不允许"}</div></div>` +
        `<div style="font-size:12px;color:var(--text-3);line-height:1.7;margin-top:8px">说明：只作用于代理路径，管理后台（/_admin）一律不允许跨域。预检 OPTIONS 请求直接返回 204，不占用上游与限流配额。规则可在「代理规则」里按条目覆盖允许来源（留空继承此处全局配置）。</div>`;
    }
  } catch (_) {}
}

export function openCorsSettings() {
  openFormModal({
    title: "跨域资源共享 (CORS)",
    size: 560,
    sub: "允许浏览器跨源直接拉取代理资源；管理接口不受影响。",
    schema: [
      { type: "note", text: "【作用范围】仅代理路径；/_admin 永不注入 CORS 头。\n【来源】逗号分隔，如 https://a.com, https://b.com；填 * 表示全部（此时不能开启凭据）。\n【预检】浏览器预检 OPTIONS 由本服务直接应答 204，不转发上游。" },
      { key: "enabled", label: "启用 CORS", type: "switch", default: false },
      { key: "allowed_origins", label: "允许来源", type: "text", placeholder: "https://app.example.com 或 *", hint: "逗号分隔多个来源" },
      { key: "allowed_methods", label: "允许方法", type: "text", default: "GET,HEAD,OPTIONS" },
      { key: "allow_credentials", label: "允许携带凭据", type: "switch", default: false, hint: "Cookie / Authorization；来源为 * 时不可开启" },
      { key: "max_age", label: "预检缓存（秒）", type: "number", default: 600 },
    ],
    values: {
      enabled: getValue("cors_enabled") === "1",
      allowed_origins: getValue("cors_allowed_origins") || "",
      allowed_methods: getValue("cors_allowed_methods") || "GET,HEAD,OPTIONS",
      allow_credentials: getValue("cors_allow_credentials") === "1",
      max_age: Number(getValue("cors_max_age") || 600),
    },
    validate: (out) => {
      if (out.enabled) {
        if (!String(out.allowed_origins || "").trim()) return "启用 CORS 时必须填写允许来源";
        const creds = Boolean(out.allow_credentials);
        const wildcard = String(out.allowed_origins).split(",").map((s) => s.trim()).includes("*");
        if (creds && wildcard) return "来源为 * 时不能开启「允许携带凭据」（CORS 规范禁止）";
      }
      return null;
    },
    onSave: async (out) => {
      const data = await apiFetch("/_admin/api/cors", {
        method: "PUT",
        body: JSON.stringify({
          enabled: Boolean(out.enabled),
          allowed_origins: String(out.allowed_origins || "").trim(),
          allowed_methods: String(out.allowed_methods || "GET,HEAD,OPTIONS").trim(),
          allow_credentials: Boolean(out.allow_credentials),
          max_age: Math.max(0, Number(out.max_age ?? 600) || 0),
        }),
      });
      setValue("cors_enabled", data.enabled ? "1" : "0");
      setValue("cors_allowed_origins", String(data.allowed_origins || ""));
      setValue("cors_allowed_methods", String(data.allowed_methods || ""));
      setValue("cors_allow_credentials", data.allow_credentials ? "1" : "0");
      setValue("cors_max_age", String(data.max_age ?? 600));
      await loadCorsSettings();
      showToast("CORS 配置已保存。");
    },
  });
}

// ============ Webhook / IM 告警通知（P1-2.4） ============

const NOTIFY_TYPE_LABELS = { generic: "通用 Webhook", feishu: "飞书机器人", dingtalk: "钉钉机器人", slack: "Slack" };
const NOTIFY_TYPES = [
  { type: "generic", hasSecret: false },
  { type: "feishu", hasSecret: true },
  { type: "dingtalk", hasSecret: true },
  { type: "slack", hasSecret: false },
];

export async function loadNotificationsSettings() {
  try {
    const data = await apiFetch("/_admin/api/notifications");
    const pill = document.getElementById("notifyPill");
    const body = document.getElementById("notifyBody");
    const enabled = Boolean(data.enabled);
    const channels = data.channels || [];
    state.notifyChannels = channels;
    setValue("notify_enabled", enabled ? "1" : "0");
    if (pill) {
      pill.className = "pill " + (enabled ? "pill-ok" : "pill-neutral");
      const active = channels.filter((c) => c.enabled && c.url).length;
      pill.textContent = enabled ? (active ? `${active} 个启用渠道` : "无启用渠道") : "未启用";
    }
    if (body) {
      body.innerHTML = channels.length
        ? channels.map((c) =>
          `<div class="kv"><div class="k">${esc(NOTIFY_TYPE_LABELS[c.type] || c.type)}${c.name ? ` · ${esc(c.name)}` : ""}</div>`
          + `<div class="val">${c.enabled ? '<span class="pill pill-ok">启用</span>' : '<span class="pill pill-neutral">停用</span>'}`
          + `<span class="hint" style="margin-left:8px">${esc(c.url)}</span></div></div>`).join("")
        : '<div class="hint">尚未配置任何 Webhook 渠道。支持飞书 / 钉钉 / Slack / 通用 Webhook，封禁事件自动推送。</div>';
    }
  } catch (_) {}
}

export function openNotificationsSettings() {
  const channels = state.notifyChannels || [];
  const byType = {};
  channels.forEach((c) => { byType[c.type] = c; });
  const schema = [
    { type: "note", text: "【总开关】关闭后所有 Webhook 渠道都不推送（邮件不受影响）。\n【渠道】只保存填写了 URL 的渠道；type 固定为对应平台，签名密钥仅飞书/钉钉需要。\n【触发】自动封禁 / 后台手动封禁时广播；可用下方「发送 Webhook 测试」验证连通性。" },
    { key: "enabled", label: "启用 Webhook 告警", type: "switch", default: false },
    ...NOTIFY_TYPES.flatMap(({ type, hasSecret }) => {
      const label = NOTIFY_TYPE_LABELS[type];
      return [
        { key: `${type}_enabled`, label: `${label} · 启用`, type: "switch", default: false },
        { key: `${type}_url`, label: `${label} · Webhook 地址`, type: "text", placeholder: type === "feishu" ? "https://open.feishu.cn/open-apis/bot/v2/hook/xxx" : type === "dingtalk" ? "https://oapi.dingtalk.com/robot/send?access_token=xxx" : "https://example.com/hook" },
        ...(hasSecret ? [{ key: `${type}_secret`, label: `${label} · 签名密钥`, type: "text", placeholder: "加签密钥（未启用加签可留空）" }] : []),
      ];
    }),
  ];
  const values = { enabled: getValue("notify_enabled") === "1" };
  NOTIFY_TYPES.forEach(({ type }) => {
    const ch = byType[type] || {};
    values[`${type}_enabled`] = Boolean(ch.enabled);
    values[`${type}_url`] = ch.url || "";
    values[`${type}_secret`] = ch.secret || "";
  });

  openFormModal({
    title: "Webhook / IM 告警",
    size: 640,
    schema,
    values,
    validate: (out) => {
      for (const { type } of NOTIFY_TYPES) {
        if (out[`${type}_enabled`] && !String(out[`${type}_url`] || "").trim()) {
          return `${NOTIFY_TYPE_LABELS[type]}已启用但未填写 Webhook 地址`;
        }
      }
      return null;
    },
    onSave: async (out) => {
      const channels = NOTIFY_TYPES
        .map(({ type }) => ({
          type,
          name: "",
          enabled: Boolean(out[`${type}_enabled`]),
          url: String(out[`${type}_url`] || "").trim(),
          secret: String(out[`${type}_secret`] || "").trim(),
        }))
        .filter((c) => c.url);
      const data = await apiFetch("/_admin/api/notifications", {
        method: "PUT",
        body: JSON.stringify({ enabled: Boolean(out.enabled), channels }),
      });
      state.notifyChannels = data.channels || [];
      setValue("notify_enabled", data.enabled ? "1" : "0");
      await loadNotificationsSettings();
      showToast("Webhook 告警配置已保存。");
    },
  });
}

export async function testNotifications() {
  const el = document.getElementById("notifyTestResult");
  const channels = (state.notifyChannels || []).filter((c) => c.enabled && c.url);
  if (!channels.length) {
    if (el) el.textContent = "没有已启用的渠道，请先编辑配置。";
    return;
  }
  if (el) el.textContent = "发送中……";
  const results = [];
  for (const c of channels) {
    try {
      const r = await apiFetch("/_admin/api/notifications/test", { method: "POST", body: JSON.stringify(c) });
      results.push(`${NOTIFY_TYPE_LABELS[c.type] || c.type}：${r.ok ? "✅ 发送成功" : `❌ ${r.message}`}`);
    } catch (e) {
      results.push(`${NOTIFY_TYPE_LABELS[c.type] || c.type}：❌ ${e.message}`);
    }
  }
  if (el) el.innerHTML = results.map((s) => `<div>${esc(s)}</div>`).join("");
}

// ============ 配置版本历史 / 导出导入（P2-3.5） ============

export async function loadSettingsHistory() {
  const module = getValue("settings_history_module") || document.getElementById("settingsModuleSelect")?.value || "rate-limit";
  setValue("settings_history_module", module);
  await renderSettingsHistory(module);
}

async function renderSettingsHistory(module) {
  const body = document.getElementById("settingsHistoryBody");
  if (!body) return;
  try {
    const data = await apiFetch(`/_admin/api/settings-history/${encodeURIComponent(module)}?limit=10`);
    const items = data.items || [];
    body.innerHTML = items.length
      ? items.map((h) =>
        `<div class="kv"><div class="k">#${h.id} · ${esc(String(h.created_at || "").replace("T", " "))}</div>`
        + `<div class="val"><span class="hint">${esc(h.changed_by || "system")}</span>`
        + `<button class="btn btn-sm" data-action="rollback-history" data-module="${esc(module)}" data-id="${h.id}">回滚</button></div></div>`
      ).join("")
      : `<div class="hint">该模块暂无变更历史。保存一次配置后即可在此回滚。</div>`;
  } catch (_) {
    body.innerHTML = '<div class="hint">历史加载失败。</div>';
  }
}

export async function rollbackSettingsHistory(module, historyId) {
  if (!module || !Number.isFinite(historyId)) return;
  // 回滚会把配置整体恢复为历史快照，误点会造成配置回退——先确认
  if (!window.confirm(`确认把「${module}」配置回滚到历史版本 #${historyId} 吗？`)) return;
  try {
    await apiFetch(`/_admin/api/settings-history/${encodeURIComponent(module)}/rollback`, {
      method: "POST",
      body: JSON.stringify({ history_id: historyId }),
    });
    showToast("已回滚到指定历史版本。");
    await loadSettingsHistory();
  } catch (e) {
    showToast(e.message || "回滚失败", true);
  }
}

export async function exportSettingsModule() {
  const module = document.getElementById("settingsModuleSelect")?.value || "rate-limit";
  const data = await apiFetch(`/_admin/api/settings-export/${encodeURIComponent(module)}`);
  const text = JSON.stringify(data, null, 2);
  try {
    const blob = new Blob([text], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${module}-config-${new Date().toISOString().slice(0, 10)}.json`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  } catch (_) {}
  showToast("配置已导出（下载 JSON 文件）。");
}

export function openSettingsImport() {
  const module = document.getElementById("settingsModuleSelect")?.value || "rate-limit";
  openFormModal({
    title: `导入配置：${module}`,
    size: 560,
    sub: "粘贴此前导出的 JSON 配置；导入等价于一次普通保存（含校验与钳制）。",
    schema: [
      { key: "payload_text", label: "配置 JSON", type: "textarea", required: true, placeholder: '{\n  "enabled": true, ...\n}' },
    ],
    validate: (out) => {
      try {
        const v = JSON.parse(out.payload_text);
        if (!v || typeof v !== "object" || Array.isArray(v)) return "必须是 JSON 对象";
      } catch (_) {
        return "JSON 解析失败，请检查格式";
      }
      return null;
    },
    onSave: async (out) => {
      const parsed = JSON.parse(out.payload_text);
      await apiFetch(`/_admin/api/settings-import/${encodeURIComponent(module)}`, {
        method: "POST",
        body: JSON.stringify(parsed.payload ? parsed : { payload: parsed }),
      });
      showToast("配置导入成功。");
      await loadSettingsHistory();
    },
  });
}

// ============ 管理操作审计日志（P0-1.3） ============

const AUDIT_ACTION_LABELS = {
  create: "新增",
  update: "修改",
  delete: "删除",
  login: "登录",
};

// 动作语义配色：新增=绿 / 修改=蓝 / 删除=红 / 登录=中性灰；未知动作回落中性
const AUDIT_ACTION_PILLS = {
  create: "pill-ok",
  update: "pill-info",
  delete: "pill-danger",
  login: "pill-neutral",
};

const AUDIT_TARGET_LABELS = {
  rule: "转发规则",
  route_group: "路由组",
  banned_ip: "封禁 IP",
  api_key: "API 密钥",
  backup: "备份",
  email_settings: "邮件配置",
  log_settings: "日志设置",
  logging_settings: "运行日志",
  ip_cache_settings: "结果缓存",
  dedup_settings: "请求去重",
  auto_ban_settings: "自动封禁",
  stream_guard_settings: "并发限制",
  signed_url_settings: "签名 URL",
  redirect_signing_settings: "302 加签",
  geoip: "IP 定位",
  route_log: "请求日志",
  app_log: "应用日志",
  auth: "登录",
};

const AUDIT_ACTOR_LABELS = {
  session: "后台会话",
  apikey: "API 密钥",
  anonymous: "未登录",
};

export function collectAuditFilters() {
  return {
    keyword: getValue("auditKeyword"),
    action: getValue("auditAction"),
    target_type: getValue("auditTargetType"),
    date_from: getValue("auditDateFrom"),
    date_to: getValue("auditDateTo"),
    page: state.auditCurrentPage || 1,
    limit: Number(getValue("auditPageSize") || 20),
  };
}

function buildAuditQuery(filters) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value !== "" && value !== null && value !== undefined) params.set(key, String(value));
  });
  return params.toString();
}

export async function loadAuditLogs() {
  const query = buildAuditQuery(collectAuditFilters());
  const payload = await apiFetch(`/_admin/api/audit-logs${query ? `?${query}` : ""}`);
  renderAuditLogs(payload || { items: [], total: 0 });
}

export async function goToAuditPage(page) {
  page = Math.max(1, Math.min(state.auditTotalPages || 1, page));
  state.auditCurrentPage = page;
  await loadAuditLogs();
}

export function renderAuditLogs(payload) {
  const items = Array.isArray(payload.items) ? payload.items : [];
  const total = payload.total ?? items.length;
  const totalPages = payload.total_pages ?? 1;
  const currentPage = payload.page ?? 1;
  const limit = payload.limit ?? 20;
  state.auditTotalPages = totalPages;
  state.auditCurrentPage = currentPage;

  const startOffset = (currentPage - 1) * limit + 1;
  const endOffset = Math.min(currentPage * limit, total);
  setText("auditTotalCount", total > 0 ? `共 ${total} 条（${startOffset}-${endOffset} / ${total}）` : "共 0 条");

  const container = document.getElementById("auditListBody");
  if (!container) return;
  container.innerHTML = "";
  if (!items.length) {
    container.innerHTML = '<div class="route-log-empty">暂无审计记录。</div>';
    renderPagination(1, 1, "auditPagination", goToAuditPage);
    return;
  }

  items.forEach((log) => {
    const actorTypeText = AUDIT_ACTOR_LABELS[log.actor_type] || log.actor_type || "-";
    const actionText = AUDIT_ACTION_LABELS[log.action] || log.action || "-";
    const targetText = AUDIT_TARGET_LABELS[log.target_type] || log.target_type || "-";
    const card = document.createElement("article");
    card.className = "route-log-item";
    card.innerHTML = `
      <div class="route-log-item-main">
        <div class="route-log-item-body">
          <div class="route-log-item-header">
            <div class="route-log-item-time"><strong>${esc(formatDateTime(log.created_at))}</strong></div>
          </div>
          <div class="route-log-item-fields">
            <div class="route-log-field"><span class="route-log-field-label">操作者</span><div class="route-log-field-value"><strong>${esc(log.actor_id || "-")}</strong><span class="hint">${esc(actorTypeText)}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">动作</span><div class="route-log-field-value"><span class="pill ${AUDIT_ACTION_PILLS[log.action] || "pill-neutral"}">${esc(actionText)}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">对象</span><div class="route-log-field-value"><strong>${esc(targetText)}</strong>${log.target_id ? `<span class="hint">${esc(log.target_id)}</span>` : ""}</div></div>
            <div class="route-log-field"><span class="route-log-field-label">详情</span><div class="route-log-field-value"><span class="route-log-chain" title="${esc(log.detail || "")}">${esc(log.detail || "-")}</span></div></div>
          </div>
        </div>
      </div>`;
    container.appendChild(card);
  });
  renderPagination(state.auditCurrentPage, state.auditTotalPages, "auditPagination", goToAuditPage);
}

// ============ 签名 URL（HOTLINK_PROTECTION.md 阶段 4） ============

export async function loadSignedUrlSettings() {
  try {
    const data = await apiFetch("/_admin/api/signed-url");
    const body = document.getElementById("signedUrlBody");
    const pill = document.getElementById("signedUrlPill");
    setValue("signed_url_enabled", data.enabled ? "1" : "0");
    setValue("signed_url_ttl_seconds", String(data.ttl_seconds || 3600));
    if (pill) {
      pill.className = "pill " + (data.enabled ? "pill-ok" : "pill-neutral");
      pill.textContent = data.enabled ? "已启用" : "未启用";
    }
    if (body) {
      const ttlSec = data.ttl_seconds || 3600;
      const ttlHuman = formatTtl(ttlSec);
      body.innerHTML =
        // 顶部色带：明确这是「入口校验」—— 播放器→代理方向
        `<div class="feature-banner in"><span class="dir">↓ 入口校验</span><span class="bt">对进入代理的请求做签名校验</span></div>` +
        `<div class="feature-purpose"><b>做什么：</b>所有代理请求必须携带有效签名 <code>?_st=…&amp;_sig=…</code>，否则直接返回 403，不放行到上游。</div>` +
        // 流向示意：把校验环节高亮出来
        `<div class="feature-flow"><span>客户端</span><span class="arrow">→</span><span class="step in">验签</span><span class="arrow">→</span><span>代理转发</span><span class="arrow">→</span><span>上游</span></div>` +
        `<div class="feature-block-title">适用与注意</div>` +
        `<div class="feature-applies"><b>适用：</b>播放链路能拿到带签名的链接（需配合「302 加签改写」等签发层）。</div>` +
        `<div class="feature-applies warn"><b>慎用：</b>固定 URL 直连代理的播放器开启后会被误拦 —— 此功能不签发链接，只校验。</div>` +
        // 当前配置
        `<div class="feature-block-title">当前配置</div>` +
        `<div class="kv"><div class="k">状态</div><div class="val">${data.enabled ? '<span class="pill pill-ok" style="font-size:11px">已启用</span> 未签名请求将被拒绝<div class="val-sub">关闭后不校验签名，行为与旧版一致</div>' : '<span class="pill pill-neutral" style="font-size:11px">未启用</span> 不校验签名<div class="val-sub">当前行为与旧版完全一致</div>'}</div></div>` +
        `<div class="kv"><div class="k">链接有效期</div><div class="val">${esc(ttlHuman)}<div class="val-sub">${ttlSec} 秒；建议 ≥ 预计播放时长 + 30 分钟</div></div></div>` +
        `<div class="kv"><div class="k">签名密钥</div><div class="val">${data.has_secret ? "已生成<div class=\"val-sub\">HMAC-SHA256，仅存服务端数据库，签发结果不含密钥</div>" : '<span class="text-warn">未生成</span>'}</div></div>` +
        `<div class="feature-desc">完整说明与使用流程见编辑弹窗「开启前必读」与「生成签名链接」弹窗。</div>`;
    }
  } catch (_) {}
}

export function openSignedUrlSettings() {
  openFormModal({
    title: "签名 URL 配置",
    size: 580,
    sub: "启用后，所有代理请求必须携带有效签名（?_st=...&_sig=...），否则返回 403。",
    schema: [
      { type: "note", text: "【开启前必读】\n1. 签名只对「能拿到带签名链接」的播放链路有效。若你的播放器 / Emby 使用固定 URL 直连代理，开启后正常用户也会被 403 —— 此功能需配合 302 签发层使用（固定 URL → 鉴权 → 302 跳转到带签名链接），单独开启会拦截固定 URL 的正常用户。\n2. 使用流程：开启开关 → 点「生成签名链接」输入代理路径 → 得到相对链接 → 拼上访问域名后下发给播放器。\n3. 链接过期即 403，需重新签发；TTL 建议 ≥ 预计播放时长 + 30 分钟。\n4. 密钥仅存服务端数据库，签发结果不含密钥；签名采用 HMAC-SHA256，无法伪造。" },
      { key: "enabled", label: "启用签名校验", type: "switch", hint: "全局开关；关闭后所有请求不校验签名（行为与旧版完全一致）" },
      { key: "ttl_seconds", label: "链接有效期（秒）", type: "number", default: 3600, hint: "签发链接的有效秒数，最小 60；例：2 小时影片建议 9000（2.5 小时）" },
      { key: "regenerate_secret", label: "保存时重新生成密钥", type: "switch", hint: "立即作废所有已签发链接；正在播放的连接不受影响，新请求需重新签发，请谨慎勾选" },
    ],
    values: {
      enabled: getValue("signed_url_enabled") === "1",
      ttl_seconds: Number(getValue("signed_url_ttl_seconds") || 3600),
      regenerate_secret: false,
    },
    validate: (out) => {
      if (Number(out.ttl_seconds ?? 0) < 60) return "有效期不能小于 60 秒";
      return null;
    },
    onSave: async (out) => {
      const data = await apiFetch("/_admin/api/signed-url", {
        method: "PUT",
        body: JSON.stringify({
          enabled: Boolean(out.enabled),
          ttl_seconds: Math.max(60, Number(out.ttl_seconds ?? 3600)),
          regenerate_secret: Boolean(out.regenerate_secret),
        }),
      });
      if (data && data.has_secret !== undefined) setValue("signed_url_enabled", data.enabled ? "1" : "0");
      await loadSignedUrlSettings();
      showToast(out.regenerate_secret ? "签名 URL 配置已保存，密钥已轮换。" : "签名 URL 配置已保存。");
    },
  });
}

export function openSignedUrlTool() {
  const modalEl = document.getElementById("modal");
  const mask = document.getElementById("modalMask");
  if (!modalEl || !mask) return;
  modalEl.style.width = "min(560px,100%)";
  modalEl.innerHTML = `
    <div class="modal-head"><div><div class="modal-title">生成签名链接</div><div class="modal-sub">输入代理路径，服务端签发带时效的签名 URL（密钥不出浏览器）</div></div><button class="icon-btn" id="modalClose">✕</button></div>
    <div class="modal-body">
      <div class="form-note">1. 输入以 / 开头的代理路径（如 /d/xxx.m3u8），服务端用密钥签发，密钥不出浏览器；\n2. 生成的是相对链接，需拼上访问域名后下发给播放器；\n3. 链接在「链接有效期」（TTL）内有效，过期自动 403，需重新签发；\n4. 签名开关未启用时链接同样可以签发（只是不强制校验），适合先生成测试、再正式启用。</div>
      <div class="kv"><div class="k">代理路径</div><div class="val"><input class="input" id="signedUrlPath" placeholder="/d/xxx.m3u8" style="width:100%;box-sizing:border-box"></div></div>
      <div id="signedUrlResult" style="display:none;margin-top:12px">
        <div class="kv"><div class="k">签名链接</div><div class="val"><textarea class="input" id="signedUrlOutput" readonly rows="3" style="width:100%;box-sizing:border-box;font-family:var(--mono,monospace);font-size:12px"></textarea></div></div>
      </div>
    </div>
    <div class="modal-foot"><button class="btn" id="modalCancel">关闭</button><button class="btn btn-primary" id="signedUrlGenerate">生成</button><button class="btn" id="signedUrlCopy" style="display:none">复制链接</button></div>`;
  document.getElementById("modalClose").onclick = closeModal;
  document.getElementById("modalCancel").onclick = closeModal;

  const input = document.getElementById("signedUrlPath");
  const resultWrap = document.getElementById("signedUrlResult");
  const output = document.getElementById("signedUrlOutput");
  const copyBtn = document.getElementById("signedUrlCopy");
  const genBtn = document.getElementById("signedUrlGenerate");

  genBtn.onclick = async () => {
    const path = (input.value || "").trim();
    if (!path.startsWith("/")) { showToast("路径必须以 / 开头", true); return; }
    genBtn.disabled = true;
    try {
      const data = await apiFetch("/_admin/api/signed-url/generate", {
        method: "POST",
        body: JSON.stringify({ path }),
      });
      if (data && data.url) {
        output.value = data.url;
        resultWrap.style.display = "";
        copyBtn.style.display = "";
      } else {
        showToast(data && data.error ? data.error : "生成失败", true);
      }
    } catch (e) {
      showToast(e && e.message ? e.message : "生成失败", true);
    } finally {
      genBtn.disabled = false;
    }
  };
  copyBtn.onclick = () => {
    if (!output.value) return;
    navigator.clipboard?.writeText(output.value).then(
      () => showToast("已复制到剪贴板。"),
      () => showToast("复制失败，请手动复制。", true),
    );
  };
  mask.classList.add("open");
  window.setTimeout(() => input.focus(), 50);
}

// ============ 302 加签改写 ============

export async function loadRedirectSigningSettings() {
  try {
    const data = await apiFetch("/_admin/api/redirect-signing");
    const body = document.getElementById("redirectSigningBody");
    const pill = document.getElementById("redirectSigningPill");
    setValue("redirect_signing_enabled", data.enabled ? "1" : "0");
    setValue("redirect_signing_ttl_seconds", String(data.ttl_seconds || 21600));
    setValue("redirect_signing_bind_ip", data.bind_ip ? "1" : "0");
    setValue("public_base_url", data.base_url || "");
    if (pill) {
      pill.className = "pill " + (data.enabled ? "pill-ok" : "pill-neutral");
      pill.textContent = data.enabled ? "已启用" : "未启用";
    }
    if (body) {
      const ttlSec = data.ttl_seconds || 21600;
      const ttlHuman = formatTtl(ttlSec);
      const baseUrl = data.base_url || "";
      body.innerHTML =
        // 顶部色带：明确这是「出口改写」—— 代理→客户端方向；长限定语放「做什么」，banner 只留短句防挤压
        `<div class="feature-banner out"><span class="dir">↑ 出口改写</span><span class="bt">跳转一律改写为本系统签名链接</span></div>` +
        `<div class="feature-purpose"><b>做什么：</b>开启后，播放器的<b>首次代理请求一律改写</b>为 <code>{base_url}/_signed/{资源id}?_st&amp;_sig</code>，客户端始终只见系统签名链接，裸的上游 / CDN 地址不再外泄。跟随型 / 本地代理规则同样生效。</div>` +
        // 流向示意：签发环节高亮
        `<div class="feature-flow"><span>客户端</span><span class="arrow">→</span><span>代理</span><span class="arrow">→</span><span>上游</span><span class="arrow">→</span><span class="step out">改写签名</span><span class="arrow">→</span><span>客户端领取</span></div>` +
        // A/B 双模式：独立分区 + 徽章行，替代堆在一起的灰色提示块
        `<div class="feature-block-title">领取后的两种出流模式</div>` +
        `<div class="mode-row"><span class="mode-badge">A</span><span><b>不跟随型规则</b> —— 领取时换回签发时缓存的上游 302，客户端直连 CDN，媒体不过本机。</span></div>` +
        `<div class="mode-row"><span class="mode-badge">B</span><span><b>跟随型 / 本地代理</b> —— 领取后由本系统凭签发快照内部代理穿流，媒体经本机回传。</span></div>` +
        `<div class="feature-block-title">适用与注意</div>` +
        `<div class="feature-applies"><b>适用：</b>隐藏上游 / CDN 地址，阻断「固定 URL 直连代理」的盗链与抓包；重放原始地址或命中结果缓存时同样重新签发，无法绕过。</div>` +
        `<div class="feature-applies warn"><b>注意：</b>「对外基础地址」须填写播放器可达的地址，否则回退请求 Host（经反代可能是内网地址）；IP 绑定开启后手机切网需重新取地址。</div>` +
        // 当前配置：主值短句 + val-sub 副行补充，避免 pill 后拖长串
        `<div class="feature-block-title">当前配置</div>` +
        `<div class="kv"><div class="k">状态</div><div class="val">${data.enabled ? '<span class="pill pill-ok" style="font-size:11px">已启用</span> 首次请求一律改写为签名链接<div class="val-sub">A 不跟随型＝领取换 302 直连；B 跟随型＝领取后本机穿流</div>' : '<span class="pill pill-neutral" style="font-size:11px">未启用</span> 不改写，行为与旧版一致<div class="val-sub">不跟随型原样透传上游 302；跟随型内部跟随直出</div>'}</div></div>` +
        `<div class="kv"><div class="k">签名链接有效期</div><div class="val">${esc(ttlHuman)}<div class="val-sub">${ttlSec} 秒；须覆盖完整观看会话（播放器用同一 URL 持续发 Range）</div></div></div>` +
        `<div class="kv"><div class="k">IP 绑定</div><div class="val">${data.bind_ip ? '<span class="pill pill-ok" style="font-size:11px">开启</span> 领取与使用必须同 IP，否则 403<div class="val-sub">_ip 为盲化令牌（密钥哈希），链接不含明文客户端 IP</div>' : '<span class="pill pill-neutral" style="font-size:11px">关闭</span> 不校验 IP<div class="val-sub">链接转分享后仍可领取使用</div>'}</div></div>` +
        `<div class="kv"><div class="k">对外基础地址</div><div class="val">${baseUrl ? esc(baseUrl) : '<span class="text-warn">未配置（回退请求 Host 头）</span>'}</div></div>` +
        `<div class="feature-desc">完整工作原理与场景说明见「编辑配置」弹窗首屏。</div>`;
    }
  } catch (_) {}
}

export function openRedirectSigningSettings() {
  openFormModal({
    title: "302 加签改写配置",
    size: 620,
    sub: "把对外返回的跳转改写为系统固定签名链接（所有代理规则生效，含跟随上游的本地代理），收回最终跳转权，阻断固定 URL 盗链。",
    schema: [
      { type: "note", text: "【工作原理】\n1. 开启后，无论代理规则是否「跟随上游」，播放器的首次代理请求都会被改写为固定签名链接：{base_url}/_signed/{资源id}?_st=...&_ip=...&_sig=...（_ip 为盲化令牌，链接不含明文客户端 IP）。\n2. 播放器跟随链接回到 /_signed/{资源id} → 验签（时效 + IP 盲化令牌比对）→ 按签发时快照双模式出流：\n　· A 模式（规则不跟随上游）：换回签发时缓存的上游 302，客户端直连 CDN，媒体不过本机；\n　· B 模式（规则跟随上游/本地代理）：本系统凭快照内部代理穿流，媒体经本机回传。\n3. 防绕过：签发后播放器再请求原始地址（含命中请求结果缓存）一律重新签发签名链接，无法跳过签名拿到媒体；签发后删除/修改规则也不影响已签发链接领取（快照解耦）。\n4. 客户端全程只见系统签名链接，裸的上游/CDN 地址不再外泄；链接有 TTL 且与领取 IP 绑定，转分享/抓包过期即 403。\n【注意事项】\n5. 需正确填写「对外基础地址」（如 https://media.example.com），否则回退使用请求 Host（经反代时可能是内网地址导致播放器无法访问）。\n6. IP 绑定开启后，手机 Wi-Fi/流量切换会换 IP，续播需重新取地址（播放器会自动重走链路）。\n7. 该功能与签名 URL（入口强签）相互独立；关闭后行为与现状完全一致。" },
      { key: "enabled", label: "启用 302 加签改写", type: "switch", hint: "全局开关；开启后所有代理规则（含跟随上游的本地代理）首次请求均改写为签名链接；关闭后行为与旧版一致" },
      { key: "base_url", label: "对外基础地址", type: "text", default: "", hint: "播放器能访问到的对外地址，如 https://media.example.com；留空则回退请求 Host 头" },
      { key: "ttl_seconds", label: "签名链接有效期（秒）", type: "number", default: 21600, hint: "须覆盖完整观看会话（播放器会用同一 URL 持续发 Range）；默认 6 小时 = 21600" },
      { key: "bind_ip", label: "绑定客户端 IP", type: "switch", hint: "领取与使用签名链接的 IP 必须一致，否则 403；转分享即失效。链接中 _ip 为盲化令牌（密钥哈希），不含明文客户端 IP" },
    ],
    values: {
      enabled: getValue("redirect_signing_enabled") === "1",
      base_url: getValue("public_base_url") || "",
      ttl_seconds: Number(getValue("redirect_signing_ttl_seconds") || 21600),
      bind_ip: getValue("redirect_signing_bind_ip") !== "0",
    },
    validate: (out) => {
      if (Number(out.ttl_seconds ?? 0) < 60) return "有效期不能小于 60 秒";
      return null;
    },
    onSave: async (out) => {
      await apiFetch("/_admin/api/redirect-signing", {
        method: "PUT",
        body: JSON.stringify({
          enabled: Boolean(out.enabled),
          base_url: (out.base_url || "").trim().replace(/\/+$/, ""),
          ttl_seconds: Math.max(60, Number(out.ttl_seconds ?? 21600)),
          bind_ip: Boolean(out.bind_ip),
        }),
      });
      await loadRedirectSigningSettings();
      showToast("302 加签改写配置已保存。");
    },
  });
}

// ============ 邮件提醒 ============

export async function loadEmailSettings() {
  try {
    const data = await apiFetch("/_admin/api/email");
    if (data) {
      setValue("email_enabled", data.enabled ? "1" : "0");
      setValue("email_smtp_host", data.smtp_host || "");
      setValue("email_smtp_port", String(data.smtp_port || 465));
      setValue("email_smtp_ssl", data.smtp_ssl ? "1" : "0");
      setValue("email_sender", data.sender || "");
      setValue("email_sender_name", data.sender_name || "");
      setValue("email_password", data.password || "");
      setValue("email_recipients", data.recipients || "");
      setValue("email_block_link_base_url", data.block_link_base_url || "");
      setValue("email_alert_window_seconds", String(data.alert_window_seconds || 60));
      setValue("email_alert_max_requests", String(data.alert_max_requests || 80));
      setValue("email_alert_max_404", String(data.alert_max_404 || 15));
      setValue("email_alert_cooldown_minutes", String(data.alert_cooldown_minutes || 30));
      const pill = document.getElementById("emailStatusPill");
      const body = document.getElementById("emailSummaryBody");
      if (pill) { pill.className = "pill " + (data.enabled ? "pill-ok" : "pill-neutral"); pill.textContent = data.enabled ? "已启用" : "未配置"; }
      if (body) {
        // 注意：esc() 会转义 &，所以 HTML 实体（&lt; &gt;）必须在 esc 之后拼接，否则按字面显示
        const senderHtml = data.sender_name
          ? `${esc(data.sender_name)} &lt;${esc(data.sender || "-")}&gt;`
          : esc(data.sender || "-");
        const masked = data.password ? "••••••••" : '<span class="text-warn">未设置</span>';
        body.innerHTML = `
          <div class="kv"><div class="k">SMTP 主机</div><div class="val">${esc(data.smtp_host || "-")}</div></div>
          <div class="kv"><div class="k">SMTP 端口</div><div class="val">${esc(String(data.smtp_port || 465))}${data.smtp_ssl ? ' <span class="pill pill-ok" style="font-size:11px">SSL</span>' : ""}</div></div>
          <div class="kv"><div class="k">发件人</div><div class="val">${senderHtml}</div></div>
          <div class="kv"><div class="k">收件人</div><div class="val">${esc(data.recipients || "-")}</div></div>
          <div class="kv"><div class="k">授权码</div><div class="val">${masked}</div></div>
          <div class="kv"><div class="k">确认页地址</div><div class="val">${esc(data.block_link_base_url || "未配置")}</div></div>`;
      }
      // 告警阈值卡：把窗口/上限/冷却单独列出，避免与 SMTP 通道混淆
      const alertBody = document.getElementById("emailAlertBody");
      const alertPill = document.getElementById("emailAlertPill");
      if (alertPill) {
        alertPill.className = "pill " + (data.enabled ? "pill-ok" : "pill-neutral");
        alertPill.textContent = data.enabled ? "生效中" : "未生效";
      }
      if (alertBody) {
        const win = data.alert_window_seconds || 60;
        const maxReq = data.alert_max_requests || 80;
        const max404 = data.alert_max_404 || 15;
        const cool = data.alert_cooldown_minutes || 30;
        alertBody.innerHTML = `
          <div class="kv"><div class="k">告警窗口</div><div class="val">${esc(formatTtl(win))}（${win} 秒）</div></div>
          <div class="kv"><div class="k">窗口内最大请求</div><div class="val">${esc(String(maxReq))} 次</div></div>
          <div class="kv"><div class="k">窗口内最大 404</div><div class="val">${esc(String(max404))} 次</div></div>
          <div class="kv"><div class="k">告警冷却</div><div class="val">${esc(formatTtl(cool * 60))}（${cool} 分钟）</div></div>
          <div class="feature-desc">同一 IP 在告警窗口内请求数或 404 数任一超阈值即发信，随后进入冷却期不再重复提醒。</div>`;
      }
    }
  } catch (_) {}
}

export function openEmailSettings() {
  openFormModal({
    title: "邮件提醒配置",
    size: 680,
    schema: [
      { key: "enabled", label: "启用邮件提醒", type: "switch" },
      { key: "smtp_host", label: "SMTP 主机", type: "text", placeholder: "smtp.example.com" },
      { key: "smtp_port", label: "SMTP 端口", type: "number", default: 465 },
      { key: "smtp_ssl", label: "使用 SSL", type: "switch" },
      { key: "sender", label: "发件邮箱", type: "text" },
      { key: "sender_name", label: "发件人名称", type: "text" },
      { key: "password", label: "密码/授权码", type: "password", hint: "留空表示不修改" },
      { key: "recipients", label: "收件人（逗号分隔）", type: "text" },
      { key: "block_link_base_url", label: "封禁确认页基础 URL", type: "text" },
      { key: "alert_window_seconds", label: "告警窗口（秒）", type: "number", default: 60 },
      { key: "alert_max_requests", label: "窗口内最大请求", type: "number", default: 80 },
      { key: "alert_max_404", label: "窗口内最大 404", type: "number", default: 15 },
      { key: "alert_cooldown_minutes", label: "告警冷却（分钟）", type: "number", default: 30 },
    ],
    values: {
      enabled: getValue("email_enabled") === "1",
      smtp_host: getValue("email_smtp_host"), smtp_port: Number(getValue("email_smtp_port") || 465),
      smtp_ssl: getValue("email_smtp_ssl") === "1", sender: getValue("email_sender"),
      sender_name: getValue("email_sender_name"), password: "", recipients: getValue("email_recipients"),
      block_link_base_url: getValue("email_block_link_base_url"),
      alert_window_seconds: Number(getValue("email_alert_window_seconds") || 60),
      alert_max_requests: Number(getValue("email_alert_max_requests") || 80),
      alert_max_404: Number(getValue("email_alert_max_404") || 15),
      alert_cooldown_minutes: Number(getValue("email_alert_cooldown_minutes") || 30),
    },
    onSave: async (out) => {
      const payload = {
        enabled: Boolean(out.enabled),
        smtp_host: out.smtp_host || "",
        smtp_port: Number(out.smtp_port ?? 465),
        smtp_ssl: Boolean(out.smtp_ssl),
        sender: out.sender || "",
        sender_name: out.sender_name || "",
        recipients: out.recipients || "",
        block_link_base_url: out.block_link_base_url || "",
        alert_window_seconds: Number(out.alert_window_seconds ?? 60),
        alert_max_requests: Number(out.alert_max_requests ?? 80),
        alert_max_404: Number(out.alert_max_404 ?? 15),
        alert_cooldown_minutes: Number(out.alert_cooldown_minutes ?? 30),
      };
      if (out.password) payload.password = out.password;
      await apiFetch("/_admin/api/email", { method: "PUT", body: JSON.stringify(payload) });
      await loadEmailSettings();
      showToast("邮件提醒配置已保存。");
    },
  });
}

export async function testEmail() {
  try {
    const result = await apiFetch("/_admin/api/email/test", {
      method: "POST",
      body: JSON.stringify({
        smtp_host: getValue("email_smtp_host") || "",
        smtp_port: Number(getValue("email_smtp_port") || 465),
        smtp_ssl: getValue("email_smtp_ssl") === "1",
        sender: getValue("email_sender") || "",
        sender_name: getValue("email_sender_name") || "",
        password: getValue("email_password") || "",
        recipients: getValue("email_recipients") || "",
        template_type: "alert",
      }),
    });
    showToast(result.message, !result.success);
  } catch (e) { showToast(e.message, true); }
}

// ============ 封禁管理 ============

export function isValidIpOrCidr(str) {
  if (!str) return false;
  const s = str.trim();
  if (s.includes("/")) {
    const parts = s.split("/");
    if (parts.length !== 2) return false;
    const prefix = parseInt(parts[1], 10);
    if (isNaN(prefix) || prefix < 0 || prefix > 128) return false;
    const ipPart = parts[0];
    if (ipPart.includes(":")) return prefix <= 128;
    if (prefix > 32) return false;
    const octets = ipPart.split(".");
    if (octets.length !== 4) return false;
    return octets.every((o) => { const n = parseInt(o, 10); return !isNaN(n) && n >= 0 && n <= 255; });
  }
  if (s.includes(".")) {
    const octets = s.split(".");
    if (octets.length !== 4) return false;
    return octets.every((o) => { const n = parseInt(o, 10); return !isNaN(n) && n >= 0 && n <= 255; });
  }
  if (s.includes(":")) return s.split(":").length >= 2;
  return false;
}

export function isIpBanned(ip, bannedList) {
  if (!ip || ip === "-" || !bannedList || !bannedList.length) return false;
  if (bannedList.some((b) => b.ip === ip)) return true;
  for (const b of bannedList) {
    if (b.ip && b.ip.includes("/") && ipInCidr(ip, b.ip)) return true;
  }
  return false;
}

function ipInCidr(ip, cidr) {
  try {
    const [range, prefixStr] = cidr.split("/");
    const prefix = parseInt(prefixStr, 10);
    if (isNaN(prefix)) return false;
    if (ip.includes(".") && range.includes(".")) {
      if (prefix > 32) return false;
      const ipParts = ip.split(".").map(Number);
      const rangeParts = range.split(".").map(Number);
      if (ipParts.length !== 4 || rangeParts.length !== 4) return false;
      const ipNum = (ipParts[0] << 24) | (ipParts[1] << 16) | (ipParts[2] << 8) | ipParts[3];
      const rangeNum = (rangeParts[0] << 24) | (rangeParts[1] << 16) | (rangeParts[2] << 8) | rangeParts[3];
      const mask = prefix === 0 ? 0 : (0xFFFFFFFF << (32 - prefix)) >>> 0;
      return (ipNum & mask) === (rangeNum & mask);
    }
    if (ip.includes(":") && range.includes(":")) {
      const ipBig = ipv6ToBigInt(ip);
      const rangeBig = ipv6ToBigInt(range);
      if (ipBig === null || rangeBig === null) return false;
      const mask = prefix === 0 ? 0n : ((1n << 128n) - 1n) ^ ((1n << BigInt(128 - prefix)) - 1n);
      return (ipBig & mask) === (rangeBig & mask);
    }
  } catch { return false; }
  return false;
}

function ipv6ToBigInt(ip) {
  try {
    const parts = ip.split(":");
    if (parts.length < 3) return null;
    const doubleColon = ip.indexOf("::");
    let fullParts;
    if (doubleColon >= 0) {
      const before = ip.substring(0, doubleColon).split(":").filter(Boolean);
      const after = ip.substring(doubleColon + 2).split(":").filter(Boolean);
      const missing = 8 - before.length - after.length;
      fullParts = [...before, ...Array(missing).fill("0"), ...after];
    } else fullParts = parts;
    if (fullParts.length !== 8) return null;
    let result = 0n;
    for (const part of fullParts) {
      const num = parseInt(part || "0", 16);
      if (isNaN(num)) return null;
      result = (result << 16n) | BigInt(num);
    }
    return result;
  } catch { return null; }
}

export async function loadBannedIpList() {
  try {
    const data = await apiFetch("/_admin/api/banned-ips");
    state.bannedIps = data.items || [];
    setText("navCountSecurity", String(state.bannedIps.length));
    renderBannedIpListPage();
  } catch (_) {}
}

export function renderBannedIpListPage() {
  const allItems = state.bannedIps;
  const totalCount = allItems.length;
  const pageSize = state.banPageSize;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  if (state.banCurrentPage > totalPages) state.banCurrentPage = totalPages;
  const currentPage = state.banCurrentPage;
  const pageItems = allItems.slice((currentPage - 1) * pageSize, currentPage * pageSize);
  setText("banSummary", `共 ${totalCount} 条`);
  renderBannedIpList(pageItems);
  renderPagination(currentPage, totalPages, "banPagination", goToBanPage);
}

function goToBanPage(page, totalPages) {
  state.banCurrentPage = Math.max(1, Math.min(totalPages, page));
  renderBannedIpListPage();
}

function renderBannedIpList(items) {
  const tbody = document.getElementById("banBody");
  if (!tbody) return;
  tbody.innerHTML = "";
  if (!items.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty" style="padding:26px 0">暂无封禁 IP 记录。</td></tr>`;
    return;
  }
  const nowSec = Math.floor(Date.now() / 1000);
  items.forEach((item) => {
    let expireText, statusBadge;
    if (item.permanent) {
      expireText = "永久";
      statusBadge = '<span class="pill pill-danger">永久封禁</span>';
    } else if (item.expire_at && item.expire_at > 0) {
      const isExpired = item.expire_at <= nowSec;
      const formatted = new Date(item.expire_at * 1000).toLocaleString("zh-CN", { hour12: false });
      if (isExpired) {
        expireText = `${formatted}（已过期）`;
        statusBadge = '<span class="pill pill-neutral">已过期</span>';
      } else {
        expireText = `${formatted}（剩 ${formatRemainTime(item.expire_at - nowSec)}）`;
        statusBadge = '<span class="pill pill-warn">临时封禁</span>';
      }
    } else {
      expireText = "-";
      statusBadge = '<span class="pill pill-neutral">未知</span>';
    }
    const extendBtn = item.permanent ? "" : `<button class="btn btn-sm" data-action="extend-ban-ip" data-ip="${esc(item.ip)}" data-expire="${item.expire_at || 0}">延长</button>`;
    const permanentBtn = item.permanent ? "" : `<button class="btn btn-sm" data-action="set-ban-permanent" data-ip="${esc(item.ip)}">设为永久</button>`;
    // 封禁路径：空=全局封禁，非空=仅拦截该路径前缀
    const pathCell = item.path_prefix
      ? `<code class="mono">${esc(item.path_prefix)}</code>`
      : '<span class="pill pill-neutral">全局</span>';
    const sourceText = item.banned_by ? `<div class="hint">来源: ${esc(item.banned_by)}</div>` : "";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><strong>${esc(item.ip)}</strong></td>
      <td>${pathCell}</td>
      <td>${statusBadge}</td>
      <td>${esc(item.reason || "-")}${sourceText}</td>
      <td>${expireText}</td>
      <td><div style="display:flex;gap:6px;justify-content:flex-end;flex-wrap:wrap">${permanentBtn}${extendBtn}<button class="btn btn-sm btn-danger" data-action="unban-ip" data-ip="${esc(item.ip)}">解封</button></div></td>`;
    tbody.appendChild(tr);
  });
}

export function openBanModal(options = {}) {
  const mode = options.mode || "add";
  openFormModal({
    title: mode === "from-log" ? "从日志封禁 IP" : "封禁 IP",
    schema: [
      { key: "ip", label: "IP 地址 / 网段", type: "text", required: true, placeholder: "1.2.3.4 或 192.168.1.0/24" },
      { key: "path_prefix", label: "路径前缀（可选）", type: "text", placeholder: "留空表示全局封禁", hint: "留空拦截该 IP 的所有代理请求；填写如 /play 仅拦截该前缀。封禁只影响代理转发，不影响后台管理 /_admin 访问。" },
      { key: "reason", label: "封禁原因", type: "text" },
      { key: "permanent", label: "封禁类型", type: "select", options: [{ value: "1", label: "永久封禁" }, { value: "0", label: "临时封禁" }] },
      { key: "duration", label: "封禁时长（小时）", type: "number", default: 1, hint: "仅临时封禁时生效", dependsOn: { field: "permanent", value: "0" } },
    ],
    values: { ip: options.ip || "", path_prefix: options.pathPrefix || "", reason: options.reason || "", permanent: "0", duration: 1 },
    validate: (out) => {
      if (!String(out.ip || "").trim()) return "IP 地址不能为空";
      if (!isValidIpOrCidr(String(out.ip).trim())) return "IP 格式无效，请输入单个 IP 或 CIDR 网段";
      if (out.permanent === "0" && (Number(out.duration) || 0) <= 0) return "临时封禁时长必须大于 0";
      return null;
    },
    onSave: async (out) => {
      const permanent = out.permanent === "1";
      const durationSeconds = permanent ? 0 : Math.max(60, Math.round((Number(out.duration) || 0) * 3600));
      await apiFetch("/_admin/api/banned-ips", {
        method: "POST",
        body: JSON.stringify({ ip: String(out.ip).trim(), reason: out.reason || "", banned_by: "admin", permanent, duration_seconds: durationSeconds, path_prefix: out.path_prefix || "" }),
      });
      const scopeText = out.path_prefix ? `路径前缀 ${out.path_prefix}` : "全局";
      showToast(`${String(out.ip).includes("/") ? "IP段" : "IP"} ${out.ip} 已封禁（${scopeText}）`);
      loadBannedIpList();
    },
  });
}

export function openBanExtendModal(ip, currentExpireAt) {
  openFormModal({
    title: `延长封禁 ${ip}`,
    schema: [
      { key: "duration", label: "延长时长（小时）", type: "number", default: 1, required: true },
    ],
    values: { duration: 1 },
    onSave: async (out) => {
      const hours = Number(out.duration) || 0;
      if (hours <= 0) throw new Error("延长时长必须大于 0");
      await apiFetch(`/_admin/api/banned-ips/${encodeURIComponent(ip)}/extend`, { method: "POST", body: JSON.stringify({ duration_hours: hours }) });
      showToast(`IP ${ip} 封禁时间已延长 ${hours} 小时`);
      loadBannedIpList();
    },
  });
}

export async function banIpFromLog(ip, pathPrefix = "") {
  // pathPrefix 来自日志条目命中的路由组前缀，预填后用户可清空改为全局封禁
  openBanModal({ ip, pathPrefix: pathPrefix || "", reason: "从日志手动封禁", mode: "from-log" });
}

export async function unbanIp(ip) {
  openConfirm({
    title: "解封 IP",
    message: `确认解封 IP ${ip} 吗？`,
    onOk: async () => {
      try {
        await apiFetch(`/_admin/api/banned-ips/${encodeURIComponent(ip)}`, { method: "DELETE" });
        showToast(`IP ${ip} 已解封`);
        loadBannedIpList();
      } catch (e) { showToast(e.message, true); }
    },
  });
}

export async function setBanPermanent(ip) {
  openConfirm({
    title: "设为永久封禁",
    message: `确认将 IP ${ip} 转为永久封禁吗？转换后不再自动过期。`,
    onOk: async () => {
      try {
        await apiFetch(`/_admin/api/banned-ips/${encodeURIComponent(ip)}/permanent`, { method: "POST" });
        showToast(`IP ${ip} 已设为永久封禁`);
        loadBannedIpList();
      } catch (e) { showToast(e.message, true); }
    },
  });
}

export async function clearBans() {
  openConfirm({
    title: "清空封禁记录",
    message: "确认清空所有封禁记录吗？此操作不可恢复！",
    onOk: async () => {
      try {
        await apiFetch("/_admin/api/banned-ips/clear", { method: "POST" });
        showToast("所有封禁记录已清空");
        state.banCurrentPage = 1;
        loadBannedIpList();
      } catch (e) { showToast(e.message, true); }
    },
  });
}

// ---- 封禁自动刷新 ----

let _banAutoRefreshTimer = null;
const BAN_AUTO_REFRESH_STORAGE_KEY = "ban_auto_refresh";

export function getBanAutoRefreshConfig() {
  try { const raw = localStorage.getItem(BAN_AUTO_REFRESH_STORAGE_KEY); if (raw) return JSON.parse(raw); } catch (_) {}
  return { enabled: false, interval: 5 };
}
export function saveBanAutoRefreshConfig(cfg) { localStorage.setItem(BAN_AUTO_REFRESH_STORAGE_KEY, JSON.stringify(cfg)); }

export function stopBanAutoRefresh() {
  if (_banAutoRefreshTimer !== null) { clearInterval(_banAutoRefreshTimer); _banAutoRefreshTimer = null; }
  const el = document.getElementById("ban_auto_refresh_status");
  if (el) { el.textContent = ""; el.className = "auto-refresh-status stopped"; }
}

export function startBanAutoRefresh() {
  stopBanAutoRefresh();
  if (!getChecked("ban_auto_refresh_enabled")) return;
  const interval = Math.max(1, parseInt(getValue("ban_auto_refresh_interval") || "5", 10) || 5);
  saveBanAutoRefreshConfig({ enabled: true, interval });
  _banAutoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "security") { stopBanAutoRefresh(); return; }
    loadBannedIpList().catch((e) => { showToast(e.message, true); stopBanAutoRefresh(); setChecked("ban_auto_refresh_enabled", false); });
  }, interval * 1000);
  const el = document.getElementById("ban_auto_refresh_status");
  if (el) { el.textContent = "●"; el.className = "auto-refresh-status running"; }
}

// ============ API 密钥管理 ============

// 列表只含前缀 / 哈希派生信息，明文只在签发响应中出现一次
export async function loadApiKeys() {
  try {
    const data = await apiFetch("/_admin/api/keys");
    state.apiKeys = data.items || [];
    renderApiKeys();
  } catch (_) {}
}

function formatApiKeyTime(value) {
  if (!value || value <= 0) return "—";
  try {
    // 双格式兼容：expires_at 为 Unix 秒；created_at/last_used_at 为 UTC isoformat 字符串
    if (typeof value === "number" || /^\d+$/.test(String(value))) {
      return new Date(Number(value) * 1000).toLocaleString("zh-CN", { hour12: false });
    }
    const d = new Date(String(value));
    if (Number.isNaN(d.getTime())) return String(value);
    return d.toLocaleString("zh-CN", { hour12: false });
  } catch (_) { return String(value); }
}

function renderApiKeys() {
  const tbody = document.getElementById("apiKeysBody");
  const summary = document.getElementById("apiKeySummary");
  const items = state.apiKeys || [];
  if (summary) summary.textContent = `共 ${items.length} 个`;
  if (!tbody) return;
  if (!items.length) {
    tbody.innerHTML = `<tr><td colspan="9" class="empty" style="padding:26px 0">暂无 API 密钥，点右上角「签发密钥」创建第一个。</td></tr>`;
    return;
  }
  const nowSec = Math.floor(Date.now() / 1000);
  tbody.innerHTML = items.map((k) => {
    const expired = k.expired || (k.expires_at && k.expires_at > 0 && k.expires_at <= nowSec);
    let statusBadge;
    if (expired) statusBadge = '<span class="pill pill-warn">已过期</span>';
    else if (!k.enabled) statusBadge = '<span class="pill pill-danger">已停用</span>';
    else statusBadge = '<span class="pill pill-ok">启用中</span>';
    const permBadge = k.readonly ? '<span class="pill pill-neutral">只读</span>' : '<span class="pill pill-ok">读写</span>';
    // P1-2.3 权限摘要徽标
    const scopeCount = String(k.scopes || "").split(",").map((s) => s.trim()).filter(Boolean).length;
    const scopeBadge = k.scopes
      ? `<span class="pill pill-neutral" title="${esc(k.scopes)}">限定 ${scopeCount} 项</span>`
      : '<span class="pill pill-neutral" title="未限定端点">全部端点</span>';
    const rateBadge = (k.rate_limit || 0) > 0 ? `<span class="pill pill-warn">${k.rate_limit}/s</span>` : "";
    const toggleLabel = k.enabled ? "停用" : "启用";
    const expiresText = k.expires_at && k.expires_at > 0
      ? `${formatApiKeyTime(k.expires_at)}${expired ? "（已过期）" : ""}`
      : "永久";
    return `
    <tr>
      <td><strong>${esc(k.name)}</strong></td>
      <td><code class="mono">${esc(k.key_prefix)}…</code></td>
      <td>${permBadge}${scopeBadge}${rateBadge}</td>
      <td>${statusBadge}</td>
      <td>${k.use_count || 0}</td>
      <td>${formatApiKeyTime(k.created_at)}</td>
      <td>${k.last_used_at ? formatApiKeyTime(k.last_used_at) : "—"}</td>
      <td>${expiresText}</td>
      <td><div style="display:flex;gap:6px;justify-content:flex-end;flex-wrap:wrap">
        <button class="btn btn-sm" data-action="edit-api-key" data-id="${k.id}">编辑</button>
        <button class="btn btn-sm" data-action="toggle-api-key" data-id="${k.id}" data-enabled="${k.enabled ? 1 : 0}" ${expired ? "disabled" : ""}>${toggleLabel}</button>
        <button class="btn btn-sm btn-danger" data-action="delete-api-key" data-id="${k.id}" data-name="${esc(k.name)}">删除</button>
      </div></td>
    </tr>`;
  }).join("");
}

// ============ API 密钥细粒度权限（P1-2.3） ============

const API_KEY_SCOPES = ["routing", "security", "geo", "logs", "system", "backup", "email", "signing", "apidoc", "apikeys"];

const API_KEY_PERM_SCHEMA = [
  { type: "note", text: "【端点权限】每行一个 tag：routing / security / geo / logs / system / backup / email / signing / apidoc / apikeys。\n留空 = 不限制（可访问全部端点）。\n【绑定 IP】逗号分隔、精确匹配（如 10.0.0.5,192.168.1.23），留空 = 不限来源。\n【速率上限】该密钥每秒允许的请求数，超限返回 429 + Retry-After；0 = 不限。" },
  { key: "scopes", label: "端点权限 scopes（每行一个）", type: "textarea", placeholder: "routing\nlogs\nsystem" },
  { key: "allowed_ips", label: "绑定 IP 白名单（逗号分隔）", type: "text", placeholder: "10.0.0.5, 192.168.1.23（留空不限）" },
  { key: "rate_limit", label: "速率上限（请求/秒）", type: "number", default: 0, hint: "0 表示不限速" },
];

function csvToLines(v) {
  return String(v || "").split(",").map((s) => s.trim()).filter(Boolean).join("\n");
}
function linesToCsv(v) {
  return String(v || "").split(/\r?\n/).map((s) => s.trim()).filter(Boolean).join(",");
}
function validateKeyPerm(out) {
  const tags = String(out.scopes || "").split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  for (const tag of tags) {
    if (!API_KEY_SCOPES.includes(tag)) return `未知端点 tag：${tag}（可选：${API_KEY_SCOPES.join(" / ")}）`;
  }
  const rps = Number(out.rate_limit);
  if (!Number.isFinite(rps) || rps < 0 || rps > 10000) return "速率上限须在 0（不限）~ 10000 之间";
  return null;
}
function permPayload(out) {
  return {
    scopes: linesToCsv(out.scopes),
    allowed_ips: String(out.allowed_ips || "").replace(/，/g, ",").split(",").map((s) => s.trim()).filter(Boolean).join(","),
    rate_limit: Math.max(0, Math.round(Number(out.rate_limit) || 0)),
  };
}

export function openApiKeyPermModal(keyId) {
  const key = (state.apiKeys || []).find((k) => k.id === Number(keyId));
  if (!key) return;
  openFormModal({
    title: `编辑密钥权限 · ${key.name}`,
    size: 560,
    sub: `前缀 ${key.key_prefix}… · ${key.readonly ? "只读" : "读写"}密钥`,
    schema: API_KEY_PERM_SCHEMA,
    values: {
      scopes: csvToLines(key.scopes),
      allowed_ips: String(key.allowed_ips || ""),
      rate_limit: key.rate_limit || 0,
    },
    validate: validateKeyPerm,
    onSave: async (out) => {
      await apiFetch(`/_admin/api/keys/${key.id}`, { method: "PUT", body: JSON.stringify(permPayload(out)) });
      showToast("密钥权限已更新。");
      await loadApiKeys();
    },
  });
}

export function openApiKeyCreateModal() {
  openFormModal({
    title: "签发 API 密钥",
    // 保存成功后不自动关窗：由 onSave 打开「明文仅此一次」弹窗接管
    autoClose: false,
    schema: [
      { key: "name", label: "密钥名称", type: "text", required: true, placeholder: "如：自动化脚本 / 监控面板", hint: "仅用于辨识用途，≤64 字符" },
      { key: "readonly", label: "只读模式", type: "switch", default: false, hint: "开启后该密钥仅能调用 GET 接口（查询类），且无法访问密钥管理本身" },
      { key: "expires_days", label: "有效期（天）", type: "number", default: 0, hint: "0 表示永久有效，最大 3650 天" },
      ...API_KEY_PERM_SCHEMA,
    ],
    values: { name: "", readonly: false, expires_days: 0, scopes: "", allowed_ips: "", rate_limit: 0 },
    validate: (out) => {
      if (!String(out.name || "").trim()) return "密钥名称不能为空";
      if (String(out.name).trim().length > 64) return "密钥名称过长（≤64 字符）";
      const days = Number(out.expires_days);
      if (!Number.isFinite(days) || days < 0 || days > 3650) return "有效期须在 0（永久）~ 3650 之间";
      return validateKeyPerm(out);
    },
    onSave: async (out) => {
      const created = await apiFetch("/_admin/api/keys", {
        method: "POST",
        body: JSON.stringify({
          name: String(out.name).trim(),
          readonly: Boolean(out.readonly),
          expires_days: Math.round(Number(out.expires_days) || 0),
          ...permPayload(out),
        }),
      });
      showToast("API 密钥已签发，请立即保存明文");
      await loadApiKeys();
      showApiKeyOnceModal(created);
    },
  });
}

// 明文一次性展示弹窗（不走 openFormModal 的保存流程，仅展示 + 复制）
function showApiKeyOnceModal(created) {
  const modalEl = document.getElementById("modal");
  const mask = document.getElementById("modalMask");
  if (!modalEl || !mask) return;
  const raw = created && created.key ? String(created.key) : "";
  const expiredText = created && created.expires_at ? formatApiKeyTime(created.expires_at) : "永久";
  modalEl.style.width = "";
  modalEl.innerHTML = `
    <div class="modal-head"><div class="modal-title">密钥已签发 · 仅此一次展示</div><button class="icon-btn" id="modalClose">✕</button></div>
    <div class="modal-body">
      <div class="form-note" style="border-left:3px solid var(--warn,#e6a23c);padding-left:10px;line-height:1.7">
        系统只保存密钥哈希，<strong>关闭本弹窗后明文不可再查看</strong>。请立即复制并妥善保管。
      </div>
      <div class="form-field"><label>密钥明文</label>
        <div style="display:flex;gap:8px;align-items:center">
          <input class="input mono" id="apiKeyPlaintext" readonly value="${esc(raw)}" style="flex:1;user-select:all">
          <button class="btn btn-primary" id="apiKeyCopyBtn" type="button">复制</button>
        </div>
      </div>
      <div class="form-field"><label>密钥信息</label>
        <div class="hint">名称：${esc(created && created.name ? created.name : "")} · 前缀：<code class="mono">${esc(created && created.key_prefix ? created.key_prefix : "")}…</code> · 到期：${esc(expiredText)}</div>
      </div>
    </div>
    <div class="modal-foot"><button class="btn btn-primary" id="modalCancel">我已保存，关闭</button></div>`;
  document.getElementById("modalClose").onclick = closeModal;
  document.getElementById("modalCancel").onclick = closeModal;
  const copyBtn = document.getElementById("apiKeyCopyBtn");
  copyBtn.onclick = () => {
    copyToClipboard(raw);
    const original = copyBtn.textContent;
    copyBtn.textContent = "已复制 ✓";
    window.setTimeout(() => { copyBtn.textContent = original; }, 1200);
  };
  mask.classList.add("open");
  window.setTimeout(() => {
    const input = document.getElementById("apiKeyPlaintext");
    if (input) { input.focus(); input.select(); }
  }, 50);
}

export function toggleApiKey(keyId, enabled) {
  const actionText = enabled ? "启用" : "停用";
  openConfirm({
    title: `${actionText} API 密钥`,
    message: `确认${actionText}该 API 密钥吗？${enabled ? "" : "停用后使用此密钥的调用会立即收到 401。"}`,
    danger: !enabled,
    onOk: async () => {
      try {
        await apiFetch(`/_admin/api/keys/${keyId}/toggle`, { method: "POST", body: JSON.stringify({ enabled }) });
        showToast(`API 密钥已${actionText}`);
        loadApiKeys();
      } catch (e) { showToast(e.message, true); }
    },
  });
}

export function deleteApiKey(keyId, name) {
  openConfirm({
    title: "删除 API 密钥",
    message: `确认删除密钥「${esc(name)}」吗？删除后使用此密钥的调用会立即收到 401，此操作不可恢复。`,
    onOk: async () => {
      try {
        await apiFetch(`/_admin/api/keys/${keyId}`, { method: "DELETE" });
        showToast("API 密钥已删除");
        loadApiKeys();
      } catch (e) { showToast(e.message, true); }
    },
  });
}

// ============ API 文档（自动维护） ============

const _API_DOC_METHOD_CLASS = {
  GET: "pill-ok", POST: "pill-info", PUT: "pill-warn", DELETE: "pill-danger",
};

function _apiDocMethodBadge(method) {
  const cls = _API_DOC_METHOD_CLASS[method] || "pill-neutral";
  return `<span class="pill ${cls}">${esc(method)}</span>`;
}

export async function loadApiDoc() {
  // 恢复上一次填写的密钥
  const savedKey = localStorage.getItem("api_doc_key") || "";
  const keyInput = document.getElementById("apiDocKey");
  if (keyInput && !keyInput.value && savedKey) keyInput.value = savedKey;
  try {
    const data = await apiFetch("/_admin/api/doc");
    state.apiDoc = data.items || [];
    // 重新加载时沿用当前搜索词，避免「输入框有值但列表全显」的不一致
    const searchInput = document.getElementById("apiDocSearch");
    renderApiDoc(searchInput ? searchInput.value : "");
  } catch (e) { showToast(e.message, true); }
}

function renderApiDoc(keyword = "") {
  const body = document.getElementById("apiDocBody");
  if (!body) return;
  const all = state.apiDoc || [];
  const kw = String(keyword || "").trim().toLowerCase();
  // 关键词匹配：方法 / 路径 / 摘要 / 分组 / 用法 / 参数名与说明
  const matched = kw
    ? all.filter((it) => {
        const hay = [
          it.method, it.path, it.summary, it.tag, it.usage,
          ...(it.params || []).flatMap((p) => [p.name, p.desc, p.in, p.type]),
        ].join(" ").toLowerCase();
        return hay.includes(kw);
      })
    : all;
  // 搜索结果汇总（渲染在搜索栏旁），只在有数据时更新
  const summary = document.getElementById("apiDocResultCount");
  if (summary) {
    summary.textContent = kw ? `${matched.length} / ${all.length}` : `共 ${all.length}`;
    summary.className = "pill " + (kw ? "pill-info" : "pill-neutral");
  }
  if (!all.length) {
    body.innerHTML = `<div class="card"><div class="panel-body">暂无可展示的接口。</div></div>`;
    return;
  }
  if (!matched.length) {
    body.innerHTML = `<div class="card"><div class="panel-body">没有匹配「${esc(keyword)}」的接口。</div></div>`;
    return;
  }
  // 按 tag 分组并保持出现顺序
  const order = [];
  const groups = {};
  matched.forEach((it) => {
    const g = it.tag || "其他";
    if (!groups[g]) { groups[g] = []; order.push(g); }
    groups[g].push(it);
  });
  body.innerHTML = order.map((g) => `
    <div class="card" style="margin-bottom:16px">
      <div class="panel-head"><div class="panel-title">${esc(g)}</div><span class="pill pill-neutral">${groups[g].length}</span></div>
      <div class="panel-body" style="padding:0">
        ${groups[g].map((it) => _apiDocRow(it)).join("")}
      </div>
    </div>`).join("");
}

export function filterApiDoc() {
  const input = document.getElementById("apiDocSearch");
  renderApiDoc(input ? input.value : "");
}

function _apiDocRow(it) {
  const paramRows = (it.params || []).length
    ? `<table class="api-param-table"><thead><tr><th>参数</th><th>位置</th><th>类型</th><th>必填</th><th>说明</th></tr></thead><tbody>${
        it.params.map((p) => `<tr><td><code class="mono">${esc(p.name)}</code></td><td>${esc(p.in || "")}</td><td>${esc(p.type || "")}</td><td>${p.required ? "是" : "否"}</td><td>${esc(p.desc || "")}</td></tr>`).join("")
      }</tbody></table>`
    : "";
  const sample = it.body_sample
    ? `<div class="hint" style="margin:6px 0 2px">请求体示例</div><pre class="api-sample">${esc(it.body_sample)}</pre>`
    : "";
  const usage = it.usage
    ? `<div class="hint api-usage"><b>用法：</b>${esc(it.usage)}</div>`
    : "";
  const note = it.note ? `<div class="hint" style="color:var(--warn)">${esc(it.note)}</div>` : "";
  const undocumentedBadge = it.documented ? "" : '<span class="pill pill-warn" style="margin-left:6px">待补充说明</span>';
  return `
    <div class="api-doc-row">
      <div class="api-doc-head">
        ${_apiDocMethodBadge(it.method)}
        <code class="mono api-doc-path">${esc(it.path)}</code>
        <span class="api-doc-summary">${esc(it.summary || "")}${undocumentedBadge}</span>
        <button class="btn btn-sm api-doc-try" data-method="${esc(it.method)}" data-path="${esc(it.path)}" data-sample="${esc(it.body_sample || "")}">试一试</button>
      </div>
      ${paramRows}
      ${sample}
      ${usage}
      ${note}
    </div>`;
}

export function openApiDocTry(method, path, sample) {
  const modalEl = document.getElementById("modal");
  const mask = document.getElementById("modalMask");
  if (!modalEl || !mask) return;
  const keyInput = document.getElementById("apiDocKey");
  const key = (keyInput && keyInput.value || "").trim();
  const needsBody = method !== "GET" && method !== "HEAD";
  modalEl.style.width = "";
  modalEl.innerHTML = `
    <div class="modal-head"><div class="modal-title">试调用 ${esc(method)} ${esc(path)}</div><button class="icon-btn" id="modalClose">✕</button></div>
    <div class="modal-body">
      <div class="form-field"><label>鉴权头</label>
        <div class="hint">将使用下方密钥以 <code class="mono">Authorization: Bearer</code> 发送</div>
        <input class="input mono" id="apiDocTryKey" value="${esc(key)}" autocomplete="off">
      </div>
      ${needsBody ? `<div class="form-field"><label>请求体 (JSON)</label><textarea class="input mono" id="apiDocTryBody" rows="6" style="font-family:var(--mono)">${esc(sample || "{}")}</textarea></div>` : ""}
      <div class="form-field"><label>响应</label><pre class="api-sample" id="apiDocTryResp" style="max-height:280px;overflow:auto">（点击「发送」查看结果）</pre></div>
    </div>
    <div class="modal-foot"><button class="btn" id="modalCancel">关闭</button><button class="btn btn-primary" id="apiDocTrySend">发送</button></div>`;
  document.getElementById("modalClose").onclick = closeModal;
  document.getElementById("modalCancel").onclick = closeModal;
  document.getElementById("apiDocTrySend").onclick = async () => {
    const k = document.getElementById("apiDocTryKey").value.trim();
    const sendBtn = document.getElementById("apiDocTrySend");
    const respEl = document.getElementById("apiDocTryResp");
    sendBtn.disabled = true; sendBtn.textContent = "发送中…";
    try {
      const headers = { "Accept": "application/json" };
      if (k) headers["Authorization"] = `Bearer ${k}`;
      const opts = { method, headers };
      if (needsBody) {
        headers["Content-Type"] = "application/json";
        opts.body = document.getElementById("apiDocTryBody").value;
      }
      const resp = await fetch(path, opts);
      let text = await resp.text();
      let pretty = text;
      try { pretty = JSON.stringify(JSON.parse(text), null, 2); } catch (_) {}
      respEl.textContent = `HTTP ${resp.status} ${resp.statusText}\n\n${pretty}`;
      respEl.style.color = resp.ok ? "var(--text)" : "var(--danger,#d9534f)";
    } catch (e) {
      respEl.textContent = `请求失败: ${e.message}`;
      respEl.style.color = "var(--danger,#d9534f)";
    } finally {
      sendBtn.disabled = false; sendBtn.textContent = "发送";
    }
  };
  mask.classList.add("open");
}

// ============ 备份管理 ============

function formatBackupSize(bytes) {
  if (bytes >= 1024 * 1024) return (bytes / 1024 / 1024).toFixed(2) + " MB";
  if (bytes >= 1024) return (bytes / 1024).toFixed(1) + " KB";
  return bytes + " B";
}
function formatBackupTime(isoStr) {
  try { return new Date(isoStr).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" }); } catch { return isoStr; }
}

// 备份页顶部三张 KPI：数量 / 占用 / 最近备份，并同步侧边栏计数
function renderBackupStats() {
  const list = state.backups || [];
  const totalBytes = list.reduce((sum, b) => sum + (Number(b.size) || 0), 0);
  const setText = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  setText("backupCountKpi", String(list.length));
  setText("backupSizeKpi", list.length ? formatBackupSize(totalBytes) : "0 B");
  const times = list.map((b) => new Date(b.created_at).getTime()).filter((t) => !Number.isNaN(t));
  setText("backupLatestKpi", times.length ? formatBackupTime(new Date(Math.max(...times)).toISOString()) : "—");
  const pill = document.getElementById("backupCountPill");
  if (pill) {
    pill.className = "pill " + (list.length ? "pill-ok" : "pill-neutral");
    pill.textContent = `${list.length} 个`;
  }
  const navCount = document.getElementById("navCountBackup");
  if (navCount) {
    navCount.textContent = String(list.length);
    navCount.style.display = list.length ? "" : "none";
  }
}

export async function loadBackups() {
  try {
    const data = await apiFetch("/_admin/api/backup/list");
    state.backups = data.items || [];
    renderBackupList();
  } catch (e) { showToast(e.message, true); }
}

function renderBackupList() {
  const tbody = document.getElementById("backupBody");
  renderBackupStats();
  if (!tbody) return;
  if (!state.backups.length) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty" style="padding:26px 0">暂无备份，点右上角「创建备份」生成第一份快照</td></tr>`;
    return;
  }
  tbody.innerHTML = state.backups.map((b) => `
    <tr>
      <td class="mono" style="word-break:break-all">${esc(b.filename)}</td>
      <td>${esc(formatBackupSize(b.size))}</td>
      <td>${esc(formatBackupTime(b.created_at))}</td>
      <td><div style="display:flex;gap:6px;justify-content:flex-end;flex-wrap:wrap">
        <button class="btn btn-sm" data-action="download-backup" data-filename="${esc(b.filename)}">下载</button>
        <button class="btn btn-sm" data-action="restore-backup" data-filename="${esc(b.filename)}">恢复</button>
        <button class="btn btn-sm btn-danger" data-action="delete-backup" data-filename="${esc(b.filename)}">删除</button>
      </div></td>
    </tr>`).join("");
}

export async function createBackup() {
  try {
    const data = await apiFetch("/_admin/api/backup/create", { method: "POST" });
    showToast(`备份已创建: ${data.filename}`);
    await loadBackups();
  } catch (e) { showToast(e.message, true); }
}

export function downloadBackup(filename) {
  const a = document.createElement("a");
  a.href = `/_admin/api/backup/download/${encodeURIComponent(filename)}`;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

export function openRestoreModal(filename) {
  openFormModal({
    title: "恢复备份",
    sub: filename,
    schema: [
      { key: "mode", label: "恢复模式", type: "select", options: [{ value: "overwrite", label: "覆盖模式" }, { value: "merge", label: "合并模式" }] },
    ],
    values: { mode: "overwrite" },
    onSave: async (out) => {
      const formData = new FormData();
      formData.append("restore_mode", out.mode);
      formData.append("backup_filename", filename);
      const response = await fetch("/_admin/api/backup/restore", { method: "POST", body: formData });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || "恢复失败");
      showToast(data.message || "恢复成功");
      await loadBackups();
    },
  });
}

export function openUploadRestoreModal() {
  const fileInput = document.getElementById("backupFile");
  if (!fileInput || !fileInput.files.length) { showToast("请先选择要上传的数据库文件", true); return; }
  openFormModal({
    title: "上传并恢复",
    sub: fileInput.files[0].name,
    schema: [
      { key: "mode", label: "恢复模式", type: "select", options: [{ value: "overwrite", label: "覆盖模式" }, { value: "merge", label: "合并模式" }] },
    ],
    values: { mode: "overwrite" },
    onSave: async (out) => {
      const formData = new FormData();
      formData.append("restore_mode", out.mode);
      formData.append("file", fileInput.files[0]);
      const response = await fetch("/_admin/api/backup/restore", { method: "POST", body: formData });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || "恢复失败");
      showToast(data.message || "恢复成功");
      fileInput.value = "";
      await loadBackups();
    },
  });
}

export async function deleteBackup(filename) {
  openConfirm({
    title: "删除备份",
    message: `确认删除备份文件 ${filename} 吗？`,
    onOk: async () => {
      try {
        await apiFetch(`/_admin/api/backup/${encodeURIComponent(filename)}`, { method: "DELETE" });
        showToast("备份已删除");
        await loadBackups();
      } catch (e) { showToast(e.message, true); }
    },
  });
}

// ============ 筛选 chips 初始化 ============

export function initFilterSelects() {
  const toolbars = [
    document.getElementById("ruleToolbar"),
    document.querySelector("#page-logs [data-panel='req'] .toolbar"),
  ];
  toolbars.forEach((tb) => {
    if (!tb) return;
    tb.querySelectorAll("select").forEach((sel) => {
      if (sel.id === "log_page_size") return; // 分页大小不是筛选条件，不进 chips
      if (sel.dataset.default === undefined) {
        sel.dataset.default = sel.value;
        const item = sel.closest(".filter-item");
        sel.dataset.label = item ? (item.querySelector(".fi-label")?.textContent || "") : "";
      }
    });
  });
}
