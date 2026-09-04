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
  openDrawer, closeDrawer, syncSelect,
} from './components.js';

const esc = (v) => escapeHtml(v == null ? "" : String(v));

// ============ 页面激活 / 导航 ============

const VALID_PAGES = ["overview", "routing", "security", "geo", "logs", "system"];

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
    geo: "IP 定位", logs: "日志与审计", system: "系统设置",
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
      break;
    case "routing":
      refreshRouting();
      break;
    case "security":
      loadBannedIpList();
      loadAutoBanSettings();
      loadAutoBanStats();
      if (getChecked("ban_auto_refresh_enabled")) startBanAutoRefresh();
      break;
    case "geo":
      refreshGeo();
      break;
    case "logs":
      refreshRouteLogModule().catch((e) => showToast(e.message, true));
      refreshAppLogModule().catch((e) => showToast(e.message, true));
      if (getChecked("log_auto_refresh_enabled")) startAutoRefresh();
      break;
    case "system":
      loadIpCacheSettings();
      loadIpCacheStats();
      loadDedupSettings();
      loadDedupStats();
      loadEmailSettings();
      loadBackups();
      break;
  }
}

let _hashRoutingInitialized = false;
export function initHashRouting() {
  const hash = window.location.hash.replace(/^#\/?/, "");
  if (hash && VALID_PAGES.includes(hash) && hash !== state.activeModule) {
    activatePage(hash);
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

function renderTrendSvg(hours) {
  const container = document.getElementById("trendChart");
  if (!container) return;
  const h = Array.isArray(hours) ? hours : [];
  const counts = h.map((x) => x.count || 0);
  const redirects = h.map((x) => x.redirects || 0);
  const failed = h.map((x) => x.failed || 0);
  const n = Math.max(counts.length, 1);
  const W = 680, H = 210, padL = 6, padR = 6, padT = 14, padB = 22;
  if (n < 2) {
    container.innerHTML = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:${H}px"><text x="${W / 2}" y="${H / 2 + 4}" fill="var(--text-3)" font-size="12" text-anchor="middle">暂无 24 小时趋势数据</text></svg>`;
    return;
  }
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;
  const stepX = innerW / (n - 1);
  const maxValue = Math.max(1, ...counts, ...redirects, ...failed);
  const yOf = (v) => padT + innerH - (maxValue > 0 ? (v / maxValue) * innerH : 0);
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
  let grid = "";
  for (let g = 1; g <= 3; g++) {
    const y = padT + (innerH * g) / 4;
    grid += `<line x1="${padL}" y1="${y.toFixed(1)}" x2="${W - padR}" y2="${y.toFixed(1)}" stroke="var(--border)" stroke-width="1" stroke-dasharray="3 4"/>`;
  }
  let labels = "";
  [0, 6, 12, 18, 23].forEach((i) => {
    if (i >= n) return;
    const x = padL + i * stepX;
    const anchor = i === 0 ? "start" : i === n - 1 ? "end" : "middle";
    const ts = h[i] && h[i].ts;
    let label;
    if (ts) {
      // 桶内 ts 为 UTC 整点；按浏览器本地时区呈现为正常时钟时间（HH:00）
      const d = new Date(ts * 1000);
      label = String(d.getHours()).padStart(2, "0") + ":00";
    } else {
      const hoursAgo = n - 1 - i;
      label = hoursAgo === 0 ? "现在" : "-" + hoursAgo + "h";
    }
    labels += `<text x="${x.toFixed(1)}" y="${H - 6}" fill="var(--text-3)" font-size="10" text-anchor="${anchor}">${label}</text>`;
  });
  const lastX = padL + (n - 1) * stepX;
  const lastY = yOf(counts[n - 1] || 0);
  container.innerHTML = `
    <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:${H}px">
      <defs>
        <linearGradient id="ovTrendFill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--brand)" stop-opacity="0.22"/>
          <stop offset="100%" stop-color="var(--brand)" stop-opacity="0"/>
        </linearGradient>
      </defs>
      ${grid}
      <path d="${area}" fill="url(#ovTrendFill)"/>
      <path d="${line}" fill="none" stroke="var(--brand)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
      <path d="${lineRedirect}" fill="none" stroke="var(--info)" stroke-width="1.5" stroke-dasharray="4 3" stroke-linejoin="round" stroke-linecap="round"/>
      <path d="${lineFailed}" fill="none" stroke="var(--danger)" stroke-width="1.5" stroke-dasharray="2 3" stroke-linejoin="round" stroke-linecap="round"/>
      <circle cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="3.5" fill="var(--brand)" stroke="var(--surface)" stroke-width="1.5"/>
      ${labels}
    </svg>`;
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
function startNetAutoRefresh() {
  stopNetAutoRefresh();
  _netAutoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "overview") { stopNetAutoRefresh(); return; }
    loadNetThroughput().catch(() => {});
  }, 2000);
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
  const redirect = st.redirected_requests || 0;
  const failed = st.failed_requests || 0;

  setText("kpiTotal", todayRequests.toLocaleString("en-US"));
  const totalDelta = document.getElementById("kpiTotalDelta");
  if (totalDelta) totalDelta.textContent = `累计 ${totalAll.toLocaleString("en-US")} 次请求`;
  setText("kpiRedirect", redirect.toLocaleString("en-US"));
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

  // 趋势三序列（总请求 / 302 跟随 / 失败拦截），24 个整点桶（含 UTC ts，供 x 轴呈现本地时钟时间）
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
  const values = group ? {
    path_prefix: group.path_prefix,
    request_host: normalizeRequestHost(group.request_host),
    access_ip_whitelist: group.access_ip_whitelist || "",
    ip_blacklist: group.ip_blacklist || "",
    region_whitelist: group.region_whitelist || "",
    region_blacklist: group.region_blacklist || "",
    notes: group.notes || "",
    region_matching_enabled: Boolean(group.region_matching_enabled),
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
      return null;
    },
    onSave: async (out) => {
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
      };
      if (isEdit) {
        await apiFetch("/_admin/api/route-groups", { method: "PUT", body: JSON.stringify(payload) });
        showToast("路径前缀已更新。");
      } else {
        await apiFetch("/_admin/api/route-groups", { method: "POST", body: JSON.stringify(payload) });
        showToast("路径前缀已创建。");
      }
      await loadDashboard();
    },
  });
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
      <td class="cell-truncate" title="${esc(rule.target_url)}">${esc(rule.target_url)}</td>
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

const RULE_SCHEMA = [
  { key: "name", label: "规则名称", type: "text", required: true, placeholder: "示例流媒体" },
  { key: "target_url", label: "目标地址", type: "text", required: true, placeholder: "https://target.example.com" },
  { key: "priority", label: "优先级", type: "number", default: 0 },
  { key: "timeout", label: "超时（秒）", type: "number", default: 30 },
  { key: "max_redirects", label: "最大重定向", type: "number", default: 10 },
  { key: "retry_times", label: "重试次数", type: "number", default: 3 },
  { key: "path_rewrite_pattern", label: "正则重写·匹配", type: "text", placeholder: "^(.*)$" },
  { key: "path_rewrite_replacement", label: "正则重写·替换", type: "text", placeholder: "/new$1" },
  { key: "ip_whitelist", label: "IP 白名单（路由）", type: "text" },
  { key: "region_filters", label: "地区条件", type: "text", placeholder: "CN,HK" },
  { key: "access_ip_whitelist", label: "访问控制 IP 白", type: "text" },
  { key: "ip_blacklist", label: "访问控制 IP 黑", type: "text" },
  { key: "region_whitelist", label: "地区白名单", type: "text" },
  { key: "region_blacklist", label: "地区黑名单", type: "text" },
  { key: "notes", label: "备注", type: "text" },
  { key: "enabled", label: "启用", type: "switch" },
  { key: "is_default", label: "默认规则", type: "switch" },
  { key: "strip_prefix", label: "去前缀", type: "switch" },
  { key: "follow_redirects", label: "跟随重定向", type: "switch" },
  { key: "enable_streaming", label: "流式转发", type: "switch" },
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

  const values = rule ? {
    route_group: String(selectedIndex),
    name: rule.name, target_url: rule.target_url, priority: rule.priority ?? 0, timeout: rule.timeout ?? 30,
    max_redirects: rule.max_redirects ?? 10, retry_times: rule.retry_times ?? 3,
    path_rewrite_pattern: rule.path_rewrite_pattern || "", path_rewrite_replacement: rule.path_rewrite_replacement || "",
    ip_whitelist: rule.ip_whitelist || "", region_filters: rule.region_filters || "",
    access_ip_whitelist: rule.access_ip_whitelist || "", ip_blacklist: rule.ip_blacklist || "",
    region_whitelist: rule.region_whitelist || "", region_blacklist: rule.region_blacklist || "",
    notes: rule.notes || "", enabled: Boolean(rule.enabled), is_default: Boolean(rule.is_default),
    strip_prefix: Boolean(rule.strip_prefix), follow_redirects: rule.follow_redirects !== false,
    enable_streaming: Boolean(rule.enable_streaming),
  } : {
    route_group: String(selectedIndex),
    priority: 0, timeout: 30, max_redirects: 10, retry_times: 3,
    enabled: true, is_default: false, strip_prefix: false, follow_redirects: true, enable_streaming: true,
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
      const payload = {
        name: out.name, path_prefix: gi.path_prefix, request_host: gi.request_host,
        target_url: out.target_url, ip_whitelist: out.ip_whitelist || "", region_filters: out.region_filters || "",
        access_ip_whitelist: out.access_ip_whitelist || "", ip_blacklist: out.ip_blacklist || "",
        region_whitelist: out.region_whitelist || "", region_blacklist: out.region_blacklist || "",
        priority: Number(out.priority ?? 0), timeout: Number(out.timeout ?? 30),
        max_redirects: Number(out.max_redirects ?? 10), retry_times: Number(out.retry_times ?? 3),
        notes: out.notes || "", path_rewrite_pattern: out.path_rewrite_pattern || "",
        path_rewrite_replacement: out.path_rewrite_replacement || "",
        enabled: Boolean(out.enabled), is_default: Boolean(out.is_default),
        strip_prefix: Boolean(out.strip_prefix), follow_redirects: Boolean(out.follow_redirects),
        enable_streaming: Boolean(out.enable_streaming),
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
      ${kv("优先级", cell(rule.priority ?? 0))}
      ${kv("默认规则", bool(rule.is_default))}
      ${kv("启用状态", bool(rule.enabled))}
      <div class="section-h" style="margin-top:16px">请求处理</div>
      ${kv("超时(秒)", cell(rule.timeout ?? 30))}
      ${kv("最大重定向", cell(rule.max_redirects ?? 10))}
      ${kv("重试次数", cell(rule.retry_times ?? 3))}
      ${kv("去前缀", bool(rule.strip_prefix))}
      ${kv("跟随重定向", bool(rule.follow_redirects !== false))}
      ${kv("流式转发", bool(rule.enable_streaming))}
      ${kv("正则模式", cell(rule.path_rewrite_pattern))}
      ${kv("正则替换", cell(rule.path_rewrite_replacement))}
      <div class="section-h" style="margin-top:16px">访问控制</div>
      ${kv("IP 白名单(路由)", cell(rule.ip_whitelist))}
      ${kv("地区条件", cell(rule.region_filters))}
      ${kv("访问控制 IP 白", cell(rule.access_ip_whitelist))}
      ${kv("访问控制 IP 黑", cell(rule.ip_blacklist))}
      ${kv("地区白名单", cell(rule.region_whitelist))}
      ${kv("地区黑名单", cell(rule.region_blacklist))}
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
            <div class="route-log-field"><span class="route-log-field-label">请求</span><div class="route-log-field-value"><strong>${esc(log.request_method || "-")}</strong><span class="route-log-path" title="${esc(log.request_path || "")}">${esc(log.request_path || "-")}</span>${log.request_query_string ? `<span class="route-log-query" title="${esc(log.request_query_string)}">?${esc(log.request_query_string)}</span>` : ""}</div></div>
            <div class="route-log-field"><span class="route-log-field-label">域名</span><div class="route-log-field-value"><span>${esc(formatRouteLogRequestHost(log.request_host || ""))}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">前缀</span><div class="route-log-field-value"><strong>${esc(log.path_prefix || "-")}</strong></div></div>
            <div class="route-log-field"><span class="route-log-field-label">规则</span><div class="route-log-field-value"><span>${esc(log.rule_name || "-")}</span><span class="hint">命中域名: ${esc(formatRouteLogRuleRequestHost(log.rule_request_host || ""))}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">地区</span><div class="route-log-field-value"><strong>${esc(log.geo_summary || "-")}</strong><span class="hint">命中: ${esc(log.matched_region || "-")}</span><span class="hint">源: ${esc(log.geo_source || "-")}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">匹配</span><div class="route-log-field-value"><strong>${esc(formatMatchStrategy(log.match_strategy))}</strong><span class="hint">${esc(formatMatchDetail(log.match_detail))}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">302地址</span><div class="route-log-field-value"><strong class="route-log-target-url" title="${esc(log.redirect_location || "")}">${esc(log.redirect_location || "-")}</strong></div></div>
            <div class="route-log-field"><span class="route-log-field-label">转发结果</span><div class="route-log-field-value"><strong class="route-log-target-url" title="${esc(log.target_url || "")}">${esc(log.target_url || "-")}</strong><span class="hint">上游: ${esc(String(log.upstream_status || 0))}</span><span class="cache-status-badge ${cacheStatusInfo.cls}">${esc(cacheStatusInfo.text)}</span><span class="hint">结果: ${esc(formatResultStatus(log.result_status))}</span></div></div>
            <div class="route-log-field"><span class="route-log-field-label">IP</span><div class="route-log-field-value"><span>原始: ${esc(log.original_client_ip || "-")}</span><span>匹配: ${esc(log.client_ip || "-")}</span></div></div>
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
  await Promise.all([loadRouteLogSettings(), loadRouteLogs()]);
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
    <div class="file-item ${f.name === prev ? "current" : ""}" data-action="select-log-file" data-file="${esc(f.name)}">
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
      const pill = document.getElementById("autoBanStatusPill");
      if (pill) { pill.className = "pill " + (data.enabled ? "pill-ok" : "pill-neutral"); pill.textContent = data.enabled ? "已启用" : "已停用"; }
    }
  } catch (_) {}
}

export async function loadAutoBanStats() {
  try {
    const stats = await apiFetch("/_admin/api/auto-ban/stats");
    const body = document.getElementById("autoBanStatsBody");
    if (!body || !stats) return;
    body.innerHTML = `
      <div class="kv"><div class="k">跟踪 IP 数</div><div class="val">${esc(String(stats.tracked_ips ?? 0))}</div></div>
      <div class="kv"><div class="k">白名单 IP 数</div><div class="val">${esc(String(stats.whitelisted_ips ?? 0))}</div></div>
      <div class="kv"><div class="k">总请求数</div><div class="val">${esc(String(stats.total_requests ?? 0))}</div></div>
      <div class="kv"><div class="k">累计封禁</div><div class="val">${esc(String(stats.total_bans ?? 0))}</div></div>`;
  } catch (_) {}
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
        }),
      });
      await Promise.all([loadAutoBanSettings(), loadAutoBanStats()]);
      showToast("自动封禁配置已保存。");
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
      if (pill) { pill.className = "pill " + (data.enabled ? "pill-ok" : "pill-neutral"); pill.textContent = data.enabled ? "已配置" : "未配置"; }
      if (body) {
        body.innerHTML = `
          <div class="kv"><div class="k">SMTP 主机</div><div class="val">${esc(data.smtp_host || "-")}</div></div>
          <div class="kv"><div class="k">发件人</div><div class="val">${esc(data.sender || "-")}</div></div>
          <div class="kv"><div class="k">收件人</div><div class="val">${esc(data.recipients || "-")}</div></div>`;
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
      <td><div style="display:flex;gap:6px;justify-content:flex-end;flex-wrap:wrap">${extendBtn}<button class="btn btn-sm btn-danger" data-action="unban-ip" data-ip="${esc(item.ip)}">解封</button></div></td>`;
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

// ============ 备份管理 ============

function formatBackupSize(bytes) {
  if (bytes >= 1024 * 1024) return (bytes / 1024 / 1024).toFixed(2) + " MB";
  if (bytes >= 1024) return (bytes / 1024).toFixed(1) + " KB";
  return bytes + " B";
}
function formatBackupTime(isoStr) {
  try { return new Date(isoStr).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" }); } catch { return isoStr; }
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
  if (!tbody) return;
  if (!state.backups.length) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty" style="padding:26px 0">暂无备份</td></tr>`;
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
