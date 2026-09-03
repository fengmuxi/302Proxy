/**
 * admin.js - 入口 / 外壳（原型 6 页版）
 * 负责：DOMContentLoaded 初始化、导航、鉴权、主题、⌘K 搜索、事件委托接线
 * 业务逻辑见 js/modules.js；UI 组件见 js/components.js；请求见 js/api.js。
 */

import { state } from './js/state.js';
import {
  setValue, setChecked, getValue, getChecked,
  normalizeRequestHost, formatRequestHostLabel, findRouteGroup, focusField, escapeHtml,
} from './js/utils.js';
import { apiFetch, loadAuthStatus, submitLogin, performLogout } from './js/api.js';
import {
  showToast, setAuthError, applyAuthState,
  initTheme, applyTheme,
  openConfirm, closeModal, closeDrawer,
  showUrlTooltip, hideUrlTooltip, copyToClipboard,
  renderAllChips, closeSelectPops, upgradeSelect,
} from './js/components.js';
import {
  setActivePage, activatePage, initHashRouting,
  loadDashboard, renderOverview,
  renderRouteGroups, openRouteGroupModal, updateGroupRegionSwitch,
  renderRules, openRuleModal, prepareRuleForGroup, removeRule, toggleRule, openRuleDrawer,
  renderGeoSources, openGeoSourceModal, persistGeoSettings,
  openGeoOnlineSettings, openGeoOfflineSettings, openGeoSourceTest, openOfflineTest,
  syncOffline, rollbackOffline, clearGeoCache,
  loadRouteLogs, refreshRouteLogModule, saveLogRetention, cleanupLogs,
  getAutoRefreshConfig, saveAutoRefreshConfig, startAutoRefresh, stopAutoRefresh,
  loadAppLogContent, startAppLogAutoRefresh, stopAppLogAutoRefresh,
  refreshAppLogModule, cleanupAppLogFiles,
  loadIpCacheSettings, loadIpCacheStats, openIpCacheSettings, clearIpCache,
  loadDedupSettings, loadDedupStats, openDedupSettings, clearDedupCache,
  loadAutoBanSettings, loadAutoBanStats, openAutoBanSettings,
  loadEmailSettings, openEmailSettings, testEmail,
  loadBannedIpList, renderBannedIpListPage, openBanModal, openBanExtendModal,
  banIpFromLog, unbanIp, clearBans,
  getBanAutoRefreshConfig, saveBanAutoRefreshConfig, startBanAutoRefresh, stopBanAutoRefresh,
  loadBackups, createBackup, downloadBackup, openRestoreModal, openUploadRestoreModal, deleteBackup,
  initFilterSelects,
} from './js/modules.js';

// ============ 小工具 ============

const $ = (id) => document.getElementById(id);

function confirmAsync(title, message, fn) {
  openConfirm({
    title,
    message,
    onOk: async () => {
      try { await fn(); }
      catch (e) { showToast(e.message, true); }
    },
  });
}

// ============ 导航 ============

function bindNavigation() {
  document.querySelectorAll(".nav-item[data-page]").forEach((item) => {
    item.addEventListener("click", () => activatePage(item.dataset.page));
  });

  // 概览卡片内跳转按钮
  document.addEventListener("click", (e) => {
    const target = e.target.closest("[data-goto]");
    if (target) activatePage(target.dataset.goto);
  });

  // 移动端侧边栏开关
  $("menuToggle")?.addEventListener("click", () => {
    $("sidebar")?.classList.toggle("open");
  });
}

// ============ 顶栏 ============

function bindTopbar() {
  $("themeBtn")?.addEventListener("click", () => {
    const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
    applyTheme(next);
  });

  $("newRuleBtn")?.addEventListener("click", () => openRuleModal(null));

  $("ovRefreshBtn")?.addEventListener("click", async () => {
    const btn = $("ovRefreshBtn");
    if (btn) { btn.disabled = true; btn.textContent = "刷新中…"; }
    try { await renderOverview(); }
    catch (e) { showToast(e.message, true); }
    finally { if (btn) { btn.disabled = false; btn.textContent = "刷新"; } }
  });
}

// ============ 路由配置 ============

function bindRouting() {
  $("newGroupBtn")?.addEventListener("click", () => openRouteGroupModal(null));
  $("newRuleInlineBtn")?.addEventListener("click", () => openRuleModal(null));

  const applyRulesFilter = () => { renderRules(state.rules); renderAllChips(); };
  $("ruleSearch")?.addEventListener("input", applyRulesFilter);
  $("ruleFilter")?.addEventListener("change", applyRulesFilter);
  $("ruleHost")?.addEventListener("change", applyRulesFilter);
  $("resetFilter")?.addEventListener("click", () => {
    setValue("ruleSearch", "");
    setValue("ruleFilter", "");
    setValue("ruleHost", "");
    applyRulesFilter();
  });

  // 路由组表事件委托
  $("groupBody")?.addEventListener("click", async (e) => {
    const sw = e.target.closest(".switch[data-action='toggle-group-region']");
    if (sw) {
      const pathPrefix = sw.dataset.pathPrefix;
      const requestHost = normalizeRequestHost(sw.dataset.requestHost);
      const next = !sw.classList.contains("on");
      try {
        await updateGroupRegionSwitch(pathPrefix, requestHost, next);
        await loadDashboard();
        showToast(`${pathPrefix} @ ${formatRequestHostLabel(requestHost)} 的地区匹配已${next ? "开启" : "关闭"}。`);
      } catch (err) { showToast(err.message, true); }
      return;
    }
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const pathPrefix = btn.dataset.pathPrefix;
    const requestHost = normalizeRequestHost(btn.dataset.requestHost);
    const action = btn.dataset.action;
    if (action === "create-rule-for-group") {
      prepareRuleForGroup(pathPrefix, requestHost);
    } else if (action === "edit-group") {
      const group = findRouteGroup(pathPrefix, requestHost, state);
      if (group) openRouteGroupModal(group);
    } else if (action === "delete-group") {
      const group = findRouteGroup(pathPrefix, requestHost, state);
      if (!group) return;
      confirmAsync("删除路由组", `确认删除路径前缀 ${pathPrefix} @ ${formatRequestHostLabel(requestHost)} 吗？此操作不可撤销。`, async () => {
        await apiFetch("/_admin/api/route-groups", {
          method: "DELETE",
          body: JSON.stringify({ path_prefix: pathPrefix, request_host: requestHost }),
        });
        await loadDashboard();
        showToast("路径前缀已删除。");
      });
    }
  });

  // 规则表事件委托（开关 / 编辑 / 删除 / 行点击查看抽屉）
  $("rulesBody")?.addEventListener("click", async (e) => {
    const sw = e.target.closest(".switch[data-action='toggle-rule']");
    if (sw) {
      const ruleId = Number(sw.dataset.id);
      const enabled = !sw.classList.contains("on");
      await toggleRule(ruleId, enabled);
      return;
    }
    const btn = e.target.closest("button[data-action]");
    if (btn) {
      const action = btn.dataset.action;
      const ruleId = Number(btn.dataset.id);
      if (action === "edit-rule") {
        const rule = state.rules.find((r) => r.id === ruleId);
        if (rule) openRuleModal(rule);
      } else if (action === "delete-rule") {
        removeRule(ruleId);
      }
      return;
    }
    const row = e.target.closest("tr[data-rule-id]");
    if (row) openRuleDrawer(row.dataset.ruleId);
  });
}

// ============ 安全与封禁 ============

function bindSecurity() {
  $("manualBanBtn")?.addEventListener("click", () => openBanModal({ mode: "add" }));
  $("clearBans")?.addEventListener("click", () => clearBans());
  $("editAutoBanBtn")?.addEventListener("click", () => openAutoBanSettings());

  // 封禁表事件委托
  $("banBody")?.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const ip = btn.dataset.ip;
    const action = btn.dataset.action;
    if (action === "unban-ip") unbanIp(ip);
    else if (action === "extend-ban-ip") openBanExtendModal(ip, parseFloat(btn.dataset.expire || "0") || 0);
  });

  // 封禁自动刷新
  $("ban_auto_refresh_enabled")?.addEventListener("change", () => {
    if (getChecked("ban_auto_refresh_enabled")) startBanAutoRefresh();
    else { stopBanAutoRefresh(); saveBanAutoRefreshConfig({ enabled: false, interval: parseInt(getValue("ban_auto_refresh_interval") || "5", 10) || 5 }); }
  });
  $("ban_auto_refresh_interval")?.addEventListener("change", () => {
    const interval = parseInt(getValue("ban_auto_refresh_interval") || "5", 10) || 5;
    saveBanAutoRefreshConfig({ enabled: getChecked("ban_auto_refresh_enabled"), interval });
    if (getChecked("ban_auto_refresh_enabled")) startBanAutoRefresh();
  });
  $("ban_page_size")?.addEventListener("change", () => {
    const size = parseInt(getValue("ban_page_size") || "20", 10) || 20;
    state.banPageSize = Math.max(1, size);
    state.banCurrentPage = 1;
    localStorage.setItem("ban_page_size", String(state.banPageSize));
    renderBannedIpListPage();
  });
}

// ============ IP 定位 ============

function bindGeo() {
  $("editGeoOnlineBtn")?.addEventListener("click", () => openGeoOnlineSettings());
  $("addGeoSourceBtn")?.addEventListener("click", () => openGeoSourceModal(null, null));
  $("editGeoOfflineBtn")?.addEventListener("click", () => openGeoOfflineSettings());
  $("editGeoCacheBtn")?.addEventListener("click", () => openGeoOnlineSettings());

  $("syncMMDB")?.addEventListener("click", () => syncOffline().catch((e) => showToast(e.message, true)));
  $("rollbackMMDB")?.addEventListener("click", () => rollbackOffline().catch((e) => showToast(e.message, true)));
  $("testMMDB")?.addEventListener("click", () => openOfflineTest());
  $("clearGeoCache")?.addEventListener("click", () => clearGeoCache().catch((e) => showToast(e.message, true)));
  $("clearCache")?.addEventListener("click", () => clearGeoCache().catch((e) => showToast(e.message, true)));

  // 在线源事件委托（开关 / 测试 / 编辑 / 删除）
  $("geoSourceBody")?.addEventListener("click", async (e) => {
    const sw = e.target.closest(".switch[data-action='toggle-geo-source']");
    if (sw) {
      const idx = Number(sw.dataset.index);
      const src = state.geoSources[idx];
      if (!src) return;
      const prev = state.geoSources.map((s) => ({ ...s }));
      src.enabled = !src.enabled;
      renderGeoSources();
      try { await persistGeoSettings(src.enabled ? "在线源已启用。" : "在线源已禁用。"); }
      catch (err) { state.geoSources = prev; renderGeoSources(); showToast(err.message, true); }
      return;
    }
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const idx = Number(btn.dataset.index);
    const src = state.geoSources[idx];
    const action = btn.dataset.action;
    if (action === "edit-geo-source") {
      if (src) openGeoSourceModal(src, idx);
    } else if (action === "test-geo-source") {
      openGeoSourceTest(idx);
    } else if (action === "delete-geo-source") {
      if (!src) return;
      confirmAsync("删除在线源", `确认删除在线源 ${src.name || src.url} 吗？`, async () => {
        const prev = state.geoSources.map((s) => ({ ...s }));
        state.geoSources.splice(idx, 1);
        renderGeoSources();
        try { await persistGeoSettings("在线源已删除。"); }
        catch (err) { state.geoSources = prev; renderGeoSources(); showToast(err.message, true); }
      });
    }
  });
}

// ============ 日志与审计 ============

function switchLogTab(tabName) {
  document.querySelectorAll(".tab[data-tab]").forEach((t) => t.classList.toggle("active", t.dataset.tab === tabName));
  document.querySelectorAll("#page-logs [data-panel]").forEach((p) => {
    p.hidden = p.dataset.panel !== tabName;
  });
}

function bindLogs() {
  // Tab 切换（请求日志 / 应用日志）
  document.querySelectorAll(".tab[data-tab]").forEach((tab) => {
    tab.addEventListener("click", () => switchLogTab(tab.dataset.tab));
  });

  // 请求日志筛选表单
  $("route-log-filter-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    state.logCurrentPage = 1;
    try { await loadRouteLogs(); showToast("日志查询已更新。"); }
    catch (err) { showToast(err.message, true); }
  });

  $("route-log-reset-btn")?.addEventListener("click", async () => {
    setValue("log_keyword", "");
    setValue("log_path_prefix", "");
    setValue("log_rule_request_host", "");
    setValue("log_match_strategy", "");
    setValue("log_result_status", "");
    setValue("log_date_from", "");
    setValue("log_date_to", "");
    state.logCurrentPage = 1;
    renderAllChips();
    try { await loadRouteLogs(); } catch (err) { showToast(err.message, true); }
  });

  // 筛选下拉变化时更新 chips（数据由“查询”按钮触发）
  $("log_match_strategy")?.addEventListener("change", renderAllChips);
  $("log_result_status")?.addEventListener("change", renderAllChips);

  // 全选 / 批量删除 / 清空
  $("route-log-select-all")?.addEventListener("change", (e) => {
    const checked = Boolean(e.target.checked);
    document.querySelectorAll(".route-log-checkbox").forEach((cb) => { cb.checked = checked; });
  });

  $("route-log-delete-selected-btn")?.addEventListener("click", () => {
    const ids = Array.from(document.querySelectorAll(".route-log-checkbox:checked"))
      .map((cb) => Number(cb.dataset.id)).filter((v) => Number.isInteger(v) && v > 0);
    if (!ids.length) { showToast("请先选择要删除的日志", true); return; }
    confirmAsync("删除日志", `确认删除选中的 ${ids.length} 条日志吗？`, async () => {
      await apiFetch("/_admin/api/logs", { method: "DELETE", body: JSON.stringify({ ids }) });
      await refreshRouteLogModule();
      showToast("选中日志已删除。");
    });
  });

  const clearAllLogs = () => confirmAsync("清空日志", "确认清空所有规则转发日志吗？此操作不可恢复！", async () => {
    await apiFetch("/_admin/api/logs", { method: "DELETE", body: JSON.stringify({ delete_all: true }) });
    await refreshRouteLogModule();
    showToast("规则转发日志已清空。");
  });
  $("route-log-delete-all-btn")?.addEventListener("click", clearAllLogs);
  $("logClearAllBtn")?.addEventListener("click", clearAllLogs);

  // 日志列表事件委托（复制 / 删除 / 封禁 / 解禁 + URL tooltip）
  const logList = $("route-logs-list-body");
  logList?.addEventListener("mouseover", (e) => {
    const t = e.target.closest(".route-log-target-url");
    if (t) showUrlTooltip(e, t.getAttribute("title") || t.textContent);
  });
  logList?.addEventListener("mouseout", (e) => {
    if (e.target.closest(".route-log-target-url")) hideUrlTooltip();
  });
  logList?.addEventListener("click", async (e) => {
    const urlTarget = e.target.closest(".route-log-target-url");
    if (urlTarget) { copyToClipboard(urlTarget.getAttribute("title") || urlTarget.textContent); return; }
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const action = btn.dataset.action;
    if (action === "delete-route-log") {
      const logId = Number(btn.dataset.id);
      confirmAsync("删除日志", `确认删除日志 #${logId} 吗？`, async () => {
        await apiFetch("/_admin/api/logs", { method: "DELETE", body: JSON.stringify({ ids: [logId] }) });
        await refreshRouteLogModule();
        showToast("日志已删除。");
      });
    } else if (action === "ban-ip-from-log") {
      const ip = btn.dataset.ip;
      if (!ip || ip === "-") { showToast("该日志没有可封禁的 IP 地址", true); return; }
      banIpFromLog(ip);
    } else if (action === "unban-ip-from-log") {
      const ip = btn.dataset.ip;
      if (!ip || ip === "-") { showToast("该日志没有可解禁的 IP 地址", true); return; }
      unbanIp(ip);
    }
  });

  // 日志自动刷新
  $("log_auto_refresh_enabled")?.addEventListener("change", () => {
    if (getChecked("log_auto_refresh_enabled")) startAutoRefresh();
    else { stopAutoRefresh(); saveAutoRefreshConfig({ enabled: false, interval: parseInt(getValue("log_auto_refresh_interval") || "5", 10) || 5 }); }
  });
  $("log_auto_refresh_interval")?.addEventListener("change", () => {
    const interval = parseInt(getValue("log_auto_refresh_interval") || "5", 10) || 5;
    saveAutoRefreshConfig({ enabled: getChecked("log_auto_refresh_enabled"), interval });
    if (getChecked("log_auto_refresh_enabled")) startAutoRefresh();
  });
  $("log_page_size")?.addEventListener("change", () => {
    const size = parseInt(getValue("log_page_size") || "50", 10) || 50;
    state.logPageSize = Math.max(1, size);
    state.logCurrentPage = 1;
    localStorage.setItem("log_page_size", String(state.logPageSize));
    loadRouteLogs().catch((err) => showToast(err.message, true));
  });

  // 保留策略
  $("saveRetention")?.addEventListener("click", () => saveLogRetention().catch((e) => showToast(e.message, true)));
  $("cleanupLogs")?.addEventListener("click", () => cleanupLogs().catch((e) => showToast(e.message, true)));

  // 应用日志
  $("app-log-refresh-btn")?.addEventListener("click", () => refreshAppLogModule().catch((e) => showToast(e.message, true)));
  $("app-log-cleanup-btn")?.addEventListener("click", () => cleanupAppLogFiles().catch((e) => showToast(e.message, true)));
  $("app-log-search-btn")?.addEventListener("click", () => loadAppLogContent().catch((e) => showToast(e.message, true)));
  $("app-log-tail-lines")?.addEventListener("change", () => loadAppLogContent().catch((e) => showToast(e.message, true)));
  $("app-log-keyword")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") loadAppLogContent().catch((err) => showToast(err.message, true));
  });
  $("app-log-auto-refresh")?.addEventListener("change", () => {
    if (getChecked("app-log-auto-refresh")) startAppLogAutoRefresh();
    else stopAppLogAutoRefresh();
  });
  $("appLogFiles")?.addEventListener("click", (e) => {
    const item = e.target.closest("[data-action='select-log-file']");
    if (!item) return;
    state.appLogFile = item.dataset.file;
    refreshAppLogModule().catch(() => {});
  });
}

// ============ 系统设置 ============

function bindSystem() {
  $("createBackupBtn")?.addEventListener("click", () => createBackup().catch((e) => showToast(e.message, true)));
  $("backup-refresh-btn")?.addEventListener("click", () => loadBackups().catch((e) => showToast(e.message, true)));
  $("backupFile")?.addEventListener("change", () => openUploadRestoreModal());

  $("editEmailBtn")?.addEventListener("click", () => openEmailSettings());
  $("testEmailBtn")?.addEventListener("click", () => testEmail());

  $("editIpCacheBtn")?.addEventListener("click", () => openIpCacheSettings());
  $("clearResultCache")?.addEventListener("click", () => clearIpCache());

  $("saveDedup")?.addEventListener("click", () => openDedupSettings());
  $("clearDedup")?.addEventListener("click", () => clearDedupCache());

  // 备份表事件委托
  $("backupBody")?.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const filename = btn.dataset.filename;
    const action = btn.dataset.action;
    if (action === "download-backup") downloadBackup(filename);
    else if (action === "restore-backup") openRestoreModal(filename);
    else if (action === "delete-backup") deleteBackup(filename);
  });
}

// ============ 鉴权 ============

function bindAuth() {
  $("auth-login-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      const ok = await submitLogin(state, showToast);
      if (ok) {
        await loadDashboard();
        initHashRouting();
      }
    } catch (err) {
      setAuthError(err.message);
    }
  });

  $("auth-logout-btn")?.addEventListener("click", async () => {
    confirmAsync("退出登录", "确认退出登录吗？", async () => {
      await performLogout(state, showToast);
    });
  });
}

// ============ 遮罩关闭 ============

function bindOverlayClosers() {
  $("modalMask")?.addEventListener("click", (e) => { if (e.target === $("modalMask")) closeModal(); });
  $("drawerClose")?.addEventListener("click", closeDrawer);
  $("drawerMask")?.addEventListener("click", closeDrawer);
}

// ============ 全局快捷键 ============

function bindGlobalShortcuts() {
  document.addEventListener("keydown", (e) => {
    if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
      e.preventDefault();
      const search = $("globalSearch");
      if (search) { search.focus(); search.select(); }
      return;
    }
    if (e.key === "Escape") {
      closeSelectPops();
      closeDrawer();
      const sidebar = $("sidebar");
      if (sidebar && sidebar.classList.contains("open")) sidebar.classList.remove("open");
    }
  });
}

// ============ ⌘K 全局检索 ============

const PAGE_INDEX = [
  { page: "overview", title: "系统概览", kw: "概览 首页 总览 仪表盘 dashboard overview" },
  { page: "routing", title: "路由配置", kw: "路由 规则 转发 前缀 重定向 routing rule" },
  { page: "security", title: "安全与封禁", kw: "安全 封禁 黑名单 白名单 解封 security ban" },
  { page: "geo", title: "IP 定位", kw: "定位 地理 离线库 mmdb 在线源 geo" },
  { page: "logs", title: "日志与审计", kw: "日志 审计 请求日志 应用日志 logs log" },
  { page: "system", title: "系统设置", kw: "系统 设置 备份 邮件 缓存 去重 恢复 system" },
];

const COMMAND_INDEX = [
  { title: "新建规则", sub: "创建一个转发规则", page: "routing", action: "new-rule" },
  { title: "新建路由组", sub: "创建一个路径前缀路由组", page: "routing", action: "new-group" },
  { title: "手动封禁 IP", sub: "封禁一个 IP 或网段", page: "security", action: "new-ban" },
  { title: "编辑自动封禁策略", sub: "配置自动封禁参数", page: "security", action: "auto-ban-settings" },
  { title: "在线定位源配置", sub: "编辑在线定位源", page: "geo", action: "geo-online-settings" },
  { title: "离线库配置", sub: "编辑离线 MMDB 配置", page: "geo", action: "geo-offline-settings" },
  { title: "清空定位缓存", sub: "清除在线定位结果缓存", page: "geo", action: "clear-geo-cache" },
  { title: "编辑邮件配置", sub: "配置 SMTP 邮件提醒", page: "system", action: "email-settings" },
  { title: "发送测试邮件", sub: "验证邮件提醒配置", page: "system", action: "test-email" },
  { title: "请求缓存配置", sub: "编辑请求结果缓存", page: "system", action: "ip-cache-settings" },
  { title: "请求去重配置", sub: "编辑请求去重参数", page: "system", action: "dedup-settings" },
  { title: "创建备份", sub: "生成一份数据快照", page: "system", action: "create-backup" },
];

function searchIndex(q) {
  q = (q || "").trim().toLowerCase();
  if (!q) return [];
  const results = [];
  const hit = (text) => String(text || "").toLowerCase().includes(q);

  // 页面导航
  PAGE_INDEX.forEach((p) => {
    if (hit(p.title) || hit(p.kw)) {
      results.push({ type: "页面", title: p.title, sub: "前往页面", page: p.page, action: "goto" });
    }
  });

  // 常用命令
  COMMAND_INDEX.forEach((c) => {
    if (hit(c.title) || hit(c.sub)) {
      results.push({ type: "命令", title: c.title, sub: c.sub, page: c.page, action: c.action });
    }
  });

  // 规则
  (state.rules || []).forEach((r) => {
    const hay = [r.name, r.path_prefix, r.target_url, r.notes].map((v) => String(v || "")).join(" ").toLowerCase();
    if (hay.includes(q)) {
      results.push({ type: "规则", title: r.name || r.path_prefix || "(未命名规则)", sub: `${r.path_prefix || ""} → ${r.target_url || ""}`, page: "routing", action: "filter-rule", filter: r.name || r.path_prefix || "" });
    }
  });

  // 路由组
  (state.routeGroups || []).forEach((g) => {
    const hay = [g.path_prefix, g.request_host].map((v) => String(v || "")).join(" ").toLowerCase();
    if (hay.includes(q)) {
      results.push({ type: "路由组", title: g.path_prefix, sub: formatRequestHostLabel(normalizeRequestHost(g.request_host)), page: "routing", action: "filter-rule", filter: g.path_prefix });
    }
  });

  // 封禁 IP
  (state.bannedIps || []).forEach((b) => {
    const hay = [b.ip, b.reason].map((v) => String(v || "")).join(" ").toLowerCase();
    if (hay.includes(q)) results.push({ type: "封禁IP", title: b.ip, sub: b.reason || "", page: "security", action: "goto" });
  });

  // 在线定位源
  (state.geoSources || []).forEach((s) => {
    const hay = [s.name, s.url].map((v) => String(v || "")).join(" ").toLowerCase();
    if (hay.includes(q)) results.push({ type: "定位源", title: s.name || s.url || "(未命名源)", sub: s.url || "", page: "geo", action: "goto" });
  });

  // 备份文件
  (state.backups || []).forEach((b) => {
    const name = b.filename || b.name || "";
    if (name.toLowerCase().includes(q)) results.push({ type: "备份", title: name, sub: "数据备份", page: "system", action: "goto" });
  });

  // 应用日志文件
  (state.logFiles || []).forEach((f) => {
    const name = f.name || f.filename || "";
    if (name.toLowerCase().includes(q)) results.push({ type: "日志文件", title: name, sub: "应用日志", page: "logs", action: "goto-app-log", appFile: name });
  });

  return results.slice(0, 12);
}

function highlightHtml(text, q) {
  const safe = escapeHtml(text == null ? "" : String(text));
  if (!q) return safe;
  const idx = safe.toLowerCase().indexOf(q.toLowerCase());
  if (idx < 0) return safe;
  return safe.slice(0, idx) + "<mark>" + safe.slice(idx, idx + q.length) + "</mark>" + safe.slice(idx + q.length);
}

function applySearchResult(r) {
  if (!r) return;
  if (r.action === "goto") { activatePage(r.page); return; }
  if (r.action === "filter-rule") {
    activatePage("routing");
    setValue("ruleSearch", r.filter);
    renderRules(state.rules);
    return;
  }
  if (r.action === "goto-app-log") {
    activatePage("logs");
    switchLogTab("app");
    state.appLogFile = r.appFile;
    loadAppLogContent().catch(() => {});
    return;
  }
  switch (r.action) {
    case "new-rule": activatePage("routing"); openRuleModal(null); break;
    case "new-group": activatePage("routing"); openRouteGroupModal(null); break;
    case "new-ban": activatePage("security"); openBanModal(); break;
    case "auto-ban-settings": activatePage("security"); openAutoBanSettings(); break;
    case "geo-online-settings": activatePage("geo"); openGeoOnlineSettings(); break;
    case "geo-offline-settings": activatePage("geo"); openGeoOfflineSettings(); break;
    case "clear-geo-cache":
      activatePage("geo");
      confirmAsync("清空定位缓存", "确认清空所有在线定位结果缓存吗？", () => clearGeoCache());
      break;
    case "email-settings": activatePage("system"); openEmailSettings(); break;
    case "test-email": activatePage("system"); testEmail(); break;
    case "ip-cache-settings": activatePage("system"); openIpCacheSettings(); break;
    case "dedup-settings": activatePage("system"); openDedupSettings(); break;
    case "create-backup": activatePage("system"); createBackup(); break;
  }
}

function bindSearch() {
  const input = $("globalSearch");
  const searchBox = input?.closest(".search");
  if (!input || !searchBox) return;

  const wrap = document.createElement("div");
  wrap.className = "search-results";
  searchBox.appendChild(wrap);

  let timer = null;
  let results = [];
  let activeIdx = -1;

  const setActive = (i) => {
    activeIdx = i;
    wrap.querySelectorAll(".search-result").forEach((el) => {
      el.classList.toggle("is-active", Number(el.dataset.idx) === i);
    });
    const activeEl = wrap.querySelector(".search-result.is-active");
    if (activeEl && activeEl.scrollIntoView) activeEl.scrollIntoView({ block: "nearest" });
  };

  const close = () => { wrap.classList.remove("open"); };

  const commit = (r) => {
    close();
    input.value = "";
    input.blur();
    applySearchResult(r);
  };

  const render = () => {
    const q = input.value.trim();
    results = searchIndex(q);
    activeIdx = -1;
    if (!q) { close(); wrap.innerHTML = ""; return; }
    if (!results.length) {
      wrap.innerHTML = `<div class="search-result-empty">未找到「${escapeHtml(q)}」的匹配项</div>`;
    } else {
      const typeClass = (t) => {
        if (t === "封禁IP") return "t-danger";
        if (t === "定位源" || t === "日志文件" || t === "备份") return "t-ok";
        return "";
      };
      wrap.innerHTML = results.map((r, i) => `
        <div class="search-result" data-idx="${i}">
          <span class="sr-type ${typeClass(r.type)}">${escapeHtml(r.type)}</span>
          <span class="sr-text">
            <span class="sr-title">${highlightHtml(r.title, q)}</span>
            ${r.sub ? `<span class="sr-sub">${highlightHtml(r.sub, q)}</span>` : ""}
          </span>
        </div>`).join("") +
        `<div class="search-results-footer"><span>↑↓ 选择 · Enter 打开 · Esc 关闭</span><span>${results.length} 项</span></div>`;
      wrap.querySelectorAll(".search-result").forEach((el) => {
        el.addEventListener("mouseenter", () => setActive(Number(el.dataset.idx)));
        el.addEventListener("click", () => commit(results[Number(el.dataset.idx)]));
      });
    }
    if (results.length) setActive(0);
    wrap.classList.add("open");
  };

  input.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(render, 150);
  });

  input.addEventListener("focus", render);
  input.addEventListener("keydown", (e) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      if (results.length) setActive((activeIdx + 1) % results.length);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (results.length) setActive((activeIdx - 1 + results.length) % results.length);
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (results.length && activeIdx >= 0) commit(results[activeIdx]);
      else {
        const q = input.value.trim();
        close();
        input.blur();
        if (q) { activatePage("routing"); setValue("ruleSearch", q); renderRules(state.rules); }
      }
    } else if (e.key === "Escape") {
      close();
    }
  });

  document.addEventListener("click", (e) => {
    if (!searchBox.contains(e.target)) close();
  });

  // 点击搜索框任意位置（图标、⌘K、空白）聚焦输入框并打开结果
  searchBox.addEventListener("click", (e) => {
    if (e.target === input) return;
    if (e.target.closest(".search-results")) return;
    e.preventDefault();
    input.focus();
    input.select();
    render();
  });
}

// ============ DOMContentLoaded 初始化 ============

window.addEventListener("DOMContentLoaded", async () => {
  initTheme();
  initFilterSelects();
  // 静态 select 全部升级为自定义下拉（美化展开面板 + 主题适配）
  document.querySelectorAll("select").forEach((s) => upgradeSelect(s));
  renderAllChips();

  // 恢复每页条数
  const savedLogPageSize = parseInt(localStorage.getItem("log_page_size") || "50", 10) || 50;
  state.logPageSize = savedLogPageSize;
  setValue("log_page_size", String(savedLogPageSize));
  const savedBanPageSize = parseInt(localStorage.getItem("ban_page_size") || "20", 10) || 20;
  state.banPageSize = savedBanPageSize;
  setValue("ban_page_size", String(savedBanPageSize));

  // 恢复自动刷新配置
  const savedAutoRefresh = getAutoRefreshConfig();
  setChecked("log_auto_refresh_enabled", savedAutoRefresh.enabled);
  setValue("log_auto_refresh_interval", String(savedAutoRefresh.interval));
  const savedBanRefresh = getBanAutoRefreshConfig();
  setChecked("ban_auto_refresh_enabled", savedBanRefresh.enabled);
  setValue("ban_auto_refresh_interval", String(savedBanRefresh.interval));

  // 绑定事件
  bindNavigation();
  bindTopbar();
  bindRouting();
  bindSecurity();
  bindGeo();
  bindLogs();
  bindSystem();
  bindAuth();
  bindOverlayClosers();
  bindGlobalShortcuts();
  bindSearch();

  // 初始化
  try {
    const auth = await loadAuthStatus();
    applyAuthState(auth || {}, state);
    if (!auth.enabled || auth.authenticated) {
      await loadDashboard();
      initHashRouting();
    } else {
      focusField("auth_username");
    }
  } catch (error) {
    showToast(error.message, true);
  }
});
