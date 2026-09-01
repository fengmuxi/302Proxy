/**
 * admin.js - 入口文件
 * 负责：DOMContentLoaded初始化、事件委托绑定、模块激活
 */

import { state } from './js/state.js';
import {
  setValue, setChecked, getValue, getChecked,
  normalizeRequestHost, formatRequestHostLabel,
  focusField,
} from './js/utils.js';
import { apiFetch, loadAuthStatus, submitLogin, performLogout } from './js/api.js';
import {
  els, openModal, closeModal,
  showToast, showUrlTooltip, hideUrlTooltip, copyToClipboard,
  setAuthError, applyAuthState,
  initTheme, applyTheme,
} from './js/components.js';
import {
  activateModule, initHashRouting,
  loadDashboard,
  renderRouteGroups,
  resetRouteGroupForm, openPrefixEditor,
  submitRouteGroup, updateGroupRegionSwitch,
  resetRuleForm,
  submitRule, openRuleEditor, removeRule, toggleRule, toggleRuleField,
  prepareRuleForGroup,
  renderGeoSources, resetGeoSourceForm, fillGeoSourceForm,
  collectGeoSourceForm, resetGeoSourceTestResult,
  renderGeoSourceTestResult, fillGeoSourceTestSelect,
  resetOfflineGeoTestResult, renderOfflineGeoTestResult,
  buildGeoSettingsPayload, persistGeoSettings, bindGeoNumericInputSafety,
  ensureRouteLogFilterFields,
  loadRouteLogs, refreshRouteLogModule,
  getAutoRefreshConfig, saveAutoRefreshConfig, startAutoRefresh, stopAutoRefresh,
  loadAppLogContent, startAppLogAutoRefresh, stopAppLogAutoRefresh,
  refreshAppLogModule,
  loadIpCacheSettings, loadIpCacheStats,
  loadAutoBanSettings, loadAutoBanStats,
  loadEmailSettings,
  loadBannedIpList, renderBannedIpListPage,
  openBanModal, toggleBanDurationLabel, openBanExtendModal, banIpFromLog,
  isValidIpOrCidr,
  loadBackups, createBackup, downloadBackup,
  openRestoreModal, confirmRestoreBackup, deleteBackup, uploadAndRestore,
  getBanAutoRefreshConfig, saveBanAutoRefreshConfig, startBanAutoRefresh, stopBanAutoRefresh,
  findRouteGroup,
} from './js/modules.js';

// ============ DOMContentLoaded 初始化 ============

window.addEventListener("DOMContentLoaded", async () => {
  initTheme();
  setActiveModule("overview");
  bindGeoNumericInputSafety();
  ensureRouteLogFilterFields();
  resetRouteGroupForm();
  resetGeoSourceForm();
  resetRuleForm();
  const savedAutoRefresh = getAutoRefreshConfig();
  setChecked("log_auto_refresh_enabled", savedAutoRefresh.enabled);
  setValue("log_auto_refresh_interval", String(savedAutoRefresh.interval));
  updateAutoRefreshStatusUI();

  const savedBanRefresh = getBanAutoRefreshConfig();
  setChecked("ban_auto_refresh_enabled", savedBanRefresh.enabled);
  setValue("ban_auto_refresh_interval", String(savedBanRefresh.interval));

  const savedLogPageSize = parseInt(localStorage.getItem("log_page_size") || "50", 10) || 50;
  state.logPageSize = savedLogPageSize;
  setValue("log_page_size", String(savedLogPageSize));

  const savedBanPageSize = parseInt(localStorage.getItem("ban_page_size") || "20", 10) || 20;
  state.banPageSize = savedBanPageSize;
  setValue("ban_page_size", String(savedBanPageSize));

  // 模块按钮事件
  document.querySelectorAll(".module-btn").forEach((button) => {
    button.addEventListener("click", () => {
      activateModule(button.dataset.moduleTarget);
    });
  });

  // 仪表板模块卡片
  document.querySelectorAll(".dash-module-card").forEach((card) => {
    card.addEventListener("click", () => {
      activateModule(card.dataset.moduleTarget);
    });
  });

  const statCardModuleMap = {
    "dash-stat-routes": "route-config",
    "dash-stat-rules": "route-config",
    "dash-stat-bans": "ip-ban-manager",
    "dash-stat-sources": "geoip-online",
    "dash-stat-logfiles": "app-logs",
    "dash-stat-backups": "backup-manager",
  };
  Object.entries(statCardModuleMap).forEach(([id, target]) => {
    const card = document.getElementById(id);
    if (card) {
      card.addEventListener("click", () => activateModule(target));
    }
  });

  // 主题选择
  document.querySelectorAll(".theme-dot").forEach((dot) => {
    dot.addEventListener("click", () => {
      applyTheme(dot.dataset.themeVal);
    });
  });

  // 新增按钮
  document.getElementById("add-prefix-btn").addEventListener("click", () => {
    resetRouteGroupForm();
    document.getElementById("route-group-form-title").textContent = "新增路径前缀";
    openModal("prefix-modal");
  });

  document.getElementById("add-geo-source-btn")?.addEventListener("click", () => {
    resetGeoSourceForm();
    document.getElementById("geo-source-form-title").textContent = "新增在线源";
    openModal("geo-source-modal");
  });

  document.getElementById("add-rule-btn").addEventListener("click", () => {
    resetRuleForm();
    document.getElementById("rule-form-title").textContent = "新增规则";
    openModal("rule-modal");
  });

  // 路由过滤
  const filterKeyword = document.getElementById("route_filter_keyword");
  const filterStatus = document.getElementById("route_filter_status");
  const filterDefault = document.getElementById("route_filter_default");
  const filterResetBtn = document.getElementById("route-filter-reset-btn");

  const applyRouteFilter = () => {
    state.routeFilter.keyword = filterKeyword ? filterKeyword.value : "";
    state.routeFilter.status = filterStatus ? filterStatus.value : "";
    state.routeFilter.isDefault = filterDefault ? filterDefault.value : "";
    renderRouteGroups(state.routeGroups);
  };

  if (filterKeyword) filterKeyword.addEventListener("input", applyRouteFilter);
  if (filterStatus) filterStatus.addEventListener("change", applyRouteFilter);
  if (filterDefault) filterDefault.addEventListener("change", applyRouteFilter);
  if (filterResetBtn) {
    filterResetBtn.addEventListener("click", () => {
      if (filterKeyword) filterKeyword.value = "";
      if (filterStatus) filterStatus.value = "";
      if (filterDefault) filterDefault.value = "";
      applyRouteFilter();
    });
  }

  // 弹框关闭按钮
  document.querySelectorAll("[data-close-modal]").forEach(btn => {
    btn.addEventListener("click", () => {
      closeModal(btn.dataset.closeModal);
    });
  });

  // 弹框背景点击关闭
  document.querySelectorAll(".modal-overlay").forEach(modal => {
    modal.addEventListener("click", (e) => {
      if (e.target === modal) {
        closeModal(modal.id);
      }
    });
  });

  // 表单提交
  document.getElementById("prefix-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    await submitRouteGroup();
    closeModal("prefix-modal");
  });

  document.getElementById("rule-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    await submitRule();
    closeModal("rule-modal");
  });

  // 路由组卡片事件委托
  document.getElementById("route-group-cards").addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) return;

    const action = button.dataset.action;
    const pathPrefix = button.dataset.pathPrefix;
    const requestHost = normalizeRequestHost(button.dataset.requestHost);
    const ruleId = button.dataset.id;

    if (action === "create-rule-for-group") {
      prepareRuleForGroup(pathPrefix, requestHost);
      return;
    }
    if (action === "edit-group") {
      openPrefixEditor(pathPrefix, requestHost);
      return;
    }
    if (action === "delete-group") {
      const group = findRouteGroup(pathPrefix, requestHost, state);
      if (!group) return;
      if (!window.confirm(`确认删除路径前缀 ${pathPrefix} @ ${formatRequestHostLabel(requestHost)} 吗？`)) return;
      try {
        await apiFetch("/_admin/api/route-groups", {
          method: "DELETE",
          body: JSON.stringify({ path_prefix: pathPrefix, request_host: requestHost }),
        });
        resetRouteGroupForm();
        await loadDashboard();
        showToast("路径前缀已删除。");
      } catch (error) {
        showToast(error.message, true);
      }
      return;
    }
    if (action === "edit-rule-from-group") {
      openRuleEditor(ruleId);
      return;
    }
    if (action === "toggle-rule-from-group") {
      const enabled = button.classList.contains("off");
      await toggleRule(ruleId, enabled);
      return;
    }
    if (action === "toggle-rule-field") {
      const field = button.dataset.field;
      const nextValue = button.classList.contains("off");
      await toggleRuleField(ruleId, field, nextValue);
      return;
    }
    if (action === "delete-rule-from-group") {
      await removeRule(ruleId);
    }
  });

  // 路由组地区开关
  document.getElementById("route-group-cards").addEventListener("change", async (event) => {
    const checkbox = event.target.closest('input[data-action="toggle-group-region"]');
    if (!checkbox) return;
    const pathPrefix = checkbox.dataset.pathPrefix;
    const requestHost = normalizeRequestHost(checkbox.dataset.requestHost);
    const nextValue = checkbox.checked;
    try {
      await updateGroupRegionSwitch(pathPrefix, requestHost, nextValue);
      await loadDashboard();
      showToast(`${pathPrefix} @ ${formatRequestHostLabel(requestHost)} 的地区匹配已${nextValue ? "开启" : "关闭"}。`);
    } catch (error) {
      checkbox.checked = !nextValue;
      showToast(error.message, true);
    }
  });

  // 日志过滤表单
  document.getElementById("route-log-filter-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    state.logCurrentPage = 1;
    try {
      await loadRouteLogs();
      showToast("日志查询已更新。");
    } catch (error) {
      showToast(error.message, true);
    }
  });

  document.getElementById("route-log-reset-btn").addEventListener("click", async () => {
    setValue("log_keyword", "");
    setValue("log_path_prefix", "");
    setValue("log_rule_request_host", "");
    setValue("log_match_strategy", "");
    setValue("log_result_status", "");
    setValue("log_date_from", "");
    setValue("log_date_to", "");
    setValue("log_limit", "50");
    state.logCurrentPage = 1;
    try {
      await loadRouteLogs();
    } catch (error) {
      showToast(error.message, true);
    }
  });

  document.getElementById("route-log-settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await apiFetch("/_admin/api/log-settings", {
        method: "PUT",
        body: JSON.stringify({ retention_days: Number(getValue("log_retention_days") || 30) }),
      });
      await refreshRouteLogModule();
      showToast("日志保留策略已保存。");
    } catch (error) {
      showToast(error.message, true);
    }
  });

  document.getElementById("log-cleanup-btn").addEventListener("click", async () => {
    try {
      const result = await apiFetch("/_admin/api/log-cleanup", { method: "POST" });
      showToast(`清理完成，删除了 ${result.deleted_count} 条过期日志记录。`);
      await refreshRouteLogModule();
    } catch (error) {
      showToast(error.message, true);
    }
  });

  // 日志列表事件委托
  document.getElementById("route-logs-list-body").addEventListener("mouseover", (event) => {
    const target = event.target.closest(".route-log-target-url");
    if (target) {
      const url = target.getAttribute("title") || target.textContent;
      showUrlTooltip(event, url);
    }
  });

  document.getElementById("route-logs-list-body").addEventListener("mouseout", (event) => {
    const target = event.target.closest(".route-log-target-url");
    if (target) hideUrlTooltip();
  });

  document.getElementById("route-logs-list-body").addEventListener("click", async (event) => {
    const target = event.target.closest(".route-log-target-url");
    if (target) {
      const url = target.getAttribute("title") || target.textContent;
      copyToClipboard(url);
      return;
    }
    const button = event.target.closest("button[data-action]");
    if (!button) return;
    const action = button.dataset.action;
    if (action === "delete-route-log") {
      const logId = Number(button.dataset.id);
      if (!window.confirm(`确认删除日志 #${logId} 吗？`)) return;
      try {
        await apiFetch("/_admin/api/logs", { method: "DELETE", body: JSON.stringify({ ids: [logId] }) });
        await refreshRouteLogModule();
        showToast("日志已删除。");
      } catch (error) {
        showToast(error.message, true);
      }
    } else if (action === "ban-ip-from-log") {
      const ip = button.dataset.ip;
      if (!ip || ip === "-") { showToast("该日志没有可封禁的IP地址", true); return; }
      try { await banIpFromLog(ip); } catch (error) { showToast(error.message, true); }
    } else if (action === "unban-ip-from-log") {
      const ip = button.dataset.ip;
      if (!ip || ip === "-") { showToast("该日志没有可解禁的IP地址", true); return; }
      if (!window.confirm(`确认解禁 IP ${ip} 吗？`)) return;
      try {
        await apiFetch(`/_admin/api/banned-ips/${encodeURIComponent(ip)}`, { method: "DELETE" });
        showToast(`IP ${ip} 已解禁`);
        await loadRouteLogs();
      } catch (error) { showToast(error.message, true); }
    }
  });

  // 日志全选/批量删除
  document.getElementById("route-log-select-all").addEventListener("change", (event) => {
    const checked = Boolean(event.target.checked);
    document.querySelectorAll(".route-log-checkbox").forEach((cb) => { cb.checked = checked; });
  });

  document.getElementById("route-log-delete-selected-btn").addEventListener("click", async () => {
    const ids = Array.from(document.querySelectorAll(".route-log-checkbox:checked"))
      .map((cb) => Number(cb.dataset.id))
      .filter((v) => Number.isInteger(v) && v > 0);
    if (!ids.length) { showToast("请先选择要删除的日志", true); return; }
    if (!window.confirm(`确认删除选中的 ${ids.length} 条日志吗？`)) return;
    try {
      await apiFetch("/_admin/api/logs", { method: "DELETE", body: JSON.stringify({ ids }) });
      await refreshRouteLogModule();
      showToast("选中日志已删除。");
    } catch (error) { showToast(error.message, true); }
  });

  document.getElementById("route-log-delete-all-btn").addEventListener("click", async () => {
    if (!window.confirm("确认清空所有规则转发日志吗？")) return;
    try {
      await apiFetch("/_admin/api/logs", { method: "DELETE", body: JSON.stringify({ delete_all: true }) });
      await refreshRouteLogModule();
      showToast("规则转发日志已清空。");
    } catch (error) { showToast(error.message, true); }
  });

  // 日志自动刷新
  document.getElementById("log_auto_refresh_enabled")?.addEventListener("change", () => {
    if (getChecked("log_auto_refresh_enabled")) startAutoRefresh();
    else { stopAutoRefresh(); saveAutoRefreshConfig({ enabled: false, interval: parseInt(getValue("log_auto_refresh_interval") || "5", 10) || 5 }); }
  });
  document.getElementById("log_auto_refresh_interval")?.addEventListener("change", () => {
    const interval = parseInt(getValue("log_auto_refresh_interval") || "5", 10) || 5;
    saveAutoRefreshConfig({ enabled: getChecked("log_auto_refresh_enabled"), interval });
    if (getChecked("log_auto_refresh_enabled")) startAutoRefresh();
  });
  document.getElementById("log_page_size")?.addEventListener("change", () => {
    const size = parseInt(getValue("log_page_size") || "50", 10) || 50;
    state.logPageSize = Math.max(1, size);
    state.logCurrentPage = 1;
    localStorage.setItem("log_page_size", String(state.logPageSize));
    loadRouteLogs().catch((error) => { showToast(error.message, true); });
  });

  // GeoIP 表单
  document.getElementById("geo-source-save-btn").addEventListener("click", async () => {
    const payload = collectGeoSourceForm();
    if (!payload.url) { showToast("在线源接口地址不能为空", true); return; }
    const button = document.getElementById("geo-source-save-btn");
    const originalText = button.textContent;
    const indexText = getValue("geo_source_id");
    const index = indexText === "" ? null : Number(indexText);
    const previousSources = state.geoSources.map((s) => ({ ...s }));
    const isEdit = index !== null && Number.isInteger(index) && index >= 0;
    if (isEdit) state.geoSources[index] = payload;
    else state.geoSources.push(payload);
    renderGeoSources();
    button.disabled = true;
    button.textContent = "保存中...";
    try {
      await persistGeoSettings(isEdit ? "在线源已更新。" : "在线源已新增。");
      closeModal("geo-source-modal");
      resetGeoSourceForm();
    } catch (error) {
      state.geoSources = previousSources;
      renderGeoSources();
      showToast(error.message, true);
    } finally {
      button.disabled = false;
      button.textContent = originalText;
    }
  });

  document.getElementById("geo-source-test-btn").addEventListener("click", () => {
    resetGeoSourceTestResult();
    setValue("geo_source_test_ip", "");
    fillGeoSourceTestSelect();
    openModal("geo-source-test-modal");
  });

  document.getElementById("geo-source-test-select")?.addEventListener("change", () => {
    resetGeoSourceTestResult();
  });

  document.getElementById("geo-source-test-run-btn").addEventListener("click", async () => {
    const ip = getValue("geo_source_test_ip").trim();
    const selectedIndex = getValue("geo_source_test_select");
    const button = document.getElementById("geo-source-test-run-btn");
    const originalText = button.textContent;
    if (!ip) { showToast("测试 IP 不能为空", true); return; }
    let source = null;
    if (selectedIndex !== "") source = state.geoSources[Number(selectedIndex)] || null;
    if (!source || !source.url) { showToast("请先选择一个有效的在线源", true); return; }
    button.disabled = true;
    button.textContent = "测试中...";
    resetGeoSourceTestResult("正在请求在线定位源，请稍候...");
    try {
      const result = await apiFetch("/_admin/api/geoip/test", { method: "POST", body: JSON.stringify({ ip, source }) });
      renderGeoSourceTestResult(result);
      showToast(result.success ? "在线源测试完成。" : "在线源测试失败。", !result.success);
    } catch (error) {
      renderGeoSourceTestResult({ success: false, stage: "online", provider: source.name || source.url, message: error.message, location: null });
      showToast(error.message, true);
    } finally {
      button.disabled = false;
      button.textContent = originalText;
    }
  });

  // GeoIP 在线源表格事件委托
  document.getElementById("geo-sources-table-body").addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) return;
    const index = Number(button.dataset.index);
    const action = button.dataset.action;
    const source = state.geoSources[index];
    if (!source) return;
    if (action === "edit-geo-source") { fillGeoSourceForm(source, index); openModal("geo-source-modal"); return; }
    if (action === "toggle-geo-source") {
      const previousSources = state.geoSources.map((item) => ({ ...item }));
      source.enabled = !source.enabled;
      renderGeoSources();
      try { await persistGeoSettings(source.enabled ? "在线源已启用。" : "在线源已禁用。"); }
      catch (error) { state.geoSources = previousSources; renderGeoSources(); showToast(error.message, true); }
      return;
    }
    if (action === "test-geo-source") { resetGeoSourceTestResult(); setValue("geo_source_test_ip", ""); fillGeoSourceTestSelect(index); openModal("geo-source-test-modal"); return; }
    if (action === "delete-geo-source") {
      if (!window.confirm(`确认删除在线源 ${source.name || source.url} 吗？`)) return;
      const previousSources = state.geoSources.map((item) => ({ ...item }));
      state.geoSources.splice(index, 1);
      renderGeoSources();
      try { await persistGeoSettings("在线源已删除"); resetGeoSourceForm(); }
      catch (error) { state.geoSources = previousSources; renderGeoSources(); showToast(error.message, true); }
    }
  });

  ["geoip-online-form", "geoip-offline-form"].forEach((formId) => {
    const form = document.getElementById(formId);
    if (!form) return;
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      try { await persistGeoSettings(formId === "geoip-online-form" ? "在线定位配置已保存。" : "离线定位配置已保存。"); }
      catch (error) { showToast(error.message, true); }
    });
  });

  document.getElementById("geo-online-cache-clear-btn").addEventListener("click", async () => {
    const button = document.getElementById("geo-online-cache-clear-btn");
    const originalText = button.textContent;
    button.disabled = true;
    button.textContent = "清理中...";
    try {
      const result = await apiFetch("/_admin/api/geoip/cache/clear", { method: "POST", body: JSON.stringify({}) });
      showToast(result.message || "在线定位缓存已清空。");
    } catch (error) { showToast(error.message, true); }
    finally { button.disabled = false; button.textContent = originalText; }
  });

  document.getElementById("geo-offline-sync-btn").addEventListener("click", async () => {
    const button = document.getElementById("geo-offline-sync-btn");
    const originalText = button.textContent;
    button.disabled = true;
    button.textContent = "同步中...";
    try {
      const result = await apiFetch("/_admin/api/geoip/offline/sync", { method: "POST", body: JSON.stringify({ geoip: buildGeoSettingsPayload() }) });
      await loadDashboard();
      showToast(result.message || "离线 GeoIP 同步完成。");
    } catch (error) { showToast(error.message, true); }
    finally { button.disabled = false; button.textContent = originalText; }
  });

  document.getElementById("geo-offline-rollback-btn").addEventListener("click", async () => {
    if (!window.confirm("确认回滚到离线 GeoIP 备份吗？")) return;
    const button = document.getElementById("geo-offline-rollback-btn");
    const originalText = button.textContent;
    button.disabled = true;
    button.textContent = "回滚中...";
    try {
      const result = await apiFetch("/_admin/api/geoip/offline/rollback", { method: "POST", body: JSON.stringify({}) });
      await loadDashboard();
      showToast(result.message || "离线 GeoIP 回滚完成。");
    } catch (error) { showToast(error.message, true); }
    finally { button.disabled = false; button.textContent = originalText; }
  });

  document.getElementById("geo-offline-test-btn").addEventListener("click", () => {
    resetOfflineGeoTestResult();
    setValue("geo_offline_test_ip", "");
    openModal("geo-offline-test-modal");
  });

  document.getElementById("geo-offline-test-run-btn").addEventListener("click", async () => {
    const ip = getValue("geo_offline_test_ip").trim();
    const button = document.getElementById("geo-offline-test-run-btn");
    const originalText = button.textContent;
    if (!ip) { showToast("离线库测试 IP 不能为空", true); return; }
    button.disabled = true;
    button.textContent = "测试中...";
    resetOfflineGeoTestResult("正在使用离线库进行定位，请稍候...");
    try {
      const result = await apiFetch("/_admin/api/geoip/offline/test", { method: "POST", body: JSON.stringify({ ip, geoip: buildGeoSettingsPayload() }) });
      renderOfflineGeoTestResult(result);
      showToast(result.success ? "离线定位测试完成。" : "离线定位测试失败。", !result.success);
    } catch (error) {
      renderOfflineGeoTestResult({ success: false, provider: "offline_mmdb", message: error.message, location: null });
      showToast(error.message, true);
    } finally { button.disabled = false; button.textContent = originalText; }
  });

  // IP 缓存
  document.getElementById("ip-cache-settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await apiFetch("/_admin/api/ip-cache-settings", {
        method: "PUT",
        body: JSON.stringify({
          enabled: getValue("ip_cache_enabled") === "1",
          ttl_seconds: Number(getValue("ip_cache_ttl_seconds") || 300),
          max_entries: Number(getValue("ip_cache_max_entries") || 5000),
        }),
      });
      await Promise.all([loadIpCacheSettings(), loadIpCacheStats()]);
      showToast("请求结果缓存配置已保存。");
    } catch (error) { showToast(error.message, true); }
  });

  document.getElementById("clear-ip-cache-btn")?.addEventListener("click", async () => {
    if (!window.confirm("确认清空所有请求结果缓存吗？")) return;
    try {
      const data = await apiFetch("/_admin/api/ip-cache/clear", { method: "POST" });
      showToast(data.message || "缓存已清空");
      loadIpCacheStats();
    } catch (error) { showToast(error.message, true); }
  });

  // 自动封禁
  document.getElementById("auto-ban-settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await apiFetch("/_admin/api/auto-ban", {
        method: "PUT",
        body: JSON.stringify({
          enabled: getValue("auto_ban_enabled") === "1",
          window_seconds: Number(getValue("auto_ban_window_seconds") || 60),
          max_requests: Number(getValue("auto_ban_max_requests") || 100),
          ban_duration_seconds: Number(getValue("auto_ban_ban_duration_seconds") || 3600),
          max_404: Number(getValue("auto_ban_max_404") || 20),
          auto_ban_on_404: getValue("auto_ban_auto_ban_on_404") === "1",
          whitelist: getValue("auto_ban_whitelist") || "",
          email_on_ban: getValue("auto_ban_email_on_ban") === "1",
        }),
      });
      await Promise.all([loadAutoBanSettings(), loadAutoBanStats()]);
      closeModal("auto-ban-modal");
      showToast("自动封禁配置已保存。");
    } catch (error) { showToast(error.message, true); }
  });

  document.getElementById("open-auto-ban-modal-btn").addEventListener("click", async () => {
    await loadAutoBanSettings();
    openModal("auto-ban-modal");
  });

  // 邮件配置
  document.getElementById("email-settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const password = getValue("email_password") || "";
      const payload = {
        enabled: getValue("email_enabled") === "1",
        smtp_host: getValue("email_smtp_host") || "",
        smtp_port: Number(getValue("email_smtp_port") || 465),
        smtp_ssl: getValue("email_smtp_ssl") === "1",
        sender: getValue("email_sender") || "",
        sender_name: getValue("email_sender_name") || "",
        recipients: getValue("email_recipients") || "",
        block_link_base_url: getValue("email_block_link_base_url") || "",
        alert_window_seconds: Number(getValue("email_alert_window_seconds") || 60),
        alert_max_requests: Number(getValue("email_alert_max_requests") || 80),
        alert_max_404: Number(getValue("email_alert_max_404") || 15),
        alert_cooldown_minutes: Number(getValue("email_alert_cooldown_minutes") || 30),
      };
      if (password) payload.password = password;
      await apiFetch("/_admin/api/email", { method: "PUT", body: JSON.stringify(payload) });
      await loadEmailSettings();
      showToast("邮件提醒配置已保存。");
    } catch (error) { showToast(error.message, true); }
  });

  document.getElementById("test-email-btn").addEventListener("click", async () => {
    const btn = document.getElementById("test-email-btn");
    btn.disabled = true;
    btn.textContent = "发送中...";
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
          template_type: getValue("email_test_template_type") || "alert",
        }),
      });
      showToast(result.message, !result.success);
    } catch (error) { showToast(error.message, true); }
    finally { btn.disabled = false; btn.textContent = "发送测试邮件"; }
  });

  // 封禁管理
  document.getElementById("ban-ip-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const ip = getValue("ban_ip_address").trim();
    if (!ip) { showToast("IP地址不能为空", true); return; }
    if (!isValidIpOrCidr(ip)) { showToast("IP格式无效，请输入单个IP（如 1.2.3.4）或 CIDR 网段（如 192.168.1.0/24）", true); return; }
    const pathPrefix = getValue("ban_ip_path_prefix").trim();
    const reason = getValue("ban_ip_reason").trim();
    const selectEl = document.getElementById("ban_ip_permanent");
    const permanent = selectEl ? selectEl.value === "1" : true;
    const durationHours = parseFloat(getValue("ban_ip_duration") || "0") || 0;
    if (!permanent && durationHours <= 0) { showToast("临时封禁时长必须大于0", true); return; }
    const durationSeconds = permanent ? 0 : Math.max(60, Math.round(durationHours * 3600));
    try {
      await apiFetch("/_admin/api/banned-ips", {
        method: "POST",
        body: JSON.stringify({ ip, reason: reason || "", banned_by: "admin", permanent, duration_seconds: durationSeconds, path_prefix: pathPrefix }),
      });
      const scopeText = pathPrefix ? `路径前缀 ${pathPrefix}` : "全局";
      showToast(`${ip.includes("/") ? "IP段" : "IP"} ${ip} 已封禁（${scopeText}）`);
      closeModal("ban-ip-modal");
      loadBannedIpList();
      if (getValue("ban_ip_mode") === "from-log") loadRouteLogs();
    } catch (error) { showToast(error.message, true); }
  });

  document.getElementById("ban-extend-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const ip = getValue("ban_extend_ip").trim();
    const durationHours = parseFloat(getValue("ban_extend_duration") || "0") || 0;
    if (!ip) { showToast("IP地址不能为空", true); return; }
    if (durationHours <= 0) { showToast("延长时长必须大于0", true); return; }
    try {
      await apiFetch(`/_admin/api/banned-ips/${encodeURIComponent(ip)}/extend`, {
        method: "POST", body: JSON.stringify({ duration_hours: durationHours }),
      });
      showToast(`IP ${ip} 封禁时间已延长 ${durationHours} 小时`);
      closeModal("ban-extend-modal");
      loadBannedIpList();
    } catch (error) { showToast(error.message, true); }
  });

  document.getElementById("banned-ips-table-body")?.addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) return;
    const action = button.dataset.action;
    const ip = button.dataset.ip;
    if (action === "unban-ip") {
      if (!window.confirm(`确认解封 IP ${ip} 吗？`)) return;
      try {
        await apiFetch(`/_admin/api/banned-ips/${encodeURIComponent(ip)}`, { method: "DELETE" });
        showToast(`IP ${ip} 已解封`);
        loadBannedIpList();
      } catch (error) { showToast(error.message, true); }
    } else if (action === "extend-ban-ip") {
      const expireAt = parseFloat(button.dataset.expire || "0") || 0;
      openBanExtendModal(ip, expireAt);
    }
  });

  document.getElementById("add-ban-btn")?.addEventListener("click", () => { openBanModal({ mode: "add" }); });

  document.getElementById("clear-bans-btn")?.addEventListener("click", async () => {
    if (!window.confirm("确认清空所有封禁记录吗？此操作不可恢复！")) return;
    try {
      await apiFetch("/_admin/api/banned-ips/clear", { method: "POST" });
      showToast("所有封禁记录已清空");
      state.banCurrentPage = 1;
      loadBannedIpList();
    } catch (error) { showToast(error.message, true); }
  });

  document.getElementById("ban_ip_permanent")?.addEventListener("change", toggleBanDurationLabel);

  document.getElementById("ban_auto_refresh_enabled")?.addEventListener("change", () => {
    if (getChecked("ban_auto_refresh_enabled")) startBanAutoRefresh();
    else { stopBanAutoRefresh(); saveBanAutoRefreshConfig({ enabled: false, interval: parseInt(getValue("ban_auto_refresh_interval") || "5", 10) || 5 }); }
  });
  document.getElementById("ban_auto_refresh_interval")?.addEventListener("change", () => {
    const interval = parseInt(getValue("ban_auto_refresh_interval") || "5", 10) || 5;
    saveBanAutoRefreshConfig({ enabled: getChecked("ban_auto_refresh_enabled"), interval });
    if (getChecked("ban_auto_refresh_enabled")) startBanAutoRefresh();
  });
  document.getElementById("ban_page_size")?.addEventListener("change", () => {
    const size = parseInt(getValue("ban_page_size") || "20", 10) || 20;
    state.banPageSize = Math.max(1, size);
    state.banCurrentPage = 1;
    localStorage.setItem("ban_page_size", String(state.banPageSize));
    renderBannedIpListPage();
  });

  // 应用日志
  document.getElementById("app-log-cleanup-btn").addEventListener("click", async () => {
    try {
      const result = await apiFetch("/_admin/api/log-file-cleanup", { method: "POST" });
      showToast(`清理完成，删除了 ${result.deleted_count} 个过期日志文件。`);
      await refreshAppLogModule();
    } catch (error) { showToast(error.message, true); }
  });
  document.getElementById("app-log-file-select")?.addEventListener("change", (e) => { state.appLogFile = e.target.value; loadAppLogContent(); });
  document.getElementById("app-log-refresh-btn").addEventListener("click", () => { refreshAppLogModule().catch((error) => { showToast(error.message, true); }); });
  document.getElementById("app-log-search-btn").addEventListener("click", () => { loadAppLogContent().catch((error) => { showToast(error.message, true); }); });
  document.getElementById("app-log-tail-lines").addEventListener("change", () => { loadAppLogContent().catch((error) => { showToast(error.message, true); }); });
  document.getElementById("app-log-auto-refresh").addEventListener("change", () => {
    if (getChecked("app-log-auto-refresh")) startAppLogAutoRefresh();
    else stopAppLogAutoRefresh();
  });
  document.getElementById("app-log-keyword").addEventListener("keydown", (e) => {
    if (e.key === "Enter") loadAppLogContent().catch((error) => { showToast(error.message, true); });
  });

  // 备份管理
  document.getElementById("backup-table-body")?.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const action = btn.dataset.action;
    const filename = btn.dataset.filename;
    if (action === "download-backup") downloadBackup(filename);
    else if (action === "restore-backup") openRestoreModal(filename);
    else if (action === "delete-backup") deleteBackup(filename);
  });
  document.getElementById("backup-create-btn")?.addEventListener("click", createBackup);
  document.getElementById("backup-refresh-btn")?.addEventListener("click", loadBackups);
  document.getElementById("backup-restore-confirm-btn")?.addEventListener("click", confirmRestoreBackup);
  document.getElementById("backup-upload-form")?.addEventListener("submit", (e) => { e.preventDefault(); uploadAndRestore(); });
  document.getElementById("backup_restore_mode")?.addEventListener("change", (e) => {
    const hint = document.getElementById("backup-mode-hint");
    if (hint) hint.textContent = e.target.value === "overwrite" ? "覆盖模式：用上传的数据库文件完全替换当前数据库。" : "合并模式：仅导入上传文件中的新规则，跳过已存在的规则。";
  });
  document.getElementById("restore_mode")?.addEventListener("change", (e) => {
    const hint = document.getElementById("restore-mode-hint");
    if (hint) hint.textContent = e.target.value === "overwrite" ? "覆盖模式会替换当前数据库中的所有配置和数据。" : "合并模式会导入所有配置表（系统设置、路由规则、GeoIP 等），跳过已存在的条目，运行日志和封禁列表不会被导入。";
  });

  // 登录
  document.getElementById("auth-login-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await submitLogin(state, showToast);
      await loadDashboard();
      showToast("后台登录成功");
    } catch (error) {
      setAuthError(error.message);
    }
  });

  els.authLogoutBtn?.addEventListener("click", async () => {
    if (!window.confirm("确认退出登录吗？")) return;
    try { await performLogout(state, showToast); setAuthError("已退出登录。"); showToast("已退出登录。"); }
    catch (error) { showToast(error.message, true); }
  });

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

// ============ 辅助函数（供入口文件内部使用） ============

function setActiveModule(moduleName) {
  state.activeModule = moduleName;
  document.querySelectorAll(".module-btn").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.moduleTarget === moduleName);
  });
  document.querySelectorAll(".module-panel").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.modulePanel === moduleName);
  });
  try {
    const url = new URL(window.location.href);
    url.hash = moduleName;
    window.history.replaceState(null, "", url.toString());
  } catch (_) {}
}

function updateAutoRefreshStatusUI() {
  const el = document.getElementById("log_auto_refresh_status");
  if (!el) return;
  el.textContent = "";
  el.className = "auto-refresh-status stopped";
}
