/**
 * modules.js - 业务逻辑模块
 * 包含：路由管理、规则管理、GeoIP、封禁、日志、备份等所有业务功能
 */

import { state } from './state.js';
import {
  setValue, setChecked, getValue, getChecked,
  getNonNegativeIntValue, getPositiveIntValue,
  setText, focusField, scrollToElement,
  escapeHtml, formatDateTime, formatRemainTime, formatBytes,
  normalizeRequestHost, formatRequestHostLabel,
  isSameRouteGroup, findRouteGroup, toIsoDateTime,
  formatTypeLabel, formatMatchStrategy, formatResultStatus,
  formatCacheStatus, formatMatchDetail, formatRouteLogRequestHost,
  formatRouteLogRuleRequestHost,
} from './utils.js';
import { apiFetch } from './api.js';
import {
  els, openModal, closeModal,
  showToast,
  renderDashboardMetrics, renderPagination, renderSummary,
} from './components.js';

// ============ 模块激活 ============

export function activateModule(target) {
  if (!target) return;
  setActiveModule(target);
  if (target === "logs") {
    refreshRouteLogModule().catch((error) => {
      showToast(error.message, true);
    });
    if (getChecked("log_auto_refresh_enabled")) {
      startAutoRefresh();
    }
  } else {
    stopAutoRefresh();
  }
  if (target === "app-logs") {
    refreshAppLogModule().catch((error) => {
      showToast(error.message, true);
    });
    if (getChecked("app-log-auto-refresh")) {
      startAppLogAutoRefresh();
    }
  } else {
    stopAppLogAutoRefresh();
  }
  if (target !== "ip-ban-manager") {
    stopBanAutoRefresh();
  }
  if (target === "ip-cache-manager") {
    loadIpCacheSettings();
    loadIpCacheStats();
  }
  if (target === "ip-ban-manager") {
    loadBannedIpList();
    loadAutoBanSettings();
    loadAutoBanStats();
    if (getChecked("ban_auto_refresh_enabled")) {
      startBanAutoRefresh();
    }
  }
  if (target === "email-manager") {
    loadEmailSettings();
  }
  if (target === "backup-manager") {
    loadBackups();
  }
  if (target === "overview") {
    renderDashboardMetrics(state, false);
  }
}

export function setActiveModule(moduleName) {
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

let _hashRoutingInitialized = false;

export function initHashRouting() {
  const VALID_MODULES = ["overview", "route-config", "logs", "app-logs", "geoip-online", "geoip-offline", "ip-ban-manager", "ip-cache-manager", "backup-manager"];
  const hash = window.location.hash.replace(/^#\/?/, "");
  if (hash && VALID_MODULES.includes(hash)) {
    activateModule(hash);
  }
  if (!_hashRoutingInitialized) {
    _hashRoutingInitialized = true;
    window.addEventListener("hashchange", () => {
      const newHash = window.location.hash.replace(/^#\/?/, "");
      if (newHash && VALID_MODULES.includes(newHash) && newHash !== state.activeModule) {
        activateModule(newHash);
      }
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
  renderSummary(data.summary || {});
  renderRules(data.rules || []);
  renderRouteGroups(data.route_groups || []);
  fillGeoConfig(data.geoip || {});
  renderDashboardMetrics(state);
}

// ============ 路由组管理 ============

export function getRulesForGroup(pathPrefix, requestHost = "") {
  const normalizedHost = normalizeRequestHost(requestHost);
  return state.rules
    .filter(
      (rule) =>
        rule.path_prefix === pathPrefix &&
        normalizeRequestHost(rule.request_host) === normalizedHost,
    )
    .sort((left, right) => {
      if (Number(right.is_default) !== Number(left.is_default)) {
        return Number(right.is_default) - Number(left.is_default);
      }
      if (right.priority !== left.priority) {
        return right.priority - left.priority;
      }
      return (left.id || 0) - (right.id || 0);
    });
}

export function renderRouteGroupOptions() {
  els.routeGroupOptions.innerHTML = "";
  state.routeGroups.forEach((group) => {
    const option = document.createElement("option");
    option.value = group.path_prefix;
    option.label = `${group.path_prefix} [域名: ${formatRequestHostLabel(
      normalizeRequestHost(group.request_host),
    )}]`;
    els.routeGroupOptions.appendChild(option);
  });
}

export function matchRouteFilter(keyword, status, isDefault, group, rules) {
  const kw = String(keyword || "").trim().toLowerCase();
  if (kw) {
    const groupHost = normalizeRequestHost(group.request_host);
    const hostLabel = formatRequestHostLabel(groupHost).toLowerCase();
    const groupNotes = String(group.notes || "").toLowerCase();
    const prefixMatch = String(group.path_prefix || "").toLowerCase().includes(kw);
    const hostMatch = hostLabel.includes(kw);
    const groupNotesMatch = groupNotes.includes(kw);
    const ruleMatch = rules.some((rule) => {
      return [
        rule.name, rule.target_url, rule.ip_whitelist, rule.region_filters, rule.notes,
        rule.request_host,
      ].some((field) => String(field || "").toLowerCase().includes(kw));
    });
    if (!(prefixMatch || hostMatch || groupNotesMatch || ruleMatch)) {
      return false;
    }
  }

  if (status === "enabled" && !rules.some((r) => r.enabled)) return false;
  if (status === "disabled" && !rules.some((r) => !r.enabled)) return false;

  if (isDefault === "yes" && !rules.some((r) => r.is_default)) return false;
  if (isDefault === "no" && !rules.some((r) => !r.is_default)) return false;

  return true;
}

export function renderRouteGroups(routeGroups) {
  state.routeGroups = routeGroups;
  renderRouteGroupOptions();

  const container = document.getElementById("route-group-cards");
  container.innerHTML = "";

  const { keyword, status, isDefault } = state.routeFilter;
  const filteredGroups = routeGroups.map((group) => {
    const normalizedGroupHost = normalizeRequestHost(group.request_host);
    const groupRules = getRulesForGroup(group.path_prefix, normalizedGroupHost);
    return { group, groupRules };
  }).filter(({ group, groupRules }) => matchRouteFilter(keyword, status, isDefault, group, groupRules));

  const summaryEl = document.getElementById("route-filter-summary");
  if (summaryEl) {
    const totalGroups = routeGroups.length;
    const totalRules = routeGroups.reduce((acc, g) => {
      const normalizedGroupHost = normalizeRequestHost(g.request_host);
      return acc + getRulesForGroup(g.path_prefix, normalizedGroupHost).length;
    }, 0);
    const filteredRules = filteredGroups.reduce((acc, item) => acc + item.groupRules.length, 0);
    const hasFilter = Boolean(keyword || status || isDefault);
    if (hasFilter) {
      summaryEl.textContent = `共 ${totalGroups} 个前缀 / ${totalRules} 条规则，当前匹配 ${filteredGroups.length} 个前缀 / ${filteredRules} 条规则`;
    } else {
      summaryEl.textContent = `共 ${totalGroups} 个前缀 / ${totalRules} 条规则`;
    }
  }

  if (!routeGroups.length) {
    container.innerHTML = `
      <div class="empty-group-state">
        <p>还没有路径前缀。先在右侧新增一个前缀，再为它添加转发规则。</p>
      </div>
    `;
    return;
  }

  if (!filteredGroups.length) {
    container.innerHTML = `
      <div class="empty-group-state">
        <p>没有匹配当前查询条件的前缀或规则。</p>
      </div>
    `;
    return;
  }

  filteredGroups.forEach(({ group, groupRules }) => {
    const normalizedGroupHost = normalizeRequestHost(group.request_host);
    const hostLabel = formatRequestHostLabel(normalizedGroupHost);
    const statChips = `
      <div class="prefix-card-stats">
        <span class="stat-chip">规则 ${group.rule_count ?? groupRules.length}</span>
        <span class="stat-chip">默认 ${group.default_rule_count ?? groupRules.filter((rule) => rule.is_default).length}</span>
        <span class="stat-chip">启用 ${group.enabled_rule_count ?? groupRules.filter((rule) => rule.enabled).length}</span>
      </div>
    `;

    const rulesMarkup = groupRules.length
      ? `
        <div class="rule-list-wrap prefix-card-rules">
          <table class="rules-table compact-table">
            <thead>
              <tr>
                <th>规则</th>
                <th>目标地址</th>
                <th>请求域名</th>
                <th>白名单IP</th>
                <th>地区过滤</th>
                <th>优先级</th>
                <th>超时(秒)</th>
                <th>备注</th>
                <th>正则匹配</th>
                <th>状态</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              ${groupRules
                .map((rule) => {
                  const statusBadges = [
                    rule.is_default ? '<span class="badge badge-default">默认</span>' : "",
                  ].join("");
                  const requestHost = normalizeRequestHost(rule.request_host);
                  const hostLabel = formatRequestHostLabel(requestHost);
                  const ipWhitelist = rule.ip_whitelist || "";
                  const regionFilters = rule.region_filters || "";
                  const notesText = rule.notes || "";
                  const rewritePattern = rule.path_rewrite_pattern || "";
                  const rewriteReplacement = rule.path_rewrite_replacement || "";
                  const hasRewrite = Boolean(rewritePattern);
                  const rewriteTitle = hasRewrite
                    ? `模式: ${rewritePattern}\n替换: ${rewriteReplacement || "(空)"}`
                    : "";
                  const rewriteCell = hasRewrite
                    ? `<div class="rewrite-cell"><code class="rewrite-pattern" title="${escapeHtml(rewritePattern)}">${escapeHtml(rewritePattern)}</code><code class="rewrite-replacement" title="${escapeHtml(rewriteReplacement)}">→ ${escapeHtml(rewriteReplacement || "(空)")}</code></div>`
                    : `<span class="text-muted">-</span>`;
                  return `
                    <tr>
                      <td data-label="规则">
                        <strong>${escapeHtml(rule.name || "(未命名规则)")}</strong>
                        <div>${statusBadges}</div>
                      </td>
                      <td data-label="目标地址" class="cell-truncate" title="${escapeHtml(rule.target_url)}">${escapeHtml(rule.target_url)}</td>
                      <td data-label="请求域名" class="cell-truncate" title="${escapeHtml(hostLabel)}">${escapeHtml(hostLabel)}</td>
                      <td data-label="白名单IP" class="cell-truncate" title="${escapeHtml(ipWhitelist)}">${ipWhitelist ? escapeHtml(ipWhitelist) : "-"}</td>
                      <td data-label="地区过滤" class="cell-truncate" title="${escapeHtml(regionFilters)}">${regionFilters ? escapeHtml(regionFilters) : "默认"}</td>
                      <td data-label="优先级">${rule.priority ?? 0}</td>
                      <td data-label="超时(秒)">${rule.timeout ?? 30}</td>
                      <td data-label="备注" class="cell-notes" title="${escapeHtml(notesText)}">${notesText ? escapeHtml(notesText) : "-"}</td>
                      <td data-label="正则匹配" class="cell-rewrite" title="${escapeHtml(rewriteTitle)}">${rewriteCell}</td>
                      <td data-label="状态">
                        <div class="rule-status-tags">
                          <button class="toggle-status-btn ${rule.enabled ? "on" : "off"}" data-action="toggle-rule-from-group" data-id="${rule.id}" type="button" title="${rule.enabled ? "点击禁用" : "点击启用"}">
                            <span class="toggle-status-dot"></span>
                            <span class="toggle-status-text">${rule.enabled ? "启用" : "禁用"}</span>
                          </button>
                          <button class="toggle-tag ${rule.strip_prefix ? "on" : "off"}" data-action="toggle-rule-field" data-field="strip_prefix" data-id="${rule.id}" type="button" title="${rule.strip_prefix ? "点击关闭去前缀" : "点击开启去前缀"}">
                            <span class="toggle-tag-dot"></span>去前缀
                          </button>
                          <button class="toggle-tag ${rule.follow_redirects !== false ? "on" : "off"}" data-action="toggle-rule-field" data-field="follow_redirects" data-id="${rule.id}" type="button" title="${rule.follow_redirects !== false ? "点击关闭跟随重定向" : "点击开启跟随重定向"}">
                            <span class="toggle-tag-dot"></span>跟随重定向
                          </button>
                          <button class="toggle-tag ${rule.enable_streaming ? "on" : "off"}" data-action="toggle-rule-field" data-field="enable_streaming" data-id="${rule.id}" type="button" title="${rule.enable_streaming ? "点击关闭流式转发" : "点击开启流式转发"}">
                            <span class="toggle-tag-dot"></span>流式转发
                          </button>
                        </div>
                      </td>
                      <td data-label="操作">
                        <div class="table-actions">
                          <button class="table-btn" data-action="edit-rule-from-group" data-id="${rule.id}" type="button">编辑规则</button>
                          <button class="table-btn delete" data-action="delete-rule-from-group" data-id="${rule.id}" type="button">删除规则</button>
                        </div>
                      </td>
                    </tr>
                  `;
                })
                .join("")}
            </tbody>
          </table>
        </div>
      `
      : `
        <div class="empty-group-state inline-empty-state">
          <p>这个前缀下还没有转发规则。</p>
        </div>
      `;

    const card = document.createElement("article");
    card.className = "prefix-card";
    const groupAccessChips = [];
    if (group.access_ip_whitelist) {
      groupAccessChips.push(`<span class="access-chip access-chip-ip-whitelist" title="访问控制 IP白名单: ${escapeHtml(group.access_ip_whitelist)}">IP白: ${escapeHtml(group.access_ip_whitelist)}</span>`);
    }
    if (group.ip_blacklist) {
      groupAccessChips.push(`<span class="access-chip access-chip-ip-blacklist" title="访问控制 IP黑名单: ${escapeHtml(group.ip_blacklist)}">IP黑: ${escapeHtml(group.ip_blacklist)}</span>`);
    }
    if (group.region_whitelist) {
      groupAccessChips.push(`<span class="access-chip access-chip-region-whitelist" title="访问控制 地区白名单: ${escapeHtml(group.region_whitelist)}">地区白: ${escapeHtml(group.region_whitelist)}</span>`);
    }
    if (group.region_blacklist) {
      groupAccessChips.push(`<span class="access-chip access-chip-region-blacklist" title="访问控制 地区黑名单: ${escapeHtml(group.region_blacklist)}">地区黑: ${escapeHtml(group.region_blacklist)}</span>`);
    }
    const groupAccessHtml = groupAccessChips.length ? `<p class="hint">${groupAccessChips.join(" ")}</p>` : "";
    card.innerHTML = `
      <div class="prefix-card-head">
        <div class="prefix-card-title">
          <h3>${escapeHtml(group.path_prefix)}</h3>
          <p class="hint">域名: <code>${escapeHtml(hostLabel)}</code></p>
          <p class="hint">${escapeHtml(group.notes || "未填写备注")}</p>
          ${groupAccessHtml}
          ${statChips}
        </div>
        <div class="table-actions">
          <button class="table-btn" data-action="create-rule-for-group" data-path-prefix="${escapeHtml(group.path_prefix)}" data-request-host="${escapeHtml(normalizedGroupHost)}" type="button">新增规则</button>
          <button class="table-btn" data-action="edit-group" data-path-prefix="${escapeHtml(group.path_prefix)}" data-request-host="${escapeHtml(normalizedGroupHost)}" type="button">编辑前缀</button>
          <button class="table-btn delete" data-action="delete-group" data-path-prefix="${escapeHtml(group.path_prefix)}" data-request-host="${escapeHtml(normalizedGroupHost)}" type="button">删除前缀</button>
        </div>
      </div>
      <div class="prefix-card-toggle">
        <div>
          <strong>地区匹配开关</strong>
          <p class="hint">这里控制 ${escapeHtml(group.path_prefix)} 这个前缀下所有规则是否按地区过滤命中。</p>
        </div>
        <label class="switch-inline">
          <input
            data-action="toggle-group-region" data-path-prefix="${escapeHtml(group.path_prefix)}" data-request-host="${escapeHtml(normalizedGroupHost)}"
            type="checkbox"
            ${group.region_matching_enabled ? "checked" : ""}
          />
          <span>${group.region_matching_enabled ? "已开启" : "已关闭"}</span>
        </label>
      </div>
      ${rulesMarkup}
      <div class="prefix-card-footer">
        <button class="ghost-btn" data-action="create-rule-for-group" data-path-prefix="${escapeHtml(group.path_prefix)}" data-request-host="${escapeHtml(normalizedGroupHost)}" type="button">给这个前缀新增转发规则</button>
      </div>
    `;
    container.appendChild(card);
  });
}

export function resetRouteGroupForm() {
  setValue("route_group_old_path_prefix", "");
  setValue("route_group_old_request_host", "");
  setValue("route_group_path_prefix", "");
  setValue("route_group_request_host", "");
  setValue("route_group_access_ip_whitelist", "");
  setValue("route_group_ip_blacklist", "");
  setValue("route_group_region_whitelist", "");
  setValue("route_group_region_blacklist", "");
  setValue("route_group_notes", "");
  document.getElementById("route-group-form-title").textContent = "新增路径前缀";
}

export function fillRouteGroupForm(group) {
  setValue("route_group_old_path_prefix", group.path_prefix);
  setValue("route_group_old_request_host", normalizeRequestHost(group.request_host));
  setValue("route_group_path_prefix", group.path_prefix);
  setValue("route_group_request_host", normalizeRequestHost(group.request_host));
  setValue("route_group_access_ip_whitelist", group.access_ip_whitelist || "");
  setValue("route_group_ip_blacklist", group.ip_blacklist || "");
  setValue("route_group_region_whitelist", group.region_whitelist || "");
  setValue("route_group_region_blacklist", group.region_blacklist || "");
  setValue("route_group_notes", group.notes || "");
  const hostLabel = formatRequestHostLabel(normalizeRequestHost(group.request_host));
  document.getElementById("route-group-form-title").textContent = `编辑路径前缀 ${group.path_prefix} @ ${hostLabel}`;
}

export function openPrefixEditor(pathPrefix, requestHost) {
  const group = findRouteGroup(pathPrefix, requestHost, state);
  if (!group) return;
  fillRouteGroupForm(group);
  document.getElementById("route-group-form-title").textContent = `编辑路径前缀 ${pathPrefix}`;
  openModal("prefix-modal");
}

export function collectRouteGroupForm() {
  return {
    old_path_prefix: getValue("route_group_old_path_prefix"),
    old_request_host: normalizeRequestHost(getValue("route_group_old_request_host")),
    path_prefix: getValue("route_group_path_prefix"),
    request_host: normalizeRequestHost(getValue("route_group_request_host")),
    access_ip_whitelist: getValue("route_group_access_ip_whitelist"),
    ip_blacklist: getValue("route_group_ip_blacklist"),
    region_whitelist: getValue("route_group_region_whitelist"),
    region_blacklist: getValue("route_group_region_blacklist"),
    notes: getValue("route_group_notes"),
  };
}

export async function submitRouteGroup() {
  const payload = collectRouteGroupForm();
  try {
    if (payload.old_path_prefix) {
      await apiFetch("/_admin/api/route-groups", {
        method: "PUT",
        body: JSON.stringify(payload),
      });
      showToast("路径前缀已更新。");
    } else {
      await apiFetch("/_admin/api/route-groups", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      showToast("路径前缀已创建。");
    }
    resetRouteGroupForm();
    await loadDashboard();
  } catch (error) {
    showToast(error.message, true);
  }
}

export async function updateGroupRegionSwitch(pathPrefix, requestHost, enabled) {
  const group = findRouteGroup(pathPrefix, requestHost, state);
  if (!group) {
    throw new Error(`未找到路径前缀 ${pathPrefix} @ ${formatRequestHostLabel(requestHost)}`);
  }

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

// ============ 规则管理 ============

export function renderRules(rules) {
  state.rules = rules;
  const tbody = document.getElementById("rules-table-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!rules.length) {
    tbody.innerHTML = '<tr><td colspan="6">暂无转发规则，请先新增规则。</td></tr>';
    return;
  }

  rules.forEach((rule) => {
    const tr = document.createElement("tr");
    const requestHost = normalizeRequestHost(rule.request_host);
    const statusBadges = [
      rule.is_default ? '<span class="badge badge-default">默认</span>' : "",
    ].join("");
    const conditions = [];
    if (rule.ip_whitelist) {
      conditions.push(`IP白(路由): ${escapeHtml(rule.ip_whitelist)}`);
    }
    if (rule.region_filters) {
      conditions.push(`地区条件: ${escapeHtml(rule.region_filters)}`);
    }
    if (rule.access_ip_whitelist) {
      conditions.push(`IP白(访问): ${escapeHtml(rule.access_ip_whitelist)}`);
    }
    if (rule.ip_blacklist) {
      conditions.push(`IP黑: ${escapeHtml(rule.ip_blacklist)}`);
    }
    if (rule.region_whitelist) {
      conditions.push(`地区白: ${escapeHtml(rule.region_whitelist)}`);
    }
    if (rule.region_blacklist) {
      conditions.push(`地区黑: ${escapeHtml(rule.region_blacklist)}`);
    }
    const conditionsText = conditions.length ? conditions.join("\n") : "默认";

    tr.innerHTML = `
      <td>
        <strong>${escapeHtml(rule.name || "(未命名规则)")}</strong>
        <div>${statusBadges}</div>
      </td>
      <td title="${escapeHtml(rule.path_prefix + (requestHost ? "\n域名: " + formatRequestHostLabel(requestHost) : ""))}">
        <div>${escapeHtml(rule.path_prefix)}</div>
        <div class="hint">域名: ${escapeHtml(formatRequestHostLabel(requestHost))}</div>
      </td>
      <td title="${escapeHtml(rule.target_url)}">${escapeHtml(rule.target_url)}</td>
      <td title="${escapeHtml(conditionsText)}">${conditions.length ? conditions.join("<br>") : "默认"}</td>
      <td>
        <button class="toggle-status-btn ${rule.enabled ? "on" : "off"}" data-action="toggle-rule" data-id="${rule.id}" type="button" title="${rule.enabled ? "点击禁用" : "点击启用"}">
          <span class="toggle-status-dot"></span>
          <span class="toggle-status-text">${rule.enabled ? "启用" : "禁用"}</span>
        </button>
      </td>
      <td>
        <div class="table-actions">
          <button class="table-btn" data-action="edit-rule" data-id="${rule.id}" type="button">编辑</button>
          <button class="table-btn delete" data-action="delete-rule" data-id="${rule.id}" type="button">删除</button>
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

export function resetRuleForm() {
  setValue("rule_id", "");
  setValue("rule_name", "");
  setValue("rule_path_prefix", "");
  setValue("rule_request_host", "");
  setValue("rule_target_url", "");
  setValue("rule_ip_whitelist", "");
  setValue("rule_region_filters", "");
  setValue("rule_access_ip_whitelist", "");
  setValue("rule_ip_blacklist", "");
  setValue("rule_region_whitelist", "");
  setValue("rule_region_blacklist", "");
  setValue("rule_priority", "0");
  setValue("rule_timeout", "30");
  setValue("rule_max_redirects", "10");
  setValue("rule_retry_times", "3");
  setValue("rule_notes", "");
  setValue("rule_path_rewrite_pattern", "");
  setValue("rule_path_rewrite_replacement", "");
  setChecked("rule_enabled", true);
  setChecked("rule_is_default", false);
  setChecked("rule_strip_prefix", false);
  setChecked("rule_follow_redirects", true);
  setChecked("rule_enable_streaming", true);
  document.getElementById("rule-form-title").textContent = "新增规则";
}

export function fillRuleForm(rule) {
  setValue("rule_id", rule.id);
  setValue("rule_name", rule.name || "");
  setValue("rule_path_prefix", rule.path_prefix || "");
  setValue("rule_request_host", normalizeRequestHost(rule.request_host));
  setValue("rule_target_url", rule.target_url || "");
  setValue("rule_ip_whitelist", rule.ip_whitelist || "");
  setValue("rule_region_filters", rule.region_filters || "");
  setValue("rule_access_ip_whitelist", rule.access_ip_whitelist || "");
  setValue("rule_ip_blacklist", rule.ip_blacklist || "");
  setValue("rule_region_whitelist", rule.region_whitelist || "");
  setValue("rule_region_blacklist", rule.region_blacklist || "");
  setValue("rule_priority", rule.priority ?? 0);
  setValue("rule_timeout", rule.timeout ?? 30);
  setValue("rule_max_redirects", rule.max_redirects ?? 10);
  setValue("rule_retry_times", rule.retry_times ?? 3);
  setValue("rule_notes", rule.notes || "");
  setValue("rule_path_rewrite_pattern", rule.path_rewrite_pattern || "");
  setValue("rule_path_rewrite_replacement", rule.path_rewrite_replacement || "");
  setChecked("rule_enabled", rule.enabled);
  setChecked("rule_is_default", rule.is_default);
  setChecked("rule_strip_prefix", rule.strip_prefix);
  setChecked("rule_follow_redirects", rule.follow_redirects !== false);
  setChecked("rule_enable_streaming", rule.enable_streaming);
  document.getElementById("rule-form-title").textContent = `编辑规则 #${rule.id}`;
}

export function collectRuleForm() {
  return {
    id: getValue("rule_id") || undefined,
    name: getValue("rule_name"),
    path_prefix: getValue("rule_path_prefix"),
    request_host: normalizeRequestHost(getValue("rule_request_host")),
    target_url: getValue("rule_target_url"),
    ip_whitelist: getValue("rule_ip_whitelist"),
    region_filters: getValue("rule_region_filters"),
    access_ip_whitelist: getValue("rule_access_ip_whitelist"),
    ip_blacklist: getValue("rule_ip_blacklist"),
    region_whitelist: getValue("rule_region_whitelist"),
    region_blacklist: getValue("rule_region_blacklist"),
    priority: Number(getValue("rule_priority") || 0),
    timeout: Number(getValue("rule_timeout") || 30),
    max_redirects: Number(getValue("rule_max_redirects") || 10),
    retry_times: Number(getValue("rule_retry_times") || 3),
    notes: getValue("rule_notes"),
    path_rewrite_pattern: getValue("rule_path_rewrite_pattern"),
    path_rewrite_replacement: getValue("rule_path_rewrite_replacement"),
    enabled: getChecked("rule_enabled"),
    is_default: getChecked("rule_is_default"),
    strip_prefix: getChecked("rule_strip_prefix"),
    follow_redirects: getChecked("rule_follow_redirects"),
    enable_streaming: getChecked("rule_enable_streaming"),
  };
}

export function prepareRuleForGroup(pathPrefix, requestHost = "") {
  resetRuleForm();
  setValue("rule_path_prefix", pathPrefix);
  setValue("rule_request_host", normalizeRequestHost(requestHost));
  document.getElementById("rule-form-title").textContent = "新增规则";
  openModal("rule-modal");
  focusField("rule_name");
}

export function openRuleEditor(ruleId) {
  const id = Number(ruleId);
  const rule = state.rules.find(r => r.id === id);
  if (!rule) return;
  fillRuleForm(rule);
  document.getElementById("rule-form-title").textContent = `编辑规则 #${rule.id}`;
  openModal("rule-modal");
}

export async function submitRule() {
  const payload = collectRuleForm();
  const ruleId = payload.id;
  delete payload.id;

  try {
    if (ruleId) {
      await apiFetch(`/_admin/api/rules/${ruleId}`, {
        method: "PUT",
        body: JSON.stringify(payload),
      });
      showToast("规则已更新。");
    } else {
      await apiFetch("/_admin/api/rules", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      showToast("规则已创建。");
    }
    resetRuleForm();
    await loadDashboard();
  } catch (error) {
    showToast(error.message, true);
  }
}

export async function removeRule(ruleId) {
  if (!window.confirm(`确认删除规则 #${ruleId} 吗？`)) {
    return;
  }
  try {
    await apiFetch(`/_admin/api/rules/${ruleId}`, { method: "DELETE" });
    resetRuleForm();
    await loadDashboard();
    showToast("规则已删除。");
  } catch (error) {
    showToast(error.message, true);
  }
}

export async function toggleRule(ruleId, enabled) {
  try {
    await apiFetch(`/_admin/api/rules/${ruleId}`, {
      method: "PUT",
      body: JSON.stringify({ enabled }),
    });
    await loadDashboard();
    showToast(enabled ? "规则已启用。" : "规则已禁用。");
  } catch (error) {
    showToast(error.message, true);
  }
}

const RULE_FIELD_LABELS = {
  strip_prefix: "去前缀",
  follow_redirects: "跟随重定向",
  enable_streaming: "流式转发",
};

export async function toggleRuleField(ruleId, field, nextValue) {
  const label = RULE_FIELD_LABELS[field] || field;
  try {
    await apiFetch(`/_admin/api/rules/${ruleId}`, {
      method: "PUT",
      body: JSON.stringify({ [field]: nextValue }),
    });
    await loadDashboard();
    showToast(`${label}已${nextValue ? "开启" : "关闭"}。`);
  } catch (error) {
    showToast(error.message, true);
  }
}

// ============ GeoIP 配置 ============

export function bindGeoNumericInputSafety() {
  const bindInput = (id, sanitizer) => {
    const input = document.getElementById(id);
    if (!input) return;
    input.addEventListener("wheel", (event) => {
      if (document.activeElement === input) {
        event.preventDefault();
        input.blur();
      }
    }, { passive: false });
    input.addEventListener("change", () => {
      sanitizer();
    });
    input.addEventListener("blur", () => {
      sanitizer();
    });
  };

  bindInput("geo_online_cache_ttl_seconds", () => {
    setValue("geo_online_cache_ttl_seconds", getNonNegativeIntValue("geo_online_cache_ttl_seconds", 120));
  });
  bindInput("geo_offline_refresh_interval_hours", () => {
    setValue("geo_offline_refresh_interval_hours", getPositiveIntValue("geo_offline_refresh_interval_hours", 24));
  });
}

export function fillGeoConfig(geo) {
  setChecked("geo_enabled", geo.enabled);
  setValue("geo_online_cache_ttl_seconds", Math.max(0, Number.parseInt(String(geo.online_cache_ttl_seconds ?? 120), 10) || 120));
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
  resetGeoSourceForm();
  resetGeoSourceTestResult();

  setChecked("geo_offline_enabled", geo.offline?.enabled);
  setValue("geo_offline_db_path", geo.offline?.db_path || "");
  setValue("geo_offline_locale", geo.offline?.locale || "zh-CN");
  setValue("geo_offline_download_url", geo.offline?.download_url || "");
  setValue("geo_offline_download_headers_json", geo.offline?.download_headers_json || "{}");
  setValue("geo_offline_refresh_interval_hours", Math.max(1, Number.parseInt(String(geo.offline?.refresh_interval_hours ?? 24), 10) || 24));
  renderOfflineStatus(geo.offline || {});
  resetOfflineGeoTestResult();
}

export function renderGeoSources() {
  const tbody = document.getElementById("geo-sources-table-body");
  tbody.innerHTML = "";

  if (!state.geoSources.length) {
    tbody.innerHTML = '<tr><td colspan="6">暂无在线定位源，将直接使用离线库或默认规则。</td></tr>';
    return;
  }

  state.geoSources.forEach((source, index) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td data-label="名称"><strong>${escapeHtml(source.name || `source-${index + 1}`)}</strong></td>
      <td data-label="地址">${escapeHtml(source.url)}</td>
      <td data-label="权重">${source.weight}</td>
      <td data-label="方式">${escapeHtml(source.method)} / ${escapeHtml(source.request_location)}</td>
      <td data-label="状态">
        <button class="toggle-status-btn ${source.enabled ? "on" : "off"}" data-action="toggle-geo-source" data-index="${index}" type="button" title="${source.enabled ? "点击禁用" : "点击启用"}">
          <span class="toggle-status-dot"></span>
          <span class="toggle-status-text">${source.enabled ? "启用" : "停用"}</span>
        </button>
      </td>
      <td data-label="操作">
        <div class="table-actions">
          <button class="table-btn" data-action="test-geo-source" data-index="${index}" type="button">测试</button>
          <button class="table-btn" data-action="edit-geo-source" data-index="${index}" type="button">编辑</button>
          <button class="table-btn delete" data-action="delete-geo-source" data-index="${index}" type="button">删除</button>
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

export function resetGeoSourceForm() {
  setValue("geo_source_id", "");
  setValue("geo_source_name", "");
  setValue("geo_source_url", "");
  setValue("geo_source_weight", "1");
  setValue("geo_source_priority", "0");
  setValue("geo_source_method", "GET");
  setValue("geo_source_request_location", "query");
  setValue("geo_source_body_format", "json");
  setValue("geo_source_query_params_json", "{}");
  setValue("geo_source_headers_json", "{}");
  setValue("geo_source_body_template", "");
  setValue("geo_source_ip_param_name", "ip");
  setValue("geo_source_timeout", "3");
  setValue("geo_source_country_path", "country");
  setValue("geo_source_region_path", "region");
  setValue("geo_source_city_path", "city");
  setValue("geo_source_full_path", "");
  setValue("geo_source_notes", "");
  setChecked("geo_source_enabled", true);
  document.getElementById("geo-source-form-title").textContent = "新增在线源";
  resetGeoSourceTestResult();
}

export function fillGeoSourceForm(source, index) {
  setValue("geo_source_id", index);
  setValue("geo_source_name", source.name || "");
  setValue("geo_source_url", source.url || "");
  setValue("geo_source_weight", source.weight ?? 1);
  setValue("geo_source_priority", source.priority ?? 0);
  setValue("geo_source_method", source.method || "GET");
  setValue("geo_source_request_location", source.request_location || "query");
  setValue("geo_source_body_format", source.body_format || "json");
  setValue("geo_source_query_params_json", source.query_params_json || "{}");
  setValue("geo_source_headers_json", source.headers_json || "{}");
  setValue("geo_source_body_template", source.body_template || "");
  setValue("geo_source_ip_param_name", source.ip_param_name || "ip");
  setValue("geo_source_timeout", source.timeout ?? 3);
  setValue("geo_source_country_path", source.country_path || "country");
  setValue("geo_source_region_path", source.region_path || "region");
  setValue("geo_source_city_path", source.city_path || "city");
  setValue("geo_source_full_path", source.full_path || "");
  setValue("geo_source_notes", source.notes || "");
  setChecked("geo_source_enabled", source.enabled);
  document.getElementById("geo-source-form-title").textContent = `编辑在线源 #${index + 1}`;
  resetGeoSourceTestResult();
}

export function collectGeoSourceForm() {
  return {
    name: getValue("geo_source_name"),
    enabled: getChecked("geo_source_enabled"),
    weight: Number(getValue("geo_source_weight") || 1),
    url: getValue("geo_source_url"),
    method: getValue("geo_source_method"),
    request_location: getValue("geo_source_request_location"),
    body_format: getValue("geo_source_body_format"),
    query_params_json: getValue("geo_source_query_params_json"),
    headers_json: getValue("geo_source_headers_json"),
    body_template: getValue("geo_source_body_template"),
    ip_param_name: getValue("geo_source_ip_param_name"),
    timeout: Number(getValue("geo_source_timeout") || 3),
    country_path: getValue("geo_source_country_path"),
    region_path: getValue("geo_source_region_path"),
    city_path: getValue("geo_source_city_path"),
    full_path: getValue("geo_source_full_path"),
    priority: Number(getValue("geo_source_priority") || 0),
    notes: getValue("geo_source_notes"),
  };
}

export function resetGeoSourceTestResult(message = "输入测试 IP 后，可查看测试结果和区域信息。") {
  const container = document.getElementById("geo-source-test-result");
  container.classList.add("is-empty");
  container.innerHTML = `<p class="test-result-placeholder">${escapeHtml(message)}</p>`;
}

export function formatTestRawPayload(payload) {
  if (payload === undefined || payload === null) {
    return "";
  }
  if (typeof payload === "string") {
    return payload;
  }
  try {
    return JSON.stringify(payload, null, 2);
  } catch {
    return String(payload);
  }
}

export function renderGeoSourceTestResult(result) {
  const container = document.getElementById("geo-source-test-result");
  const location = result.location || {};
  const sourceName = result.provider || "-";
  const testIp = location.ip || getValue("geo_source_test_ip").trim() || "-";
  const statusText = result.success ? "测试成功" : "测试失败";
  const statusClass = result.success ? "success" : "failed";
  const upstreamResponse = result.upstream_response || {};
  const upstreamPayload =
    upstreamResponse.payload !== undefined ? upstreamResponse.payload : location.raw;
  const upstreamPayloadText = formatTestRawPayload(upstreamPayload);
  const rawJson =
    upstreamPayloadText
      ? `
        <details class="test-result-raw">
          <summary>查看接口原始返回</summary>
          <pre>${escapeHtml(upstreamPayloadText)}</pre>
        </details>
      `
      : "";

  container.classList.remove("is-empty");
  container.innerHTML = `
    <div class="test-result-head">
      <div>
        <h4>${statusText}</h4>
        <p class="test-result-message">${escapeHtml(result.message || "-")}</p>
      </div>
      <span class="status-pill ${statusClass}">${statusText}</span>
    </div>
    <div class="result-grid">
      <div class="result-item">
        <strong>测试 IP</strong>
        <span>${escapeHtml(testIp)}</span>
      </div>
      <div class="result-item">
        <strong>测试源</strong>
        <span>${escapeHtml(sourceName)}</span>
      </div>
      <div class="result-item">
        <strong>定位阶段</strong>
        <span>${escapeHtml(result.stage || "-")}</span>
      </div>
      <div class="result-item">
        <strong>上游状态</strong>
        <span>${escapeHtml(String(upstreamResponse.status ?? "-"))}</span>
      </div>
      <div class="result-item">
        <strong>国家</strong>
        <span>${escapeHtml(location.country || "-")}</span>
      </div>
      <div class="result-item">
        <strong>地区</strong>
        <span>${escapeHtml(location.region || "-")}</span>
      </div>
      <div class="result-item">
        <strong>城市</strong>
        <span>${escapeHtml(location.city || "-")}</span>
      </div>
      <div class="result-item">
        <strong>区域汇总</strong>
        <span>${escapeHtml(location.summary || location.full_text || "-")}</span>
      </div>
    </div>
    ${rawJson}
  `;
}

export function renderOfflineStatus(offline) {
  const container = document.getElementById("geo-offline-status");
  const status = offline.status || {};
  container.classList.remove("is-empty");
  container.innerHTML = `
    <div class="test-result-head">
      <div>
        <h4>离线库维护状态</h4>
        <p class="test-result-message">${escapeHtml(status.last_sync_message || "尚未执行同步。")}</p>
      </div>
      <span class="status-pill ${status.file_exists ? "success" : "failed"}">${status.file_exists ? "文件可用" : "文件缺失"}</span>
    </div>
    <div class="result-grid">
      <div class="result-item">
        <strong>本地路径</strong>
        <span>${escapeHtml(offline.db_path || "-")}</span>
      </div>
      <div class="result-item">
        <strong>下载链接</strong>
        <span>${escapeHtml(offline.download_url || "-")}</span>
      </div>
      <div class="result-item">
        <strong>文件大小</strong>
        <span>${escapeHtml(formatBytes(status.file_size || 0))}</span>
      </div>
      <div class="result-item">
        <strong>文件更新时间</strong>
        <span>${escapeHtml(formatDateTime(status.file_updated_at || ""))}</span>
      </div>
      <div class="result-item">
        <strong>备份文件</strong>
        <span>${escapeHtml(status.backup_exists ? (status.backup_path || "-") : "暂无备份")}</span>
      </div>
      <div class="result-item">
        <strong>备份大小</strong>
        <span>${escapeHtml(formatBytes(status.backup_size || 0))}</span>
      </div>
      <div class="result-item">
        <strong>备份更新时间</strong>
        <span>${escapeHtml(formatDateTime(status.backup_updated_at || ""))}</span>
      </div>
      <div class="result-item">
        <strong>最近同步</strong>
        <span>${escapeHtml(formatDateTime(status.last_sync_at || ""))}</span>
      </div>
      <div class="result-item">
        <strong>最近成功</strong>
        <span>${escapeHtml(formatDateTime(status.last_success_at || ""))}</span>
      </div>
      <div class="result-item">
        <strong>同步状态</strong>
        <span>${escapeHtml(status.last_sync_status || "-")}</span>
      </div>
      <div class="result-item">
        <strong>下次自动同步</strong>
        <span>${escapeHtml(formatDateTime(status.next_sync_at || ""))}</span>
      </div>
    </div>
  `;
}

export function resetOfflineGeoTestResult(message = "输入测试 IP 后，可以直接查看离线库定位结果。") {
  const container = document.getElementById("geo-offline-test-result");
  container.classList.add("is-empty");
  container.innerHTML = `<p class="test-result-placeholder">${escapeHtml(message)}</p>`;
}

export function renderOfflineGeoTestResult(result) {
  const container = document.getElementById("geo-offline-test-result");
  const location = result.location || {};
  const testIp = location.ip || getValue("geo_offline_test_ip").trim() || "-";
  const statusText = result.success ? "测试成功" : "测试失败";
  const statusClass = result.success ? "success" : "failed";

  container.classList.remove("is-empty");
  container.innerHTML = `
    <div class="test-result-head">
      <div>
        <h4>${statusText}</h4>
        <p class="test-result-message">${escapeHtml(result.message || "-")}</p>
      </div>
      <span class="status-pill ${statusClass}">${statusText}</span>
    </div>
    <div class="result-grid">
      <div class="result-item">
        <strong>测试 IP</strong>
        <span>${escapeHtml(testIp)}</span>
      </div>
      <div class="result-item">
        <strong>定位来源</strong>
        <span>${escapeHtml(result.provider || "offline_mmdb")}</span>
      </div>
      <div class="result-item">
        <strong>国家</strong>
        <span>${escapeHtml(location.country || "-")}</span>
      </div>
      <div class="result-item">
        <strong>地区</strong>
        <span>${escapeHtml(location.region || "-")}</span>
      </div>
      <div class="result-item">
        <strong>城市</strong>
        <span>${escapeHtml(location.city || "-")}</span>
      </div>
      <div class="result-item">
        <strong>区域汇总</strong>
        <span>${escapeHtml(location.summary || location.full_text || "-")}</span>
      </div>
    </div>
  `;
}

export function buildGeoSettingsPayload() {
  return {
    enabled: getChecked("geo_enabled"),
    online_cache_ttl_seconds: getNonNegativeIntValue("geo_online_cache_ttl_seconds", 120),
    sources: state.geoSources.map((source) => ({
      name: source.name,
      enabled: source.enabled,
      weight: source.weight,
      url: source.url,
      method: source.method,
      request_location: source.request_location,
      body_format: source.body_format,
      query_params_json: source.query_params_json,
      headers_json: source.headers_json,
      body_template: source.body_template,
      ip_param_name: source.ip_param_name,
      timeout: source.timeout,
      country_path: source.country_path,
      region_path: source.region_path,
      city_path: source.city_path,
      full_path: source.full_path,
      priority: source.priority,
      notes: source.notes,
    })),
    offline: {
      enabled: getChecked("geo_offline_enabled"),
      db_path: getValue("geo_offline_db_path"),
      locale: getValue("geo_offline_locale"),
      download_url: getValue("geo_offline_download_url"),
      download_headers_json: getValue("geo_offline_download_headers_json"),
      refresh_interval_hours: getPositiveIntValue("geo_offline_refresh_interval_hours", 24),
    },
  };
}

export async function persistGeoSettings(successMessage = "IP 定位配置已保存。") {
  await apiFetch("/_admin/api/geoip", {
    method: "PUT",
    body: JSON.stringify(buildGeoSettingsPayload()),
  });
  await loadDashboard();
  showToast(successMessage);
}

export function fillGeoSourceTestSelect(selectedIndex) {
  const select = document.getElementById("geo_source_test_select");
  if (!select) return;
  select.innerHTML = '<option value="">请选择在线源</option>';
  state.geoSources.forEach((source, index) => {
    const opt = document.createElement("option");
    opt.value = String(index);
    opt.textContent = source.name || `source-${index + 1}`;
    select.appendChild(opt);
  });
  if (selectedIndex !== undefined && selectedIndex !== null) {
    select.value = String(selectedIndex);
  }
}

// ============ 日志管理 ============

export function renderRouteLogSettings(settings) {
  state.routeLogSettings = settings;
  setValue("log_retention_days", settings.retention_days ?? 30);
  const container = document.getElementById("route-log-settings-status");
  container.classList.remove("is-empty");
  container.innerHTML = `
    <div class="test-result-head">
      <div>
        <h4>日志清理状态</h4>
        <p class="test-result-message">当前保留策略为 ${escapeHtml(String(settings.retention_days ?? 30))} 天。</p>
      </div>
      <span class="status-pill success">已启用</span>
    </div>
    <div class="result-grid">
      <div class="result-item">
        <strong>日志总数</strong>
        <span>${escapeHtml(String(settings.total_logs ?? 0))}</span>
      </div>
      <div class="result-item">
        <strong>最大保留天数</strong>
        <span>${escapeHtml(String(settings.retention_days ?? 30))}</span>
      </div>
      <div class="result-item">
        <strong>最近清理时间</strong>
        <span>${escapeHtml(formatDateTime(settings.last_pruned_at || ""))}</span>
      </div>
      <div class="result-item">
        <strong>设置更新时间</strong>
        <span>${escapeHtml(formatDateTime(settings.updated_at || ""))}</span>
      </div>
    </div>
  `;
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
  container.innerHTML = "";
  setChecked("route-log-select-all", false);

  if (!state.routeLogs.length) {
    container.innerHTML = '<div class="route-log-empty">当前没有匹配到规则转发日志。</div>';
    renderPagination(1, 1, "log-pagination", goToPage);
    return;
  }

  state.routeLogs.forEach((log) => {
    const requestMethod = escapeHtml(log.request_method || "-");
    const requestPath = escapeHtml(log.request_path || "-");
    const requestQuery = escapeHtml(log.request_query_string || "");
    const requestHost = escapeHtml(formatRouteLogRequestHost(log.request_host || ""));
    const originalClientIp = escapeHtml(log.original_client_ip || "-");
    const clientIp = escapeHtml(log.client_ip || "-");
    const pathPrefix = escapeHtml(log.path_prefix || "-");
    const ruleName = escapeHtml(log.rule_name || "-");
    const ruleRequestHost = escapeHtml(formatRouteLogRuleRequestHost(log.rule_request_host || ""));
    const geoSummary = escapeHtml(log.geo_summary || "-");
    const matchedRegion = escapeHtml(log.matched_region || "-");
    const geoSource = escapeHtml(log.geo_source || "-");
    const matchStrategy = escapeHtml(formatMatchStrategy(log.match_strategy));
    const matchDetail = escapeHtml(formatMatchDetail(log.match_detail));
    const matchedWhitelist = escapeHtml(log.matched_ip_whitelist || "-");
    const configuredWhitelist = escapeHtml(log.configured_ip_whitelist || "-");
    const configuredRegions = escapeHtml(log.configured_regions || "-");
    const redirectLocation = escapeHtml(log.redirect_location || "");
    const targetUrl = escapeHtml(log.target_url || "-");
    const upstreamStatus = escapeHtml(String(log.upstream_status || 0));
    const cacheStatusInfo = formatCacheStatus(log.cache_status);
    const resultStatus = escapeHtml(formatResultStatus(log.result_status));
    const durationText = escapeHtml(`${log.operation_duration_ms || 0} ms`);
    const createdAt = escapeHtml(formatDateTime(log.created_at));
    const banIp = escapeHtml(log.client_ip || log.original_client_ip || "-");
    const ipBanned = banIp !== "-" && isIpBanned(banIp, state.bannedIps);
    const banButtonHtml = ipBanned
      ? `<button class="table-btn unban-btn" data-action="unban-ip-from-log" data-ip="${banIp}" type="button" title="解禁IP: ${banIp}">解禁IP</button>`
      : `<button class="table-btn ban-btn" data-action="ban-ip-from-log" data-ip="${banIp}" type="button" title="封禁IP: ${banIp}">封禁IP</button>`;

    const card = document.createElement("article");
    card.className = "route-log-item";
    card.innerHTML = `
      <div class="route-log-item-main">
        <div class="route-log-item-check">
          <input class="route-log-checkbox" data-id="${log.id}" type="checkbox" />
        </div>
        <div class="route-log-item-body">
          <div class="route-log-item-header">
            <div class="route-log-item-time">
              <strong>${createdAt}</strong>
              <span class="route-log-duration">${durationText}</span>
            </div>
            <div class="route-log-item-actions">
              ${banButtonHtml}
              <button class="table-btn delete" data-action="delete-route-log" data-id="${log.id}" type="button">删除</button>
            </div>
          </div>
          <div class="route-log-item-fields">
            <div class="route-log-field-group">
              <div class="route-log-field">
                <span class="route-log-field-label">请求</span>
                <div class="route-log-field-value">
                  <strong>${requestMethod}</strong>
                  <span class="route-log-path" title="${requestPath}">${requestPath}</span>
                  ${requestQuery ? `<span class="route-log-query hint" title="${requestQuery}">?${requestQuery}</span>` : ""}
                </div>
              </div>
              <div class="route-log-field">
                <span class="route-log-field-label">域名</span>
                <div class="route-log-field-value"><span>${requestHost}</span></div>
              </div>
            </div>
            <div class="route-log-field-group">
              <div class="route-log-field">
                <span class="route-log-field-label">前缀</span>
                <div class="route-log-field-value"><strong>${pathPrefix}</strong></div>
              </div>
              <div class="route-log-field">
                <span class="route-log-field-label">规则</span>
                <div class="route-log-field-value"><span>${ruleName}</span></div>
              </div>
              <div class="route-log-field">
                <span class="route-log-field-label">命中域名规则</span>
                <div class="route-log-field-value"><span>${ruleRequestHost}</span></div>
              </div>
            </div>
            <div class="route-log-field-group">
              <div class="route-log-field">
                <span class="route-log-field-label">地区</span>
                <div class="route-log-field-value">
                  <strong>${geoSummary}</strong>
                  <span class="hint">命中: ${matchedRegion}</span>
                  <span class="hint">源: ${geoSource}</span>
                </div>
              </div>
            </div>
            <div class="route-log-field-group">
              <div class="route-log-field">
                <span class="route-log-field-label">匹配</span>
                <div class="route-log-field-value">
                  <strong>${matchStrategy}</strong>
                  <span class="hint">${matchDetail}</span>
                  <span class="hint">命中白名单: ${matchedWhitelist}</span>
                  <span class="hint">规则白名单: ${configuredWhitelist}</span>
                  <span class="hint">规则地区: ${configuredRegions}</span>
                </div>
              </div>
            </div>
            <div class="route-log-field-group">
              <div class="route-log-field">
                <span class="route-log-field-label">302地址</span>
                <div class="route-log-field-value">
                  <strong class="route-log-target-url" title="${redirectLocation}">${redirectLocation || "-"}</strong>
                </div>
              </div>
            </div>
            <div class="route-log-field-group">
              <div class="route-log-field">
                <span class="route-log-field-label">转发结果</span>
                <div class="route-log-field-value">
                  <strong class="route-log-target-url" title="${targetUrl}">${targetUrl}</strong>
                  <span class="hint">上游状态: ${upstreamStatus}</span>
                  <span class="cache-status-badge ${cacheStatusInfo.cls}">缓存命中：${cacheStatusInfo.text}</span>
                  <span class="hint">结果: ${resultStatus}</span>
                </div>
              </div>
            </div>
            <div class="route-log-field-group">
              <div class="route-log-field">
                <span class="route-log-field-label">IP</span>
                <div class="route-log-field-value">
                  <span>原始: ${originalClientIp}</span>
                  <span>匹配: ${clientIp}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
    container.appendChild(card);
  });
  renderPagination(state.logCurrentPage, state.logTotalPages, "log-pagination", goToPage);
}

export function ensureRouteLogFilterFields() {
  const keywordInput = document.getElementById("log_keyword");
  if (keywordInput) {
    keywordInput.placeholder = "路径、域名、目标地址、302地址、地区、IP、原始IP";
  }

  const filtersContainer = document.querySelector("#route-log-filter-form .two-col");
  if (!filtersContainer || document.getElementById("log_rule_request_host")) {
    return;
  }

  const label = document.createElement("label");
  label.innerHTML = `
    <span>命中域名规则</span>
    <input id="log_rule_request_host" type="text" placeholder="example.com（输入 * 查询通配规则）" />
  `;

  const pathPrefixInput = document.getElementById("log_path_prefix");
  const anchorLabel = pathPrefixInput ? pathPrefixInput.closest("label") : null;
  if (anchorLabel && anchorLabel.nextSibling) {
    filtersContainer.insertBefore(label, anchorLabel.nextSibling);
  } else {
    filtersContainer.appendChild(label);
  }
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

export function buildRouteLogQuery(filters) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value === "" || value === null || value === undefined) {
      return;
    }
    params.set(key, String(value));
  });
  return params.toString();
}

export async function loadRouteLogSettings() {
  const data = await apiFetch("/_admin/api/log-settings");
  renderRouteLogSettings(data || {});
}

export async function loadRouteLogs() {
  const filters = collectRouteLogFilters();
  const query = buildRouteLogQuery(filters);
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

// ============ 自动刷新（日志） ============

let _autoRefreshTimer = null;
const AUTO_REFRESH_STORAGE_KEY = "log_auto_refresh";

export function getAutoRefreshConfig() {
  try {
    const raw = localStorage.getItem(AUTO_REFRESH_STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch (e) {}
  return { enabled: false, interval: 5 };
}

export function saveAutoRefreshConfig(cfg) {
  localStorage.setItem(AUTO_REFRESH_STORAGE_KEY, JSON.stringify(cfg));
}

function updateAutoRefreshStatusUI() {
  const el = document.getElementById("log_auto_refresh_status");
  if (!el) return;
  if (_autoRefreshTimer !== null) {
    el.textContent = "●";
    el.className = "auto-refresh-status running";
  } else {
    el.textContent = "";
    el.className = "auto-refresh-status stopped";
  }
}

export function stopAutoRefresh() {
  if (_autoRefreshTimer !== null) {
    clearInterval(_autoRefreshTimer);
    _autoRefreshTimer = null;
  }
  updateAutoRefreshStatusUI();
}

export function startAutoRefresh() {
  stopAutoRefresh();
  const enabled = getChecked("log_auto_refresh_enabled");
  if (!enabled) return;
  const interval = Math.max(1, parseInt(getValue("log_auto_refresh_interval") || "5", 10) || 5);
  saveAutoRefreshConfig({ enabled: true, interval });
  _autoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "logs") {
      stopAutoRefresh();
      return;
    }
    loadRouteLogs().catch((error) => {
      showToast(error.message, true);
      stopAutoRefresh();
      setChecked("log_auto_refresh_enabled", false);
    });
  }, interval * 1000);
  updateAutoRefreshStatusUI();
}

// ============ 应用日志 ============

let _appLogAutoRefreshTimer = null;
const APP_LOG_MAX_DOM_NODES = 600;

export function highlightLogLine(line) {
  if (!line) return line;
  let safe = line.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  safe = safe.replace(
    /^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}[,\.]?\d*)/,
    '<span class="log-ts">$1</span>',
  );
  safe = safe.replace(
    /\b(INFO|DEBUG|WARNING|ERROR|CRITICAL)\b/g,
    '<span class="log-level-$1">$1</span>',
  );
  safe = safe.replace(
    /\b(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\/\S*)\s+(\d{3})\s+([\d.]+ms)\b/g,
    '<span class="log-method">$1</span> $2 <span class="log-status-$3">$3</span> <span class="log-duration">$4</span>',
  );
  return safe;
}

export async function loadAppLogFiles() {
  const data = await apiFetch("/_admin/api/app-logs");
  if (!data) return;
  const select = document.getElementById("app-log-file-select");
  if (!select) return;
  const prev = select.value || state.appLogFile;
  select.innerHTML = "";
  const files = data.items || [];
  if (!files.length) {
    select.innerHTML = '<option value="">暂无日志文件</option>';
    return;
  }
  files.forEach((file) => {
    const opt = document.createElement("option");
    opt.value = file.name;
    const sizeText = file.size >= 1024 * 1024
      ? (file.size / 1024 / 1024).toFixed(1) + " MB"
      : file.size >= 1024
        ? (file.size / 1024).toFixed(1) + " KB"
        : file.size + " B";
    opt.textContent = file.name + " (" + sizeText + ")";
    opt.selected = file.name === (prev || data.current);
    select.appendChild(opt);
  });
  const selected = select.value || data.current || files[0].name;
  if (state.appLogFile !== selected) {
    state.appLogFile = selected;
    loadAppLogContent();
  }
}

export async function loadAppLogContent(isAutoRefresh = false) {
  const params = new URLSearchParams();
  if (state.appLogFile) params.set("file", state.appLogFile);
  const keyword = (getValue("app-log-keyword") || "").trim();
  if (keyword) params.set("keyword", keyword);
  const tailLines = getValue("app-log-tail-lines") || "100";
  params.set("tail", tailLines);

  const contentEl = document.getElementById("app-log-content");
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
    for (let i = 0; i < newLines.length; i++) {
      const span = document.createElement("span");
      span.innerHTML = highlightLogLine(newLines[i]);
      fragment.appendChild(document.createElement("br"));
      fragment.appendChild(span);
    }
    contentEl.appendChild(fragment);
    state.logLastLineCount = newTotal;

    const allChildren = contentEl.childNodes;
    while (allChildren.length > APP_LOG_MAX_DOM_NODES) {
      contentEl.removeChild(allChildren[0]);
    }
  } else {
    const fragment = document.createDocumentFragment();
    for (let i = 0; i < lines.length; i++) {
      if (i > 0) fragment.appendChild(document.createElement("br"));
      const span = document.createElement("span");
      span.innerHTML = highlightLogLine(lines[i]);
      fragment.appendChild(span);
    }
    contentEl.innerHTML = "";
    contentEl.appendChild(fragment);
    state.logLastLineCount = newTotal;
  }

  if (fileInfoEl) fileInfoEl.textContent = `文件: ${data.file || state.appLogFile || "-"}`;
  if (lineInfoEl) {
    const matched = data.matched_lines != null ? data.matched_lines : data.total_lines;
    lineInfoEl.textContent = keyword
      ? `匹配: ${matched} / 总计: ${data.total_lines} 行`
      : `共 ${data.total_lines} 行`;
  }

  if (state.logAutoScroll) {
    contentEl.scrollTop = contentEl.scrollHeight;
  }
}

export function startAppLogAutoRefresh() {
  stopAppLogAutoRefresh();
  if (!getChecked("app-log-auto-refresh")) return;
  _appLogAutoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "app-logs") {
      stopAppLogAutoRefresh();
      return;
    }
    if (!state.logAutoScroll) return;
    loadAppLogContent(true).catch(() => {});
  }, 3000);
}

export function stopAppLogAutoRefresh() {
  if (_appLogAutoRefreshTimer !== null) {
    clearInterval(_appLogAutoRefreshTimer);
    _appLogAutoRefreshTimer = null;
  }
}

export async function refreshAppLogModule() {
  await loadAppLogFiles();
  initLogScrollDetection();
}

export function initLogScrollDetection() {
  const contentEl = document.getElementById("app-log-content");
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

// ============ IP 缓存管理 ============

export async function loadIpCacheSettings() {
  try {
    const data = await apiFetch("/_admin/api/ip-cache-settings");
    if (data) {
      setValue("ip_cache_enabled", data.enabled ? "1" : "0");
      setValue("ip_cache_ttl_seconds", String(data.ttl_seconds || 300));
      setValue("ip_cache_max_entries", String(data.max_entries || 5000));
    }
  } catch (error) {}
}

export async function loadIpCacheStats() {
  try {
    const stats = await apiFetch("/_admin/api/ip-cache/stats");
    const el = document.getElementById("ip-cache-stats");
    if (!el || !stats) return;
    el.className = "test-result-card";
    el.innerHTML = `
      <div class="test-result-row"><span>状态</span><span>${stats.enabled ? "已启用" : "已禁用"}</span></div>
      <div class="test-result-row"><span>当前条目</span><span>${stats.current_entries}</span></div>
      <div class="test-result-row"><span>命中次数</span><span>${stats.hits}</span></div>
      <div class="test-result-row"><span>未命中次数</span><span>${stats.misses}</span></div>
      <div class="test-result-row"><span>命中率</span><span>${stats.hit_rate}</span></div>
      <div class="test-result-row"><span>TTL</span><span>${stats.ttl_seconds}秒</span></div>
      <div class="test-result-row"><span>最大条目</span><span>${stats.max_entries}</span></div>
    `;
  } catch (error) {}
}

// ============ 自动封禁配置 ============

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
    }
  } catch (error) {}
}

export async function loadAutoBanStats() {
  try {
    const stats = await apiFetch("/_admin/api/auto-ban/stats");
    if (stats) {
      document.getElementById("auto-ban-status").textContent = stats.enabled ? "已启用" : "已禁用";
      document.getElementById("auto-ban-tracked-count").textContent = stats.tracked_ips || 0;
      document.getElementById("auto-ban-whitelist-count").textContent = stats.whitelisted_ips || 0;
      document.getElementById("auto-ban-total-requests").textContent = stats.total_requests || 0;
      document.getElementById("auto-ban-total-bans").textContent = stats.total_bans || 0;
    }
  } catch (error) {}
}

// ============ 邮件配置 ============

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
    }
  } catch (error) {}
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
    try {
      const ipPart = parts[0];
      if (ipPart.includes(":")) {
        return prefix <= 128;
      } else {
        if (prefix > 32) return false;
        const octets = ipPart.split(".");
        if (octets.length !== 4) return false;
        return octets.every((oct) => {
          const n = parseInt(oct, 10);
          return !isNaN(n) && n >= 0 && n <= 255;
        });
      }
    } catch {
      return false;
    }
  }
  if (s.includes(".")) {
    const octets = s.split(".");
    if (octets.length !== 4) return false;
    return octets.every((oct) => {
      const n = parseInt(oct, 10);
      return !isNaN(n) && n >= 0 && n <= 255;
    });
  }
  if (s.includes(":")) {
    return s.split(":").length >= 2;
  }
  return false;
}

export function isIpBanned(ip, bannedList) {
  if (!ip || ip === "-" || !bannedList || !bannedList.length) return false;
  if (bannedList.some((b) => b.ip === ip)) return true;
  for (const b of bannedList) {
    if (b.ip && b.ip.includes("/")) {
      if (ipInCidr(ip, b.ip)) return true;
    }
  }
  return false;
}

export function ipInCidr(ip, cidr) {
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
  } catch {
    return false;
  }
  return false;
}

export function ipv6ToBigInt(ip) {
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
    } else {
      fullParts = parts;
    }
    if (fullParts.length !== 8) return null;
    let result = 0n;
    for (const part of fullParts) {
      const num = parseInt(part || "0", 16);
      if (isNaN(num)) return null;
      result = (result << 16n) | BigInt(num);
    }
    return result;
  } catch {
    return null;
  }
}

export async function loadBannedIpList() {
  try {
    const data = await apiFetch("/_admin/api/banned-ips");
    state.bannedIps = data.items || [];
    renderBannedIpListPage();
  } catch (error) {}
}

export function renderBannedIpListPage() {
  const allItems = state.bannedIps;
  const totalCount = allItems.length;
  const pageSize = state.banPageSize;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  if (state.banCurrentPage > totalPages) state.banCurrentPage = totalPages;
  const currentPage = state.banCurrentPage;
  const startIdx = (currentPage - 1) * pageSize;
  const pageItems = allItems.slice(startIdx, startIdx + pageSize);

  setText("ban-total-count", `共 ${totalCount} 条`);
  renderBannedIpList(pageItems);
  renderPagination(currentPage, totalPages, "ban-pagination", goToBanPage);
}

export function goToBanPage(page, totalPages) {
  state.banCurrentPage = Math.max(1, Math.min(totalPages, page));
  renderBannedIpListPage();
}

export function renderBannedIpList(items) {
  const tbody = document.getElementById("banned-ips-table-body");
  if (!tbody) return;
  tbody.innerHTML = "";
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="7">暂无封禁IP记录。</td></tr>';
    return;
  }
  const nowSec = Math.floor(Date.now() / 1000);
  items.forEach((item) => {
    const tr = document.createElement("tr");

    let expireText;
    let statusBadge;
    let isExpired = false;
    if (item.permanent) {
      expireText = "永久";
      statusBadge = '<span class="ban-status ban-status-permanent">永久封禁</span>';
    } else if (item.expire_at && item.expire_at > 0) {
      isExpired = item.expire_at <= nowSec;
      const expireDate = new Date(item.expire_at * 1000);
      const remainSec = item.expire_at - nowSec;
      const formatted = expireDate.toLocaleString("zh-CN", { hour12: false });
      if (isExpired) {
        expireText = `${formatted}（已过期）`;
        statusBadge = '<span class="ban-status ban-status-expired">已过期</span>';
      } else {
        expireText = `${formatted}（剩 ${formatRemainTime(remainSec)}）`;
        statusBadge = '<span class="ban-status ban-status-temporary">临时封禁</span>';
      }
    } else {
      expireText = "-";
      statusBadge = '<span class="ban-status ban-status-unknown">未知</span>';
    }

    const pathPrefixText = item.path_prefix
      ? escapeHtml(item.path_prefix)
      : '<span class="ban-scope-global">全局</span>';

    const extendBtn = item.permanent
      ? ''
      : `<button class="table-btn" data-action="extend-ban-ip" data-ip="${escapeHtml(item.ip)}" data-expire="${item.expire_at || 0}" type="button">延长</button>`;

    tr.innerHTML = `
      <td data-label="IP地址/段"><strong>${escapeHtml(item.ip)}</strong></td>
      <td data-label="路径前缀">${pathPrefixText}</td>
      <td data-label="原因">${escapeHtml(item.reason || "-")}</td>
      <td data-label="操作者">${escapeHtml(item.banned_by || "admin")}</td>
      <td data-label="封禁时间">${escapeHtml(formatDateTime(new Date(item.banned_at * 1000).toISOString()))}</td>
      <td data-label="到期时间">${expireText}</td>
      <td data-label="操作">
        <div class="table-actions">
          ${extendBtn}
          <button class="table-btn delete" data-action="unban-ip" data-ip="${escapeHtml(item.ip)}" type="button">解封</button>
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

export function openBanModal(options = {}) {
  const mode = options.mode || "add";
  const titleEl = document.getElementById("ban-ip-modal-title");
  titleEl.textContent = mode === "from-log" ? "从日志封禁IP" : "封禁IP";
  setValue("ban_ip_mode", mode);
  setValue("ban_ip_address", options.ip || "");
  setValue("ban_ip_path_prefix", options.pathPrefix || "");
  setValue("ban_ip_reason", options.reason || "");
  const permanentSelect = document.getElementById("ban_ip_permanent");
  if (permanentSelect) permanentSelect.value = "1";
  setValue("ban_ip_duration", "1");
  toggleBanDurationLabel();
  openModal("ban-ip-modal");
}

export function toggleBanDurationLabel() {
  const selectEl = document.getElementById("ban_ip_permanent");
  const isPermanent = selectEl ? selectEl.value === "1" : true;
  const durationLabel = document.getElementById("ban_ip_duration_label");
  const durationInput = document.getElementById("ban_ip_duration");
  if (durationLabel) durationLabel.style.display = isPermanent ? "none" : "";
  if (durationInput) durationInput.required = !isPermanent;
}

export function openBanExtendModal(ip, currentExpireAt) {
  setValue("ban_extend_ip", ip);
  setValue("ban_extend_ip_display", ip);
  let displayText;
  if (!currentExpireAt || currentExpireAt <= 0) {
    displayText = "永久封禁";
  } else {
    const nowSec = Math.floor(Date.now() / 1000);
    const expired = currentExpireAt <= nowSec;
    const dateStr = new Date(currentExpireAt * 1000).toLocaleString("zh-CN", { hour12: false });
    displayText = expired ? `${dateStr}（已过期）` : dateStr;
  }
  setValue("ban_extend_current_expire", displayText);
  setValue("ban_extend_duration", "1");
  openModal("ban-extend-modal");
}

export async function banIpFromLog(ip) {
  openBanModal({
    ip: ip,
    reason: "从日志手动封禁",
    mode: "from-log",
  });
}

// ============ 备份管理 ============

export function formatBackupSize(bytes) {
  if (bytes >= 1024 * 1024) return (bytes / 1024 / 1024).toFixed(2) + " MB";
  if (bytes >= 1024) return (bytes / 1024).toFixed(1) + " KB";
  return bytes + " B";
}

export function formatBackupTime(isoStr) {
  try {
    const d = new Date(isoStr);
    return d.toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });
  } catch {
    return isoStr;
  }
}

export async function loadBackups() {
  try {
    const data = await apiFetch("/_admin/api/backup/list");
    state.backups = data.items || [];
    renderBackupList();
  } catch (error) {
    showToast(error.message, true);
  }
}

export function renderBackupList() {
  const tbody = document.getElementById("backup-table-body");
  const countEl = document.getElementById("backup-total-count");
  if (!tbody) return;
  if (countEl) countEl.textContent = `共 ${state.backups.length} 个备份`;
  if (state.backups.length === 0) {
    tbody.innerHTML = '<tr><td colspan="4">暂无备份</td></tr>';
    return;
  }
  tbody.innerHTML = state.backups.map(b => `
    <tr>
      <td>${escapeHtml(b.filename)}</td>
      <td>${formatBackupSize(b.size)}</td>
      <td>${formatBackupTime(b.created_at)}</td>
      <td class="row-actions">
        <button class="table-btn" data-action="download-backup" data-filename="${escapeHtml(b.filename)}">下载</button>
        <button class="table-btn" data-action="restore-backup" data-filename="${escapeHtml(b.filename)}">恢复</button>
        <button class="table-btn delete" data-action="delete-backup" data-filename="${escapeHtml(b.filename)}">删除</button>
      </td>
    </tr>
  `).join("");
}

export async function createBackup() {
  try {
    const data = await apiFetch("/_admin/api/backup/create", { method: "POST" });
    showToast(`备份已创建: ${data.filename}`);
    await loadBackups();
  } catch (error) {
    showToast(error.message, true);
  }
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
  document.getElementById("restore_backup_filename").value = filename;
  document.getElementById("restore-backup-name").textContent = filename;
  document.getElementById("restore_mode").value = "overwrite";
  openModal("backup-restore-modal");
}

export async function confirmRestoreBackup() {
  const filename = document.getElementById("restore_backup_filename").value;
  const mode = document.getElementById("restore_mode").value;
  if (!filename) return;
  try {
    const formData = new FormData();
    formData.append("restore_mode", mode);
    formData.append("backup_filename", filename);
    const response = await fetch("/_admin/api/backup/restore", {
      method: "POST",
      body: formData,
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "恢复失败");
    showToast(data.message || "恢复成功");
    closeModal("backup-restore-modal");
    await loadBackups();
  } catch (error) {
    showToast(error.message, true);
  }
}

export async function deleteBackup(filename) {
  if (!window.confirm(`确认删除备份文件 ${filename} 吗？`)) return;
  try {
    await apiFetch(`/_admin/api/backup/${encodeURIComponent(filename)}`, { method: "DELETE" });
    showToast("备份已删除");
    await loadBackups();
  } catch (error) {
    showToast(error.message, true);
  }
}

export async function uploadAndRestore() {
  const fileInput = document.getElementById("backup-file-input");
  const mode = document.getElementById("backup_restore_mode").value;
  if (!fileInput.files.length) {
    showToast("请选择数据库文件", true);
    return;
  }
  if (!window.confirm(mode === "overwrite"
    ? "确认用上传的文件覆盖当前数据库吗？此操作不可撤销。"
    : "确认从上传的文件合并导入新规则吗？")) {
    return;
  }
  try {
    const formData = new FormData();
    formData.append("restore_mode", mode);
    formData.append("file", fileInput.files[0]);
    const response = await fetch("/_admin/api/backup/restore", {
      method: "POST",
      body: formData,
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "恢复失败");
    showToast(data.message || "恢复成功");
    fileInput.value = "";
    await loadBackups();
  } catch (error) {
    showToast(error.message, true);
  }
}

// ============ 封禁自动刷新 ============

let _banAutoRefreshTimer = null;
const BAN_AUTO_REFRESH_STORAGE_KEY = "ban_auto_refresh";

export function getBanAutoRefreshConfig() {
  try {
    const raw = localStorage.getItem(BAN_AUTO_REFRESH_STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch (e) {}
  return { enabled: false, interval: 5 };
}

export function saveBanAutoRefreshConfig(cfg) {
  localStorage.setItem(BAN_AUTO_REFRESH_STORAGE_KEY, JSON.stringify(cfg));
}

export function stopBanAutoRefresh() {
  if (_banAutoRefreshTimer !== null) {
    clearInterval(_banAutoRefreshTimer);
    _banAutoRefreshTimer = null;
  }
}

export function startBanAutoRefresh() {
  stopBanAutoRefresh();
  const enabled = getChecked("ban_auto_refresh_enabled");
  if (!enabled) return;
  const interval = Math.max(1, parseInt(getValue("ban_auto_refresh_interval") || "5", 10) || 5);
  saveBanAutoRefreshConfig({ enabled: true, interval });
  _banAutoRefreshTimer = setInterval(() => {
    if (state.activeModule !== "ip-ban-manager") {
      stopBanAutoRefresh();
      return;
    }
    loadBannedIpList().catch((error) => {
      showToast(error.message, true);
      stopBanAutoRefresh();
      setChecked("ban_auto_refresh_enabled", false);
    });
  }, interval * 1000);
}
