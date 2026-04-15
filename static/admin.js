const state = {
  auth: {
    enabled: false,
    authenticated: false,
    username: "",
  },
  rules: [],
  routeGroups: [],
  geoSources: [],
  routeLogs: [],
  routeLogSettings: null,
  activeModule: "overview",
};

const els = {
  authError: document.getElementById("auth-error"),
  authLogoutBtn: document.getElementById("auth-logout-btn"),
  authOverlay: document.getElementById("auth-overlay"),
  toast: document.getElementById("toast"),
  routeGroupOptions: document.getElementById("route-group-options"),
};

const MATCH_STRATEGY_LABELS = {
  ip_whitelist_match: "IP 白名单命中",
  region_match: "地区命中",
  default_route: "默认路由",
  priority_fallback: "优先级回退",
  no_route: "未匹配路由",
};

const RESULT_STATUS_LABELS = {
  forwarded: "转发成功",
  cache_hit: "缓存命中",
  forwarded_client_error: "上游 4xx",
  upstream_error: "上游异常",
  proxy_error: "代理异常",
  no_route: "未匹配路由",
};

const MATCH_DETAIL_LABELS = {
  matched_by_ip_whitelist: "IP 白名单命中",
  ip_whitelist_not_matched: "IP 白名单未命中",
  matched_by_region_filter: "地区规则命中",
  matched_by_region_filter_online_cache: "地区规则命中（在线定位缓存命中）",
  geo_lookup_success_but_no_region_match: "定位成功但地区未命中",
  geo_lookup_failed_or_unavailable: "定位失败或不可用",
  geo_resolver_unavailable: "定位解析器不可用",
  region_matching_enabled_without_regional_rules: "已开启地区匹配但无地区规则",
  region_matching_disabled: "地区匹配已关闭",
  default_rule_selected: "默认规则命中",
  priority_rule_selected: "按优先级命中",
  fallback_to_highest_priority_rule: "回退到最高优先级规则",
  no_matching_rule_found: "未找到匹配规则",
};

function showToast(message, isError = false) {
  els.toast.textContent = message;
  els.toast.style.background = isError
    ? "rgba(143, 43, 22, 0.94)"
    : "rgba(24, 35, 30, 0.92)";
  els.toast.classList.add("visible");
  window.clearTimeout(showToast._timer);
  showToast._timer = window.setTimeout(() => {
    els.toast.classList.remove("visible");
  }, 2600);
}

async function apiFetch(url, options = {}) {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const text = await response.text();
  let data = {};
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { error: text };
    }
  }

  if (response.status === 401) {
    if (!url.includes("/_admin/api/auth/login")) {
      applyAuthState({
        enabled: true,
        authenticated: false,
        username: "",
      });
      setAuthError(data.error || "登录状态已失效，请重新登录。");
    }
    throw new Error(data.error || "未登录或登录已失效。");
  }

  if (!response.ok) {
    throw new Error(data.error || data.message || text || "请求失败");
  }
  return data;
}

function setAuthError(message = "") {
  if (!els.authError) return;
  const normalized = String(message || "").trim();
  els.authError.hidden = !normalized;
  els.authError.textContent = normalized;
}

function applyAuthState(auth) {
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

function setValue(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.value = value ?? "";
  }
}

function setChecked(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.checked = Boolean(value);
  }
}

function getValue(id) {
  const el = document.getElementById(id);
  return el ? el.value : "";
}

function getChecked(id) {
  const el = document.getElementById(id);
  return Boolean(el && el.checked);
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = value ?? "-";
  }
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!bytes) return "0 B";
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(2)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  return `${bytes} B`;
}

function normalizeRequestHost(value) {
  const normalizeSingleHost = (input) => {
    const text = String(input || "").trim().toLowerCase();
    if (!text) return "";
    if (text.startsWith("[") && text.includes("]")) {
      return text.slice(1, text.indexOf("]")).trim();
    }
    if (text.includes(":") && text.indexOf(":") === text.lastIndexOf(":")) {
      return text.split(":", 1)[0].trim();
    }
    return text;
  };

  const hostParts = String(value || "").split(/[,\uFF0C]/);
  const normalizedHosts = [];
  const seenHosts = new Set();
  hostParts.forEach((part) => {
    const normalized = normalizeSingleHost(part);
    if (!normalized || seenHosts.has(normalized)) {
      return;
    }
    seenHosts.add(normalized);
    normalizedHosts.push(normalized);
  });

  return normalizedHosts.join(",");
}

function formatRequestHostLabel(requestHost) {
  if (!requestHost) {
    return "*";
  }
  return String(requestHost)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .join(", ");
}

function isSameRouteGroup(group, pathPrefix, requestHost) {
  return (
    group.path_prefix === pathPrefix &&
    normalizeRequestHost(group.request_host) === normalizeRequestHost(requestHost)
  );
}

function findRouteGroup(pathPrefix, requestHost) {
  return state.routeGroups.find((item) => isSameRouteGroup(item, pathPrefix, requestHost));
}

function toIsoDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toISOString().replace("Z", "+00:00");
}

function formatMatchStrategy(value) {
  return MATCH_STRATEGY_LABELS[value] || value || "-";
}

function formatResultStatus(value) {
  return RESULT_STATUS_LABELS[value] || value || "-";
}

function formatMatchDetail(value) {
  return MATCH_DETAIL_LABELS[value] || value || "-";
}

function setActiveModule(moduleName) {
  state.activeModule = moduleName;
  document.querySelectorAll(".module-btn").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.moduleTarget === moduleName);
  });
  document.querySelectorAll(".module-panel").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.modulePanel === moduleName);
  });
}

function focusField(id) {
  const el = document.getElementById(id);
  if (el) {
    window.requestAnimationFrame(() => el.focus());
  }
}

function scrollToElement(id) {
  const el = document.getElementById(id);
  if (el) {
    el.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function renderSummary(summary) {
  setText("metric-db-path", summary.database_path || "-");
  setText("metric-total-rules", summary.total_rules ?? 0);
  setText("metric-enabled-rules", summary.enabled_rules ?? 0);
  setText("metric-route-groups", summary.route_group_count ?? 0);
  setText("metric-region-groups", summary.region_enabled_group_count ?? 0);
}

function getRulesForGroup(pathPrefix, requestHost = "") {
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

function renderRouteGroupOptions() {
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

function renderRouteGroups(routeGroups) {
  state.routeGroups = routeGroups;
  renderRouteGroupOptions();

  const container = document.getElementById("route-group-cards");
  container.innerHTML = "";

  if (!routeGroups.length) {
    container.innerHTML = `
      <div class="empty-group-state">
        <p>还没有路径前缀。先在右侧新增一个前缀，再为它添加转发规则。</p>
      </div>
    `;
    return;
  }

  routeGroups.forEach((group) => {
    const normalizedGroupHost = normalizeRequestHost(group.request_host);
    const hostLabel = formatRequestHostLabel(normalizedGroupHost);
    const groupRules = getRulesForGroup(group.path_prefix, normalizedGroupHost);
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
                <th>地区条件</th>
                <th>状态</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              ${groupRules
                .map((rule) => {
                  const statusBadges = [
                    rule.is_default ? '<span class="badge badge-default">默认</span>' : "",
                    !rule.enabled ? '<span class="badge badge-disabled">禁用</span>' : "",
                  ].join("");
                  const conditions = [];
                  if (rule.ip_whitelist) {
                    conditions.push(`IP: ${escapeHtml(rule.ip_whitelist)}`);
                  }
                  if (rule.region_filters) {
                    conditions.push(`地区: ${escapeHtml(rule.region_filters)}`);
                  }
                  return `
                    <tr>
                      <td>
                        <strong>${escapeHtml(rule.name || "(未命名规则)")}</strong>
                        <div>${statusBadges}</div>
                      </td>
                      <td>${escapeHtml(rule.target_url)}</td>
                      <td>${conditions.length ? conditions.join("<br>") : "默认"}</td>
                      <td>${rule.enabled ? "启用" : "停用"}</td>
                      <td>
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
    card.innerHTML = `
      <div class="prefix-card-head">
        <div class="prefix-card-title">
          <h3>${escapeHtml(group.path_prefix)}</h3>
          <p class="hint">域名: <code>${escapeHtml(hostLabel)}</code></p>
          <p class="hint">${escapeHtml(group.notes || "未填写备注")}</p>
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

function resetRouteGroupForm() {
  setValue("route_group_old_path_prefix", "");
  setValue("route_group_old_request_host", "");
  setValue("route_group_path_prefix", "");
  setValue("route_group_request_host", "");
  setValue("route_group_notes", "");
  document.getElementById("route-group-form-title").textContent = "新增路径前缀";
}

function fillRouteGroupForm(group) {
  setValue("route_group_old_path_prefix", group.path_prefix);
  setValue("route_group_old_request_host", normalizeRequestHost(group.request_host));
  setValue("route_group_path_prefix", group.path_prefix);
  setValue("route_group_request_host", normalizeRequestHost(group.request_host));
  setValue("route_group_notes", group.notes || "");
  const hostLabel = formatRequestHostLabel(normalizeRequestHost(group.request_host));
  document.getElementById("route-group-form-title").textContent = `编辑路径前缀 ${group.path_prefix} @ ${hostLabel}`;
}

function collectRouteGroupForm() {
  return {
    old_path_prefix: getValue("route_group_old_path_prefix"),
    old_request_host: normalizeRequestHost(getValue("route_group_old_request_host")),
    path_prefix: getValue("route_group_path_prefix"),
    request_host: normalizeRequestHost(getValue("route_group_request_host")),
    notes: getValue("route_group_notes"),
  };
}

function fillGeoConfig(geo) {
  setChecked("geo_enabled", geo.enabled);
  setValue("geo_online_cache_ttl_seconds", geo.online_cache_ttl_seconds ?? 120);
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
  setValue("geo_offline_refresh_interval_hours", geo.offline?.refresh_interval_hours ?? 24);
  renderOfflineStatus(geo.offline || {});
  resetOfflineGeoTestResult();
}

function renderGeoSources() {
  const tbody = document.getElementById("geo-sources-table-body");
  tbody.innerHTML = "";

  if (!state.geoSources.length) {
    tbody.innerHTML = '<tr><td colspan="6">暂无在线定位源，将直接使用离线库或默认规则。</td></tr>';
    return;
  }

  state.geoSources.forEach((source, index) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><strong>${escapeHtml(source.name || `source-${index + 1}`)}</strong></td>
      <td>${escapeHtml(source.url)}</td>
      <td>${source.weight}</td>
      <td>${escapeHtml(source.method)} / ${escapeHtml(source.request_location)}</td>
      <td>${source.enabled ? "启用" : "停用"}</td>
      <td>
        <div class="table-actions">
          <button class="table-btn" data-action="edit-geo-source" data-index="${index}" type="button">编辑</button>
          <button class="table-btn delete" data-action="delete-geo-source" data-index="${index}" type="button">删除</button>
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

function resetGeoSourceForm() {
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

function fillGeoSourceForm(source, index) {
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

function collectGeoSourceForm() {
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

function resetGeoSourceTestResult(message = "输入测试 IP 后，可查看测试结果和区域信息。") {
  const container = document.getElementById("geo-source-test-result");
  container.classList.add("is-empty");
  container.innerHTML = `<p class="test-result-placeholder">${escapeHtml(message)}</p>`;
}

function renderGeoSourceTestResult(result) {
  const container = document.getElementById("geo-source-test-result");
  const location = result.location || {};
  const sourceName = result.provider || "-";
  const testIp = location.ip || getValue("geo_source_test_ip").trim() || "-";
  const statusText = result.success ? "测试成功" : "测试失败";
  const statusClass = result.success ? "success" : "failed";
  const rawJson =
    location.raw && Object.keys(location.raw).length
      ? `
        <details class="test-result-raw">
          <summary>查看接口原始返回</summary>
          <pre>${escapeHtml(JSON.stringify(location.raw, null, 2))}</pre>
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

function renderOfflineStatus(offline) {
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

function resetOfflineGeoTestResult(message = "输入测试 IP 后，可以直接查看离线库定位结果。") {
  const container = document.getElementById("geo-offline-test-result");
  container.classList.add("is-empty");
  container.innerHTML = `<p class="test-result-placeholder">${escapeHtml(message)}</p>`;
}

function renderOfflineGeoTestResult(result) {
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

function renderRouteLogSettings(settings) {
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

function renderRouteLogs(payload) {
  state.routeLogs = Array.isArray(payload.items) ? payload.items : [];
  setText("route-log-total-count", `共 ${payload.total ?? state.routeLogs.length} 条`);

  const tbody = document.getElementById("route-logs-table-body");
  tbody.innerHTML = "";
  setChecked("route-log-select-all", false);

  if (!state.routeLogs.length) {
    tbody.innerHTML = '<tr><td colspan="8">当前没有匹配到规则转发日志。</td></tr>';
    return;
  }

  state.routeLogs.forEach((log) => {
    const tr = document.createElement("tr");
    const requestMethod = escapeHtml(log.request_method || "-");
    const requestPath = escapeHtml(log.request_path || "-");
    const requestQuery = escapeHtml(log.request_query_string || "");
    const originalClientIp = escapeHtml(log.original_client_ip || "-");
    const clientIp = escapeHtml(log.client_ip || "-");
    const pathPrefix = escapeHtml(log.path_prefix || "-");
    const ruleName = escapeHtml(log.rule_name || "-");
    const geoSummary = escapeHtml(log.geo_summary || "-");
    const matchedRegion = escapeHtml(log.matched_region || "-");
    const geoSource = escapeHtml(log.geo_source || "-");
    const matchStrategy = escapeHtml(formatMatchStrategy(log.match_strategy));
    const matchDetail = escapeHtml(formatMatchDetail(log.match_detail));
    const matchedWhitelist = escapeHtml(log.matched_ip_whitelist || "-");
    const configuredWhitelist = escapeHtml(log.configured_ip_whitelist || "-");
    const configuredRegions = escapeHtml(log.configured_regions || "-");
    const targetUrl = escapeHtml(log.target_url || "-");
    const upstreamStatus = escapeHtml(String(log.upstream_status || 0));
    const cacheStatus = escapeHtml(log.cache_status || "-");
    const resultStatus = escapeHtml(formatResultStatus(log.result_status));
    const durationText = escapeHtml(`${log.operation_duration_ms || 0} ms`);
    const createdAt = escapeHtml(formatDateTime(log.created_at));

    tr.innerHTML = `
      <td><input class="route-log-checkbox" data-id="${log.id}" type="checkbox" /></td>
      <td class="route-log-time-cell">
        <strong>${createdAt}</strong>
        <div class="hint">${durationText}</div>
      </td>
      <td class="route-log-request-cell">
        <strong>${requestMethod}</strong>
        <div class="route-log-request-path" title="${requestPath}">${requestPath}</div>
        ${requestQuery ? `<div class="hint route-log-query" title="${requestQuery}">${requestQuery}</div>` : ""}
        <div class="hint">原始 IP: ${originalClientIp}</div>
        <div class="hint">匹配 IP: ${clientIp}</div>
      </td>
      <td class="route-log-prefix-cell">
        <strong>${pathPrefix}</strong>
        <div class="hint">规则: ${ruleName}</div>
      </td>
      <td class="route-log-geo-cell">
        <strong>${geoSummary}</strong>
        <div class="hint">命中地区: ${matchedRegion}</div>
        <div class="hint">定位源: ${geoSource}</div>
      </td>
      <td class="route-log-match-cell">
        <strong>${matchStrategy}</strong>
        <div>${matchDetail}</div>
        <div class="hint">命中白名单: ${matchedWhitelist}</div>
        <div class="hint">规则白名单: ${configuredWhitelist}</div>
        <div class="hint">规则地区: ${configuredRegions}</div>
      </td>
      <td class="route-log-result-cell">
        <strong class="route-log-target-url" title="${targetUrl}">${targetUrl}</strong>
        <div class="hint">状态: ${upstreamStatus}</div>
        <div class="hint">缓存: ${cacheStatus}</div>
        <div class="hint">结果: ${resultStatus}</div>
      </td>
      <td class="route-log-action-cell">
        <div class="table-actions">
          <button class="table-btn delete" data-action="delete-route-log" data-id="${log.id}" type="button">删除</button>
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

function collectRouteLogFilters() {
  return {
    keyword: getValue("log_keyword").trim(),
    path_prefix: getValue("log_path_prefix").trim(),
    match_strategy: getValue("log_match_strategy"),
    result_status: getValue("log_result_status"),
    date_from: toIsoDateTime(getValue("log_date_from")),
    date_to: toIsoDateTime(getValue("log_date_to")),
    limit: Number(getValue("log_limit") || 100),
  };
}

function buildRouteLogQuery(filters) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value === "" || value === null || value === undefined) {
      return;
    }
    params.set(key, String(value));
  });
  return params.toString();
}

async function loadRouteLogSettings() {
  const data = await apiFetch("/_admin/api/log-settings");
  renderRouteLogSettings(data || {});
}

async function loadRouteLogs() {
  const filters = collectRouteLogFilters();
  const query = buildRouteLogQuery(filters);
  const payload = await apiFetch(`/_admin/api/logs${query ? `?${query}` : ""}`);
  renderRouteLogs(payload || { items: [], total: 0 });
}

async function refreshRouteLogModule() {
  await Promise.all([loadRouteLogSettings(), loadRouteLogs()]);
}

function renderRules(rules) {
  state.rules = rules;
  const tbody = document.getElementById("rules-table-body");
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
      !rule.enabled ? '<span class="badge badge-disabled">禁用</span>' : "",
    ].join("");
    const conditions = [];
    if (rule.ip_whitelist) {
      conditions.push(`IP: ${escapeHtml(rule.ip_whitelist)}`);
    }
    if (rule.region_filters) {
      conditions.push(`地区: ${escapeHtml(rule.region_filters)}`);
    }

    tr.innerHTML = `
      <td>
        <strong>${escapeHtml(rule.name || "(未命名规则)")}</strong>
        <div>${statusBadges}</div>
      </td>
      <td>
        <div>${escapeHtml(rule.path_prefix)}</div>
        <div class="hint">域名: ${escapeHtml(formatRequestHostLabel(requestHost))}</div>
      </td>
      <td>${escapeHtml(rule.target_url)}</td>
      <td>${conditions.length ? conditions.join("<br>") : "默认"}</td>
      <td>${rule.enabled ? "启用" : "停用"}</td>
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

function resetRuleForm() {
  setValue("rule_id", "");
  setValue("rule_name", "");
  setValue("rule_path_prefix", "");
  setValue("rule_request_host", "");
  setValue("rule_target_url", "");
  setValue("rule_ip_whitelist", "");
  setValue("rule_region_filters", "");
  setValue("rule_priority", "0");
  setValue("rule_timeout", "30");
  setValue("rule_max_redirects", "10");
  setValue("rule_retry_times", "3");
  setValue("rule_notes", "");
  setChecked("rule_enabled", true);
  setChecked("rule_is_default", false);
  setChecked("rule_strip_prefix", false);
  setChecked("rule_follow_redirects", true);
  setChecked("rule_enable_streaming", true);
  document.getElementById("rule-form-title").textContent = "新增规则";
}

function fillRuleForm(rule) {
  setValue("rule_id", rule.id);
  setValue("rule_name", rule.name || "");
  setValue("rule_path_prefix", rule.path_prefix || "");
  setValue("rule_request_host", normalizeRequestHost(rule.request_host));
  setValue("rule_target_url", rule.target_url || "");
  setValue("rule_ip_whitelist", rule.ip_whitelist || "");
  setValue("rule_region_filters", rule.region_filters || "");
  setValue("rule_priority", rule.priority ?? 0);
  setValue("rule_timeout", rule.timeout ?? 30);
  setValue("rule_max_redirects", rule.max_redirects ?? 10);
  setValue("rule_retry_times", rule.retry_times ?? 3);
  setValue("rule_notes", rule.notes || "");
  setChecked("rule_enabled", rule.enabled);
  setChecked("rule_is_default", rule.is_default);
  setChecked("rule_strip_prefix", rule.strip_prefix);
  setChecked("rule_follow_redirects", rule.follow_redirects !== false);
  setChecked("rule_enable_streaming", rule.enable_streaming);
  document.getElementById("rule-form-title").textContent = `编辑规则 #${rule.id}`;
}

function collectRuleForm() {
  return {
    id: getValue("rule_id") || undefined,
    name: getValue("rule_name"),
    path_prefix: getValue("rule_path_prefix"),
    request_host: normalizeRequestHost(getValue("rule_request_host")),
    target_url: getValue("rule_target_url"),
    ip_whitelist: getValue("rule_ip_whitelist"),
    region_filters: getValue("rule_region_filters"),
    priority: Number(getValue("rule_priority") || 0),
    timeout: Number(getValue("rule_timeout") || 30),
    max_redirects: Number(getValue("rule_max_redirects") || 10),
    retry_times: Number(getValue("rule_retry_times") || 3),
    notes: getValue("rule_notes"),
    enabled: getChecked("rule_enabled"),
    is_default: getChecked("rule_is_default"),
    strip_prefix: getChecked("rule_strip_prefix"),
    follow_redirects: getChecked("rule_follow_redirects"),
    enable_streaming: getChecked("rule_enable_streaming"),
  };
}

function prepareRuleForGroup(pathPrefix, requestHost = "") {
  resetRuleForm();
  setValue("rule_path_prefix", pathPrefix);
  setValue("rule_request_host", normalizeRequestHost(requestHost));
  setActiveModule("rules");
  scrollToElement("rule-form");
  focusField("rule_name");
}

function openRuleEditor(ruleId) {
  const rule = state.rules.find((item) => item.id === Number(ruleId));
  if (!rule) {
    showToast(`未找到规则 #${ruleId}`, true);
    return;
  }
  fillRuleForm(rule);
  setActiveModule("rules");
  scrollToElement("rule-form");
  focusField("rule_name");
}

async function removeRule(ruleId) {
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

async function updateGroupRegionSwitch(pathPrefix, requestHost, enabled) {
  const group = findRouteGroup(pathPrefix, requestHost);
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

async function loadDashboard() {
  const data = await apiFetch("/_admin/api/bootstrap");
  renderSummary(data.summary || {});
  renderRules(data.rules || []);
  renderRouteGroups(data.route_groups || []);
  fillGeoConfig(data.geoip || {});
}

function buildGeoSettingsPayload() {
  return {
    enabled: getChecked("geo_enabled"),
    online_cache_ttl_seconds: Number(getValue("geo_online_cache_ttl_seconds") || 0),
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
      refresh_interval_hours: Number(getValue("geo_offline_refresh_interval_hours") || 24),
    },
  };
}

async function persistGeoSettings(successMessage = "IP 定位配置已保存。") {
  await apiFetch("/_admin/api/geoip", {
    method: "PUT",
    body: JSON.stringify(buildGeoSettingsPayload()),
  });
  await loadDashboard();
  showToast(successMessage);
}

document.querySelectorAll(".module-btn").forEach((button) => {
  button.addEventListener("click", () => {
    setActiveModule(button.dataset.moduleTarget);
    if (button.dataset.moduleTarget === "logs") {
      refreshRouteLogModule().catch((error) => {
        showToast(error.message, true);
      });
    }
  });
});

document.getElementById("route-group-form").addEventListener("submit", async (event) => {
  event.preventDefault();
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
});

document.getElementById("geo-online-cache-clear-btn").addEventListener("click", async () => {
  const button = document.getElementById("geo-online-cache-clear-btn");
  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "清理中...";
  try {
    const result = await apiFetch("/_admin/api/geoip/cache/clear", {
      method: "POST",
      body: JSON.stringify({}),
    });
    showToast(result.message || "在线定位缓存已清空。");
  } catch (error) {
    showToast(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = originalText;
  }
});

document.getElementById("route-group-reset-btn").addEventListener("click", () => {
  resetRouteGroupForm();
});

document.getElementById("route-group-cards").addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;

  const action = button.dataset.action;
  const pathPrefix = button.dataset.pathPrefix;
  const requestHost = normalizeRequestHost(button.dataset.requestHost);
  const ruleId = button.dataset.id;
  const group = findRouteGroup(pathPrefix, requestHost);

  if (action === "create-rule-for-group") {
    prepareRuleForGroup(pathPrefix, requestHost);
    return;
  }

  if (action === "edit-group") {
    if (!group) return;
    fillRouteGroupForm(group);
    scrollToElement("route-group-form");
    focusField("route_group_path_prefix");
    return;
  }

  if (action === "delete-group") {
    if (!group) return;
    if (!window.confirm(`确认删除路径前缀 ${pathPrefix} @ ${formatRequestHostLabel(requestHost)} 吗？`)) {
      return;
    }
    try {
      await apiFetch("/_admin/api/route-groups", {
        method: "DELETE",
        body: JSON.stringify({
          path_prefix: pathPrefix,
          request_host: requestHost,
        }),
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

  if (action === "delete-rule-from-group") {
    await removeRule(ruleId);
  }
});

document.getElementById("route-group-cards").addEventListener("change", async (event) => {
  const checkbox = event.target.closest('input[data-action="toggle-group-region"]');
  if (!checkbox) return;

  const pathPrefix = checkbox.dataset.pathPrefix;
  const requestHost = normalizeRequestHost(checkbox.dataset.requestHost);
  const nextValue = checkbox.checked;

  try {
    await updateGroupRegionSwitch(pathPrefix, requestHost, nextValue);
    await loadDashboard();
    showToast(
      `${pathPrefix} @ ${formatRequestHostLabel(requestHost)} 的地区匹配已${nextValue ? "开启" : "关闭"}。`,
    );
  } catch (error) {
    checkbox.checked = !nextValue;
    showToast(error.message, true);
  }
});

document.getElementById("route-log-filter-form").addEventListener("submit", async (event) => {
  event.preventDefault();
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
  setValue("log_match_strategy", "");
  setValue("log_result_status", "");
  setValue("log_date_from", "");
  setValue("log_date_to", "");
  setValue("log_limit", "100");
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
      body: JSON.stringify({
        retention_days: Number(getValue("log_retention_days") || 30),
      }),
    });
    await refreshRouteLogModule();
    showToast("日志保留策略已保存。");
  } catch (error) {
    showToast(error.message, true);
  }
});

document.getElementById("route-log-select-all").addEventListener("change", (event) => {
  const checked = Boolean(event.target.checked);
  document.querySelectorAll(".route-log-checkbox").forEach((checkbox) => {
    checkbox.checked = checked;
  });
});

document.getElementById("route-log-delete-selected-btn").addEventListener("click", async () => {
  const ids = Array.from(document.querySelectorAll(".route-log-checkbox:checked"))
    .map((checkbox) => Number(checkbox.dataset.id))
    .filter((value) => Number.isInteger(value) && value > 0);
  if (!ids.length) {
    showToast("请先选择要删除的日志", true);
    return;
  }
  if (!window.confirm(`确认删除选中的 ${ids.length} 条日志吗？`)) {
    return;
  }
  try {
    await apiFetch("/_admin/api/logs", {
      method: "DELETE",
      body: JSON.stringify({ ids }),
    });
    await refreshRouteLogModule();
    showToast("选中日志已删除。");
  } catch (error) {
    showToast(error.message, true);
  }
});

document.getElementById("route-log-delete-all-btn").addEventListener("click", async () => {
  if (!window.confirm("确认清空所有规则转发日志吗？")) {
    return;
  }
  try {
    await apiFetch("/_admin/api/logs", {
      method: "DELETE",
      body: JSON.stringify({ delete_all: true }),
    });
    await refreshRouteLogModule();
    showToast("规则转发日志已清空。");
  } catch (error) {
    showToast(error.message, true);
  }
});

document.getElementById("route-logs-table-body").addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  if (button.dataset.action !== "delete-route-log") return;

  const logId = Number(button.dataset.id);
  if (!window.confirm(`确认删除日志 #${logId} 吗？`)) {
    return;
  }
  try {
    await apiFetch("/_admin/api/logs", {
      method: "DELETE",
      body: JSON.stringify({ ids: [logId] }),
    });
    await refreshRouteLogModule();
    showToast("日志已删除。");
  } catch (error) {
    showToast(error.message, true);
  }
});

document.getElementById("geo-source-save-btn").addEventListener("click", async () => {
  const payload = collectGeoSourceForm();
  if (!payload.url) {
    showToast("在线源接口地址不能为空", true);
    return;
  }

  const button = document.getElementById("geo-source-save-btn");
  const originalText = button.textContent;
  const indexText = getValue("geo_source_id");
  const index = indexText === "" ? null : Number(indexText);
  const previousSources = state.geoSources.map((source) => ({ ...source }));
  const isEdit = index !== null && Number.isInteger(index) && index >= 0;

  if (isEdit) {
    state.geoSources[index] = payload;
  } else {
    state.geoSources.push(payload);
  }

  renderGeoSources();
  button.disabled = true;
  button.textContent = "保存中...";

  try {
    await persistGeoSettings(isEdit ? "在线源已更新。" : "在线源已新增。");
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

document.getElementById("geo-source-reset-btn").addEventListener("click", () => {
  resetGeoSourceForm();
});

document.getElementById("geo-source-test-btn").addEventListener("click", async () => {
  const ip = getValue("geo_source_test_ip").trim();
  const source = collectGeoSourceForm();
  const button = document.getElementById("geo-source-test-btn");
  const originalText = button.textContent;

  if (!ip) {
    showToast("测试 IP 不能为空", true);
    return;
  }

  if (!source.url) {
    showToast("请先填写当前在线源的接口地址", true);
    return;
  }

  button.disabled = true;
  button.textContent = "测试中...";
  resetGeoSourceTestResult("正在请求在线定位源，请稍候...");

  try {
    const result = await apiFetch("/_admin/api/geoip/test", {
      method: "POST",
      body: JSON.stringify({
        ip,
        source,
      }),
    });
    renderGeoSourceTestResult(result);
    showToast(
      result.success ? "在线源测试完成。" : "在线源测试失败。",
      !result.success,
    );
  } catch (error) {
    renderGeoSourceTestResult({
      success: false,
      stage: "online",
      provider: source.name || source.url,
      message: error.message,
      location: null,
    });
    showToast(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = originalText;
  }
});

document.getElementById("geo-offline-sync-btn").addEventListener("click", async () => {
  const button = document.getElementById("geo-offline-sync-btn");
  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "同步中...";

  try {
    const result = await apiFetch("/_admin/api/geoip/offline/sync", {
      method: "POST",
      body: JSON.stringify({
        geoip: buildGeoSettingsPayload(),
      }),
    });
    await loadDashboard();
    showToast(result.message || "离线 GeoIP 同步完成。");
  } catch (error) {
    showToast(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = originalText;
  }
});

document.getElementById("geo-offline-rollback-btn").addEventListener("click", async () => {
  if (!window.confirm("确认回滚到离线 GeoIP 备份吗？")) {
    return;
  }

  const button = document.getElementById("geo-offline-rollback-btn");
  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "回滚中...";

  try {
    const result = await apiFetch("/_admin/api/geoip/offline/rollback", {
      method: "POST",
      body: JSON.stringify({}),
    });
    await loadDashboard();
    showToast(result.message || "离线 GeoIP 回滚完成。");
  } catch (error) {
    showToast(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = originalText;
  }
});

document.getElementById("geo-offline-test-btn").addEventListener("click", async () => {
  const ip = getValue("geo_offline_test_ip").trim();
  const button = document.getElementById("geo-offline-test-btn");
  const originalText = button.textContent;

  if (!ip) {
    showToast("离线库测试 IP 不能为空", true);
    return;
  }

  button.disabled = true;
  button.textContent = "测试中...";
  resetOfflineGeoTestResult("正在使用离线库进行定位，请稍候...");

  try {
    const result = await apiFetch("/_admin/api/geoip/offline/test", {
      method: "POST",
      body: JSON.stringify({
        ip,
        geoip: buildGeoSettingsPayload(),
      }),
    });
    renderOfflineGeoTestResult(result);
    showToast(
      result.success ? "离线定位测试完成。" : "离线定位测试失败。",
      !result.success,
    );
  } catch (error) {
    renderOfflineGeoTestResult({
      success: false,
      provider: "offline_mmdb",
      message: error.message,
      location: null,
    });
    showToast(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = originalText;
  }
});

document.getElementById("geo-sources-table-body").addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;

  const index = Number(button.dataset.index);
  const action = button.dataset.action;
  const source = state.geoSources[index];
  if (!source) return;

  if (action === "edit-geo-source") {
    fillGeoSourceForm(source, index);
    scrollToElement("geoip-form");
    focusField("geo_source_name");
    return;
  }

  if (action === "delete-geo-source") {
    if (!window.confirm(`确认删除在线源 ${source.name || source.url} 吗？`)) {
      return;
    }
    const previousSources = state.geoSources.map((item) => ({ ...item }));
    state.geoSources.splice(index, 1);
    renderGeoSources();
    try {
      await persistGeoSettings("在线源已删除");
      resetGeoSourceForm();
    } catch (error) {
      state.geoSources = previousSources;
      renderGeoSources();
      showToast(error.message, true);
    }
  }
});

document.getElementById("geoip-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await persistGeoSettings();
  } catch (error) {
    showToast(error.message, true);
  }
});

document.getElementById("rule-form").addEventListener("submit", async (event) => {
  event.preventDefault();
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
});

document.getElementById("rule-reset-btn").addEventListener("click", () => {
  resetRuleForm();
});

document.getElementById("rules-table-body").addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;

  const ruleId = Number(button.dataset.id);
  const action = button.dataset.action;

  if (action === "edit-rule") {
    openRuleEditor(ruleId);
    return;
  }

  if (action === "delete-rule") {
    await removeRule(ruleId);
  }
});

async function loadAuthStatus() {
  const auth = await apiFetch("/_admin/api/auth/status");
  applyAuthState(auth || {});
  return auth || {};
}

async function submitLogin() {
  const username = getValue("auth_username").trim();
  const password = getValue("auth_password");
  setAuthError("");
  const result = await apiFetch("/_admin/api/auth/login", {
    method: "POST",
    body: JSON.stringify({
      username,
      password,
    }),
  });
  applyAuthState(result || {});
  setValue("auth_password", "");
  return result;
}

async function performLogout() {
  await apiFetch("/_admin/api/auth/logout", {
    method: "POST",
    body: JSON.stringify({}),
  });
  applyAuthState({
    enabled: state.auth.enabled,
    authenticated: false,
    username: "",
  });
  setValue("auth_password", "");
}

document.getElementById("auth-login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await submitLogin();
    await loadDashboard();
    showToast("后台登录成功");
  } catch (error) {
    setAuthError(error.message);
  }
});

els.authLogoutBtn?.addEventListener("click", async () => {
  try {
    await performLogout();
    setAuthError("已退出登录。");
    showToast("已退出登录。");
  } catch (error) {
    showToast(error.message, true);
  }
});

window.addEventListener("DOMContentLoaded", async () => {
  setActiveModule("overview");
  resetRouteGroupForm();
  resetGeoSourceForm();
  resetRuleForm();
  try {
    const auth = await loadAuthStatus();
    if (!auth.enabled || auth.authenticated) {
      await loadDashboard();
    } else {
      focusField("auth_username");
    }
  } catch (error) {
    showToast(error.message, true);
  }
});


