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
  bannedIps: [],
  logFiles: [],
  backups: [],
  activeModule: "overview",
  logCurrentPage: 1,
  logTotalPages: 1,
  logPageSize: 50,
  banCurrentPage: 1,
  banTotalPages: 1,
  banPageSize: 20,
  routeFilter: { keyword: "", status: "", isDefault: "" },
  backups: [],
  logAutoScroll: true,
  logLastScrollTop: 0,
  logLastLineCount: 0,
};

function openModal(modalId) {
  const modal = document.getElementById(modalId);
  if (modal) {
    modal.hidden = false;
    document.body.style.overflow = 'hidden';
  }
}

function closeModal(modalId) {
  const modal = document.getElementById(modalId);
  if (modal) {
    modal.hidden = true;
    document.body.style.overflow = '';
  }
}

function closeAllModals() {
  document.querySelectorAll('.modal-overlay').forEach(modal => {
    modal.hidden = true;
  });
  document.body.style.overflow = '';
}

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

const CACHE_STATUS_LABELS = {
  BYPASS: "未命中",
  HIT_REDIRECT: "缓存命中(重定向)",
  HIT_STREAMING: "缓存命中(流式)",
  BANNED: "已封禁",
};

const MATCH_DETAIL_LABELS = {
  matched_by_ip_whitelist: "IP 白名单命中",
  ip_whitelist_not_matched: "IP 白名单未命中",
  matched_by_region_filter: "地区规则命中",
  matched_by_region_filter_online_cache: "地区规则命中（在线定位缓存命中）",
  geo_lookup_success_online_cache_but_no_region_match: "定位成功（在线定位缓存命中）但地区未命中",
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

// ===== RSA 密码加密功能 =====
let cachedPublicKey = null;

async function getPublicKey() {
  if (cachedPublicKey) return cachedPublicKey;
  try {
    const result = await apiFetch("/_admin/api/auth/public-key");
    if (result && result.public_key) {
      cachedPublicKey = result.public_key;
      return cachedPublicKey;
    }
  } catch (e) {
    console.error("获取公钥失败:", e);
  }
  return null;
}

async function encryptPassword(password, publicKeyPem) {
  // 将 PEM 格式公钥转换为 ArrayBuffer
  const pemHeader = "-----BEGIN PUBLIC KEY-----";
  const pemFooter = "-----END PUBLIC KEY-----";
  const pemContents = publicKeyPem
    .replace(pemHeader, "")
    .replace(pemFooter, "")
    .replace(/\s/g, "");
  
  const binaryString = atob(pemContents);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }
  
  const publicKey = await crypto.subtle.importKey(
    "spki",
    bytes.buffer,
    { name: "RSA-OAEP", hash: "SHA-256" },
    false,
    ["encrypt"]
  );
  
  const encoded = new TextEncoder().encode(password);
  const encrypted = await crypto.subtle.encrypt(
    { name: "RSA-OAEP" },
    publicKey,
    encoded
  );
  
  // 转换为 Base64
  return btoa(String.fromCharCode(...new Uint8Array(encrypted)));
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

function getNonNegativeIntValue(id, fallback = 0) {
  const raw = String(getValue(id) || "").trim();
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) {
    return Math.max(0, Number.parseInt(String(fallback), 10) || 0);
  }
  return Math.max(0, parsed);
}

function getPositiveIntValue(id, fallback = 1) {
  const raw = String(getValue(id) || "").trim();
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) {
    return Math.max(1, Number.parseInt(String(fallback), 10) || 1);
  }
  return Math.max(1, parsed);
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

// URL Tooltip 和复制功能
let urlTooltip = null;

function showUrlTooltip(e, url) {
  if (!url || url === "-") return;
  
  hideUrlTooltip();
  
  urlTooltip = document.createElement("div");
  urlTooltip.className = "url-tooltip";
  urlTooltip.textContent = url;
  document.body.appendChild(urlTooltip);
  
  const rect = e.target.getBoundingClientRect();
  let top = rect.bottom + 8;
  let left = rect.left;
  
  // 防止超出右边界
  if (left + 500 > window.innerWidth) {
    left = window.innerWidth - 510;
  }
  
  // 防止超出下边界
  if (top + 200 > window.innerHeight) {
    top = rect.top - 208;
  }
  
  urlTooltip.style.top = top + "px";
  urlTooltip.style.left = left + "px";
}

function hideUrlTooltip() {
  if (urlTooltip) {
    urlTooltip.remove();
    urlTooltip = null;
  }
}

function copyToClipboard(text) {
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

function showCopyToast(message) {
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

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatRemainTime(seconds) {
  const sec = Math.max(0, Math.floor(seconds));
  if (sec === 0) return "已到期";
  const days = Math.floor(sec / 86400);
  const hours = Math.floor((sec % 86400) / 3600);
  const minutes = Math.floor((sec % 3600) / 60);
  const secs = sec % 60;
  const parts = [];
  if (days > 0) parts.push(`${days}天`);
  if (hours > 0) parts.push(`${hours}小时`);
  if (minutes > 0) parts.push(`${minutes}分`);
  if (secs > 0 && days === 0 && hours === 0) parts.push(`${secs}秒`);
  return parts.join("") || "不足1秒";
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

function formatTypeLabel(value, labels) {
  const normalized = String(value || "").trim();
  if (!normalized) return "-";
  if (labels[normalized]) return labels[normalized];
  if (/^[a-z0-9_:-]+$/i.test(normalized)) {
    return `未映射类型（${normalized}）`;
  }
  return normalized;
}

function formatMatchStrategy(value) {
  return formatTypeLabel(value, MATCH_STRATEGY_LABELS);
}

function formatResultStatus(value) {
  return formatTypeLabel(value, RESULT_STATUS_LABELS);
}

function formatCacheStatus(value) {
  const normalized = String(value || "").trim();
  if (!normalized) return { text: "-", cls: "" };
  if (CACHE_STATUS_LABELS[normalized]) {
    const isHit = normalized.startsWith("HIT");
    const isBanned = normalized === "BANNED";
    return {
      text: CACHE_STATUS_LABELS[normalized],
      cls: isHit ? "cache-hit" : isBanned ? "cache-banned" : "cache-bypass",
    };
  }
  return { text: normalized, cls: "" };
}

function formatMatchDetail(value) {
  return formatTypeLabel(value, MATCH_DETAIL_LABELS);
}

function formatRouteLogRequestHost(value) {
  const normalized = normalizeRequestHost(value);
  if (!normalized) return "-";
  return formatRequestHostLabel(normalized);
}

function formatRouteLogRuleRequestHost(value) {
  const normalized = normalizeRequestHost(value);
  if (!normalized) return "通配（未限定域名）";
  return formatRequestHostLabel(normalized);
}

function ensureRouteLogFilterFields() {
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

const THEME_STORAGE_KEY = "proxy_admin_theme";
const THEME_VALUES = ["light", "dark", "cosmic", "ocean", "amber", "forest", "sakura"];
const THEME_MIGRATIONS = { sunset: "amber" };

function initTheme() {
  let saved = localStorage.getItem(THEME_STORAGE_KEY);
  if (THEME_MIGRATIONS[saved]) {
    saved = THEME_MIGRATIONS[saved];
  }
  const theme = THEME_VALUES.includes(saved) ? saved : "light";
  applyTheme(theme);
}

function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem(THEME_STORAGE_KEY, theme);
  document.querySelectorAll(".theme-dot").forEach((dot) => {
    dot.classList.toggle("active", dot.dataset.themeVal === theme);
  });
}

function initHashRouting() {
  const VALID_MODULES = ["overview", "route-config", "logs", "app-logs", "geoip-online", "geoip-offline", "ip-ban-manager", "ip-cache-manager", "backup-manager"];
  const hash = window.location.hash.replace(/^#\/?/, "");
  if (hash && VALID_MODULES.includes(hash)) {
    activateModule(hash);
  }
  window.addEventListener("hashchange", () => {
    const newHash = window.location.hash.replace(/^#\/?/, "");
    if (newHash && VALID_MODULES.includes(newHash) && newHash !== state.activeModule) {
      activateModule(newHash);
    }
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

function animateCounter(el, target, duration = 600) {
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

function renderDashboardMetrics(animate = true) {
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

function matchRouteFilter(keyword, status, isDefault, group, rules) {
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

function renderRouteGroups(routeGroups) {
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
    // 路由前缀级别的访问控制徽标
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

function resetRouteGroupForm() {
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

function fillRouteGroupForm(group) {
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

function openPrefixEditor(pathPrefix, requestHost) {
  const group = findRouteGroup(pathPrefix, requestHost);
  if (!group) return;
  fillRouteGroupForm(group);
  document.getElementById("route-group-form-title").textContent = `编辑路径前缀 ${pathPrefix}`;
  openModal("prefix-modal");
}

async function submitRouteGroup() {
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

async function submitRule() {
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

function collectRouteGroupForm() {
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

function bindGeoNumericInputSafety() {
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

function fillGeoConfig(geo) {
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

function formatTestRawPayload(payload) {
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

function renderGeoSourceTestResult(result) {
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
    renderPagination(1, 1);
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
  renderPagination(state.logCurrentPage, state.logTotalPages);
}

function renderPagination(currentPage, totalPages, containerId, onPageChange) {
  const _containerId = containerId || "log-pagination";
  const _onPageChange = onPageChange || goToPage;
  const container = document.getElementById(_containerId);
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

  const frag = document.createDocumentFragment();

  const prevBtn = document.createElement("button");
  prevBtn.type = "button";
  prevBtn.className = `page-btn ${currentPage <= 1 ? "disabled" : ""}`;
  prevBtn.textContent = "‹ 上一页";
  prevBtn.disabled = currentPage <= 1;
  prevBtn.addEventListener("click", () => _onPageChange(currentPage - 1, effectiveTotal));
  frag.appendChild(prevBtn);

  pages.forEach((p) => {
    if (p === "...") {
      const dots = document.createElement("span");
      dots.className = "page-ellipsis";
      dots.textContent = "…";
      frag.appendChild(dots);
    } else {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = `page-btn ${p === currentPage ? "active" : ""}`;
      btn.textContent = p;
      btn.addEventListener("click", () => _onPageChange(p, effectiveTotal));
      frag.appendChild(btn);
    }
  });

  const nextBtn = document.createElement("button");
  nextBtn.type = "button";
  nextBtn.className = `page-btn ${currentPage >= effectiveTotal ? "disabled" : ""}`;
  nextBtn.textContent = "下一页 ›";
  nextBtn.disabled = currentPage >= effectiveTotal;
  nextBtn.addEventListener("click", () => _onPageChange(currentPage + 1, effectiveTotal));
  frag.appendChild(nextBtn);

  const jumpInputId = `${_containerId}-jump-input`;
  const jumpWrap = document.createElement("div");
  jumpWrap.className = "page-jump";
  jumpWrap.innerHTML = `
    <span class="jump-label">跳至</span>
    <input id="${jumpInputId}" type="number" min="1" max="${effectiveTotal}" value="${currentPage}" />
    <span class="jump-label">/ ${effectiveTotal} 页</span>
    <button type="button" class="page-btn jump-btn">跳转</button>
  `;
  frag.appendChild(jumpWrap);

  container.appendChild(frag);

  container.querySelector(`#${jumpInputId}`)?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      const val = parseInt(e.target.value, 10);
      if (val >= 1 && val <= effectiveTotal) _onPageChange(val, effectiveTotal);
    }
  });

  container.querySelector(".jump-btn")?.addEventListener("click", () => {
    const input = container.querySelector(`#${jumpInputId}`);
    const val = parseInt(input?.value, 10);
    if (val >= 1 && val <= effectiveTotal) _onPageChange(val, effectiveTotal);
  });
}

async function goToPage(page, totalPages) {
  page = Math.max(1, Math.min(totalPages, page));
  state.logCurrentPage = page;
  await loadRouteLogs();
}

function collectRouteLogFilters() {
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
  const [payload, bansData] = await Promise.all([
    apiFetch(`/_admin/api/logs${query ? `?${query}` : ""}`),
    apiFetch("/_admin/api/banned-ips").catch(() => ({ items: [] })),
  ]);
  state.bannedIps = bansData.items || [];
  renderRouteLogs(payload || { items: [], total: 0 });
}

async function refreshRouteLogModule() {
  await Promise.all([loadRouteLogSettings(), loadRouteLogs()]);
}

let _autoRefreshTimer = null;
const AUTO_REFRESH_STORAGE_KEY = "log_auto_refresh";

function getAutoRefreshConfig() {
  try {
    const raw = localStorage.getItem(AUTO_REFRESH_STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch (e) {}
  return { enabled: false, interval: 5 };
}

function saveAutoRefreshConfig(cfg) {
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

function stopAutoRefresh() {
  if (_autoRefreshTimer !== null) {
    clearInterval(_autoRefreshTimer);
    _autoRefreshTimer = null;
  }
  updateAutoRefreshStatusUI();
}

function startAutoRefresh() {
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

let _appLogAutoRefreshTimer = null;
state.appLogFile = "";
state.appLogKeyword = "";

async function loadAppLogFiles() {
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

function highlightLogLine(line) {
  if (!line) return line;
  let safe = line.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const tsMatch = safe.match(/^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}[,\.]?\d*)/);
  if (tsMatch) safe = '<span class="log-ts">' + tsMatch[1] + '</span>' + safe.slice(tsMatch[1].length);
  const reqMatch = safe.match(/\[([a-f0-9]{8,12})\]/);
  if (reqMatch) safe = safe.replace(reqMatch[0], '<span class="log-reqid">' + reqMatch[0] + '</span>');
  safe = safe.replace(/\b(INFO|DEBUG|WARNING|ERROR|CRITICAL)\b/g, '<span class="log-level-$1">$1</span>');
  safe = safe.replace(/\b(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\b/g, '<span class="log-method">$1</span>');
  safe = safe.replace(/\b([1-5]\d{2})\b/g, '<span class="log-status-$1">$1</span>');
  safe = safe.replace(/\b(\d+\.?\d*ms)\b/, '<span class="log-duration">$1</span>');
  return safe;
}

async function loadAppLogContent(isAutoRefresh = false) {
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
    return;
  }
  
  const lines = raw.split("\n");
  const fragment = document.createDocumentFragment();
  for (let i = 0; i < lines.length; i++) {
    if (i > 0) fragment.appendChild(document.createElement("br"));
    const span = document.createElement("span");
    span.innerHTML = highlightLogLine(lines[i]);
    fragment.appendChild(span);
  }
  contentEl.innerHTML = "";
  contentEl.appendChild(fragment);
  
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

function startAppLogAutoRefresh() {
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

function stopAppLogAutoRefresh() {
  if (_appLogAutoRefreshTimer !== null) {
    clearInterval(_appLogAutoRefreshTimer);
    _appLogAutoRefreshTimer = null;
  }
}

async function refreshAppLogModule() {
  await loadAppLogFiles();
  initLogScrollDetection();
}

function initLogScrollDetection() {
  const contentEl = document.getElementById("app-log-content");
  if (!contentEl || contentEl._scrollListenerAdded) return;
  contentEl._scrollListenerAdded = true;
  contentEl.addEventListener("scroll", () => {
    const isAtBottom = contentEl.scrollHeight - contentEl.scrollTop - contentEl.clientHeight < 50;
    state.logAutoScroll = isAtBottom;
  });
}

function renderRules(rules) {
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

function resetRuleForm() {
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

function fillRuleForm(rule) {
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

function collectRuleForm() {
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

function prepareRuleForGroup(pathPrefix, requestHost = "") {
  resetRuleForm();
  setValue("rule_path_prefix", pathPrefix);
  setValue("rule_request_host", normalizeRequestHost(requestHost));
  document.getElementById("rule-form-title").textContent = "新增规则";
  openModal("rule-modal");
  focusField("rule_name");
}

function openRuleEditor(ruleId) {
  const id = Number(ruleId);
  const rule = state.rules.find(r => r.id === id);
  if (!rule) return;
  fillRuleForm(rule);
  document.getElementById("rule-form-title").textContent = `编辑规则 #${rule.id}`;
  openModal("rule-modal");
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

async function toggleRule(ruleId, enabled) {
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

async function toggleRuleField(ruleId, field, nextValue) {
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
  renderDashboardMetrics();
}

function buildGeoSettingsPayload() {
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

async function persistGeoSettings(successMessage = "IP 定位配置已保存。") {
  await apiFetch("/_admin/api/geoip", {
    method: "PUT",
    body: JSON.stringify(buildGeoSettingsPayload()),
  });
  await loadDashboard();
  showToast(successMessage);
}

function activateModule(target) {
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
  // 切换模块时同步停止封禁列表自动刷新（仅当目标不是封禁管理时）
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
    renderDashboardMetrics(false);
  }
}

document.querySelectorAll(".module-btn").forEach((button) => {
  button.addEventListener("click", () => {
    activateModule(button.dataset.moduleTarget);
  });
});

document.getElementById("app-log-cleanup-btn").addEventListener("click", async () => {
  try {
    const result = await apiFetch("/_admin/api/log-file-cleanup", { method: "POST" });
    showToast(`清理完成，删除了 ${result.deleted_count} 个过期日志文件。`);
    await refreshAppLogModule();
  } catch (error) {
    showToast(error.message, true);
  }
});

document.getElementById("app-log-file-select")?.addEventListener("change", (e) => {
  state.appLogFile = e.target.value;
  loadAppLogContent();
});

document.getElementById("app-log-refresh-btn").addEventListener("click", () => {
  refreshAppLogModule().catch((error) => {
    showToast(error.message, true);
  });
});

document.getElementById("app-log-search-btn").addEventListener("click", () => {
  loadAppLogContent().catch((error) => {
    showToast(error.message, true);
  });
});

document.getElementById("app-log-tail-lines").addEventListener("change", () => {
  loadAppLogContent().catch((error) => {
    showToast(error.message, true);
  });
});

document.getElementById("app-log-auto-refresh").addEventListener("change", () => {
  if (getChecked("app-log-auto-refresh")) {
    startAppLogAutoRefresh();
  } else {
    stopAppLogAutoRefresh();
  }
});

document.getElementById("app-log-keyword").addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    loadAppLogContent().catch((error) => {
      showToast(error.message, true);
    });
  }
});

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

document.querySelectorAll(".theme-dot").forEach((dot) => {
  dot.addEventListener("click", () => {
    applyTheme(dot.dataset.themeVal);
  });
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
    openPrefixEditor(pathPrefix, requestHost);
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

document.getElementById("log-cleanup-btn").addEventListener("click", async () => {
  try {
    const result = await apiFetch("/_admin/api/log-cleanup", { method: "POST" });
    showToast(`清理完成，删除了 ${result.deleted_count} 条过期日志记录。`);
    await refreshRouteLogModule();
  } catch (error) {
    showToast(error.message, true);
  }
});

async function loadIpCacheSettings() {
  try {
    const data = await apiFetch("/_admin/api/ip-cache-settings");
    if (data) {
      setValue("ip_cache_enabled", data.enabled ? "1" : "0");
      setValue("ip_cache_ttl_seconds", String(data.ttl_seconds || 300));
      setValue("ip_cache_max_entries", String(data.max_entries || 5000));
    }
  } catch (error) {}
}

async function loadIpCacheStats() {
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
  } catch (error) {
    showToast(error.message, true);
  }
});

async function loadAutoBanSettings() {
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

async function loadAutoBanStats() {
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
  } catch (error) {
    showToast(error.message, true);
  }
});

document.getElementById("open-auto-ban-modal-btn").addEventListener("click", async () => {
  await loadAutoBanSettings();
  openModal("auto-ban-modal");
});

async function loadEmailSettings() {
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
      setValue("email_alert_window_seconds", String(data.alert_window_seconds || 60));
      setValue("email_alert_max_requests", String(data.alert_max_requests || 80));
      setValue("email_alert_max_404", String(data.alert_max_404 || 15));
      setValue("email_alert_cooldown_minutes", String(data.alert_cooldown_minutes || 30));
    }
  } catch (error) {}
}

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
      alert_window_seconds: Number(getValue("email_alert_window_seconds") || 60),
      alert_max_requests: Number(getValue("email_alert_max_requests") || 80),
      alert_max_404: Number(getValue("email_alert_max_404") || 15),
      alert_cooldown_minutes: Number(getValue("email_alert_cooldown_minutes") || 30),
    };
    if (password) {
      payload.password = password;
    }
    await apiFetch("/_admin/api/email", {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    await loadEmailSettings();
    showToast("邮件提醒配置已保存。");
  } catch (error) {
    showToast(error.message, true);
  }
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
    if (result.success) {
      showToast(result.message);
    } else {
      showToast(result.message, true);
    }
  } catch (error) {
    showToast(error.message, true);
  } finally {
    btn.disabled = false;
    btn.textContent = "发送测试邮件";
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

document.getElementById("route-logs-list-body").addEventListener("mouseover", (event) => {
  const target = event.target.closest(".route-log-target-url");
  if (target) {
    const url = target.getAttribute("title") || target.textContent;
    showUrlTooltip(event, url);
  }
});

document.getElementById("route-logs-list-body").addEventListener("mouseout", (event) => {
  const target = event.target.closest(".route-log-target-url");
  if (target) {
    hideUrlTooltip();
  }
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
  } else if (action === "ban-ip-from-log") {
    const ip = button.dataset.ip;
    if (!ip || ip === "-") {
      showToast("该日志没有可封禁的IP地址", true);
      return;
    }
    try {
      await banIpFromLog(ip);
    } catch (error) {
      showToast(error.message, true);
    }
  } else if (action === "unban-ip-from-log") {
    const ip = button.dataset.ip;
    if (!ip || ip === "-") {
      showToast("该日志没有可解禁的IP地址", true);
      return;
    }
    if (!window.confirm(`确认解禁 IP ${ip} 吗？`)) return;
    try {
      await apiFetch(`/_admin/api/banned-ips/${encodeURIComponent(ip)}`, { method: "DELETE" });
      showToast(`IP ${ip} 已解禁`);
      await loadRouteLogs();
    } catch (error) {
      showToast(error.message, true);
    }
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

function fillGeoSourceTestSelect(selectedIndex) {
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

  if (!ip) {
    showToast("测试 IP 不能为空", true);
    return;
  }

  let source = null;
  if (selectedIndex !== "") {
    source = state.geoSources[Number(selectedIndex)] || null;
  }

  if (!source || !source.url) {
    showToast("请先选择一个有效的在线源", true);
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

document.getElementById("geo-offline-test-btn").addEventListener("click", () => {
  resetOfflineGeoTestResult();
  setValue("geo_offline_test_ip", "");
  openModal("geo-offline-test-modal");
});

document.getElementById("geo-offline-test-run-btn").addEventListener("click", async () => {
  const ip = getValue("geo_offline_test_ip").trim();
  const button = document.getElementById("geo-offline-test-run-btn");
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
    openModal("geo-source-modal");
    return;
  }

  if (action === "toggle-geo-source") {
    const previousSources = state.geoSources.map((item) => ({ ...item }));
    source.enabled = !source.enabled;
    renderGeoSources();
    try {
      await persistGeoSettings(source.enabled ? "在线源已启用。" : "在线源已禁用。");
    } catch (error) {
      state.geoSources = previousSources;
      renderGeoSources();
      showToast(error.message, true);
    }
    return;
  }

  if (action === "test-geo-source") {
    resetGeoSourceTestResult();
    setValue("geo_source_test_ip", "");
    fillGeoSourceTestSelect(index);
    openModal("geo-source-test-modal");
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

["geoip-online-form", "geoip-offline-form"].forEach((formId) => {
  const form = document.getElementById(formId);
  if (!form) return;
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await persistGeoSettings(formId === "geoip-online-form" ? "在线定位配置已保存。" : "离线定位配置已保存。");
    } catch (error) {
      showToast(error.message, true);
    }
  });
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
  
  let encryptedPassword = password;
  let encrypted = false;
  
  // 尝试加密密码
  const publicKey = await getPublicKey();
  if (publicKey) {
    try {
      encryptedPassword = await encryptPassword(password, publicKey);
      encrypted = true;
    } catch (e) {
      console.error("密码加密失败，使用明文:", e);
    }
  }
  
  const result = await apiFetch("/_admin/api/auth/login", {
    method: "POST",
    body: JSON.stringify({
      username,
      password: encryptedPassword,
      encrypted,
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
  if (!window.confirm("确认退出登录吗？")) return;
  try {
    await performLogout();
    setAuthError("已退出登录。");
    showToast("已退出登录。");
  } catch (error) {
    showToast(error.message, true);
  }
});

async function banIpFromLog(ip) {
  // 从日志点击封禁IP：打开封禁弹窗，预填IP和原因
  openBanModal({
    ip: ip,
    reason: "从日志手动封禁",
    mode: "from-log",
  });
}

async function loadBannedIpList() {
  try {
    const data = await apiFetch("/_admin/api/banned-ips");
    state.bannedIps = data.items || [];
    renderBannedIpListPage();
  } catch (error) {}
}

function renderBannedIpListPage() {
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

function goToBanPage(page, totalPages) {
  state.banCurrentPage = Math.max(1, Math.min(totalPages, page));
  renderBannedIpListPage();
}

let _banAutoRefreshTimer = null;
const BAN_AUTO_REFRESH_STORAGE_KEY = "ban_auto_refresh";

function getBanAutoRefreshConfig() {
  try {
    const raw = localStorage.getItem(BAN_AUTO_REFRESH_STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch (e) {}
  return { enabled: false, interval: 5 };
}

function saveBanAutoRefreshConfig(cfg) {
  localStorage.setItem(BAN_AUTO_REFRESH_STORAGE_KEY, JSON.stringify(cfg));
}

function stopBanAutoRefresh() {
  if (_banAutoRefreshTimer !== null) {
    clearInterval(_banAutoRefreshTimer);
    _banAutoRefreshTimer = null;
  }
}

function startBanAutoRefresh() {
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

function renderBannedIpList(items) {
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

    // 路径前缀展示：空=全局，否则显示具体前缀
    const pathPrefixText = item.path_prefix
      ? escapeHtml(item.path_prefix)
      : '<span class="ban-scope-global">全局</span>';

    // 永久封禁不显示延长按钮（永久无到期概念，延长会转为临时）
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

// ============ 封禁IP弹窗 ============

/**
 * 验证 IP 或 CIDR 格式
 * 支持: 单个IP (1.2.3.4)、IPv6 (2001:db8::1)、CIDR网段 (192.168.1.0/24)
 */
function isValidIpOrCidr(str) {
  if (!str) return false;
  const s = str.trim();
  // CIDR 网段
  if (s.includes("/")) {
    const parts = s.split("/");
    if (parts.length !== 2) return false;
    const prefix = parseInt(parts[1], 10);
    if (isNaN(prefix) || prefix < 0 || prefix > 128) return false;
    try {
      // 使用浏览器内置 API 验证 IP 部分
      const ipPart = parts[0];
      if (ipPart.includes(":")) {
        // IPv6 CIDR
        return prefix <= 128;
      } else {
        // IPv4 CIDR
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
  // 单个 IPv4
  if (s.includes(".")) {
    const octets = s.split(".");
    if (octets.length !== 4) return false;
    return octets.every((oct) => {
      const n = parseInt(oct, 10);
      return !isNaN(n) && n >= 0 && n <= 255;
    });
  }
  // 单个 IPv6 (简化验证)
  if (s.includes(":")) {
    return s.split(":").length >= 2;
  }
  return false;
}

/**
 * 检查 IP 是否在封禁列表中（支持单 IP 精确匹配和 CIDR 网段匹配）
 */
function isIpBanned(ip, bannedList) {
  if (!ip || ip === "-" || !bannedList || !bannedList.length) return false;
  // 精确匹配
  if (bannedList.some((b) => b.ip === ip)) return true;
  // CIDR 网段匹配
  for (const b of bannedList) {
    if (b.ip && b.ip.includes("/")) {
      if (ipInCidr(ip, b.ip)) return true;
    }
  }
  return false;
}

/**
 * 检查单个 IP 是否在 CIDR 网段内
 */
function ipInCidr(ip, cidr) {
  try {
    const [range, prefixStr] = cidr.split("/");
    const prefix = parseInt(prefixStr, 10);
    if (isNaN(prefix)) return false;
    // IPv4
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
    // IPv6 (简化处理，用 BigInt)
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

/**
 * IPv6 地址转 BigInt
 */
function ipv6ToBigInt(ip) {
  try {
    const parts = ip.split(":");
    if (parts.length < 3) return null;
    // 展开缩写 ::
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

function openBanModal(options = {}) {
  // options: {ip, reason, mode, pathPrefix}
  const mode = options.mode || "add";
  const titleEl = document.getElementById("ban-ip-modal-title");
  titleEl.textContent = mode === "from-log" ? "从日志封禁IP" : "封禁IP";
  setValue("ban_ip_mode", mode);
  setValue("ban_ip_address", options.ip || "");
  setValue("ban_ip_path_prefix", options.pathPrefix || "");
  setValue("ban_ip_reason", options.reason || "");
  // 默认永久封禁
  const permanentSelect = document.getElementById("ban_ip_permanent");
  if (permanentSelect) permanentSelect.value = "1";
  setValue("ban_ip_duration", "1");
  toggleBanDurationLabel();
  openModal("ban-ip-modal");
}

function toggleBanDurationLabel() {
  const selectEl = document.getElementById("ban_ip_permanent");
  const isPermanent = selectEl ? selectEl.value === "1" : true;
  const durationLabel = document.getElementById("ban_ip_duration_label");
  const durationInput = document.getElementById("ban_ip_duration");
  if (durationLabel) durationLabel.style.display = isPermanent ? "none" : "";
  if (durationInput) durationInput.required = !isPermanent;
}

document.getElementById("ban_ip_permanent")?.addEventListener("change", toggleBanDurationLabel);

document.getElementById("ban-ip-form")?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const ip = getValue("ban_ip_address").trim();
  if (!ip) {
    showToast("IP地址不能为空", true);
    return;
  }
  // 验证 IP 或 CIDR 格式
  if (!isValidIpOrCidr(ip)) {
    showToast("IP格式无效，请输入单个IP（如 1.2.3.4）或 CIDR 网段（如 192.168.1.0/24）", true);
    return;
  }
  const pathPrefix = getValue("ban_ip_path_prefix").trim();
  const reason = getValue("ban_ip_reason").trim();
  const selectEl = document.getElementById("ban_ip_permanent");
  const permanent = selectEl ? selectEl.value === "1" : true;
  const durationHours = parseFloat(getValue("ban_ip_duration") || "0") || 0;
  if (!permanent && durationHours <= 0) {
    showToast("临时封禁时长必须大于0", true);
    return;
  }
  // 小时转秒
  const durationSeconds = permanent ? 0 : Math.max(60, Math.round(durationHours * 3600));
  try {
    await apiFetch("/_admin/api/banned-ips", {
      method: "POST",
      body: JSON.stringify({
        ip: ip,
        reason: reason || "",
        banned_by: "admin",
        permanent: permanent,
        duration_seconds: durationSeconds,
        path_prefix: pathPrefix,
      }),
    });
    const scopeText = pathPrefix ? `路径前缀 ${pathPrefix}` : "全局";
    const isCidr = ip.includes("/");
    showToast(`${isCidr ? "IP段" : "IP"} ${ip} 已封禁（${scopeText}）`);
    closeModal("ban-ip-modal");
    loadBannedIpList();
    // 从日志触发的封禁，刷新日志列表以更新按钮状态
    if (getValue("ban_ip_mode") === "from-log") {
      loadRouteLogs();
    }
  } catch (error) {
    showToast(error.message, true);
  }
});

// ============ 延长封禁弹窗 ============
function openBanExtendModal(ip, currentExpireAt) {
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

document.getElementById("ban-extend-form")?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const ip = getValue("ban_extend_ip").trim();
  const durationHours = parseFloat(getValue("ban_extend_duration") || "0") || 0;
  if (!ip) {
    showToast("IP地址不能为空", true);
    return;
  }
  if (durationHours <= 0) {
    showToast("延长时长必须大于0", true);
    return;
  }
  try {
    await apiFetch(`/_admin/api/banned-ips/${encodeURIComponent(ip)}/extend`, {
      method: "POST",
      body: JSON.stringify({ duration_hours: durationHours }),
    });
    showToast(`IP ${ip} 封禁时间已延长 ${durationHours} 小时`);
    closeModal("ban-extend-modal");
    loadBannedIpList();
  } catch (error) {
    showToast(error.message, true);
  }
});

// 封禁列表事件委托：解封 + 延长
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
    } catch (error) {
      showToast(error.message, true);
    }
  } else if (action === "extend-ban-ip") {
    const expireAt = parseFloat(button.dataset.expire || "0") || 0;
    openBanExtendModal(ip, expireAt);
  }
});

// 添加封禁按钮：打开空白封禁弹窗
document.getElementById("add-ban-btn")?.addEventListener("click", () => {
  openBanModal({ mode: "add" });
});

document.getElementById("clear-bans-btn")?.addEventListener("click", async () => {
  if (!window.confirm("确认清空所有封禁记录吗？此操作不可恢复！")) return;
  try {
    await apiFetch("/_admin/api/banned-ips/clear", { method: "POST" });
    showToast("所有封禁记录已清空");
    state.banCurrentPage = 1;
    loadBannedIpList();
  } catch (error) {
    showToast(error.message, true);
  }
});

// 封禁列表自动刷新开关
document.getElementById("ban_auto_refresh_enabled")?.addEventListener("change", () => {
  if (getChecked("ban_auto_refresh_enabled")) {
    startBanAutoRefresh();
  } else {
    stopBanAutoRefresh();
    saveBanAutoRefreshConfig({ enabled: false, interval: parseInt(getValue("ban_auto_refresh_interval") || "5", 10) || 5 });
  }
});

// 封禁列表自动刷新间隔变更
document.getElementById("ban_auto_refresh_interval")?.addEventListener("change", () => {
  const interval = parseInt(getValue("ban_auto_refresh_interval") || "5", 10) || 5;
  saveBanAutoRefreshConfig({ enabled: getChecked("ban_auto_refresh_enabled"), interval });
  if (getChecked("ban_auto_refresh_enabled")) {
    startBanAutoRefresh();
  }
});

// 封禁列表每页大小变更
document.getElementById("ban_page_size")?.addEventListener("change", () => {
  const size = parseInt(getValue("ban_page_size") || "20", 10) || 20;
  state.banPageSize = Math.max(1, size);
  state.banCurrentPage = 1;
  localStorage.setItem("ban_page_size", String(state.banPageSize));
  renderBannedIpListPage();
});

document.getElementById("log_auto_refresh_enabled")?.addEventListener("change", () => {
  if (getChecked("log_auto_refresh_enabled")) {
    startAutoRefresh();
  } else {
    stopAutoRefresh();
    saveAutoRefreshConfig({ enabled: false, interval: parseInt(getValue("log_auto_refresh_interval") || "5", 10) || 5 });
  }
});

document.getElementById("log_auto_refresh_interval")?.addEventListener("change", () => {
  const interval = parseInt(getValue("log_auto_refresh_interval") || "5", 10) || 5;
  saveAutoRefreshConfig({ enabled: getChecked("log_auto_refresh_enabled"), interval });
  if (getChecked("log_auto_refresh_enabled")) {
    startAutoRefresh();
  }
});

// 日志每页大小变更
document.getElementById("log_page_size")?.addEventListener("change", () => {
  const size = parseInt(getValue("log_page_size") || "50", 10) || 50;
  state.logPageSize = Math.max(1, size);
  state.logCurrentPage = 1;
  localStorage.setItem("log_page_size", String(state.logPageSize));
  loadRouteLogs().catch((error) => {
    showToast(error.message, true);
  });
});

document.getElementById("clear-ip-cache-btn")?.addEventListener("click", async () => {
  if (!window.confirm("确认清空所有请求结果缓存吗？")) return;
  try {
    const data = await apiFetch("/_admin/api/ip-cache/clear", { method: "POST" });
    showToast(data.message || "缓存已清空");
    loadIpCacheStats();
  } catch (error) {
    showToast(error.message, true);
  }
});

// ===== 备份管理 =====

function formatBackupSize(bytes) {
  if (bytes >= 1024 * 1024) return (bytes / 1024 / 1024).toFixed(2) + " MB";
  if (bytes >= 1024) return (bytes / 1024).toFixed(1) + " KB";
  return bytes + " B";
}

function formatBackupTime(isoStr) {
  try {
    const d = new Date(isoStr);
    return d.toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });
  } catch {
    return isoStr;
  }
}

async function loadBackups() {
  try {
    const data = await apiFetch("/_admin/api/backup/list");
    state.backups = data.items || [];
    renderBackupList();
  } catch (error) {
    showToast(error.message, true);
  }
}

function renderBackupList() {
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

async function createBackup() {
  try {
    const data = await apiFetch("/_admin/api/backup/create", { method: "POST" });
    showToast(`备份已创建: ${data.filename}`);
    await loadBackups();
  } catch (error) {
    showToast(error.message, true);
  }
}

function downloadBackup(filename) {
  const a = document.createElement("a");
  a.href = `/_admin/api/backup/download/${encodeURIComponent(filename)}`;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

function openRestoreModal(filename) {
  document.getElementById("restore_backup_filename").value = filename;
  document.getElementById("restore-backup-name").textContent = filename;
  document.getElementById("restore_mode").value = "overwrite";
  openModal("backup-restore-modal");
}

async function confirmRestoreBackup() {
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

async function deleteBackup(filename) {
  if (!window.confirm(`确认删除备份文件 ${filename} 吗？`)) return;
  try {
    await apiFetch(`/_admin/api/backup/${encodeURIComponent(filename)}`, { method: "DELETE" });
    showToast("备份已删除");
    await loadBackups();
  } catch (error) {
    showToast(error.message, true);
  }
}

async function uploadAndRestore() {
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
document.getElementById("backup-upload-form")?.addEventListener("submit", (e) => {
  e.preventDefault();
  uploadAndRestore();
});

document.getElementById("backup_restore_mode")?.addEventListener("change", (e) => {
  const hint = document.getElementById("backup-mode-hint");
  if (hint) {
    hint.textContent = e.target.value === "overwrite"
      ? "覆盖模式：用上传的数据库文件完全替换当前数据库。"
      : "合并模式：仅导入上传文件中的新规则，跳过已存在的规则。";
  }
});

document.getElementById("restore_mode")?.addEventListener("change", (e) => {
  const hint = document.getElementById("restore-mode-hint");
  if (hint) {
    hint.textContent = e.target.value === "overwrite"
      ? "覆盖模式会替换当前数据库中的所有配置和数据。"
      : "合并模式会导入所有配置表（系统设置、路由规则、GeoIP 等），跳过已存在的条目，运行日志和封禁列表不会被导入。";
  }
});

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

  // 恢复封禁列表自动刷新配置
  const savedBanRefresh = getBanAutoRefreshConfig();
  setChecked("ban_auto_refresh_enabled", savedBanRefresh.enabled);
  setValue("ban_auto_refresh_interval", String(savedBanRefresh.interval));

  // 恢复日志每页大小配置
  const savedLogPageSize = parseInt(localStorage.getItem("log_page_size") || "50", 10) || 50;
  state.logPageSize = savedLogPageSize;
  setValue("log_page_size", String(savedLogPageSize));

  // 恢复封禁每页大小配置
  const savedBanPageSize = parseInt(localStorage.getItem("ban_page_size") || "20", 10) || 20;
  state.banPageSize = savedBanPageSize;
  setValue("ban_page_size", String(savedBanPageSize));

  // 打开新增前缀弹框
  document.getElementById("add-prefix-btn").addEventListener("click", () => {
    resetRouteGroupForm();
    document.getElementById("route-group-form-title").textContent = "新增路径前缀";
    openModal("prefix-modal");
  });

  // 打开新增在线源弹框
  document.getElementById("add-geo-source-btn")?.addEventListener("click", () => {
    resetGeoSourceForm();
    document.getElementById("geo-source-form-title").textContent = "新增在线源";
    openModal("geo-source-modal");
  });

  // 打开新增规则弹框
  document.getElementById("add-rule-btn").addEventListener("click", () => {
    resetRuleForm();
    document.getElementById("rule-form-title").textContent = "新增规则";
    openModal("rule-modal");
  });

  // 路由配置查询表单：实时过滤
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

  if (filterKeyword) {
    filterKeyword.addEventListener("input", applyRouteFilter);
  }
  if (filterStatus) {
    filterStatus.addEventListener("change", applyRouteFilter);
  }
  if (filterDefault) {
    filterDefault.addEventListener("change", applyRouteFilter);
  }
  if (filterResetBtn) {
    filterResetBtn.addEventListener("click", () => {
      if (filterKeyword) filterKeyword.value = "";
      if (filterStatus) filterStatus.value = "";
      if (filterDefault) filterDefault.value = "";
      applyRouteFilter();
    });
  }

  // 关闭弹框按钮
  document.querySelectorAll("[data-close-modal]").forEach(btn => {
    btn.addEventListener("click", () => {
      closeModal(btn.dataset.closeModal);
    });
  });

  // 点击弹框背景关闭
  document.querySelectorAll(".modal-overlay").forEach(modal => {
    modal.addEventListener("click", (e) => {
      if (e.target === modal) {
        closeModal(modal.id);
      }
    });
  });

  // 路径前缀表单提交（改为弹框内的表单）
  document.getElementById("prefix-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    await submitRouteGroup();
    closeModal("prefix-modal");
  });

  // 规则表单提交（改为弹框内的表单）
  document.getElementById("rule-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    await submitRule();
    closeModal("rule-modal");
  });

  try {
    const auth = await loadAuthStatus();
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


