/**
 * utils.js - 工具函数模块
 * 包含：格式化、验证、DOM操作等通用工具函数
 */

// ===== DOM 操作工具 =====

export function setValue(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.value = value ?? "";
  }
}

export function setChecked(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.checked = Boolean(value);
  }
}

export function getValue(id) {
  const el = document.getElementById(id);
  return el ? el.value : "";
}

export function getChecked(id) {
  const el = document.getElementById(id);
  return Boolean(el && el.checked);
}

export function getNonNegativeIntValue(id, fallback = 0) {
  const raw = String(getValue(id) || "").trim();
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) {
    return Math.max(0, Number.parseInt(String(fallback), 10) || 0);
  }
  return Math.max(0, parsed);
}

export function getPositiveIntValue(id, fallback = 1) {
  const raw = String(getValue(id) || "").trim();
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) {
    return Math.max(1, Number.parseInt(String(fallback), 10) || 1);
  }
  return Math.max(1, parsed);
}

export function setText(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = value ?? "-";
  }
}

export function focusField(id) {
  const el = document.getElementById(id);
  if (el) {
    window.requestAnimationFrame(() => el.focus());
  }
}

export function scrollToElement(id) {
  const el = document.getElementById(id);
  if (el) {
    el.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

// ===== HTML 转义 =====

export function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

// ===== 格式化函数 =====

export function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}

export function formatRemainTime(seconds) {
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

export function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!bytes) return "0 B";
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(2)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  return `${bytes} B`;
}

export function toIsoDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toISOString().replace("Z", "+00:00");
}

// ===== 主机名处理 =====

export function normalizeRequestHost(value) {
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

export function formatRequestHostLabel(requestHost) {
  if (!requestHost) {
    return "*";
  }
  return String(requestHost)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .join(", ");
}

// ===== 类型标签映射 =====

export const MATCH_STRATEGY_LABELS = {
  ip_whitelist_match: "IP 白名单命中",
  region_match: "地区命中",
  default_route: "默认路由",
  priority_fallback: "优先级回退",
  no_route: "未匹配路由",
};

export const RESULT_STATUS_LABELS = {
  forwarded: "转发成功",
  cache_hit: "缓存命中",
  forwarded_client_error: "上游 4xx",
  upstream_error: "上游异常",
  proxy_error: "代理异常",
  no_route: "未匹配路由",
};

export const CACHE_STATUS_LABELS = {
  BYPASS: "未命中",
  HIT_REDIRECT: "缓存命中(重定向)",
  HIT_STREAMING: "缓存命中(流式)",
  BANNED: "已封禁",
};

export const MATCH_DETAIL_LABELS = {
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

// ===== 格式化标签 =====

export function formatTypeLabel(value, labels) {
  const normalized = String(value || "").trim();
  if (!normalized) return "-";
  if (labels[normalized]) return labels[normalized];
  if (/^[a-z0-9_:-]+$/i.test(normalized)) {
    return `未映射类型（${normalized}）`;
  }
  return normalized;
}

export function formatMatchStrategy(value) {
  return formatTypeLabel(value, MATCH_STRATEGY_LABELS);
}

export function formatResultStatus(value) {
  return formatTypeLabel(value, RESULT_STATUS_LABELS);
}

export function formatCacheStatus(value) {
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

export function formatMatchDetail(value) {
  return formatTypeLabel(value, MATCH_DETAIL_LABELS);
}

export function formatRouteLogRequestHost(value) {
  const normalized = normalizeRequestHost(value);
  if (!normalized) return "-";
  return formatRequestHostLabel(normalized);
}

export function formatRouteLogRuleRequestHost(value) {
  const normalized = normalizeRequestHost(value);
  if (!normalized) return "通配（未限定域名）";
  return formatRequestHostLabel(normalized);
}

// ===== 路由组工具 =====

export function isSameRouteGroup(group, pathPrefix, requestHost, state) {
  return (
    group.path_prefix === pathPrefix &&
    normalizeRequestHost(group.request_host) === normalizeRequestHost(requestHost)
  );
}

export function findRouteGroup(pathPrefix, requestHost, state) {
  return state.routeGroups.find((item) => isSameRouteGroup(item, pathPrefix, requestHost, state));
}
