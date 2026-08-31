from __future__ import annotations

import aiohttp
import asyncio
import ipaddress
import logging
import re
import time
from dataclasses import dataclass
from typing import AsyncGenerator, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

from config import Config, ProxyRule, normalize_request_host, split_request_hosts
from geo_service import GeoLocation, GeoResolver
from ip_result_cache import IpResultCache, IpCacheEntry
from ip_ban_manager import IpBanManager


logger = logging.getLogger("proxy")


# ===== 500 错误页面渲染（隐藏内部异常细节）=====
_500_HTML_CACHE: Optional[str] = None


def _load_500_template() -> Optional[str]:
    """加载 static/500.html 模板，带模块级缓存"""
    global _500_HTML_CACHE
    if _500_HTML_CACHE is not None:
        return _500_HTML_CACHE
    try:
        from pathlib import Path
        template_path = Path(__file__).parent / "static" / "500.html"
        if template_path.exists():
            _500_HTML_CACHE = template_path.read_text(encoding="utf-8")
            return _500_HTML_CACHE
    except Exception as e:
        logger.error("读取 500 页面失败: %s", e)
    return None


def _build_500_html(reason: str = "") -> str:
    """构建 500 错误页面 HTML，注入通用原因（不暴露内部异常）"""
    import html as html_module
    template = _load_500_template()
    if template:
        if reason:
            safe_reason = html_module.escape(reason)
            template = template.replace(
                'id="error-reason-container" hidden>',
                'id="error-reason-container">'
            )
            template = template.replace(
                '<div class="error-reason-text" id="error-reason-text">-</div>',
                f'<div class="error-reason-text" id="error-reason-text">{safe_reason}</div>'
            )
        return template
    # 回退到内置 HTML
    safe_reason = html_module.escape(reason) if reason else "服务异常"
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>500 - 服务异常</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           display: flex; align-items: center; justify-content: center;
           min-height: 100vh; margin: 0; background: #f8fafc; color: #1e293b; }}
    .container {{ text-align: center; padding: 2rem; max-width: 480px; }}
    .code {{ font-size: 4rem; font-weight: 700; color: #d97706; }}
    .title {{ font-size: 1.5rem; margin: 1rem 0; }}
    .reason {{ background: #fef3c7; padding: 1rem; border-radius: 8px; margin: 1rem 0;
               text-align: left; font-size: 0.875rem; }}
    a {{ color: #2563eb; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="code">500</div>
    <h1 class="title">服务异常</h1>
    <p>代理服务在处理请求时遇到问题。</p>
    <div class="reason"><strong>类型：</strong>{safe_reason}</div>
    <p><a href="/">返回首页</a></p>
  </div>
</body>
</html>"""


def _build_500_bytes(reason: str = "") -> bytes:
    """构建 500 错误页面字节"""
    return _build_500_html(reason).encode("utf-8")


@dataclass
class RedirectInfo:
    original_url: str
    redirect_url: str
    status_code: int
    redirect_count: int
    redirect_chain: list


@dataclass
class RouteDecision:
    rule: ProxyRule
    target_url: str
    client_ip: str
    geo_location: Optional[GeoLocation]
    match_strategy: str
    region_matching_enabled: bool
    request_host: str = ""
    rule_request_host: str = ""
    matched_region: Optional[str] = None
    matched_ip_whitelist: Optional[str] = None
    match_detail: str = ""
    # 黑白名单拒绝标记：命中黑名单或不在白名单时为 True，由 handle_proxy 返回 403
    blocked: bool = False
    block_reason: str = ""


@dataclass
class StreamingResponse:
    status: int
    headers: Dict[str, str]
    body_stream: Optional[AsyncGenerator[bytes, None]]
    redirect_info: Optional[RedirectInfo]
    content_length: Optional[int] = None
    route_decision: Optional[RouteDecision] = None
    cache_status: str = ""


class RedirectHandler:
    REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}

    def __init__(
        self,
        max_redirects: int = 10,
        timeout: int = 30,
        stream_timeout: int = 3600,
        follow_redirects: bool = True,
    ):
        self.max_redirects = max_redirects
        self.follow_redirects_enabled = follow_redirects
        self.timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout)
        self.stream_timeout = aiohttp.ClientTimeout(
            total=stream_timeout,
            connect=timeout,
            sock_read=timeout,
        )

    async def follow_redirects_streaming(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> Tuple[Optional[aiohttp.ClientResponse], RedirectInfo]:
        return await self._follow_redirects(url, method, headers, body, session, streaming=True)

    async def follow_redirects(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> Tuple[Optional[aiohttp.ClientResponse], RedirectInfo]:
        return await self._follow_redirects(url, method, headers, body, session, streaming=False)

    async def _follow_redirects(
        self,
        url: str,
        method: str,
        headers: Optional[Dict[str, str]],
        body: Optional[bytes],
        session: Optional[aiohttp.ClientSession],
        streaming: bool,
    ) -> Tuple[Optional[aiohttp.ClientResponse], RedirectInfo]:
        redirect_chain = []
        current_url = url
        redirect_count = 0
        own_session = session is None
        response: Optional[aiohttp.ClientResponse] = None

        if own_session:
            session = aiohttp.ClientSession(timeout=self.stream_timeout if streaming else self.timeout)

        try:
            while redirect_count <= self.max_redirects:
                request_headers = headers.copy() if headers else {}
                request_body = None if method.upper() in {"GET", "HEAD", "OPTIONS", "TRACE"} else body
                response = await session.request(
                    method=method,
                    url=current_url,
                    headers=request_headers,
                    data=request_body,
                    allow_redirects=False,
                    ssl=False,
                )

                if response.status not in self.REDIRECT_STATUS_CODES or not self.follow_redirects_enabled:
                    location = response.headers.get("Location")
                    resolved_redirect_url = urljoin(current_url, location) if location else str(response.url)
                    return response, RedirectInfo(
                        original_url=url,
                        redirect_url=resolved_redirect_url,
                        status_code=response.status,
                        redirect_count=redirect_count,
                        redirect_chain=redirect_chain,
                    )

                location = response.headers.get("Location")
                if not location:
                    return response, RedirectInfo(
                        original_url=url,
                        redirect_url=current_url,
                        status_code=response.status,
                        redirect_count=redirect_count,
                        redirect_chain=redirect_chain,
                    )

                redirect_url = urljoin(current_url, location)
                redirect_chain.append(
                    {
                        "from": current_url,
                        "to": redirect_url,
                        "status": response.status,
                    }
                )
                redirect_count += 1
                logger.debug(
                    "重定向跟踪: %d -> %s (状态 %d)",
                    redirect_count, redirect_url, response.status,
                )

                if response.status in {301, 302, 303} and method.upper() not in {"GET", "HEAD"}:
                    method = "GET"
                    body = None

                response.release()
                current_url = redirect_url

            logger.warning("超过最大重定向次数 (%s): %s", self.max_redirects, url)
            return None, RedirectInfo(
                original_url=url,
                redirect_url=current_url,
                status_code=310,
                redirect_count=redirect_count,
                redirect_chain=redirect_chain,
            )
        finally:
            if own_session and response is None:
                await session.close()


class ProxyRequestHandler:
    def __init__(self, config: Config, geo_resolver: Optional[GeoResolver] = None,
                 ip_cache: Optional[IpResultCache] = None, ip_ban_manager=None):
        self.config = config
        self.geo_resolver = geo_resolver
        self.ip_cache = ip_cache
        self.ip_ban_manager = ip_ban_manager
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_cache_status = "BYPASS"
        self._refresh_redirect_handler()

    def _refresh_redirect_handler(self) -> None:
        self.redirect_handler = RedirectHandler(
            max_redirects=self.config.max_redirects,
            timeout=self.config.default_timeout,
            stream_timeout=self.config.streaming.stream_timeout,
            follow_redirects=self.config.follow_redirects,
        )

    def update_config(self, config: Config) -> None:
        self.config = config
        self._refresh_redirect_handler()

    def set_ip_cache(self, ip_cache: Optional[IpResultCache]) -> None:
        self.ip_cache = ip_cache

    def set_ip_ban_manager(self, ip_ban_manager) -> None:
        self.ip_ban_manager = ip_ban_manager

    def get_last_cache_status(self) -> str:
        return getattr(self, '_last_cache_status', 'BYPASS')

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(
                total=self.config.streaming.stream_timeout,
                connect=self.config.default_timeout,
                sock_read=self.config.streaming.read_timeout,
            )
            connector = aiohttp.TCPConnector(
                limit=self.config.server.max_connections,
                limit_per_host=self.config.server.max_connections_per_host,
                enable_cleanup_closed=True,
                force_close=False,
            )
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def _get_header_value(self, headers: Dict[str, str], header_name: str) -> str:
        lowered = header_name.lower()
        for key, value in headers.items():
            if str(key).lower() == lowered:
                return str(value)
        return ""

    def _extract_first_host_value(self, raw_value: str) -> str:
        value = str(raw_value or "").strip()
        if not value:
            return ""
        first = value.split(",", 1)[0].strip()
        return normalize_request_host(first)

    def _extract_host_from_forwarded_header(self, forwarded_header: str) -> str:
        raw = str(forwarded_header or "").strip()
        if not raw:
            return ""

        first_hop = raw.split(",", 1)[0]
        parts = [item.strip() for item in first_hop.split(";") if item.strip()]
        for part in parts:
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key.strip().lower() != "host":
                continue
            cleaned = value.strip().strip('"')
            return self._extract_first_host_value(cleaned)
        return ""

    def extract_request_host(self, headers: Dict[str, str]) -> str:
        if self.config.trust_forward_headers:
            candidates = (
                self._get_header_value(headers, "X-Original-Host"),
                self._get_header_value(headers, "X-Forwarded-Host"),
                self._extract_host_from_forwarded_header(self._get_header_value(headers, "Forwarded")),
                self._get_header_value(headers, "X-Host"),
            )
            for candidate in candidates:
                normalized = self._extract_first_host_value(candidate)
                if normalized:
                    return normalized

        request_host = self._get_header_value(headers, "Host")
        return self._extract_first_host_value(request_host)

    def find_matching_rules(self, path: str, request_host: str = "") -> List[ProxyRule]:
        request_hosts = set(split_request_hosts(request_host))
        matches: List[Tuple[ProxyRule, bool]] = []

        for rule in self.config.proxy_rules:
            if not rule.enabled or not path.startswith(rule.path_prefix):
                continue
            rule_hosts = split_request_hosts(rule.request_host)
            if rule_hosts:
                if request_hosts and any(host in request_hosts for host in rule_hosts):
                    matches.append((rule, True))
                continue
            matches.append((rule, False))

        if not matches:
            return []

        longest_prefix = max(len(rule.path_prefix) for rule, _ in matches)
        scoped = [(rule, host_matched) for rule, host_matched in matches if len(rule.path_prefix) == longest_prefix]
        if any(host_matched for _, host_matched in scoped):
            scoped_rules = [rule for rule, host_matched in scoped if host_matched]
        else:
            scoped_rules = [rule for rule, _ in scoped]

        return sorted(scoped_rules, key=lambda rule: (-rule.priority, rule.rule_id or 0))

    def _find_route_group_for_host(self, path_prefix: str, request_host: str):
        normalized_request_host = normalize_request_host(request_host)
        if not normalized_request_host:
            return None
        for group in self.config.route_groups:
            if group.path_prefix != path_prefix:
                continue
            if normalized_request_host in split_request_hosts(group.request_host):
                return group
        return None

    def find_matching_rule(self, path: str, request_host: str = "") -> Optional[ProxyRule]:
        matches = self.find_matching_rules(path, request_host=request_host)
        return matches[0] if matches else None

    def build_target_url(self, path: str, rule: ProxyRule, query_string: Optional[str] = None) -> str:
        parsed_target = urlparse(rule.target_url)
        # 正则改写在 strip_prefix 之前执行，命中的 path 先被 re.sub 改写
        effective_path = path
        if rule.path_rewrite_pattern:
            try:
                effective_path = re.sub(rule.path_rewrite_pattern, rule.path_rewrite_replacement or "", path)
            except re.error:
                logger.warning("规则 %s 的正则改写模式无效: %s", rule.rule_id, rule.path_rewrite_pattern)
                effective_path = path

        if rule.strip_prefix:
            remaining_path = effective_path[len(rule.path_prefix):]
            if not remaining_path.startswith("/"):
                remaining_path = "/" + remaining_path
            new_path = parsed_target.path + remaining_path
        else:
            new_path = parsed_target.path + effective_path

        encoded_path = quote(new_path, safe="/")
        base_url = f"{parsed_target.scheme}://{parsed_target.netloc}{encoded_path}"
        return f"{base_url}?{query_string}" if query_string else base_url

    def filter_headers(self, headers: Dict[str, str], is_request: bool = True) -> Dict[str, str]:
        filtered = {}
        hop_by_hop = {header.lower() for header in self.config.hop_by_hop_headers}
        for key, value in headers.items():
            if key.lower() not in hop_by_hop:
                filtered[key] = value
        return filtered

    def add_forward_headers(self, headers: Dict[str, str], client_host: str, scheme: str) -> Dict[str, str]:
        original_host = headers.get("Host", "")
        if self.config.trust_forward_headers:
            existing_forwarded_for = headers.get("X-Forwarded-For", "").strip()
            if existing_forwarded_for and client_host:
                headers["X-Forwarded-For"] = f"{existing_forwarded_for}, {client_host}"
            else:
                headers["X-Forwarded-For"] = existing_forwarded_for or client_host

            headers["X-Forwarded-Proto"] = scheme
            headers["X-Forwarded-Host"] = original_host

        headers.pop("Host", None)
        return headers

    def is_streaming_content(self, headers: Dict[str, str], content_length: Optional[int] = None) -> bool:
        content_type = headers.get("Content-Type", "").lower()
        streaming_types = [
            "video/",
            "audio/",
            "application/octet-stream",
            "application/x-mpegurl",
            "application/vnd.apple.mpegurl",
            "application/dash+xml",
            "multipart/",
        ]
        if any(stream_type in content_type for stream_type in streaming_types):
            return True

        transfer_encoding = headers.get("Transfer-Encoding", "").lower()
        if "chunked" in transfer_encoding:
            return True

        if content_length is not None and content_length > self.config.streaming.large_file_threshold:
            return True

        return False

    def extract_client_ip(self, headers: Dict[str, str], client_host: str) -> str:
        candidates: List[str] = []
        for header_name in ("CF-Connecting-IP", "X-Real-IP", "X-Forwarded-For"):
            raw_value = headers.get(header_name, "")
            if not raw_value:
                continue
            for part in raw_value.split(","):
                candidate = part.strip()
                if candidate:
                    candidates.append(candidate)

        if client_host:
            candidates.append(client_host)

        parsed_candidates = []
        for candidate in candidates:
            try:
                parsed_candidates.append(ipaddress.ip_address(candidate))
            except ValueError:
                continue

        for candidate in parsed_candidates:
            if getattr(candidate, "is_global", False):
                return str(candidate)

        return str(parsed_candidates[0]) if parsed_candidates else client_host

    def _normalize_location(self, value: Optional[str]) -> str:
        return "".join((value or "").strip().lower().split())

    def _default_candidate(self, candidates: List[ProxyRule]) -> Optional[ProxyRule]:
        for rule in candidates:
            if rule.is_default:
                return rule
        for rule in candidates:
            if not rule.normalized_regions():
                return rule
        return None

    def _match_region(self, rule: ProxyRule, geo_location: GeoLocation) -> Optional[str]:
        filters = rule.normalized_regions()
        if not filters:
            return None

        haystacks = [
            self._normalize_location(geo_location.country),
            self._normalize_location(geo_location.region),
            self._normalize_location(geo_location.city),
            self._normalize_location(geo_location.full_text),
            self._normalize_location(geo_location.summary),
        ]
        haystacks = [value for value in haystacks if value]

        for region_name in filters:
            if any(region_name in haystack for haystack in haystacks):
                return region_name
        return None

    def _match_ip_whitelist(self, rule: ProxyRule, client_ip: str) -> Optional[str]:
        entries = rule.normalized_ip_whitelist()
        if not entries:
            return None

        try:
            parsed_client_ip = ipaddress.ip_address(client_ip)
        except ValueError:
            return None

        for entry in entries:
            try:
                if "/" in entry:
                    network = ipaddress.ip_network(entry, strict=False)
                    if parsed_client_ip in network:
                        return entry
                else:
                    if parsed_client_ip == ipaddress.ip_address(entry):
                        return entry
            except ValueError:
                continue
        return None

    def _match_ip_in_entries(self, entries: List[str], client_ip: str) -> Optional[str]:
        """通用IP匹配：检查 client_ip 是否在 entries 列表中（支持单IP和CIDR网段）。命中返回条目字符串。"""
        if not entries:
            return None
        try:
            parsed_client_ip = ipaddress.ip_address(client_ip)
        except ValueError:
            return None
        for entry in entries:
            try:
                if "/" in entry:
                    network = ipaddress.ip_network(entry, strict=False)
                    if parsed_client_ip in network:
                        return entry
                else:
                    if parsed_client_ip == ipaddress.ip_address(entry):
                        return entry
            except ValueError:
                continue
        return None

    def _match_region_in_whitelist(self, regions: List[str], geo_location: Optional[GeoLocation]) -> bool:
        """检查 geo_location 是否在地区白名单内。True=在白名单内（允许），False=不在（拒绝）。
        白名单为空视为不限制（返回True）；配了白名单但 geo_location 为 None 时保守拒绝。"""
        if not regions:
            return True
        if not geo_location:
            return False
        haystacks = [
            self._normalize_location(geo_location.country),
            self._normalize_location(geo_location.region),
            self._normalize_location(geo_location.city),
            self._normalize_location(geo_location.full_text),
            self._normalize_location(geo_location.summary),
        ]
        haystacks = [value for value in haystacks if value]
        for region_name in regions:
            if any(region_name in haystack for haystack in haystacks):
                return True
        return False

    def _evaluate_access_control(
        self,
        access_ip_whitelist: List[str],
        ip_blacklist: List[str],
        region_whitelist: List[str],
        region_blacklist: List[str],
        *,
        client_ip: str,
        geo_location: Optional[GeoLocation],
        scope_label: str,
    ) -> Optional[Tuple[str, str, str]]:
        """通用访问控制检查：按 IP白 → IP黑 → 地区白 → 地区黑 顺序判断。

        返回 None 表示放行；返回 (match_strategy, match_detail, block_reason) 表示拦截。
        """
        # 1. IP 白名单：有配置且 client_ip 不在白名单内 → 拦截
        if access_ip_whitelist:
            hit = self._match_ip_in_entries(access_ip_whitelist, client_ip)
            if not hit:
                logger.warning(
                    "%s IP白名单拦截: IP=%s 不在白名单内",
                    scope_label, client_ip,
                )
                return (
                    "blocked_by_access_ip_whitelist",
                    "access_ip_whitelist_not_matched",
                    f"{scope_label} IP白名单拒绝: IP={client_ip} 不在白名单内",
                )

        # 2. IP 黑名单：有配置且 client_ip 命中 → 拦截
        if ip_blacklist:
            hit_entry = self._match_ip_in_entries(ip_blacklist, client_ip)
            if hit_entry:
                logger.warning(
                    "%s IP黑名单拦截: IP=%s 命中=%s",
                    scope_label, client_ip, hit_entry,
                )
                return (
                    "blocked_by_ip_blacklist",
                    f"ip_blacklist_hit:{hit_entry}",
                    f"{scope_label} IP黑名单命中: {hit_entry}",
                )

        # 3. 地区白名单：有配置且 geo_location 不在白名单内 → 拦截
        if region_whitelist:
            if not self._match_region_in_whitelist(region_whitelist, geo_location):
                logger.warning(
                    "%s 地区白名单拦截: IP=%s 地区=%s",
                    scope_label, client_ip,
                    geo_location.summary if geo_location else "未知",
                )
                return (
                    "blocked_by_region_whitelist",
                    "region_whitelist_blocked",
                    f"{scope_label} 地区白名单拒绝: 地区={geo_location.summary if geo_location else '未知'}",
                )

        # 4. 地区黑名单：有配置且 geo_location 在黑名单内 → 拦截
        if region_blacklist:
            if self._match_region_in_whitelist(region_blacklist, geo_location):
                logger.warning(
                    "%s 地区黑名单拦截: IP=%s 地区=%s",
                    scope_label, client_ip,
                    geo_location.summary if geo_location else "未知",
                )
                return (
                    "blocked_by_region_blacklist",
                    "region_blacklist_hit",
                    f"{scope_label} 地区黑名单命中: 地区={geo_location.summary if geo_location else '未知'}",
                )

        return None

    def _apply_route_headers(self, headers: Dict[str, str], route_decision: RouteDecision) -> None:
        headers["X-Proxy-Rule-Id"] = str(route_decision.rule.rule_id or "")
        headers["X-Proxy-Rule-Source"] = route_decision.rule.source
        headers["X-Proxy-Match-Strategy"] = route_decision.match_strategy
        headers["X-Proxy-Region-Matching"] = "enabled" if route_decision.region_matching_enabled else "disabled"
        if route_decision.matched_ip_whitelist:
            headers["X-Proxy-Matched-IP-Whitelist"] = route_decision.matched_ip_whitelist
        if route_decision.matched_region:
            headers["X-Proxy-Matched-Region"] = route_decision.matched_region
        if route_decision.geo_location:
            headers["X-Proxy-Geo-Location"] = route_decision.geo_location.summary or route_decision.geo_location.full_text
            headers["X-Proxy-Geo-Source"] = route_decision.geo_location.source

    async def select_route(
        self,
        path: str,
        headers: Dict[str, str],
        client_host: str,
        query_string: Optional[str] = None,
    ) -> Optional[RouteDecision]:
        request_host = self.extract_request_host(headers)
        candidates = self.find_matching_rules(path, request_host=request_host)
        if not candidates:
            return None

        client_ip = self.extract_client_ip(headers, client_host)
        geo_location: Optional[GeoLocation] = None
        matched_region: Optional[str] = None
        matched_ip_whitelist: Optional[str] = None
        selected_rule: Optional[ProxyRule] = None
        match_strategy = "priority_fallback"
        match_detail = "priority_rule_selected"
        route_group = self.config.get_route_group(candidates[0].path_prefix, request_host)
        if route_group is None:
            route_group = self._find_route_group_for_host(candidates[0].path_prefix, request_host)
        candidate_host = normalize_request_host(candidates[0].request_host)
        if route_group is None and candidate_host:
            route_group = self.config.get_route_group(candidates[0].path_prefix, candidate_host)
        if route_group is None and candidate_host:
            route_group = self._find_route_group_for_host(candidates[0].path_prefix, candidate_host)
        if route_group is None:
            route_group = self.config.get_route_group(candidates[0].path_prefix, "")
        region_matching_enabled = bool(route_group.region_matching_enabled) if route_group else False

        # ===== 路由前缀级别访问控制（前缀4项检查：IP白名单 → IP黑名单 → 地区白名单 → 地区黑名单）=====
        if route_group and (
            route_group.normalized_access_ip_whitelist()
            or route_group.normalized_ip_blacklist()
            or route_group.normalized_region_whitelist()
            or route_group.normalized_region_blacklist()
        ):
            # 配置了任何地区类规则时需要解析 geo_location
            if (
                route_group.normalized_region_whitelist()
                or route_group.normalized_region_blacklist()
            ) and geo_location is None and self.geo_resolver:
                geo_start = time.perf_counter()
                geo_location = await self.geo_resolver.resolve(client_ip, self.config.geoip)
                geo_ms = (time.perf_counter() - geo_start) * 1000
                if geo_location:
                    logger.debug(
                        "GeoIP解析: IP=%s 结果=%s/%s/%s 耗时=%.1fms",
                        client_ip, geo_location.country, geo_location.region, geo_location.city, geo_ms,
                    )
                else:
                    logger.debug("GeoIP解析: IP=%s 无结果 耗时=%.1fms", client_ip, geo_ms)

            group_block = self._evaluate_access_control(
                route_group.normalized_access_ip_whitelist(),
                route_group.normalized_ip_blacklist(),
                route_group.normalized_region_whitelist(),
                route_group.normalized_region_blacklist(),
                client_ip=client_ip,
                geo_location=geo_location,
                scope_label=f"路由前缀 {candidates[0].path_prefix}",
            )
            if group_block is not None:
                strategy, detail, reason = group_block
                selected_rule = candidates[0]
                target_url = self.build_target_url(path, selected_rule, query_string)
                logger.warning(
                    "路由前缀访问控制拦截: IP=%s 路径=%s 前缀=%s 策略=%s 原因=%s",
                    client_ip, path, candidates[0].path_prefix, strategy, reason,
                )
                return RouteDecision(
                    rule=selected_rule,
                    target_url=target_url,
                    client_ip=client_ip,
                    geo_location=geo_location,
                    match_strategy=strategy,
                    region_matching_enabled=region_matching_enabled,
                    request_host=request_host,
                    rule_request_host=normalize_request_host(selected_rule.request_host),
                    match_detail=detail,
                    blocked=True,
                    block_reason=reason,
                )

        # 判断是否需要解析 geo_location：地区匹配开启 或 路由前缀/任意候选规则配了地区白/黑名单
        group_has_region_whitelist = bool(route_group and route_group.normalized_region_whitelist())
        group_has_region_blacklist = bool(route_group and route_group.normalized_region_blacklist())
        any_rule_has_region_whitelist = any(rule.normalized_region_whitelist() for rule in candidates)
        any_rule_has_region_blacklist = any(rule.normalized_region_blacklist() for rule in candidates)
        need_geo_resolution = (
            region_matching_enabled
            or group_has_region_whitelist
            or group_has_region_blacklist
            or any_rule_has_region_whitelist
            or any_rule_has_region_blacklist
        )

        whitelist_candidates = [rule for rule in candidates if rule.normalized_ip_whitelist()]
        regional_candidates = [rule for rule in candidates if rule.normalized_regions() and not rule.is_default]
        default_rule = self._default_candidate(candidates)

        if whitelist_candidates:
            for rule in whitelist_candidates:
                matched_ip_whitelist = self._match_ip_whitelist(rule, client_ip)
                if matched_ip_whitelist:
                    selected_rule = rule
                    match_strategy = "ip_whitelist_match"
                    match_detail = "matched_by_ip_whitelist"
                    break
            if selected_rule is None:
                match_detail = "ip_whitelist_not_matched"

        if selected_rule is None and region_matching_enabled:
            if regional_candidates and self.geo_resolver:
                geo_location = await self.geo_resolver.resolve(client_ip, self.config.geoip)
                if geo_location:
                    online_geo_cache_hit = (
                        geo_location.online_cache_hit
                        and str(geo_location.source or "").startswith("online:")
                    )
                    if online_geo_cache_hit:
                        match_detail = "geo_lookup_success_online_cache_but_no_region_match"
                    else:
                        match_detail = "geo_lookup_success_but_no_region_match"
                    for rule in regional_candidates:
                        matched_region = self._match_region(rule, geo_location)
                        if matched_region:
                            selected_rule = rule
                            match_strategy = "region_match"
                            if online_geo_cache_hit:
                                match_detail = "matched_by_region_filter_online_cache"
                            else:
                                match_detail = "matched_by_region_filter"
                            break
                else:
                    match_detail = "geo_lookup_failed_or_unavailable"
            elif regional_candidates and not self.geo_resolver:
                match_detail = "geo_resolver_unavailable"
            else:
                match_detail = "region_matching_enabled_without_regional_rules"
        else:
            match_detail = "region_matching_disabled"

        if selected_rule is None and default_rule is not None:
            selected_rule = default_rule
            match_strategy = "default_route"
            if match_detail == "priority_rule_selected":
                match_detail = "default_rule_selected"

        if selected_rule is None:
            selected_rule = candidates[0]
            if not match_detail:
                match_detail = "fallback_to_highest_priority_rule"

        # ===== 规则级别访问控制（规则4项检查：IP白名单 → IP黑名单 → 地区白名单 → 地区黑名单）=====
        if (
            selected_rule.normalized_access_ip_whitelist()
            or selected_rule.normalized_ip_blacklist()
            or selected_rule.normalized_region_whitelist()
            or selected_rule.normalized_region_blacklist()
        ):
            # 配置了任何地区类规则时需要解析 geo_location
            if (
                selected_rule.normalized_region_whitelist()
                or selected_rule.normalized_region_blacklist()
            ) and geo_location is None and self.geo_resolver:
                geo_location = await self.geo_resolver.resolve(client_ip, self.config.geoip)

            rule_block = self._evaluate_access_control(
                selected_rule.normalized_access_ip_whitelist(),
                selected_rule.normalized_ip_blacklist(),
                selected_rule.normalized_region_whitelist(),
                selected_rule.normalized_region_blacklist(),
                client_ip=client_ip,
                geo_location=geo_location,
                scope_label=f"规则#{selected_rule.rule_id}",
            )
            if rule_block is not None:
                strategy, detail, reason = rule_block
                logger.warning(
                    "规则访问控制拦截: IP=%s 路径=%s 规则ID=%s 策略=%s 原因=%s",
                    client_ip, path, selected_rule.rule_id, strategy, reason,
                )
                target_url = self.build_target_url(path, selected_rule, query_string)
                return RouteDecision(
                    rule=selected_rule,
                    target_url=target_url,
                    client_ip=client_ip,
                    geo_location=geo_location,
                    match_strategy=strategy,
                    region_matching_enabled=region_matching_enabled,
                    request_host=request_host,
                    rule_request_host=normalize_request_host(selected_rule.request_host),
                    match_detail=detail,
                    blocked=True,
                    block_reason=reason,
                )

        target_url = self.build_target_url(path, selected_rule, query_string)
        geo_source_for_log = geo_location.source if geo_location else "-"
        if (
            geo_location
            and geo_location.online_cache_hit
            and str(geo_source_for_log or "").startswith("online:")
        ):
            geo_source_for_log = f"{geo_source_for_log}|cache_hit"
        logger.info(
            "路由选择: 主机=%s 路径=%s 前缀=%s 客户端IP=%s 策略=%s 详情=%s 定位源=%s 目标=%s",
            request_host or "*",
            path,
            selected_rule.path_prefix,
            client_ip,
            match_strategy,
            match_detail,
            geo_source_for_log,
            target_url,
        )

        return RouteDecision(
            rule=selected_rule,
            target_url=target_url,
            client_ip=client_ip,
            geo_location=geo_location,
            match_strategy=match_strategy,
            region_matching_enabled=region_matching_enabled,
            request_host=request_host,
            rule_request_host=normalize_request_host(selected_rule.request_host),
            matched_region=matched_region,
            matched_ip_whitelist=matched_ip_whitelist,
            match_detail=match_detail,
        )

    async def stream_response(
        self,
        response: aiohttp.ClientResponse,
        chunk_size: int = 64 * 1024,
    ) -> AsyncGenerator[bytes, None]:
        try:
            async for chunk in response.content.iter_chunked(chunk_size):
                yield chunk
        except asyncio.CancelledError:
            logger.info("流式响应被下游客户端取消")
            raise
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as exc:
            logger.info("下游连接已关闭: %s", type(exc).__name__)
            raise
        except Exception as exc:
            logger.error("流式响应失败: %s", exc)
            raise
        finally:
            response.close()

    async def handle_request_streaming(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        client_host: str,
        scheme: str = "http",
        query_string: str = None,
        route_decision: Optional[RouteDecision] = None,
    ) -> StreamingResponse:
        route_decision = route_decision or await self.select_route(path, headers, client_host, query_string)
        if not route_decision:
            async def error_stream():
                yield '{"error": "未找到匹配的代理规则"}'.encode("utf-8")

            return StreamingResponse(
                status=404,
                headers={"Content-Type": "application/json"},
                body_stream=error_stream(),
                redirect_info=None,
            )

        rule = route_decision.rule
        target_url = route_decision.target_url
        client_ip = route_decision.client_ip or client_host

        session = await self.get_session()

        request_headers = self.filter_headers(headers, is_request=True)
        request_headers = self.add_forward_headers(request_headers, client_ip, scheme)

        if self.ip_cache:
            cached = await self.ip_cache.get(client_ip, target_url)
            if cached is not None:
                if cached.result_type == "redirect":
                    logger.info(
                            "请求结果缓存命中(重定向): IP=%s 目标=%s -> %s",
                            client_ip, target_url, cached.redirect_url,
                        )
                    return StreamingResponse(
                        status=cached.status_code,
                        headers={"Location": cached.redirect_url},
                        body_stream=self._empty_stream(),
                        redirect_info=None,
                        route_decision=route_decision,
                        cache_status="HIT_REDIRECT",
                    )
                elif cached.result_type == "streaming" and cached.final_url:
                    logger.info(
                            "请求结果缓存命中(流式): IP=%s 目标=%s 最终URL=%s",
                            client_ip, target_url, cached.final_url,
                        )
                    try:
                        direct_timeout = aiohttp.ClientTimeout(
                            total=rule.timeout,
                            connect=rule.timeout,
                            sock_read=self.config.streaming.stream_timeout,
                        )
                        async with session.get(
                            cached.final_url,
                            headers=request_headers,
                            allow_redirects=False,
                            timeout=direct_timeout,
                        ) as direct_response:
                            if direct_response.status in (200, 206):
                                resp_headers = dict(direct_response.headers)
                                filtered_headers = self.filter_headers(resp_headers, is_request=False)
                                self._apply_route_headers(filtered_headers, route_decision)
                                if "Accept-Ranges" not in filtered_headers:
                                    filtered_headers["Accept-Ranges"] = "bytes"
                                content_length = None
                                if "Content-Length" in resp_headers:
                                    try:
                                        content_length = int(resp_headers["Content-Length"])
                                    except ValueError:
                                        content_length = None
                                return StreamingResponse(
                                    status=direct_response.status,
                                    headers=filtered_headers,
                                    body_stream=self.stream_response(direct_response, self.config.streaming.chunk_size),
                                    redirect_info=None,
                                    content_length=content_length,
                                    route_decision=route_decision,
                                    cache_status="HIT_STREAMING",
                                )
                    except Exception as exc:
                        logger.warning("请求结果缓存直接请求失败，回退到正常流程: %s", exc)

        logger.debug("IP缓存未命中: IP=%s 目标=%s", client_ip, target_url)

        redirect_handler = RedirectHandler(
            max_redirects=rule.max_redirects,
            timeout=rule.timeout,
            stream_timeout=self.config.streaming.stream_timeout,
            follow_redirects=rule.follow_redirects,
        )

        retry_count = 0
        last_error = None
        last_error_type = None

        while retry_count < rule.retry_times:
            try:
                response, redirect_info = await redirect_handler.follow_redirects_streaming(
                    url=target_url,
                    method=method,
                    headers=request_headers,
                    body=body,
                    session=session,
                )

                if response is None:
                    if self.ip_cache and redirect_info and redirect_info.redirect_url:
                        await self.ip_cache.put_redirect(
                            client_ip, target_url,
                            redirect_info.status_code or 302,
                            redirect_info.redirect_url,
                        )
                    async def error_stream():
                        yield _build_500_bytes("重定向次数超限")

                    return StreamingResponse(
                        status=500,
                        headers={"Content-Type": "text/html; charset=utf-8"},
                        body_stream=error_stream(),
                        redirect_info=redirect_info,
                        route_decision=route_decision,
                        cache_status="BYPASS",
                    )

                response_headers = dict(response.headers)
                filtered_headers = self.filter_headers(response_headers, is_request=False)
                self._apply_route_headers(filtered_headers, route_decision)

                content_length = None
                if "Content-Length" in response_headers:
                    try:
                        content_length = int(response_headers["Content-Length"])
                    except ValueError:
                        content_length = None

                if "Accept-Ranges" not in filtered_headers:
                    filtered_headers["Accept-Ranges"] = "bytes"

                final_url = str(response.url) if response.url else target_url
                if self.ip_cache and response.status in (200, 206):
                    await self.ip_cache.put_streaming(
                        client_ip, target_url,
                        response.status, final_url, filtered_headers,
                    )
                elif self.ip_cache and redirect_info and redirect_info.redirect_url:
                    await self.ip_cache.put_redirect(
                        client_ip, target_url,
                        redirect_info.status_code or 302,
                        redirect_info.redirect_url,
                    )

                return StreamingResponse(
                    status=response.status,
                    headers=filtered_headers,
                    body_stream=self.stream_response(response, self.config.streaming.chunk_size),
                    redirect_info=redirect_info,
                    content_length=content_length,
                    route_decision=route_decision,
                    cache_status="BYPASS",
                )
            except asyncio.TimeoutError as exc:
                last_error = str(exc)
                last_error_type = "TimeoutError"
            except aiohttp.ClientError as exc:
                last_error = str(exc)
                last_error_type = type(exc).__name__
            except Exception as exc:
                last_error = str(exc)
                last_error_type = type(exc).__name__

            retry_count += 1
            logger.warning(
                "流式上游重试 %s/%s %s 由于 %s: %s",
                retry_count,
                rule.retry_times,
                target_url,
                last_error_type,
                last_error,
            )
            await asyncio.sleep(1)

        async def error_stream():
            yield _build_500_bytes("上游重试耗尽")

        return StreamingResponse(
            status=500,
            headers={"Content-Type": "text/html; charset=utf-8"},
            body_stream=error_stream(),
            redirect_info=None,
            route_decision=route_decision,
            cache_status="BYPASS",
        )

    @staticmethod
    async def _empty_stream() -> AsyncGenerator[bytes, None]:
        return
        yield b""

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        client_host: str,
        scheme: str = "http",
        query_string: str = None,
        route_decision: Optional[RouteDecision] = None,
    ) -> Tuple[int, Dict[str, str], bytes, Optional[RedirectInfo], Optional[RouteDecision]]:
        self._last_cache_status = "BYPASS"
        route_decision = route_decision or await self.select_route(path, headers, client_host, query_string)
        if not route_decision:
            error_body = '{"error": "未找到匹配的代理规则"}'.encode("utf-8")
            return 404, {"Content-Type": "application/json"}, error_body, None, None

        rule = route_decision.rule
        target_url = route_decision.target_url
        client_ip = route_decision.client_ip or client_host

        session = await self.get_session()

        request_headers = self.filter_headers(headers, is_request=True)
        request_headers = self.add_forward_headers(request_headers, client_ip, scheme)

        if self.ip_cache:
            cached = await self.ip_cache.get(client_ip, target_url)
            if cached is not None and cached.result_type == "redirect" and cached.redirect_url:
                logger.info(
                        "请求结果缓存命中(重定向): IP=%s 目标=%s -> %s",
                        client_ip, target_url, cached.redirect_url,
                    )
                self._last_cache_status = "HIT_REDIRECT"
                return (
                    cached.status_code,
                    {"Location": cached.redirect_url},
                    b"",
                    None,
                    route_decision,
                )

        logger.debug("IP缓存未命中: IP=%s 目标=%s", client_ip, target_url)

        redirect_handler = RedirectHandler(
            max_redirects=rule.max_redirects,
            timeout=rule.timeout,
            follow_redirects=rule.follow_redirects,
        )

        retry_count = 0
        last_error = None
        last_error_type = None

        while retry_count < rule.retry_times:
            try:
                response, redirect_info = await redirect_handler.follow_redirects(
                    url=target_url,
                    method=method,
                    headers=request_headers,
                    body=body,
                    session=session,
                )

                if response is None:
                    if self.ip_cache and redirect_info and redirect_info.redirect_url:
                        await self.ip_cache.put_redirect(
                            client_ip, target_url,
                            redirect_info.status_code or 302,
                            redirect_info.redirect_url,
                        )
                    error_body = _build_500_bytes("重定向次数超限")
                    return 500, {"Content-Type": "text/html; charset=utf-8"}, error_body, redirect_info, route_decision

                response_headers = dict(response.headers)
                filtered_headers = self.filter_headers(response_headers, is_request=False)
                self._apply_route_headers(filtered_headers, route_decision)
                response_body = await response.read()
                response.close()

                final_url = str(response.url) if response.url else target_url
                if self.ip_cache and response.status in (200, 206):
                    await self.ip_cache.put_streaming(
                        client_ip, target_url,
                        response.status, final_url, filtered_headers,
                    )
                elif self.ip_cache and redirect_info and redirect_info.redirect_url:
                    await self.ip_cache.put_redirect(
                        client_ip, target_url,
                        redirect_info.status_code or 302,
                        redirect_info.redirect_url,
                    )

                return response.status, filtered_headers, response_body, redirect_info, route_decision
            except asyncio.TimeoutError as exc:
                last_error = str(exc)
                last_error_type = "TimeoutError"
            except aiohttp.ClientError as exc:
                last_error = str(exc)
                last_error_type = type(exc).__name__
            except Exception as exc:
                last_error = str(exc)
                last_error_type = type(exc).__name__

            retry_count += 1
            logger.warning(
                "上游重试 %s/%s %s 由于 %s: %s",
                retry_count,
                rule.retry_times,
                target_url,
                last_error_type,
                last_error,
            )
            await asyncio.sleep(1)

        error_body = _build_500_bytes("上游重试耗尽")
        return 500, {"Content-Type": "text/html; charset=utf-8"}, error_body, None, route_decision


class ProxyStats:
    def __init__(self):
        self.start_time = time.time()
        self.total_requests = 0
        self.failed_requests = 0
        self.redirected_requests = 0
        self.total_redirect_count = 0
        self.streaming_requests = 0
        self.total_bytes = 0
        self._lock = asyncio.Lock()

    async def record_request(
        self,
        redirected: bool = False,
        redirect_count: int = 0,
        failed: bool = False,
        streaming: bool = False,
        bytes_count: int = 0,
    ) -> None:
        async with self._lock:
            self.total_requests += 1
            if failed:
                self.failed_requests += 1
            if redirected:
                self.redirected_requests += 1
            if redirect_count:
                self.total_redirect_count += redirect_count
            if streaming:
                self.streaming_requests += 1
            if bytes_count:
                self.total_bytes += bytes_count

    def get_stats(self) -> Dict[str, float]:
        uptime = max(time.time() - self.start_time, 0)
        return {
            "uptime_seconds": uptime,
            "total_requests": self.total_requests,
            "failed_requests": self.failed_requests,
            "redirected_requests": self.redirected_requests,
            "total_redirect_count": self.total_redirect_count,
            "streaming_requests": self.streaming_requests,
            "total_bytes": self.total_bytes,
            "requests_per_second": round(self.total_requests / uptime, 4) if uptime else 0,
        }
