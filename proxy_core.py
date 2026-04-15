from __future__ import annotations

import aiohttp
import asyncio
import ipaddress
import logging
import time
from dataclasses import dataclass
from typing import AsyncGenerator, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

from config import Config, ProxyRule, normalize_request_host, split_request_hosts
from geo_service import GeoLocation, GeoResolver


logger = logging.getLogger("proxy")


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
    matched_region: Optional[str] = None
    matched_ip_whitelist: Optional[str] = None
    match_detail: str = ""


@dataclass
class StreamingResponse:
    status: int
    headers: Dict[str, str]
    body_stream: Optional[AsyncGenerator[bytes, None]]
    redirect_info: Optional[RedirectInfo]
    content_length: Optional[int] = None
    route_decision: Optional[RouteDecision] = None


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

                if response.status in {301, 302, 303} and method.upper() not in {"GET", "HEAD"}:
                    method = "GET"
                    body = None

                response.release()
                current_url = redirect_url

            logger.warning("Exceeded max redirects (%s): %s", self.max_redirects, url)
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
    def __init__(self, config: Config, geo_resolver: Optional[GeoResolver] = None):
        self.config = config
        self.geo_resolver = geo_resolver
        self._session: Optional[aiohttp.ClientSession] = None
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

    def extract_request_host(self, headers: Dict[str, str]) -> str:
        request_host = self._get_header_value(headers, "Host")
        if self.config.trust_forward_headers:
            forwarded_host = self._get_header_value(headers, "X-Forwarded-Host")
            if forwarded_host:
                request_host = forwarded_host.split(",", 1)[0].strip() or request_host
        return normalize_request_host(request_host)

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
        if rule.strip_prefix:
            remaining_path = path[len(rule.path_prefix):]
            if not remaining_path.startswith("/"):
                remaining_path = "/" + remaining_path
            new_path = parsed_target.path + remaining_path
        else:
            new_path = parsed_target.path + path

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

        target_url = self.build_target_url(path, selected_rule, query_string)
        geo_source_for_log = geo_location.source if geo_location else "-"
        if (
            geo_location
            and geo_location.online_cache_hit
            and str(geo_source_for_log or "").startswith("online:")
        ):
            geo_source_for_log = f"{geo_source_for_log}|cache_hit"
        logger.info(
            "Route selected: host=%s path=%s prefix=%s client_ip=%s strategy=%s detail=%s geo_source=%s target=%s",
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
            logger.info("Streaming response cancelled by downstream client")
            raise
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as exc:
            logger.info("Downstream connection closed: %s", type(exc).__name__)
            raise
        except Exception as exc:
            logger.error("Streaming response failed: %s", exc)
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
                yield b'{"error": "No matching proxy rule found"}'

            return StreamingResponse(
                status=404,
                headers={"Content-Type": "application/json"},
                body_stream=error_stream(),
                redirect_info=None,
            )

        rule = route_decision.rule
        target_url = route_decision.target_url
        session = await self.get_session()

        request_headers = self.filter_headers(headers, is_request=True)
        request_headers = self.add_forward_headers(request_headers, route_decision.client_ip or client_host, scheme)

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
                    async def error_stream():
                        yield b'{"error": "Too many redirects"}'

                    return StreamingResponse(
                        status=502,
                        headers={"Content-Type": "application/json"},
                        body_stream=error_stream(),
                        redirect_info=redirect_info,
                        route_decision=route_decision,
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

                return StreamingResponse(
                    status=response.status,
                    headers=filtered_headers,
                    body_stream=self.stream_response(response, self.config.streaming.chunk_size),
                    redirect_info=redirect_info,
                    content_length=content_length,
                    route_decision=route_decision,
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
                "Streaming upstream retry %s/%s for %s due to %s: %s",
                retry_count,
                rule.retry_times,
                target_url,
                last_error_type,
                last_error,
            )
            await asyncio.sleep(1)

        async def error_stream():
            yield f'{{"error": "Failed after {rule.retry_times} retries: {last_error_type}: {last_error}"}}'.encode()

        return StreamingResponse(
            status=502,
            headers={"Content-Type": "application/json"},
            body_stream=error_stream(),
            redirect_info=None,
            route_decision=route_decision,
        )

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
        route_decision = route_decision or await self.select_route(path, headers, client_host, query_string)
        if not route_decision:
            error_body = b'{"error": "No matching proxy rule found"}'
            return 404, {"Content-Type": "application/json"}, error_body, None, None

        rule = route_decision.rule
        target_url = route_decision.target_url
        session = await self.get_session()

        request_headers = self.filter_headers(headers, is_request=True)
        request_headers = self.add_forward_headers(request_headers, route_decision.client_ip or client_host, scheme)
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
                    error_body = b'{"error": "Too many redirects"}'
                    return 502, {"Content-Type": "application/json"}, error_body, redirect_info, route_decision

                response_headers = dict(response.headers)
                filtered_headers = self.filter_headers(response_headers, is_request=False)
                self._apply_route_headers(filtered_headers, route_decision)
                response_body = await response.read()
                response.close()
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
                "Upstream retry %s/%s for %s due to %s: %s",
                retry_count,
                rule.retry_times,
                target_url,
                last_error_type,
                last_error,
            )
            await asyncio.sleep(1)

        error_body = f'{{"error": "Failed after {rule.retry_times} retries: {last_error_type}: {last_error}"}}'.encode()
        return 502, {"Content-Type": "application/json"}, error_body, None, route_decision


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
