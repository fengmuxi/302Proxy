from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional
from urllib.parse import quote, urlsplit, urlunsplit

import aiohttp

from config import GeoIPSettings, OfflineGeoIPSettings, OnlineGeoIPSource

try:
    import geoip2.database
    from geoip2.errors import AddressNotFoundError

    GEOIP2_AVAILABLE = True
except ImportError:
    geoip2 = None
    AddressNotFoundError = Exception
    GEOIP2_AVAILABLE = False


logger = logging.getLogger("proxy")


def deep_get(data: Any, path: str, default: Any = None) -> Any:
    if not path:
        return default

    current = data
    normalized = path.replace("[", ".").replace("]", "")
    for part in normalized.split("."):
        if not part:
            continue
        if isinstance(current, dict):
            if part not in current:
                return default
            current = current[part]
            continue
        if isinstance(current, list):
            try:
                index = int(part)
            except ValueError:
                return default
            if index < 0 or index >= len(current):
                return default
            current = current[index]
            continue
        return default
    return current


@dataclass
class GeoLocation:
    ip: str
    country: str = ""
    region: str = ""
    city: str = ""
    source: str = ""
    full_text: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)
    online_cache_hit: bool = False

    @property
    def summary(self) -> str:
        parts = [self.country, self.region, self.city]
        return " / ".join([part for part in parts if part]) or self.full_text


class GeoResolver:
    ONLINE_CACHE_MAX_ENTRIES = 1000

    def __init__(self, session_provider: Optional[Callable[[], Awaitable[aiohttp.ClientSession]]] = None):
        self._session_provider = session_provider
        self._offline_reader = None
        self._offline_db_path: Optional[str] = None
        self._offline_db_mtime_ns: Optional[int] = None
        self._online_cache: Dict[str, tuple[float, GeoLocation]] = {}
        self._online_cache_ttl_seconds = 120
        self._weighted_cursor = 0

    def set_session_provider(self, session_provider: Callable[[], Awaitable[aiohttp.ClientSession]]) -> None:
        self._session_provider = session_provider

    def set_online_cache_ttl_seconds(self, ttl_seconds: int, *, reset_existing: bool = False) -> None:
        normalized_ttl = max(0, int(ttl_seconds or 0))
        if reset_existing or normalized_ttl != self._online_cache_ttl_seconds:
            self._online_cache.clear()
        self._online_cache_ttl_seconds = normalized_ttl

    async def resolve(self, ip_address_text: str, settings: GeoIPSettings) -> Optional[GeoLocation]:
        if not ip_address_text or not settings.enabled:
            return None

        parsed_ip = self._parse_ip(ip_address_text)
        if parsed_ip is None:
            logger.warning("Skipping geo lookup because client IP is invalid: %s", ip_address_text)
            return None

        ip_text = str(parsed_ip)

        online_sources = [source for source in settings.sources if source.enabled and source.url]
        if online_sources:
            ttl_seconds = max(0, int(getattr(settings, "online_cache_ttl_seconds", self._online_cache_ttl_seconds) or 0))
            self._online_cache_ttl_seconds = ttl_seconds
            if ttl_seconds > 0:
                cached_location = self._get_online_cache(ip_text, online_sources)
                if cached_location:
                    return cached_location
            location = await self._resolve_online(ip_text, online_sources)
            if location:
                if ttl_seconds > 0:
                    self._set_online_cache(ip_text, online_sources, location)
                return location

        if settings.offline.enabled:
            location = await self._resolve_offline(ip_text, settings)
            if location:
                return location

        logger.warning("Geo lookup failed for %s. Falling back to default route.", ip_text)
        return None

    async def test_online_source(self, ip_address_text: str, source: OnlineGeoIPSource) -> Dict[str, Any]:
        if not str(source.url or "").strip():
            raise ValueError("在线定位源接口地址不能为空。")

        parsed_ip = self._parse_ip(ip_address_text)
        if parsed_ip is None:
            raise ValueError("测试 IP 格式无效，请输入合法的 IPv4 或 IPv6 地址。")

        ip_text = str(parsed_ip)
        location, message, upstream_response = await self._resolve_online_source_detailed(ip_text, source)
        provider_name = source.name or source.url

        return {
            "success": location is not None,
            "stage": "online",
            "provider": provider_name,
            "message": message,
            "location": self._serialize_location(location) if location else None,
            "upstream_response": upstream_response,
        }

    async def test_offline_database(
        self,
        ip_address_text: str,
        settings: GeoIPSettings | OfflineGeoIPSettings,
    ) -> Dict[str, Any]:
        offline_settings = settings.offline if isinstance(settings, GeoIPSettings) else settings
        parsed_ip = self._parse_ip(ip_address_text)
        if parsed_ip is None:
            raise ValueError("测试 IP 格式无效，请输入合法的 IPv4 或 IPv6 地址。")

        ip_text = str(parsed_ip)
        location, message = await self._resolve_offline_detailed(ip_text, offline_settings)
        return {
            "success": location is not None,
            "stage": "offline",
            "provider": "offline_mmdb",
            "message": message,
            "location": self._serialize_location(location) if location else None,
        }

    async def close(self) -> None:
        reader = self._offline_reader
        self._offline_reader = None
        self._offline_db_path = None
        self._offline_db_mtime_ns = None
        self._online_cache.clear()
        if reader is not None:
            reader.close()

    async def invalidate_offline_cache(self) -> None:
        reader = self._offline_reader
        self._offline_reader = None
        self._offline_db_path = None
        self._offline_db_mtime_ns = None
        if reader is not None:
            reader.close()

    def clear_online_cache(self) -> Dict[str, int]:
        cleared_count = len(self._online_cache)
        self._online_cache.clear()
        return {"cleared_count": cleared_count}

    def get_online_cache_summary(self) -> Dict[str, int]:
        self._purge_expired_online_cache()
        return {
            "ttl_seconds": max(0, int(self._online_cache_ttl_seconds or 0)),
            "cached_entries": len(self._online_cache),
            "max_entries": self.ONLINE_CACHE_MAX_ENTRIES,
        }

    async def _resolve_online(self, ip_text: str, sources: List[OnlineGeoIPSource]) -> Optional[GeoLocation]:
        ordered_sources = self._select_online_attempts(sources)
        if not ordered_sources:
            return None

        for source in ordered_sources:
            location = await self._resolve_online_source(ip_text, source)
            if location:
                return location
        return None

    def _select_online_attempts(self, sources: List[OnlineGeoIPSource]) -> List[OnlineGeoIPSource]:
        weighted_sources: List[OnlineGeoIPSource] = []
        ordered_sources = sorted(sources, key=lambda item: (-int(item.priority or 0), item.name or item.url))
        for source in ordered_sources:
            weighted_sources.extend([source] * max(1, int(source.weight or 1)))

        if not weighted_sources:
            return []

        primary_index = self._weighted_cursor % len(weighted_sources)
        self._weighted_cursor += 1
        primary_source = weighted_sources[primary_index]
        attempts = [primary_source]

        for offset in range(1, len(weighted_sources)):
            fallback_source = weighted_sources[(primary_index + offset) % len(weighted_sources)]
            if fallback_source.url != primary_source.url or fallback_source.name != primary_source.name:
                attempts.append(fallback_source)
                break

        return attempts

    async def _resolve_online_source(self, ip_text: str, source: OnlineGeoIPSource) -> Optional[GeoLocation]:
        location, _, _ = await self._resolve_online_source_detailed(ip_text, source)
        return location

    async def _resolve_online_source_detailed(
        self,
        ip_text: str,
        source: OnlineGeoIPSource,
    ) -> tuple[Optional[GeoLocation], str, Dict[str, Any]]:
        own_session = self._session_provider is None
        session = await self._get_session(source.timeout)
        headers = self._parse_json(source.headers_json, {})
        request_kwargs: Dict[str, Any] = {
            "headers": headers,
            "ssl": False,
            "timeout": aiohttp.ClientTimeout(total=source.timeout),
        }
        payload: Any = None
        upstream_response: Dict[str, Any] = {
            "status": None,
            "ok": False,
            "content_type": "",
            "payload": None,
            "error": "",
        }
        method = (source.method or "GET").upper()
        request_location = self._normalize_request_location(method, source.request_location)
        request_url = self._build_online_request_url(source.url, ip_text, request_location)

        try:
            request_kwargs.update(
                self._build_online_request_kwargs(source, ip_text, request_location=request_location)
            )
            logger.info("Online geo source '%s' request: url=%s kwargs=%s", source.name or source.url, request_url, request_kwargs)
            async with session.request(method, request_url, **request_kwargs) as response:
                upstream_response["status"] = int(response.status)
                upstream_response["ok"] = 200 <= response.status < 300
                upstream_response["content_type"] = str(response.headers.get("Content-Type", ""))
                response_bytes = await response.read()
                response_charset = response.charset or "utf-8"
                try:
                    response_text = response_bytes.decode(response_charset, errors="replace")
                except LookupError:
                    response_text = response_bytes.decode("utf-8", errors="replace")
                payload = self._parse_online_source_payload(response_text)
                upstream_response["payload"] = payload
                if response.status >= 400:
                    logger.error(
                        "Online geo source '%s' failed for %s: HTTP %s",
                        source.name or source.url,
                        ip_text,
                        response.status,
                    )
                    return None, f"在线源请求失败: HTTP {response.status}", upstream_response
        except Exception as exc:
            logger.error("Online geo source '%s' failed for %s: %s", source.name or source.url, ip_text, exc)
            upstream_response["error"] = str(exc)
            return None, f"在线源请求失败: {exc}", upstream_response
        finally:
            if own_session:
                await session.close()

        logger.info("Online geo source '%s' response: %s", source.name or source.url, payload)
        country = self._stringify(deep_get(payload, source.country_path, ""))
        region = self._stringify(deep_get(payload, source.region_path, ""))
        city = self._stringify(deep_get(payload, source.city_path, ""))
        full_text = self._stringify(deep_get(payload, source.full_path, ""))
        if not any([country, region, city, full_text]):
            logger.warning("Online geo source '%s' returned no usable location for %s.", source.name or source.url, ip_text)
            return None, "接口调用成功，但未根据字段路径提取到有效区域信息。", upstream_response

        return (
            GeoLocation(
                ip=ip_text,
                country=country,
                region=region,
                city=city,
                source=f"online:{source.name or source.url}",
                full_text=full_text,
                raw=payload if isinstance(payload, dict) else {"payload": payload},
            ),
            "在线定位测试成功。",
            upstream_response,
        )

    def _build_online_request_kwargs(
        self,
        source: OnlineGeoIPSource,
        ip_text: str,
        *,
        request_location: Optional[str] = None,
    ) -> Dict[str, Any]:
        method = (source.method or "GET").upper()
        resolved_location = self._normalize_request_location(method, request_location or source.request_location)

        request_kwargs: Dict[str, Any] = {}
        query_params = self._build_query_params(source, ip_text, include_ip=resolved_location == "query")
        if query_params:
            request_kwargs["params"] = query_params

        if resolved_location in {"query", "path"}:
            return request_kwargs

        rendered = self._render_template_text(source.body_template or "", ip_text)
        body_format = (source.body_format or "json").lower()
        if rendered:
            if body_format == "json":
                try:
                    request_kwargs["json"] = json.loads(rendered)
                    return request_kwargs
                except json.JSONDecodeError:
                    request_kwargs["data"] = rendered
                    return request_kwargs
            if body_format == "form":
                try:
                    parsed = json.loads(rendered)
                    if isinstance(parsed, dict):
                        request_kwargs["data"] = parsed
                        return request_kwargs
                except json.JSONDecodeError:
                    request_kwargs["data"] = rendered
                    return request_kwargs
                request_kwargs["data"] = rendered
                return request_kwargs
            request_kwargs["data"] = rendered
            return request_kwargs

        if body_format == "form":
            request_kwargs["data"] = {source.ip_param_name: ip_text}
            return request_kwargs
        request_kwargs["json"] = {source.ip_param_name: ip_text}
        return request_kwargs

    def _build_online_request_url(
        self,
        raw_url: str,
        ip_text: str,
        request_location: str,
    ) -> str:
        base_url = str(raw_url or "").strip()
        if request_location != "path":
            return self._render_template_text(base_url, ip_text)

        rendered_url, has_ip_placeholder = self._render_path_template_text(base_url, ip_text)
        if has_ip_placeholder:
            return rendered_url
        return self._append_url_path_segment(rendered_url, quote(ip_text, safe=""))

    def _normalize_request_location(self, method: str, request_location: str) -> str:
        normalized_location = str(request_location or "query").strip().lower() or "query"
        if normalized_location not in {"query", "body", "path"}:
            normalized_location = "query"
        if method.upper() == "GET" and normalized_location == "body":
            return "query"
        return normalized_location

    def _render_path_template_text(self, text: str, ip_text: str) -> tuple[str, bool]:
        rendered = str(text or "")
        encoded_ip = quote(ip_text, safe="")
        placeholder_applied = False
        for token in ("{{ip}}", "{ip}"):
            if token in rendered:
                rendered = rendered.replace(token, encoded_ip)
                placeholder_applied = True
        rendered = rendered.replace("{{timestamp}}", datetime.now(timezone.utc).isoformat(timespec="seconds"))
        return rendered, placeholder_applied

    def _append_url_path_segment(self, url_text: str, segment: str) -> str:
        parsed = urlsplit(url_text)
        path = parsed.path or ""
        if not path.endswith("/"):
            path = f"{path}/" if path else "/"
        path = f"{path}{segment}"
        return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment))

    def _build_query_params(
        self,
        source: OnlineGeoIPSource,
        ip_text: str,
        include_ip: bool = True,
    ) -> Dict[str, str]:
        raw_params = self._parse_json(source.query_params_json, {})
        rendered_params = self._render_template_data(raw_params, ip_text)
        params: Dict[str, str] = {}

        if isinstance(rendered_params, dict):
            for key, value in rendered_params.items():
                normalized_key = str(key).strip()
                if not normalized_key or value is None:
                    continue
                params[normalized_key] = self._stringify_query_value(value)

        if include_ip:
            ip_param_name = str(source.ip_param_name or "ip").strip() or "ip"
            params.setdefault(ip_param_name, ip_text)

        return params

    async def _resolve_offline(self, ip_text: str, settings: GeoIPSettings) -> Optional[GeoLocation]:
        location, _ = await self._resolve_offline_detailed(ip_text, settings.offline)
        return location

    async def _resolve_offline_detailed(
        self,
        ip_text: str,
        offline_settings: OfflineGeoIPSettings,
    ) -> tuple[Optional[GeoLocation], str]:
        if not GEOIP2_AVAILABLE:
            logger.error("Offline geo lookup requested but geoip2 is not installed.")
            return None, "离线定位依赖 geoip2 尚未安装。"

        db_path = (offline_settings.db_path or "").strip()
        if not db_path:
            logger.warning("Offline geo lookup is enabled but no local database path is configured.")
            return None, "尚未配置离线 IP 库本地路径。"

        path = Path(db_path)
        if not path.exists():
            logger.error("Offline geo database does not exist: %s", path)
            return None, "离线 IP 库文件不存在，请先执行同步下载。"

        try:
            location = await asyncio.to_thread(self._read_offline_lookup, ip_text, str(path), offline_settings.locale)
            if location is None:
                return None, "离线 IP 库中未找到该 IP 的定位记录。"
            return location, "离线定位测试成功。"
        except Exception as exc:
            logger.error("Offline geo lookup failed for %s: %s", ip_text, exc)
            return None, f"离线定位失败: {exc}"

    async def _get_session(self, timeout_seconds: int) -> aiohttp.ClientSession:
        if self._session_provider is None:
            return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_seconds))
        return await self._session_provider()

    def _read_offline_lookup(self, ip_text: str, db_path: str, locale: str) -> Optional[GeoLocation]:
        reader = self._get_offline_reader(db_path)
        try:
            record = reader.city(ip_text)
        except AddressNotFoundError:
            logger.warning("Offline geo database has no record for %s", ip_text)
            return None

        country = self._localized_name(getattr(record, "country", None), locale)
        region = ""
        subdivisions = getattr(record, "subdivisions", None)
        if subdivisions and getattr(subdivisions, "most_specific", None):
            region = self._localized_name(subdivisions.most_specific, locale)
        city = self._localized_name(getattr(record, "city", None), locale)

        return GeoLocation(
            ip=ip_text,
            country=country,
            region=region,
            city=city,
            source="offline_mmdb",
            raw={},
        )

    def _get_offline_reader(self, db_path: str):
        current_mtime_ns = Path(db_path).stat().st_mtime_ns
        if (
            self._offline_reader is None
            or self._offline_db_path != db_path
            or self._offline_db_mtime_ns != current_mtime_ns
        ):
            if self._offline_reader is not None:
                self._offline_reader.close()
            self._offline_reader = geoip2.database.Reader(db_path)
            self._offline_db_path = db_path
            self._offline_db_mtime_ns = current_mtime_ns
        return self._offline_reader

    def _get_online_cache(self, ip_text: str, sources: List[OnlineGeoIPSource]) -> Optional[GeoLocation]:
        self._purge_expired_online_cache()
        cache_key = self._build_online_cache_key(ip_text, sources)
        cached = self._online_cache.get(cache_key)
        if not cached:
            return None
        expires_at, location = cached
        if expires_at <= time.monotonic():
            self._online_cache.pop(cache_key, None)
            return None
        cached_location = replace(location, online_cache_hit=True)
        logger.info(
            "Online geo cache hit: ip=%s source=%s summary=%s",
            ip_text,
            cached_location.source or "online:unknown",
            cached_location.summary or "-",
        )
        return cached_location

    def _set_online_cache(self, ip_text: str, sources: List[OnlineGeoIPSource], location: GeoLocation) -> None:
        self._purge_expired_online_cache()
        cache_key = self._build_online_cache_key(ip_text, sources)
        self._online_cache[cache_key] = (
            time.monotonic() + max(0, int(self._online_cache_ttl_seconds or 0)),
            replace(location, online_cache_hit=False),
        )
        while len(self._online_cache) > self.ONLINE_CACHE_MAX_ENTRIES:
            oldest_key = next(iter(self._online_cache))
            self._online_cache.pop(oldest_key, None)

    def _build_online_cache_key(self, ip_text: str, sources: List[OnlineGeoIPSource]) -> str:
        signature = [
            (
                source.name,
                source.url,
                source.method,
                source.request_location,
                source.body_format,
                source.query_params_json,
                source.headers_json,
                source.body_template,
                source.ip_param_name,
                source.country_path,
                source.region_path,
                source.city_path,
                source.full_path,
                source.weight,
                source.priority,
            )
            for source in sources
        ]
        return f"{ip_text}|{json.dumps(signature, ensure_ascii=False, sort_keys=False)}"

    def _purge_expired_online_cache(self) -> None:
        now = time.monotonic()
        expired_keys = [key for key, (expires_at, _) in self._online_cache.items() if expires_at <= now]
        for key in expired_keys:
            self._online_cache.pop(key, None)

    def _localized_name(self, node: Any, locale: str) -> str:
        if node is None:
            return ""
        names = getattr(node, "names", None) or {}
        if locale in names:
            return self._stringify(names.get(locale))
        if "zh-CN" in names:
            return self._stringify(names.get("zh-CN"))
        if "en" in names:
            return self._stringify(names.get("en"))
        return self._stringify(getattr(node, "name", ""))

    def _parse_json(self, text: str, default: Dict[str, Any]) -> Dict[str, Any]:
        if not text:
            return default
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("Failed to parse geo headers JSON, using default headers.")
            return default
        return parsed if isinstance(parsed, dict) else default

    def _parse_online_source_payload(self, response_text: str) -> Any:
        if not response_text:
            return {}
        try:
            return json.loads(response_text)
        except json.JSONDecodeError:
            return response_text

    def _render_template_text(self, text: str, ip_text: str) -> str:
        rendered = text.replace("{{ip}}", ip_text)
        rendered = rendered.replace("{{timestamp}}", datetime.now(timezone.utc).isoformat(timespec="seconds"))
        return rendered

    def _render_template_data(self, value: Any, ip_text: str) -> Any:
        if isinstance(value, str):
            return self._render_template_text(value, ip_text)
        if isinstance(value, list):
            return [self._render_template_data(item, ip_text) for item in value]
        if isinstance(value, dict):
            return {
                str(key): self._render_template_data(item, ip_text)
                for key, item in value.items()
            }
        return value

    def _parse_ip(self, value: str) -> Optional[ipaddress._BaseAddress]:
        try:
            return ipaddress.ip_address(value.strip())
        except ValueError:
            return None

    def _serialize_location(self, location: GeoLocation) -> Dict[str, Any]:
        return {
            "ip": location.ip,
            "country": location.country,
            "region": location.region,
            "city": location.city,
            "full_text": location.full_text,
            "summary": location.summary,
            "source": location.source,
            "online_cache_hit": bool(location.online_cache_hit),
            "raw": location.raw,
        }

    def _stringify_query_value(self, value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return self._stringify(value)

    def _stringify(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()
