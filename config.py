from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml


DEFAULT_DB_PATH = "data/proxy_config.db"
SIZE_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)?$", re.IGNORECASE)
REGION_SPLIT_PATTERN = re.compile(r"[,，]")
IP_SPLIT_PATTERN = re.compile(r"[,，]")


def parse_size(value: Union[str, int, float, None]) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)

    raw_value = str(value).strip().upper()
    if not raw_value:
        return 0

    match = SIZE_PATTERN.match(raw_value)
    if not match:
        raise ValueError(f"Unable to parse size value: {value}")

    number = float(match.group(1))
    unit = match.group(2) or "B"
    units = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }
    return int(number * units[unit])


def coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def normalize_region_filter_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return ",".join(parts)
    return str(value).strip()


@dataclass
class ProxyRule:
    path_prefix: str
    target_url: str
    request_host: str = ""
    strip_prefix: bool = False
    timeout: int = 30
    max_redirects: int = 10
    follow_redirects: bool = True
    retry_times: int = 3
    enable_streaming: bool = True
    ip_whitelist: str = ""
    region_filters: str = ""
    is_default: bool = False
    enabled: bool = True
    priority: int = 0
    name: str = ""
    notes: str = ""
    source: str = "manual"
    rule_id: Optional[int] = None
    external_id: Optional[str] = None

    def normalized_regions(self) -> List[str]:
        raw_regions = normalize_region_filter_value(self.region_filters)
        regions: List[str] = []
        for part in REGION_SPLIT_PATTERN.split(raw_regions):
            normalized = normalize_location_text(part)
            if normalized:
                regions.append(normalized)
        return regions

    def normalized_ip_whitelist(self) -> List[str]:
        raw_ips = normalize_region_filter_value(self.ip_whitelist)
        return [part.strip() for part in IP_SPLIT_PATTERN.split(raw_ips) if part.strip()]


@dataclass
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    workers: int = 1
    keepalive_timeout: int = 75
    max_connections: int = 1000
    max_connections_per_host: int = 100


@dataclass
class SSLConfig:
    enabled: bool = False
    cert_file: Optional[str] = None
    key_file: Optional[str] = None


@dataclass
class LoggingConfig:
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None
    max_size: int = 10 * 1024 * 1024
    backup_count: int = 5


@dataclass
class AdminAuthConfig:
    enabled: bool = False
    username: str = "admin"
    password: str = ""
    session_ttl_hours: int = 12
    cookie_name: str = "proxy_admin_session"


@dataclass
class StreamingConfig:
    enabled: bool = True
    chunk_size: int = 64 * 1024
    large_file_threshold: int = 10 * 1024 * 1024
    stream_timeout: int = 3600
    read_timeout: int = 300
    write_timeout: int = 300
    buffer_size: int = 64 * 1024
    enable_range_support: bool = True
    max_request_body_size: int = 0


@dataclass
class CacheConfig:
    enabled: bool = False
    cache_dir: str = "./cache"
    max_size: int = 10 * 1024 * 1024 * 1024
    max_entries: int = 1000
    default_ttl: int = 86400
    chunk_size: int = 64 * 1024
    max_entry_size: int = 0
    max_request_cache_size: int = 0
    enable_preload: bool = True
    preload_concurrency: int = 3
    enable_validation: bool = True
    validation_interval: int = 3600


@dataclass
class RemoteConfigSettings:
    enabled: bool = False
    url: str = ""
    method: str = "GET"
    headers_json: str = "{}"
    body_template: str = ""
    timeout: int = 5
    data_path: str = "data"
    external_id_field: str = "id"
    name_field: str = "name"
    path_prefix_field: str = "path_prefix"
    target_url_field: str = "target_url"
    strip_prefix_field: str = "strip_prefix"
    timeout_field: str = "timeout"
    max_redirects_field: str = "max_redirects"
    retry_times_field: str = "retry_times"
    enable_streaming_field: str = "enable_streaming"
    region_filters_field: str = "region_filters"
    is_default_field: str = "is_default"
    enabled_field: str = "enabled"
    priority_field: str = "priority"


@dataclass
class PrimaryGeoIPSettings:
    enabled: bool = False
    url: str = ""
    method: str = "GET"
    headers_json: str = "{}"
    body_template: str = ""
    ip_param_name: str = "ip"
    timeout: int = 3
    country_path: str = "country"
    region_path: str = "region"
    city_path: str = "city"
    full_path: str = ""


@dataclass
class OnlineGeoIPSource:
    name: str = ""
    enabled: bool = True
    weight: int = 1
    url: str = ""
    method: str = "GET"
    request_location: str = "query"
    body_format: str = "json"
    query_params_json: str = "{}"
    headers_json: str = "{}"
    body_template: str = ""
    ip_param_name: str = "ip"
    timeout: int = 3
    country_path: str = "country"
    region_path: str = "region"
    city_path: str = "city"
    full_path: str = ""
    priority: int = 0
    source_id: Optional[int] = None
    notes: str = ""


@dataclass
class OfflineGeoIPSettings:
    enabled: bool = False
    db_path: str = ""
    locale: str = "zh-CN"
    download_url: str = ""
    download_headers_json: str = "{}"
    refresh_interval_hours: int = 24
    last_sync_at: str = ""
    last_sync_status: str = ""
    last_sync_message: str = ""
    last_success_at: str = ""


@dataclass
class GeoIPSettings:
    enabled: bool = True
    sources: List[OnlineGeoIPSource] = field(default_factory=list)
    online_cache_ttl_seconds: int = 120
    primary: PrimaryGeoIPSettings = field(default_factory=PrimaryGeoIPSettings)
    offline: OfflineGeoIPSettings = field(default_factory=OfflineGeoIPSettings)


@dataclass
class RouteGroupConfig:
    path_prefix: str
    request_host: str = ""
    region_matching_enabled: bool = False
    notes: str = ""


@dataclass
class Config:
    server: ServerConfig = field(default_factory=ServerConfig)
    ssl: SSLConfig = field(default_factory=SSLConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    admin_auth: AdminAuthConfig = field(default_factory=AdminAuthConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    proxy_rules: List[ProxyRule] = field(default_factory=list)
    default_timeout: int = 30
    max_redirects: int = 10
    follow_redirects: bool = True
    trust_forward_headers: bool = True
    database_path: str = DEFAULT_DB_PATH
    region_matching_enabled: bool = False
    route_groups: List[RouteGroupConfig] = field(default_factory=list)
    remote_config: RemoteConfigSettings = field(default_factory=RemoteConfigSettings)
    geoip: GeoIPSettings = field(default_factory=GeoIPSettings)
    hop_by_hop_headers: List[str] = field(
        default_factory=lambda: [
            "Connection",
            "Keep-Alive",
            "Proxy-Authenticate",
            "Proxy-Authorization",
            "TE",
            "Trailers",
            "Transfer-Encoding",
            "Upgrade",
        ]
    )

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "Config":
        with open(yaml_path, "r", encoding="utf-8") as file_obj:
            data = yaml.safe_load(file_obj) or {}
        return cls._parse_config(data)

    @classmethod
    def _parse_config(cls, data: Dict[str, Any]) -> "Config":
        config = cls()

        server_data = data.get("server", {})
        if server_data:
            config.server = ServerConfig(
                host=server_data.get("host", config.server.host),
                port=int(server_data.get("port", config.server.port)),
                workers=int(server_data.get("workers", config.server.workers)),
                keepalive_timeout=int(server_data.get("keepalive_timeout", config.server.keepalive_timeout)),
                max_connections=int(server_data.get("max_connections", config.server.max_connections)),
                max_connections_per_host=int(
                    server_data.get("max_connections_per_host", config.server.max_connections_per_host)
                ),
            )

        ssl_data = data.get("ssl", {})
        if ssl_data:
            config.ssl = SSLConfig(
                enabled=coerce_bool(ssl_data.get("enabled"), config.ssl.enabled),
                cert_file=ssl_data.get("cert_file", config.ssl.cert_file),
                key_file=ssl_data.get("key_file", config.ssl.key_file),
            )

        logging_data = data.get("logging", {})
        if logging_data:
            config.logging = LoggingConfig(
                level=str(logging_data.get("level", config.logging.level)),
                format=str(logging_data.get("format", config.logging.format)),
                file_path=logging_data.get("file_path", config.logging.file_path),
                max_size=parse_size(logging_data.get("max_size", config.logging.max_size)),
                backup_count=int(logging_data.get("backup_count", config.logging.backup_count)),
            )

        admin_auth_data = data.get("admin_auth", {})
        if isinstance(admin_auth_data, dict):
            config.admin_auth = AdminAuthConfig(
                enabled=coerce_bool(admin_auth_data.get("enabled"), config.admin_auth.enabled),
                username=str(admin_auth_data.get("username", config.admin_auth.username)).strip()
                or config.admin_auth.username,
                password=str(admin_auth_data.get("password", config.admin_auth.password)),
                session_ttl_hours=max(
                    1,
                    int(admin_auth_data.get("session_ttl_hours", config.admin_auth.session_ttl_hours) or 1),
                ),
                cookie_name=str(admin_auth_data.get("cookie_name", config.admin_auth.cookie_name)).strip()
                or config.admin_auth.cookie_name,
            )

        streaming_data = data.get("streaming", {})
        if streaming_data:
            config.streaming = StreamingConfig(
                enabled=coerce_bool(streaming_data.get("enabled"), config.streaming.enabled),
                chunk_size=parse_size(streaming_data.get("chunk_size", config.streaming.chunk_size)),
                large_file_threshold=parse_size(
                    streaming_data.get("large_file_threshold", config.streaming.large_file_threshold)
                ),
                stream_timeout=int(streaming_data.get("stream_timeout", config.streaming.stream_timeout)),
                read_timeout=int(streaming_data.get("read_timeout", config.streaming.read_timeout)),
                write_timeout=int(streaming_data.get("write_timeout", config.streaming.write_timeout)),
                buffer_size=parse_size(streaming_data.get("buffer_size", config.streaming.buffer_size)),
                enable_range_support=coerce_bool(
                    streaming_data.get("enable_range_support"),
                    config.streaming.enable_range_support,
                ),
                max_request_body_size=parse_size(
                    streaming_data.get("max_request_body_size", config.streaming.max_request_body_size)
                ),
            )

        cache_data = data.get("cache", {})
        if cache_data:
            config.cache = CacheConfig(
                enabled=coerce_bool(cache_data.get("enabled"), config.cache.enabled),
                cache_dir=str(cache_data.get("cache_dir", config.cache.cache_dir)),
                max_size=parse_size(cache_data.get("max_size", config.cache.max_size)),
                max_entries=int(cache_data.get("max_entries", config.cache.max_entries)),
                default_ttl=int(cache_data.get("default_ttl", config.cache.default_ttl)),
                chunk_size=parse_size(cache_data.get("chunk_size", config.cache.chunk_size)),
                max_entry_size=parse_size(cache_data.get("max_entry_size", config.cache.max_entry_size)),
                max_request_cache_size=parse_size(
                    cache_data.get("max_request_cache_size", config.cache.max_request_cache_size)
                ),
                enable_preload=coerce_bool(cache_data.get("enable_preload"), config.cache.enable_preload),
                preload_concurrency=int(cache_data.get("preload_concurrency", config.cache.preload_concurrency)),
                enable_validation=coerce_bool(
                    cache_data.get("enable_validation"),
                    config.cache.enable_validation,
                ),
                validation_interval=int(cache_data.get("validation_interval", config.cache.validation_interval)),
            )

        config.proxy_rules = []
        for rule_data in data.get("proxy_rules", []):
            config.proxy_rules.append(
                ProxyRule(
                    path_prefix=str(rule_data.get("path_prefix", "")),
                    target_url=str(rule_data.get("target_url", "")),
                    request_host=normalize_request_host(rule_data.get("request_host", "")),
                    strip_prefix=coerce_bool(rule_data.get("strip_prefix"), False),
                    timeout=int(rule_data.get("timeout", config.default_timeout)),
                    max_redirects=int(rule_data.get("max_redirects", config.max_redirects)),
                    follow_redirects=coerce_bool(
                        rule_data.get("follow_redirects"),
                        config.follow_redirects,
                    ),
                    retry_times=int(rule_data.get("retry_times", 3)),
                    enable_streaming=coerce_bool(rule_data.get("enable_streaming"), True),
                    ip_whitelist=normalize_region_filter_value(rule_data.get("ip_whitelist", "")),
                    region_filters=normalize_region_filter_value(rule_data.get("region_filters", "")),
                    is_default=coerce_bool(rule_data.get("is_default"), False),
                    enabled=coerce_bool(rule_data.get("enabled"), True),
                    priority=int(rule_data.get("priority", 0)),
                    name=str(rule_data.get("name", "")),
                    notes=str(rule_data.get("notes", "")),
                    source=str(rule_data.get("source", "manual")),
                    external_id=_string_or_none(rule_data.get("external_id")),
                )
            )

        config.default_timeout = int(data.get("default_timeout", config.default_timeout))
        config.max_redirects = int(data.get("max_redirects", config.max_redirects))
        config.follow_redirects = coerce_bool(data.get("follow_redirects"), config.follow_redirects)
        config.trust_forward_headers = coerce_bool(
            data.get("trust_forward_headers"),
            config.trust_forward_headers,
        )

        database_data = data.get("database", {})
        if isinstance(database_data, dict):
            config.database_path = str(database_data.get("path", config.database_path))

        region_matching_data = data.get("region_matching", {})
        if isinstance(region_matching_data, dict):
            config.region_matching_enabled = coerce_bool(
                region_matching_data.get("enabled"),
                config.region_matching_enabled,
            )

        config.route_groups = []
        for group_data in data.get("route_groups", []):
            path_prefix = str(group_data.get("path_prefix", "")).strip()
            if not path_prefix:
                continue
            config.route_groups.append(
                RouteGroupConfig(
                    path_prefix=path_prefix,
                    request_host=normalize_request_host(group_data.get("request_host", "")),
                    region_matching_enabled=coerce_bool(group_data.get("region_matching_enabled"), False),
                    notes=str(group_data.get("notes", "")),
                )
            )

        if not config.route_groups and config.proxy_rules:
            group_keys = sorted(
                {(normalize_request_host(rule.request_host), rule.path_prefix) for rule in config.proxy_rules},
                key=lambda item: (item[0], item[1]),
            )
            for request_host, path_prefix in group_keys:
                config.route_groups.append(
                    RouteGroupConfig(
                        path_prefix=path_prefix,
                        request_host=request_host,
                        region_matching_enabled=config.region_matching_enabled,
                    )
                )

        remote_data = data.get("remote_config", {})
        if isinstance(remote_data, dict):
            config.remote_config = RemoteConfigSettings(
                enabled=coerce_bool(remote_data.get("enabled"), config.remote_config.enabled),
                url=str(remote_data.get("url", config.remote_config.url)),
                method=str(remote_data.get("method", config.remote_config.method)).upper(),
                headers_json=str(remote_data.get("headers_json", config.remote_config.headers_json)),
                body_template=str(remote_data.get("body_template", config.remote_config.body_template)),
                timeout=int(remote_data.get("timeout", config.remote_config.timeout)),
                data_path=str(remote_data.get("data_path", config.remote_config.data_path)),
                external_id_field=str(
                    remote_data.get("external_id_field", config.remote_config.external_id_field)
                ),
                name_field=str(remote_data.get("name_field", config.remote_config.name_field)),
                path_prefix_field=str(
                    remote_data.get("path_prefix_field", config.remote_config.path_prefix_field)
                ),
                target_url_field=str(
                    remote_data.get("target_url_field", config.remote_config.target_url_field)
                ),
                strip_prefix_field=str(
                    remote_data.get("strip_prefix_field", config.remote_config.strip_prefix_field)
                ),
                timeout_field=str(remote_data.get("timeout_field", config.remote_config.timeout_field)),
                max_redirects_field=str(
                    remote_data.get("max_redirects_field", config.remote_config.max_redirects_field)
                ),
                retry_times_field=str(
                    remote_data.get("retry_times_field", config.remote_config.retry_times_field)
                ),
                enable_streaming_field=str(
                    remote_data.get("enable_streaming_field", config.remote_config.enable_streaming_field)
                ),
                region_filters_field=str(
                    remote_data.get("region_filters_field", config.remote_config.region_filters_field)
                ),
                is_default_field=str(
                    remote_data.get("is_default_field", config.remote_config.is_default_field)
                ),
                enabled_field=str(remote_data.get("enabled_field", config.remote_config.enabled_field)),
                priority_field=str(remote_data.get("priority_field", config.remote_config.priority_field)),
            )

        geoip_data = data.get("geoip", {})
        if isinstance(geoip_data, dict):
            primary_data = geoip_data.get("primary", {})
            offline_data = geoip_data.get("offline", {})
            source_items = geoip_data.get("sources", [])
            parsed_sources: List[OnlineGeoIPSource] = []
            if isinstance(source_items, list):
                for source_data in source_items:
                    if not isinstance(source_data, dict):
                        continue
                    parsed_sources.append(
                        OnlineGeoIPSource(
                            source_id=source_data.get("id"),
                            name=str(source_data.get("name", "")),
                            enabled=coerce_bool(source_data.get("enabled"), True),
                            weight=max(1, int(source_data.get("weight", 1) or 1)),
                            url=str(source_data.get("url", "")).strip(),
                            method=str(source_data.get("method", "GET")).upper(),
                            request_location=str(source_data.get("request_location", "query")).lower(),
                            body_format=str(source_data.get("body_format", "json")).lower(),
                            query_params_json=str(source_data.get("query_params_json", "{}")),
                            headers_json=str(source_data.get("headers_json", "{}")),
                            body_template=str(source_data.get("body_template", "")),
                            ip_param_name=str(source_data.get("ip_param_name", "ip")),
                            timeout=int(source_data.get("timeout", 3) or 3),
                            country_path=str(source_data.get("country_path", "country")),
                            region_path=str(source_data.get("region_path", "region")),
                            city_path=str(source_data.get("city_path", "city")),
                            full_path=str(source_data.get("full_path", "")),
                            priority=int(source_data.get("priority", 0) or 0),
                            notes=str(source_data.get("notes", "")),
                        )
                    )
            elif isinstance(primary_data, dict) and primary_data.get("url"):
                parsed_sources.append(
                    OnlineGeoIPSource(
                        name="primary",
                        enabled=coerce_bool(primary_data.get("enabled"), False),
                        weight=1,
                        url=str(primary_data.get("url", "")),
                        method=str(primary_data.get("method", "GET")).upper(),
                        request_location="query" if str(primary_data.get("method", "GET")).upper() == "GET" else "body",
                        body_format="json",
                        query_params_json="{}",
                        headers_json=str(primary_data.get("headers_json", "{}")),
                        body_template=str(primary_data.get("body_template", "")),
                        ip_param_name=str(primary_data.get("ip_param_name", "ip")),
                        timeout=int(primary_data.get("timeout", 3) or 3),
                        country_path=str(primary_data.get("country_path", "country")),
                        region_path=str(primary_data.get("region_path", "region")),
                        city_path=str(primary_data.get("city_path", "city")),
                        full_path=str(primary_data.get("full_path", "")),
                    )
                )
            config.geoip = GeoIPSettings(
                enabled=coerce_bool(geoip_data.get("enabled"), config.geoip.enabled),
                sources=parsed_sources,
                online_cache_ttl_seconds=max(
                    0,
                    int(geoip_data.get("online_cache_ttl_seconds", config.geoip.online_cache_ttl_seconds) or 0),
                ),
                primary=PrimaryGeoIPSettings(
                    enabled=coerce_bool(primary_data.get("enabled"), config.geoip.primary.enabled),
                    url=str(primary_data.get("url", config.geoip.primary.url)),
                    method=str(primary_data.get("method", config.geoip.primary.method)).upper(),
                    headers_json=str(
                        primary_data.get("headers_json", config.geoip.primary.headers_json)
                    ),
                    body_template=str(
                        primary_data.get("body_template", config.geoip.primary.body_template)
                    ),
                    ip_param_name=str(
                        primary_data.get("ip_param_name", config.geoip.primary.ip_param_name)
                    ),
                    timeout=int(primary_data.get("timeout", config.geoip.primary.timeout)),
                    country_path=str(
                        primary_data.get("country_path", config.geoip.primary.country_path)
                    ),
                    region_path=str(primary_data.get("region_path", config.geoip.primary.region_path)),
                    city_path=str(primary_data.get("city_path", config.geoip.primary.city_path)),
                    full_path=str(primary_data.get("full_path", config.geoip.primary.full_path)),
                ),
                offline=OfflineGeoIPSettings(
                    enabled=coerce_bool(offline_data.get("enabled"), config.geoip.offline.enabled),
                    db_path=str(offline_data.get("db_path", config.geoip.offline.db_path)),
                    locale=str(offline_data.get("locale", config.geoip.offline.locale)),
                    download_url=str(offline_data.get("download_url", config.geoip.offline.download_url)),
                    download_headers_json=str(
                        offline_data.get("download_headers_json", config.geoip.offline.download_headers_json)
                    ),
                    refresh_interval_hours=int(
                        offline_data.get("refresh_interval_hours", config.geoip.offline.refresh_interval_hours) or 24
                    ),
                    last_sync_at=str(offline_data.get("last_sync_at", config.geoip.offline.last_sync_at)),
                    last_sync_status=str(
                        offline_data.get("last_sync_status", config.geoip.offline.last_sync_status)
                    ),
                    last_sync_message=str(
                        offline_data.get("last_sync_message", config.geoip.offline.last_sync_message)
                    ),
                    last_success_at=str(offline_data.get("last_success_at", config.geoip.offline.last_success_at)),
                ),
            )

        return config

    def to_dict(self) -> Dict[str, Any]:
        return {
            "server": {
                "host": self.server.host,
                "port": self.server.port,
                "workers": self.server.workers,
                "keepalive_timeout": self.server.keepalive_timeout,
                "max_connections": self.server.max_connections,
                "max_connections_per_host": self.server.max_connections_per_host,
            },
            "ssl": {
                "enabled": self.ssl.enabled,
                "cert_file": self.ssl.cert_file,
                "key_file": self.ssl.key_file,
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
                "file_path": self.logging.file_path,
                "max_size": self.logging.max_size,
                "backup_count": self.logging.backup_count,
            },
            "admin_auth": {
                "enabled": self.admin_auth.enabled,
                "username": self.admin_auth.username,
                "password": self.admin_auth.password,
                "session_ttl_hours": self.admin_auth.session_ttl_hours,
                "cookie_name": self.admin_auth.cookie_name,
            },
            "streaming": {
                "enabled": self.streaming.enabled,
                "chunk_size": self.streaming.chunk_size,
                "large_file_threshold": self.streaming.large_file_threshold,
                "stream_timeout": self.streaming.stream_timeout,
                "read_timeout": self.streaming.read_timeout,
                "write_timeout": self.streaming.write_timeout,
                "buffer_size": self.streaming.buffer_size,
                "enable_range_support": self.streaming.enable_range_support,
                "max_request_body_size": self.streaming.max_request_body_size,
            },
            "cache": {
                "enabled": self.cache.enabled,
                "cache_dir": self.cache.cache_dir,
                "max_size": self.cache.max_size,
                "max_entries": self.cache.max_entries,
                "default_ttl": self.cache.default_ttl,
                "chunk_size": self.cache.chunk_size,
                "max_entry_size": self.cache.max_entry_size,
                "max_request_cache_size": self.cache.max_request_cache_size,
                "enable_preload": self.cache.enable_preload,
                "preload_concurrency": self.cache.preload_concurrency,
                "enable_validation": self.cache.enable_validation,
                "validation_interval": self.cache.validation_interval,
            },
            "proxy_rules": [
                {
                    "id": rule.rule_id,
                    "name": rule.name,
                    "path_prefix": rule.path_prefix,
                    "target_url": rule.target_url,
                    "request_host": rule.request_host,
                    "strip_prefix": rule.strip_prefix,
                    "timeout": rule.timeout,
                    "max_redirects": rule.max_redirects,
                    "follow_redirects": rule.follow_redirects,
                    "retry_times": rule.retry_times,
                    "enable_streaming": rule.enable_streaming,
                    "ip_whitelist": rule.ip_whitelist,
                    "region_filters": rule.region_filters,
                    "is_default": rule.is_default,
                    "enabled": rule.enabled,
                    "priority": rule.priority,
                    "source": rule.source,
                    "external_id": rule.external_id,
                    "notes": rule.notes,
                }
                for rule in self.proxy_rules
            ],
            "default_timeout": self.default_timeout,
            "max_redirects": self.max_redirects,
            "follow_redirects": self.follow_redirects,
            "trust_forward_headers": self.trust_forward_headers,
            "database": {"path": self.database_path},
            "route_groups": [
                {
                    "path_prefix": group.path_prefix,
                    "request_host": group.request_host,
                    "region_matching_enabled": group.region_matching_enabled,
                    "notes": group.notes,
                }
                for group in self.route_groups
            ],
            "geoip": {
                "enabled": self.geoip.enabled,
                "online_cache_ttl_seconds": self.geoip.online_cache_ttl_seconds,
                "sources": [
                    {
                        "id": source.source_id,
                        "name": source.name,
                        "enabled": source.enabled,
                        "weight": source.weight,
                        "url": source.url,
                        "method": source.method,
                        "request_location": source.request_location,
                        "body_format": source.body_format,
                        "query_params_json": source.query_params_json,
                        "headers_json": source.headers_json,
                        "body_template": source.body_template,
                        "ip_param_name": source.ip_param_name,
                        "timeout": source.timeout,
                        "country_path": source.country_path,
                        "region_path": source.region_path,
                        "city_path": source.city_path,
                        "full_path": source.full_path,
                        "priority": source.priority,
                        "notes": source.notes,
                    }
                    for source in self.geoip.sources
                ],
                "offline": {
                    "enabled": self.geoip.offline.enabled,
                    "db_path": self.geoip.offline.db_path,
                    "locale": self.geoip.offline.locale,
                    "download_url": self.geoip.offline.download_url,
                    "download_headers_json": self.geoip.offline.download_headers_json,
                    "refresh_interval_hours": self.geoip.offline.refresh_interval_hours,
                    "last_sync_at": self.geoip.offline.last_sync_at,
                    "last_sync_status": self.geoip.offline.last_sync_status,
                    "last_sync_message": self.geoip.offline.last_sync_message,
                    "last_success_at": self.geoip.offline.last_success_at,
                },
            },
        }

    def ensure_route_group(
        self,
        path_prefix: str,
        request_host: str = "",
        region_matching_enabled: bool = False,
    ) -> RouteGroupConfig:
        normalized_host = normalize_request_host(request_host)
        existing = self.get_route_group(path_prefix, normalized_host)
        if existing:
            return existing
        group = RouteGroupConfig(
            path_prefix=path_prefix,
            request_host=normalized_host,
            region_matching_enabled=region_matching_enabled,
        )
        self.route_groups.append(group)
        self.route_groups.sort(key=lambda item: (item.request_host, item.path_prefix))
        return group

    def get_route_group(self, path_prefix: str, request_host: str = "") -> Optional[RouteGroupConfig]:
        normalized_host = normalize_request_host(request_host)
        for group in self.route_groups:
            if group.path_prefix == path_prefix and normalize_request_host(group.request_host) == normalized_host:
                return group
        return None

    def save_yaml(self, yaml_path: str) -> None:
        with open(yaml_path, "w", encoding="utf-8") as file_obj:
            yaml.safe_dump(self.to_dict(), file_obj, allow_unicode=True, sort_keys=False)


def _string_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_location_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    if not text:
        return ""
    text = re.sub(r"\s+", "", text)
    return text


def normalize_request_host(value: Any) -> str:
    return ",".join(split_request_hosts(value))


def split_request_hosts(value: Any) -> Tuple[str, ...]:
    if value is None:
        return tuple()

    raw_parts: List[str] = []
    if isinstance(value, (list, tuple, set)):
        for item in value:
            raw_parts.extend(re.split(r"[,，]", str(item)))
    else:
        raw_parts = re.split(r"[,，]", str(value))

    normalized_hosts: List[str] = []
    seen_hosts = set()
    for raw in raw_parts:
        normalized = _normalize_single_request_host(raw)
        if not normalized or normalized in seen_hosts:
            continue
        seen_hosts.add(normalized)
        normalized_hosts.append(normalized)

    return tuple(normalized_hosts)


def _normalize_single_request_host(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if text.startswith("[") and "]" in text:
        return text[1:text.index("]")].strip()
    if ":" in text and text.count(":") == 1:
        return text.split(":", 1)[0].strip()
    return text


def setup_logging(config: LoggingConfig) -> logging.Logger:
    logger = logging.getLogger("proxy")
    logger.setLevel(getattr(logging, config.level.upper(), logging.INFO))
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(config.format)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if config.file_path:
        log_path = Path(config.file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            str(log_path),
            maxBytes=config.max_size,
            backupCount=config.backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def load_config(config_path: Optional[str] = None) -> Config:
    if config_path and os.path.exists(config_path):
        return Config.from_yaml(config_path)

    for default_path in ("config.yaml", "config.yml", "etc/config.yaml"):
        if os.path.exists(default_path):
            return Config.from_yaml(default_path)

    return Config()
