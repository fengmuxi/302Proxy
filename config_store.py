from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

from config import (
    DEFAULT_DB_PATH,
    CacheConfig,
    Config,
    GeoIPSettings,
    LoggingConfig,
    OnlineGeoIPSource,
    OfflineGeoIPSettings,
    PrimaryGeoIPSettings,
    ProxyRule,
    RouteGroupConfig,
    RemoteConfigSettings,
    SSLConfig,
    ServerConfig,
    StreamingConfig,
    coerce_bool,
    normalize_request_host,
    normalize_region_filter_value,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def deep_get(data: Any, path: str, default: Any = None) -> Any:
    if not path:
        return data

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


class ConfigStore:
    def __init__(self, db_path: Optional[str] = None, bootstrap_config: Optional[Config] = None):
        if bootstrap_config and db_path is None:
            db_path = bootstrap_config.database_path

        self.db_path = Path(db_path or DEFAULT_DB_PATH)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.bootstrap_config = bootstrap_config or Config()

        self._initialize_schema()
        self._bootstrap_if_needed(self.bootstrap_config)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS system_settings (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    workers INTEGER NOT NULL,
                    keepalive_timeout INTEGER NOT NULL,
                    max_connections INTEGER NOT NULL,
                    max_connections_per_host INTEGER NOT NULL,
                    ssl_enabled INTEGER NOT NULL,
                    cert_file TEXT,
                    key_file TEXT,
                    logging_level TEXT NOT NULL,
                    logging_format TEXT NOT NULL,
                    logging_file_path TEXT,
                    logging_max_size INTEGER NOT NULL,
                    logging_backup_count INTEGER NOT NULL,
                    streaming_enabled INTEGER NOT NULL,
                    streaming_chunk_size INTEGER NOT NULL,
                    streaming_large_file_threshold INTEGER NOT NULL,
                    streaming_stream_timeout INTEGER NOT NULL,
                    streaming_read_timeout INTEGER NOT NULL,
                    streaming_write_timeout INTEGER NOT NULL,
                    streaming_buffer_size INTEGER NOT NULL,
                    streaming_enable_range_support INTEGER NOT NULL,
                    streaming_max_request_body_size INTEGER NOT NULL,
                    cache_enabled INTEGER NOT NULL,
                    cache_dir TEXT NOT NULL,
                    cache_max_size INTEGER NOT NULL,
                    cache_max_entries INTEGER NOT NULL,
                    cache_default_ttl INTEGER NOT NULL,
                    cache_chunk_size INTEGER NOT NULL,
                    cache_max_entry_size INTEGER NOT NULL,
                    cache_max_request_cache_size INTEGER NOT NULL,
                    cache_enable_preload INTEGER NOT NULL,
                    cache_preload_concurrency INTEGER NOT NULL,
                    cache_enable_validation INTEGER NOT NULL,
                    cache_validation_interval INTEGER NOT NULL,
                    default_timeout INTEGER NOT NULL,
                    max_redirects INTEGER NOT NULL,
                    follow_redirects INTEGER NOT NULL,
                    trust_forward_headers INTEGER NOT NULL,
                    database_path TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS feature_flags (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    region_matching_enabled INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS route_groups (
                    request_host TEXT NOT NULL DEFAULT '',
                    path_prefix TEXT NOT NULL,
                    region_matching_enabled INTEGER NOT NULL DEFAULT 0,
                    notes TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (request_host, path_prefix)
                );

                CREATE TABLE IF NOT EXISTS remote_config_sources (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    enabled INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    method TEXT NOT NULL,
                    headers_json TEXT NOT NULL,
                    body_template TEXT NOT NULL,
                    timeout INTEGER NOT NULL,
                    data_path TEXT NOT NULL,
                    external_id_field TEXT NOT NULL,
                    name_field TEXT NOT NULL,
                    path_prefix_field TEXT NOT NULL,
                    target_url_field TEXT NOT NULL,
                    strip_prefix_field TEXT NOT NULL,
                    timeout_field TEXT NOT NULL,
                    max_redirects_field TEXT NOT NULL,
                    retry_times_field TEXT NOT NULL,
                    enable_streaming_field TEXT NOT NULL,
                    region_filters_field TEXT NOT NULL,
                    is_default_field TEXT NOT NULL,
                    enabled_field TEXT NOT NULL,
                    priority_field TEXT NOT NULL,
                    last_sync_at TEXT,
                    last_sync_status TEXT,
                    last_sync_message TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS geoip_settings (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    enabled INTEGER NOT NULL,
                    online_cache_ttl_seconds INTEGER NOT NULL DEFAULT 120,
                    primary_enabled INTEGER NOT NULL,
                    primary_url TEXT NOT NULL,
                    primary_method TEXT NOT NULL,
                    primary_headers_json TEXT NOT NULL,
                    primary_body_template TEXT NOT NULL,
                    primary_ip_param_name TEXT NOT NULL,
                    primary_timeout INTEGER NOT NULL,
                    primary_country_path TEXT NOT NULL,
                    primary_region_path TEXT NOT NULL,
                    primary_city_path TEXT NOT NULL,
                    primary_full_path TEXT NOT NULL,
                    offline_enabled INTEGER NOT NULL,
                    offline_db_path TEXT NOT NULL,
                    offline_locale TEXT NOT NULL,
                    offline_download_url TEXT NOT NULL DEFAULT '',
                    offline_download_headers_json TEXT NOT NULL DEFAULT '{}',
                    offline_refresh_interval_hours INTEGER NOT NULL DEFAULT 24,
                    offline_last_sync_at TEXT,
                    offline_last_sync_status TEXT NOT NULL DEFAULT '',
                    offline_last_sync_message TEXT NOT NULL DEFAULT '',
                    offline_last_success_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS geoip_online_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    weight INTEGER NOT NULL DEFAULT 1,
                    url TEXT NOT NULL DEFAULT '',
                    method TEXT NOT NULL DEFAULT 'GET',
                    request_location TEXT NOT NULL DEFAULT 'query',
                    body_format TEXT NOT NULL DEFAULT 'json',
                    query_params_json TEXT NOT NULL DEFAULT '{}',
                    headers_json TEXT NOT NULL DEFAULT '{}',
                    body_template TEXT NOT NULL DEFAULT '',
                    ip_param_name TEXT NOT NULL DEFAULT 'ip',
                    timeout INTEGER NOT NULL DEFAULT 3,
                    country_path TEXT NOT NULL DEFAULT 'country',
                    region_path TEXT NOT NULL DEFAULT 'region',
                    city_path TEXT NOT NULL DEFAULT 'city',
                    full_path TEXT NOT NULL DEFAULT '',
                    priority INTEGER NOT NULL DEFAULT 0,
                    notes TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS route_log_settings (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    retention_days INTEGER NOT NULL DEFAULT 30,
                    last_pruned_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS route_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_method TEXT NOT NULL DEFAULT '',
                    request_path TEXT NOT NULL DEFAULT '',
                    request_query_string TEXT NOT NULL DEFAULT '',
                    path_prefix TEXT NOT NULL DEFAULT '',
                    rule_id INTEGER,
                    rule_name TEXT NOT NULL DEFAULT '',
                    rule_source TEXT NOT NULL DEFAULT '',
                    target_url TEXT NOT NULL DEFAULT '',
                    original_client_ip TEXT NOT NULL DEFAULT '',
                    client_ip TEXT NOT NULL DEFAULT '',
                    region_matching_enabled INTEGER NOT NULL DEFAULT 0,
                    geo_source TEXT NOT NULL DEFAULT '',
                    geo_summary TEXT NOT NULL DEFAULT '',
                    geo_country TEXT NOT NULL DEFAULT '',
                    geo_region TEXT NOT NULL DEFAULT '',
                    geo_city TEXT NOT NULL DEFAULT '',
                    configured_ip_whitelist TEXT NOT NULL DEFAULT '',
                    matched_ip_whitelist TEXT NOT NULL DEFAULT '',
                    configured_regions TEXT NOT NULL DEFAULT '',
                    matched_region TEXT NOT NULL DEFAULT '',
                    match_strategy TEXT NOT NULL DEFAULT '',
                    match_detail TEXT NOT NULL DEFAULT '',
                    upstream_status INTEGER NOT NULL DEFAULT 0,
                    cache_status TEXT NOT NULL DEFAULT '',
                    redirect_count INTEGER NOT NULL DEFAULT 0,
                    transport_mode TEXT NOT NULL DEFAULT '',
                    operation_duration_ms INTEGER NOT NULL DEFAULT 0,
                    result_status TEXT NOT NULL DEFAULT '',
                    error_message TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS forward_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL DEFAULT 'manual',
                    external_id TEXT,
                    name TEXT NOT NULL DEFAULT '',
                    request_host TEXT NOT NULL DEFAULT '',
                    path_prefix TEXT NOT NULL,
                    target_url TEXT NOT NULL,
                    strip_prefix INTEGER NOT NULL DEFAULT 0,
                    timeout INTEGER NOT NULL DEFAULT 30,
                    max_redirects INTEGER NOT NULL DEFAULT 10,
                    follow_redirects INTEGER NOT NULL DEFAULT 1,
                    retry_times INTEGER NOT NULL DEFAULT 3,
                    enable_streaming INTEGER NOT NULL DEFAULT 1,
                    ip_whitelist TEXT NOT NULL DEFAULT '',
                    region_filters TEXT NOT NULL DEFAULT '',
                    is_default INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    priority INTEGER NOT NULL DEFAULT 0,
                    notes TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_forward_rules_path_prefix
                    ON forward_rules(path_prefix);
                CREATE INDEX IF NOT EXISTS idx_forward_rules_enabled
                    ON forward_rules(enabled);
                CREATE INDEX IF NOT EXISTS idx_forward_rules_source
                    ON forward_rules(source);
                CREATE INDEX IF NOT EXISTS idx_route_logs_created_at
                    ON route_logs(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_route_logs_path_prefix
                    ON route_logs(path_prefix);
                CREATE INDEX IF NOT EXISTS idx_route_logs_match_strategy
                    ON route_logs(match_strategy);
                CREATE INDEX IF NOT EXISTS idx_route_logs_result_status
                    ON route_logs(result_status);
                """
            )
            self._migrate_route_groups_table(connection)
            self._ensure_column(
                connection,
                "geoip_online_sources",
                "query_params_json",
                "TEXT NOT NULL DEFAULT '{}'",
            )
            self._ensure_column(
                connection,
                "geoip_settings",
                "online_cache_ttl_seconds",
                "INTEGER NOT NULL DEFAULT 120",
            )
            self._ensure_column(
                connection,
                "geoip_settings",
                "offline_download_url",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                connection,
                "geoip_settings",
                "offline_download_headers_json",
                "TEXT NOT NULL DEFAULT '{}'",
            )
            self._ensure_column(
                connection,
                "geoip_settings",
                "offline_refresh_interval_hours",
                "INTEGER NOT NULL DEFAULT 24",
            )
            self._ensure_column(
                connection,
                "geoip_settings",
                "offline_last_sync_at",
                "TEXT",
            )
            self._ensure_column(
                connection,
                "geoip_settings",
                "offline_last_sync_status",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                connection,
                "geoip_settings",
                "offline_last_sync_message",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                connection,
                "geoip_settings",
                "offline_last_success_at",
                "TEXT",
            )
            self._ensure_column(
                connection,
                "forward_rules",
                "request_host",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                connection,
                "forward_rules",
                "follow_redirects",
                "INTEGER NOT NULL DEFAULT 1",
            )
            self._ensure_column(
                connection,
                "forward_rules",
                "ip_whitelist",
                "TEXT NOT NULL DEFAULT ''",
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_forward_rules_request_host
                ON forward_rules(request_host)
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO route_log_settings (id, retention_days, last_pruned_at, updated_at)
                VALUES (1, 30, NULL, ?)
                """,
                (utc_now(),),
            )
            self._ensure_column(
                connection,
                "route_logs",
                "original_client_ip",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                connection,
                "route_logs",
                "configured_ip_whitelist",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                connection,
                "route_logs",
                "matched_ip_whitelist",
                "TEXT NOT NULL DEFAULT ''",
            )

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
        definition_sql: str,
    ) -> None:
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing_columns = {row["name"] for row in rows}
        if column_name in existing_columns:
            return
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition_sql}")

    def _migrate_route_groups_table(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute("PRAGMA table_info(route_groups)").fetchall()
        existing_columns = {row["name"] for row in rows}
        if not existing_columns or "request_host" in existing_columns:
            return

        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS route_groups_v2 (
                request_host TEXT NOT NULL DEFAULT '',
                path_prefix TEXT NOT NULL,
                region_matching_enabled INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (request_host, path_prefix)
            );

            INSERT INTO route_groups_v2 (request_host, path_prefix, region_matching_enabled, notes, updated_at)
            SELECT '', path_prefix, region_matching_enabled, notes, updated_at
            FROM route_groups;

            DROP TABLE route_groups;
            ALTER TABLE route_groups_v2 RENAME TO route_groups;
            """
        )

    def _bootstrap_if_needed(self, config: Config) -> None:
        with self._connect() as connection:
            existing = connection.execute("SELECT COUNT(*) FROM system_settings").fetchone()[0]
            if existing:
                return

            now = utc_now()
            connection.execute(
                """
                INSERT INTO system_settings (
                    id, host, port, workers, keepalive_timeout, max_connections,
                    max_connections_per_host, ssl_enabled, cert_file, key_file,
                    logging_level, logging_format, logging_file_path, logging_max_size,
                    logging_backup_count, streaming_enabled, streaming_chunk_size,
                    streaming_large_file_threshold, streaming_stream_timeout,
                    streaming_read_timeout, streaming_write_timeout, streaming_buffer_size,
                    streaming_enable_range_support, streaming_max_request_body_size,
                    cache_enabled, cache_dir, cache_max_size, cache_max_entries,
                    cache_default_ttl, cache_chunk_size, cache_max_entry_size,
                    cache_max_request_cache_size, cache_enable_preload,
                    cache_preload_concurrency, cache_enable_validation,
                    cache_validation_interval, default_timeout, max_redirects,
                    follow_redirects, trust_forward_headers, database_path, updated_at
                ) VALUES (
                    1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    config.server.host,
                    config.server.port,
                    config.server.workers,
                    config.server.keepalive_timeout,
                    config.server.max_connections,
                    config.server.max_connections_per_host,
                    int(config.ssl.enabled),
                    config.ssl.cert_file,
                    config.ssl.key_file,
                    config.logging.level,
                    config.logging.format,
                    config.logging.file_path,
                    config.logging.max_size,
                    config.logging.backup_count,
                    int(config.streaming.enabled),
                    config.streaming.chunk_size,
                    config.streaming.large_file_threshold,
                    config.streaming.stream_timeout,
                    config.streaming.read_timeout,
                    config.streaming.write_timeout,
                    config.streaming.buffer_size,
                    int(config.streaming.enable_range_support),
                    config.streaming.max_request_body_size,
                    int(config.cache.enabled),
                    config.cache.cache_dir,
                    config.cache.max_size,
                    config.cache.max_entries,
                    config.cache.default_ttl,
                    config.cache.chunk_size,
                    config.cache.max_entry_size,
                    config.cache.max_request_cache_size,
                    int(config.cache.enable_preload),
                    config.cache.preload_concurrency,
                    int(config.cache.enable_validation),
                    config.cache.validation_interval,
                    config.default_timeout,
                    config.max_redirects,
                    int(config.follow_redirects),
                    int(config.trust_forward_headers),
                    str(self.db_path),
                    now,
                ),
            )

            connection.execute(
                """
                INSERT INTO feature_flags (id, region_matching_enabled, updated_at)
                VALUES (1, ?, ?)
                """,
                (int(config.region_matching_enabled), now),
            )

            groups = config.route_groups or [
                RouteGroupConfig(
                    path_prefix=path_prefix,
                    request_host=request_host,
                    region_matching_enabled=config.region_matching_enabled,
                )
                for request_host, path_prefix in sorted(
                    {(normalize_request_host(rule.request_host), rule.path_prefix) for rule in config.proxy_rules}
                )
            ]
            for group in groups:
                connection.execute(
                    """
                    INSERT INTO route_groups (request_host, path_prefix, region_matching_enabled, notes, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        normalize_request_host(group.request_host),
                        group.path_prefix,
                        int(group.region_matching_enabled),
                        group.notes,
                        now,
                    ),
                )

            connection.execute(
                """
                INSERT INTO remote_config_sources (
                    id, enabled, url, method, headers_json, body_template, timeout, data_path,
                    external_id_field, name_field, path_prefix_field, target_url_field,
                    strip_prefix_field, timeout_field, max_redirects_field, retry_times_field,
                    enable_streaming_field, region_filters_field, is_default_field,
                    enabled_field, priority_field, last_sync_at, last_sync_status,
                    last_sync_message, updated_at
                ) VALUES (
                    1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL,
                    NULL, NULL, ?
                )
                """,
                (
                    int(config.remote_config.enabled),
                    config.remote_config.url,
                    config.remote_config.method,
                    config.remote_config.headers_json,
                    config.remote_config.body_template,
                    config.remote_config.timeout,
                    config.remote_config.data_path,
                    config.remote_config.external_id_field,
                    config.remote_config.name_field,
                    config.remote_config.path_prefix_field,
                    config.remote_config.target_url_field,
                    config.remote_config.strip_prefix_field,
                    config.remote_config.timeout_field,
                    config.remote_config.max_redirects_field,
                    config.remote_config.retry_times_field,
                    config.remote_config.enable_streaming_field,
                    config.remote_config.region_filters_field,
                    config.remote_config.is_default_field,
                    config.remote_config.enabled_field,
                    config.remote_config.priority_field,
                    now,
                ),
            )

            connection.execute(
                """
                INSERT INTO geoip_settings (
                    id, enabled, online_cache_ttl_seconds, primary_enabled, primary_url, primary_method,
                    primary_headers_json, primary_body_template, primary_ip_param_name,
                    primary_timeout, primary_country_path, primary_region_path,
                    primary_city_path, primary_full_path, offline_enabled,
                    offline_db_path, offline_locale, offline_download_url,
                    offline_download_headers_json, offline_refresh_interval_hours,
                    offline_last_sync_at, offline_last_sync_status, offline_last_sync_message,
                    offline_last_success_at, updated_at
                ) VALUES (
                    1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, '', '', NULL, ?
                )
                """,
                (
                    int(config.geoip.enabled),
                    max(0, int(config.geoip.online_cache_ttl_seconds or 0)),
                    int(config.geoip.primary.enabled),
                    config.geoip.primary.url,
                    config.geoip.primary.method,
                    config.geoip.primary.headers_json,
                    config.geoip.primary.body_template,
                    config.geoip.primary.ip_param_name,
                    config.geoip.primary.timeout,
                    config.geoip.primary.country_path,
                    config.geoip.primary.region_path,
                    config.geoip.primary.city_path,
                    config.geoip.primary.full_path,
                    int(config.geoip.offline.enabled),
                    config.geoip.offline.db_path,
                    config.geoip.offline.locale,
                    config.geoip.offline.download_url,
                    config.geoip.offline.download_headers_json,
                    int(config.geoip.offline.refresh_interval_hours or 24),
                    now,
                ),
            )

            geo_sources = list(config.geoip.sources)
            if not geo_sources and config.geoip.primary.url:
                geo_sources.append(
                    OnlineGeoIPSource(
                        name="primary",
                        enabled=config.geoip.primary.enabled,
                        weight=1,
                        url=config.geoip.primary.url,
                        method=config.geoip.primary.method,
                        request_location="query" if config.geoip.primary.method.upper() == "GET" else "body",
                        body_format="json",
                        query_params_json="{}",
                        headers_json=config.geoip.primary.headers_json,
                        body_template=config.geoip.primary.body_template,
                        ip_param_name=config.geoip.primary.ip_param_name,
                        timeout=config.geoip.primary.timeout,
                        country_path=config.geoip.primary.country_path,
                        region_path=config.geoip.primary.region_path,
                        city_path=config.geoip.primary.city_path,
                        full_path=config.geoip.primary.full_path,
                    )
                )
            for source in geo_sources:
                self._insert_geoip_source(connection, source)

            for rule in config.proxy_rules:
                self._insert_rule(connection, rule, source=rule.source or "manual")

    def _insert_rule(
        self,
        connection: sqlite3.Connection,
        rule: ProxyRule,
        source: str = "manual",
    ) -> int:
        now = utc_now()
        cursor = connection.execute(
            """
            INSERT INTO forward_rules (
                source, external_id, name, request_host, path_prefix, target_url, strip_prefix, timeout,
                max_redirects, follow_redirects, retry_times, enable_streaming, ip_whitelist, region_filters,
                is_default, enabled, priority, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source,
                rule.external_id,
                rule.name,
                normalize_request_host(rule.request_host),
                rule.path_prefix,
                rule.target_url,
                int(rule.strip_prefix),
                rule.timeout,
                rule.max_redirects,
                int(rule.follow_redirects),
                rule.retry_times,
                int(rule.enable_streaming),
                normalize_region_filter_value(rule.ip_whitelist),
                normalize_region_filter_value(rule.region_filters),
                int(rule.is_default),
                int(rule.enabled),
                rule.priority,
                rule.notes,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)

    def _insert_geoip_source(
        self,
        connection: sqlite3.Connection,
        source: OnlineGeoIPSource,
    ) -> int:
        now = utc_now()
        cursor = connection.execute(
            """
            INSERT INTO geoip_online_sources (
                name, enabled, weight, url, method, request_location, body_format,
                query_params_json, headers_json, body_template, ip_param_name, timeout, country_path,
                region_path, city_path, full_path, priority, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source.name,
                int(source.enabled),
                max(1, int(source.weight or 1)),
                source.url,
                source.method,
                source.request_location,
                source.body_format,
                self._normalize_json_text(source.query_params_json or "{}"),
                source.headers_json,
                source.body_template,
                source.ip_param_name,
                int(source.timeout or 3),
                source.country_path,
                source.region_path,
                source.city_path,
                source.full_path,
                int(source.priority or 0),
                source.notes,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)

    def _ensure_route_groups(self, connection: sqlite3.Connection) -> None:
        now = utc_now()
        connection.execute(
            """
            INSERT INTO route_groups (request_host, path_prefix, region_matching_enabled, notes, updated_at)
            SELECT DISTINCT COALESCE(forward_rules.request_host, ''), forward_rules.path_prefix, 0, '', ?
            FROM forward_rules
            WHERE NOT EXISTS (
                SELECT 1
                FROM route_groups
                WHERE route_groups.request_host = COALESCE(forward_rules.request_host, '')
                  AND route_groups.path_prefix = forward_rules.path_prefix
            )
            """,
            (now,),
        )

    def _upsert_route_group(
        self,
        connection: sqlite3.Connection,
        path_prefix: str,
        request_host: str = "",
        region_matching_enabled: Optional[bool] = None,
        notes: Optional[str] = None,
    ) -> None:
        normalized_host = normalize_request_host(request_host)
        normalized_enabled = None if region_matching_enabled is None else coerce_bool(region_matching_enabled, False)
        existing = connection.execute(
            "SELECT * FROM route_groups WHERE request_host = ? AND path_prefix = ?",
            (normalized_host, path_prefix),
        ).fetchone()
        now = utc_now()
        if existing:
            connection.execute(
                """
                UPDATE route_groups
                SET region_matching_enabled = ?, notes = ?, updated_at = ?
                WHERE request_host = ? AND path_prefix = ?
                """,
                (
                    int(normalized_enabled if normalized_enabled is not None else bool(existing["region_matching_enabled"])),
                    notes if notes is not None else existing["notes"],
                    now,
                    normalized_host,
                    path_prefix,
                ),
            )
            return

        connection.execute(
            """
            INSERT INTO route_groups (request_host, path_prefix, region_matching_enabled, notes, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                normalized_host,
                path_prefix,
                int(normalized_enabled) if normalized_enabled is not None else 0,
                notes or "",
                now,
            ),
        )

    def _cleanup_orphan_route_group(self, connection: sqlite3.Connection, path_prefix: str, request_host: str = "") -> None:
        normalized_host = normalize_request_host(request_host)
        row = connection.execute(
            "SELECT COUNT(*) FROM forward_rules WHERE request_host = ? AND path_prefix = ?",
            (normalized_host, path_prefix),
        ).fetchone()
        if row and row[0] == 0:
            connection.execute(
                "DELETE FROM route_groups WHERE request_host = ? AND path_prefix = ?",
                (normalized_host, path_prefix),
            )

    def load_runtime_config(self) -> Config:
        with self._connect() as connection:
            self._ensure_route_groups(connection)
            system_row = connection.execute("SELECT * FROM system_settings WHERE id = 1").fetchone()
            feature_row = connection.execute("SELECT * FROM feature_flags WHERE id = 1").fetchone()
            remote_row = connection.execute("SELECT * FROM remote_config_sources WHERE id = 1").fetchone()
            geo_row = connection.execute("SELECT * FROM geoip_settings WHERE id = 1").fetchone()
            geo_source_rows = connection.execute(
                """
                SELECT *
                FROM geoip_online_sources
                ORDER BY priority DESC, id ASC
                """
            ).fetchall()
            group_rows = connection.execute(
                """
                SELECT *
                FROM route_groups
                ORDER BY
                    CASE WHEN request_host = '' THEN 1 ELSE 0 END,
                    request_host ASC,
                    LENGTH(path_prefix) DESC,
                    path_prefix ASC
                """
            ).fetchall()
            rules = connection.execute(
                """
                SELECT *
                FROM forward_rules
                ORDER BY
                    CASE WHEN request_host = '' THEN 1 ELSE 0 END,
                    request_host ASC,
                    LENGTH(path_prefix) DESC,
                    priority DESC,
                    id ASC
                """
            ).fetchall()

        config = Config()
        if system_row:
            config.server = ServerConfig(
                host=system_row["host"],
                port=system_row["port"],
                workers=system_row["workers"],
                keepalive_timeout=system_row["keepalive_timeout"],
                max_connections=system_row["max_connections"],
                max_connections_per_host=system_row["max_connections_per_host"],
            )
            config.ssl = SSLConfig(
                enabled=bool(system_row["ssl_enabled"]),
                cert_file=system_row["cert_file"],
                key_file=system_row["key_file"],
            )
            config.logging = LoggingConfig(
                level=system_row["logging_level"],
                format=system_row["logging_format"],
                file_path=system_row["logging_file_path"],
                max_size=system_row["logging_max_size"],
                backup_count=system_row["logging_backup_count"],
            )
            config.streaming = StreamingConfig(
                enabled=bool(system_row["streaming_enabled"]),
                chunk_size=system_row["streaming_chunk_size"],
                large_file_threshold=system_row["streaming_large_file_threshold"],
                stream_timeout=system_row["streaming_stream_timeout"],
                read_timeout=system_row["streaming_read_timeout"],
                write_timeout=system_row["streaming_write_timeout"],
                buffer_size=system_row["streaming_buffer_size"],
                enable_range_support=bool(system_row["streaming_enable_range_support"]),
                max_request_body_size=system_row["streaming_max_request_body_size"],
            )
            config.cache = CacheConfig(
                enabled=bool(system_row["cache_enabled"]),
                cache_dir=system_row["cache_dir"],
                max_size=system_row["cache_max_size"],
                max_entries=system_row["cache_max_entries"],
                default_ttl=system_row["cache_default_ttl"],
                chunk_size=system_row["cache_chunk_size"],
                max_entry_size=system_row["cache_max_entry_size"],
                max_request_cache_size=system_row["cache_max_request_cache_size"],
                enable_preload=bool(system_row["cache_enable_preload"]),
                preload_concurrency=system_row["cache_preload_concurrency"],
                enable_validation=bool(system_row["cache_enable_validation"]),
                validation_interval=system_row["cache_validation_interval"],
            )
            config.default_timeout = system_row["default_timeout"]
            config.max_redirects = system_row["max_redirects"]
            config.follow_redirects = bool(system_row["follow_redirects"])
            config.trust_forward_headers = bool(system_row["trust_forward_headers"])
            config.database_path = system_row["database_path"]

        if feature_row:
            config.region_matching_enabled = bool(feature_row["region_matching_enabled"])

        config.route_groups = [
            RouteGroupConfig(
                path_prefix=row["path_prefix"],
                request_host=normalize_request_host(row["request_host"]),
                region_matching_enabled=bool(row["region_matching_enabled"]),
                notes=row["notes"],
            )
            for row in group_rows
        ]

        if remote_row:
            config.remote_config = RemoteConfigSettings(
                enabled=bool(remote_row["enabled"]),
                url=remote_row["url"],
                method=remote_row["method"],
                headers_json=remote_row["headers_json"],
                body_template=remote_row["body_template"],
                timeout=remote_row["timeout"],
                data_path=remote_row["data_path"],
                external_id_field=remote_row["external_id_field"],
                name_field=remote_row["name_field"],
                path_prefix_field=remote_row["path_prefix_field"],
                target_url_field=remote_row["target_url_field"],
                strip_prefix_field=remote_row["strip_prefix_field"],
                timeout_field=remote_row["timeout_field"],
                max_redirects_field=remote_row["max_redirects_field"],
                retry_times_field=remote_row["retry_times_field"],
                enable_streaming_field=remote_row["enable_streaming_field"],
                region_filters_field=remote_row["region_filters_field"],
                is_default_field=remote_row["is_default_field"],
                enabled_field=remote_row["enabled_field"],
                priority_field=remote_row["priority_field"],
            )

        if geo_row:
            sources = [
                OnlineGeoIPSource(
                    source_id=row["id"],
                    name=row["name"],
                    enabled=bool(row["enabled"]),
                    weight=row["weight"],
                    url=row["url"],
                    method=row["method"],
                    request_location=row["request_location"],
                    body_format=row["body_format"],
                    query_params_json=row["query_params_json"],
                    headers_json=row["headers_json"],
                    body_template=row["body_template"],
                    ip_param_name=row["ip_param_name"],
                    timeout=row["timeout"],
                    country_path=row["country_path"],
                    region_path=row["region_path"],
                    city_path=row["city_path"],
                    full_path=row["full_path"],
                    priority=row["priority"],
                    notes=row["notes"],
                )
                for row in geo_source_rows
            ]
            if not sources and geo_row["primary_url"]:
                sources.append(
                    OnlineGeoIPSource(
                        name="primary",
                        enabled=bool(geo_row["primary_enabled"]),
                        weight=1,
                        url=geo_row["primary_url"],
                        method=geo_row["primary_method"],
                        request_location="query" if str(geo_row["primary_method"]).upper() == "GET" else "body",
                        body_format="json",
                        query_params_json="{}",
                        headers_json=geo_row["primary_headers_json"],
                        body_template=geo_row["primary_body_template"],
                        ip_param_name=geo_row["primary_ip_param_name"],
                        timeout=geo_row["primary_timeout"],
                        country_path=geo_row["primary_country_path"],
                        region_path=geo_row["primary_region_path"],
                        city_path=geo_row["primary_city_path"],
                        full_path=geo_row["primary_full_path"],
                    )
                )
            config.geoip = GeoIPSettings(
                enabled=bool(geo_row["enabled"]),
                sources=sources,
                online_cache_ttl_seconds=max(0, int(geo_row["online_cache_ttl_seconds"] or 0)),
                primary=PrimaryGeoIPSettings(
                    enabled=bool(geo_row["primary_enabled"]),
                    url=geo_row["primary_url"],
                    method=geo_row["primary_method"],
                    headers_json=geo_row["primary_headers_json"],
                    body_template=geo_row["primary_body_template"],
                    ip_param_name=geo_row["primary_ip_param_name"],
                    timeout=geo_row["primary_timeout"],
                    country_path=geo_row["primary_country_path"],
                    region_path=geo_row["primary_region_path"],
                    city_path=geo_row["primary_city_path"],
                    full_path=geo_row["primary_full_path"],
                ),
                offline=OfflineGeoIPSettings(
                    enabled=bool(geo_row["offline_enabled"]),
                    db_path=geo_row["offline_db_path"],
                    locale=geo_row["offline_locale"],
                    download_url=geo_row["offline_download_url"],
                    download_headers_json=geo_row["offline_download_headers_json"],
                    refresh_interval_hours=geo_row["offline_refresh_interval_hours"],
                    last_sync_at=geo_row["offline_last_sync_at"] or "",
                    last_sync_status=geo_row["offline_last_sync_status"] or "",
                    last_sync_message=geo_row["offline_last_sync_message"] or "",
                    last_success_at=geo_row["offline_last_success_at"] or "",
                ),
            )

        config.proxy_rules = [self._row_to_rule(row) for row in rules]
        config.admin_auth = self.bootstrap_config.admin_auth
        return config

    def get_dashboard_data(self) -> Dict[str, Any]:
        config = self.load_runtime_config()
        rules = [self.serialize_rule(rule) for rule in config.proxy_rules]
        groups = [self.serialize_route_group(group, rules) for group in config.route_groups]
        log_settings = self.get_route_log_settings()
        return {
            "summary": {
                "database_path": str(self.db_path),
                "total_rules": len(rules),
                "enabled_rules": sum(1 for rule in rules if rule["enabled"]),
                "default_rules": sum(1 for rule in rules if rule["is_default"]),
                "route_group_count": len(groups),
                "region_enabled_group_count": sum(1 for group in groups if group["region_matching_enabled"]),
                "route_log_count": log_settings.get("total_logs", 0),
                "server": {
                    "host": config.server.host,
                    "port": config.server.port,
                    "streaming_enabled": config.streaming.enabled,
                    "cache_enabled": config.cache.enabled,
                },
            },
            "route_groups": groups,
            "geoip": self.get_geoip_settings(),
            "route_log_settings": log_settings,
            "rules": rules,
        }

    def get_route_log_settings(self) -> Dict[str, Any]:
        with self._connect() as connection:
            settings_row = connection.execute("SELECT * FROM route_log_settings WHERE id = 1").fetchone()
            total_logs = connection.execute("SELECT COUNT(*) FROM route_logs").fetchone()[0]
        if not settings_row:
            return {
                "retention_days": 30,
                "last_pruned_at": "",
                "updated_at": "",
                "total_logs": total_logs,
            }
        return {
            "retention_days": settings_row["retention_days"],
            "last_pruned_at": settings_row["last_pruned_at"] or "",
            "updated_at": settings_row["updated_at"] or "",
            "total_logs": total_logs,
        }

    def update_route_log_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        retention_days = max(1, int(payload.get("retention_days", 30) or 30))
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE route_log_settings
                SET retention_days = ?, updated_at = ?
                WHERE id = 1
                """,
                (retention_days, now),
            )
        self.prune_route_logs(force=True)
        return self.get_route_log_settings()

    def insert_route_log(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        created_at = str(payload.get("created_at", "")).strip() or utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO route_logs (
                    request_method, request_path, request_query_string, path_prefix, rule_id,
                    rule_name, rule_source, target_url, original_client_ip, client_ip, region_matching_enabled,
                    geo_source, geo_summary, geo_country, geo_region, geo_city,
                    configured_ip_whitelist, matched_ip_whitelist, configured_regions, matched_region, match_strategy, match_detail,
                    upstream_status, cache_status, redirect_count, transport_mode,
                    operation_duration_ms, result_status, error_message, created_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    str(payload.get("request_method", "")).strip(),
                    str(payload.get("request_path", "")).strip(),
                    str(payload.get("request_query_string", "")).strip(),
                    str(payload.get("path_prefix", "")).strip(),
                    payload.get("rule_id"),
                    str(payload.get("rule_name", "")).strip(),
                    str(payload.get("rule_source", "")).strip(),
                    str(payload.get("target_url", "")).strip(),
                    str(payload.get("original_client_ip", "")).strip(),
                    str(payload.get("client_ip", "")).strip(),
                    int(coerce_bool(payload.get("region_matching_enabled"), False)),
                    str(payload.get("geo_source", "")).strip(),
                    str(payload.get("geo_summary", "")).strip(),
                    str(payload.get("geo_country", "")).strip(),
                    str(payload.get("geo_region", "")).strip(),
                    str(payload.get("geo_city", "")).strip(),
                    str(payload.get("configured_ip_whitelist", "")).strip(),
                    str(payload.get("matched_ip_whitelist", "")).strip(),
                    str(payload.get("configured_regions", "")).strip(),
                    str(payload.get("matched_region", "")).strip(),
                    str(payload.get("match_strategy", "")).strip(),
                    str(payload.get("match_detail", "")).strip(),
                    int(payload.get("upstream_status", 0) or 0),
                    str(payload.get("cache_status", "")).strip(),
                    int(payload.get("redirect_count", 0) or 0),
                    str(payload.get("transport_mode", "")).strip(),
                    int(payload.get("operation_duration_ms", 0) or 0),
                    str(payload.get("result_status", "")).strip(),
                    str(payload.get("error_message", "")).strip(),
                    created_at,
                ),
            )
            log_id = int(cursor.lastrowid)
        self.prune_route_logs(force=False)
        try:
            return self.get_route_log(log_id)
        except KeyError:
            return {
                "id": log_id,
                "created_at": created_at,
                **payload,
            }

    def get_route_log(self, log_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM route_logs WHERE id = ?", (log_id,)).fetchone()
        if not row:
            raise KeyError(f"Route log {log_id} not found")
        return self.serialize_route_log(row)

    def list_route_logs(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        filters = filters or {}
        clauses: List[str] = []
        params: List[Any] = []

        keyword = str(filters.get("keyword", "")).strip()
        if keyword:
            like_value = f"%{keyword}%"
            clauses.append(
                "("
                "request_path LIKE ? OR path_prefix LIKE ? OR rule_name LIKE ? OR "
                "target_url LIKE ? OR geo_summary LIKE ? OR matched_region LIKE ? OR client_ip LIKE ? OR original_client_ip LIKE ?"
                ")"
            )
            params.extend([like_value] * 8)

        path_prefix = str(filters.get("path_prefix", "")).strip()
        if path_prefix:
            clauses.append("path_prefix = ?")
            params.append(path_prefix)

        match_strategy = str(filters.get("match_strategy", "")).strip()
        if match_strategy:
            clauses.append("match_strategy = ?")
            params.append(match_strategy)

        result_status = str(filters.get("result_status", "")).strip()
        if result_status:
            clauses.append("result_status = ?")
            params.append(result_status)

        date_from = str(filters.get("date_from", "")).strip()
        if date_from:
            clauses.append("created_at >= ?")
            params.append(date_from)

        date_to = str(filters.get("date_to", "")).strip()
        if date_to:
            clauses.append("created_at <= ?")
            params.append(date_to)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit = max(1, min(500, int(filters.get("limit", 100) or 100)))

        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM route_logs
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
            total = connection.execute(
                f"SELECT COUNT(*) FROM route_logs {where_sql}",
                params,
            ).fetchone()[0]

        return {
            "items": [self.serialize_route_log(row) for row in rows],
            "total": total,
            "limit": limit,
            "filters": {
                "keyword": keyword,
                "path_prefix": path_prefix,
                "match_strategy": match_strategy,
                "result_status": result_status,
                "date_from": date_from,
                "date_to": date_to,
            },
        }

    def delete_route_logs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ids = payload.get("ids") or []
        delete_all = coerce_bool(payload.get("delete_all"), False)
        with self._connect() as connection:
            if delete_all:
                deleted_count = connection.execute("SELECT COUNT(*) FROM route_logs").fetchone()[0]
                connection.execute("DELETE FROM route_logs")
            else:
                normalized_ids = [int(item) for item in ids if str(item).strip()]
                if not normalized_ids:
                    raise ValueError("No route log ids were provided.")
                placeholders = ", ".join(["?"] * len(normalized_ids))
                deleted_count = connection.execute(
                    f"SELECT COUNT(*) FROM route_logs WHERE id IN ({placeholders})",
                    normalized_ids,
                ).fetchone()[0]
                connection.execute(
                    f"DELETE FROM route_logs WHERE id IN ({placeholders})",
                    normalized_ids,
                )
        return {
            "deleted_count": deleted_count,
            "settings": self.get_route_log_settings(),
        }

    def prune_route_logs(self, *, force: bool = False) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        now_text = now.isoformat(timespec="seconds")
        with self._connect() as connection:
            settings_row = connection.execute("SELECT * FROM route_log_settings WHERE id = 1").fetchone()
            if not settings_row:
                return {"deleted_count": 0, "retention_days": 30, "last_pruned_at": ""}

            retention_days = max(1, int(settings_row["retention_days"] or 30))
            last_pruned_at = str(settings_row["last_pruned_at"] or "").strip()
            if not force and last_pruned_at:
                try:
                    last_pruned_dt = datetime.fromisoformat(last_pruned_at)
                    if now - last_pruned_dt < timedelta(hours=1):
                        return {
                            "deleted_count": 0,
                            "retention_days": retention_days,
                            "last_pruned_at": last_pruned_at,
                        }
                except ValueError:
                    pass

            cutoff = (now - timedelta(days=retention_days)).isoformat(timespec="seconds")
            deleted_count = connection.execute(
                "SELECT COUNT(*) FROM route_logs WHERE created_at < ?",
                (cutoff,),
            ).fetchone()[0]
            connection.execute("DELETE FROM route_logs WHERE created_at < ?", (cutoff,))
            connection.execute(
                """
                UPDATE route_log_settings
                SET last_pruned_at = ?, updated_at = ?
                WHERE id = 1
                """,
                (now_text, now_text),
            )

        return {
            "deleted_count": deleted_count,
            "retention_days": retention_days,
            "last_pruned_at": now_text,
        }

    def list_rules(self) -> List[Dict[str, Any]]:
        config = self.load_runtime_config()
        return [self.serialize_rule(rule) for rule in config.proxy_rules]

    def list_route_groups(self) -> List[Dict[str, Any]]:
        config = self.load_runtime_config()
        rules = [self.serialize_rule(rule) for rule in config.proxy_rules]
        return [self.serialize_route_group(group, rules) for group in config.route_groups]

    def create_route_group(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        path_prefix = str(payload.get("path_prefix", "")).strip()
        request_host = normalize_request_host(payload.get("request_host", ""))
        if not path_prefix:
            raise ValueError("path_prefix is required.")
        if not path_prefix.startswith("/"):
            raise ValueError("path_prefix must start with '/'.")

        with self._connect() as connection:
            existing = connection.execute(
                "SELECT 1 FROM route_groups WHERE request_host = ? AND path_prefix = ?",
                (request_host, path_prefix),
            ).fetchone()
            if existing:
                raise ValueError(f"Route group {request_host or '*'} {path_prefix} already exists.")
            self._upsert_route_group(
                connection,
                path_prefix,
                request_host=request_host,
                region_matching_enabled=coerce_bool(payload.get("region_matching_enabled"), False),
                notes=str(payload.get("notes", "")).strip(),
            )
        return self.get_route_group(path_prefix, request_host)

    def get_route_group(self, path_prefix: str, request_host: str = "") -> Dict[str, Any]:
        path_prefix = str(path_prefix).strip()
        normalized_host = normalize_request_host(request_host)
        groups = self.list_route_groups()
        for group in groups:
            if (
                group["path_prefix"] == path_prefix
                and normalize_request_host(group.get("request_host", "")) == normalized_host
            ):
                return group
        raise KeyError(f"Route group {normalized_host or '*'} {path_prefix} not found")

    def update_route_group(self, path_prefix: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        old_path_prefix = str(payload.get("old_path_prefix", path_prefix)).strip()
        new_path_prefix = str(payload.get("path_prefix", path_prefix)).strip()
        old_request_host = normalize_request_host(payload.get("old_request_host", payload.get("request_host", "")))
        new_request_host = normalize_request_host(payload.get("request_host", old_request_host))
        if not old_path_prefix:
            raise ValueError("old_path_prefix is required.")
        if not new_path_prefix:
            raise ValueError("path_prefix is required.")
        if not old_path_prefix.startswith("/") or not new_path_prefix.startswith("/"):
            raise ValueError("path_prefix must start with '/'.")

        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM route_groups WHERE request_host = ? AND path_prefix = ?",
                (old_request_host, old_path_prefix),
            ).fetchone()
            if not existing:
                raise KeyError(f"Route group {old_request_host or '*'} {old_path_prefix} not found")

            if old_path_prefix != new_path_prefix or old_request_host != new_request_host:
                duplicate = connection.execute(
                    "SELECT 1 FROM route_groups WHERE request_host = ? AND path_prefix = ?",
                    (new_request_host, new_path_prefix),
                ).fetchone()
                if duplicate:
                    raise ValueError(f"Route group {new_request_host or '*'} {new_path_prefix} already exists.")
                now = utc_now()
                connection.execute(
                    """
                    UPDATE route_groups
                    SET request_host = ?, path_prefix = ?, region_matching_enabled = ?, notes = ?, updated_at = ?
                    WHERE request_host = ? AND path_prefix = ?
                    """,
                    (
                        new_request_host,
                        new_path_prefix,
                        int(coerce_bool(payload.get("region_matching_enabled"), bool(existing["region_matching_enabled"]))),
                        str(payload.get("notes", existing["notes"])).strip(),
                        now,
                        old_request_host,
                        old_path_prefix,
                    ),
                )
                connection.execute(
                    """
                    UPDATE forward_rules
                    SET request_host = ?, path_prefix = ?, updated_at = ?
                    WHERE request_host = ? AND path_prefix = ?
                    """,
                    (new_request_host, new_path_prefix, now, old_request_host, old_path_prefix),
                )
            else:
                self._upsert_route_group(
                    connection,
                    new_path_prefix,
                    request_host=new_request_host,
                    region_matching_enabled=coerce_bool(
                        payload.get("region_matching_enabled"),
                        bool(existing["region_matching_enabled"]),
                    ),
                    notes=str(payload.get("notes", existing["notes"])).strip(),
                )

        return self.get_route_group(new_path_prefix, new_request_host)

    def delete_route_group(self, path_prefix: str, request_host: str = "") -> None:
        path_prefix = str(path_prefix).strip()
        normalized_host = normalize_request_host(request_host)
        if not path_prefix:
            raise ValueError("path_prefix is required.")
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT 1 FROM route_groups WHERE request_host = ? AND path_prefix = ?",
                (normalized_host, path_prefix),
            ).fetchone()
            if not existing:
                raise KeyError(f"Route group {normalized_host or '*'} {path_prefix} not found")
            rule_count = connection.execute(
                "SELECT COUNT(*) FROM forward_rules WHERE request_host = ? AND path_prefix = ?",
                (normalized_host, path_prefix),
            ).fetchone()[0]
            if rule_count:
                raise ValueError("This path prefix still has forwarding rules. Delete or migrate those rules first.")
            connection.execute(
                "DELETE FROM route_groups WHERE request_host = ? AND path_prefix = ?",
                (normalized_host, path_prefix),
            )

    def get_rule(self, rule_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM forward_rules WHERE id = ?", (rule_id,)).fetchone()
        if not row:
            raise KeyError(f"Rule {rule_id} not found")
        return self.serialize_rule(self._row_to_rule(row))

    def create_rule(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        rule = self._payload_to_rule(payload)
        source = str(payload.get("source", "manual")).strip() or "manual"
        with self._connect() as connection:
            self._upsert_route_group(
                connection,
                rule.path_prefix,
                request_host=rule.request_host,
                region_matching_enabled=payload.get("region_matching_enabled"),
                notes=payload.get("group_notes"),
            )
            rule_id = self._insert_rule(connection, rule, source=source)
            row = connection.execute("SELECT * FROM forward_rules WHERE id = ?", (rule_id,)).fetchone()
        return self.serialize_rule(self._row_to_rule(row))

    def update_rule(self, rule_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._connect() as connection:
            existing_row = connection.execute(
                "SELECT * FROM forward_rules WHERE id = ?",
                (rule_id,),
            ).fetchone()
            if not existing_row:
                raise KeyError(f"Rule {rule_id} not found")

            old_path_prefix = existing_row["path_prefix"]
            old_request_host = normalize_request_host(existing_row["request_host"])
            merged_payload = dict(self.serialize_rule(self._row_to_rule(existing_row)))
            merged_payload.update(payload)
            rule = self._payload_to_rule(merged_payload)
            source = str(merged_payload.get("source", existing_row["source"])).strip() or "manual"
            external_id = merged_payload.get("external_id")
            now = utc_now()
            connection.execute(
                """
                UPDATE forward_rules
                SET source = ?, external_id = ?, name = ?, request_host = ?, path_prefix = ?, target_url = ?,
                    strip_prefix = ?, timeout = ?, max_redirects = ?, follow_redirects = ?, retry_times = ?,
                    enable_streaming = ?, ip_whitelist = ?, region_filters = ?, is_default = ?, enabled = ?,
                    priority = ?, notes = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    source,
                    external_id,
                    rule.name,
                    normalize_request_host(rule.request_host),
                    rule.path_prefix,
                    rule.target_url,
                    int(rule.strip_prefix),
                    rule.timeout,
                    rule.max_redirects,
                    int(rule.follow_redirects),
                    rule.retry_times,
                    int(rule.enable_streaming),
                    normalize_region_filter_value(rule.ip_whitelist),
                    normalize_region_filter_value(rule.region_filters),
                    int(rule.is_default),
                    int(rule.enabled),
                    rule.priority,
                    rule.notes,
                    now,
                    rule_id,
                ),
            )
            self._upsert_route_group(
                connection,
                rule.path_prefix,
                request_host=rule.request_host,
                region_matching_enabled=payload.get("region_matching_enabled"),
                notes=payload.get("group_notes"),
            )
            if old_path_prefix != rule.path_prefix or old_request_host != normalize_request_host(rule.request_host):
                self._cleanup_orphan_route_group(connection, old_path_prefix, old_request_host)
            row = connection.execute("SELECT * FROM forward_rules WHERE id = ?", (rule_id,)).fetchone()
        return self.serialize_rule(self._row_to_rule(row))

    def delete_rule(self, rule_id: int) -> None:
        with self._connect() as connection:
            existing_row = connection.execute(
                "SELECT path_prefix, request_host FROM forward_rules WHERE id = ?",
                (rule_id,),
            ).fetchone()
            cursor = connection.execute("DELETE FROM forward_rules WHERE id = ?", (rule_id,))
            if cursor.rowcount == 0:
                raise KeyError(f"Rule {rule_id} not found")
            if existing_row:
                self._cleanup_orphan_route_group(
                    connection,
                    existing_row["path_prefix"],
                    normalize_request_host(existing_row["request_host"]),
                )

    def get_feature_flags(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM feature_flags WHERE id = 1").fetchone()
        if not row:
            return {"region_matching_enabled": False}
        return {"region_matching_enabled": bool(row["region_matching_enabled"])}

    def update_feature_flags(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = utc_now()
        region_matching_enabled = coerce_bool(payload.get("region_matching_enabled"), False)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE feature_flags
                SET region_matching_enabled = ?, updated_at = ?
                WHERE id = 1
                """,
                (int(region_matching_enabled), now),
            )
        return self.get_feature_flags()

    def get_remote_config(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM remote_config_sources WHERE id = 1").fetchone()
        if not row:
            return {}
        return {
            "enabled": bool(row["enabled"]),
            "url": row["url"],
            "method": row["method"],
            "headers_json": row["headers_json"],
            "body_template": row["body_template"],
            "timeout": row["timeout"],
            "data_path": row["data_path"],
            "external_id_field": row["external_id_field"],
            "name_field": row["name_field"],
            "path_prefix_field": row["path_prefix_field"],
            "target_url_field": row["target_url_field"],
            "strip_prefix_field": row["strip_prefix_field"],
            "timeout_field": row["timeout_field"],
            "max_redirects_field": row["max_redirects_field"],
            "retry_times_field": row["retry_times_field"],
            "enable_streaming_field": row["enable_streaming_field"],
            "region_filters_field": row["region_filters_field"],
            "is_default_field": row["is_default_field"],
            "enabled_field": row["enabled_field"],
            "priority_field": row["priority_field"],
            "last_sync_at": row["last_sync_at"],
            "last_sync_status": row["last_sync_status"],
            "last_sync_message": row["last_sync_message"],
        }

    def update_remote_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE remote_config_sources
                SET enabled = ?, url = ?, method = ?, headers_json = ?, body_template = ?,
                    timeout = ?, data_path = ?, external_id_field = ?, name_field = ?,
                    path_prefix_field = ?, target_url_field = ?, strip_prefix_field = ?,
                    timeout_field = ?, max_redirects_field = ?, retry_times_field = ?,
                    enable_streaming_field = ?, region_filters_field = ?, is_default_field = ?,
                    enabled_field = ?, priority_field = ?, updated_at = ?
                WHERE id = 1
                """,
                (
                    int(coerce_bool(payload.get("enabled"), False)),
                    str(payload.get("url", "")).strip(),
                    str(payload.get("method", "GET")).strip().upper() or "GET",
                    self._normalize_json_text(payload.get("headers_json", "{}")),
                    str(payload.get("body_template", "")),
                    int(payload.get("timeout", 5) or 5),
                    str(payload.get("data_path", "data")).strip(),
                    str(payload.get("external_id_field", "id")).strip() or "id",
                    str(payload.get("name_field", "name")).strip() or "name",
                    str(payload.get("path_prefix_field", "path_prefix")).strip() or "path_prefix",
                    str(payload.get("target_url_field", "target_url")).strip() or "target_url",
                    str(payload.get("strip_prefix_field", "strip_prefix")).strip() or "strip_prefix",
                    str(payload.get("timeout_field", "timeout")).strip() or "timeout",
                    str(payload.get("max_redirects_field", "max_redirects")).strip() or "max_redirects",
                    str(payload.get("retry_times_field", "retry_times")).strip() or "retry_times",
                    str(payload.get("enable_streaming_field", "enable_streaming")).strip()
                    or "enable_streaming",
                    str(payload.get("region_filters_field", "region_filters")).strip()
                    or "region_filters",
                    str(payload.get("is_default_field", "is_default")).strip() or "is_default",
                    str(payload.get("enabled_field", "enabled")).strip() or "enabled",
                    str(payload.get("priority_field", "priority")).strip() or "priority",
                    now,
                ),
            )
        return self.get_remote_config()

    def get_geoip_settings(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM geoip_settings WHERE id = 1").fetchone()
            source_rows = connection.execute(
                """
                SELECT *
                FROM geoip_online_sources
                ORDER BY priority DESC, id ASC
                """
            ).fetchall()
        if not row:
            return {}
        sources = [
            {
                "id": source_row["id"],
                "name": source_row["name"],
                "enabled": bool(source_row["enabled"]),
                "weight": source_row["weight"],
                "url": source_row["url"],
                "method": source_row["method"],
                "request_location": source_row["request_location"],
                "body_format": source_row["body_format"],
                "query_params_json": source_row["query_params_json"],
                "headers_json": source_row["headers_json"],
                "body_template": source_row["body_template"],
                "ip_param_name": source_row["ip_param_name"],
                "timeout": source_row["timeout"],
                "country_path": source_row["country_path"],
                "region_path": source_row["region_path"],
                "city_path": source_row["city_path"],
                "full_path": source_row["full_path"],
                "priority": source_row["priority"],
                "notes": source_row["notes"],
            }
            for source_row in source_rows
        ]
        if not sources and row["primary_url"]:
            sources.append(
                {
                    "id": None,
                    "name": "primary",
                    "enabled": bool(row["primary_enabled"]),
                    "weight": 1,
                    "url": row["primary_url"],
                    "method": row["primary_method"],
                    "request_location": "query" if str(row["primary_method"]).upper() == "GET" else "body",
                    "body_format": "json",
                    "query_params_json": "{}",
                    "headers_json": row["primary_headers_json"],
                    "body_template": row["primary_body_template"],
                    "ip_param_name": row["primary_ip_param_name"],
                    "timeout": row["primary_timeout"],
                    "country_path": row["primary_country_path"],
                    "region_path": row["primary_region_path"],
                    "city_path": row["primary_city_path"],
                    "full_path": row["primary_full_path"],
                    "priority": 0,
                    "notes": "",
                }
            )
        offline_status = self._build_offline_geoip_status(
            db_path=row["offline_db_path"],
            refresh_interval_hours=row["offline_refresh_interval_hours"],
            last_sync_at=row["offline_last_sync_at"],
            last_success_at=row["offline_last_success_at"],
            last_sync_status=row["offline_last_sync_status"],
            last_sync_message=row["offline_last_sync_message"],
        )
        return {
            "enabled": bool(row["enabled"]),
            "online_cache_ttl_seconds": max(0, int(row["online_cache_ttl_seconds"] or 0)),
            "sources": sources,
            "offline": {
                "enabled": bool(row["offline_enabled"]),
                "db_path": row["offline_db_path"],
                "locale": row["offline_locale"],
                "download_url": row["offline_download_url"],
                "download_headers_json": row["offline_download_headers_json"],
                "refresh_interval_hours": row["offline_refresh_interval_hours"],
                "last_sync_at": row["offline_last_sync_at"],
                "last_sync_status": row["offline_last_sync_status"],
                "last_sync_message": row["offline_last_sync_message"],
                "last_success_at": row["offline_last_success_at"],
                "status": offline_status,
            },
        }

    def update_geoip_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        source_payloads = payload.get("sources", []) or []
        primary_payload = payload.get("primary", {}) or {}
        offline_payload = payload.get("offline", {}) or {}
        now = utc_now()
        normalized_sources = []
        if isinstance(source_payloads, list):
            for index, item in enumerate(source_payloads):
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url", "")).strip()
                if not url:
                    continue
                normalized_sources.append(
                    OnlineGeoIPSource(
                        name=str(item.get("name", f"source-{index + 1}")).strip(),
                        enabled=coerce_bool(item.get("enabled"), True),
                        weight=max(1, int(item.get("weight", 1) or 1)),
                        url=url,
                        method=str(item.get("method", "GET")).strip().upper() or "GET",
                        request_location=str(item.get("request_location", "query")).strip().lower() or "query",
                        body_format=str(item.get("body_format", "json")).strip().lower() or "json",
                        query_params_json=self._normalize_json_text(item.get("query_params_json", "{}")),
                        headers_json=self._normalize_json_text(item.get("headers_json", "{}")),
                        body_template=str(item.get("body_template", "")),
                        ip_param_name=str(item.get("ip_param_name", "ip")).strip() or "ip",
                        timeout=int(item.get("timeout", 3) or 3),
                        country_path=str(item.get("country_path", "country")).strip() or "country",
                        region_path=str(item.get("region_path", "region")).strip() or "region",
                        city_path=str(item.get("city_path", "city")).strip() or "city",
                        full_path=str(item.get("full_path", "")).strip(),
                        priority=int(item.get("priority", 0) or 0),
                        notes=str(item.get("notes", "")),
                    )
                )
        elif isinstance(primary_payload, dict) and primary_payload.get("url"):
            normalized_sources.append(
                OnlineGeoIPSource(
                    name="primary",
                    enabled=coerce_bool(primary_payload.get("enabled"), False),
                    weight=1,
                    url=str(primary_payload.get("url", "")).strip(),
                    method=str(primary_payload.get("method", "GET")).strip().upper() or "GET",
                    request_location="query" if str(primary_payload.get("method", "GET")).strip().upper() == "GET" else "body",
                    body_format="json",
                    query_params_json="{}",
                    headers_json=self._normalize_json_text(primary_payload.get("headers_json", "{}")),
                    body_template=str(primary_payload.get("body_template", "")),
                    ip_param_name=str(primary_payload.get("ip_param_name", "ip")).strip() or "ip",
                    timeout=int(primary_payload.get("timeout", 3) or 3),
                    country_path=str(primary_payload.get("country_path", "country")).strip() or "country",
                    region_path=str(primary_payload.get("region_path", "region")).strip() or "region",
                    city_path=str(primary_payload.get("city_path", "city")).strip() or "city",
                    full_path=str(primary_payload.get("full_path", "")).strip(),
                )
            )
        first_source = normalized_sources[0] if normalized_sources else None
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE geoip_settings
                SET enabled = ?, online_cache_ttl_seconds = ?, primary_enabled = ?, primary_url = ?, primary_method = ?,
                    primary_headers_json = ?, primary_body_template = ?, primary_ip_param_name = ?,
                    primary_timeout = ?, primary_country_path = ?, primary_region_path = ?,
                    primary_city_path = ?, primary_full_path = ?, offline_enabled = ?,
                    offline_db_path = ?, offline_locale = ?, offline_download_url = ?,
                    offline_download_headers_json = ?, offline_refresh_interval_hours = ?, updated_at = ?
                WHERE id = 1
                """,
                (
                    int(coerce_bool(payload.get("enabled"), True)),
                    max(0, int(payload.get("online_cache_ttl_seconds", 120) or 0)),
                    int(first_source.enabled) if first_source else 0,
                    first_source.url if first_source else "",
                    first_source.method if first_source else "GET",
                    first_source.headers_json if first_source else "{}",
                    first_source.body_template if first_source else "",
                    first_source.ip_param_name if first_source else "ip",
                    first_source.timeout if first_source else 3,
                    first_source.country_path if first_source else "country",
                    first_source.region_path if first_source else "region",
                    first_source.city_path if first_source else "city",
                    first_source.full_path if first_source else "",
                    int(coerce_bool(offline_payload.get("enabled"), False)),
                    str(offline_payload.get("db_path", "")).strip(),
                    str(offline_payload.get("locale", "zh-CN")).strip() or "zh-CN",
                    str(offline_payload.get("download_url", "")).strip(),
                    self._normalize_json_text(offline_payload.get("download_headers_json", "{}")),
                    max(1, int(offline_payload.get("refresh_interval_hours", 24) or 24)),
                    now,
                ),
            )
            connection.execute("DELETE FROM geoip_online_sources")
            for source in normalized_sources:
                self._insert_geoip_source(connection, source)
        return self.get_geoip_settings()

    def update_offline_geoip_sync_state(
        self,
        *,
        status: str,
        message: str,
        last_sync_at: Optional[str] = None,
        last_success_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        sync_time = last_sync_at or utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE geoip_settings
                SET offline_last_sync_at = ?,
                    offline_last_sync_status = ?,
                    offline_last_sync_message = ?,
                    offline_last_success_at = COALESCE(?, offline_last_success_at),
                    updated_at = ?
                WHERE id = 1
                """,
                (
                    sync_time,
                    str(status).strip(),
                    str(message).strip(),
                    last_success_at,
                    utc_now(),
                ),
            )
        return self.get_geoip_settings()

    async def sync_remote_rules(self, session: Optional[aiohttp.ClientSession] = None) -> Dict[str, Any]:
        remote = self.get_remote_config()
        if not remote.get("enabled"):
            return {"status": "skipped", "message": "Remote config sync is disabled.", "count": 0}
        if not remote.get("url"):
            raise ValueError("Remote config URL is required before sync.")

        own_session = False
        if session is None:
            timeout = aiohttp.ClientTimeout(total=int(remote.get("timeout", 5) or 5))
            session = aiohttp.ClientSession(timeout=timeout)
            own_session = True

        try:
            method = str(remote.get("method", "GET")).upper()
            headers = json.loads(remote.get("headers_json", "{}") or "{}")
            body_template = str(remote.get("body_template", "") or "")
            request_kwargs: Dict[str, Any] = {
                "headers": headers,
                "ssl": False,
                "timeout": aiohttp.ClientTimeout(total=int(remote.get("timeout", 5) or 5)),
            }

            if method != "GET" and body_template:
                rendered = body_template.replace("{{timestamp}}", utc_now())
                try:
                    request_kwargs["json"] = json.loads(rendered)
                except json.JSONDecodeError:
                    request_kwargs["data"] = rendered

            async with session.request(method, remote["url"], **request_kwargs) as response:
                response.raise_for_status()
                payload = await response.json(content_type=None)

            data_path = str(remote.get("data_path", "") or "").strip()
            remote_rules = deep_get(payload, data_path, payload)
            if not isinstance(remote_rules, list):
                raise ValueError("Remote config response did not resolve to a rule list.")

            parsed_rules: List[ProxyRule] = []
            for item in remote_rules:
                if not isinstance(item, dict):
                    continue
                parsed_rules.append(self._remote_item_to_rule(item, remote))

            if not parsed_rules:
                raise ValueError("Remote config sync completed but returned no valid rules.")

            with self._connect() as connection:
                connection.execute("DELETE FROM forward_rules WHERE source = 'remote'")
                for rule in parsed_rules:
                    self._insert_rule(connection, rule, source="remote")
                connection.execute(
                    """
                    UPDATE remote_config_sources
                    SET last_sync_at = ?, last_sync_status = ?, last_sync_message = ?
                    WHERE id = 1
                    """,
                    (utc_now(), "success", f"Synced {len(parsed_rules)} remote rules."),
                )

            return {"status": "success", "message": "Remote rules synced.", "count": len(parsed_rules)}
        except Exception as exc:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE remote_config_sources
                    SET last_sync_at = ?, last_sync_status = ?, last_sync_message = ?
                    WHERE id = 1
                    """,
                    (utc_now(), "error", str(exc)),
                )
            raise
        finally:
            if own_session:
                await session.close()

    def serialize_rule(self, rule: ProxyRule) -> Dict[str, Any]:
        return {
            "id": rule.rule_id,
            "source": rule.source,
            "external_id": rule.external_id,
            "name": rule.name,
            "request_host": normalize_request_host(rule.request_host),
            "path_prefix": rule.path_prefix,
            "target_url": rule.target_url,
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
            "notes": rule.notes,
        }

    def serialize_route_group(
        self,
        group: RouteGroupConfig,
        serialized_rules: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        serialized_rules = serialized_rules if serialized_rules is not None else self.list_rules()
        normalized_group_host = normalize_request_host(group.request_host)
        group_rules = [
            rule
            for rule in serialized_rules
            if rule["path_prefix"] == group.path_prefix
            and normalize_request_host(rule.get("request_host", "")) == normalized_group_host
        ]
        return {
            "request_host": normalized_group_host,
            "path_prefix": group.path_prefix,
            "region_matching_enabled": group.region_matching_enabled,
            "notes": group.notes,
            "rule_count": len(group_rules),
            "enabled_rule_count": sum(1 for rule in group_rules if rule["enabled"]),
            "default_rule_count": sum(1 for rule in group_rules if rule["is_default"]),
        }

    def serialize_route_log(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "request_method": row["request_method"],
            "request_path": row["request_path"],
            "request_query_string": row["request_query_string"],
            "path_prefix": row["path_prefix"],
            "rule_id": row["rule_id"],
            "rule_name": row["rule_name"],
            "rule_source": row["rule_source"],
            "target_url": row["target_url"],
            "original_client_ip": row["original_client_ip"],
            "client_ip": row["client_ip"],
            "region_matching_enabled": bool(row["region_matching_enabled"]),
            "geo_source": row["geo_source"],
            "geo_summary": row["geo_summary"],
            "geo_country": row["geo_country"],
            "geo_region": row["geo_region"],
            "geo_city": row["geo_city"],
            "configured_ip_whitelist": row["configured_ip_whitelist"],
            "matched_ip_whitelist": row["matched_ip_whitelist"],
            "configured_regions": row["configured_regions"],
            "matched_region": row["matched_region"],
            "match_strategy": row["match_strategy"],
            "match_detail": row["match_detail"],
            "upstream_status": row["upstream_status"],
            "cache_status": row["cache_status"],
            "redirect_count": row["redirect_count"],
            "transport_mode": row["transport_mode"],
            "operation_duration_ms": row["operation_duration_ms"],
            "result_status": row["result_status"],
            "error_message": row["error_message"],
            "created_at": row["created_at"],
        }

    def _row_to_rule(self, row: sqlite3.Row) -> ProxyRule:
        return ProxyRule(
            rule_id=row["id"],
            source=row["source"],
            external_id=row["external_id"],
            name=row["name"],
            request_host=normalize_request_host(row["request_host"]),
            path_prefix=row["path_prefix"],
            target_url=row["target_url"],
            strip_prefix=bool(row["strip_prefix"]),
            timeout=row["timeout"],
            max_redirects=row["max_redirects"],
            follow_redirects=bool(row["follow_redirects"]),
            retry_times=row["retry_times"],
            enable_streaming=bool(row["enable_streaming"]),
            ip_whitelist=row["ip_whitelist"],
            region_filters=row["region_filters"],
            is_default=bool(row["is_default"]),
            enabled=bool(row["enabled"]),
            priority=row["priority"],
            notes=row["notes"],
        )

    def _payload_to_rule(self, payload: Dict[str, Any]) -> ProxyRule:
        path_prefix = str(payload.get("path_prefix", "")).strip()
        target_url = str(payload.get("target_url", "")).strip()
        if not path_prefix:
            raise ValueError("path_prefix is required.")
        if not path_prefix.startswith("/"):
            raise ValueError("path_prefix must start with '/'.")
        if not target_url:
            raise ValueError("target_url is required.")

        return ProxyRule(
            rule_id=payload.get("id"),
            external_id=payload.get("external_id"),
            name=str(payload.get("name", "")).strip(),
            request_host=normalize_request_host(payload.get("request_host", "")),
            path_prefix=path_prefix,
            target_url=target_url,
            strip_prefix=coerce_bool(payload.get("strip_prefix"), False),
            timeout=int(payload.get("timeout", 30) or 30),
            max_redirects=int(payload.get("max_redirects", 10) or 10),
            follow_redirects=coerce_bool(payload.get("follow_redirects"), True),
            retry_times=int(payload.get("retry_times", 3) or 3),
            enable_streaming=coerce_bool(payload.get("enable_streaming"), True),
            ip_whitelist=normalize_region_filter_value(payload.get("ip_whitelist", "")),
            region_filters=normalize_region_filter_value(payload.get("region_filters", "")),
            is_default=coerce_bool(payload.get("is_default"), False),
            enabled=coerce_bool(payload.get("enabled"), True),
            priority=int(payload.get("priority", 0) or 0),
            notes=str(payload.get("notes", "")).strip(),
            source=str(payload.get("source", "manual")).strip() or "manual",
        )

    def _remote_item_to_rule(self, item: Dict[str, Any], remote: Dict[str, Any]) -> ProxyRule:
        path_prefix = str(deep_get(item, remote["path_prefix_field"], "") or "").strip()
        target_url = str(deep_get(item, remote["target_url_field"], "") or "").strip()
        if not path_prefix or not target_url:
            raise ValueError("Remote rule is missing path_prefix or target_url.")

        return ProxyRule(
            name=str(deep_get(item, remote.get("name_field", "name"), "") or "").strip(),
            request_host=normalize_request_host(
                deep_get(item, remote.get("request_host_field", "request_host"), "")
            ),
            path_prefix=path_prefix,
            target_url=target_url,
            strip_prefix=coerce_bool(deep_get(item, remote.get("strip_prefix_field", "strip_prefix"), False)),
            timeout=int(deep_get(item, remote.get("timeout_field", "timeout"), 30) or 30),
            max_redirects=int(
                deep_get(item, remote.get("max_redirects_field", "max_redirects"), 10) or 10
            ),
            follow_redirects=coerce_bool(deep_get(item, "follow_redirects", True), True),
            retry_times=int(deep_get(item, remote.get("retry_times_field", "retry_times"), 3) or 3),
            enable_streaming=coerce_bool(
                deep_get(item, remote.get("enable_streaming_field", "enable_streaming"), True)
            ),
            region_filters=normalize_region_filter_value(
                deep_get(item, remote.get("region_filters_field", "region_filters"), "")
            ),
            is_default=coerce_bool(deep_get(item, remote.get("is_default_field", "is_default"), False)),
            enabled=coerce_bool(deep_get(item, remote.get("enabled_field", "enabled"), True), True),
            priority=int(deep_get(item, remote.get("priority_field", "priority"), 0) or 0),
            external_id=str(
                deep_get(item, remote.get("external_id_field", "id"), "") or ""
            ).strip()
            or None,
            source="remote",
        )

    def _normalize_json_text(self, value: Any) -> str:
        if value in (None, ""):
            return "{}"
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        parsed = json.loads(str(value))
        return json.dumps(parsed, ensure_ascii=False)

    def _build_offline_geoip_status(
        self,
        *,
        db_path: str,
        refresh_interval_hours: int,
        last_sync_at: Optional[str],
        last_success_at: Optional[str],
        last_sync_status: Optional[str],
        last_sync_message: Optional[str],
    ) -> Dict[str, Any]:
        path = Path(str(db_path or "").strip()) if str(db_path or "").strip() else None
        exists = bool(path and path.exists())
        file_size = path.stat().st_size if exists and path is not None else 0
        updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(timespec="seconds") if exists and path is not None else ""
        backup_path = self._build_offline_backup_path(path) if path is not None else None
        backup_exists = bool(backup_path and backup_path.exists())
        backup_size = backup_path.stat().st_size if backup_exists and backup_path is not None else 0
        backup_updated_at = (
            datetime.fromtimestamp(backup_path.stat().st_mtime, tz=timezone.utc).isoformat(timespec="seconds")
            if backup_exists and backup_path is not None
            else ""
        )
        next_sync_at = ""

        baseline = last_sync_at or last_success_at or ""
        if baseline:
            try:
                baseline_dt = datetime.fromisoformat(str(baseline))
                next_sync_at = (
                    baseline_dt + timedelta(hours=max(1, int(refresh_interval_hours or 24)))
                ).isoformat(timespec="seconds")
            except ValueError:
                next_sync_at = ""

        return {
            "file_exists": exists,
            "file_size": file_size,
            "file_updated_at": updated_at,
            "backup_path": str(backup_path) if backup_path is not None else "",
            "backup_exists": backup_exists,
            "backup_size": backup_size,
            "backup_updated_at": backup_updated_at,
            "last_sync_at": last_sync_at or "",
            "last_success_at": last_success_at or "",
            "last_sync_status": last_sync_status or "",
            "last_sync_message": last_sync_message or "",
            "next_sync_at": next_sync_at,
        }

    def _build_offline_backup_path(self, path: Path) -> Path:
        suffix = path.suffix or ""
        if suffix:
            return path.with_suffix(f"{suffix}.bak")
        return path.with_name(f"{path.name}.bak")
