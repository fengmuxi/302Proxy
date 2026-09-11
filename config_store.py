from __future__ import annotations

import json
import hashlib
import logging
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from config import (
    DEFAULT_DB_PATH,
    AutoBanConfig,
    Config,
    EmailConfig,
    GeoIPSettings,
    IpResultCacheConfig,
    LoggingConfig,
    OnlineGeoIPSource,
    OfflineGeoIPSettings,
    PrimaryGeoIPSettings,
    ProxyRule,
    RateLimitConfig,
    CorsConfig,
    NotificationsConfig,
    RequestDedupConfig,
    RouteGroupConfig,
    RemoteConfigSettings,
    SignedRedirectConfig,
    SignedUrlConfig,
    SSLConfig,
    ServerConfig,
    StreamingConfig,
    GROUP_RULE_DEFAULT_FIELDS,
    RULE_INHERIT_SENTINEL_INT,
    RULE_EXPLICIT_OFF_SENTINEL,
    coerce_bool,
    normalize_request_host,
    normalize_region_filter_value,
)


logger = logging.getLogger("proxy")


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
    # 旧库列补齐清单：CREATE TABLE IF NOT EXISTS 对已存在的表不会新增列，
    # 这些列由后续 migration 演进而来（007/010/011/013/016），创建于对应
    # 功能上线之前的旧库会缺列，导致裸 UPDATE/SELECT 报 no such column。
    # 启动时按此清单幂等补齐（全部带 DEFAULT，可安全 ADD COLUMN）。
    LEGACY_SYSTEM_SETTINGS_COLUMNS: Tuple[Tuple[str, str], ...] = (
        ("ip_cache_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("ip_cache_ttl_seconds", "INTEGER NOT NULL DEFAULT 300"),
        ("ip_cache_max_entries", "INTEGER NOT NULL DEFAULT 5000"),
        ("logging_retention_days", "INTEGER NOT NULL DEFAULT 30"),
        ("auto_ban_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("auto_ban_window_seconds", "INTEGER NOT NULL DEFAULT 60"),
        ("auto_ban_max_requests", "INTEGER NOT NULL DEFAULT 100"),
        ("auto_ban_ban_duration_seconds", "INTEGER NOT NULL DEFAULT 3600"),
        ("auto_ban_max_404", "INTEGER NOT NULL DEFAULT 20"),
        ("auto_ban_auto_ban_on_404", "INTEGER NOT NULL DEFAULT 1"),
        ("auto_ban_whitelist", "TEXT NOT NULL DEFAULT ''"),
        ("auto_ban_email_on_ban", "INTEGER NOT NULL DEFAULT 0"),
        ("auto_ban_max_bytes", "INTEGER NOT NULL DEFAULT 0"),
        ("streaming_max_concurrent_per_ip", "INTEGER NOT NULL DEFAULT 0"),
        ("email_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("email_smtp_host", "TEXT NOT NULL DEFAULT ''"),
        ("email_smtp_port", "INTEGER NOT NULL DEFAULT 465"),
        ("email_smtp_ssl", "INTEGER NOT NULL DEFAULT 1"),
        ("email_sender", "TEXT NOT NULL DEFAULT ''"),
        ("email_sender_name", "TEXT NOT NULL DEFAULT ''"),
        ("email_password", "TEXT NOT NULL DEFAULT ''"),
        ("email_recipients", "TEXT NOT NULL DEFAULT ''"),
        ("email_block_link_base_url", "TEXT NOT NULL DEFAULT ''"),
        ("email_alert_window_seconds", "INTEGER NOT NULL DEFAULT 60"),
        ("email_alert_max_requests", "INTEGER NOT NULL DEFAULT 80"),
        ("email_alert_max_404", "INTEGER NOT NULL DEFAULT 15"),
        ("email_alert_cooldown_minutes", "INTEGER NOT NULL DEFAULT 30"),
        ("session_secret", "TEXT NOT NULL DEFAULT ''"),
        ("rsa_private_key", "TEXT NOT NULL DEFAULT ''"),
        ("dedup_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("dedup_window_seconds", "REAL NOT NULL DEFAULT 2.0"),
        ("dedup_max_cache_entries", "INTEGER NOT NULL DEFAULT 10000"),
        ("signed_url_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("signed_url_secret", "TEXT NOT NULL DEFAULT ''"),
        ("signed_url_ttl_seconds", "INTEGER NOT NULL DEFAULT 3600"),
        ("redirect_signing_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("redirect_signing_ttl_seconds", "INTEGER NOT NULL DEFAULT 21600"),
        ("redirect_signing_bind_ip", "INTEGER NOT NULL DEFAULT 1"),
        ("public_base_url", "TEXT NOT NULL DEFAULT ''"),
        ("login_max_attempts", "INTEGER NOT NULL DEFAULT 5"),
        ("login_lockout_minutes", "INTEGER NOT NULL DEFAULT 15"),
        ("login_cooldown_seconds", "INTEGER NOT NULL DEFAULT 0"),
        ("rate_limit_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("rate_limit_rps", "REAL NOT NULL DEFAULT 10.0"),
        ("rate_limit_burst", "INTEGER NOT NULL DEFAULT 20"),
        ("rate_limit_per_ip", "INTEGER NOT NULL DEFAULT 1"),
        ("cors_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("cors_allowed_origins", "TEXT NOT NULL DEFAULT ''"),
        ("cors_allowed_methods", "TEXT NOT NULL DEFAULT 'GET,HEAD,OPTIONS'"),
        ("cors_allow_credentials", "INTEGER NOT NULL DEFAULT 0"),
        ("cors_max_age", "INTEGER NOT NULL DEFAULT 600"),
        ("notifications_config", "TEXT NOT NULL DEFAULT ''"),
    )

    # route_logs 的演进列（017 引入）：旧库 base schema（CREATE TABLE IF NOT EXISTS）
    # 不含这些列，启动时必须按此清单幂等补齐，否则 SELECT/INSERT 报 no such column。
    LEGACY_ROUTE_LOGS_COLUMNS: Tuple[Tuple[str, str], ...] = (
        ("referer", "TEXT NOT NULL DEFAULT ''"),
        ("user_agent", "TEXT NOT NULL DEFAULT ''"),
        ("bytes_transferred", "INTEGER NOT NULL DEFAULT 0"),
        ("chain", "TEXT NOT NULL DEFAULT ''"),
    )

    # forward_rules 的演进列（018/019/022 引入）：Referer 防盗链 + UA 黑白名单
    LEGACY_FORWARD_RULES_COLUMNS: Tuple[Tuple[str, str], ...] = (
        ("referer_whitelist", "TEXT NOT NULL DEFAULT ''"),
        ("referer_policy", "TEXT NOT NULL DEFAULT 'allow'"),
        ("ua_blacklist", "TEXT NOT NULL DEFAULT ''"),
        ("ua_whitelist", "TEXT NOT NULL DEFAULT ''"),
        # P1-2.2 多上游 + 健康检查
        ("target_urls", "TEXT NOT NULL DEFAULT ''"),
        ("health_check_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("health_check_path", "TEXT NOT NULL DEFAULT ''"),
        ("health_check_interval", "INTEGER NOT NULL DEFAULT 30"),
        ("health_check_timeout", "INTEGER NOT NULL DEFAULT 5"),
        # P2-3.2 CORS 规则级覆盖
        ("cors_origins", "TEXT NOT NULL DEFAULT ''"),
        # P2-3.3 每规则自定义请求头 + 上游 TLS
        ("inject_request_headers", "TEXT NOT NULL DEFAULT ''"),
        ("upstream_verify_ssl", "INTEGER NOT NULL DEFAULT -1"),
        ("client_cert", "TEXT NOT NULL DEFAULT ''"),
        ("client_key", "TEXT NOT NULL DEFAULT ''"),
    )

    # api_keys 的演进列（031 引入）：P1-2.3 细粒度权限
    LEGACY_API_KEYS_COLUMNS: Tuple[Tuple[str, str], ...] = (
        ("scopes", "TEXT NOT NULL DEFAULT ''"),
        ("allowed_ips", "TEXT NOT NULL DEFAULT ''"),
        ("rate_limit", "INTEGER NOT NULL DEFAULT 0"),
    )

    def __init__(self, db_path: Optional[str] = None, bootstrap_config: Optional[Config] = None):
        if bootstrap_config and db_path is None:
            db_path = bootstrap_config.database_path

        self.db_path = Path(db_path or DEFAULT_DB_PATH)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.bootstrap_config = bootstrap_config or Config()
        self._lock = threading.Lock()

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
                    logging_retention_days INTEGER NOT NULL DEFAULT 30,
                    login_max_attempts INTEGER NOT NULL DEFAULT 5,
                    login_lockout_minutes INTEGER NOT NULL DEFAULT 15,
                    login_cooldown_seconds INTEGER NOT NULL DEFAULT 0,
                    rate_limit_enabled INTEGER NOT NULL DEFAULT 0,
                    rate_limit_rps REAL NOT NULL DEFAULT 10.0,
                    rate_limit_burst INTEGER NOT NULL DEFAULT 20,
                    rate_limit_per_ip INTEGER NOT NULL DEFAULT 1,
                    cors_enabled INTEGER NOT NULL DEFAULT 0,
                    cors_allowed_origins TEXT NOT NULL DEFAULT '',
                    cors_allowed_methods TEXT NOT NULL DEFAULT 'GET,HEAD,OPTIONS',
                    cors_allow_credentials INTEGER NOT NULL DEFAULT 0,
                    cors_max_age INTEGER NOT NULL DEFAULT 600,
                    notifications_config TEXT NOT NULL DEFAULT '',
                    streaming_enabled INTEGER NOT NULL,
                    streaming_chunk_size INTEGER NOT NULL,
                    streaming_large_file_threshold INTEGER NOT NULL,
                    streaming_stream_timeout INTEGER NOT NULL,
                    streaming_read_timeout INTEGER NOT NULL,
                    streaming_write_timeout INTEGER NOT NULL,
                    streaming_buffer_size INTEGER NOT NULL,
                    streaming_enable_range_support INTEGER NOT NULL,
                    streaming_max_request_body_size INTEGER NOT NULL,
                    ip_cache_enabled INTEGER NOT NULL,
                    ip_cache_ttl_seconds INTEGER NOT NULL,
                    ip_cache_max_entries INTEGER NOT NULL,
                    auto_ban_enabled INTEGER NOT NULL DEFAULT 0,
                    auto_ban_window_seconds INTEGER NOT NULL DEFAULT 60,
                    auto_ban_max_requests INTEGER NOT NULL DEFAULT 100,
                    auto_ban_ban_duration_seconds INTEGER NOT NULL DEFAULT 3600,
                    auto_ban_max_404 INTEGER NOT NULL DEFAULT 20,
                    auto_ban_auto_ban_on_404 INTEGER NOT NULL DEFAULT 1,
                    auto_ban_whitelist TEXT NOT NULL DEFAULT '',
                    auto_ban_email_on_ban INTEGER NOT NULL DEFAULT 0,
                    auto_ban_max_bytes INTEGER NOT NULL DEFAULT 0,
                    streaming_max_concurrent_per_ip INTEGER NOT NULL DEFAULT 0,
                    email_enabled INTEGER NOT NULL DEFAULT 0,
                    email_smtp_host TEXT NOT NULL DEFAULT '',
                    email_smtp_port INTEGER NOT NULL DEFAULT 465,
                    email_smtp_ssl INTEGER NOT NULL DEFAULT 1,
                    email_sender TEXT NOT NULL DEFAULT '',
                    email_sender_name TEXT NOT NULL DEFAULT '',
                    email_password TEXT NOT NULL DEFAULT '',
                    email_recipients TEXT NOT NULL DEFAULT '',
                    email_block_link_base_url TEXT NOT NULL DEFAULT '',
                    email_alert_window_seconds INTEGER NOT NULL DEFAULT 60,
                    email_alert_max_requests INTEGER NOT NULL DEFAULT 80,
                    email_alert_max_404 INTEGER NOT NULL DEFAULT 15,
                    email_alert_cooldown_minutes INTEGER NOT NULL DEFAULT 30,
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
                    access_ip_whitelist TEXT NOT NULL DEFAULT '',
                    ip_blacklist TEXT NOT NULL DEFAULT '',
                    region_whitelist TEXT NOT NULL DEFAULT '',
                    region_blacklist TEXT NOT NULL DEFAULT '',
                    rule_defaults TEXT NOT NULL DEFAULT '{}',
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
                    request_host TEXT NOT NULL DEFAULT '',
                    path_prefix TEXT NOT NULL DEFAULT '',
                    rule_id INTEGER,
                    rule_name TEXT NOT NULL DEFAULT '',
                    rule_request_host TEXT NOT NULL DEFAULT '',
                    rule_source TEXT NOT NULL DEFAULT '',
                    target_url TEXT NOT NULL DEFAULT '',
                    redirect_location TEXT NOT NULL DEFAULT '',
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
                    chain TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS banned_ips (
                    ip TEXT PRIMARY KEY,
                    reason TEXT NOT NULL DEFAULT '',
                    banned_by TEXT NOT NULL DEFAULT 'admin',
                    banned_at REAL NOT NULL DEFAULT 0,
                    expire_at REAL NOT NULL DEFAULT 0,
                    permanent INTEGER NOT NULL DEFAULT 1,
                    path_prefix TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL DEFAULT '',
                    key_prefix TEXT NOT NULL DEFAULT '',
                    key_hash TEXT NOT NULL UNIQUE,
                    readonly INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    use_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_used_at TEXT NOT NULL DEFAULT '',
                    expires_at INTEGER,
                    scopes TEXT NOT NULL DEFAULT '',
                    allowed_ips TEXT NOT NULL DEFAULT '',
                    rate_limit INTEGER NOT NULL DEFAULT 0
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
                    path_rewrite_pattern TEXT NOT NULL DEFAULT '',
                    path_rewrite_replacement TEXT NOT NULL DEFAULT '',
                    access_ip_whitelist TEXT NOT NULL DEFAULT '',
                    ip_blacklist TEXT NOT NULL DEFAULT '',
                    region_whitelist TEXT NOT NULL DEFAULT '',
                    region_blacklist TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    target_urls TEXT NOT NULL DEFAULT '',
                    health_check_enabled INTEGER NOT NULL DEFAULT 0,
                    health_check_path TEXT NOT NULL DEFAULT '',
                    health_check_interval INTEGER NOT NULL DEFAULT 30,
                    health_check_timeout INTEGER NOT NULL DEFAULT 5,
                    cors_origins TEXT NOT NULL DEFAULT '',
                    inject_request_headers TEXT NOT NULL DEFAULT '',
                    upstream_verify_ssl INTEGER NOT NULL DEFAULT -1,
                    client_cert TEXT NOT NULL DEFAULT '',
                    client_key TEXT NOT NULL DEFAULT ''
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

                CREATE TABLE IF NOT EXISTS email_block_tokens (
                    token TEXT PRIMARY KEY,
                    ip TEXT NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    used INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS admin_login_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ip TEXT NOT NULL DEFAULT '',
                    username TEXT NOT NULL DEFAULT '',
                    fail_count INTEGER NOT NULL DEFAULT 0,
                    first_fail_at INTEGER NOT NULL DEFAULT 0,
                    locked_until INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_login_attempts_ip_user
                    ON admin_login_attempts(ip, username);

                CREATE TABLE IF NOT EXISTS admin_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor_type TEXT NOT NULL DEFAULT '',
                    actor_id TEXT NOT NULL DEFAULT '',
                    action TEXT NOT NULL DEFAULT '',
                    target_type TEXT NOT NULL DEFAULT '',
                    target_id TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_audit_created ON admin_audit_log(created_at);
                CREATE INDEX IF NOT EXISTS idx_audit_target
                    ON admin_audit_log(target_type, target_id);

                CREATE TABLE IF NOT EXISTS system_settings_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    module TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    changed_by TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_settings_history_module
                    ON system_settings_history(module, id);
                """
            )
            # 基表 CREATE IF NOT EXISTS 对已存在的旧表不会加列；这里先补一次
            # route_groups 演进列（_run_migrations 的 fresh-DB 分支会提前 return 跳过补列循环）
            self._ensure_column(connection, "route_groups", "rule_defaults", "TEXT NOT NULL DEFAULT '{}'")
        self._run_migrations()
        # 旧库补齐后，确保签名 URL 密钥非空（幂等：仅当为空时生成）
        with self._connect() as connection:
            self._ensure_signed_url_secret(connection)

    def _ensure_signed_url_secret(self, connection: sqlite3.Connection) -> None:
        """确保 system_settings.signed_url_secret 非空（旧库启动时幂等生成）。

        新库由 _bootstrap_if_needed 在 INSERT 时直接生成；旧库经
        LEGACY_SYSTEM_SETTINGS_COLUMNS 补齐后该列为空串，此处回填。
        """
        import secrets as secrets_module

        row = connection.execute(
            "SELECT signed_url_secret FROM system_settings WHERE id = 1"
        ).fetchone()
        if row is None or row["signed_url_secret"]:
            return
        connection.execute(
            "UPDATE system_settings SET signed_url_secret = ? WHERE id = 1",
            (secrets_module.token_hex(32),),
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

    def _run_migrations(self) -> None:
        """Execute yoyo database migrations for fresh databases only.

        Existing databases skip yoyo migrations (their base schema comes from
        CREATE TABLE IF NOT EXISTS), but that statement cannot add columns to
        existing tables — so backfill any columns introduced by later
        migrations via LEGACY_SYSTEM_SETTINGS_COLUMNS instead.
        """
        with self._connect() as connection:
            try:
                count = connection.execute("SELECT COUNT(*) FROM system_settings").fetchone()[0]
            except Exception:
                count = 0

        if count == 0:
            from yoyo import read_migrations, get_backend

            migrations_dir = Path(__file__).parent / "migrations"
            if not migrations_dir.exists():
                return

            backend = get_backend(f"sqlite:///{self.db_path}")
            migrations = read_migrations(str(migrations_dir))
            # yoyo 在记录迁移日志时会调用 socket.getfqdn() 做反向 DNS，在无 DNS 的
            # 主机/沙箱环境会阻塞数十秒甚至卡死启动。该值仅用于日志 hostname 字段，
            # 此处临时替换为 gethostname()（读本机名、不触发 DNS），迁移完成后还原。
            import socket as _socket
            _original_getfqdn = _socket.getfqdn
            _socket.getfqdn = lambda name="": _socket.gethostname()
            try:
                backend.apply_migrations(migrations)
            finally:
                _socket.getfqdn = _original_getfqdn
            return

        # 旧库：幂等补齐后续迁移新增的列，避免 UPDATE/SELECT 报 no such column
        with self._connect() as connection:
            for column_name, definition in self.LEGACY_SYSTEM_SETTINGS_COLUMNS:
                self._ensure_column(connection, "system_settings", column_name, definition)
            for column_name, definition in self.LEGACY_ROUTE_LOGS_COLUMNS:
                self._ensure_column(connection, "route_logs", column_name, definition)
            for column_name, definition in self.LEGACY_FORWARD_RULES_COLUMNS:
                self._ensure_column(connection, "forward_rules", column_name, definition)
            for column_name, definition in self.LEGACY_API_KEYS_COLUMNS:
                self._ensure_column(connection, "api_keys", column_name, definition)
            # route_groups 演进列（034 引入）：组级规则默认配置（规则继承 P2-4.1）
            self._ensure_column(connection, "route_groups", "rule_defaults", "TEXT NOT NULL DEFAULT '{}'")

    def _ensure_security_keys(self, connection: sqlite3.Connection) -> None:
        """确保旧数据库也有 session_secret 和 rsa_private_key"""
        import secrets as secrets_module
        row = connection.execute(
            "SELECT session_secret, rsa_private_key FROM system_settings WHERE id = 1"
        ).fetchone()
        if not row:
            return
        
        updates = {}
        if not row["session_secret"]:
            updates["session_secret"] = secrets_module.token_hex(32)
        
        if not row["rsa_private_key"]:
            try:
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.primitives.asymmetric import rsa
                
                private_key = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=2048,
                )
                private_pem = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ).decode('utf-8')
                updates["rsa_private_key"] = private_pem
            except ImportError:
                # cryptography 未安装，跳过 RSA 密钥生成
                pass
        
        if updates:
            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            vals = list(updates.values())
            connection.execute(
                f"UPDATE system_settings SET {set_clause} WHERE id = 1",
                vals,
            )

    def _migrate_route_groups_table(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute("PRAGMA table_info(route_groups)").fetchall()
        existing_columns = {row["name"] for row in rows}
        if not existing_columns or "request_host" in existing_columns:
            # 清理可能残留的 route_groups_v2 表（上次迁移中断导致）
            connection.execute("DROP TABLE IF EXISTS route_groups_v2")
            return

        # 清理可能残留的 route_groups_v2 表（上次迁移中断导致）
        connection.execute("DROP TABLE IF EXISTS route_groups_v2")

        connection.executescript(
            """
            CREATE TABLE route_groups_v2 (
                request_host TEXT NOT NULL DEFAULT '',
                path_prefix TEXT NOT NULL,
                region_matching_enabled INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                access_ip_whitelist TEXT NOT NULL DEFAULT '',
                ip_blacklist TEXT NOT NULL DEFAULT '',
                region_whitelist TEXT NOT NULL DEFAULT '',
                region_blacklist TEXT NOT NULL DEFAULT '',
                rule_defaults TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (request_host, path_prefix)
            );

            INSERT INTO route_groups_v2 (request_host, path_prefix, region_matching_enabled, notes,
                access_ip_whitelist, ip_blacklist, region_whitelist, region_blacklist, updated_at)
            SELECT '', path_prefix, region_matching_enabled, notes,
                COALESCE(access_ip_whitelist, ''), COALESCE(ip_blacklist, ''),
                COALESCE(region_whitelist, ''), COALESCE(region_blacklist, ''), updated_at
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

            import secrets as secrets_module
            session_secret = secrets_module.token_hex(32)
            signed_url_secret = secrets_module.token_hex(32)
            
            # 生成 RSA 密钥对
            rsa_private_key = ""
            try:
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.primitives.asymmetric import rsa
                
                private_key = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=2048,
                )
                rsa_private_key = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ).decode('utf-8')
            except ImportError:
                pass

            now = utc_now()
            connection.execute(
                """
                INSERT INTO system_settings (
                    id, host, port, workers, keepalive_timeout, max_connections,
                    max_connections_per_host, ssl_enabled, cert_file, key_file,
                    logging_level, logging_format, logging_file_path, logging_retention_days,
                    streaming_enabled, streaming_chunk_size,
                    streaming_large_file_threshold, streaming_stream_timeout,
                    streaming_read_timeout, streaming_write_timeout, streaming_buffer_size,
                    streaming_enable_range_support, streaming_max_request_body_size,
                    ip_cache_enabled, ip_cache_ttl_seconds, ip_cache_max_entries,
                    auto_ban_enabled, auto_ban_window_seconds, auto_ban_max_requests,
                    auto_ban_ban_duration_seconds, auto_ban_max_404, auto_ban_auto_ban_on_404,
                    auto_ban_whitelist, auto_ban_email_on_ban,
                    email_enabled, email_smtp_host, email_smtp_port, email_smtp_ssl,
                    email_sender, email_sender_name, email_password, email_recipients,
                    email_block_link_base_url, email_alert_window_seconds, email_alert_max_requests,
                    email_alert_max_404, email_alert_cooldown_minutes,
                    default_timeout, max_redirects, follow_redirects, trust_forward_headers,
                    database_path, updated_at, session_secret, rsa_private_key, signed_url_secret
                ) VALUES (
                    1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?
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
                    config.logging.retention_days,
                    int(config.streaming.enabled),
                    config.streaming.chunk_size,
                    config.streaming.large_file_threshold,
                    config.streaming.stream_timeout,
                    config.streaming.read_timeout,
                    config.streaming.write_timeout,
                    config.streaming.buffer_size,
                    int(config.streaming.enable_range_support),
                    config.streaming.max_request_body_size,
                    int(config.ip_result_cache.enabled),
                    config.ip_result_cache.ttl_seconds,
                    config.ip_result_cache.max_entries,
                    int(config.auto_ban.enabled),
                    config.auto_ban.window_seconds,
                    config.auto_ban.max_requests,
                    config.auto_ban.ban_duration_seconds,
                    config.auto_ban.max_404,
                    int(config.auto_ban.auto_ban_on_404),
                    config.auto_ban.whitelist,
                    int(config.auto_ban.email_on_ban),
                    int(config.email.enabled),
                    config.email.smtp_host,
                    config.email.smtp_port,
                    int(config.email.smtp_ssl),
                    config.email.sender,
                    config.email.sender_name,
                    config.email.password,
                    config.email.recipients,
                    config.email.block_link_base_url,
                    config.email.alert_window_seconds,
                    config.email.alert_max_requests,
                    config.email.alert_max_404,
                    config.email.alert_cooldown_minutes,
                    config.default_timeout,
                    config.max_redirects,
                    int(config.follow_redirects),
                    int(config.trust_forward_headers),
                    str(self.db_path),
                    now,
                    session_secret,
                    rsa_private_key,
                    signed_url_secret,
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
                is_default, enabled, priority, notes, path_rewrite_pattern, path_rewrite_replacement,
                access_ip_whitelist, ip_blacklist, region_whitelist, region_blacklist,
                referer_whitelist, referer_policy, ua_blacklist, ua_whitelist,
                target_urls, health_check_enabled, health_check_path, health_check_interval, health_check_timeout,
                cors_origins,
                inject_request_headers, upstream_verify_ssl, client_cert, client_key,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                rule.path_rewrite_pattern or "",
                rule.path_rewrite_replacement or "",
                normalize_region_filter_value(rule.access_ip_whitelist),
                normalize_region_filter_value(rule.ip_blacklist),
                normalize_region_filter_value(rule.region_whitelist),
                normalize_region_filter_value(rule.region_blacklist),
                normalize_region_filter_value(rule.referer_whitelist),
                rule.stored_referer_policy(),
                normalize_region_filter_value(rule.ua_blacklist),
                normalize_region_filter_value(rule.ua_whitelist),
                rule.target_urls or "",
                int(rule.health_check_enabled),
                rule.health_check_path or "",
                rule.health_check_interval,
                rule.health_check_timeout,
                rule.cors_origins or "",
                rule.inject_request_headers or "",
                int(rule.upstream_verify_ssl),
                rule.client_cert or "",
                rule.client_key or "",
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)

    def _clear_existing_default_in_group(
        self,
        connection: sqlite3.Connection,
        path_prefix: str,
        request_host: str,
        exclude_rule_id: Optional[int] = None,
    ) -> None:
        """同一分组下只能存在一个默认规则。将其他规则的 is_default 置为 0。

        forward_rules 表里的 request_host 字段写入时已经被 normalize 处理过，
        所以 SQL 中可以直接用 `request_host = ?` 进行比较。
        """
        normalized_host = normalize_request_host(request_host)
        if exclude_rule_id is None:
            connection.execute(
                """
                UPDATE forward_rules
                SET is_default = 0, updated_at = ?
                WHERE path_prefix = ? AND request_host = ? AND is_default = 1
                """,
                (utc_now(), path_prefix, normalized_host),
            )
        else:
            connection.execute(
                """
                UPDATE forward_rules
                SET is_default = 0, updated_at = ?
                WHERE path_prefix = ? AND request_host = ?
                  AND id != ? AND is_default = 1
                """,
                (utc_now(), path_prefix, normalized_host, exclude_rule_id),
            )

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

    # ===== 组级规则默认（P2-4.1 规则继承）=====

    def _parse_group_rule_defaults(self, raw: Any) -> Dict[str, Any]:
        """route_groups.rule_defaults JSON 串 → 清洗后的 dict。

        非法 JSON / 非对象 / 未知键 / 类型不合法一律剔除（宁缺勿错：
        剔除后回落全局默认，比把脏值灌进转发链路安全）。
        """
        if raw in (None, ""):
            return {}
        if isinstance(raw, dict):
            parsed = raw
        else:
            try:
                parsed = json.loads(str(raw))
            except (json.JSONDecodeError, TypeError):
                logger.warning("route_groups.rule_defaults 解析失败，忽略组级默认: %r", raw)
                return {}
        if not isinstance(parsed, dict):
            return {}
        return self._sanitize_group_rule_defaults(parsed)

    def _sanitize_group_rule_defaults(self, value: Any) -> Dict[str, Any]:
        """校验/归一化组级默认 dict：只保留 GROUP_RULE_DEFAULT_FIELDS 里的合法键值。"""
        if not isinstance(value, dict):
            return {}
        out: Dict[str, Any] = {}
        for key, raw in value.items():
            if key not in GROUP_RULE_DEFAULT_FIELDS or raw is None:
                continue
            kind = GROUP_RULE_DEFAULT_FIELDS[key]
            try:
                if kind == "int":
                    num = int(raw)
                    if num <= 0:
                        continue
                    out[key] = num
                elif kind == "bool":
                    parsed_bool = coerce_bool(raw, None)
                    if parsed_bool is not None:
                        out[key] = parsed_bool
                else:
                    text = normalize_region_filter_value(raw)
                    if not text:
                        continue
                    if key == "referer_policy":
                        text = text.lower()
                        if text not in ("allow", "deny"):
                            continue
                    out[key] = text
            except (TypeError, ValueError):
                continue
        return out

    def _group_defaults_map(self, connection: sqlite3.Connection) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """从 DB 直读 route_groups.rule_defaults → (host, prefix) → 默认 dict。"""
        rows = connection.execute("SELECT request_host, path_prefix, rule_defaults FROM route_groups").fetchall()
        out: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for row in rows:
            raw = row["rule_defaults"] if "rule_defaults" in row.keys() else "{}"
            out[(normalize_request_host(row["request_host"]), row["path_prefix"])] = self._parse_group_rule_defaults(raw)
        return out

    def _apply_group_defaults_to_rules(
        self,
        rules: List[ProxyRule],
        group_defaults_map: Dict[Tuple[str, str], Dict[str, Any]],
    ) -> None:
        """把「继承组默认」的规则字段原地替换为组级默认值（P2-4.1）。

        解析后 proxy_core / 健康探针拿到的是有效值，无需感知哨兵；
        inherit_fields 保留在规则对象上，供 serialize_rule 输出给后台 UI。
        """
        for rule in rules:
            inherit = rule.inherit_set()
            if not inherit:
                continue
            defaults = group_defaults_map.get((normalize_request_host(rule.request_host), rule.path_prefix))
            if not defaults:
                continue
            for field in inherit:
                if field not in GROUP_RULE_DEFAULT_FIELDS or field not in defaults:
                    continue
                kind = GROUP_RULE_DEFAULT_FIELDS[field]
                value = defaults[field]
                try:
                    if kind == "int":
                        setattr(rule, field, int(value))
                    elif kind == "bool":
                        setattr(rule, field, bool(value))
                    else:
                        setattr(rule, field, str(value))
                except (TypeError, ValueError):
                    continue

    def _serialize_rule_resolved(self, row: sqlite3.Row) -> Dict[str, Any]:
        """单条规则的序列化（组默认已合入）：get/create/update 返回值用。"""
        rule = self._row_to_rule(row)
        with self._connect() as connection:
            gmap = self._group_defaults_map(connection)
        self._apply_group_defaults_to_rules([rule], gmap)
        return self.serialize_rule(rule)

    def _upsert_route_group(
        self,
        connection: sqlite3.Connection,
        path_prefix: str,
        request_host: str = "",
        region_matching_enabled: Optional[bool] = None,
        notes: Optional[str] = None,
        access_ip_whitelist: Optional[str] = None,
        ip_blacklist: Optional[str] = None,
        region_whitelist: Optional[str] = None,
        region_blacklist: Optional[str] = None,
        rule_defaults: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized_host = normalize_request_host(request_host)
        normalized_enabled = None if region_matching_enabled is None else coerce_bool(region_matching_enabled, False)
        existing = connection.execute(
            "SELECT * FROM route_groups WHERE request_host = ? AND path_prefix = ?",
            (normalized_host, path_prefix),
        ).fetchone()
        now = utc_now()
        # rule_defaults=None 表示本次调用不涉及组默认（如规则保存时顺带 upsert 组），保留现值
        if rule_defaults is None and existing and "rule_defaults" in existing.keys():
            existing_defaults_raw = existing["rule_defaults"]
        else:
            existing_defaults_raw = None
        defaults_json = (
            self._normalize_json_text(self._sanitize_group_rule_defaults(rule_defaults))
            if rule_defaults is not None
            else (existing_defaults_raw if existing_defaults_raw else "{}")
        )
        if existing:
            connection.execute(
                """
                UPDATE route_groups
                SET region_matching_enabled = ?, notes = ?,
                    access_ip_whitelist = ?, ip_blacklist = ?, region_whitelist = ?, region_blacklist = ?,
                    rule_defaults = ?,
                    updated_at = ?
                WHERE request_host = ? AND path_prefix = ?
                """,
                (
                    int(normalized_enabled if normalized_enabled is not None else bool(existing["region_matching_enabled"])),
                    notes if notes is not None else existing["notes"],
                    normalize_region_filter_value(access_ip_whitelist) if access_ip_whitelist is not None else (existing["access_ip_whitelist"] if "access_ip_whitelist" in existing.keys() else ""),
                    normalize_region_filter_value(ip_blacklist) if ip_blacklist is not None else (existing["ip_blacklist"] if "ip_blacklist" in existing.keys() else ""),
                    normalize_region_filter_value(region_whitelist) if region_whitelist is not None else (existing["region_whitelist"] if "region_whitelist" in existing.keys() else ""),
                    normalize_region_filter_value(region_blacklist) if region_blacklist is not None else (existing["region_blacklist"] if "region_blacklist" in existing.keys() else ""),
                    defaults_json,
                    now,
                    normalized_host,
                    path_prefix,
                ),
            )
            return

        connection.execute(
            """
            INSERT INTO route_groups (request_host, path_prefix, region_matching_enabled, notes,
                access_ip_whitelist, ip_blacklist, region_whitelist, region_blacklist, rule_defaults, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_host,
                path_prefix,
                int(normalized_enabled) if normalized_enabled is not None else 0,
                notes or "",
                normalize_region_filter_value(access_ip_whitelist or ""),
                normalize_region_filter_value(ip_blacklist or ""),
                normalize_region_filter_value(region_whitelist or ""),
                normalize_region_filter_value(region_blacklist or ""),
                defaults_json,
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
                retention_days=system_row["logging_retention_days"] if "logging_retention_days" in system_row.keys() else 30,
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
                max_concurrent_per_ip=int(system_row["streaming_max_concurrent_per_ip"]) if "streaming_max_concurrent_per_ip" in system_row.keys() else 0,
            )
            config.ip_result_cache = IpResultCacheConfig(
                enabled=bool(system_row["ip_cache_enabled"]),
                ttl_seconds=system_row["ip_cache_ttl_seconds"],
                max_entries=system_row["ip_cache_max_entries"],
            )
            config.request_dedup = RequestDedupConfig(
                enabled=bool(system_row["dedup_enabled"]) if "dedup_enabled" in system_row.keys() else False,
                window_seconds=float(system_row["dedup_window_seconds"]) if "dedup_window_seconds" in system_row.keys() else 2.0,
                max_cache_entries=int(system_row["dedup_max_cache_entries"]) if "dedup_max_cache_entries" in system_row.keys() else 10000,
            )
            config.auto_ban = AutoBanConfig(
                enabled=bool(system_row["auto_ban_enabled"]),
                window_seconds=system_row["auto_ban_window_seconds"],
                max_requests=system_row["auto_ban_max_requests"],
                ban_duration_seconds=system_row["auto_ban_ban_duration_seconds"],
                max_404=system_row["auto_ban_max_404"],
                auto_ban_on_404=bool(system_row["auto_ban_auto_ban_on_404"]),
                whitelist=system_row["auto_ban_whitelist"],
                email_on_ban=bool(system_row["auto_ban_email_on_ban"]) if "auto_ban_email_on_ban" in system_row.keys() else False,
                max_bytes=int(system_row["auto_ban_max_bytes"]) if "auto_ban_max_bytes" in system_row.keys() else 0,
            )
            config.rate_limit = RateLimitConfig(
                enabled=bool(system_row["rate_limit_enabled"]) if "rate_limit_enabled" in system_row.keys() else False,
                requests_per_second=float(system_row["rate_limit_rps"]) if "rate_limit_rps" in system_row.keys() else 10.0,
                burst=int(system_row["rate_limit_burst"]) if "rate_limit_burst" in system_row.keys() else 20,
                per_ip=coerce_bool(system_row["rate_limit_per_ip"]) if "rate_limit_per_ip" in system_row.keys() else True,
            )
            keys = system_row.keys()
            config.cors = CorsConfig(
                enabled=bool(system_row["cors_enabled"]) if "cors_enabled" in keys else False,
                allowed_origins=system_row["cors_allowed_origins"] if "cors_allowed_origins" in keys else "",
                allowed_methods=system_row["cors_allowed_methods"] if "cors_allowed_methods" in keys else "GET,HEAD,OPTIONS",
                allow_credentials=bool(system_row["cors_allow_credentials"]) if "cors_allow_credentials" in keys else False,
                max_age=int(system_row["cors_max_age"]) if "cors_max_age" in keys else 600,
            )
            config.email = EmailConfig(
                enabled=bool(system_row["email_enabled"]),
                smtp_host=system_row["email_smtp_host"],
                smtp_port=system_row["email_smtp_port"],
                smtp_ssl=bool(system_row["email_smtp_ssl"]),
                sender=system_row["email_sender"],
                sender_name=system_row["email_sender_name"] if "email_sender_name" in system_row.keys() else "",
                password=system_row["email_password"],
                recipients=system_row["email_recipients"],
                block_link_base_url=system_row["email_block_link_base_url"] if "email_block_link_base_url" in system_row.keys() else "",
                alert_window_seconds=system_row["email_alert_window_seconds"],
                alert_max_requests=system_row["email_alert_max_requests"],
                alert_max_404=system_row["email_alert_max_404"],
                alert_cooldown_minutes=system_row["email_alert_cooldown_minutes"],
            )
            config.default_timeout = system_row["default_timeout"]
            config.max_redirects = system_row["max_redirects"]
            config.follow_redirects = bool(system_row["follow_redirects"])
            config.trust_forward_headers = bool(system_row["trust_forward_headers"])
            config.database_path = system_row["database_path"]
            config.signed_url = SignedUrlConfig(
                enabled=bool(system_row["signed_url_enabled"]) if "signed_url_enabled" in system_row.keys() else False,
                secret=system_row["signed_url_secret"] if "signed_url_secret" in system_row.keys() else "",
                ttl_seconds=int(system_row["signed_url_ttl_seconds"]) if "signed_url_ttl_seconds" in system_row.keys() else 3600,
            )
            config.signed_redirect = SignedRedirectConfig(
                enabled=bool(system_row["redirect_signing_enabled"]) if "redirect_signing_enabled" in system_row.keys() else False,
                ttl_seconds=int(system_row["redirect_signing_ttl_seconds"]) if "redirect_signing_ttl_seconds" in system_row.keys() else 21600,
                bind_ip=bool(system_row["redirect_signing_bind_ip"]) if "redirect_signing_bind_ip" in system_row.keys() else True,
                base_url=system_row["public_base_url"] if "public_base_url" in system_row.keys() else "",
            )
            config.notifications = self._parse_notifications_config(
                system_row["notifications_config"] if "notifications_config" in system_row.keys() else ""
            )

        if feature_row:
            config.region_matching_enabled = bool(feature_row["region_matching_enabled"])

        config.route_groups = [
            RouteGroupConfig(
                path_prefix=row["path_prefix"],
                request_host=normalize_request_host(row["request_host"]),
                region_matching_enabled=bool(row["region_matching_enabled"]),
                notes=row["notes"],
                access_ip_whitelist=row["access_ip_whitelist"] if "access_ip_whitelist" in row.keys() else "",
                ip_blacklist=row["ip_blacklist"] if "ip_blacklist" in row.keys() else "",
                region_whitelist=row["region_whitelist"] if "region_whitelist" in row.keys() else "",
                region_blacklist=row["region_blacklist"] if "region_blacklist" in row.keys() else "",
                rule_defaults=self._parse_group_rule_defaults(
                    row["rule_defaults"] if "rule_defaults" in row.keys() else "{}"
                ),
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
                    query_params_json=row["query_params_json"] if "query_params_json" in row.keys() else "{}",
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
                online_cache_ttl_seconds=max(0, int(geo_row["online_cache_ttl_seconds"] or 0)) if "online_cache_ttl_seconds" in geo_row.keys() else 120,
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
                    download_url=geo_row["offline_download_url"] if "offline_download_url" in geo_row.keys() else "",
                    download_headers_json=geo_row["offline_download_headers_json"] if "offline_download_headers_json" in geo_row.keys() else "{}",
                    refresh_interval_hours=geo_row["offline_refresh_interval_hours"] if "offline_refresh_interval_hours" in geo_row.keys() else 24,
                    last_sync_at=geo_row["offline_last_sync_at"] if "offline_last_sync_at" in geo_row.keys() else "",
                    last_sync_status=geo_row["offline_last_sync_status"] if "offline_last_sync_status" in geo_row.keys() else "",
                    last_sync_message=geo_row["offline_last_sync_message"] if "offline_last_sync_message" in geo_row.keys() else "",
                    last_success_at=geo_row["offline_last_success_at"] if "offline_last_success_at" in geo_row.keys() else "",
                ),
            )

        config.proxy_rules = [self._row_to_rule(row) for row in rules]
        # 组级默认合并（P2-4.1）：「继承组」字段回落组 rule_defaults（未设置的键回落内建/全局默认）
        self._apply_group_defaults_to_rules(
            config.proxy_rules,
            {
                (normalize_request_host(g.request_host), g.path_prefix): (g.rule_defaults or {})
                for g in config.route_groups
            },
        )
        config.admin_auth = self.bootstrap_config.admin_auth
        # yaml-only 字段：数据库不存储，从引导配置透传（upstream_ipv4_only 等）
        config.upstream_ipv4_only = bool(getattr(self.bootstrap_config, "upstream_ipv4_only", False))
        # 从数据库读取安全密钥
        if system_row:
            config.admin_auth.session_secret = system_row["session_secret"] if "session_secret" in system_row.keys() else ""
            config.admin_auth.rsa_private_key = system_row["rsa_private_key"] if "rsa_private_key" in system_row.keys() else ""
        return config

    # 启动信息（server/ssl/logging）字段 → system_settings 列名映射
    _STARTUP_FIELD_COLUMNS: Dict[str, Dict[str, str]] = {
        "server": {
            "host": "host",
            "port": "port",
            "workers": "workers",
            "keepalive_timeout": "keepalive_timeout",
            "max_connections": "max_connections",
            "max_connections_per_host": "max_connections_per_host",
        },
        "ssl": {
            "enabled": "ssl_enabled",
            "cert_file": "cert_file",
            "key_file": "key_file",
        },
        "logging": {
            "level": "logging_level",
            "format": "logging_format",
            "file_path": "logging_file_path",
            "retention_days": "logging_retention_days",
        },
    }

    def apply_file_startup_overrides(self, file_config: Config, runtime_config: Config) -> List[str]:
        """启动信息以配置文件为最高优先级：yaml 显式配置的 server/ssl/logging 键覆盖数据库值。

        - 仅覆盖配置文件里**显式出现**的键（空段/缺段不产生覆盖，避免默认值
          意外回退后台已调整的配置）；
        - 覆盖结果回写 system_settings，保证后台系统设置页与实际生效值一致；
        - 优先级：CLI -p/--host > config.yaml > 数据库 system_settings。
        返回人类可读的覆盖说明列表（供启动日志输出），无覆盖时返回空列表。
        """
        explicit = getattr(file_config, "yaml_explicit_keys", None) or {}
        updates: Dict[str, Any] = {}
        notes: List[str] = []

        for section, field_columns in self._STARTUP_FIELD_COLUMNS.items():
            section_keys = explicit.get(section)
            if not section_keys:
                continue
            file_section = getattr(file_config, section)
            runtime_section = getattr(runtime_config, section)
            for field_name, column in field_columns.items():
                if field_name not in section_keys:
                    continue
                value = getattr(file_section, field_name)
                setattr(runtime_section, field_name, value)
                updates[column] = int(bool(value)) if isinstance(value, bool) else value
                notes.append(f"{section}.{field_name}={value}")

        if not updates:
            return notes

        set_clause = ", ".join(f"{column} = ?" for column in updates)
        try:
            with self._connect() as connection:
                connection.execute(
                    f"UPDATE system_settings SET {set_clause}, updated_at = ? WHERE id = 1",
                    (*updates.values(), utc_now()),
                )
        except Exception:
            # 回写失败不影响启动（内存值已覆盖），仅在日志层可见
            logging.getLogger("proxy").warning("启动配置回写 system_settings 失败（不影响本次启动生效值）", exc_info=True)
        return notes

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
                    "ip_cache_enabled": config.ip_result_cache.enabled,
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

    def get_logging_settings(self) -> Dict[str, Any]:
        """读取磁盘运行日志文件的保留配置（来自 system_settings）。"""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT logging_retention_days, logging_file_path FROM system_settings WHERE id = 1"
            ).fetchone()
        if not row:
            return {"retention_days": 30, "file_path": ""}
        return {
            "retention_days": int(row["logging_retention_days"] or 30),
            "file_path": row["logging_file_path"] or "",
        }

    def update_logging_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """更新磁盘运行日志文件的保留天数（最小 1 天），返回最新设置。"""
        retention_days = max(1, int(payload.get("retention_days", 30) or 30))
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE system_settings
                SET logging_retention_days = ?, updated_at = ?
                WHERE id = 1
                """,
                (retention_days, now),
            )
        return self.get_logging_settings()

    def insert_route_log(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        created_at = str(payload.get("created_at", "")).strip() or utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO route_logs (
                    request_id, request_method, request_path, request_query_string, request_host, path_prefix, rule_id,
                    rule_name, rule_request_host, rule_source, target_url, redirect_location, original_client_ip, client_ip, region_matching_enabled,
                    geo_source, geo_summary, geo_country, geo_region, geo_city,
                    configured_ip_whitelist, matched_ip_whitelist, configured_regions, matched_region, match_strategy, match_detail,
                    upstream_status, cache_status, redirect_count, transport_mode,
                    operation_duration_ms, result_status, error_message, referer, user_agent, bytes_transferred, chain, created_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    str(payload.get("request_id", "")).strip(),
                    str(payload.get("request_method", "")).strip(),
                    str(payload.get("request_path", "")).strip(),
                    str(payload.get("request_query_string", "")).strip(),
                    normalize_request_host(payload.get("request_host", "")),
                    str(payload.get("path_prefix", "")).strip(),
                    payload.get("rule_id"),
                    str(payload.get("rule_name", "")).strip(),
                    normalize_request_host(payload.get("rule_request_host", "")),
                    str(payload.get("rule_source", "")).strip(),
                    str(payload.get("target_url", "")).strip(),
                    str(payload.get("redirect_location", "")).strip(),
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
                    str(payload.get("referer", "")).strip(),
                    str(payload.get("user_agent", "")).strip(),
                    int(payload.get("bytes_transferred", 0) or 0),
                    str(payload.get("chain", "")).strip(),
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
                "request_path LIKE ? OR request_host LIKE ? OR rule_request_host LIKE ? OR "
                "path_prefix LIKE ? OR rule_name LIKE ? OR target_url LIKE ? OR redirect_location LIKE ? OR "
                "geo_summary LIKE ? OR matched_region LIKE ? OR client_ip LIKE ? OR original_client_ip LIKE ? OR "
                "chain LIKE ?"
                ")"
            )
            params.extend([like_value] * 12)

        path_prefix = str(filters.get("path_prefix", "")).strip()
        if path_prefix:
            clauses.append("path_prefix = ?")
            params.append(path_prefix)

        raw_rule_request_host = str(filters.get("rule_request_host", "")).strip()
        normalized_rule_request_host = normalize_request_host(raw_rule_request_host)
        if raw_rule_request_host:
            if raw_rule_request_host == "*":
                clauses.append("rule_request_host = ''")
            else:
                rule_request_hosts = [
                    host.strip()
                    for host in normalized_rule_request_host.split(",")
                    if host.strip()
                ]
                if rule_request_hosts:
                    host_clauses = []
                    for host in rule_request_hosts:
                        host_clauses.append("(',' || rule_request_host || ',') LIKE ?")
                        params.append(f"%,{host},%")
                    clauses.append(f"({' OR '.join(host_clauses)})")

        match_strategy = str(filters.get("match_strategy", "")).strip()
        if match_strategy:
            clauses.append("match_strategy = ?")
            params.append(match_strategy)

        result_status = str(filters.get("result_status", "")).strip()
        if result_status:
            clauses.append("result_status = ?")
            params.append(result_status)

        referer = str(filters.get("referer", "")).strip()
        if referer:
            clauses.append("referer LIKE ?")
            params.append(f"%{referer}%")

        date_from = str(filters.get("date_from", "")).strip()
        if date_from:
            clauses.append("created_at >= ?")
            params.append(date_from)

        date_to = str(filters.get("date_to", "")).strip()
        if date_to:
            clauses.append("created_at <= ?")
            params.append(date_to)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit = max(1, min(500, int(filters.get("limit", 50) or 50)))
        page = max(1, int(filters.get("page", 1) or 1))
        offset = (page - 1) * limit

        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM route_logs
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
            total = connection.execute(
                f"SELECT COUNT(*) FROM route_logs {where_sql}",
                params,
            ).fetchone()[0]

        return {
            "items": [self.serialize_route_log(row) for row in rows],
            "total": total,
            "limit": limit,
            "page": page,
            "offset": offset,
            "total_pages": max(1, (total + limit - 1) // limit),
            "filters": {
                "keyword": keyword,
                "path_prefix": path_prefix,
                "rule_request_host": raw_rule_request_host,
                "match_strategy": match_strategy,
                "result_status": result_status,
                "referer": referer,
                "date_from": date_from,
                "date_to": date_to,
            },
        }

    # ===== 管理操作审计日志（P0-1.3） =====

    def insert_audit_log(
        self,
        actor_type: str,
        actor_id: str,
        action: str,
        target_type: str,
        target_id: str = "",
        detail: str = "",
    ) -> Dict[str, Any]:
        """落一条管理操作审计。created_at 与 route_logs 同格式（UTC ISO 字符串）。"""
        created_at = utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO admin_audit_log (
                    actor_type, actor_id, action, target_type, target_id, detail, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(actor_type or "").strip(),
                    str(actor_id or "").strip(),
                    str(action or "").strip(),
                    str(target_type or "").strip(),
                    str(target_id or "").strip(),
                    str(detail or "").strip(),
                    created_at,
                ),
            )
            row = connection.execute(
                "SELECT * FROM admin_audit_log WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()
        return self.serialize_audit_log(row) if row else {
            "id": cursor.lastrowid,
            "actor_type": actor_type,
            "actor_id": actor_id,
            "action": action,
            "target_type": target_type,
            "target_id": target_id,
            "detail": detail,
            "created_at": created_at,
        }

    @staticmethod
    def serialize_audit_log(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "actor_type": row["actor_type"],
            "actor_id": row["actor_id"],
            "action": row["action"],
            "target_type": row["target_type"],
            "target_id": row["target_id"],
            "detail": row["detail"],
            "created_at": row["created_at"],
        }

    def list_audit_logs(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """审计日志分页查询（语义与 list_route_logs 保持一致）。"""
        filters = filters or {}
        clauses: List[str] = []
        params: List[Any] = []

        keyword = str(filters.get("keyword", "")).strip()
        if keyword:
            like_value = f"%{keyword}%"
            clauses.append(
                "(actor_id LIKE ? OR action LIKE ? OR target_type LIKE ? OR "
                "target_id LIKE ? OR detail LIKE ?)"
            )
            params.extend([like_value] * 5)

        action = str(filters.get("action", "")).strip()
        if action:
            clauses.append("action = ?")
            params.append(action)

        target_type = str(filters.get("target_type", "")).strip()
        if target_type:
            clauses.append("target_type = ?")
            params.append(target_type)

        date_from = str(filters.get("date_from", "")).strip()
        if date_from:
            clauses.append("created_at >= ?")
            params.append(date_from)

        date_to = str(filters.get("date_to", "")).strip()
        if date_to:
            clauses.append("created_at <= ?")
            params.append(date_to)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit = max(1, min(500, int(filters.get("limit", 20) or 20)))
        page = max(1, int(filters.get("page", 1) or 1))
        offset = (page - 1) * limit

        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM admin_audit_log
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
            total = connection.execute(
                f"SELECT COUNT(*) FROM admin_audit_log {where_sql}",
                params,
            ).fetchone()[0]

        return {
            "items": [self.serialize_audit_log(row) for row in rows],
            "total": total,
            "limit": limit,
            "page": page,
            "offset": offset,
            "total_pages": max(1, (total + limit - 1) // limit),
            "filters": {
                "keyword": keyword,
                "action": action,
                "target_type": target_type,
                "date_from": date_from,
                "date_to": date_to,
            },
        }

    def get_hotlink_stats(self, hours: int = 24) -> Dict[str, Any]:
        """盗链监控聚合（HOTLINK_PROTECTION.md 阶段 1）。

        返回近 N 小时：
        - top_referrers：外部 Referer 域名 TOP10。route_logs 只存原始 Referer
          字符串，SQLite 没有域名解析函数，因此在 Python 侧按 hostname 聚合；
          排除空 Referer 与与请求命中域名相同的"自引用"（本站页面跳转不算盗链）。
        - top_ips_by_bytes：单 IP 流量 TOP10（SUM(bytes_transferred)），纯 SQL 聚合。

        列为 017 迁移新增，旧库已在 _run_migrations 补齐；为防极端情况下缺列，
        参考列存在性后降级返回空结果而不是抛错。
        """
        from datetime import datetime, timedelta, timezone as tz
        from urllib.parse import urlparse

        hours = max(1, min(168, int(hours or 24)))
        cutoff = (datetime.now(tz.utc) - timedelta(hours=hours)).isoformat(timespec="seconds")

        with self._connect() as connection:
            log_columns = {row["name"] for row in connection.execute("PRAGMA table_info(route_logs)").fetchall()}
            if "referer" not in log_columns or "bytes_transferred" not in log_columns:
                return {
                    "hours": hours,
                    "external_referer_total": 0,
                    "top_referrers": [],
                    "top_ips_by_bytes": [],
                }

            ip_rows = connection.execute(
                """
                SELECT client_ip, SUM(bytes_transferred) AS total_bytes, COUNT(*) AS request_count
                FROM route_logs
                WHERE created_at >= ? AND client_ip != '' AND bytes_transferred > 0
                GROUP BY client_ip
                ORDER BY total_bytes DESC
                LIMIT 10
                """,
                (cutoff,),
            ).fetchall()
            # Referer 聚合需要逐行解析域名；上限防极端日志量撑爆内存
            referer_rows = connection.execute(
                """
                SELECT referer, request_host, client_ip
                FROM route_logs
                WHERE created_at >= ? AND referer != ''
                LIMIT 20000
                """,
                (cutoff,),
            ).fetchall()

        referer_counts: Dict[str, Dict[str, Any]] = {}
        external_referer_total = 0
        for row in referer_rows:
            referer = str(row["referer"] or "").strip()
            if not referer:
                continue
            parsed = urlparse(referer)
            host = (parsed.hostname or referer).strip().lower()
            if not host:
                continue
            request_host = str(row["request_host"] or "").strip().lower()
            if host == request_host:
                continue  # 自引用（本站页面跳转），不算盗链
            external_referer_total += 1
            entry = referer_counts.setdefault(
                host, {"host": host, "count": 0, "last_client_ip": str(row["client_ip"] or "")}
            )
            entry["count"] += 1
            if row["client_ip"]:
                entry["last_client_ip"] = str(row["client_ip"])

        top_referrers = sorted(referer_counts.values(), key=lambda item: item["count"], reverse=True)[:10]
        return {
            "hours": hours,
            "external_referer_total": external_referer_total,
            "top_referrers": top_referrers,
            "top_ips_by_bytes": [
                {
                    "client_ip": str(row["client_ip"]),
                    "bytes_transferred": int(row["total_bytes"] or 0),
                    "request_count": int(row["request_count"] or 0),
                }
                for row in ip_rows
            ],
        }

    def get_overview_stats(self) -> Dict[str, Any]:
        """概览页聚合统计：24h 分桶（总请求/302跳转/本地代理/失败）、今日口径三项、平均延迟。

        口径约定（KPI 卡与趋势图共用，保证相加自洽）：
        - 「302 跳转」= 客户端最终拿到 30x 的请求（redirect_count>0 且非 streaming 行）；
        - 「本地代理」= transport_mode='streaming' 的穿流请求（内部跟随上游 302 属实现
          细节，客户端拿到的是媒体流，不计入 302 跳转）——两者按最终出流模式互斥；
        - 「失败/拦截」= upstream_status>=400（错误维度，可与上两者重叠）。

        route_logs.created_at 为 UTC ISO 字符串（'2026-09-02T02:55:50+00:00'，
        由 main._build_route_log_payload 写入），因此：
        - 用 substr(created_at,1,13) 按 UTC 小时分桶，SQL 直算不受 list 接口
          limit=500 截断影响（此前前端取最近 500 条自行分桶，趋势图与平均
          延迟只反映最近 500 条且 Number(ISO) 为 NaN 导致趋势恒为空）；
        - "今日"按本地时区 0 点换算成对应 UTC 时刻再比较。
        """
        from datetime import datetime, timedelta, timezone as tz

        now_utc = datetime.now(tz.utc)
        hour0 = now_utc.replace(minute=0, second=0, microsecond=0)
        cutoff_24h = (now_utc - timedelta(hours=24)).isoformat(timespec="seconds")
        # 本地今日 0 点对应的 UTC 时刻（库内 created_at 均为 UTC）
        local_midnight = datetime.now().astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
        today_cutoff_utc = local_midnight.astimezone(tz.utc).isoformat(timespec="seconds")

        with self._connect() as connection:
            requests_total = connection.execute("SELECT COUNT(*) FROM route_logs").fetchone()[0]
            requests_24h = connection.execute(
                "SELECT COUNT(*) FROM route_logs WHERE created_at >= ?", (cutoff_24h,)
            ).fetchone()[0]
            requests_today = connection.execute(
                "SELECT COUNT(*) FROM route_logs WHERE created_at >= ?", (today_cutoff_utc,)
            ).fetchone()[0]
            # 24h 三项汇总（与小时桶同源同窗口）：KPI 卡不再用进程内存计数器，
            # 服务重启后依旧与趋势图一致（此前 ProxyStats 重启归零导致「有数据但显示 0」）
            # 「302 跳转」排除 streaming 行：B 模式穿流请求若上游经历 302 跟随，
            # 该行同时带 redirect_count>0 与 transport_mode='streaming'，不排除会与
            # 「本地代理」重复计数（48+41>70 的根因）；按最终出流模式互斥归类
            kpi_row = connection.execute(
                """
                SELECT SUM(CASE WHEN redirect_count > 0 AND transport_mode != 'streaming' THEN 1 ELSE 0 END) AS redirects,
                       SUM(CASE WHEN upstream_status >= 400 THEN 1 ELSE 0 END) AS failed,
                       SUM(CASE WHEN transport_mode = 'streaming' THEN 1 ELSE 0 END) AS streamed
                FROM route_logs
                WHERE created_at >= ?
                """,
                (cutoff_24h,),
            ).fetchone()
            # 「今日」口径（与 requests_today 同窗口）：302/本地代理/拦截 KPI 与「今日请求」
            # 同窗对比，避免 24h 滚动窗 ⊃ 今日 导致「分项相加 > 今日请求」的口径错位
            kpi_today_row = connection.execute(
                """
                SELECT SUM(CASE WHEN redirect_count > 0 AND transport_mode != 'streaming' THEN 1 ELSE 0 END) AS redirects,
                       SUM(CASE WHEN upstream_status >= 400 THEN 1 ELSE 0 END) AS failed,
                       SUM(CASE WHEN transport_mode = 'streaming' THEN 1 ELSE 0 END) AS streamed
                FROM route_logs
                WHERE created_at >= ?
                """,
                (today_cutoff_utc,),
            ).fetchone()
            lat_row = connection.execute(
                "SELECT AVG(operation_duration_ms), COUNT(*) FROM route_logs "
                "WHERE created_at >= ? AND operation_duration_ms > 0",
                (cutoff_24h,),
            ).fetchone()
            bucket_rows = connection.execute(
                """
                SELECT substr(created_at, 1, 13) AS hour_key,
                       COUNT(*) AS cnt,
                       SUM(CASE WHEN redirect_count > 0 AND transport_mode != 'streaming' THEN 1 ELSE 0 END) AS redirects,
                       SUM(CASE WHEN upstream_status >= 400 THEN 1 ELSE 0 END) AS failed,
                       SUM(CASE WHEN transport_mode = 'streaming' THEN 1 ELSE 0 END) AS streamed
                FROM route_logs
                WHERE created_at >= ?
                GROUP BY hour_key
                """,
                (cutoff_24h,),
            ).fetchall()

        avg_latency = lat_row[0] if lat_row and lat_row[0] is not None else None

        # 24 个小时桶：索引 0 = 23 小时前的整点，23 = 当前小时（UTC 整点对齐）
        buckets = [
            {"ts": int((hour0 - timedelta(hours=23 - i)).timestamp()), "count": 0, "redirects": 0, "failed": 0, "streamed": 0}
            for i in range(24)
        ]
        for row in bucket_rows:
            try:
                hour_start = datetime.strptime(str(row["hour_key"]), "%Y-%m-%dT%H").replace(tzinfo=tz.utc)
            except (ValueError, TypeError):
                continue  # 兼容异格式/旧数据，跳过不入桶
            idx = 23 - int((hour0 - hour_start).total_seconds() // 3600)
            if 0 <= idx < 24:
                bucket = buckets[idx]
                bucket["count"] += row["cnt"] or 0
                bucket["redirects"] += row["redirects"] or 0
                bucket["failed"] += row["failed"] or 0
                bucket["streamed"] += row["streamed"] or 0

        def _kpi_num(value: Any) -> int:
            return int(value or 0)

        return {
            "requests_total": requests_total,
            "requests_24h": requests_24h,
            "requests_today": requests_today,
            "redirects_24h": _kpi_num(kpi_row["redirects"]) if kpi_row else 0,
            "failed_24h": _kpi_num(kpi_row["failed"]) if kpi_row else 0,
            "streamed_24h": _kpi_num(kpi_row["streamed"]) if kpi_row else 0,
            "redirects_today": _kpi_num(kpi_today_row["redirects"]) if kpi_today_row else 0,
            "failed_today": _kpi_num(kpi_today_row["failed"]) if kpi_today_row else 0,
            "streamed_today": _kpi_num(kpi_today_row["streamed"]) if kpi_today_row else 0,
            "avg_latency_ms": round(avg_latency, 1) if avg_latency is not None else None,
            "latency_sample_count": lat_row[1] if lat_row else 0,
            "hours": buckets,
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

    def list_banned_ips(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM banned_ips ORDER BY banned_at DESC").fetchall()
        return [self._serialize_banned_ip(row) for row in rows]

    def get_banned_ip(self, ip: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM banned_ips WHERE ip = ?", (ip,)).fetchone()
        if not row:
            return None
        return self._serialize_banned_ip(row)

    def add_banned_ip(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ip = str(payload.get("ip", "")).strip()
        if not ip:
            raise ValueError("IP地址不能为空")
        now = utc_now()
        banned_at = float(payload.get("banned_at", 0) or time.time())
        permanent = bool(payload.get("permanent", True))
        reason = str(payload.get("reason", "")).strip()
        banned_by = str(payload.get("banned_by", "admin")).strip()
        path_prefix = str(payload.get("path_prefix", "") or "").strip()

        # 优先使用前端传入的 expire_at；否则根据 duration_seconds 计算
        expire_at = float(payload.get("expire_at", 0) or 0)
        if not permanent:
            duration_seconds = int(payload.get("duration_seconds", 0) or 0)
            if duration_seconds > 0:
                expire_at = time.time() + duration_seconds
            elif expire_at <= 0:
                # 临时封禁但未指定时长，按永久处理避免无限期封禁
                permanent = True
                expire_at = 0.0
        else:
            expire_at = 0.0

        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO banned_ips (ip, reason, banned_by, banned_at, expire_at, permanent, path_prefix, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ip, reason, banned_by, banned_at, expire_at, int(permanent), path_prefix, now),
            )
        return self.get_banned_ip(ip)

    def remove_banned_ip(self, ip: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM banned_ips WHERE ip = ?", (ip,))
        return cursor.rowcount > 0

    def extend_banned_ip(self, ip: str, duration_hours: float) -> Dict[str, Any]:
        """延长临时封禁时长。若已过期则从当前时间起算；未过期则在原 expire_at 基础上累加。
        永久封禁调用此方法将转为临时封禁。"""
        duration_seconds = float(duration_hours) * 3600.0
        if duration_seconds <= 0:
            raise ValueError("延长时长必须大于0")
        now_ts = time.time()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT expire_at, permanent FROM banned_ips WHERE ip = ?", (ip,)
            ).fetchone()
            if not row:
                raise KeyError(f"IP {ip} 不在封禁列表中")
            current_expire = float(row["expire_at"] or 0)
            current_permanent = bool(row["permanent"])
            # 永久封禁或已过期：从当前时间起算
            if current_permanent or current_expire <= 0 or current_expire < now_ts:
                new_expire = now_ts + duration_seconds
            else:
                # 未过期：在原到期时间基础上累加
                new_expire = current_expire + duration_seconds
            connection.execute(
                "UPDATE banned_ips SET expire_at = ?, permanent = 0 WHERE ip = ?",
                (new_expire, ip),
            )
        return self.get_banned_ip(ip)

    def set_banned_ip_permanent(self, ip: str) -> Dict[str, Any]:
        """将指定临时封禁转为永久封禁。"""
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE banned_ips SET permanent = 1, expire_at = 0 WHERE ip = ?",
                (ip,),
            )
            if cursor.rowcount == 0:
                raise KeyError(f"IP {ip} 不在封禁列表中")
        return self.get_banned_ip(ip)

    def clear_all_banned_ips(self) -> int:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM banned_ips")
        return cursor.rowcount

    def cleanup_expired_bans(self) -> int:
        """删除数据库中已过期的临时封禁记录（非永久且 expire_at > 0 且小于当前时间）。"""
        now_ts = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM banned_ips WHERE permanent = 0 AND expire_at > 0 AND expire_at < ?",
                (now_ts,),
            )
        return cursor.rowcount

    # ===== API 密钥（后台接口程序化调用）=====

    @staticmethod
    def _hash_api_key(raw_key: str) -> str:
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    @staticmethod
    def _serialize_api_key(row: sqlite3.Row) -> Dict[str, Any]:
        expires_at = row["expires_at"]
        keys = row.keys()
        return {
            "id": row["id"],
            "name": row["name"],
            "key_prefix": row["key_prefix"],
            "readonly": bool(row["readonly"]),
            "enabled": bool(row["enabled"]),
            "use_count": int(row["use_count"] or 0),
            "created_at": row["created_at"],
            "last_used_at": row["last_used_at"],
            "expires_at": expires_at,
            "expired": bool(expires_at) and int(expires_at) <= int(time.time()),
            # P1-2.3 细粒度权限（row.keys() 守卫兼容未迁移的库）
            "scopes": row["scopes"] if "scopes" in keys else "",
            "allowed_ips": row["allowed_ips"] if "allowed_ips" in keys else "",
            "rate_limit": int(row["rate_limit"] or 0) if "rate_limit" in keys else 0,
        }

    def create_api_key(self, name: str, readonly: bool = False, expires_days: int = 0,
                       scopes: str = "", allowed_ips: str = "", rate_limit: int = 0) -> Dict[str, Any]:
        """签发 API 密钥；完整明文只在本返回值中出现一次，库中仅存 SHA256。

        P1-2.3：scopes=逗号分隔端点 tag（空=全部）；allowed_ips=逗号分隔 IP（空=不限）；
        rate_limit=每秒请求数上限（0=不限）。
        """
        name = str(name or "").strip() or "未命名密钥"
        raw_key = f"n302_{secrets.token_hex(16)}"
        days = int(expires_days or 0)
        expires_at = int(time.time()) + days * 86400 if days > 0 else None
        now = utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO api_keys (name, key_prefix, key_hash, readonly, enabled, use_count, created_at, last_used_at, expires_at,
                                      scopes, allowed_ips, rate_limit)
                VALUES (?, ?, ?, ?, 1, 0, ?, '', ?, ?, ?, ?)
                """,
                (
                    name, raw_key[:13], self._hash_api_key(raw_key), int(bool(readonly)), now, expires_at,
                    str(scopes or "").strip(), str(allowed_ips or "").strip(), max(0, int(rate_limit or 0)),
                ),
            )
            key_id = int(cursor.lastrowid)
        return {
            "id": key_id,
            "name": name,
            "key": raw_key,
            "key_prefix": raw_key[:13],
            "readonly": bool(readonly),
            "expires_at": expires_at,
        }

    def update_api_key_config(self, key_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        """更新密钥的细粒度权限（P1-2.3）：scopes / allowed_ips / rate_limit。"""
        scopes = str(payload.get("scopes", "") or "").strip()
        allowed_ips = str(payload.get("allowed_ips", "") or "").strip()
        rate_limit = max(0, int(payload.get("rate_limit", 0) or 0))
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE api_keys SET scopes = ?, allowed_ips = ?, rate_limit = ? WHERE id = ?",
                (scopes, allowed_ips, rate_limit, int(key_id)),
            )
            if cursor.rowcount == 0:
                raise KeyError(f"API 密钥 {key_id} 不存在")
        return self.get_api_key(key_id)

    def list_api_keys(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM api_keys ORDER BY id DESC").fetchall()
        return [self._serialize_api_key(row) for row in rows]

    def get_api_key(self, key_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM api_keys WHERE id = ?", (int(key_id),)).fetchone()
        if not row:
            raise KeyError(f"API 密钥 {key_id} 不存在")
        return self._serialize_api_key(row)

    def find_api_key(self, raw_key: str) -> Optional[Dict[str, Any]]:
        """按明文 Key 查找有效密钥（SHA256 精确命中；停用/过期一律视为无效）。"""
        raw_key = str(raw_key or "").strip()
        if not raw_key.startswith("n302_"):
            return None
        key_hash = self._hash_api_key(raw_key)
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM api_keys WHERE key_hash = ?", (key_hash,)).fetchone()
        if not row:
            return None
        item = self._serialize_api_key(row)
        if not item["enabled"] or item["expired"]:
            return None
        return item

    def touch_api_key(self, key_id: int) -> None:
        """更新最近使用时间与累计次数（鉴权热路径，单条 UPDATE；调用方负责节流）。"""
        with self._connect() as connection:
            connection.execute(
                "UPDATE api_keys SET use_count = use_count + 1, last_used_at = ? WHERE id = ?",
                (utc_now(), int(key_id)),
            )

    def set_api_key_enabled(self, key_id: int, enabled: bool) -> Dict[str, Any]:
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE api_keys SET enabled = ? WHERE id = ?",
                (int(bool(enabled)), int(key_id)),
            )
            if cursor.rowcount == 0:
                raise KeyError(f"API 密钥 {key_id} 不存在")
        return self.get_api_key(key_id)

    def delete_api_key(self, key_id: int) -> bool:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM api_keys WHERE id = ?", (int(key_id),))
        return cursor.rowcount > 0

    def import_banned_ips(self, bans: List[Dict[str, Any]]) -> int:
        count = 0
        now = utc_now()
        with self._connect() as connection:
            for b in bans:
                ip = b.get("ip", "")
                if not ip:
                    continue
                connection.execute(
                    """
                    INSERT OR REPLACE INTO banned_ips (ip, reason, banned_by, banned_at, expire_at, permanent, path_prefix, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ip,
                        str(b.get("reason", "")).strip(),
                        str(b.get("banned_by", "import")).strip(),
                        float(b.get("banned_at", time.time())),
                        float(b.get("expire_at", 0)),
                        int(bool(b.get("permanent", True))),
                        str(b.get("path_prefix", "") or "").strip(),
                        now,
                    ),
                )
                count += 1
        return count

    def _serialize_banned_ip(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "ip": row["ip"],
            "reason": row["reason"],
            "banned_by": row["banned_by"],
            "banned_at": row["banned_at"],
            "expire_at": row["expire_at"],
            "permanent": bool(row["permanent"]),
            "path_prefix": row["path_prefix"] if "path_prefix" in row.keys() else "",
            "created_at": row["created_at"],
        }

    def get_ip_cache_config(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM system_settings WHERE id = 1").fetchone()
        if not row:
            return {"enabled": False, "ttl_seconds": 300, "max_entries": 5000}
        keys = row.keys()
        enabled_val = row["ip_cache_enabled"] if "ip_cache_enabled" in keys else 0
        ttl_val = row["ip_cache_ttl_seconds"] if "ip_cache_ttl_seconds" in keys else 300
        max_val = row["ip_cache_max_entries"] if "ip_cache_max_entries" in keys else 5000
        return {
            "enabled": bool(enabled_val),
            "ttl_seconds": int(ttl_val or 300),
            "max_entries": int(max_val or 5000),
        }

    def update_ip_cache_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled = coerce_bool(payload.get("enabled", False))
        ttl_seconds = max(0, int(payload.get("ttl_seconds", 300) or 300))
        max_entries = max(0, int(payload.get("max_entries", 5000) or 5000))
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE system_settings
                SET ip_cache_enabled = ?, ip_cache_ttl_seconds = ?, ip_cache_max_entries = ?, updated_at = ?
                WHERE id = 1
                """,
                (int(enabled), ttl_seconds, max_entries, now),
            )
        return self.get_ip_cache_config()

    def get_dedup_config(self) -> Dict[str, Any]:
        config = self.load_runtime_config()
        return {
            "enabled": config.request_dedup.enabled,
            "window_seconds": config.request_dedup.window_seconds,
            "max_cache_entries": config.request_dedup.max_cache_entries,
        }

    def update_dedup_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled = coerce_bool(payload.get("enabled", False))
        window_seconds = max(0.5, float(payload.get("window_seconds", 2.0) or 2.0))
        max_cache_entries = max(100, int(payload.get("max_cache_entries", 10000) or 10000))
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE system_settings
                SET dedup_enabled = ?, dedup_window_seconds = ?, dedup_max_cache_entries = ?, updated_at = ?
                WHERE id = 1
                """,
                (int(enabled), window_seconds, max_cache_entries, now),
            )
        return self.get_dedup_config()

    def get_auto_ban_config(self) -> Dict[str, Any]:
        config = self.load_runtime_config()
        return {
            "enabled": config.auto_ban.enabled,
            "window_seconds": config.auto_ban.window_seconds,
            "max_requests": config.auto_ban.max_requests,
            "ban_duration_seconds": config.auto_ban.ban_duration_seconds,
            "max_404": config.auto_ban.max_404,
            "auto_ban_on_404": config.auto_ban.auto_ban_on_404,
            "whitelist": config.auto_ban.whitelist,
            "email_on_ban": config.auto_ban.email_on_ban,
            "max_bytes": config.auto_ban.max_bytes,
        }

    def update_auto_ban_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled = coerce_bool(payload.get("enabled", False))
        window_seconds = max(10, int(payload.get("window_seconds", 60) or 60))
        max_requests = max(1, int(payload.get("max_requests", 100) or 100))
        ban_duration_seconds = max(60, int(payload.get("ban_duration_seconds", 3600) or 3600))
        max_404 = max(1, int(payload.get("max_404", 20) or 20))
        auto_ban_on_404 = coerce_bool(payload.get("auto_ban_on_404", True))
        whitelist = str(payload.get("whitelist", "") or "")
        email_on_ban = coerce_bool(payload.get("email_on_ban", False))
        max_bytes = max(0, int(payload.get("max_bytes", 0) or 0))
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE system_settings
                SET auto_ban_enabled = ?, auto_ban_window_seconds = ?, auto_ban_max_requests = ?,
                    auto_ban_ban_duration_seconds = ?, auto_ban_max_404 = ?, auto_ban_auto_ban_on_404 = ?,
                    auto_ban_whitelist = ?, auto_ban_email_on_ban = ?, auto_ban_max_bytes = ?, updated_at = ?
                WHERE id = 1
                """,
                (int(enabled), window_seconds, max_requests, ban_duration_seconds, max_404, int(auto_ban_on_404), whitelist, int(email_on_ban), max_bytes, now),
            )
        return self.get_auto_ban_config()

    def get_stream_guard_config(self) -> Dict[str, Any]:
        """单 IP 并发限制配置（HOTLINK_PROTECTION.md 阶段 3.2）。"""
        config = self.load_runtime_config()
        return {"max_concurrent_per_ip": config.streaming.max_concurrent_per_ip}

    def update_stream_guard_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        max_concurrent = max(0, int(payload.get("max_concurrent_per_ip", 0) or 0))
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                "UPDATE system_settings SET streaming_max_concurrent_per_ip = ?, updated_at = ? WHERE id = 1",
                (max_concurrent, now),
            )
        return self.get_stream_guard_config()

    def get_signed_url_config(self) -> Dict[str, Any]:
        """签名 URL 配置（HOTLINK_PROTECTION.md 阶段 4）。绝不返回 secret 明文。"""
        config = self.load_runtime_config()
        return {
            "enabled": config.signed_url.enabled,
            "ttl_seconds": config.signed_url.ttl_seconds,
            "has_secret": bool(config.signed_url.secret),
        }

    def update_signed_url_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        import secrets as secrets_module

        # 部分更新语义：未提供的字段保留现值，避免「只轮换密钥」顺带把开关关掉
        current = self.load_runtime_config().signed_url
        enabled = coerce_bool(payload.get("enabled", current.enabled))
        ttl_seconds = max(60, int(payload.get("ttl_seconds", current.ttl_seconds) or current.ttl_seconds))
        regenerate = coerce_bool(payload.get("regenerate_secret", False))
        now = utc_now()

        set_clause = "signed_url_enabled = ?, signed_url_ttl_seconds = ?, updated_at = ?"
        params: List[Any] = [int(enabled), ttl_seconds, now]
        if regenerate:
            set_clause += ", signed_url_secret = ?"
            params.append(secrets_module.token_hex(32))

        with self._connect() as connection:
            connection.execute(
                f"UPDATE system_settings SET {set_clause} WHERE id = 1",
                params,
            )
        return self.get_signed_url_config()

    # ===== 登录防爆破（P0-1.2） =====

    def get_login_protection_config(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM system_settings WHERE id = 1").fetchone()
        keys = row.keys() if row else []
        max_attempts = int(row["login_max_attempts"]) if row and "login_max_attempts" in keys else 5
        lockout_minutes = int(row["login_lockout_minutes"]) if row and "login_lockout_minutes" in keys else 15
        cooldown_seconds = int(row["login_cooldown_seconds"]) if row and "login_cooldown_seconds" in keys else 0
        return {
            "max_attempts": max_attempts,
            "lockout_minutes": lockout_minutes,
            "cooldown_seconds": cooldown_seconds,
        }

    def update_login_protection_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        max_attempts = max(1, int(payload.get("max_attempts", 5) or 5))
        lockout_minutes = max(1, int(payload.get("lockout_minutes", 15) or 15))
        cooldown_seconds = max(0, int(payload.get("cooldown_seconds", 0) or 0))
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                "UPDATE system_settings SET login_max_attempts = ?, login_lockout_minutes = ?, "
                "login_cooldown_seconds = ?, updated_at = ? WHERE id = 1",
                (max_attempts, lockout_minutes, cooldown_seconds, now),
            )
        return self.get_login_protection_config()

    # ===== 主动速率限制（P1-2.1） =====

    def get_rate_limit_config(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM system_settings WHERE id = 1").fetchone()
        keys = row.keys() if row else []
        enabled = bool(row["rate_limit_enabled"]) if row and "rate_limit_enabled" in keys else False
        rps = float(row["rate_limit_rps"]) if row and "rate_limit_rps" in keys else 10.0
        burst = int(row["rate_limit_burst"]) if row and "rate_limit_burst" in keys else 20
        per_ip = coerce_bool(row["rate_limit_per_ip"]) if row and "rate_limit_per_ip" in keys else True
        return {
            "enabled": enabled,
            "requests_per_second": rps,
            "burst": burst,
            "per_ip": per_ip,
        }

    def update_rate_limit_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled = coerce_bool(payload.get("enabled", False))
        # rps 钳制为 (0, 1000]，避免配置成 0 导致全部请求被拒或除零。
        # 必须用 is not None 判断缺失，否则显式传入的 0 会被 `or 10` 兜底成默认值，
        # 导致 max/min 钳制完全失效（例如 burst=0 既不会回落到 1，也不会保留 20）。
        rps_raw = payload.get("requests_per_second", 10)
        rps = max(0.1, min(1000.0, float(rps_raw))) if rps_raw is not None else 10.0
        burst_raw = payload.get("burst", 20)
        burst = max(1, int(burst_raw)) if burst_raw is not None else 20
        per_ip = coerce_bool(payload.get("per_ip", True))
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                "UPDATE system_settings SET rate_limit_enabled = ?, rate_limit_rps = ?, "
                "rate_limit_burst = ?, rate_limit_per_ip = ?, updated_at = ? WHERE id = 1",
                (int(enabled), rps, burst, int(per_ip), now),
            )
        return self.get_rate_limit_config()

    # ===== 跨域资源共享 CORS（P2-3.2） =====

    def get_cors_config(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM system_settings WHERE id = 1").fetchone()
        keys = row.keys() if row else []
        return {
            "enabled": bool(row["cors_enabled"]) if row and "cors_enabled" in keys else False,
            "allowed_origins": (row["cors_allowed_origins"] if row and "cors_allowed_origins" in keys else "") or "",
            "allowed_methods": (row["cors_allowed_methods"] if row and "cors_allowed_methods" in keys else "") or "GET,HEAD,OPTIONS",
            "allow_credentials": coerce_bool(row["cors_allow_credentials"]) if row and "cors_allow_credentials" in keys else False,
            "max_age": int(row["cors_max_age"]) if row and "cors_max_age" in keys else 600,
        }

    def update_cors_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled = coerce_bool(payload.get("enabled", False))
        origins = " ".join(str(payload.get("allowed_origins", "") or "").replace(";", ",").split())
        methods = str(payload.get("allowed_methods", "") or "").strip() or "GET,HEAD,OPTIONS"
        credentials = coerce_bool(payload.get("allow_credentials", False))
        max_age_raw = payload.get("max_age", 600)
        max_age = max(0, min(86400, int(max_age_raw))) if max_age_raw is not None else 600
        if enabled and not origins.strip():
            raise ValueError("启用 CORS 时必须填写允许的来源（allowed_origins）")
        if enabled and credentials and origins.strip() == "*":
            # CORS 规范：Allow-Origin: * 与 Allow-Credentials: true 互斥，浏览器会直接拒绝
            raise ValueError("allow_credentials=true 时 allowed_origins 不能为 *，请列出具体来源")
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                "UPDATE system_settings SET cors_enabled = ?, cors_allowed_origins = ?, "
                "cors_allowed_methods = ?, cors_allow_credentials = ?, cors_max_age = ?, updated_at = ? WHERE id = 1",
                (int(enabled), origins, methods, int(credentials), max_age, now),
            )
        return self.get_cors_config()

    # ===== 配置版本历史 / 导出导入（P2-3.5） =====

    # 模块名 -> (getter, updater)。历史/回滚/导入全部复用 update 方法的
    # 校验与钳制逻辑，回滚相当于「把历史快照当一次普通更新重新写入」。
    _SETTINGS_MODULE_METHODS: Dict[str, Tuple[str, str]] = {
        "rate-limit": ("get_rate_limit_config", "update_rate_limit_config"),
        "cors": ("get_cors_config", "update_cors_config"),
        "notifications": ("get_notifications_config", "update_notifications_config"),
        "signed-url": ("get_signed_url_config", "update_signed_url_config"),
        "redirect-signing": ("get_redirect_signing_config", "update_redirect_signing_config"),
        "auto-ban": ("get_auto_ban_config", "update_auto_ban_config"),
        "email": ("get_email_config", "update_email_config"),
        "ip-cache": ("get_ip_cache_config", "update_ip_cache_config"),
        "dedup": ("get_dedup_config", "update_dedup_config"),
        "stream-guard": ("get_stream_guard_config", "update_stream_guard_config"),
        "login-protection": ("get_login_protection_config", "update_login_protection_config"),
        "remote-config": ("get_remote_config", "update_remote_config"),
    }

    def _resolve_settings_methods(self, module: str) -> Tuple[str, str]:
        methods = self._SETTINGS_MODULE_METHODS.get(str(module or "").strip())
        if not methods:
            raise ValueError(f"未知配置模块: {module}")
        return methods

    def record_settings_history(self, module: str, payload: Dict[str, Any], changed_by: str = "") -> None:
        """配置更新成功后落一条历史快照（失败仅告警，不影响主流程）。"""
        try:
            methods = self._resolve_settings_methods(module)
            _ = methods  # 未知名直接在 _resolve 抛 ValueError
            now = utc_now()
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO system_settings_history (module, payload_json, changed_by, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (module, json.dumps(payload, ensure_ascii=False), str(changed_by or ""), now),
                )
        except ValueError:
            raise
        except Exception as exc:  # pragma: no cover - 历史失败不阻断配置更新
            logger.warning("配置历史记录失败: module=%s err=%s", module, exc)

    def list_settings_history(self, module: str, limit: int = 20) -> Dict[str, Any]:
        getter, _ = self._resolve_settings_methods(module)
        current = getattr(self, getter)()
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id, module, changed_by, created_at FROM system_settings_history "
                "WHERE module = ? ORDER BY id DESC LIMIT ?",
                (module, max(1, min(100, int(limit or 20)))),
            ).fetchall()
        return {
            "module": module,
            "current": current,
            "items": [
                {"id": r["id"], "module": r["module"], "changed_by": r["changed_by"], "created_at": r["created_at"]}
                for r in rows
            ],
        }

    def rollback_settings(self, module: str, history_id: int) -> Dict[str, Any]:
        """把指定历史快照作为一次普通更新写回（复用 update 校验/钳制）。"""
        getter, updater = self._resolve_settings_methods(module)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM system_settings_history WHERE id = ? AND module = ?",
                (int(history_id), module),
            ).fetchone()
        if row is None:
            raise KeyError("历史记录不存在")
        try:
            payload = json.loads(row["payload_json"])
        except (json.JSONDecodeError, TypeError):
            raise ValueError("历史快照已损坏，无法回滚")
        if not isinstance(payload, dict):
            raise ValueError("历史快照格式非法，无法回滚")
        result = getattr(self, updater)(payload)
        self.record_settings_history(module, {**result, "rollback_from": int(history_id)}, "rollback")
        return {**result, "rollback_from": int(history_id)}

    def export_module(self, module: str) -> Dict[str, Any]:
        getter, _ = self._resolve_settings_methods(module)
        return {"module": module, "exported_at": utc_now(), "payload": getattr(self, getter)()}

    def import_module(self, module: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        _, updater = self._resolve_settings_methods(module)
        if not isinstance(payload, dict):
            raise ValueError("导入内容必须是 JSON 对象")
        return getattr(self, updater)(payload)

    # ===== Webhook / IM 告警通知（P1-2.4） =====

    _NOTIFICATION_CHANNEL_TYPES = ("generic", "feishu", "dingtalk", "slack")

    @classmethod
    def _parse_notifications_config(cls, raw: str) -> NotificationsConfig:
        """notifications_config JSON 串 → NotificationsConfig（解析失败回落未启用）。"""
        if not raw:
            return NotificationsConfig()
        try:
            data = json.loads(raw)
            if not isinstance(data, dict):
                return NotificationsConfig()
            channels = []
            for ch in data.get("channels", []) or []:
                if not isinstance(ch, dict):
                    continue
                ch_type = str(ch.get("type", "")).strip()
                if ch_type not in cls._NOTIFICATION_CHANNEL_TYPES:
                    continue
                channels.append({
                    "type": ch_type,
                    "name": str(ch.get("name", "") or "").strip(),
                    "enabled": coerce_bool(ch.get("enabled"), False),
                    "url": str(ch.get("url", "") or "").strip(),
                    "secret": str(ch.get("secret", "") or "").strip(),
                })
            return NotificationsConfig(enabled=coerce_bool(data.get("enabled"), False), channels=channels)
        except (json.JSONDecodeError, TypeError):
            logger.warning("notifications_config 解析失败，回落未启用")
            return NotificationsConfig()

    def get_notifications_config(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM system_settings WHERE id = 1").fetchone()
        raw = row["notifications_config"] if row and "notifications_config" in row.keys() else ""
        cfg = self._parse_notifications_config(raw)
        return {"enabled": cfg.enabled, "channels": cfg.channels}

    def update_notifications_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled = coerce_bool(payload.get("enabled"), False)
        channels = []
        for ch in payload.get("channels", []) or []:
            if not isinstance(ch, dict):
                continue
            ch_type = str(ch.get("type", "")).strip()
            if ch_type not in self._NOTIFICATION_CHANNEL_TYPES:
                continue
            channels.append({
                "type": ch_type,
                "name": str(ch.get("name", "") or "").strip()[:64],
                "enabled": coerce_bool(ch.get("enabled"), False),
                "url": str(ch.get("url", "") or "").strip()[:2048],
                "secret": str(ch.get("secret", "") or "").strip()[:512],
            })
        raw = json.dumps({"enabled": enabled, "channels": channels}, ensure_ascii=False)
        with self._connect() as connection:
            connection.execute(
                "UPDATE system_settings SET notifications_config = ?, updated_at = ? WHERE id = 1",
                (raw, utc_now()),
            )
        return self.get_notifications_config()

    def record_login_failure(self, ip: str, username: str) -> int:
        """记录一次登录失败；达到阈值即加锁。

        返回**本次失败后剩余的锁定秒数**（未达阈值/未锁定返回 0），
        便于 login 直接用作 429 的 Retry-After。
        """
        cfg = self.get_login_protection_config()
        max_attempts = max(1, int(cfg.get("max_attempts", 5) or 5))
        lockout_seconds = max(1, int(cfg.get("lockout_minutes", 15) or 15)) * 60
        now_ts = int(time.time())

        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM admin_login_attempts WHERE ip = ? AND username = ?",
                (ip, username),
            ).fetchone()

            if row is None:
                count = 1
                first_fail_at = now_ts
                prev_locked_until = 0
            elif int(row["locked_until"] or 0) and int(row["locked_until"]) < now_ts:
                # 上一次锁定已过期 → 从本次重新开始计数
                count = 1
                first_fail_at = now_ts
                prev_locked_until = 0
            else:
                count = int(row["fail_count"]) + 1
                first_fail_at = int(row["first_fail_at"] or now_ts)
                prev_locked_until = int(row["locked_until"] or 0)

            locked_until = prev_locked_until
            if count >= max_attempts:
                locked_until = now_ts + lockout_seconds

            if row is None:
                connection.execute(
                    "INSERT INTO admin_login_attempts "
                    "(ip, username, fail_count, first_fail_at, locked_until, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (ip, username, count, first_fail_at, locked_until, now_ts),
                )
            else:
                connection.execute(
                    "UPDATE admin_login_attempts "
                    "SET fail_count = ?, first_fail_at = ?, locked_until = ?, updated_at = ? "
                    "WHERE id = ?",
                    (count, first_fail_at, locked_until, now_ts, row["id"]),
                )

        return max(0, locked_until - now_ts) if locked_until > now_ts else 0

    def get_active_lock(self, ip: str, username: str) -> int:
        """返回当前剩余锁定秒数；未锁定返回 0。"""
        now_ts = int(time.time())
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM admin_login_attempts WHERE ip = ? AND username = ?",
                (ip, username),
            ).fetchone()
        if not row:
            return 0
        locked_until = int(row["locked_until"] or 0)
        if locked_until and locked_until > now_ts:
            return locked_until - now_ts
        return 0

    def clear_login_failures(self, ip: str, username: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM admin_login_attempts WHERE ip = ? AND username = ?",
                (ip, username),
            )

    # ===== 管理操作审计日志（P0-1.3） =====

    def insert_audit_log(
        self,
        actor_type: str,
        actor_id: str,
        action: str,
        target_type: str,
        target_id: str = "",
        detail: str = "",
    ) -> None:
        """写入一条管理操作审计记录。created_at 与 route_logs 同格式（UTC ISO 字符串）。"""
        created_at = utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO admin_audit_log
                    (actor_type, actor_id, action, target_type, target_id, detail, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(actor_type or "").strip(),
                    str(actor_id or "").strip(),
                    str(action or "").strip(),
                    str(target_type or "").strip(),
                    str(target_id or "").strip(),
                    str(detail or "").strip()[:2000],
                    created_at,
                ),
            )

    @staticmethod
    def serialize_audit_log(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "actor_type": row["actor_type"],
            "actor_id": row["actor_id"],
            "action": row["action"],
            "target_type": row["target_type"],
            "target_id": row["target_id"],
            "detail": row["detail"],
            "created_at": row["created_at"],
        }

    def list_audit_logs(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """审计日志分页查询（语义与 list_route_logs 保持一致）。"""
        filters = filters or {}
        clauses: List[str] = []
        params: List[Any] = []

        keyword = str(filters.get("keyword", "")).strip()
        if keyword:
            like_value = f"%{keyword}%"
            clauses.append(
                "("
                "actor_id LIKE ? OR action LIKE ? OR target_type LIKE ? OR "
                "target_id LIKE ? OR detail LIKE ?"
                ")"
            )
            params.extend([like_value] * 5)

        action = str(filters.get("action", "")).strip()
        if action:
            clauses.append("action = ?")
            params.append(action)

        target_type = str(filters.get("target_type", "")).strip()
        if target_type:
            clauses.append("target_type = ?")
            params.append(target_type)

        date_from = str(filters.get("date_from", "")).strip()
        if date_from:
            clauses.append("created_at >= ?")
            params.append(date_from)

        date_to = str(filters.get("date_to", "")).strip()
        if date_to:
            clauses.append("created_at <= ?")
            params.append(date_to)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit = max(1, min(500, int(filters.get("limit", 50) or 50)))
        page = max(1, int(filters.get("page", 1) or 1))
        offset = (page - 1) * limit

        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM admin_audit_log
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
            total = connection.execute(
                f"SELECT COUNT(*) FROM admin_audit_log {where_sql}",
                params,
            ).fetchone()[0]

        return {
            "items": [self.serialize_audit_log(row) for row in rows],
            "total": total,
            "limit": limit,
            "page": page,
            "offset": offset,
            "total_pages": max(1, (total + limit - 1) // limit),
            "filters": {
                "keyword": keyword,
                "action": action,
                "target_type": target_type,
                "date_from": date_from,
                "date_to": date_to,
            },
        }

    def generate_signed_url(self, path: str) -> str:
        """签出一个相对 URL。服务端持有 secret，绝不外泄。"""
        from signed_url import sign_url

        config = self.load_runtime_config()
        if not config.signed_url.secret:
            raise ValueError("签名密钥未初始化")
        return sign_url(path, config.signed_url.secret, config.signed_url.ttl_seconds)

    def get_redirect_signing_config(self) -> Dict[str, Any]:
        """302 加签改写配置。"""
        config = self.load_runtime_config().signed_redirect
        return {
            "enabled": config.enabled,
            "ttl_seconds": config.ttl_seconds,
            "bind_ip": config.bind_ip,
            "base_url": config.base_url,
        }

    def update_redirect_signing_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # 部分更新语义：未提供的字段保留现值
        current = self.load_runtime_config().signed_redirect
        enabled = coerce_bool(payload.get("enabled", current.enabled))
        ttl_seconds = max(60, int(payload.get("ttl_seconds", current.ttl_seconds) or current.ttl_seconds))
        bind_ip = coerce_bool(payload.get("bind_ip", current.bind_ip))
        base_url = str(payload.get("base_url", current.base_url) or "").strip().rstrip("/")
        now = utc_now()

        with self._connect() as connection:
            connection.execute(
                "UPDATE system_settings SET redirect_signing_enabled = ?, "
                "redirect_signing_ttl_seconds = ?, redirect_signing_bind_ip = ?, "
                "public_base_url = ?, updated_at = ? WHERE id = 1",
                (int(enabled), ttl_seconds, int(bind_ip), base_url, now),
            )
        return self.get_redirect_signing_config()

    def get_email_config(self) -> Dict[str, Any]:
        config = self.load_runtime_config()
        return {
            "enabled": config.email.enabled,
            "smtp_host": config.email.smtp_host,
            "smtp_port": config.email.smtp_port,
            "smtp_ssl": config.email.smtp_ssl,
            "sender": config.email.sender,
            "sender_name": config.email.sender_name,
            "password": config.email.password,
            "recipients": config.email.recipients,
            "block_link_base_url": config.email.block_link_base_url,
            "alert_window_seconds": config.email.alert_window_seconds,
            "alert_max_requests": config.email.alert_max_requests,
            "alert_max_404": config.email.alert_max_404,
            "alert_cooldown_minutes": config.email.alert_cooldown_minutes,
        }

    def update_email_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled = coerce_bool(payload.get("enabled", False))
        smtp_host = str(payload.get("smtp_host", "") or "")
        smtp_port = max(1, int(payload.get("smtp_port", 465) or 465))
        smtp_ssl = coerce_bool(payload.get("smtp_ssl", True))
        sender = str(payload.get("sender", "") or "")
        sender_name = str(payload.get("sender_name", "") or "")
        # 密码字段：如果payload中没有提供，则保持数据库中的原值
        password = str(payload.get("password", "") or "")
        recipients = str(payload.get("recipients", "") or "")
        block_link_base_url = str(payload.get("block_link_base_url", "") or "")
        alert_window_seconds = max(10, int(payload.get("alert_window_seconds", 60) or 60))
        alert_max_requests = max(1, int(payload.get("alert_max_requests", 80) or 80))
        alert_max_404 = max(1, int(payload.get("alert_max_404", 15) or 15))
        alert_cooldown_minutes = max(1, int(payload.get("alert_cooldown_minutes", 30) or 30))
        now = utc_now()
        
        # 如果密码为空，从数据库获取原密码
        if not password:
            config = self.load_runtime_config()
            password = config.email.password
        
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE system_settings
                SET email_enabled = ?, email_smtp_host = ?, email_smtp_port = ?,
                    email_smtp_ssl = ?, email_sender = ?, email_sender_name = ?,
                    email_password = ?, email_recipients = ?, email_block_link_base_url = ?,
                    email_alert_window_seconds = ?,
                    email_alert_max_requests = ?, email_alert_max_404 = ?, email_alert_cooldown_minutes = ?,
                    updated_at = ?
                WHERE id = 1
                """,
                (int(enabled), smtp_host, smtp_port, int(smtp_ssl), sender, sender_name, password, recipients,
                 block_link_base_url, alert_window_seconds, alert_max_requests, alert_max_404, alert_cooldown_minutes, now),
            )
        return self.get_email_config()

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
                access_ip_whitelist=str(payload.get("access_ip_whitelist", "") or "").strip(),
                ip_blacklist=str(payload.get("ip_blacklist", "") or "").strip(),
                region_whitelist=str(payload.get("region_whitelist", "") or "").strip(),
                region_blacklist=str(payload.get("region_blacklist", "") or "").strip(),
                rule_defaults=self._sanitize_group_rule_defaults(payload.get("rule_defaults") or {}),
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
                # 组默认：payload 未携带时保留现值（updateGroupRegionSwitch 等局部更新场景）
                new_defaults = (
                    self._sanitize_group_rule_defaults(payload["rule_defaults"])
                    if "rule_defaults" in payload and payload["rule_defaults"] is not None
                    else self._parse_group_rule_defaults(
                        existing["rule_defaults"] if "rule_defaults" in existing.keys() else "{}"
                    )
                )
                connection.execute(
                    """
                    UPDATE route_groups
                    SET request_host = ?, path_prefix = ?, region_matching_enabled = ?, notes = ?,
                        access_ip_whitelist = ?, ip_blacklist = ?, region_whitelist = ?, region_blacklist = ?,
                        rule_defaults = ?,
                        updated_at = ?
                    WHERE request_host = ? AND path_prefix = ?
                    """,
                    (
                        new_request_host,
                        new_path_prefix,
                        int(coerce_bool(payload.get("region_matching_enabled"), bool(existing["region_matching_enabled"]))),
                        str(payload.get("notes", existing["notes"])).strip(),
                        normalize_region_filter_value(str(payload.get("access_ip_whitelist", existing["access_ip_whitelist"] if "access_ip_whitelist" in existing.keys() else "") or "")),
                        normalize_region_filter_value(str(payload.get("ip_blacklist", existing["ip_blacklist"] if "ip_blacklist" in existing.keys() else "") or "")),
                        normalize_region_filter_value(str(payload.get("region_whitelist", existing["region_whitelist"] if "region_whitelist" in existing.keys() else "") or "")),
                        normalize_region_filter_value(str(payload.get("region_blacklist", existing["region_blacklist"] if "region_blacklist" in existing.keys() else "") or "")),
                        self._normalize_json_text(new_defaults),
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
                    access_ip_whitelist=str(payload.get("access_ip_whitelist", existing["access_ip_whitelist"] if "access_ip_whitelist" in existing.keys() else "") or ""),
                    ip_blacklist=str(payload.get("ip_blacklist", existing["ip_blacklist"] if "ip_blacklist" in existing.keys() else "") or ""),
                    region_whitelist=str(payload.get("region_whitelist", existing["region_whitelist"] if "region_whitelist" in existing.keys() else "") or ""),
                    region_blacklist=str(payload.get("region_blacklist", existing["region_blacklist"] if "region_blacklist" in existing.keys() else "") or ""),
                    rule_defaults=(
                        self._sanitize_group_rule_defaults(payload["rule_defaults"])
                        if "rule_defaults" in payload and payload["rule_defaults"] is not None
                        else None
                    ),
                )

        return self.get_route_group(new_path_prefix, new_request_host)

    def convert_matching_rules_to_inherit(
        self,
        path_prefix: str,
        request_host: str = "",
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """把组内「显式值 == 组级默认」的规则字段转为继承（P2-4.1 存量迁移）。

        仅处理 fields ∩ 组已设置的默认键；已是继承的字段跳过。
        数值/开关写 -1 哨兵；字符串写空串（继承语义）。
        返回转换明细供后台确认弹窗统计展示。
        """
        path_prefix = str(path_prefix).strip()
        normalized_host = normalize_request_host(request_host)
        if not path_prefix:
            raise ValueError("path_prefix is required.")
        wanted = [f for f in (fields or list(GROUP_RULE_DEFAULT_FIELDS)) if f in GROUP_RULE_DEFAULT_FIELDS]
        with self._connect() as connection:
            group_row = connection.execute(
                "SELECT * FROM route_groups WHERE request_host = ? AND path_prefix = ?",
                (normalized_host, path_prefix),
            ).fetchone()
            if not group_row:
                raise KeyError(f"Route group {normalized_host or '*'} {path_prefix} not found")
            defaults = self._parse_group_rule_defaults(
                group_row["rule_defaults"] if "rule_defaults" in group_row.keys() else "{}"
            )
            if not defaults:
                return {"converted_rules": 0, "converted_fields": 0, "details": []}
            now = utc_now()
            converted_rules = 0
            converted_fields = 0
            details: List[Dict[str, Any]] = []
            rule_rows = connection.execute(
                "SELECT * FROM forward_rules WHERE request_host = ? AND path_prefix = ?",
                (normalized_host, path_prefix),
            ).fetchall()
            for row in rule_rows:
                rule = self._row_to_rule(row)
                inherit = rule.inherit_set()
                updates: Dict[str, Any] = {}
                touched: List[str] = []
                for field in wanted:
                    if field not in defaults or field in inherit:
                        continue
                    kind = GROUP_RULE_DEFAULT_FIELDS[field]
                    current = getattr(rule, field)
                    default_val = defaults[field]
                    if kind == "int":
                        try:
                            matched = current is not None and int(current) == int(default_val)
                        except (TypeError, ValueError):
                            matched = False
                        if matched:
                            updates[field] = RULE_INHERIT_SENTINEL_INT
                    elif kind == "bool":
                        if bool(current) == bool(default_val):
                            updates[field] = RULE_INHERIT_SENTINEL_INT
                    else:
                        if normalize_region_filter_value(current) == str(default_val).strip():
                            updates[field] = ""
                    if field in updates:
                        touched.append(field)
                if updates:
                    set_clause = ", ".join(f"{k} = ?" for k in updates)
                    connection.execute(
                        f"UPDATE forward_rules SET {set_clause}, updated_at = ? WHERE id = ?",
                        (*updates.values(), now, rule.rule_id),
                    )
                    converted_rules += 1
                    converted_fields += len(touched)
                    details.append({"rule_id": rule.rule_id, "rule_name": rule.name, "fields": touched})
            return {
                "converted_rules": converted_rules,
                "converted_fields": converted_fields,
                "details": details,
            }

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
        return self._serialize_rule_resolved(row)

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
                access_ip_whitelist=payload.get("group_access_ip_whitelist"),
                ip_blacklist=payload.get("group_ip_blacklist"),
                region_whitelist=payload.get("group_region_whitelist"),
                region_blacklist=payload.get("group_region_blacklist"),
            )
            if rule.is_default:
                self._clear_existing_default_in_group(
                    connection, rule.path_prefix, rule.request_host
                )
            rule_id = self._insert_rule(connection, rule, source=source)
            row = connection.execute("SELECT * FROM forward_rules WHERE id = ?", (rule_id,)).fetchone()
        return self._serialize_rule_resolved(row)

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
                    priority = ?, notes = ?, path_rewrite_pattern = ?, path_rewrite_replacement = ?,
                    access_ip_whitelist = ?, ip_blacklist = ?, region_whitelist = ?, region_blacklist = ?,
                    referer_whitelist = ?, referer_policy = ?, ua_blacklist = ?, ua_whitelist = ?,
                    target_urls = ?, health_check_enabled = ?, health_check_path = ?, health_check_interval = ?, health_check_timeout = ?,
                    cors_origins = ?,
                    inject_request_headers = ?, upstream_verify_ssl = ?, client_cert = ?, client_key = ?,
                    updated_at = ?
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
                    rule.path_rewrite_pattern or "",
                    rule.path_rewrite_replacement or "",
                    normalize_region_filter_value(rule.access_ip_whitelist),
                    normalize_region_filter_value(rule.ip_blacklist),
                    normalize_region_filter_value(rule.region_whitelist),
                    normalize_region_filter_value(rule.region_blacklist),
                    normalize_region_filter_value(rule.referer_whitelist),
                    rule.stored_referer_policy(),
                    normalize_region_filter_value(rule.ua_blacklist),
                    normalize_region_filter_value(rule.ua_whitelist),
                    rule.target_urls or "",
                    int(rule.health_check_enabled),
                    rule.health_check_path or "",
                    rule.health_check_interval,
                    rule.health_check_timeout,
                    rule.cors_origins or "",
                    rule.inject_request_headers or "",
                    int(rule.upstream_verify_ssl),
                    rule.client_cert or "",
                    rule.client_key or "",
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
                access_ip_whitelist=payload.get("group_access_ip_whitelist"),
                ip_blacklist=payload.get("group_ip_blacklist"),
                region_whitelist=payload.get("group_region_whitelist"),
                region_blacklist=payload.get("group_region_blacklist"),
            )
            if rule.is_default:
                self._clear_existing_default_in_group(
                    connection, rule.path_prefix, rule.request_host, exclude_rule_id=rule_id
                )
            if old_path_prefix != rule.path_prefix or old_request_host != normalize_request_host(rule.request_host):
                self._cleanup_orphan_route_group(connection, old_path_prefix, old_request_host)
            row = connection.execute("SELECT * FROM forward_rules WHERE id = ?", (rule_id,)).fetchone()
        return self._serialize_rule_resolved(row)

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
                "query_params_json": source_row["query_params_json"] if "query_params_json" in source_row.keys() else "{}",
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
            refresh_interval_hours=row["offline_refresh_interval_hours"] if "offline_refresh_interval_hours" in row.keys() else 24,
            last_sync_at=row["offline_last_sync_at"] if "offline_last_sync_at" in row.keys() else "",
            last_success_at=row["offline_last_success_at"] if "offline_last_success_at" in row.keys() else "",
            last_sync_status=row["offline_last_sync_status"] if "offline_last_sync_status" in row.keys() else "",
            last_sync_message=row["offline_last_sync_message"] if "offline_last_sync_message" in row.keys() else "",
        )
        return {
            "enabled": bool(row["enabled"]),
            "online_cache_ttl_seconds": max(0, int(row["online_cache_ttl_seconds"] or 0)) if "online_cache_ttl_seconds" in row.keys() else 120,
            "sources": sources,
            "offline": {
                "enabled": bool(row["offline_enabled"]),
                "db_path": row["offline_db_path"],
                "locale": row["offline_locale"],
                "download_url": row["offline_download_url"] if "offline_download_url" in row.keys() else "",
                "download_headers_json": row["offline_download_headers_json"] if "offline_download_headers_json" in row.keys() else "{}",
                "refresh_interval_hours": row["offline_refresh_interval_hours"] if "offline_refresh_interval_hours" in row.keys() else 24,
                "last_sync_at": row["offline_last_sync_at"] if "offline_last_sync_at" in row.keys() else "",
                "last_sync_status": row["offline_last_sync_status"] if "offline_last_sync_status" in row.keys() else "",
                "last_sync_message": row["offline_last_sync_message"] if "offline_last_sync_message" in row.keys() else "",
                "last_success_at": row["offline_last_success_at"] if "offline_last_success_at" in row.keys() else "",
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
            "path_rewrite_pattern": rule.path_rewrite_pattern,
            "path_rewrite_replacement": rule.path_rewrite_replacement,
            "access_ip_whitelist": rule.access_ip_whitelist,
            "ip_blacklist": rule.ip_blacklist,
            "region_whitelist": rule.region_whitelist,
            "region_blacklist": rule.region_blacklist,
            # Referer 防盗链（HOTLINK_PROTECTION.md 阶段 2）
            "referer_whitelist": rule.referer_whitelist,
            "referer_policy": rule.normalized_referer_policy(),
            # UA 黑名单（HOTLINK_PROTECTION.md 阶段 3.1）
            "ua_blacklist": rule.ua_blacklist,
            "ua_whitelist": rule.ua_whitelist,
            # 多上游 + 健康检查（P1-2.2）
            "target_urls": rule.target_urls,
            "health_check_enabled": rule.health_check_enabled,
            "health_check_path": rule.health_check_path,
            "health_check_interval": rule.health_check_interval,
            "health_check_timeout": rule.health_check_timeout,
            # CORS 规则级覆盖（P2-3.2）
            "cors_origins": rule.cors_origins,
            "inject_request_headers": rule.inject_request_headers,
            "upstream_verify_ssl": rule.upstream_verify_ssl,
            "client_cert": rule.client_cert,
            "client_key": rule.client_key,
            # 组级继承（P2-4.1）：标记「继承组默认」的字段名列表；
            # 其余字段输出的是有效值（组默认已合入），供占位提示与抽屉展示
            "inherit_fields": [f for f in rule.inherit_set() if f in GROUP_RULE_DEFAULT_FIELDS],
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
            "access_ip_whitelist": group.access_ip_whitelist,
            "ip_blacklist": group.ip_blacklist,
            "region_whitelist": group.region_whitelist,
            "region_blacklist": group.region_blacklist,
            # 组级规则默认配置（P2-4.1 规则继承）：键 ∈ GROUP_RULE_DEFAULT_FIELDS
            "rule_defaults": group.rule_defaults or {},
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
            "request_host": row["request_host"] if "request_host" in row.keys() else "",
            "path_prefix": row["path_prefix"],
            "rule_id": row["rule_id"],
            "rule_name": row["rule_name"],
            "rule_request_host": row["rule_request_host"] if "rule_request_host" in row.keys() else "",
            "rule_source": row["rule_source"],
            "target_url": row["target_url"],
            "redirect_location": row["redirect_location"] if "redirect_location" in row.keys() else "",
            "original_client_ip": row["original_client_ip"] if "original_client_ip" in row.keys() else "",
            "client_ip": row["client_ip"],
            "region_matching_enabled": bool(row["region_matching_enabled"]),
            "geo_source": row["geo_source"],
            "geo_summary": row["geo_summary"],
            "geo_country": row["geo_country"],
            "geo_region": row["geo_region"],
            "geo_city": row["geo_city"],
            "configured_ip_whitelist": row["configured_ip_whitelist"] if "configured_ip_whitelist" in row.keys() else "",
            "matched_ip_whitelist": row["matched_ip_whitelist"] if "matched_ip_whitelist" in row.keys() else "",
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
            # 盗链监控字段（HOTLINK_PROTECTION.md 阶段 1），用 row.keys() 守卫以兼容
            # 尚未运行迁移 017 的库（理论上 _run_migrations 已补齐，此处双保险）。
            "referer": row["referer"] if "referer" in row.keys() else "",
            "user_agent": row["user_agent"] if "user_agent" in row.keys() else "",
            "bytes_transferred": int(row["bytes_transferred"] or 0) if "bytes_transferred" in row.keys() else 0,
            # 请求链路节点（迁移 025）：「签名重入:通过 → 签名重入:缓存命中(内部代理) → 代理:200」
            "chain": row["chain"] if "chain" in row.keys() else "",
            "created_at": row["created_at"],
        }

    def _row_to_rule(self, row: sqlite3.Row) -> ProxyRule:
        # 组级继承哨兵检测（P2-4.1）：数值/开关列 -1 = 继承组默认；
        # 字符串列（referer/ua）空值天然是「继承组」语义（组也没配 = 不启用）。
        inherit = []
        timeout_raw = int(row["timeout"]) if str(row["timeout"] or "").lstrip("-").isdigit() else 30
        max_redirects_raw = int(row["max_redirects"]) if str(row["max_redirects"] or "").lstrip("-").isdigit() else 10
        retry_raw = int(row["retry_times"]) if str(row["retry_times"] or "").lstrip("-").isdigit() else 3
        follow_raw = int(row["follow_redirects"]) if "follow_redirects" in row.keys() else 1
        streaming_raw = int(row["enable_streaming"]) if "enable_streaming" in row.keys() else 1
        strip_raw = int(row["strip_prefix"]) if "strip_prefix" in row.keys() else 0
        if timeout_raw == RULE_INHERIT_SENTINEL_INT:
            inherit.append("timeout")
        if max_redirects_raw == RULE_INHERIT_SENTINEL_INT:
            inherit.append("max_redirects")
        if retry_raw == RULE_INHERIT_SENTINEL_INT:
            inherit.append("retry_times")
        if follow_raw == RULE_INHERIT_SENTINEL_INT:
            inherit.append("follow_redirects")
        if streaming_raw == RULE_INHERIT_SENTINEL_INT:
            inherit.append("enable_streaming")
        if strip_raw == RULE_INHERIT_SENTINEL_INT:
            inherit.append("strip_prefix")
        # 字符串列：空值 = 继承组默认（组也未配置时等价于「不启用」）；
        # "-"（显式停用哨兵）不算继承。referer_policy 空值同理。
        referer_raw = normalize_region_filter_value(row["referer_whitelist"] if "referer_whitelist" in row.keys() else "")
        referer_policy_raw = str(row["referer_policy"] if "referer_policy" in row.keys() else "allow").strip().lower()
        ua_black_raw = normalize_region_filter_value(row["ua_blacklist"] if "ua_blacklist" in row.keys() else "")
        ua_white_raw = normalize_region_filter_value(row["ua_whitelist"] if "ua_whitelist" in row.keys() else "")
        if not referer_raw and referer_raw != RULE_EXPLICIT_OFF_SENTINEL:
            inherit.append("referer_whitelist")
        if referer_policy_raw not in ("allow", "deny"):
            inherit.append("referer_policy")
        if not ua_black_raw and ua_black_raw != RULE_EXPLICIT_OFF_SENTINEL:
            inherit.append("ua_blacklist")
        if not ua_white_raw and ua_white_raw != RULE_EXPLICIT_OFF_SENTINEL:
            inherit.append("ua_whitelist")
        return ProxyRule(
            rule_id=row["id"],
            source=row["source"],
            external_id=row["external_id"],
            name=row["name"],
            request_host=normalize_request_host(row["request_host"]),
            path_prefix=row["path_prefix"],
            target_url=row["target_url"],
            strip_prefix=bool(strip_raw) if strip_raw != RULE_INHERIT_SENTINEL_INT else RULE_INHERIT_SENTINEL_INT,
            timeout=timeout_raw if timeout_raw != RULE_INHERIT_SENTINEL_INT else RULE_INHERIT_SENTINEL_INT,
            max_redirects=max_redirects_raw if max_redirects_raw != RULE_INHERIT_SENTINEL_INT else RULE_INHERIT_SENTINEL_INT,
            follow_redirects=bool(follow_raw) if follow_raw != RULE_INHERIT_SENTINEL_INT else RULE_INHERIT_SENTINEL_INT,
            retry_times=retry_raw if retry_raw != RULE_INHERIT_SENTINEL_INT else RULE_INHERIT_SENTINEL_INT,
            enable_streaming=bool(streaming_raw) if streaming_raw != RULE_INHERIT_SENTINEL_INT else RULE_INHERIT_SENTINEL_INT,
            ip_whitelist=row["ip_whitelist"],
            region_filters=row["region_filters"],
            is_default=bool(row["is_default"]),
            enabled=bool(row["enabled"]),
            priority=row["priority"],
            notes=row["notes"],
            path_rewrite_pattern=row["path_rewrite_pattern"] if "path_rewrite_pattern" in row.keys() else "",
            path_rewrite_replacement=row["path_rewrite_replacement"] if "path_rewrite_replacement" in row.keys() else "",
            access_ip_whitelist=row["access_ip_whitelist"] if "access_ip_whitelist" in row.keys() else "",
            ip_blacklist=row["ip_blacklist"] if "ip_blacklist" in row.keys() else "",
            region_whitelist=row["region_whitelist"] if "region_whitelist" in row.keys() else "",
            region_blacklist=row["region_blacklist"] if "region_blacklist" in row.keys() else "",
            # Referer 防盗链（018 列，row.keys() 守卫兼容未迁移的库）；
            # 空值 = 继承组默认（组也未配置时行为同「未启用」）
            referer_whitelist=row["referer_whitelist"] if "referer_whitelist" in row.keys() else "",
            referer_policy=row["referer_policy"] if "referer_policy" in row.keys() else "allow",
            # UA 黑名单（019 列）
            ua_blacklist=row["ua_blacklist"] if "ua_blacklist" in row.keys() else "",
            ua_whitelist=row["ua_whitelist"] if "ua_whitelist" in row.keys() else "",
            # 多上游 + 健康检查（P1-2.2），row.keys() 守卫兼容未迁移的库
            target_urls=row["target_urls"] if "target_urls" in row.keys() else "",
            health_check_enabled=bool(row["health_check_enabled"]) if "health_check_enabled" in row.keys() else False,
            health_check_path=row["health_check_path"] if "health_check_path" in row.keys() else "",
            health_check_interval=int(row["health_check_interval"]) if "health_check_interval" in row.keys() else 30,
            health_check_timeout=int(row["health_check_timeout"]) if "health_check_timeout" in row.keys() else 5,
            cors_origins=row["cors_origins"] if "cors_origins" in row.keys() else "",
            inject_request_headers=row["inject_request_headers"] if "inject_request_headers" in row.keys() else "",
            upstream_verify_ssl=int(row["upstream_verify_ssl"]) if "upstream_verify_ssl" in row.keys() else -1,
            client_cert=row["client_cert"] if "client_cert" in row.keys() else "",
            client_key=row["client_key"] if "client_key" in row.keys() else "",
            inherit_fields=",".join(inherit),
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

        # 组级继承（P2-4.1）：inherit_fields 标记「继承组默认」的字段（列表或逗号串）。
        # 数值/开关：标记即写 -1 哨兵；字符串（referer/ua）：空串本身就是继承哨兵，
        # 标记 referer_policy 时写空串。标记优先于 payload 携带的值 —— update_rule
        # 内部合并的是「已解析有效值 + inherit_fields」，若让值反过来清掉标记，
        # toggle 等局部更新会把继承字段悄悄转成显式值（往返一致性）。
        raw_inherit = payload.get("inherit_fields") or []
        if isinstance(raw_inherit, str):
            inherit_marks = {part.strip() for part in raw_inherit.split(",") if part.strip()}
        else:
            inherit_marks = {str(part).strip() for part in raw_inherit if str(part).strip()}
        inherit_marks &= set(GROUP_RULE_DEFAULT_FIELDS)

        timeout_inherit = "timeout" in inherit_marks
        max_redirects_inherit = "max_redirects" in inherit_marks
        retry_inherit = "retry_times" in inherit_marks
        follow_inherit = "follow_redirects" in inherit_marks
        streaming_inherit = "enable_streaming" in inherit_marks
        strip_inherit = "strip_prefix" in inherit_marks
        raw_referer_policy = str(payload.get("referer_policy", "") or "").strip().lower()

        return ProxyRule(
            rule_id=payload.get("id"),
            external_id=payload.get("external_id"),
            name=str(payload.get("name", "")).strip(),
            request_host=normalize_request_host(payload.get("request_host", "")),
            path_prefix=path_prefix,
            target_url=target_url,
            strip_prefix=RULE_INHERIT_SENTINEL_INT if strip_inherit else coerce_bool(payload.get("strip_prefix"), False),
            timeout=RULE_INHERIT_SENTINEL_INT if timeout_inherit else int(payload.get("timeout", 30) or 30),
            max_redirects=RULE_INHERIT_SENTINEL_INT if max_redirects_inherit else int(payload.get("max_redirects", 10) or 10),
            follow_redirects=RULE_INHERIT_SENTINEL_INT if follow_inherit else coerce_bool(payload.get("follow_redirects"), True),
            retry_times=RULE_INHERIT_SENTINEL_INT if retry_inherit else int(payload.get("retry_times", 3) or 3),
            enable_streaming=RULE_INHERIT_SENTINEL_INT if streaming_inherit else coerce_bool(payload.get("enable_streaming"), True),
            ip_whitelist=normalize_region_filter_value(payload.get("ip_whitelist", "")),
            region_filters=normalize_region_filter_value(payload.get("region_filters", "")),
            is_default=coerce_bool(payload.get("is_default"), False),
            enabled=coerce_bool(payload.get("enabled"), True),
            priority=int(payload.get("priority", 0) or 0),
            notes=str(payload.get("notes", "")).strip(),
            source=str(payload.get("source", "manual")).strip() or "manual",
            path_rewrite_pattern=str(payload.get("path_rewrite_pattern", "") or "").strip(),
            path_rewrite_replacement=str(payload.get("path_rewrite_replacement", "") or "").strip(),
            access_ip_whitelist=normalize_region_filter_value(payload.get("access_ip_whitelist", "")),
            ip_blacklist=normalize_region_filter_value(payload.get("ip_blacklist", "")),
            region_whitelist=normalize_region_filter_value(payload.get("region_whitelist", "")),
            region_blacklist=normalize_region_filter_value(payload.get("region_blacklist", "")),
            # Referer 防盗链：白名单走统一归一化；policy 非法值回退 allow。
            # 空串 = 继承组默认（组也未设置时运行时 normalized_referer_policy 回落 allow）。
            # 标记继承的字符串字段一律写空串哨兵，不落 payload 携带的有效值
            # （组默认变更后规则才能跟随）
            referer_whitelist=("" if "referer_whitelist" in inherit_marks else normalize_region_filter_value(payload.get("referer_whitelist", ""))),
            referer_policy=(
                ""
                if "referer_policy" in inherit_marks
                else (raw_referer_policy if raw_referer_policy in ("allow", "deny") else "")
            ),
            ua_blacklist=("" if "ua_blacklist" in inherit_marks else normalize_region_filter_value(payload.get("ua_blacklist", ""))),
            ua_whitelist=("" if "ua_whitelist" in inherit_marks else normalize_region_filter_value(payload.get("ua_whitelist", ""))),
            target_urls=str(payload.get("target_urls", "") or "").strip(),
            health_check_enabled=coerce_bool(payload.get("health_check_enabled"), False),
            health_check_path=str(payload.get("health_check_path", "") or "").strip(),
            health_check_interval=int(payload.get("health_check_interval", 30) or 30),
            health_check_timeout=int(payload.get("health_check_timeout", 5) or 5),
            cors_origins=str(payload.get("cors_origins", "") or "").strip(),
            inject_request_headers=str(payload.get("inject_request_headers", "") or "").strip(),
            upstream_verify_ssl=max(-1, min(1, int(payload.get("upstream_verify_ssl", -1) if payload.get("upstream_verify_ssl", -1) is not None else -1))),
            client_cert=str(payload.get("client_cert", "") or "").strip(),
            client_key=str(payload.get("client_key", "") or "").strip(),
            inherit_fields=",".join(sorted(inherit_marks)),
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

    def create_block_token(self, ip: str, reason: str, expires_in_seconds: int = 1800) -> str:
        """创建封禁链接token，返回token字符串"""
        import secrets
        token = secrets.token_urlsafe(32)
        now = time.time()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """INSERT INTO email_block_tokens (token, ip, reason, created_at, expires_at, used)
                       VALUES (?, ?, ?, ?, ?, 0)""",
                    (token, ip, reason, now, now + expires_in_seconds),
                )
                conn.commit()
            finally:
                conn.close()
        return token

    def validate_block_token(self, token: str) -> Optional[Dict[str, Any]]:
        """验证token，返回有效信息或None（无效/过期/已使用）"""
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT token, ip, reason, created_at, expires_at, used FROM email_block_tokens WHERE token = ?",
                    (token,),
                ).fetchone()
                if not row:
                    return None
                if row["used"]:
                    return None
                if time.time() > row["expires_at"]:
                    return None
                return {
                    "token": row["token"],
                    "ip": row["ip"],
                    "reason": row["reason"],
                    "created_at": row["created_at"],
                    "expires_at": row["expires_at"],
                }
            finally:
                conn.close()

    def use_block_token(self, token: str) -> bool:
        """标记token为已使用，返回是否成功"""
        with self._lock:
            conn = self._connect()
            try:
                result = conn.execute(
                    "UPDATE email_block_tokens SET used = 1 WHERE token = ? AND used = 0",
                    (token,),
                )
                conn.commit()
                return result.rowcount > 0
            finally:
                conn.close()
