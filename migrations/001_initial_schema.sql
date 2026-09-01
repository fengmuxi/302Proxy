-- Initial schema (extracted from config_store.py)
-- Contains full table definitions as migration baseline

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

CREATE TABLE IF NOT EXISTS email_block_tokens (
    token TEXT PRIMARY KEY,
    ip TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    used INTEGER NOT NULL DEFAULT 0
);
