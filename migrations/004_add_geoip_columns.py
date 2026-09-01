"""Add geoip related columns"""

from yoyo import step


def ensure_column(connection, table, column, definition):
    cursor = connection.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def add(connection):
    ensure_column(connection, "geoip_online_sources", "query_params_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_column(connection, "geoip_settings", "online_cache_ttl_seconds", "INTEGER NOT NULL DEFAULT 120")
    ensure_column(connection, "geoip_settings", "offline_download_url", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "geoip_settings", "offline_download_headers_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_column(connection, "geoip_settings", "offline_refresh_interval_hours", "INTEGER NOT NULL DEFAULT 24")
    ensure_column(connection, "geoip_settings", "offline_last_sync_at", "TEXT")
    ensure_column(connection, "geoip_settings", "offline_last_sync_status", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "geoip_settings", "offline_last_sync_message", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "geoip_settings", "offline_last_success_at", "TEXT")


def rollback(connection):
    pass


step(add, rollback)
