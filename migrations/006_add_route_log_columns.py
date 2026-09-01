"""Add route_logs columns"""

from yoyo import step


def ensure_column(connection, table, column, definition):
    cursor = connection.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def add(connection):
    ensure_column(connection, "route_logs", "redirect_location", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "route_logs", "original_client_ip", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "route_logs", "configured_ip_whitelist", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "route_logs", "matched_ip_whitelist", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "route_logs", "request_host", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "route_logs", "rule_request_host", "TEXT NOT NULL DEFAULT ''")
    try:
        connection.execute("CREATE INDEX IF NOT EXISTS idx_route_logs_request_host ON route_logs(request_host)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_route_logs_rule_request_host ON route_logs(rule_request_host)")
    except Exception:
        pass
    connection.execute(
        "INSERT OR IGNORE INTO route_log_settings (id, retention_days, last_pruned_at, updated_at) VALUES (1, 30, NULL, datetime('now'))"
    )


def rollback(connection):
    pass


step(add, rollback)
