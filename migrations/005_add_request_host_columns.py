"""Add forward_rules request_host columns"""

from yoyo import step


def ensure_column(connection, table, column, definition):
    cursor = connection.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def add(connection):
    ensure_column(connection, "forward_rules", "request_host", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "forward_rules", "follow_redirects", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(connection, "forward_rules", "ip_whitelist", "TEXT NOT NULL DEFAULT ''")
    try:
        connection.execute("CREATE INDEX IF NOT EXISTS idx_forward_rules_request_host ON forward_rules(request_host)")
    except Exception:
        pass


def rollback(connection):
    pass


step(add, rollback)
