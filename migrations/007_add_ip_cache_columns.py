"""Add system_settings IP cache columns"""

from yoyo import step


def ensure_column(connection, table, column, definition):
    cursor = connection.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def add(connection):
    ensure_column(connection, "system_settings", "ip_cache_enabled", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(connection, "system_settings", "ip_cache_ttl_seconds", "INTEGER NOT NULL DEFAULT 300")
    ensure_column(connection, "system_settings", "ip_cache_max_entries", "INTEGER NOT NULL DEFAULT 5000")


def rollback(connection):
    pass


step(add, rollback)
