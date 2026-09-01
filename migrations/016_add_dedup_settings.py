"""Add request deduplication settings to system_settings table"""

from yoyo import step


def add(connection):
    def _has_column(cursor, table, column):
        try:
            cursor.execute(f"PRAGMA table_info({table})")
            return any(row[1] == column for row in cursor.fetchall())
        except Exception:
            return False

    def _ensure_column(cursor, table, column, definition):
        if not _has_column(cursor, table, column):
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    _ensure_column(connection, "system_settings", "dedup_enabled", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "system_settings", "dedup_window_seconds", "REAL NOT NULL DEFAULT 2.0")
    _ensure_column(connection, "system_settings", "dedup_max_cache_entries", "INTEGER NOT NULL DEFAULT 10000")


def rollback(connection):
    pass


step(add, rollback)
