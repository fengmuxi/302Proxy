"""Add per-IP streaming concurrency limit column to system_settings table.

Phase 3.2 of HOTLINK_PROTECTION.md: streaming_max_concurrent_per_ip caps the
number of concurrent streaming responses per client IP; 0 disables the limit.
"""

from yoyo import step


def add(connection):
    def _has_column(cursor, table, column):
        try:
            # 注意必须取 execute 返回的游标再 fetchall：Connection 自身没有 fetchall，
            # 若写成 cursor.fetchall() 会 AttributeError 被吞掉、恒返回 False（016 的潜伏坑）
            rows = cursor.execute(f"PRAGMA table_info({table})").fetchall()
            return any(row[1] == column for row in rows)
        except Exception:
            return False

    def _ensure_column(cursor, table, column, definition):
        if not _has_column(cursor, table, column):
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    _ensure_column(connection, "system_settings", "streaming_max_concurrent_per_ip", "INTEGER NOT NULL DEFAULT 0")


def rollback(connection):
    pass


step(add, rollback)
