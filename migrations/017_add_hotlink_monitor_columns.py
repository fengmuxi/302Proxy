"""Add hotlink monitoring columns to route_logs table.

These columns support phase-1 monitoring (see HOTLINK_PROTECTION.md):
referer / user_agent are recorded per request so external hotlinking can be
spotted, and bytes_transferred lets us rank the heaviest consumers.
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

    _ensure_column(connection, "route_logs", "referer", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "route_logs", "user_agent", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "route_logs", "bytes_transferred", "INTEGER NOT NULL DEFAULT 0")


def rollback(connection):
    pass


step(add, rollback)
