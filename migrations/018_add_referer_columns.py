"""Add Referer hotlink protection columns to forward_rules table.

Phase 2 of HOTLINK_PROTECTION.md: rule-level Referer whitelist and
empty-Referer policy. Empty whitelist means the check is disabled for that
rule (backward compatible default).
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

    _ensure_column(connection, "forward_rules", "referer_whitelist", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "forward_rules", "referer_policy", "TEXT NOT NULL DEFAULT 'allow'")


def rollback(connection):
    pass


step(add, rollback)
