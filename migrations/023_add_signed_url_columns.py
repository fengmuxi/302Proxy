"""Add signed-URL columns to system_settings table.

Phase 4 of HOTLINK_PROTECTION.md: time-limited HMAC-signed proxy URLs,
gated by a global on/off switch. Three columns:
- signed_url_enabled: master switch (0/1, default off)
- signed_url_secret: 32-hex HMAC key (auto-generated on first use)
- signed_url_ttl_seconds: validity window for issued links (default 3600)
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

    _ensure_column(connection, "system_settings", "signed_url_enabled", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "system_settings", "signed_url_secret", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "system_settings", "signed_url_ttl_seconds", "INTEGER NOT NULL DEFAULT 3600")


def rollback(connection):
    pass


step(add, rollback)
