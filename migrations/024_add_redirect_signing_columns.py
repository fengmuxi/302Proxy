"""Add signed-redirect-rewrite columns to system_settings table.

SIGNED_REDIRECT_PLAN.md v2: rewrite outbound 3xx Location into a system-owned
signed link `{base_url}/_signed/{resource_id}?_st&_sig`. Four columns:
- redirect_signing_enabled: master switch (0/1, default off)
- redirect_signing_ttl_seconds: signed-link validity (default 21600 = 6h)
- redirect_signing_bind_ip: require same client IP as issuance (default on)
- public_base_url: externally reachable base URL for link assembly (empty = Host header)
"""

from yoyo import step


def add(connection):
    def _has_column(cursor, table, column):
        try:
            rows = cursor.execute(f"PRAGMA table_info({table})").fetchall()
            return any(row[1] == column for row in rows)
        except Exception:
            return False

    def _ensure_column(cursor, table, column, definition):
        if not _has_column(cursor, table, column):
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    _ensure_column(connection, "system_settings", "redirect_signing_enabled", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "system_settings", "redirect_signing_ttl_seconds", "INTEGER NOT NULL DEFAULT 21600")
    _ensure_column(connection, "system_settings", "redirect_signing_bind_ip", "INTEGER NOT NULL DEFAULT 1")
    _ensure_column(connection, "system_settings", "public_base_url", "TEXT NOT NULL DEFAULT ''")


def rollback(connection):
    pass


step(add, rollback)
