"""Add login brute-force protection for the admin console.

Tracks failed login attempts per (ip, username) and locks further attempts
for a configurable window once the threshold is exceeded. Login protection
settings live in `system_settings` (editable from the admin UI), not in the
yaml bootstrap config, consistent with other security toggles.
"""

from yoyo import step


def add(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_login_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip TEXT NOT NULL DEFAULT '',
            username TEXT NOT NULL DEFAULT '',
            fail_count INTEGER NOT NULL DEFAULT 0,
            first_fail_at INTEGER NOT NULL DEFAULT 0,
            locked_until INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_login_attempts_ip_user "
        "ON admin_login_attempts(ip, username)"
    )
    # 登录保护开关（system_settings 列 login_max_attempts / login_lockout_minutes /
    # login_cooldown_seconds）不在本迁移里 ALTER：它们同时由 config_store 的
    # 基表 schema（新库）与 LEGACY_SYSTEM_SETTINGS_COLUMNS（旧库）保证存在，
    # 在此重复 ALTER 会对已含该列的表报 duplicate column name。


def rollback(connection):
    connection.execute("DROP TABLE IF EXISTS admin_login_attempts")
    # SQLite 不支持 DROP COLUMN（< 3.35），保留列即可，不影响功能
