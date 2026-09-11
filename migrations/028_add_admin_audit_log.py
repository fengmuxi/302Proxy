"""Add admin operation audit log.

Every mutating admin action (rule/ban/key/settings/backup changes, email
test, signed-url generation, login) is recorded with the actor (admin
session username or API key name), action, target and a short detail, so
operators can trace who changed what from the 「日志与审计 → 审计日志」 page.
"""

from yoyo import step


def add(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_type TEXT NOT NULL DEFAULT '',
            actor_id TEXT NOT NULL DEFAULT '',
            action TEXT NOT NULL DEFAULT '',
            target_type TEXT NOT NULL DEFAULT '',
            target_id TEXT NOT NULL DEFAULT '',
            detail TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_created ON admin_audit_log(created_at)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_target ON admin_audit_log(target_type, target_id)"
    )


def rollback(connection):
    connection.execute("DROP TABLE IF EXISTS admin_audit_log")
