"""Add system_settings auto_ban and email columns"""

from yoyo import step


def ensure_column(connection, table, column, definition):
    cursor = connection.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def add(connection):
    ensure_column(connection, "system_settings", "auto_ban_enabled", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(connection, "system_settings", "auto_ban_window_seconds", "INTEGER NOT NULL DEFAULT 60")
    ensure_column(connection, "system_settings", "auto_ban_max_requests", "INTEGER NOT NULL DEFAULT 100")
    ensure_column(connection, "system_settings", "auto_ban_ban_duration_seconds", "INTEGER NOT NULL DEFAULT 3600")
    ensure_column(connection, "system_settings", "auto_ban_max_404", "INTEGER NOT NULL DEFAULT 20")
    ensure_column(connection, "system_settings", "auto_ban_auto_ban_on_404", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(connection, "system_settings", "auto_ban_whitelist", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "system_settings", "auto_ban_email_on_ban", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(connection, "system_settings", "email_enabled", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(connection, "system_settings", "email_smtp_host", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "system_settings", "email_smtp_port", "INTEGER NOT NULL DEFAULT 465")
    ensure_column(connection, "system_settings", "email_smtp_ssl", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(connection, "system_settings", "email_sender", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "system_settings", "email_sender_name", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "system_settings", "email_password", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "system_settings", "email_recipients", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "system_settings", "email_block_link_base_url", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "system_settings", "email_alert_window_seconds", "INTEGER NOT NULL DEFAULT 60")
    ensure_column(connection, "system_settings", "email_alert_max_requests", "INTEGER NOT NULL DEFAULT 80")
    ensure_column(connection, "system_settings", "email_alert_max_404", "INTEGER NOT NULL DEFAULT 15")
    ensure_column(connection, "system_settings", "email_alert_cooldown_minutes", "INTEGER NOT NULL DEFAULT 30")


def rollback(connection):
    pass


step(add, rollback)
