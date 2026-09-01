"""Add system_settings logging_retention_days column with data migration"""

from yoyo import step


def add(connection):
    cursor = connection.execute("PRAGMA table_info(system_settings)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    if "logging_retention_days" not in existing_columns:
        connection.execute(
            "ALTER TABLE system_settings ADD COLUMN logging_retention_days INTEGER NOT NULL DEFAULT 30"
        )
        if "logging_backup_count" in existing_columns:
            connection.execute(
                "UPDATE system_settings SET logging_retention_days = logging_backup_count WHERE logging_retention_days = 30"
            )


def rollback(connection):
    cursor = connection.execute("PRAGMA table_info(system_settings)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    if "logging_retention_days" in existing_columns:
        if "logging_backup_count" in existing_columns:
            connection.execute(
                "UPDATE system_settings SET logging_backup_count = logging_retention_days WHERE id = 1"
            )
        connection.execute("ALTER TABLE system_settings DROP COLUMN logging_retention_days")


step(add, rollback)
