"""Add forward_rules path rewrite columns"""

from yoyo import step


def ensure_column(connection, table, column, definition):
    cursor = connection.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def add(connection):
    ensure_column(connection, "forward_rules", "path_rewrite_pattern", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "forward_rules", "path_rewrite_replacement", "TEXT NOT NULL DEFAULT ''")


def rollback(connection):
    try:
        connection.execute("ALTER TABLE forward_rules DROP COLUMN path_rewrite_pattern")
    except Exception:
        pass
    try:
        connection.execute("ALTER TABLE forward_rules DROP COLUMN path_rewrite_replacement")
    except Exception:
        pass


step(add, rollback)
