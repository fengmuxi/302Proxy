"""Add banned_ips and route_groups columns"""

from yoyo import step


def ensure_column(connection, table, column, definition):
    cursor = connection.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def add(connection):
    ensure_column(connection, "banned_ips", "path_prefix", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "route_groups", "access_ip_whitelist", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "route_groups", "ip_blacklist", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "route_groups", "region_whitelist", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "route_groups", "region_blacklist", "TEXT NOT NULL DEFAULT ''")


def rollback(connection):
    pass


step(add, rollback)
