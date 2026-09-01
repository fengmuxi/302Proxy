"""route_groups table primary key migration"""

from yoyo import step


def migrate_route_groups(connection):
    cursor = connection.execute("PRAGMA table_info(route_groups)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    if not existing_columns or "request_host" in existing_columns:
        connection.execute("DROP TABLE IF EXISTS route_groups_v2")
        return

    connection.execute("DROP TABLE IF EXISTS route_groups_v2")

    connection.execute("""
        CREATE TABLE route_groups_v2 (
            request_host TEXT NOT NULL DEFAULT '',
            path_prefix TEXT NOT NULL,
            region_matching_enabled INTEGER NOT NULL DEFAULT 0,
            notes TEXT NOT NULL DEFAULT '',
            access_ip_whitelist TEXT NOT NULL DEFAULT '',
            ip_blacklist TEXT NOT NULL DEFAULT '',
            region_whitelist TEXT NOT NULL DEFAULT '',
            region_blacklist TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (request_host, path_prefix)
        )
    """)

    connection.execute("""
        INSERT INTO route_groups_v2 (request_host, path_prefix, region_matching_enabled, notes,
            access_ip_whitelist, ip_blacklist, region_whitelist, region_blacklist, updated_at)
        SELECT '', path_prefix, region_matching_enabled, notes,
            COALESCE(access_ip_whitelist, ''), COALESCE(ip_blacklist, ''),
            COALESCE(region_whitelist, ''), COALESCE(region_blacklist, ''), updated_at
        FROM route_groups
    """)

    connection.execute("DROP TABLE route_groups")
    connection.execute("ALTER TABLE route_groups_v2 RENAME TO route_groups")


def rollback_route_groups(connection):
    cursor = connection.execute("PRAGMA table_info(route_groups)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    if "request_host" not in existing_columns:
        return

    connection.execute("""
        CREATE TABLE route_groups_old (
            path_prefix TEXT PRIMARY KEY,
            region_matching_enabled INTEGER NOT NULL DEFAULT 0,
            notes TEXT NOT NULL DEFAULT '',
            access_ip_whitelist TEXT NOT NULL DEFAULT '',
            ip_blacklist TEXT NOT NULL DEFAULT '',
            region_whitelist TEXT NOT NULL DEFAULT '',
            region_blacklist TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
    """)

    connection.execute("""
        INSERT INTO route_groups_old (path_prefix, region_matching_enabled, notes,
            access_ip_whitelist, ip_blacklist, region_whitelist, region_blacklist, updated_at)
        SELECT path_prefix, region_matching_enabled, notes,
            access_ip_whitelist, ip_blacklist, region_whitelist, region_blacklist, updated_at
        FROM route_groups
    """)

    connection.execute("DROP TABLE route_groups")
    connection.execute("ALTER TABLE route_groups_old RENAME TO route_groups")


step(migrate_route_groups, rollback_route_groups)
