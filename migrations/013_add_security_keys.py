"""Add system_settings security key columns"""

from yoyo import step


def ensure_column(connection, table, column, definition):
    cursor = connection.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def add(connection):
    import secrets as secrets_module

    ensure_column(connection, "system_settings", "session_secret", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "system_settings", "rsa_private_key", "TEXT NOT NULL DEFAULT ''")

    row = connection.execute(
        "SELECT session_secret, rsa_private_key FROM system_settings WHERE id = 1"
    ).fetchone()

    if not row:
        return

    updates = {}
    if not row[0]:
        updates["session_secret"] = secrets_module.token_hex(32)

    if not row[1]:
        try:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import rsa

            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
            )
            private_pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            ).decode('utf-8')
            updates["rsa_private_key"] = private_pem
        except ImportError:
            pass

    if updates:
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        vals = list(updates.values())
        connection.execute(
            f"UPDATE system_settings SET {set_clause} WHERE id = 1",
            vals,
        )


def rollback(connection):
    pass


step(add, rollback)
