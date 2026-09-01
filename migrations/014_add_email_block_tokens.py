"""Create email_block_tokens table"""

from yoyo import step


def add(connection):
    connection.execute("""
        CREATE TABLE IF NOT EXISTS email_block_tokens (
            token TEXT PRIMARY KEY,
            ip TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            used INTEGER NOT NULL DEFAULT 0
        )
    """)


def rollback(connection):
    connection.execute("DROP TABLE IF EXISTS email_block_tokens")


step(add, rollback)
