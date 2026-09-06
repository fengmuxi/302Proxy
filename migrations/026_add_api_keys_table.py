"""Add api_keys table for backend API key authentication.

API Key 调用后台接口（/_admin/api/*）：
- key_prefix: 明文前缀（如 n302_a1b2c3d4）供列表识别，完整明文只在创建时返回一次；
- key_hash:  SHA256(完整明文)，校验时对请求头里的 Key 做同样哈希后精确匹配；
- readonly:  只读密钥仅允许 GET/HEAD；
- expires_at: 可空，Unix 秒；空为永久。
"""

from yoyo import step


def add(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL DEFAULT '',
            key_prefix TEXT NOT NULL DEFAULT '',
            key_hash TEXT NOT NULL UNIQUE,
            readonly INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            use_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL DEFAULT '',
            expires_at INTEGER
        )
        """
    )
    connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)")


def rollback(connection):
    pass


step(add, rollback)
