"""Add chain column to route_logs table.

请求链路可视化：把链路口志节点（_chain，如「签名重入:通过 → 签名重入:缓存命中(内部代理)
→ 上游耗时:0.4s → 代理:200」）随路由日志落库。此前 302 加签 B 模式（本地代理穿流）的
重入请求在日志页与普通代理请求完全无法区分（redirect_count/redirect_location 已按语义
归零），运维无法确认某个请求到底走没走签名端点、用的是快照还是重新匹配规则。
"""

from yoyo import step


def add(connection):
    def _has_column(cursor, table, column):
        try:
            rows = cursor.execute(f"PRAGMA table_info({table})").fetchall()
            return any(row[1] == column for row in rows)
        except Exception:
            return False

    def _ensure_column(cursor, table, column, definition):
        if not _has_column(cursor, table, column):
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    _ensure_column(connection, "route_logs", "chain", "TEXT NOT NULL DEFAULT ''")


def rollback(connection):
    pass


step(add, rollback)
