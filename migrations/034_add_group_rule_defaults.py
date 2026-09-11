"""Add route_groups.rule_defaults for group-level rule defaults (P2-4.1 规则继承).

新列由 config_store 基表 `CREATE TABLE IF NOT EXISTS route_groups` +
启动时 `_ensure_column("route_groups", "rule_defaults", ...)` 幂等补齐，
yoyo 迁移仅在全新库执行，因此此处**不**重复 DDL，仅作为迁移链占位。

列结构：rule_defaults TEXT NOT NULL DEFAULT '{}' —— JSON 对象，
键 ∈ config.GROUP_RULE_DEFAULT_FIELDS（timeout / max_redirects / retry_times /
follow_redirects / enable_streaming / strip_prefix / referer_whitelist /
referer_policy / ua_blacklist / ua_whitelist），值缺省 = 组未设置默认。
"""

from yoyo import step


def add(connection):
    # 列由 config_store 基表 + _ensure_column 维护，这里不做 DDL。
    pass


def rollback(connection):
    # 不主动 DROP，避免误删组级默认配置。
    pass


step(add, rollback)
