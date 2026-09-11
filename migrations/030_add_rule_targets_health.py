"""Add multi-target + health-check columns to forward_rules.

P1-2.2 多上游负载均衡 + 上游健康检查。列通过 config_store 基表
`CREATE TABLE forward_rules` 与 `LEGACY_FORWARD_RULES_COLUMNS` 幂等补齐
（新库建表即含、旧库启动 ALTER 补列），yoyo 迁移仅在全新库执行，且重复 ALTER
会报 duplicate column，因此此处**不**写 ALTER，仅作为迁移链占位。

迁移后 forward_rules 承载：
  target_urls / health_check_enabled / health_check_path /
  health_check_interval / health_check_timeout
对应 ProxyRule.target_urls 与各 health_check_* 字段。
"""

from yoyo import step


def add(connection):
    # 列由 config_store 双补列机制维护，这里不做 ALTER，避免对已有库重复加列报错。
    pass


def rollback(connection):
    # 同样不主动 DROP 列，避免误删已在用的配置。
    pass
