"""Add per-rule injected headers and upstream TLS columns to forward_rules.

P2-3.3 每规则自定义请求头 + 上游 TLS 精细化。列通过 config_store 基表
`CREATE TABLE forward_rules` 与 `LEGACY_FORWARD_RULES_COLUMNS` 幂等补齐
（新库建表即含、旧库启动 ALTER 补列），yoyo 迁移仅在全新库执行，且重复
ALTER 会报 duplicate column，因此此处**不**写 ALTER，仅作为迁移链占位。

迁移后 forward_rules 承载：
  inject_request_headers（JSON 对象串，转发前注入并覆盖同名头）/
  upstream_verify_ssl（-1 继承全局 / 0 关闭校验 / 1 强制校验）/
  client_cert / client_key（mTLS 客户端证书 PEM 路径，空=不启用）
"""

from yoyo import step


def add(connection):
    # 列由 config_store 双补列机制维护，这里不做 ALTER，避免对已有库重复加列报错。
    pass


def rollback(connection):
    # 同样不主动 DROP 列，避免误删已在用的配置。
    pass
