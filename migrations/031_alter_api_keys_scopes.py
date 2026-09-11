"""Add fine-grained permission columns to api_keys.

P1-2.3 API Key 细粒度权限。列通过 config_store 基表 `CREATE TABLE api_keys`
与 `LEGACY_API_KEYS_COLUMNS` 幂等补齐（新库建表即含、旧库启动 ALTER 补列），
yoyo 迁移仅在全新库执行，且重复 ALTER 会报 duplicate column，因此此处
**不**写 ALTER，仅作为迁移链占位。

迁移后 api_keys 承载：
  scopes（逗号分隔端点 tag，空=全部）/ allowed_ips（逗号分隔 IP，空=不限）/
  rate_limit（每秒请求数上限，0=不限）
"""

from yoyo import step


def add(connection):
    # 列由 config_store 双补列机制维护，这里不做 ALTER，避免对已有库重复加列报错。
    pass


def rollback(connection):
    # 同样不主动 DROP 列，避免误删已在用的配置。
    pass
