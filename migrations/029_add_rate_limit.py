"""Add active rate limiting columns to system_settings.

P1-2.1 主动速率限制（柔性节流 → 429）。列通过 config_store 基表
`CREATE TABLE system_settings` 与 `LEGACY_SYSTEM_SETTINGS_COLUMNS` 幂等补齐
（新库建表即含、旧库启动 ALTER 补列），yoyo 迁移仅在全新库执行，且重复 ALTER
会报 duplicate column，因此此处**不**写 ALTER，仅作为迁移链占位。

迁移后配置由 system_settings 三列承载：
  rate_limit_enabled / rate_limit_rps / rate_limit_burst / rate_limit_per_ip
对应 Config.rate_limit（RateLimitConfig）与 config_store.get/update_rate_limit_config。
"""

from yoyo import step


def add(connection):
    # 列由 config_store 双补列机制维护，这里不做 ALTER，避免对已有库重复加列报错。
    pass


def rollback(connection):
    # 同样不主动 DROP 列，避免误删已在用的配置。
    pass
