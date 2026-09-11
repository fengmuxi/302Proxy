"""Add system_settings_history table for config versioning (P2-3.5).

新表由 config_store 基表 `CREATE TABLE IF NOT EXISTS system_settings_history`
维护（新库建表即含、旧库启动时 IF NOT EXISTS 幂等建表），yoyo 迁移仅在全新库
执行，因此此处**不**重复 DDL，仅作为迁移链占位。

表结构：
  system_settings_history(id, module, payload_json, changed_by, created_at)
  module ∈ config_store._SETTINGS_MODULE_METHODS 定义的设置模块名。
"""

from yoyo import step


def add(connection):
    # 表由 config_store 基表 CREATE IF NOT EXISTS 维护，这里不做 DDL。
    pass


def rollback(connection):
    # 不主动 DROP，避免误删仍在使用的配置历史。
    pass
