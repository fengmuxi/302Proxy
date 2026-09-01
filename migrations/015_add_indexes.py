"""Create indexes"""

from yoyo import step


def add(connection):
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_forward_rules_path_prefix ON forward_rules(path_prefix)",
        "CREATE INDEX IF NOT EXISTS idx_forward_rules_enabled ON forward_rules(enabled)",
        "CREATE INDEX IF NOT EXISTS idx_forward_rules_source ON forward_rules(source)",
        "CREATE INDEX IF NOT EXISTS idx_forward_rules_request_host ON forward_rules(request_host)",
        "CREATE INDEX IF NOT EXISTS idx_route_logs_created_at ON route_logs(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_route_logs_path_prefix ON route_logs(path_prefix)",
        "CREATE INDEX IF NOT EXISTS idx_route_logs_match_strategy ON route_logs(match_strategy)",
        "CREATE INDEX IF NOT EXISTS idx_route_logs_result_status ON route_logs(result_status)",
        "CREATE INDEX IF NOT EXISTS idx_route_logs_request_host ON route_logs(request_host)",
        "CREATE INDEX IF NOT EXISTS idx_route_logs_rule_request_host ON route_logs(rule_request_host)",
    ]
    for sql in indexes:
        try:
            connection.execute(sql)
        except Exception:
            pass


def rollback(connection):
    pass


step(add, rollback)
