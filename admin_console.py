from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import inspect
import logging
import os
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

from aiohttp import web

from config import Config
from config_store import ConfigStore
from geo_service import GeoResolver

# 模块级日志器，与项目其他模块保持一致（config.py 等使用 "proxy" 作为 logger 名）
logger = logging.getLogger("proxy")


@web.middleware
async def static_no_cache_middleware(request: web.Request, handler):
    """管理后台静态资源使用协商缓存（no-cache + ETag/Last-Modified）。

    add_static 默认不带 Cache-Control，浏览器按启发式策略长期缓存 JS/CSS，
    导致前端修复后用户刷新仍拿到旧文件（如封禁弹窗勾选不同步的旧版逻辑）。
    no-cache 表示可缓存但每次必须 revalidate，未变更时 304，开销极小。
    """
    response = await handler(request)
    if request.path.startswith("/_admin/static/"):
        response.headers["Cache-Control"] = "no-cache"
    return response


# ============ API 文档自动维护：接口说明登记表 ============
#
# 设计目标：让「API 文档」页具备自我维护能力——
#   1) 文档页直接读取 aiohttp 路由表，任何新增的 /_admin/api/* 接口都会自动出现；
#   2) 此处登记富文本（分组 / 摘要 / 参数 / 请求体示例 / 备注），让文档更易用；
#   3) 未登记的接口也会列出（仅路径 + 方法 + 提示「待补充说明」），绝不遗漏。
#
# 约定：新增后台 API 时，在此用「handler 方法名」登记一条即可自动并入文档；
#       不登记也不会从文档消失（路由表兜底）。param.in ∈ {query, path, body}。
_API_DOC_SPECS: Dict[str, Dict[str, Any]] = {
    # —— API 密钥 ——
    "list_api_keys": {"tag": "API 密钥", "summary": "列出全部 API 密钥", "params": [],
                      "note": "响应仅含前缀/状态等元数据，不含明文与哈希"},
    "create_api_key": {"tag": "API 密钥", "summary": "签发新密钥",
                       "params": [{"name": "name", "in": "body", "type": "string", "required": True, "desc": "名称（≤64）"},
                                  {"name": "readonly", "in": "body", "type": "bool", "required": False, "desc": "是否只读（仅 GET）"},
                                  {"name": "expires_days", "in": "body", "type": "int", "required": False, "desc": "有效期天数，0=永久，≤3650"}],
                       "body_sample": '{\n  "name": "自动化脚本",\n  "readonly": false,\n  "expires_days": 30\n}',
                       "note": "明文密钥仅在响应中出现一次，请立即保存"},
    "toggle_api_key": {"tag": "API 密钥", "summary": "启用/停用密钥",
                       "params": [{"name": "key_id", "in": "path", "type": "int", "required": True, "desc": "密钥 ID"},
                                  {"name": "enabled", "in": "body", "type": "bool", "required": True, "desc": "目标状态"}],
                       "body_sample": '{"enabled": false}'},
    "delete_api_key": {"tag": "API 密钥", "summary": "删除（吊销）密钥",
                       "params": [{"name": "key_id", "in": "path", "type": "int", "required": True, "desc": "密钥 ID"}],
                       "note": "删除后该密钥调用立即 401，不可恢复"},
    # —— 路由组 ——
    "list_route_groups": {"tag": "路由配置", "summary": "列出路由组（按路径前缀 + 请求域名分组）"},
    "create_route_group": {"tag": "路由配置", "summary": "新建路由组", "params": [{"name": "path_prefix", "in": "body", "type": "string", "required": True, "desc": "路径前缀"}, {"name": "request_host", "in": "body", "type": "string", "required": True, "desc": "请求域名（空=全局）"}]},
    "update_route_group": {"tag": "路由配置", "summary": "更新路由组"},
    "delete_route_group": {"tag": "路由配置", "summary": "删除路由组"},
    # —— 封禁 ——
    "list_banned_ips": {"tag": "安全与封禁", "summary": "列出封禁名单", "params": [{"name": "limit", "in": "query", "type": "int", "required": False, "desc": "每页条数"}]},
    "add_banned_ip": {"tag": "安全与封禁", "summary": "手动封禁 IP / 网段",
                      "params": [{"name": "ip", "in": "body", "type": "string", "required": True, "desc": "IP 或 CIDR"},
                                 {"name": "path_prefix", "in": "body", "type": "string", "required": False, "desc": "留空=全局封禁"},
                                 {"name": "reason", "in": "body", "type": "string", "required": False, "desc": "封禁原因"},
                                 {"name": "permanent", "in": "body", "type": "bool", "required": True, "desc": "是否永久"},
                                 {"name": "duration_seconds", "in": "body", "type": "int", "required": False, "desc": "临时封禁时长（秒）"}],
                      "body_sample": '{\n  "ip": "1.2.3.4",\n  "reason": "滥用",\n  "permanent": true\n}'},
    "remove_banned_ip": {"tag": "安全与封禁", "summary": "解封 IP",
                         "params": [{"name": "ip", "in": "path", "type": "string", "required": True, "desc": "IP 地址"}]},
    "extend_banned_ip": {"tag": "安全与封禁", "summary": "延长封禁", "params": [{"name": "ip", "in": "path", "type": "string", "required": True, "desc": "IP"}, {"name": "duration_hours", "in": "body", "type": "number", "required": True, "desc": "延长小时数"}]},
    "clear_banned_ips": {"tag": "安全与封禁", "summary": "清空全部封禁记录"},
    # —— 日志与审计 ——
    "list_route_logs": {"tag": "日志与审计", "summary": "查询请求转发日志（分页 + 筛选）",
                        "params": [{"name": "page", "in": "query", "type": "int", "required": False, "desc": "页码"},
                                   {"name": "limit", "in": "query", "type": "int", "required": False, "desc": "每页条数"},
                                   {"name": "keyword", "in": "query", "type": "string", "required": False, "desc": "关键词"},
                                   {"name": "path_prefix", "in": "query", "type": "string", "required": False, "desc": "路径前缀"},
                                   {"name": "result_status", "in": "query", "type": "string", "required": False, "desc": "结果状态"}]},
    "delete_route_logs": {"tag": "日志与审计", "summary": "删除日志",
                          "params": [{"name": "ids", "in": "body", "type": "array", "required": False, "desc": "指定 ID 列表"}, {"name": "delete_all", "in": "body", "type": "bool", "required": False, "desc": "清空全部"}]},
    "get_hotlink_stats": {"tag": "日志与审计", "summary": "盗链监控统计", "params": [{"name": "hours", "in": "query", "type": "int", "required": False, "desc": "统计窗口（小时）"}]},
    "get_route_log_settings": {"tag": "日志与审计", "summary": "日志保留策略"},
    "update_route_log_settings": {"tag": "日志与审计", "summary": "更新日志保留策略"},
    "get_logging_settings": {"tag": "日志与审计", "summary": "磁盘日志设置"},
    "update_logging_settings": {"tag": "日志与审计", "summary": "更新磁盘日志设置"},
    "list_app_log_files": {"tag": "日志与审计", "summary": "应用日志文件列表"},
    "get_app_log_content": {"tag": "日志与审计", "summary": "读取应用日志内容", "params": [{"name": "file", "in": "query", "type": "string", "required": True, "desc": "文件名"}, {"name": "tail_lines", "in": "query", "type": "int", "required": False, "desc": "末行数"}]},
    # —— 系统设置 ——
    "get_ip_cache_settings": {"tag": "系统设置", "summary": "请求结果缓存设置"},
    "update_ip_cache_settings": {"tag": "系统设置", "summary": "更新结果缓存设置"},
    "get_dedup_settings": {"tag": "系统设置", "summary": "请求去重设置"},
    "update_dedup_settings": {"tag": "系统设置", "summary": "更新去重设置"},
    "get_auto_ban_settings": {"tag": "系统设置", "summary": "自动封禁策略"},
    "update_auto_ban_settings": {"tag": "系统设置", "summary": "更新自动封禁策略"},
    "get_stream_guard_settings": {"tag": "系统设置", "summary": "流式守卫设置"},
    "update_stream_guard_settings": {"tag": "系统设置", "summary": "更新流式守卫设置"},
    # —— 加签防护 ——
    "get_signed_url_settings": {"tag": "加签防护", "summary": "签名 URL 设置"},
    "update_signed_url_settings": {"tag": "加签防护", "summary": "更新签名 URL 设置"},
    "generate_signed_url": {"tag": "加签防护", "summary": "生成签名链接", "params": [{"name": "target_url", "in": "body", "type": "string", "required": True, "desc": "目标地址"}]},
    "get_redirect_signing_settings": {"tag": "加签防护", "summary": "302 加签改写设置"},
    "update_redirect_signing_settings": {"tag": "加签防护", "summary": "更新 302 加签改写设置"},
    # —— IP 定位 ——
    "get_geoip": {"tag": "IP 定位", "summary": "离线/在线定位源配置"},
    "update_geoip": {"tag": "IP 定位", "summary": "更新定位源配置"},
    "test_geoip": {"tag": "IP 定位", "summary": "测试在线源"},
    "clear_geoip_online_cache": {"tag": "IP 定位", "summary": "清空在线定位缓存"},
    "test_offline_geoip": {"tag": "IP 定位", "summary": "测试离线库"},
    "sync_offline_geoip": {"tag": "IP 定位", "summary": "同步离线库"},
    "rollback_offline_geoip": {"tag": "IP 定位", "summary": "回滚离线库"},
    # —— 邮件提醒 ——
    "get_email_settings": {"tag": "邮件提醒", "summary": "SMTP 邮件配置"},
    "update_email_settings": {"tag": "邮件提醒", "summary": "更新 SMTP 配置"},
    "test_email": {"tag": "邮件提醒", "summary": "发送测试邮件"},
    # —— 备份与恢复 ——
    "list_backups": {"tag": "备份与恢复", "summary": "备份列表"},
    "create_backup": {"tag": "备份与恢复", "summary": "创建备份快照"},
    "download_backup": {"tag": "备份与恢复", "summary": "下载备份文件", "params": [{"name": "filename", "in": "path", "type": "string", "required": True, "desc": "文件名"}]},
    "restore_backup": {"tag": "备份与恢复", "summary": "恢复备份（multipart）"},
    "delete_backup": {"tag": "备份与恢复", "summary": "删除备份", "params": [{"name": "filename", "in": "path", "type": "string", "required": True, "desc": "文件名"}]},
    # —— 仪表盘 ——
    "bootstrap": {"tag": "概览", "summary": "聚合数据（路由组/规则/统计），供首页仪表盘使用"},
    # —— 规则 ——
    "list_rules": {"tag": "路由配置", "summary": "列出转发规则", "params": [{"name": "group_path_prefix", "in": "query", "type": "string", "required": False, "desc": "按路由组过滤"}]},
    "create_rule": {"tag": "路由配置", "summary": "新建转发规则",
                    "params": [{"name": "path_prefix", "in": "body", "type": "string", "required": True, "desc": "路径前缀"},
                               {"name": "target_url", "in": "body", "type": "string", "required": True, "desc": "目标地址"},
                               {"name": "follow_redirects", "in": "body", "type": "bool", "required": False, "desc": "是否跟随上游重定向"}],
                    "body_sample": '{\n  "path_prefix": "/play",\n  "target_url": "https://cdn.example.com/hop",\n  "follow_redirects": true\n}'},
    "get_rule": {"tag": "路由配置", "summary": "查看单条规则", "params": [{"name": "rule_id", "in": "path", "type": "int", "required": True, "desc": "规则 ID"}]},
    "update_rule": {"tag": "路由配置", "summary": "更新规则", "params": [{"name": "rule_id", "in": "path", "type": "int", "required": True, "desc": "规则 ID"}]},
    "delete_rule": {"tag": "路由配置", "summary": "删除规则", "params": [{"name": "rule_id", "in": "path", "type": "int", "required": True, "desc": "规则 ID"}]},
    # —— 鉴权状态 ——
    "auth_status": {"tag": "概览", "summary": "查询后台鉴权开关与当前登录态"},
    # —— 日志清理 ——
    "cleanup_log_files": {"tag": "日志与审计", "summary": "按保留策略清理请求日志"},
    "cleanup_log_files_on_disk": {"tag": "日志与审计", "summary": "清理磁盘应用日志文件"},
}

# 用法说明：与 _API_DOC_SPECS 同层，按 handler 方法名补充「怎么用」。
# 文档页会把它渲染为「用法」区块，供开发者快速上手；未列出的接口不显示该区块。
_API_DOC_USAGE: Dict[str, str] = {
    "list_api_keys": "列出你签发的全部密钥（仅元数据，不含明文/哈希）。适合在吊销或审计前先查看密钥 ID 与状态。",
    "create_api_key": "签发新密钥。name 仅作辨识；readonly=true 时该密钥只能调 GET 接口且不能管理密钥本身；expires_days=0 表示永久。请务必在弹窗里复制明文——系统只存哈希，关闭后无法找回。",
    "toggle_api_key": "启用/停用指定密钥。停用后该密钥的所有调用立即返回 401，可用于紧急止血而不删除（保留审计记录）。",
    "delete_api_key": "吊销（删除）密钥。删除后调用立即 401 且不可恢复；需要长期停用时优先用 toggle 而非删除。",
    "list_route_groups": "返回所有路由组（按 路径前缀 + 请求域名 维度）。route_groups 是 rules 的容器，新增规则前先确认所属组。",
    "create_route_group": "新建路由组。path_prefix 决定命中哪些请求，request_host 为空表示全局适用。",
    "update_route_group": "更新路由组的地区匹配/备注等属性（按 path_prefix + request_host 定位）。改后建议回概览页确认分组状态。",
    "delete_route_group": "删除整个路由组及其下规则（不可恢复），删除前请先确认无线上流量依赖该前缀。",
    "list_banned_ips": "分页返回封禁名单。注意：封禁只拦截代理转发路径，不影响 /_admin 管理接口。",
    "add_banned_ip": "手动封禁一个 IP 或 CIDR。path_prefix 留空=全局封禁；permanent=false 时需带 duration_seconds。reasons 建议填来源便于审计。",
    "remove_banned_ip": "按 IP 解封。CIDR 网段需原样传回完整网段串。",
    "extend_banned_ip": "延长临时封禁时长（小时），不影响到期后的永久/临时属性。",
    "clear_banned_ips": "清空全部封禁记录（不可恢复），谨慎调用。",
    "list_route_logs": "查询请求转发日志。支持 keyword/path_prefix/result_status 等筛选与 limit 分页；result_status=upstream_error 可快速定位上游异常。",
    "delete_route_logs": "删除日志：传 ids 数组删指定条目，或 delete_all=true 清空（不可恢复）。",
    "get_hotlink_stats": "盗链监控统计。hours 指定统计窗口，用于发现异常 Referer 来源 IP。",
    "get_route_log_settings": "查看日志保留策略（按天数/条数）。",
    "update_route_log_settings": "更新日志保留策略，避免日志无限增长。",
    "get_logging_settings": "查看磁盘应用日志（文件）的滚动/保留配置。",
    "update_logging_settings": "更新磁盘应用日志配置。",
    "list_app_log_files": "列出服务端应用日志文件，配合 get_app_log_content 读取。",
    "get_app_log_content": "读取指定日志文件内容；tail_lines 控制末行数，便于排查最近问题。",
    "get_ip_cache_settings": "查看请求结果缓存（ip_result_cache）开关与 TTL。",
    "update_ip_cache_settings": "调整结果缓存参数；调大 TTL 可降上游压力，但会延迟上游变更生效。",
    "get_dedup_settings": "查看请求去重配置。",
    "update_dedup_settings": "调整去重窗口/并发限制，防止同一请求被重复打向上游。",
    "get_auto_ban_settings": "查看自动封禁策略（阈值/窗口）。",
    "update_auto_ban_settings": "调整自动封禁参数；仅针对代理转发路径，不影响后台访问。",
    "get_stream_guard_settings": "查看流式守卫配置（防滥用/限速）。",
    "update_stream_guard_settings": "调整流式守卫参数。",
    "get_signed_url_settings": "查看签名 URL（入口校验）配置：是否启用、TTL、是否绑定 IP。",
    "update_signed_url_settings": "更新签名 URL 配置。轮换 secret 会作废所有存量签名链接。",
    "generate_signed_url": "临时为一个目标地址生成签名链接，便于在不改规则的情况下快速验证签名链路。",
    "get_redirect_signing_settings": "查看 302 加签改写（出口改写）配置。",
    "update_redirect_signing_settings": "更新 302 加签改写配置；启用后上游 302 的裸地址会被改写为系统签名链接。",
    "get_geoip": "查看在线定位源与离线 MMDB 配置。",
    "update_geoip": "更新定位源配置；权重轮询 + 离线兜底。",
    "test_geoip": "用 sample IP 测试在线源是否可用。",
    "clear_geoip_online_cache": "清空在线定位结果缓存，使下一次定位重新查询源。",
    "test_offline_geoip": "用 sample IP 测试离线 MMDB 是否可用。",
    "sync_offline_geoip": "同步离线库（下载最新 MMDB），建议先 test 再 sync。",
    "rollback_offline_geoip": "回滚到上一份离线库，sync 异常时兜底。",
    "get_email_settings": "查看 SMTP 邮件提醒配置（地址脱敏）。",
    "update_email_settings": "更新 SMTP 配置；改后点「发送测试邮件」验证。",
    "test_email": "发送一封测试邮件到配置收件人，验证 SMTP 连通。",
    "list_backups": "列出服务端备份快照（含文件名/大小/时间）。",
    "create_backup": "生成一份当前配置数据快照，重大变更前建议先备份。",
    "download_backup": "下载指定备份文件到本地。",
    "restore_backup": "从备份恢复（multipart 上传或选服务端文件），支持覆盖/合并。",
    "delete_backup": "删除指定备份文件。",
    "bootstrap": "聚合仪表盘数据（路由组/规则/统计/各模块开关），前端首页与概览卡依赖此接口；开销略大，勿高频轮询。",
    "list_rules": "列出某路由组下全部转发规则，含正则重写/地区过滤/流式等开关。",
    "create_rule": "新建转发规则。target_url 为上游地址；follow_redirects 决定本地代理是否跟随上游重定向。",
    "get_rule": "查看单条规则详情。",
    "update_rule": "更新规则字段（优先级/流式/地区过滤等）。",
    "delete_rule": "删除规则；删除后该前缀的匹配将回退到默认路由/其他规则。",
    "auth_status": "只读查询鉴权开关与当前会话登录态，可用于健康检查或「API 文档」页的连通自测。",
    "cleanup_log_files": "按保留策略批量清理请求日志库数据。",
    "cleanup_log_files_on_disk": "清理磁盘上的历史应用日志文件（按保留天数）。",
}



class AdminConsole:
    def __init__(
        self,
        config_store: ConfigStore,
        reload_callback: Callable[[], Awaitable[None]],
        offline_sync_callback: Callable[[], Awaitable[Dict[str, Any]]] | None = None,
        offline_rollback_callback: Callable[[], Awaitable[Dict[str, Any]]] | None = None,
        online_cache_clear_callback: Callable[[], Awaitable[Dict[str, Any]]] | None = None,
        ban_manager_callback: Callable[[str, Dict], Awaitable[None]] | None = None,
        log_cleanup_callback: Callable[[], Awaitable[Dict[str, Any]]] | None = None,
        log_file_cleanup_callback: Callable[[], Awaitable[Dict[str, Any]]] | None = None,
    ):
        self.config_store = config_store
        self.reload_callback = reload_callback
        self.offline_sync_callback = offline_sync_callback
        self.offline_rollback_callback = offline_rollback_callback
        self.online_cache_clear_callback = online_cache_clear_callback
        self.ban_manager_callback = ban_manager_callback
        self.log_cleanup_callback = log_cleanup_callback
        self.log_file_cleanup_callback = log_file_cleanup_callback
        self.static_dir = Path(__file__).resolve().parent / "static"
        self.backup_dir = Path(__file__).resolve().parent / "data" / "backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        # API 密钥 last_used 节流（内存态，避免每次调用都写库）
        self._api_key_touch_at: Dict[int, float] = {}

    def register(self, app: web.Application) -> None:
        self.app = app
        if static_no_cache_middleware not in app.middlewares:
            app.middlewares.append(static_no_cache_middleware)
        app.router.add_get("/_admin", self.index)
        app.router.add_get("/_admin/", self.index)
        app.router.add_static("/_admin/static/", str(self.static_dir), show_index=False)
        app.router.add_get("/_admin/api/auth/status", self.auth_status)
        app.router.add_post("/_admin/api/auth/login", self.login)
        app.router.add_get("/_admin/api/auth/public-key", self.public_key)
        app.router.add_post("/_admin/api/auth/logout", self.logout)
        # API 密钥管理（仅浏览器会话可操作，API 密钥自身不可管理密钥——防权限自增殖）
        app.router.add_get("/_admin/api/keys", self.list_api_keys)
        app.router.add_post("/_admin/api/keys", self.create_api_key)
        app.router.add_post("/_admin/api/keys/{key_id:\d+}/toggle", self.toggle_api_key)
        app.router.add_delete("/_admin/api/keys/{key_id:\d+}", self.delete_api_key)
        app.router.add_get("/_admin/api/bootstrap", self.bootstrap)
        # API 文档自动维护：从路由表汇总全部接口（新增接口自动出现）
        app.router.add_get("/_admin/api/doc", self.api_doc_catalog)
        app.router.add_get("/_admin/api/route-groups", self.list_route_groups)
        app.router.add_post("/_admin/api/route-groups", self.create_route_group)
        app.router.add_put("/_admin/api/route-groups", self.update_route_group)
        app.router.add_delete("/_admin/api/route-groups", self.delete_route_group)
        app.router.add_get("/_admin/api/geoip", self.get_geoip)
        app.router.add_put("/_admin/api/geoip", self.update_geoip)
        app.router.add_post("/_admin/api/geoip/test", self.test_geoip)
        app.router.add_post("/_admin/api/geoip/cache/clear", self.clear_geoip_online_cache)
        app.router.add_post("/_admin/api/geoip/offline/test", self.test_offline_geoip)
        app.router.add_post("/_admin/api/geoip/offline/sync", self.sync_offline_geoip)
        app.router.add_post("/_admin/api/geoip/offline/rollback", self.rollback_offline_geoip)
        app.router.add_get("/_admin/api/logs", self.list_route_logs)
        app.router.add_delete("/_admin/api/logs", self.delete_route_logs)
        app.router.add_get("/_admin/api/hotlink/stats", self.get_hotlink_stats)
        app.router.add_get("/_admin/api/log-settings", self.get_route_log_settings)
        app.router.add_put("/_admin/api/log-settings", self.update_route_log_settings)
        app.router.add_get("/_admin/api/logging-settings", self.get_logging_settings)
        app.router.add_put("/_admin/api/logging-settings", self.update_logging_settings)
        app.router.add_get("/_admin/api/app-logs", self.list_app_log_files)
        app.router.add_get("/_admin/api/app-logs/content", self.get_app_log_content)
        app.router.add_get("/_admin/api/ip-cache-settings", self.get_ip_cache_settings)
        app.router.add_put("/_admin/api/ip-cache-settings", self.update_ip_cache_settings)
        app.router.add_get("/_admin/api/dedup-settings", self.get_dedup_settings)
        app.router.add_put("/_admin/api/dedup-settings", self.update_dedup_settings)
        app.router.add_get("/_admin/api/banned-ips", self.list_banned_ips)
        app.router.add_post("/_admin/api/banned-ips", self.add_banned_ip)
        app.router.add_delete("/_admin/api/banned-ips/{ip:.+}", self.remove_banned_ip)
        app.router.add_post("/_admin/api/banned-ips/clear", self.clear_banned_ips)
        app.router.add_post("/_admin/api/banned-ips/{ip:.+}/extend", self.extend_banned_ip)
        app.router.add_get("/_admin/api/auto-ban", self.get_auto_ban_settings)
        app.router.add_put("/_admin/api/auto-ban", self.update_auto_ban_settings)
        app.router.add_get("/_admin/api/stream-guard", self.get_stream_guard_settings)
        app.router.add_put("/_admin/api/stream-guard", self.update_stream_guard_settings)
        app.router.add_get("/_admin/api/signed-url", self.get_signed_url_settings)
        app.router.add_put("/_admin/api/signed-url", self.update_signed_url_settings)
        app.router.add_post("/_admin/api/signed-url/generate", self.generate_signed_url)
        app.router.add_get("/_admin/api/redirect-signing", self.get_redirect_signing_settings)
        app.router.add_put("/_admin/api/redirect-signing", self.update_redirect_signing_settings)
        app.router.add_get("/_admin/api/email", self.get_email_settings)
        app.router.add_put("/_admin/api/email", self.update_email_settings)
        app.router.add_post("/_admin/api/email/test", self.test_email)
        app.router.add_get("/_admin/api/rules", self.list_rules)
        app.router.add_post("/_admin/api/rules", self.create_rule)
        app.router.add_get("/_admin/api/rules/{rule_id:\\d+}", self.get_rule)
        app.router.add_put("/_admin/api/rules/{rule_id:\\d+}", self.update_rule)
        app.router.add_delete("/_admin/api/rules/{rule_id:\\d+}", self.delete_rule)
        app.router.add_get("/_admin/api/backup/list", self.list_backups)
        app.router.add_post("/_admin/api/backup/create", self.create_backup)
        app.router.add_get("/_admin/api/backup/download/{filename}", self.download_backup)
        app.router.add_post("/_admin/api/backup/restore", self.restore_backup)
        app.router.add_delete("/_admin/api/backup/{filename}", self.delete_backup)
        app.router.add_post("/_admin/api/log-cleanup", self.cleanup_log_files)
        app.router.add_post("/_admin/api/log-file-cleanup", self.cleanup_log_files_on_disk)

    async def index(self, request: web.Request) -> web.FileResponse:
        # admin.html 也必须 no-cache 协商缓存：FileResponse 默认无 Cache-Control，
        # 浏览器按启发式策略缓存旧 HTML，前端更新后（如新增盗链监控卡片）
        # 用户普通刷新仍拿不到新页面（静态 JS/CSS 已由 c1b9041 修复，此处补齐 HTML）
        return web.FileResponse(
            self.static_dir / "admin.html",
            headers={"Cache-Control": "no-cache"},
        )

    async def auth_status(self, request: web.Request) -> web.Response:
        config = self._get_auth_config()
        authenticated = self._is_authenticated(request)
        return self._json(
            {
                "enabled": self._is_auth_enabled(),
                "authenticated": authenticated,
                "username": config.username if authenticated else "",
            }
        )

    async def public_key(self, request: web.Request) -> web.Response:
        """返回 RSA 公钥，供前端加密密码"""
        config = self._get_auth_config()
        rsa_private_key = getattr(config, 'rsa_private_key', '')
        if not rsa_private_key:
            return self._json({"error": "RSA 密钥未初始化"}, status=500)
        
        try:
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            from cryptography.hazmat.primitives import serialization
            
            private_key = load_pem_private_key(rsa_private_key.encode('utf-8'), password=None)
            public_key = private_key.public_key()
            public_pem = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ).decode('utf-8')
            
            return self._json({"public_key": public_pem})
        except ImportError:
            return self._json({"error": "cryptography 库未安装"}, status=500)
        except Exception as e:
            return self._json({"error": f"获取公钥失败: {str(e)}"}, status=500)

    async def login(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        config = self._get_auth_config()
        if not self._is_auth_enabled():
            return self._json({"enabled": False, "authenticated": True, "username": ""})

        username = str(payload.get("username", "")).strip()
        password = str(payload.get("password", ""))
        encrypted = payload.get("encrypted", False)
        
        # 如果是加密密码，进行解密
        if encrypted:
            rsa_private_key = getattr(config, 'rsa_private_key', '')
            if rsa_private_key:
                try:
                    from cryptography.hazmat.primitives.serialization import load_pem_private_key
                    from cryptography.hazmat.primitives.asymmetric import padding
                    from cryptography.hazmat.primitives import hashes
                    import base64
                    
                    private_key = load_pem_private_key(rsa_private_key.encode('utf-8'), password=None)
                    encrypted_bytes = base64.b64decode(password)
                    password = private_key.decrypt(
                        encrypted_bytes,
                        padding.OAEP(
                            mgf=padding.MGF1(algorithm=hashes.SHA256()),
                            algorithm=hashes.SHA256(),
                            label=None
                        )
                    ).decode('utf-8')
                except ImportError:
                    return self._json({"error": "cryptography 库未安装，无法解密密码"}, status=400)
                except Exception:
                    return self._json({"error": "密码解密失败"}, status=400)
            else:
                return self._json({"error": "RSA 密钥未配置"}, status=400)
        
        if username != config.username or password != config.password:
            return self._json({"error": "账号或密码错误。"}, status=401)

        response = self._json(
            {
                "enabled": True,
                "authenticated": True,
                "username": config.username,
            }
        )
        max_age = max(3600, int(config.session_ttl_hours) * 3600)
        ssl_enabled = getattr(self.config_store.load_runtime_config().ssl, 'enabled', False)
        response.set_cookie(
            config.cookie_name,
            self._build_session_token(config.username, max_age),
            max_age=max_age,
            httponly=True,
            samesite="Lax",
            secure=ssl_enabled,
            path="/_admin",
        )
        return response

    async def logout(self, request: web.Request) -> web.Response:
        config = self._get_auth_config()
        response = self._json({"authenticated": False})
        response.del_cookie(config.cookie_name, path="/_admin")
        return response

    # ===== API 密钥管理（仅浏览器会话；API 密钥访问这里会被 _run_protected 拦下）=====

    async def list_api_keys(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: {"items": self.config_store.list_api_keys()})

    async def create_api_key(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)

        def operation():
            name = str(payload.get("name", "") or "").strip()
            if not name:
                raise ValueError("密钥名称不能为空")
            if len(name) > 64:
                raise ValueError("密钥名称过长（≤64 字符）")
            readonly = bool(payload.get("readonly", False))
            try:
                expires_days = int(payload.get("expires_days", 0) or 0)
            except (TypeError, ValueError):
                raise ValueError("有效期天数非法")
            if expires_days < 0 or expires_days > 3650:
                raise ValueError("有效期天数须在 0（永久）~ 3650 之间")
            created = self.config_store.create_api_key(name, readonly=readonly, expires_days=expires_days)
            logger.info("签发 API 密钥: name=%s readonly=%s expires_days=%s prefix=%s", name, readonly, expires_days, created["key_prefix"])
            return created

        return await self._run_protected(request, operation)

    async def toggle_api_key(self, request: web.Request) -> web.Response:
        key_id = int(request.match_info.get("key_id", "0") or 0)

        async def operation():
            payload = await self._read_json(request)
            enabled = bool(payload.get("enabled", True))
            result = self.config_store.set_api_key_enabled(key_id, enabled)
            logger.info("API 密钥 %s 已%s", key_id, "启用" if enabled else "停用")
            return result

        return await self._run_protected(request, operation)

    async def delete_api_key(self, request: web.Request) -> web.Response:
        key_id = int(request.match_info.get("key_id", "0") or 0)

        def operation():
            if not self.config_store.delete_api_key(key_id):
                raise KeyError(f"API 密钥 {key_id} 不存在")
            self._api_key_touch_at.pop(key_id, None)
            logger.info("API 密钥 %s 已删除（吊销）", key_id)
            return {"deleted": True, "id": key_id}

        return await self._run_protected(request, operation)

    async def bootstrap(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self.config_store.get_dashboard_data())

    # ===== API 文档自动维护（从路由表汇总，新增接口自动出现）=====

    async def api_doc_catalog(self, request: web.Request) -> web.Response:
        """返回全部 /_admin/api/* 接口清单（路径/方法/分组/参数/示例）。

        文档页只读此端点即可渲染；机制上不依赖手工登记——
        未登记 handler 也会出现在清单里（仅标「待补充说明」）。
        """
        return await self._run_protected(request, lambda: self._build_api_catalog())

    def _build_api_catalog(self) -> Dict[str, Any]:
        specs = _API_DOC_SPECS
        routes = [
            r for r in self.app.router.routes()
            if getattr(r, "method", "*") != "*"  # 跳过静态资源等通配路由
        ]
        # aiohttp 的 add_get 默认 allow_head=True，会为同一 handler 再注册一条 HEAD 路由
        # （HEAD 与对应 GET 完全同义，仅不返回响应体）。文档里逐条列出属于重复噪音，
        # 因此先收集「GET 路由的 (path, handler)」，再把同 handler 的 HEAD 去重掉。
        # 注：用 handler.__func__ 而非绑定方法本身做键，避免每次访问生成的绑定方法对象不同。
        get_routes: set = set()
        for route in routes:
            if route.method != "GET":
                continue
            handler = getattr(route, "handler", None)
            if handler is None:
                continue
            path = getattr(getattr(route, "resource", None), "canonical", "") or ""
            get_routes.add((path, getattr(handler, "__func__", handler)))

        entries: list = []
        seen: set = set()
        for route in routes:
            method = getattr(route, "method", "*")
            resource = getattr(route, "resource", None)
            path = getattr(resource, "canonical", "") or ""
            if not path.startswith("/_admin/api/"):
                continue
            if path in ("/_admin/api/doc",):  # 不把文档自身列入
                continue
            # 登录/登出/公钥属于鉴权握手，不纳入 API 文档
            if path in ("/_admin/api/auth/login", "/_admin/api/auth/logout", "/_admin/api/auth/public-key"):
                continue
            handler = getattr(route, "handler", None)
            # 去掉 aiohttp 为 GET 自动注册的 HEAD 重复项（同 handler + 同路径）
            if method == "HEAD" and handler is not None:
                if (path, getattr(handler, "__func__", handler)) in get_routes:
                    continue
            name = getattr(handler, "__name__", "") if handler else ""
            key = f"{method} {path}"
            if key in seen:
                continue
            seen.add(key)
            spec = specs.get(name, {})
            doc = {
                "method": method,
                "path": path,
                "tag": spec.get("tag", "其他"),
                "summary": spec.get("summary", ""),
                "params": spec.get("params", []),
                "body_sample": spec.get("body_sample", ""),
                "note": spec.get("note", ""),
                "usage": _API_DOC_USAGE.get(name, ""),
                "documented": bool(spec),
            }
            entries.append(doc)
        entries.sort(key=lambda e: (e["tag"], e["method"], e["path"]))
        return {"items": entries}

    async def list_route_groups(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: {"items": self.config_store.list_route_groups()})

    async def create_route_group(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        async def operation():
            result = self.config_store.create_route_group(payload)
            await self.reload_callback()
            return result
        return await self._run_protected(request, operation, status=201)

    async def update_route_group(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        async def operation():
            path_prefix = str(payload.get("path_prefix") or payload.get("old_path_prefix") or "").strip()
            result = self.config_store.update_route_group(path_prefix, payload)
            await self.reload_callback()
            return result
        return await self._run_protected(request, operation)

    async def delete_route_group(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        async def operation():
            path_prefix = str(payload.get("path_prefix", "")).strip()
            request_host = str(payload.get("request_host", "")).strip()
            self.config_store.delete_route_group(path_prefix, request_host)
            await self.reload_callback()
            return {"deleted": True, "path_prefix": path_prefix, "request_host": request_host}
        return await self._run_protected(request, operation)

    async def get_geoip(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self.config_store.get_geoip_settings())

    async def update_geoip(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        async def operation():
            result = self.config_store.update_geoip_settings(payload)
            await self.reload_callback()
            return result
        return await self._run_protected(request, operation)

    async def test_geoip(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)

        async def operation():
            ip_address = str(payload.get("ip", "")).strip()
            source_payload = payload.get("source")
            if not ip_address:
                raise ValueError("测试 IP 不能为空。")
            if not isinstance(source_payload, dict):
                raise ValueError("测试在线定位源时必须提供 source 配置对象。")

            parsed_config = Config._parse_config(
                {
                    "geoip": {
                        "enabled": True,
                        "sources": [source_payload],
                    }
                }
            )
            if not parsed_config.geoip.sources:
                raise ValueError("无法解析当前在线定位源配置。")

            resolver = GeoResolver()
            try:
                return await resolver.test_online_source(ip_address, parsed_config.geoip.sources[0])
            finally:
                await resolver.close()

        return await self._run_protected(request, operation)

    async def clear_geoip_online_cache(self, request: web.Request) -> web.Response:
        await self._read_json(request)

        async def operation():
            if self.online_cache_clear_callback is None:
                raise ValueError("在线定位缓存清理服务不可用。")
            return await self.online_cache_clear_callback()

        return await self._run_protected(request, operation)

    async def test_offline_geoip(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)

        async def operation():
            ip_address = str(payload.get("ip", "")).strip()
            if not ip_address:
                raise ValueError("测试 IP 不能为空。")

            geoip_payload = payload.get("geoip")
            offline_payload = payload.get("offline")
            if isinstance(geoip_payload, dict):
                parse_source = {"geoip": geoip_payload}
            elif isinstance(offline_payload, dict):
                parse_source = {"geoip": {"enabled": True, "offline": offline_payload}}
            else:
                parse_source = {"geoip": self.config_store.get_geoip_settings()}

            parsed_config = Config._parse_config(parse_source)
            resolver = GeoResolver()
            try:
                return await resolver.test_offline_database(ip_address, parsed_config.geoip)
            finally:
                await resolver.close()

        return await self._run_protected(request, operation)

    async def sync_offline_geoip(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)

        async def operation():
            geoip_payload = payload.get("geoip")
            if isinstance(geoip_payload, dict):
                self.config_store.update_geoip_settings(geoip_payload)
                await self.reload_callback()

            if self.offline_sync_callback is None:
                raise ValueError("离线 IP 库同步服务不可用。")

            return await self.offline_sync_callback()

        return await self._run_protected(request, operation)

    async def rollback_offline_geoip(self, request: web.Request) -> web.Response:
        await self._read_json(request)

        async def operation():
            if self.offline_rollback_callback is None:
                raise ValueError("离线 IP 库回滚服务不可用。")
            return await self.offline_rollback_callback()

        return await self._run(operation)

    async def list_route_logs(self, request: web.Request) -> web.Response:
        filters = {
            "keyword": request.query.get("keyword", ""),
            "path_prefix": request.query.get("path_prefix", ""),
            "rule_request_host": request.query.get("rule_request_host", ""),
            "match_strategy": request.query.get("match_strategy", ""),
            "result_status": request.query.get("result_status", ""),
            "referer": request.query.get("referer", ""),
            "date_from": request.query.get("date_from", ""),
            "date_to": request.query.get("date_to", ""),
            "limit": request.query.get("limit", "50"),
            "page": request.query.get("page", "1"),
        }
        return await self._run_protected(request, lambda: self.config_store.list_route_logs(filters))

    async def get_hotlink_stats(self, request: web.Request) -> web.Response:
        try:
            hours = int(request.query.get("hours", "24") or 24)
        except ValueError:
            hours = 24
        return await self._run_protected(
            request, lambda: self.config_store.get_hotlink_stats(hours)
        )

    async def delete_route_logs(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self.config_store.delete_route_logs(payload))

    async def get_route_log_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self.config_store.get_route_log_settings())

    async def update_route_log_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self.config_store.update_route_log_settings(payload))

    async def get_logging_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self.config_store.get_logging_settings())

    async def update_logging_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self._update_logging_settings(payload))

    def _update_logging_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.config_store.update_logging_settings(payload)
        if self.reload_callback:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.reload_callback())
            else:
                loop.run_until_complete(self.reload_callback())
        return result

    async def list_app_log_files(self, request: web.Request) -> web.Response:
        config = self.config_store.load_runtime_config()
        log_path = config.logging.file_path
        if not log_path:
            return self._json({"items": [], "current": ""})
        from pathlib import Path
        import glob as glob_module
        log_file = Path(log_path)
        log_dir = log_file.parent
        log_name = log_file.name
        patterns = [
            str(log_dir / f"{log_name}.*"),
            str(log_dir / "*.log"),
            str(log_dir / "*.log.*"),
        ]
        seen = set()
        files = []
        for pattern in patterns:
            for f in glob_module.glob(pattern):
                p = Path(f)
                if p.name in seen or not p.is_file():
                    continue
                seen.add(p.name)
                try:
                    stat = p.stat()
                except OSError:
                    continue
                files.append({
                    "name": p.name,
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                    "is_current": p.name == log_name,
                })
        files.sort(key=lambda x: x["modified"], reverse=True)
        current_name = log_name
        return self._json({"items": files, "current": current_name})

    async def get_app_log_content(self, request: web.Request) -> web.Response:
        config = self.config_store.load_runtime_config()
        log_path = config.logging.file_path
        if not log_path:
            return self._json({"content": "", "total_lines": 0})
        from pathlib import Path
        import asyncio
        from collections import deque
        
        file_name = request.query.get("file", "")
        keyword = request.query.get("keyword", "").strip()
        tail_lines = min(int(request.query.get("tail", "100") or "100"), 2000)
        offset = int(request.query.get("offset", "0") or "0")
        
        display_limit = tail_lines
        
        if file_name:
            target = Path(log_path).parent / file_name
        else:
            target = Path(log_path)
        if not target.exists() or not target.is_file():
            return self._json({"content": "", "total_lines": 0, "error": "文件不存在"})
        
        def _read_log():
            try:
                if keyword:
                    matched = []
                    total_lines = 0
                    kw_lower = keyword.lower()
                    with open(target, "r", encoding="utf-8", errors="replace") as f:
                        for line in f:
                            total_lines += 1
                            if kw_lower in line.lower():
                                matched.append(line)
                                if len(matched) > 5000:
                                    matched = matched[-3000:]
                    total_matched = len(matched)
                    if offset > 0:
                        end = max(0, total_matched - offset)
                        start = max(0, end - display_limit)
                        selected = matched[start:end]
                    else:
                        selected = matched[-display_limit:] if len(matched) > display_limit else matched
                    content = "".join(selected)
                    return {
                        "content": content,
                        "total_lines": total_lines,
                        "matched_lines": total_matched,
                        "file": target.name,
                    }
                else:
                    total_lines = 0
                    last_lines = deque(maxlen=display_limit + offset)
                    with open(target, "r", encoding="utf-8", errors="replace") as f:
                        for line in f:
                            total_lines += 1
                            last_lines.append(line)
                    if offset >= len(last_lines):
                        selected = []
                    else:
                        end_idx = len(last_lines) - offset
                        start_idx = max(0, end_idx - display_limit)
                        selected = list(last_lines)[start_idx:end_idx]
                    content = "".join(selected)
                    return {
                        "content": content,
                        "total_lines": total_lines,
                        "matched_lines": total_lines,
                        "file": target.name,
                    }
            except Exception as e:
                return {"content": "", "total_lines": 0, "error": str(e)}
        
        result = await asyncio.get_event_loop().run_in_executor(None, _read_log)
        return self._json(result)

    async def get_ip_cache_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self._get_ip_cache_settings())

    def _get_ip_cache_settings(self) -> Dict[str, Any]:
        config = self.config_store.load_runtime_config()
        return {
            "enabled": config.ip_result_cache.enabled,
            "ttl_seconds": config.ip_result_cache.ttl_seconds,
            "max_entries": config.ip_result_cache.max_entries,
        }

    async def update_ip_cache_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self._update_ip_cache_settings(payload))

    def _update_ip_cache_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.config_store.update_ip_cache_config(payload)
        if self.reload_callback:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.reload_callback())
            else:
                loop.run_until_complete(self.reload_callback())
        return {"message": "请求结果缓存配置已更新", **result}

    async def get_dedup_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self._get_dedup_settings())

    def _get_dedup_settings(self) -> Dict[str, Any]:
        config = self.config_store.load_runtime_config()
        return {
            "enabled": config.request_dedup.enabled,
            "window_seconds": config.request_dedup.window_seconds,
            "max_cache_entries": config.request_dedup.max_cache_entries,
        }

    async def update_dedup_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self._update_dedup_settings(payload))

    def _update_dedup_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.config_store.update_dedup_config(payload)
        if self.reload_callback:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.reload_callback())
            else:
                loop.run_until_complete(self.reload_callback())
        return {"message": "请求去重配置已更新", **result}

    async def get_auto_ban_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self._get_auto_ban_settings())

    def _get_auto_ban_settings(self) -> Dict[str, Any]:
        config = self.config_store.load_runtime_config()
        return {
            "enabled": config.auto_ban.enabled,
            "window_seconds": config.auto_ban.window_seconds,
            "max_requests": config.auto_ban.max_requests,
            "ban_duration_seconds": config.auto_ban.ban_duration_seconds,
            "max_404": config.auto_ban.max_404,
            "auto_ban_on_404": config.auto_ban.auto_ban_on_404,
            "whitelist": config.auto_ban.whitelist,
            "email_on_ban": config.auto_ban.email_on_ban,
            "max_bytes": config.auto_ban.max_bytes,
        }

    async def update_auto_ban_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self._update_auto_ban_settings(payload))

    def _update_auto_ban_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.config_store.update_auto_ban_config(payload)
        if self.reload_callback:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.reload_callback())
            else:
                loop.run_until_complete(self.reload_callback())
        return {"message": "自动封禁配置已更新", **result}

    async def get_stream_guard_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self.config_store.get_stream_guard_config())

    async def update_stream_guard_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self._update_stream_guard_settings(payload))

    def _update_stream_guard_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.config_store.update_stream_guard_config(payload)
        if self.reload_callback:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.reload_callback())
            else:
                loop.run_until_complete(self.reload_callback())
        return {"message": "单 IP 并发限制已更新", **result}

    async def get_signed_url_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self.config_store.get_signed_url_config())

    async def update_signed_url_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self._update_signed_url_settings(payload))

    def _update_signed_url_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.config_store.update_signed_url_config(payload)
        if self.reload_callback:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.reload_callback())
            else:
                loop.run_until_complete(self.reload_callback())
        return {"message": "签名 URL 配置已更新", **result}

    async def generate_signed_url(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        path = str(payload.get("path", "") or "").strip()
        # 防御：仅允许以 / 开头的相对路径，禁止 // 与空字节，防止开放重定向注入
        if not path.startswith("/") or path.startswith("//") or "\x00" in path:
            return self._json({"error": "路径必须以单个 / 开头且不含非法字符"}, status=400)

        def _generate() -> Dict[str, Any]:
            try:
                url = self.config_store.generate_signed_url(path)
            except ValueError as exc:
                return {"error": str(exc)}
            return {"url": url}

        return await self._run_protected(request, _generate)

    async def get_redirect_signing_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self.config_store.get_redirect_signing_config())

    async def update_redirect_signing_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self._update_redirect_signing_settings(payload))

    def _update_redirect_signing_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.config_store.update_redirect_signing_config(payload)
        if self.reload_callback:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.reload_callback())
            else:
                loop.run_until_complete(self.reload_callback())
        return {"message": "302 加签改写配置已更新", **result}

    async def get_email_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self._get_email_settings())

    def _get_email_settings(self) -> Dict[str, Any]:
        config = self.config_store.load_runtime_config()
        return {
            "enabled": config.email.enabled,
            "smtp_host": config.email.smtp_host,
            "smtp_port": config.email.smtp_port,
            "smtp_ssl": config.email.smtp_ssl,
            "sender": config.email.sender,
            "sender_name": config.email.sender_name,
            "password": config.email.password,
            "recipients": config.email.recipients,
            "block_link_base_url": config.email.block_link_base_url,
            "alert_window_seconds": config.email.alert_window_seconds,
            "alert_max_requests": config.email.alert_max_requests,
            "alert_max_404": config.email.alert_max_404,
            "alert_cooldown_minutes": config.email.alert_cooldown_minutes,
        }

    async def update_email_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self._update_email_settings(payload))

    def _update_email_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.config_store.update_email_config(payload)
        if self.reload_callback:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.reload_callback())
            else:
                loop.run_until_complete(self.reload_callback())
        return {"message": "邮件提醒配置已更新", **result}

    async def test_email(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self._test_email(payload))

    def _test_email(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        from email_notifier import EmailNotifier
        from config import EmailConfig
        
        password = str(payload.get("password", "") or "")
        masked_password = password[:2] + "****" if len(password) > 2 else "****"
        logger.info("测试邮件配置: smtp_host=%s, sender=%s, password=%s", 
                    payload.get("smtp_host", ""), payload.get("sender", ""), masked_password)
        
        email_config = EmailConfig(
            enabled=True,
            smtp_host=str(payload.get("smtp_host", "") or ""),
            smtp_port=max(1, int(payload.get("smtp_port", 465) or 465)),
            smtp_ssl=bool(payload.get("smtp_ssl", True)),
            sender=str(payload.get("sender", "") or ""),
            sender_name=str(payload.get("sender_name", "") or ""),
            password=password,
            recipients=str(payload.get("recipients", "") or ""),
        )
        
        template_type = str(payload.get("template_type", "alert") or "alert")
        
        notifier = EmailNotifier(email_config)
        success, message = notifier.send_test_email_sync(email_config, template_type)
        
        return {"success": success, "message": message}

    async def list_banned_ips(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: {"items": self.config_store.list_banned_ips()})

    async def add_banned_ip(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        async def operation():
            result = self.config_store.add_banned_ip(payload)
            if self.ban_manager_callback:
                await self.ban_manager_callback("ban", payload)
            return result
        return await self._run_protected(request, operation, status=201)

    async def remove_banned_ip(self, request: web.Request) -> web.Response:
        ip = request.match_info["ip"]
        async def operation():
            removed = self.config_store.remove_banned_ip(ip)
            if not removed:
                raise KeyError(f"IP {ip} 不在封禁列表中")
            if self.ban_manager_callback:
                await self.ban_manager_callback("unban", {"ip": ip})
            return {"removed": True, "ip": ip}
        return await self._run_protected(request, operation)

    async def extend_banned_ip(self, request: web.Request) -> web.Response:
        ip = request.match_info["ip"]
        payload = await self._read_json(request)
        async def operation():
            duration_hours = float(payload.get("duration_hours", 0) or 0)
            if duration_hours <= 0:
                raise ValueError("延长时长必须大于0")
            result = self.config_store.extend_banned_ip(ip, duration_hours)
            if self.ban_manager_callback:
                await self.ban_manager_callback("extend", {
                    "ip": ip,
                    "duration_seconds": duration_hours * 3600.0,
                })
            return result
        return await self._run_protected(request, operation)

    async def clear_banned_ips(self, request: web.Request) -> web.Response:
        await self._read_json(request)
        async def operation():
            count = self.config_store.clear_all_banned_ips()
            if self.ban_manager_callback:
                await self.ban_manager_callback("clear", {})
            return {"cleared_count": count}
        return await self._run_protected(request, operation)

    async def list_rules(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: {"items": self.config_store.list_rules()})

    async def create_rule(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        async def operation():
            result = self.config_store.create_rule(payload)
            await self.reload_callback()
            return result
        return await self._run_protected(request, operation, status=201)

    async def get_rule(self, request: web.Request) -> web.Response:
        rule_id = int(request.match_info["rule_id"])
        return await self._run_protected(request, lambda: self.config_store.get_rule(rule_id))

    async def update_rule(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        rule_id = int(request.match_info["rule_id"])
        async def operation():
            result = self.config_store.update_rule(rule_id, payload)
            await self.reload_callback()
            return result
        return await self._run_protected(request, operation)

    async def delete_rule(self, request: web.Request) -> web.Response:
        rule_id = int(request.match_info["rule_id"])
        async def operation():
            self.config_store.delete_rule(rule_id)
            await self.reload_callback()
            return {"deleted": True, "id": rule_id}
        return await self._run_protected(request, operation)

    # ===== 备份与恢复 =====

    async def list_backups(self, request: web.Request) -> web.Response:
        async def operation():
            items = []
            for f in sorted(self.backup_dir.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True):
                stat = f.stat()
                items.append({
                    "filename": f.name,
                    "size": stat.st_size,
                    "created_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(timespec="seconds"),
                })
            return {"items": items}
        return await self._run_protected(request, operation)

    async def create_backup(self, request: web.Request) -> web.Response:
        async def operation():
            db_path = self.config_store.db_path
            if not db_path.exists():
                raise ValueError("数据库文件不存在，无法创建备份。")
            now = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"backup_{now}.db"
            backup_path = self.backup_dir / backup_filename
            shutil.copy2(str(db_path), str(backup_path))
            return {
                "filename": backup_filename,
                "size": backup_path.stat().st_size,
            }
        return await self._run_protected(request, operation, status=201)

    async def download_backup(self, request: web.Request) -> web.Response:
        filename = request.match_info["filename"]
        if not filename.endswith(".db") or "/" in filename or "\\" in filename:
            return self._json({"error": "无效的文件名"}, status=400)

        if self._is_auth_enabled() and not self._is_authenticated(request):
            return self._json({"error": "未登录或登录已失效。"}, status=401)

        backup_path = self.backup_dir / filename
        if not backup_path.exists():
            return self._json({"error": "备份文件不存在"}, status=404)
        return web.FileResponse(
            backup_path,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )

    async def restore_backup(self, request: web.Request) -> web.Response:
        async def operation():
            reader = await request.multipart()
            restore_mode = ""
            backup_filename = ""
            uploaded_file: Optional[bytes] = None

            while True:
                part = await reader.next()
                if part is None:
                    break
                field_name = part.name
                if field_name == "restore_mode":
                    restore_mode = (await part.read()).decode("utf-8").strip()
                elif field_name == "backup_filename":
                    backup_filename = (await part.read()).decode("utf-8").strip()
                elif field_name == "file":
                    uploaded_file = await part.read()

            if restore_mode not in ("overwrite", "merge"):
                raise ValueError("恢复模式必须是 overwrite 或 merge。")

            db_path = self.config_store.db_path

            if uploaded_file:
                if restore_mode == "overwrite":
                    with open(str(db_path), "wb") as f:
                        f.write(uploaded_file)
                    await self.reload_callback()
                    return {"message": "数据库已覆盖恢复，服务配置已重新加载。", "mode": "overwrite"}
                else:
                    return await self._merge_import(uploaded_file)

            if backup_filename:
                backup_path = self.backup_dir / backup_filename
                if not backup_path.exists():
                    raise ValueError(f"备份文件 {backup_filename} 不存在。")
                if restore_mode == "overwrite":
                    shutil.copy2(str(backup_path), str(db_path))
                    await self.reload_callback()
                    return {"message": f"已从 {backup_filename} 覆盖恢复，服务配置已重新加载。", "mode": "overwrite"}
                else:
                    with open(str(backup_path), "rb") as f:
                        backup_data = f.read()
                    return await self._merge_import(backup_data)

            raise ValueError("请提供上传文件或指定备份文件名。")

        return await self._run_protected(request, operation)

    async def _merge_import(self, backup_data: bytes) -> Dict[str, Any]:
        db_path = self.config_store.db_path
        current_conn = sqlite3.connect(str(db_path))
        backup_conn = sqlite3.connect(":memory:")
        try:
            sql_text = backup_data.decode("utf-8", errors="replace") if isinstance(backup_data, bytes) else backup_data
            backup_conn.executescript(sql_text)

            results = {}

            # --- 单行配置表：用备份数据覆盖当前值 ---
            single_row_tables = [
                "system_settings",
                "feature_flags",
                "remote_config_sources",
                "geoip_settings",
                "route_log_settings",
            ]
            for table in single_row_tables:
                backup_row = backup_conn.execute(f"SELECT * FROM {table} WHERE id = 1").fetchone()
                if backup_row:
                    columns = [desc[0] for desc in backup_conn.execute(f"SELECT * FROM {table} LIMIT 0").description]
                    row_dict = dict(zip(columns, backup_row))
                    set_clause = ", ".join(f"{col} = ?" for col in columns if col != "id")
                    vals = [row_dict[col] for col in columns if col != "id"]
                    current_conn.execute(f"UPDATE {table} SET {set_clause} WHERE id = 1", vals)
                    results[table] = "已覆盖"

            # --- forward_rules：按 (path_prefix, request_host, target_url) 去重插入 ---
            if self._table_exists(backup_conn, "forward_rules"):
                existing_rules = set()
                for row in current_conn.execute(
                    "SELECT path_prefix, request_host, target_url FROM forward_rules"
                ):
                    existing_rules.add((row[0] or "", row[1] or "", row[2] or ""))

                columns = [desc[0] for desc in backup_conn.execute("SELECT * FROM forward_rules LIMIT 0").description]
                insert_cols = [c for c in columns if c != "id"]
                inserted = 0
                skipped = 0
                for row in backup_conn.execute("SELECT * FROM forward_rules").fetchall():
                    row_dict = dict(zip(columns, row))
                    key = (row_dict.get("path_prefix", ""), row_dict.get("request_host", ""), row_dict.get("target_url", ""))
                    if key in existing_rules:
                        skipped += 1
                        continue
                    vals = [row_dict[c] for c in insert_cols]
                    placeholders = ", ".join(["?"] * len(insert_cols))
                    col_names = ", ".join(insert_cols)
                    current_conn.execute(
                        f"INSERT OR IGNORE INTO forward_rules ({col_names}) VALUES ({placeholders})",
                        vals,
                    )
                    inserted += 1
                results["forward_rules"] = f"新增 {inserted} 条，跳过 {skipped} 条"

            # --- route_groups：按 PK (request_host, path_prefix) 去重插入 ---
            if self._table_exists(backup_conn, "route_groups"):
                existing_groups = set()
                for row in current_conn.execute("SELECT request_host, path_prefix FROM route_groups"):
                    existing_groups.add((row[0] or "", row[1] or ""))
                columns = [desc[0] for desc in backup_conn.execute("SELECT * FROM route_groups LIMIT 0").description]
                insert_cols = [c for c in columns if c not in ("request_host", "path_prefix")]
                inserted = 0
                skipped = 0
                for row in backup_conn.execute("SELECT * FROM route_groups").fetchall():
                    row_dict = dict(zip(columns, row))
                    key = (row_dict.get("request_host", ""), row_dict.get("path_prefix", ""))
                    if key in existing_groups:
                        skipped += 1
                        continue
                    all_cols = ["request_host", "path_prefix"] + insert_cols
                    vals = [row_dict[c] for c in all_cols]
                    placeholders = ", ".join(["?"] * len(all_cols))
                    col_names = ", ".join(all_cols)
                    current_conn.execute(
                        f"INSERT OR IGNORE INTO route_groups ({col_names}) VALUES ({placeholders})",
                        vals,
                    )
                    inserted += 1
                results["route_groups"] = f"新增 {inserted} 条，跳过 {skipped} 条"

            # --- geoip_online_sources：按 name 去重插入 ---
            if self._table_exists(backup_conn, "geoip_online_sources"):
                existing_sources = set()
                for row in current_conn.execute("SELECT name FROM geoip_online_sources"):
                    existing_sources.add(row[0] or "")
                columns = [desc[0] for desc in backup_conn.execute("SELECT * FROM geoip_online_sources LIMIT 0").description]
                insert_cols = [c for c in columns if c != "id"]
                inserted = 0
                skipped = 0
                for row in backup_conn.execute("SELECT * FROM geoip_online_sources").fetchall():
                    row_dict = dict(zip(columns, row))
                    if row_dict.get("name", "") in existing_sources:
                        skipped += 1
                        continue
                    vals = [row_dict[c] for c in insert_cols]
                    placeholders = ", ".join(["?"] * len(insert_cols))
                    col_names = ", ".join(insert_cols)
                    current_conn.execute(
                        f"INSERT OR IGNORE INTO geoip_online_sources ({col_names}) VALUES ({placeholders})",
                        vals,
                    )
                    inserted += 1
                results["geoip_online_sources"] = f"新增 {inserted} 条，跳过 {skipped} 条"

            current_conn.commit()
            await self.reload_callback()

            summary_parts = [f"{tbl}: {msg}" for tbl, msg in results.items()]
            return {
                "message": "合并导入完成：" + "；".join(summary_parts),
                "mode": "merge",
                "details": results,
            }
        except sqlite3.DatabaseError as exc:
            raise ValueError(f"备份文件不是有效的 SQLite 数据库: {exc}")
        finally:
            current_conn.close()
            backup_conn.close()

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return row is not None

    async def delete_backup(self, request: web.Request) -> web.Response:
        filename = request.match_info["filename"]
        async def operation():
            if not filename.endswith(".db") or "/" in filename or "\\" in filename:
                raise ValueError("无效的文件名。")
            backup_path = self.backup_dir / filename
            if not backup_path.exists():
                raise KeyError(f"备份文件 {filename} 不存在。")
            backup_path.unlink()
            return {"deleted": True, "filename": filename}
        return await self._run_protected(request, operation)

    async def cleanup_log_files(self, request: web.Request) -> web.Response:
        async def operation():
            if self.log_cleanup_callback is None:
                raise ValueError("日志清理服务不可用。")
            return await self.log_cleanup_callback()

        return await self._run_protected(request, operation)

    async def cleanup_log_files_on_disk(self, request: web.Request) -> web.Response:
        async def operation():
            if self.log_file_cleanup_callback is None:
                raise ValueError("日志文件清理服务不可用。")
            return await self.log_file_cleanup_callback()

        return await self._run_protected(request, operation)

    # ===== 内部工具方法 =====

    async def _read_json(self, request: web.Request) -> Dict[str, Any]:
        try:
            payload = await request.json()
        except json.JSONDecodeError as exc:
            raise web.HTTPBadRequest(text=f"Invalid JSON payload: {exc}") from exc
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="JSON payload must be an object.")
        return payload

    def _json(self, data: Dict[str, Any], status: int = 200) -> web.Response:
        response = web.Response(
            status=status,
            headers={"Content-Type": "application/json; charset=utf-8"},
            body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        )
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        return response

    def _get_auth_config(self):
        return self.config_store.bootstrap_config.admin_auth

    # ===== API 密钥鉴权（仅 /_admin/api/* 接口；/_admin HTML 页面与登录态不接受）=====

    _API_KEY_TOUCH_INTERVAL = 60.0

    @staticmethod
    def _extract_api_key(request: web.Request) -> str:
        """从请求头提取 API 密钥明文：Authorization: Bearer n302_xxx 或 X-API-Key: n302_xxx。"""
        auth_header = str(request.headers.get("Authorization", "") or "")
        if auth_header.lower().startswith("bearer "):
            return auth_header[7:].strip()
        return str(request.headers.get("X-API-Key", "") or "").strip()

    def _api_key_row(self, request: web.Request) -> Optional[Dict[str, Any]]:
        """校验请求头中的 API 密钥；有效返回密钥行（sha256 精确命中唯一索引），无效返回 None。"""
        raw_key = self._extract_api_key(request)
        if not raw_key:
            return None
        try:
            return self.config_store.find_api_key(raw_key)
        except Exception:  # noqa: BLE001 - 鉴权失败一律拒绝，绝不因异常放行
            return None

    def _touch_api_key_throttled(self, key_id: int) -> None:
        """更新密钥最近使用时间/次数；同一密钥 60 秒内至多落库一次（避免写放大）。"""
        now = time.monotonic()
        last = self._api_key_touch_at.get(key_id, 0.0)
        if now - last < self._API_KEY_TOUCH_INTERVAL:
            return
        self._api_key_touch_at[key_id] = now
        try:
            asyncio.get_running_loop().run_in_executor(
                None, self.config_store.touch_api_key, key_id,
            )
        except Exception:  # noqa: BLE001 - 统计失败不影响请求
            pass


    def _is_auth_enabled(self) -> bool:
        config = self._get_auth_config()
        return bool(config.enabled and config.username and config.password)

    def _build_session_token(self, username: str, max_age: int) -> str:
        expires_at = int(time.time()) + max_age
        payload = f"{username}|{expires_at}"
        signature = hmac.new(
            self._session_secret(),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"{payload}|{signature}"

    def _session_secret(self) -> bytes:
        config = self._get_auth_config()
        # 优先使用独立的 session_secret
        if hasattr(config, 'session_secret') and config.session_secret:
            return config.session_secret.encode("utf-8")
        # 回退到原有逻辑（兼容）
        return f"{config.username}\n{config.password}\n{config.cookie_name}".encode("utf-8")

    def _is_authenticated(self, request: web.Request) -> bool:
        if not self._is_auth_enabled():
            return True

        config = self._get_auth_config()
        token = request.cookies.get(config.cookie_name, "")
        if not token:
            return False

        try:
            username, expires_at_text, signature = token.split("|", 2)
            expires_at = int(expires_at_text)
        except ValueError:
            return False

        if username != config.username or expires_at <= int(time.time()):
            return False

        payload = f"{username}|{expires_at}"
        expected_signature = hmac.new(
            self._session_secret(),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(signature, expected_signature)

    async def _run_protected(self, request: web.Request, operation, status: int = 200) -> web.Response:
        if self._is_auth_enabled() and not self._is_authenticated(request):
            # Cookie 会话无效时回落校验 API 密钥（仅 /_admin/api/*；HTML 页面不走这里）
            key_row = self._api_key_row(request)
            if key_row is None:
                return self._json({"error": "未登录、登录已失效或 API 密钥无效。"}, status=401)
            if key_row["readonly"] and request.method not in ("GET", "HEAD", "OPTIONS"):
                return self._json({"error": "只读 API 密钥不允许执行写操作。"}, status=403)
            if request.path.startswith("/_admin/api/keys"):
                return self._json({"error": "API 密钥不允许管理 API 密钥，请使用浏览器会话操作。"}, status=403)
            self._touch_api_key_throttled(int(key_row["id"]))
        return await self._run(operation, status=status)

    async def _run(self, operation, status: int = 200) -> web.Response:
        try:
            result = operation()
            if inspect.isawaitable(result):
                result = await result
            return self._json(result, status=status)
        except KeyError as exc:
            return self._json({"error": str(exc)}, status=404)
        except ValueError as exc:
            return self._json({"error": str(exc)}, status=400)
