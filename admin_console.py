from __future__ import annotations

import hashlib
import hmac
import json
import inspect
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

    def register(self, app: web.Application) -> None:
        app.router.add_get("/_admin", self.index)
        app.router.add_get("/_admin/", self.index)
        app.router.add_static("/_admin/static/", str(self.static_dir), show_index=False)
        app.router.add_get("/_admin/api/auth/status", self.auth_status)
        app.router.add_post("/_admin/api/auth/login", self.login)
        app.router.add_post("/_admin/api/auth/logout", self.logout)
        app.router.add_get("/_admin/api/bootstrap", self.bootstrap)
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
        app.router.add_get("/_admin/api/log-settings", self.get_route_log_settings)
        app.router.add_put("/_admin/api/log-settings", self.update_route_log_settings)
        app.router.add_get("/_admin/api/app-logs", self.list_app_log_files)
        app.router.add_get("/_admin/api/app-logs/content", self.get_app_log_content)
        app.router.add_get("/_admin/api/ip-cache-settings", self.get_ip_cache_settings)
        app.router.add_put("/_admin/api/ip-cache-settings", self.update_ip_cache_settings)
        app.router.add_get("/_admin/api/banned-ips", self.list_banned_ips)
        app.router.add_post("/_admin/api/banned-ips", self.add_banned_ip)
        app.router.add_delete("/_admin/api/banned-ips/{ip:.+}", self.remove_banned_ip)
        app.router.add_post("/_admin/api/banned-ips/clear", self.clear_banned_ips)
        app.router.add_post("/_admin/api/banned-ips/{ip:.+}/extend", self.extend_banned_ip)
        app.router.add_get("/_admin/api/auto-ban", self.get_auto_ban_settings)
        app.router.add_put("/_admin/api/auto-ban", self.update_auto_ban_settings)
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
        return web.FileResponse(self.static_dir / "admin.html")

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

    async def login(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        config = self._get_auth_config()
        if not self._is_auth_enabled():
            return self._json({"enabled": False, "authenticated": True, "username": ""})

        username = str(payload.get("username", "")).strip()
        password = str(payload.get("password", ""))
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
        response.set_cookie(
            config.cookie_name,
            self._build_session_token(config.username, max_age),
            max_age=max_age,
            httponly=True,
            samesite="Lax",
            path="/_admin",
        )
        return response

    async def logout(self, request: web.Request) -> web.Response:
        config = self._get_auth_config()
        response = self._json({"authenticated": False})
        response.del_cookie(config.cookie_name, path="/_admin")
        return response

    async def bootstrap(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self.config_store.get_dashboard_data())

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
            "date_from": request.query.get("date_from", ""),
            "date_to": request.query.get("date_to", ""),
            "limit": request.query.get("limit", "50"),
            "page": request.query.get("page", "1"),
        }
        return await self._run_protected(request, lambda: self.config_store.list_route_logs(filters))

    async def delete_route_logs(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self.config_store.delete_route_logs(payload))

    async def get_route_log_settings(self, request: web.Request) -> web.Response:
        return await self._run_protected(request, lambda: self.config_store.get_route_log_settings())

    async def update_route_log_settings(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        return await self._run_protected(request, lambda: self.config_store.update_route_log_settings(payload))

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
        file_name = request.query.get("file", "")
        keyword = request.query.get("keyword", "").strip()
        tail_lines = int(request.query.get("tail", "500") or "500")
        if file_name:
            target = Path(log_path).parent / file_name
        else:
            target = Path(log_path)
        if not target.exists() or not target.is_file():
            return self._json({"content": "", "total_lines": 0, "error": "文件不存在"})
        try:
            with open(target, "r", encoding="utf-8", errors="replace") as f:
                all_lines = f.readlines()
        except Exception as e:
            return self._json({"content": "", "total_lines": 0, "error": str(e)})
        total_lines = len(all_lines)
        if keyword:
            matched = [line for line in all_lines if keyword.lower() in line.lower()]
            content = "".join(reversed(matched[-tail_lines:]))
            matched_count = len(matched)
        else:
            content = "".join(reversed(all_lines[-tail_lines:]))
            matched_count = total_lines
        return self._json({
            "content": content,
            "total_lines": total_lines,
            "matched_lines": matched_count,
            "file": target.name,
        })

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
        
        email_config = EmailConfig(
            enabled=True,
            smtp_host=str(payload.get("smtp_host", "") or ""),
            smtp_port=max(1, int(payload.get("smtp_port", 465) or 465)),
            smtp_ssl=bool(payload.get("smtp_ssl", True)),
            sender=str(payload.get("sender", "") or ""),
            sender_name=str(payload.get("sender_name", "") or ""),
            password=str(payload.get("password", "") or ""),
            recipients=str(payload.get("recipients", "") or ""),
        )
        
        # 获取模板类型，默认为alert
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
        await self._read_json(request)

        async def operation():
            if self.log_cleanup_callback is None:
                raise ValueError("日志清理服务不可用。")
            return await self.log_cleanup_callback()

        return await self._run_protected(request, operation)

    async def cleanup_log_files_on_disk(self, request: web.Request) -> web.Response:
        await self._read_json(request)

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
        return web.Response(
            status=status,
            headers={"Content-Type": "application/json; charset=utf-8"},
            body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        )

    def _get_auth_config(self):
        return self.config_store.bootstrap_config.admin_auth

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
            return self._json({"error": "未登录或登录已失效。"}, status=401)
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
