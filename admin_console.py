from __future__ import annotations

import hashlib
import hmac
import json
import inspect
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict

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
    ):
        self.config_store = config_store
        self.reload_callback = reload_callback
        self.offline_sync_callback = offline_sync_callback
        self.offline_rollback_callback = offline_rollback_callback
        self.online_cache_clear_callback = online_cache_clear_callback
        self.static_dir = Path(__file__).resolve().parent / "static"

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
        app.router.add_get("/_admin/api/rules", self.list_rules)
        app.router.add_post("/_admin/api/rules", self.create_rule)
        app.router.add_get("/_admin/api/rules/{rule_id:\\d+}", self.get_rule)
        app.router.add_put("/_admin/api/rules/{rule_id:\\d+}", self.update_rule)
        app.router.add_delete("/_admin/api/rules/{rule_id:\\d+}", self.delete_rule)

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
            "limit": request.query.get("limit", "100"),
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
