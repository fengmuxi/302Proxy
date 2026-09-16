"""OpenList 上游适配客户端。

职责：封装 OpenList 的登录与取链 API，把「请求路径」换算为「可直接代理/跳转的直链 +
需携带的请求头」。对应 OpenList 官方「开发自己的代理程序」指引：

    1. POST {base}/api/auth/login   {username, password}          -> data.token
    2. POST {base}/api/fs/link      {path, password?, type?}       -> data.url + data.header
       （请求头携带 Authorization: <token>）

支持两种鉴权方式（auth_mode）：
- 'password'：用账号密码调 /api/auth/login 换取令牌，令牌进程内缓存 + 落库，
  失效（业务码 401）时自动重登一次；适合"我有 OpenList 账号"的场景。
- 'token'   ：直接用后台配置的令牌请求接口，**全程不登录**；令牌由用户在连接配置里
  粘贴维护，本客户端永不改写它。适合"OpenList 关闭了密码登录 / 只发了一个长期令牌"
  或不想把账号密码存在本系统的场景。

代理侧不依赖 OpenList 自身签名：拿到直链后自行决定 A 模式（302 直连）或
B 模式（服务端拉流回传），从而实现「OpenList 对外只认代理」的防盗链效果。

设计要点：
- 客户端实例按 (base_url, auth_mode, 账号, 凭据指纹) 进程内缓存（token 随之缓存），
  凭据变更时自动重建；凭据进缓存键前先做指纹，避免令牌明文出现在字典键里；
- password 模式 token 失效自动重登一次；token 模式不重登，直接抛出可读错误
  （**绝不回落**去用账号密码登录，否则就绕过了用户「只用令牌」的意图）；
- 所有异常统一收敛为 OpenListError 子类，便于上层转 502 并写路由日志。
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Callable, Dict, Optional, Tuple

import aiohttp

logger = logging.getLogger("proxy")

# 鉴权方式常量（与 config_store.OPENLIST_AUTH_* 保持一致）
AUTH_MODE_PASSWORD = "password"
AUTH_MODE_TOKEN = "token"


def normalize_token(value: Any) -> str:
    """令牌归一化：去空白、容忍把 'Bearer ' 前缀一起粘进来。"""
    token = str(value or "").strip().strip('"').strip("'").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token


def token_fingerprint(token: str) -> str:
    """令牌指纹：仅用于进程内缓存键，避免令牌明文出现在字典键中。"""
    if not token:
        return "-"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


class OpenListError(Exception):
    """OpenList 交互失败的基类。"""


class OpenListAuthError(OpenListError):
    """鉴权失败（凭据错误 / 令牌无效 / 服务不可达 / 未配置基地址或令牌）。"""


class OpenListLinkError(OpenListError):
    """取链失败（路径不存在 / 受密码保护 / 上游返回非成功码）。"""


class OpenListClient:
    """单个 OpenList 实例的轻量客户端（password 模式下 token 进程内缓存）。"""

    def __init__(
        self,
        base_url: str,
        username: str = "",
        password: str = "",
        *,
        auth_mode: str = AUTH_MODE_PASSWORD,
        token: str = "",
        manual_token: str = "",
        timeout: float = 30.0,
        on_token_refreshed: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.base_url = (base_url or "").strip().rstrip("/")
        self.auth_mode = AUTH_MODE_TOKEN if str(auth_mode or "").strip().lower() == AUTH_MODE_TOKEN else AUTH_MODE_PASSWORD
        self.username = username or ""
        self.password = password or ""
        self.timeout = float(timeout or 30.0)
        # token 模式：令牌来自用户配置（manual_token），本客户端绝不改写；
        # password 模式：token 是登录缓存，可由登录流程刷新。
        self.manual_token = normalize_token(manual_token)
        if self.auth_mode == AUTH_MODE_TOKEN:
            self._token = self.manual_token
        else:
            self._token = str(token or "")
        # 可选回调：token 变更时回写持久层（失败不影响主流程）
        self._on_token_refreshed = on_token_refreshed

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    @property
    def uses_manual_token(self) -> bool:
        return self.auth_mode == AUTH_MODE_TOKEN

    def _api(self, path: str) -> str:
        return f"{self.base_url}{path}"

    async def _post_json(
        self,
        session: aiohttp.ClientSession,
        api_path: str,
        body: Dict[str, Any],
        *,
        with_auth: bool,
    ) -> Dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json;charset=UTF-8",
        }
        if with_auth and self._token:
            headers["Authorization"] = self._token
        try:
            async with session.post(
                self._api(api_path),
                json=body,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                status = resp.status
                try:
                    payload = await resp.json(content_type=None)
                except Exception as exc:  # noqa: BLE001 - 需兼容非 JSON 错误页
                    text = await resp.text()
                    raise OpenListError(
                        f"OpenList 返回非 JSON 响应 (HTTP {status}): {text[:200]}"
                    ) from exc
        except aiohttp.ClientError as exc:
            raise OpenListError(f"OpenList 请求失败: {exc}") from exc
        if not isinstance(payload, dict):
            raise OpenListError("OpenList 返回结构异常（非对象）")
        return payload

    async def login(self, session: aiohttp.ClientSession) -> str:
        """登录并缓存 token（仅 password 模式；token 模式明确拒绝）。

        token 模式若在这里"顺手"用账号密码登录，就等于绕过了用户「只用令牌」的选择，
        也违背了「不把账号密码交给本系统」的初衷，因此直接抛错而非静默降级。
        """
        if not self.enabled:
            raise OpenListAuthError("未配置 OpenList 基地址 (base_url)")
        if self.uses_manual_token:
            raise OpenListAuthError(
                "该连接鉴权方式为「直接使用令牌」，不执行账号密码登录；请在连接配置中更新令牌"
            )
        payload = await self._post_json(
            session,
            "/api/auth/login",
            {"username": self.username, "password": self.password},
            with_auth=False,
        )
        if int(payload.get("code", 0) or 0) != 200:
            raise OpenListAuthError(f"OpenList 登录失败: {payload.get('message') or payload}")
        data = payload.get("data") or {}
        token = str((data or {}).get("token") or "").strip()
        if not token:
            raise OpenListAuthError("OpenList 登录成功但未返回 token")
        self._token = token
        if self._on_token_refreshed:
            try:
                self._on_token_refreshed(token)
            except Exception as exc:  # pragma: no cover - 回写失败不影响主流程
                logger.warning("OpenList token 回写回调失败: %s", exc)
        return token

    async def ensure_token(self, session: aiohttp.ClientSession) -> str:
        if self._token:
            return self._token
        if self.uses_manual_token:
            raise OpenListAuthError(
                "未配置 OpenList 令牌（连接鉴权方式为「直接使用令牌」），请在连接配置中填写"
            )
        return await self.login(session)

    @staticmethod
    def _extract_link(payload: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            raise OpenListLinkError("OpenList 取链返回结构异常（data 非对象）")
        url = str(data.get("url") or data.get("raw_url") or "").strip()
        if not url:
            raise OpenListLinkError("OpenList 取链返回为空（无 url/raw_url）")
        header = data.get("header") or {}
        if not isinstance(header, dict):
            header = {}
        return url, {str(k): str(v) for k, v in header.items()}

    async def verify_token(self, session: aiohttp.ClientSession) -> str:
        """token 模式自检：确认配置的令牌被上游接受。

        以 `/api/fs/link` 探测根路径：鉴权失败时 OpenList 返回 401（HTTP 或业务码），
        其余响应说明**令牌已通过鉴权**——根路径本身能否取到直链属正常业务结果，
        与令牌有效性无关，故不据此判失败。

        Returns:
            可直接展示给用户的说明文本。
        """
        if not self.enabled:
            raise OpenListAuthError("未配置 OpenList 基地址 (base_url)")
        if not self.manual_token:
            raise OpenListAuthError("未配置 OpenList 令牌")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json;charset=UTF-8",
            "Authorization": self.manual_token,
        }
        try:
            async with session.post(
                self._api("/api/fs/link"),
                json={"path": "/"},
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                status = resp.status
                try:
                    payload: Any = await resp.json(content_type=None)
                except Exception:  # noqa: BLE001 - 非 JSON 错误页也应给出可读结论
                    payload = None
        except aiohttp.ClientError as exc:
            raise OpenListAuthError(f"OpenList 请求失败: {exc}") from exc
        code = int((payload or {}).get("code", 0) or 0) if isinstance(payload, dict) else 0
        if status == 401 or code == 401:
            raise OpenListAuthError("OpenList 令牌无效或已过期，请重新获取并更新令牌")
        if status == 403 or code == 403:
            raise OpenListAuthError("OpenList 令牌被拒绝（403）：权限不足或令牌已被吊销")
        if status >= 500:
            raise OpenListAuthError(f"OpenList 返回 HTTP {status}（服务端异常）")
        if status >= 400:
            raise OpenListAuthError(f"OpenList 返回 HTTP {status}，无法确认令牌有效性")
        return "令牌已被 OpenList 接受"

    async def _get_link_once(
        self, session: aiohttp.ClientSession, body: Dict[str, Any]
    ) -> Dict[str, Any]:
        await self.ensure_token(session)
        return await self._post_json(session, "/api/fs/link", body, with_auth=True)

    async def get_link(
        self,
        session: aiohttp.ClientSession,
        path: str,
        password: str = "",
        link_type: Optional[str] = None,
    ) -> Tuple[str, Dict[str, str]]:
        """取链：返回 (直链 URL, 需携带的请求头)。

        password 模式 token 失效（业务码 401）时强制重登一次后重试，避免偶发过期中断播放；
        token 模式不重登（没账号密码可用），直接抛出可读错误引导用户更新令牌。
        """
        if not self.enabled:
            raise OpenListError("未配置 OpenList 基地址 (base_url)")
        normalized_path = "/" + str(path or "").lstrip("/")
        body: Dict[str, Any] = {"path": normalized_path}
        if password:
            body["password"] = password
        if link_type:
            body["type"] = link_type

        payload = await self._get_link_once(session, body)
        if int(payload.get("code", 0) or 0) == 401:
            if self.uses_manual_token:
                raise OpenListAuthError(
                    "OpenList 令牌无效或已过期（连接鉴权方式为「直接使用令牌」），"
                    "请在连接配置中更新令牌"
                )
            self._token = ""
            await self.login(session)
            payload = await self._get_link_once(session, body)
        if int(payload.get("code", 0) or 0) != 200:
            raise OpenListLinkError(f"OpenList 取链失败: {payload.get('message') or payload}")
        return self._extract_link(payload)


# ===== 进程内客户端缓存（按 base_url + username 复用 token）=====

_clients: Dict[str, OpenListClient] = {}


def get_client(
    base_url: str,
    username: str = "",
    password: str = "",
    *,
    auth_mode: str = AUTH_MODE_PASSWORD,
    token: str = "",
    manual_token: str = "",
    timeout: float = 30.0,
    on_token_refreshed: Optional[Callable[[str], None]] = None,
) -> OpenListClient:
    """按 (base_url, auth_mode, 账号, 凭据指纹) 复用客户端，保证 token 缓存不串味。

    缓存键必须含凭据维度，否则多连接会撞键：
    - username：同一 OpenList 主机挂多个账号；
    - manual_token 指纹：token 模式下同一主机配多个不同令牌（用户名通常为空）时区分；
    - password 指纹：password 模式下同主机同账号但密码不同时区分。
    凭据一律先做指纹再入键，避免令牌/密码明文出现在进程内的字典键里。
    """
    norm_base = (base_url or "").strip().rstrip("/")
    mode = AUTH_MODE_TOKEN if str(auth_mode or "").strip().lower() == AUTH_MODE_TOKEN else AUTH_MODE_PASSWORD
    manual = normalize_token(manual_token)
    key = (
        f"{norm_base}|{mode}|{(username or '').strip()}"
        f"|{token_fingerprint(password)}|{token_fingerprint(manual)}"
    )
    client = _clients.get(key)
    if (
        client is None
        or client.auth_mode != mode
        or client.username != (username or "")
        or client.password != (password or "")
        or client.manual_token != manual
    ):
        client = OpenListClient(
            norm_base,
            username,
            password,
            auth_mode=mode,
            token=token,
            manual_token=manual,
            timeout=timeout,
            on_token_refreshed=on_token_refreshed,
        )
        _clients[key] = client
    return client


def reset_clients() -> None:
    """清空客户端缓存（配置变更 / 测试用）。"""
    _clients.clear()
