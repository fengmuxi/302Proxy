from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp

from config_store import ConfigStore, utc_now
from geo_service import GeoResolver

try:
    import geoip2.database

    GEOIP2_VALIDATION_AVAILABLE = True
except ImportError:
    geoip2 = None
    GEOIP2_VALIDATION_AVAILABLE = False


logger = logging.getLogger("proxy")


class OfflineGeoIPSyncService:
    def __init__(
        self,
        config_store: ConfigStore,
        geo_resolver: GeoResolver,
        *,
        check_interval_seconds: int = 60,
    ) -> None:
        self.config_store = config_store
        self.geo_resolver = geo_resolver
        self.check_interval_seconds = max(15, int(check_interval_seconds or 60))
        self._task: Optional[asyncio.Task] = None
        self._sync_lock = asyncio.Lock()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop(), name="offline-geoip-sync")

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def sync_now(self, *, force: bool = True) -> Dict[str, Any]:
        async with self._sync_lock:
            geoip_settings = self.config_store.get_geoip_settings()
            offline = dict(geoip_settings.get("offline", {}) or {})

            if not force and not self._is_due_for_sync(offline):
                offline = dict(self.config_store.get_geoip_settings().get("offline", {}) or {})
                return {
                    "status": "skipped",
                    "message": "离线 IP 库当前未到同步时间。",
                    "offline": offline,
                }

            enabled = bool(offline.get("enabled"))
            download_url = str(offline.get("download_url", "") or "").strip()
            db_path = str(offline.get("db_path", "") or "").strip()

            if not enabled:
                raise ValueError("请先启用离线 IP 库功能。")
            if not db_path:
                raise ValueError("请先配置离线 IP 库本地路径。")
            if not download_url:
                raise ValueError("请先配置离线 IP 库下载链接。")

            sync_started_at = utc_now()
            self.config_store.update_offline_geoip_sync_state(
                status="running",
                message="开始下载离线 IP 库。",
                last_sync_at=sync_started_at,
            )

            target_path = Path(db_path)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = target_path.with_suffix(f"{target_path.suffix}.download")
            backup_path = self._build_backup_path(target_path)
            downloaded_bytes = 0
            backup_created = False

            try:
                headers = self._parse_headers(offline.get("download_headers_json", "{}"))
                timeout = aiohttp.ClientTimeout(total=900)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(download_url, headers=headers, ssl=False) as response:
                        response.raise_for_status()
                        with temp_path.open("wb") as file_obj:
                            async for chunk in response.content.iter_chunked(1024 * 256):
                                if not chunk:
                                    continue
                                file_obj.write(chunk)
                                downloaded_bytes += len(chunk)

                if downloaded_bytes <= 0:
                    raise ValueError("下载结果为空，未生成可用的离线 IP 库文件。")

                self._validate_database_file(temp_path)
                if target_path.exists():
                    target_path.replace(backup_path)
                    backup_created = True
                temp_path.replace(target_path)
                await self.geo_resolver.invalidate_offline_cache()

                sync_finished_at = utc_now()
                backup_message = f"，上一版备份已保存到 {backup_path}" if backup_created else ""
                message = (
                    f"离线 IP 库下载成功，已保存到 {target_path}，大小 {downloaded_bytes} 字节{backup_message}。"
                )
                offline = self.config_store.update_offline_geoip_sync_state(
                    status="success",
                    message=message,
                    last_sync_at=sync_finished_at,
                    last_success_at=sync_finished_at,
                )["offline"]
                logger.info(message)
                return {
                    "status": "success",
                    "message": message,
                    "downloaded_bytes": downloaded_bytes,
                    "offline": offline,
                }
            except Exception as exc:
                if backup_created and not target_path.exists() and backup_path.exists():
                    backup_path.replace(target_path)
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
                message = f"离线 IP 库同步失败: {exc}"
                self.config_store.update_offline_geoip_sync_state(
                    status="error",
                    message=message,
                    last_sync_at=utc_now(),
                )
                logger.error(message)
                raise ValueError(message) from exc

    async def rollback_now(self) -> Dict[str, Any]:
        async with self._sync_lock:
            geoip_settings = self.config_store.get_geoip_settings()
            offline = dict(geoip_settings.get("offline", {}) or {})

            db_path = str(offline.get("db_path", "") or "").strip()
            if not db_path:
                raise ValueError("请先配置离线 IP 库本地路径。")

            target_path = Path(db_path)
            backup_path = self._build_backup_path(target_path)
            rollback_swap_path = target_path.with_suffix(f"{target_path.suffix}.rollback")

            if not backup_path.exists():
                raise ValueError("当前没有可用的离线库备份，无法回滚。")

            try:
                if target_path.exists():
                    target_path.replace(rollback_swap_path)
                backup_path.replace(target_path)
                if rollback_swap_path.exists():
                    rollback_swap_path.replace(backup_path)
                await self.geo_resolver.invalidate_offline_cache()

                rollback_time = utc_now()
                message = f"离线 IP 库已回滚到备份版本，当前文件: {target_path}"
                offline = self.config_store.update_offline_geoip_sync_state(
                    status="rolled_back",
                    message=message,
                    last_sync_at=rollback_time,
                )["offline"]
                logger.warning(message)
                return {
                    "status": "rolled_back",
                    "message": message,
                    "offline": offline,
                }
            except Exception as exc:
                if rollback_swap_path.exists() and not target_path.exists():
                    rollback_swap_path.replace(target_path)
                message = f"离线 IP 库回滚失败: {exc}"
                self.config_store.update_offline_geoip_sync_state(
                    status="rollback_error",
                    message=message,
                    last_sync_at=utc_now(),
                )
                logger.error(message)
                raise ValueError(message) from exc

    async def _run_loop(self) -> None:
        while True:
            try:
                geoip_settings = self.config_store.get_geoip_settings()
                offline = dict(geoip_settings.get("offline", {}) or {})
                if self._is_due_for_sync(offline):
                    await self.sync_now(force=False)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Offline GeoIP scheduled sync skipped because of an error: %s", exc)
            await asyncio.sleep(self.check_interval_seconds)

    def _is_due_for_sync(self, offline: Dict[str, Any]) -> bool:
        if not bool(offline.get("enabled")):
            return False
        if not str(offline.get("download_url", "") or "").strip():
            return False
        if not str(offline.get("db_path", "") or "").strip():
            return False

        status = dict(offline.get("status", {}) or {})
        file_exists = bool(status.get("file_exists"))
        if not file_exists:
            return True

        baseline_text = (
            str(status.get("last_sync_at", "") or "").strip()
            or str(status.get("last_success_at", "") or "").strip()
        )
        if not baseline_text:
            return True

        try:
            baseline = datetime.fromisoformat(baseline_text)
        except ValueError:
            return True

        refresh_hours = max(1, int(offline.get("refresh_interval_hours", 24) or 24))
        return datetime.now(timezone.utc) >= baseline + timedelta(hours=refresh_hours)

    def _parse_headers(self, value: Any) -> Dict[str, str]:
        if value in (None, ""):
            return {}
        if isinstance(value, dict):
            return {
                str(key).strip(): str(item)
                for key, item in value.items()
                if str(key).strip()
            }
        try:
            parsed = json.loads(str(value))
        except json.JSONDecodeError as exc:
            raise ValueError(f"离线库下载请求头 JSON 格式无效: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError("离线库下载请求头必须是 JSON 对象。")
        return {
            str(key).strip(): str(item)
            for key, item in parsed.items()
            if str(key).strip()
        }

    def _validate_database_file(self, file_path: Path) -> None:
        if not file_path.exists():
            raise ValueError("下载后的离线库文件不存在。")
        if file_path.stat().st_size <= 0:
            raise ValueError("下载后的离线库文件大小为 0。")
        if not GEOIP2_VALIDATION_AVAILABLE:
            return
        reader = geoip2.database.Reader(str(file_path))
        reader.close()

    def _build_backup_path(self, target_path: Path) -> Path:
        suffix = target_path.suffix or ""
        if suffix:
            return target_path.with_suffix(f"{suffix}.bak")
        return target_path.with_name(f"{target_path.name}.bak")
