"""Registration and configuration for the 115 provider."""

from __future__ import annotations

import json
import time
from pathlib import Path, PurePosixPath

from loguru import logger

from src.plugins import (
    PluginContext,
    PluginExtension,
    PluginRegistration,
)
from src.plugins.provider_protocol import (
    MEDIA_PROVIDER_EXTENSION_KEY,
    ConfigField,
    JsonObject,
    LibraryHandle,
    PreparedLibrary,
    ProviderOperationError,
)
from src.scheduler.contracts import JobDefinition

from .cleanup import (
    CleanupConfirmParams,
    cleanup_empty_media_dirs,
    cleanup_imported_downloads,
)
from .cloud115 import (
    Cloud115Client,
    exchange_web_cookie_for_alipaymini,
    run_sync,
)
from .exceptions import (
    Cloud115AuthError,
    Cloud115Error,
    Cloud115NotFoundError,
    safe_error_message,
)
from .offline import Cloud115OfflineDownloadComponent

PLUGIN_ID = "sakuramedia_115_provider"
DISPLAY_NAME = "115 网盘"
MANIFEST = json.loads(
    Path(__file__).with_name("manifest.json").read_text(encoding="utf-8")
)
VERSION = MANIFEST["version"]
HOST_API_VERSION = MANIFEST["host_api_version"]

LIBRARY_CONFIG_FIELDS = (
    ConfigField(
        key="web_cookie",
        label="115 Web Cookie",
        input="secret",
        required=True,
        description="从 115 网页端复制 Cookie；保存时会换取独立的支付宝小程序设备登录。",
        multiline=True,
    ),
    ConfigField(
        key="media_root_path",
        label="115 媒体目录",
        input="path",
        required=True,
        description="导入媒体的目标目录，填 115 绝对路径。",
        hint="例如 /媒体/电影",
    ),
    ConfigField(
        key="downloads_root_path",
        label="115 离线下载目录",
        input="path",
        required=True,
        description="115 离线任务的保存目录，填 115 绝对路径。",
        hint="例如 /下载/视频",
    ),
    ConfigField(
        key="device_cookie",
        label="115 专用设备 Cookie",
        input="secret",
        required=False,
        read_only=True,
        description="由 Web Cookie 自动换取，供后台任务与播放使用。",
    ),
    ConfigField(
        key="account_uid",
        label="115 账号 UID",
        input="text",
        required=False,
        read_only=True,
    ),
    ConfigField(
        key="media_root_cid",
        label="解析后的 115 媒体目录 ID",
        input="text",
        required=False,
        read_only=True,
    ),
    ConfigField(
        key="downloads_root_cid",
        label="解析后的 115 离线下载目录 ID",
        input="text",
        required=False,
        read_only=True,
    ),
)


class Cloud115MediaProviderBundle:
    provider_key = "cloud115"
    display_name = DISPLAY_NAME
    library_config_fields = LIBRARY_CONFIG_FIELDS
    playback_deliveries = ("redirect", "proxy")
    merged_playback_format = "hls"

    def __init__(self, *, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.downloads = Cloud115OfflineDownloadComponent()

    def prepare_library(
        self,
        *,
        submitted_config: JsonObject,
        previous: LibraryHandle | None,
    ) -> PreparedLibrary:
        if not isinstance(submitted_config, dict):
            raise _error("prepare_library", "invalid_config", "115 配置无效")
        unknown = set(submitted_config) - {field.key for field in LIBRARY_CONFIG_FIELDS}
        if unknown:
            raise _error("prepare_library", "invalid_config", "115 配置字段无效")
        web_cookie = submitted_config.get("web_cookie")
        if not isinstance(web_cookie, str) or not web_cookie.strip():
            raise _error("prepare_library", "invalid_config", "请填写有效的 115 Web Cookie")
        media_root_path = _normalise_directory_path(submitted_config.get("media_root_path"))
        downloads_root_path = _normalise_directory_path(
            submitted_config.get("downloads_root_path")
        )
        try:
            return run_sync(
                self._prepare(
                    web_cookie.strip(),
                    media_root_path,
                    downloads_root_path,
                    previous,
                )
            )
        except Cloud115NotFoundError as exc:
            logger.warning("115 媒体库配置准备失败 media_root={} downloads_root={} error_type={} reason={}",
                           media_root_path, downloads_root_path, type(exc).__name__, safe_error_message(exc))
            raise _error("prepare_library", "invalid_config", "115 配置的目录不存在") from exc
        except Cloud115Error as exc:
            logger.warning("115 媒体库配置准备失败 media_root={} downloads_root={} error_type={} reason={}",
                           media_root_path, downloads_root_path, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("prepare_library", exc) from exc

    async def _prepare(
        self,
        web_cookie: str,
        media_root_path: str,
        downloads_root_path: str,
        previous: LibraryHandle | None,
    ) -> PreparedLibrary:
        started = time.monotonic()
        logger.info("115 媒体库配置准备开始 media_root={} downloads_root={}", media_root_path, downloads_root_path)
        previous_config = previous.provider_config if previous is not None else {}
        device_cookie = previous_config.get("device_cookie") if isinstance(previous_config, dict) else None
        reusable = (
            isinstance(device_cookie, str)
            and bool(device_cookie)
            and previous_config.get("web_cookie") == web_cookie
        )
        if reusable:
            logger.info("115 设备登录复用验证开始")
            async with Cloud115Client(device_cookie) as client:
                if not await client.check_alive():
                    reusable = False
                    logger.info("115 已有设备登录失效，准备重新换取")
        if not reusable:
            logger.info("115 设备登录换取开始")
            device_cookie = await exchange_web_cookie_for_alipaymini(web_cookie)
        assert isinstance(device_cookie, str)
        async with Cloud115Client(device_cookie) as client:
            if not await client.check_alive():
                raise Cloud115AuthError("115 专用设备 Cookie 已失效")
            account_uid = client.user_id
            logger.info("115 设备登录验证成功 account_uid={} reused={}", account_uid, reusable)
            media_root = await _resolve_directory_path(client, media_root_path)
            logger.info("115 媒体目录解析完成 cid={}", media_root)
            downloads_root = await _resolve_directory_path(client, downloads_root_path)
            logger.info("115 下载目录解析完成 cid={}", downloads_root)
        logger.info("115 媒体库配置准备完成 account_uid={} elapsed_seconds={:.2f}", account_uid, time.monotonic() - started)
        return PreparedLibrary(
            provider_config={
                "web_cookie": web_cookie,
                "device_cookie": device_cookie,
                "account_uid": account_uid,
                "media_root_path": media_root_path,
                "downloads_root_path": downloads_root_path,
                "media_root_cid": media_root,
                "downloads_root_cid": downloads_root,
            },
            account_key=account_uid,
        )

    def build_storage(self, *, library: LibraryHandle):
        from .storage import Cloud115StorageProvider

        return Cloud115StorageProvider(library=library, data_dir=self.data_dir)


def register(context: PluginContext) -> PluginRegistration:
    bundle = Cloud115MediaProviderBundle(data_dir=context.data_dir)

    return PluginRegistration(
        plugin_id=PLUGIN_ID,
        display_name=DISPLAY_NAME,
        version=VERSION,
        host_api_version=HOST_API_VERSION,
        jobs=(
            JobDefinition(
                task_key="sakuramedia_115_cleanup_imported_downloads",
                log_name="115-cleanup-imported-downloads",
                cli_name="115-cleanup-imported-downloads",
                cli_help="删除已导入的115离线下载任务目录",
                manual_only=True,
                params_schema=CleanupConfirmParams,
                handler=cleanup_imported_downloads,
            ),
            JobDefinition(
                task_key="sakuramedia_115_cleanup_empty_media_dirs",
                log_name="115-cleanup-empty-media-dirs",
                cli_name="115-cleanup-empty-media-dirs",
                cli_help="删除 115 媒体库目录下的空子目录",
                manual_only=True,
                params_schema=CleanupConfirmParams,
                handler=cleanup_empty_media_dirs,
            ),
        ),
        extensions=(PluginExtension(key=MEDIA_PROVIDER_EXTENSION_KEY, data=bundle),),
    )


def _cloud_error(operation: str, exc: Cloud115Error) -> ProviderOperationError:
    if isinstance(exc, Cloud115AuthError):
        return _error(operation, "authentication_failed", "115 登录已失效")
    if isinstance(exc, Cloud115NotFoundError):
        return _error(operation, "source_not_found", "115 目录不存在")
    return _error(operation, "unavailable", "115 服务暂不可用", retryable=True)


def _normalise_directory_path(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise _error("prepare_library", "invalid_config", "115 目录路径无效")
    path = PurePosixPath(value.strip())
    if path.anchor != "/" or ".." in path.parts:
        raise _error("prepare_library", "invalid_config", "115 目录路径必须是绝对路径")
    return str(path)


async def _resolve_directory_path(client: Cloud115Client, path: str) -> str:
    cid = "0"
    for part in PurePosixPath(path).parts[1:]:
        offset = 0
        while True:
            entries, total = await client.list_dir(cid, offset=offset)
            directory = next(
                (entry for entry in entries if entry.is_dir and entry.name == part),
                None,
            )
            if directory is not None:
                cid = directory.entry_id
                break
            offset += len(entries)
            if not entries or offset >= total:
                raise Cloud115NotFoundError("115 目录不存在")
    return cid


def _error(
    operation: str, code: str, message: str, *, retryable: bool = False
) -> ProviderOperationError:
    return ProviderOperationError(
        provider_key="cloud115",
        operation=operation,
        code=code,  # type: ignore[arg-type]
        safe_message=message,
        retryable=retryable,
    )
