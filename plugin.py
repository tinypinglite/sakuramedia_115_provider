"""Registration and configuration for the 115 provider."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

from src.plugins import (
    HOST_API_VERSION,
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

from .cloud115 import (
    Cloud115Client,
    exchange_web_cookie_for_alipaymini,
    run_sync,
)
from .exceptions import Cloud115AuthError, Cloud115Error, Cloud115NotFoundError
from .offline import Cloud115OfflineDownloadComponent

PLUGIN_ID = "sakuramedia_115_provider"
DISPLAY_NAME = "115 网盘"
VERSION = json.loads(
    Path(__file__).with_name("manifest.json").read_text(encoding="utf-8")
)["version"]

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
            raise _error("prepare_library", "invalid_config", "115 配置的目录不存在") from exc
        except Cloud115Error as exc:
            raise _cloud_error("prepare_library", exc) from exc

    async def _prepare(
        self,
        web_cookie: str,
        media_root_path: str,
        downloads_root_path: str,
        previous: LibraryHandle | None,
    ) -> PreparedLibrary:
        previous_config = previous.provider_config if previous is not None else {}
        device_cookie = previous_config.get("device_cookie") if isinstance(previous_config, dict) else None
        reusable = (
            isinstance(device_cookie, str)
            and bool(device_cookie)
            and previous_config.get("web_cookie") == web_cookie
        )
        if reusable:
            async with Cloud115Client(device_cookie) as client:
                if not await client.check_alive():
                    reusable = False
        if not reusable:
            device_cookie = await exchange_web_cookie_for_alipaymini(web_cookie)
        assert isinstance(device_cookie, str)
        async with Cloud115Client(device_cookie) as client:
            if not await client.check_alive():
                raise Cloud115AuthError("115 专用设备 Cookie 已失效")
            account_uid = client.user_id
            media_root = await _resolve_directory_path(client, media_root_path)
            downloads_root = await _resolve_directory_path(client, downloads_root_path)
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
