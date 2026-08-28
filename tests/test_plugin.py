from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from sakuramedia_115_provider import plugin

from src.plugins import PluginContext
from src.plugins.provider_protocol import LibraryHandle, ProviderOperationError


class FakeClient:
    directories = {
        "0": (
            SimpleNamespace(is_dir=True, name="媒体", entry_id="media"),
            SimpleNamespace(is_dir=True, name="下载", entry_id="downloads"),
        ),
        "media": (SimpleNamespace(is_dir=True, name="电影", entry_id="movies"),),
        "downloads": (
            SimpleNamespace(
                is_dir=True,
                name="SakuraMedia",
                entry_id="downloads-root",
            ),
        ),
    }

    def __init__(self, _cookie: str) -> None:
        self.user_id = "123456"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def check_alive(self) -> bool:
        return True

    async def list_dir(self, cid: str, *, offset: int = 0, limit: int = 1000):
        entries = self.directories.get(cid, ())
        return entries[offset : offset + limit], len(entries)


def test_prepare_exchanges_web_cookie_and_resolves_configured_roots(
    monkeypatch, tmp_path: Path
) -> None:
    async def exchange(web_cookie: str) -> str:
        assert web_cookie == "UID=123456_A1_x"
        return "UID=123456_R2_x"

    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_alipaymini", exchange)
    bundle = plugin.register(
        PluginContext(plugin_id=plugin.PLUGIN_ID, settings={}, data_dir=tmp_path / "data")
    ).extensions[0].data

    prepared = bundle.prepare_library(
        submitted_config={
            "web_cookie": "UID=123456_A1_x",
            "media_root_path": "/媒体/电影",
            "downloads_root_path": "/下载/SakuraMedia",
        },
        previous=None,
    )

    assert prepared.account_key == "123456"
    assert prepared.provider_config == {
        "web_cookie": "UID=123456_A1_x",
        "device_cookie": "UID=123456_R2_x",
        "account_uid": "123456",
        "media_root_path": "/媒体/电影",
        "downloads_root_path": "/下载/SakuraMedia",
        "media_root_cid": "movies",
        "downloads_root_cid": "downloads-root",
    }


def test_prepare_replaces_an_expired_reusable_device_cookie(
    monkeypatch, tmp_path: Path
) -> None:
    class ExpiringClient(FakeClient):
        def __init__(self, cookie: str) -> None:
            super().__init__(cookie)
            self._cookie = cookie

        async def check_alive(self) -> bool:
            return self._cookie != "expired-device-cookie"

    exchanged: list[str] = []

    async def exchange(web_cookie: str) -> str:
        exchanged.append(web_cookie)
        return "fresh-device-cookie"

    monkeypatch.setattr(plugin, "Cloud115Client", ExpiringClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_alipaymini", exchange)
    bundle = plugin.register(
        PluginContext(plugin_id=plugin.PLUGIN_ID, settings={}, data_dir=tmp_path / "data")
    ).extensions[0].data
    previous = LibraryHandle(
        1,
        "cloud115",
        {
            "web_cookie": "UID=123456_A1_x",
            "device_cookie": "expired-device-cookie",
            "account_uid": "123456",
            "media_root_path": "/媒体/电影",
            "downloads_root_path": "/下载/SakuraMedia",
            "media_root_cid": "movies",
            "downloads_root_cid": "downloads-root",
        },
        "123456",
    )

    prepared = bundle.prepare_library(
        submitted_config={
            "web_cookie": "UID=123456_A1_x",
            "media_root_path": "/媒体/电影",
            "downloads_root_path": "/下载/SakuraMedia",
        },
        previous=previous,
    )

    assert exchanged == ["UID=123456_A1_x"]
    assert prepared.provider_config["device_cookie"] == "fresh-device-cookie"


def test_prepare_rejects_missing_configured_directory(monkeypatch, tmp_path: Path) -> None:
    async def exchange(_web_cookie: str) -> str:
        return "UID=123456_R2_x"

    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_alipaymini", exchange)
    bundle = plugin.register(
        PluginContext(plugin_id=plugin.PLUGIN_ID, settings={}, data_dir=tmp_path / "data")
    ).extensions[0].data

    with pytest.raises(ProviderOperationError) as error:
        bundle.prepare_library(
            submitted_config={
                "web_cookie": "UID=123456_A1_x",
                "media_root_path": "/不存在",
                "downloads_root_path": "/下载/SakuraMedia",
            },
            previous=None,
        )

    assert error.value.code == "invalid_config"
