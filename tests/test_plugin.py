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


def _bundle(tmp_path: Path):
    return plugin.register(
        PluginContext(plugin_id=plugin.PLUGIN_ID, settings={}, data_dir=tmp_path / "data")
    ).extensions[0].data


def _previous(**overrides) -> LibraryHandle:
    config = {
        "web_cookie": "UID=123456_A1_x",
        "device_app": "alipaymini",
        "device_cookie": "UID=123456_R2_x",
        "account_uid": "123456",
        "media_root_path": "/媒体/电影",
        "downloads_root_path": "/下载/SakuraMedia",
        "media_root_cid": "movies",
        "downloads_root_cid": "downloads-root",
    }
    config.update(overrides)
    return LibraryHandle(1, "cloud115", config, "123456")


def _submitted(**overrides) -> dict:
    config = {
        "web_cookie": "UID=123456_A1_x",
        "media_root_path": "/媒体/电影",
        "downloads_root_path": "/下载/SakuraMedia",
    }
    config.update(overrides)
    return config


def test_prepare_exchanges_web_cookie_and_resolves_configured_roots(
    monkeypatch, tmp_path: Path
) -> None:
    async def exchange(web_cookie: str, *, device_app: str) -> str:
        assert web_cookie == "UID=123456_A1_x"
        assert device_app == "alipaymini"
        return "UID=123456_R2_x"

    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_device", exchange)
    bundle = _bundle(tmp_path)

    prepared = bundle.prepare_library(submitted_config=_submitted(), previous=None)

    assert prepared.account_key == "123456"
    assert prepared.provider_config == {
        "web_cookie": "UID=123456_A1_x",
        "device_app": "alipaymini",
        "device_cookie": "UID=123456_R2_x",
        "account_uid": "123456",
        "media_root_path": "/媒体/电影",
        "downloads_root_path": "/下载/SakuraMedia",
        "media_root_cid": "movies",
        "downloads_root_cid": "downloads-root",
    }


def test_prepare_exchanges_selected_wechatmini_device(monkeypatch, tmp_path: Path) -> None:
    requested: list[str] = []

    async def exchange(web_cookie: str, *, device_app: str) -> str:
        requested.append(device_app)
        return "UID=123456_R1_x"

    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_device", exchange)
    bundle = _bundle(tmp_path)

    prepared = bundle.prepare_library(
        submitted_config=_submitted(device_app="wechatmini"), previous=None
    )

    assert requested == ["wechatmini"]
    assert prepared.provider_config["device_app"] == "wechatmini"
    assert prepared.provider_config["device_cookie"] == "UID=123456_R1_x"


def test_prepare_rejects_unknown_device_app(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    bundle = _bundle(tmp_path)

    with pytest.raises(ProviderOperationError) as error:
        bundle.prepare_library(
            submitted_config=_submitted(device_app="android"), previous=None
        )

    assert error.value.code == "invalid_config"


def test_prepare_uses_pasted_device_cookie_without_web_cookie(
    monkeypatch, tmp_path: Path
) -> None:
    async def exchange(_web_cookie: str, *, device_app: str) -> str:
        raise AssertionError("手动填写的设备 Cookie 不应触发换取")

    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_device", exchange)
    bundle = _bundle(tmp_path)

    prepared = bundle.prepare_library(
        submitted_config=_submitted(web_cookie="", device_cookie="UID=123456_R2_x"),
        previous=None,
    )

    assert prepared.provider_config["web_cookie"] == ""
    assert prepared.provider_config["device_app"] == "alipaymini"
    assert prepared.provider_config["device_cookie"] == "UID=123456_R2_x"


def test_prepare_rejects_missing_credentials(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    bundle = _bundle(tmp_path)

    with pytest.raises(ProviderOperationError) as error:
        bundle.prepare_library(
            submitted_config=_submitted(web_cookie="", device_cookie=""),
            previous=None,
        )

    assert error.value.code == "invalid_config"


def test_prepare_reuses_alive_device_cookie(monkeypatch, tmp_path: Path) -> None:
    async def exchange(_web_cookie: str, *, device_app: str) -> str:
        raise AssertionError("存活的设备 Cookie 不应触发换取")

    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_device", exchange)
    bundle = _bundle(tmp_path)

    prepared = bundle.prepare_library(
        submitted_config={
            "media_root_path": "/媒体/电影",
            "downloads_root_path": "/下载/SakuraMedia",
        },
        previous=_previous(device_app="wechatmini", device_cookie="UID=123456_R1_x"),
    )

    assert prepared.provider_config["web_cookie"] == "UID=123456_A1_x"
    assert prepared.provider_config["device_cookie"] == "UID=123456_R1_x"
    assert prepared.provider_config["device_app"] == "wechatmini"


def test_prepare_regenerates_when_device_cookie_cleared(monkeypatch, tmp_path: Path) -> None:
    exchanged: list[str] = []

    async def exchange(web_cookie: str, *, device_app: str) -> str:
        exchanged.append(device_app)
        return "UID=123456_R2_new"

    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_device", exchange)
    bundle = _bundle(tmp_path)

    prepared = bundle.prepare_library(
        submitted_config=_submitted(device_cookie=""),
        previous=_previous(),
    )

    assert exchanged == ["alipaymini"]
    assert prepared.provider_config["device_cookie"] == "UID=123456_R2_new"


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

    async def exchange(web_cookie: str, *, device_app: str) -> str:
        exchanged.append(device_app)
        return "fresh-device-cookie"

    monkeypatch.setattr(plugin, "Cloud115Client", ExpiringClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_device", exchange)
    bundle = _bundle(tmp_path)

    prepared = bundle.prepare_library(
        submitted_config=_submitted(),
        previous=_previous(device_cookie="expired-device-cookie"),
    )

    assert exchanged == ["alipaymini"]
    assert prepared.provider_config["device_cookie"] == "fresh-device-cookie"


def test_prepare_switching_device_app_re_exchanges(monkeypatch, tmp_path: Path) -> None:
    exchanged: list[str] = []

    async def exchange(web_cookie: str, *, device_app: str) -> str:
        exchanged.append(device_app)
        return "UID=123456_R1_x"

    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_device", exchange)
    bundle = _bundle(tmp_path)

    prepared = bundle.prepare_library(
        submitted_config=_submitted(device_app="wechatmini"),
        previous=_previous(),
    )

    assert exchanged == ["wechatmini"]
    assert prepared.provider_config["device_app"] == "wechatmini"
    assert prepared.provider_config["device_cookie"] == "UID=123456_R1_x"


def test_prepare_rejects_missing_configured_directory(monkeypatch, tmp_path: Path) -> None:
    async def exchange(_web_cookie: str, *, device_app: str) -> str:
        return "UID=123456_R2_x"

    monkeypatch.setattr(plugin, "Cloud115Client", FakeClient)
    monkeypatch.setattr(plugin, "exchange_web_cookie_for_device", exchange)
    bundle = _bundle(tmp_path)

    with pytest.raises(ProviderOperationError) as error:
        bundle.prepare_library(
            submitted_config=_submitted(media_root_path="/不存在"), previous=None
        )

    assert error.value.code == "invalid_config"
