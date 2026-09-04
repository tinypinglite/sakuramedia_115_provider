from __future__ import annotations

import json
from pathlib import Path

from sakuramedia_115_provider.plugin import DISPLAY_NAME, PLUGIN_ID, register

from src.plugins import PluginContext
from src.plugins.extensions.media_provider import validate_media_provider_extension
from src.plugins.provider_protocol import MEDIA_PROVIDER_EXTENSION_KEY


def test_registration_declares_v5_provider_bundle(tmp_path: Path) -> None:
    manifest = json.loads((Path(__file__).parents[1] / "manifest.json").read_text(encoding="utf-8"))
    registration = register(
        PluginContext(plugin_id=PLUGIN_ID, settings={}, data_dir=tmp_path / "plugin-data")
    )
    extension = registration.extensions[0]
    bundle = validate_media_provider_extension(plugin_id=PLUGIN_ID, extension=extension)

    assert manifest["plugin_id"] == PLUGIN_ID
    assert manifest["display_name"] == DISPLAY_NAME
    assert manifest["host_api_version"] == 5
    assert manifest["dependencies"] == []
    assert registration.host_api_version == 5
    assert extension.key == MEDIA_PROVIDER_EXTENSION_KEY
    assert bundle.provider_key == "cloud115"
    assert bundle.playback_deliveries == ("redirect", "proxy")
    assert [field.key for field in bundle.library_config_fields] == [
        "web_cookie",
        "media_root_path",
        "downloads_root_path",
        "device_cookie",
        "account_uid",
        "media_root_cid",
        "downloads_root_cid",
    ]
    assert bundle.library_config_fields[1].input == "path"
    assert bundle.library_config_fields[3].read_only is True
    assert bundle.library_config_fields[3].input == "secret"
    assert bundle.downloads is not None
    assert bundle.downloads.config_fields == ()
