from __future__ import annotations

import json
from pathlib import Path

from sakuramedia_115_provider.plugin import PLUGIN_ID, register

from src.plugins import PluginContext
from src.plugins.extensions.media_provider import validate_media_provider_extension
from src.plugins.provider_protocol import MEDIA_PROVIDER_EXTENSION_KEY


def test_registration_declares_provider_and_manual_jobs(tmp_path: Path) -> None:
    manifest = json.loads((Path(__file__).parents[1] / "manifest.json").read_text(encoding="utf-8"))
    registration = register(
        PluginContext(plugin_id=PLUGIN_ID, settings={}, data_dir=tmp_path / "plugin-data")
    )
    extension = registration.extensions[0]
    bundle = validate_media_provider_extension(plugin_id=PLUGIN_ID, extension=extension)

    assert registration.host_api_version == manifest["host_api_version"]
    assert extension.key == MEDIA_PROVIDER_EXTENSION_KEY
    assert bundle.provider_key == "cloud115"
    assert bundle.playback_deliveries == ("redirect", "proxy")
    fields = {field.key: field for field in bundle.library_config_fields}
    assert fields["web_cookie"].input == "text"
    assert fields["web_cookie"].required is False
    assert fields["device_app"].input == "text"
    assert fields["device_app"].required is False
    assert fields["device_app"].read_only is False
    assert fields["device_cookie"].input == "text"
    assert fields["device_cookie"].required is False
    assert fields["device_cookie"].read_only is False
    assert bundle.downloads is not None

    assert [job.task_key for job in registration.jobs] == [
        "sakuramedia_115_cleanup_empty_media_dirs",
    ]
    assert all(job.manual_only for job in registration.jobs)
    assert all(job.params_schema is not None for job in registration.jobs)
