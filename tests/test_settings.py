from __future__ import annotations

import pytest
from pydantic import ValidationError
from sakuramedia_115_provider.settings import Cloud115ProviderSettings


def test_defaults_to_single_hls_worker() -> None:
    assert Cloud115ProviderSettings().thumbnail_hls_max_workers == 1


@pytest.mark.parametrize("value", [0, 17])
def test_rejects_workers_out_of_range(value: int) -> None:
    with pytest.raises(ValidationError):
        Cloud115ProviderSettings(thumbnail_hls_max_workers=value)


def test_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Cloud115ProviderSettings(unknown_field=1)
