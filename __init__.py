"""SakuraMedia 115 cloud-storage provider plugin."""

from .plugin import register
from .settings import Cloud115ProviderSettings

__all__ = ["Cloud115ProviderSettings", "register"]
