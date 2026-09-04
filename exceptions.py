"""Narrow error types for the 115 HTTP adapter."""

from __future__ import annotations

import re


class Cloud115Error(RuntimeError):
    pass


class Cloud115AuthError(Cloud115Error):
    pass


class Cloud115NotFoundError(Cloud115Error):
    pass


class Cloud115DuplicateNameError(Cloud115Error):
    pass


class Cloud115RequestError(Cloud115Error):
    pass


class Cloud115RiskControlError(Cloud115Error):
    pass


class Cloud115OfflineTaskExistsError(Cloud115Error):
    pass


class Cloud115VideoUnavailableError(Cloud115Error):
    pass


class Cloud115CipherError(Cloud115Error):
    pass


def safe_error_message(exc: BaseException) -> str:
    """Keep upstream diagnostics without logging URLs or 115 Cookie values."""
    message = re.sub(r"(?i)\b(?:https?://|magnet:)[^\s<>]+", "[URL]", str(exc))
    message = re.sub(
        r"(?i)\b(UID|CID|SEID|KID)\s*=\s*[^;\s,]+",
        r"\1=[REDACTED]",
        message,
    )
    return " ".join(message.split())[:512]
