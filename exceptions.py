"""Narrow error types for the 115 HTTP adapter."""

from __future__ import annotations


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
