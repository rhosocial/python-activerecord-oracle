# src/rhosocial/activerecord/backend/impl/oracle/show/backend_mixin.py
"""
Backend mixins for Oracle SHOW-style introspection.

Mirrors the MySQL `MySQLShowMixin` pattern: the mixin adds a `show()`
factory method returning an `OracleShowFunctionality` instance that
exposes the data-dictionary query helpers (`sessions`, `locks`, etc.).

The mixin is mixed into both `OracleBackend` (sync) and
`AsyncOracleBackend`; each picks its corresponding functionality class.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .functionality import OracleShowFunctionality, AsyncOracleShowFunctionality


class OracleShowMixin:
    """Sync backend mixin exposing the introspection helpers via `show()`.

    Example:
        backend.show().sessions(active_only=True)
        backend.show().database_info()
    """

    def show(self) -> "OracleShowFunctionality":
        return self._create_show_functionality()

    def _create_show_functionality(self) -> "OracleShowFunctionality":
        from .functionality import OracleShowFunctionality

        version = getattr(self, "_version", None)
        if version is None and hasattr(self, "get_server_version"):
            try:
                version = self.get_server_version()
            except Exception:
                version = None
        return OracleShowFunctionality(self, version)


class AsyncOracleShowMixin:
    """Async backend mixin exposing introspection helpers via `show()`.

    Example:
        await (await backend.show()).sessions()
        backend.show().sessions()  # returns a coroutine the caller awaits

    The async functionality collaborates with `AsyncOracleBackend.execute`
    which itself is awaited by the functionality helpers.
    """

    def show(self) -> "AsyncOracleShowFunctionality":
        return self._create_show_functionality()

    def _create_show_functionality(self) -> "AsyncOracleShowFunctionality":
        from .functionality import AsyncOracleShowFunctionality

        version = getattr(self, "_version", None)
        if version is None and hasattr(self, "_version"):
            version = self._version
        return AsyncOracleShowFunctionality(self, version)


__all__ = ["OracleShowMixin", "AsyncOracleShowMixin"]
