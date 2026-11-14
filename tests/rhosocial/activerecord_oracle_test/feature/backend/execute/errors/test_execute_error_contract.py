# tests/rhosocial/activerecord_oracle_test/feature/backend/execute/errors/test_execute_error_contract.py
"""Oracle ``execute`` raises the framework error contract's type per phase.

Every test here runs without a database: the backend is constructed from an
inert connection config, cursors and connections are faked in memory, and the
assertions pin the exception type each phase must produce:

``connect`` failure -> ``ConnectionError`` (passes through, never ``QueryError``);
bind conversion -> ``TypeConversionError`` (passes through);
driver error -> mapped semantic error (``ORA-00060`` -> ``DeadlockError``);
commit failure -> ``TransactionError`` with the driver error as its cause;
cursor ``close`` failure -> never replaces the exception being propagated.

The synchronous and asynchronous classes keep identical method names, matching
the backends' sync/async parity contract.
"""

# ruff: noqa: E402 - oracledb is optional, so the skip guard must run first.
import pytest

pytest.importorskip("oracledb")

from oracledb.exceptions import DatabaseError as OracleDatabaseError  # noqa: E402

from rhosocial.activerecord.backend.errors import (  # noqa: E402
    ConnectionError,
    DeadlockError,
    QueryError,
    TransactionError,
    TypeConversionError,
)
from rhosocial.activerecord.backend.impl.oracle.backend import (  # noqa: E402
    AsyncOracleBackend,
    OracleBackend,
)
from rhosocial.activerecord.backend.impl.oracle.config import (  # noqa: E402
    OracleConnectionConfig,
)


DEADLOCK_MESSAGE = "ORA-00060: deadlock detected while waiting for resource"


def _config():
    """An inert connection config: construction never opens a socket."""
    return OracleConnectionConfig(
        host="localhost",
        port=1521,
        service_name="UNIT",
        username="unit",
        password="unit",
    )


class _FakeCursor:
    """A cursor whose ``execute``/``close`` behaviour each test scripts."""

    def __init__(self, execute_error=None, close_error=None):
        self.description = None
        self.rowcount = 1
        self.closed = False
        self.execute_calls = []
        self._execute_error = execute_error
        self._close_error = close_error

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))
        if self._execute_error is not None:
            raise self._execute_error

    def close(self):
        self.closed = True
        if self._close_error is not None:
            raise self._close_error


class _FakeAsyncCursor:
    """Async twin of :class:`_FakeCursor`.

    ``oracledb.AsyncCursor.close`` is synchronous (verified on 26.0.1), which is
    why the fake keeps the same split as the driver.
    """

    def __init__(self, execute_error=None, close_error=None):
        self.description = None
        self.rowcount = 1
        self.closed = False
        self.execute_calls = []
        self._execute_error = execute_error
        self._close_error = close_error

    async def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))
        if self._execute_error is not None:
            raise self._execute_error

    def close(self):
        self.closed = True
        if self._close_error is not None:
            raise self._close_error


class _FakeConnection:
    """A connection whose only job is to record and fail ``commit``."""

    def __init__(self, commit_error=None):
        self.commit_calls = 0
        self._commit_error = commit_error

    def commit(self):
        self.commit_calls += 1
        if self._commit_error is not None:
            raise self._commit_error


class _FakeAsyncConnection:
    """Async twin of :class:`_FakeConnection`."""

    def __init__(self, commit_error=None):
        self.commit_calls = 0
        self._commit_error = commit_error

    async def commit(self):
        self.commit_calls += 1
        if self._commit_error is not None:
            raise self._commit_error


def _sync_backend():
    return OracleBackend(connection_config=_config())


def _async_backend():
    return AsyncOracleBackend(connection_config=_config())


class _ContractBase:
    """Non-test helpers shared by the two backend classes."""

    @staticmethod
    def _fail_connect(expected):
        def fail_connect():
            raise expected

        return fail_connect

    @staticmethod
    def _fail_conversion(expected):
        def fail_conversion(sql, params):
            raise expected

        return fail_conversion


class TestExecuteErrorContract(_ContractBase):
    """Synchronous backend, database-free execute error phases."""

    def test_connection_error_from_automatic_connect_passes_through(self, monkeypatch):
        """``connect()`` raising a framework error must not become ``QueryError``."""
        backend = _sync_backend()
        expected = ConnectionError("ORA-01017: invalid username/password; logon denied")
        monkeypatch.setattr(backend, "connect", self._fail_connect(expected))

        with pytest.raises(ConnectionError) as info:
            backend.execute("SELECT 1 FROM DUAL")

        assert info.value is expected
        assert not isinstance(info.value, QueryError)

    def test_type_conversion_error_from_parameter_preparation_passes_through(
        self, monkeypatch
    ):
        """A ``TypeConversionError`` raised during bind conversion is not rewritten.

        The Oracle backend converts bind values in
        ``_convert_placeholders_to_oracle`` (``_convert_datetime_params``)
        rather than in the core ``prepare_parameters`` step, because Oracle
        needs its own ``?`` -> ``:N`` rewrite. It is the same phase and the same
        contract: a conversion failure before ``cursor.execute`` reaches the
        caller as ``TypeConversionError``, not as ``QueryError``.
        """
        backend = _sync_backend()
        cursor = _FakeCursor()
        expected = TypeConversionError("cannot convert bind value")
        monkeypatch.setattr(backend, "_get_cursor", lambda: cursor)
        monkeypatch.setattr(
            backend,
            "_convert_placeholders_to_oracle",
            self._fail_conversion(expected),
        )

        with pytest.raises(TypeConversionError) as info:
            backend.execute("INSERT INTO t (c) VALUES (?)", ("bad",))

        assert info.value is expected
        assert not isinstance(info.value, QueryError)

    def test_handle_error_leaves_framework_errors_alone(self):
        """``_handle_error`` is legacy-safe: it never translates a framework error."""
        backend = _sync_backend()
        expected = TypeConversionError("prepare_parameters rejected a value")

        with pytest.raises(TypeConversionError) as info:
            backend._handle_error(expected)

        assert info.value is expected

    def test_driver_deadlock_still_maps_to_deadlock_error(self, monkeypatch):
        """Driver errors keep going through the Oracle mapping."""
        backend = _sync_backend()
        driver_error = OracleDatabaseError(DEADLOCK_MESSAGE)
        cursor = _FakeCursor(execute_error=driver_error)
        monkeypatch.setattr(backend, "_get_cursor", lambda: cursor)

        with pytest.raises(DeadlockError) as info:
            backend.execute("UPDATE t SET a = ?", (1,))

        assert info.value.__cause__ is driver_error
        assert info.value.driver_code == 60
        assert info.value.transient is True

    def test_auto_commit_failure_raises_transaction_error(self, monkeypatch):
        """A failed commit is surfaced as ``TransactionError``, not swallowed."""
        backend = _sync_backend()
        cursor = _FakeCursor()
        commit_error = ConnectionError("socket is closed")
        backend._connection = _FakeConnection(commit_error)
        monkeypatch.setattr(backend, "_get_cursor", lambda: cursor)

        with pytest.raises(TransactionError) as info:
            backend.execute("INSERT INTO t (a) VALUES (?)", (1,))

        assert info.value.__cause__ is commit_error
        assert backend._connection.commit_calls == 1
        assert not isinstance(info.value, (ConnectionError, QueryError))

    def test_cursor_close_failure_does_not_replace_original_error(self, monkeypatch):
        """A ``close()`` failure in ``finally`` must not mask the real exception."""
        backend = _sync_backend()
        driver_error = OracleDatabaseError(DEADLOCK_MESSAGE)
        cursor = _FakeCursor(
            execute_error=driver_error,
            close_error=RuntimeError("close failed"),
        )
        monkeypatch.setattr(backend, "_get_cursor", lambda: cursor)

        with pytest.raises(DeadlockError) as info:
            backend.execute("UPDATE t SET a = ?", (1,))

        assert info.value.__cause__ is driver_error
        assert cursor.closed is True

    def test_execute_many_maps_driver_error_and_skips_commit(self, monkeypatch):
        """The batch path uses the same mapping as ``execute``."""
        backend = _sync_backend()
        driver_error = OracleDatabaseError(DEADLOCK_MESSAGE)
        cursor = _FakeCursor(execute_error=driver_error)
        backend._connection = _FakeConnection()
        monkeypatch.setattr(backend, "_get_cursor", lambda: cursor)

        with pytest.raises(DeadlockError) as info:
            backend.execute_many("UPDATE t SET a = ?", [(1,), (2,)])

        assert info.value.__cause__ is driver_error
        assert backend._connection.commit_calls == 0


class TestAsyncExecuteErrorContract(_ContractBase):
    """Asynchronous backend: same method names, same contract."""

    @pytest.mark.asyncio
    async def test_connection_error_from_automatic_connect_passes_through(
        self, monkeypatch
    ):
        backend = _async_backend()
        expected = ConnectionError("ORA-01017: invalid username/password; logon denied")
        monkeypatch.setattr(backend, "connect", self._fail_connect(expected))

        with pytest.raises(ConnectionError) as info:
            await backend.execute("SELECT 1 FROM DUAL")

        assert info.value is expected
        assert not isinstance(info.value, QueryError)

    @pytest.mark.asyncio
    async def test_type_conversion_error_from_parameter_preparation_passes_through(
        self, monkeypatch
    ):
        backend = _async_backend()
        cursor = _FakeAsyncCursor()
        expected = TypeConversionError("cannot convert bind value")

        async def get_cursor():
            return cursor

        monkeypatch.setattr(backend, "_get_cursor", get_cursor)
        monkeypatch.setattr(
            backend,
            "_convert_placeholders_to_oracle",
            self._fail_conversion(expected),
        )

        with pytest.raises(TypeConversionError) as info:
            await backend.execute("INSERT INTO t (c) VALUES (?)", ("bad",))

        assert info.value is expected
        assert not isinstance(info.value, QueryError)

    async def test_handle_error_leaves_framework_errors_alone(self):
        backend = _async_backend()
        expected = TypeConversionError("prepare_parameters rejected a value")

        with pytest.raises(TypeConversionError) as info:
            backend._handle_error(expected)

        assert info.value is expected

    @pytest.mark.asyncio
    async def test_driver_deadlock_still_maps_to_deadlock_error(self, monkeypatch):
        backend = _async_backend()
        driver_error = OracleDatabaseError(DEADLOCK_MESSAGE)
        cursor = _FakeAsyncCursor(execute_error=driver_error)

        async def get_cursor():
            return cursor

        monkeypatch.setattr(backend, "_get_cursor", get_cursor)

        with pytest.raises(DeadlockError) as info:
            await backend.execute("UPDATE t SET a = ?", (1,))

        assert info.value.__cause__ is driver_error
        assert info.value.driver_code == 60
        assert info.value.transient is True

    @pytest.mark.asyncio
    async def test_auto_commit_failure_raises_transaction_error(self, monkeypatch):
        backend = _async_backend()
        cursor = _FakeAsyncCursor()
        commit_error = ConnectionError("socket is closed")
        backend._connection = _FakeAsyncConnection(commit_error)

        async def get_cursor():
            return cursor

        monkeypatch.setattr(backend, "_get_cursor", get_cursor)

        with pytest.raises(TransactionError) as info:
            await backend.execute("INSERT INTO t (a) VALUES (?)", (1,))

        assert info.value.__cause__ is commit_error
        assert backend._connection.commit_calls == 1
        assert not isinstance(info.value, (ConnectionError, QueryError))

    @pytest.mark.asyncio
    async def test_cursor_close_failure_does_not_replace_original_error(
        self, monkeypatch
    ):
        backend = _async_backend()
        driver_error = OracleDatabaseError(DEADLOCK_MESSAGE)
        cursor = _FakeAsyncCursor(
            execute_error=driver_error,
            close_error=RuntimeError("close failed"),
        )

        async def get_cursor():
            return cursor

        monkeypatch.setattr(backend, "_get_cursor", get_cursor)

        with pytest.raises(DeadlockError) as info:
            await backend.execute("UPDATE t SET a = ?", (1,))

        assert info.value.__cause__ is driver_error
        assert cursor.closed is True

    @pytest.mark.asyncio
    async def test_execute_many_maps_driver_error_and_skips_commit(self, monkeypatch):
        backend = _async_backend()
        driver_error = OracleDatabaseError(DEADLOCK_MESSAGE)
        cursor = _FakeAsyncCursor(execute_error=driver_error)
        backend._connection = _FakeAsyncConnection()

        async def get_cursor():
            return cursor

        monkeypatch.setattr(backend, "_get_cursor", get_cursor)

        with pytest.raises(DeadlockError) as info:
            await backend.execute_many("UPDATE t SET a = ?", [(1,), (2,)])

        assert info.value.__cause__ is driver_error
        assert backend._connection.commit_calls == 0
