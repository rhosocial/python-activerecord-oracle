# tests/rhosocial/activerecord_oracle_test/feature/logging/test_execution_logging_unit.py
"""Database-free unit tests for Oracle execution logging paths.

The Oracle backend's execution paths are broad: ``execute``, ``execute_many``,
``bulk_insert``, the ``RETURNING INTO`` helper, ``executescript`` and the
transaction managers.  These tests drive all of them through fake
connections/cursors, so the logging branches are covered without an Oracle
listener.  Where a path exists in both the sync and the async backend, the two
test classes keep identical method names.
"""

# ruff: noqa: E402 - oracledb is optional, so the skip guard must run first.
import logging

import pytest

pytest.importorskip("oracledb")

import oracledb
from oracledb.exceptions import (
    DatabaseError as OracleDatabaseError,
    Error as OracleError,
    IntegrityError as OracleIntegrityError,
    OperationalError as OracleOperationalError,
)

from rhosocial.activerecord.backend.errors import (
    ConnectionError,
    DatabaseError,
    DeadlockError,
    IntegrityError,
    OperationalError,
    QueryError,
    TransactionError,
)
from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.impl.oracle.async_transaction import (
    AsyncOracleTransactionManager,
)
from rhosocial.activerecord.backend.impl.oracle.backend import (
    AsyncOracleBackend,
    OracleBackend,
)
from rhosocial.activerecord.backend.impl.oracle.cli import info as cli_info
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.backend.impl.oracle.mixins import OracleBackendMixin
from rhosocial.activerecord.backend.impl.oracle.transaction import OracleTransactionManager
from rhosocial.activerecord.backend.options import (
    BulkInsertOptions,
    DeleteOptions,
    ExecutionOptions,
    InsertOptions,
    LogContext,
    UpdateOptions,
)
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.transaction import (
    IsolationLevel,
    TransactionMode,
    TransactionState,
)
from rhosocial.activerecord.logging.redaction import SensitiveValue

#: The single library logger shared by the sync and async Oracle backends.
LOGGER_NAME = "rhosocial.activerecord.backend.oracle"


# ---------------------------------------------------------------------------
# Fakes: no socket, no listener, no connect()
# ---------------------------------------------------------------------------


class FakeOutVar:
    """Minimal stand-in for an oracledb output bind variable."""

    def __init__(self, value):
        self._value = value

    def getvalue(self):
        """Return the bound value the backend reads back."""
        return self._value


class FakeCursor:
    """Synchronous cursor double with the surface the backend touches."""

    def __init__(self, rowcount=1, out_value=1, error=None):
        self.description = None
        self.rowcount = rowcount
        self._out_value = out_value
        self.error = error
        self.executed = []
        self.closed = False

    def execute(self, sql, params=None):
        """Record the statement, or raise the configured driver error."""
        if self.error is not None:
            raise self.error
        self.executed.append((sql, params))

    def fetchall(self):
        """Return no rows; DML paths do not consume a result set."""
        return []

    def fetchone(self):
        """Return no dictionary row for the RETURNING column lookup."""
        return None

    def var(self, _type):
        """Return a fake output bind variable of the requested type."""
        return FakeOutVar(self._out_value)

    def setinputsizes(self, *sizes):
        """Accept the CLOB input-size hints without doing anything."""

    def close(self):
        """Mark the cursor closed so finally-blocks are observable."""
        self.closed = True


class FakeAsyncCursor(FakeCursor):
    """Asynchronous cursor double; ``close`` stays synchronous like oracledb."""

    async def execute(self, sql, params=None):
        """Awaitable execute that records the statement."""
        if self.error is not None:
            raise self.error
        self.executed.append((sql, params))

    async def fetchall(self):
        """Awaitable empty result set."""
        return []

    async def fetchone(self):
        """Awaitable empty row for the RETURNING column lookup."""
        return None


class FakeConnection:
    """Synchronous connection double with failure switches."""

    def __init__(self, cursor=None):
        self.cursor_obj = cursor if cursor is not None else FakeCursor()
        self.commit_error = None
        self.rollback_error = None
        self.cursor_error = None
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self):
        """Return the configured cursor, or raise the configured error."""
        if self.cursor_error is not None:
            raise self.cursor_error
        return self.cursor_obj

    def commit(self):
        """Count commits, or raise the configured error."""
        if self.commit_error is not None:
            raise self.commit_error
        self.commit_count += 1

    def rollback(self):
        """Count rollbacks, or raise the configured error."""
        if self.rollback_error is not None:
            raise self.rollback_error
        self.rollback_count += 1

    def close(self):
        """Mark the connection closed."""
        self.closed = True


class FakeAsyncConnection(FakeConnection):
    """Asynchronous connection double; cursor creation stays synchronous."""

    async def commit(self):
        """Awaitable commit that counts calls."""
        if self.commit_error is not None:
            raise self.commit_error
        self.commit_count += 1

    async def rollback(self):
        """Awaitable rollback that counts calls."""
        if self.rollback_error is not None:
            raise self.rollback_error
        self.rollback_count += 1

    async def close(self):
        """Awaitable close."""
        self.closed = True


# ---------------------------------------------------------------------------
# Builders and option helpers
# ---------------------------------------------------------------------------


def _connection_config() -> OracleConnectionConfig:
    """Built without connecting: the config is inert until ``connect()``."""
    return OracleConnectionConfig(
        host="localhost",
        port=1521,
        service_name="UNIT",
        username="unit",
        password="unit",
    )


def _prepare_logger(logger) -> None:
    """Enable the DEBUG records the execution helpers gate on."""
    logger.setLevel(logging.DEBUG)
    logger.propagate = True


def make_sync_backend(cursor=None):
    """Return a connected-looking sync backend plus its fake connection."""
    backend = OracleBackend(connection_config=_connection_config())
    connection = FakeConnection(cursor)
    backend._connection = connection
    backend._get_cursor = connection.cursor
    _prepare_logger(backend.logger)
    return backend, connection


def make_async_backend(cursor=None):
    """Return a connected-looking async backend plus its fake connection."""
    backend = AsyncOracleBackend(connection_config=_connection_config())
    connection = FakeAsyncConnection(cursor if cursor is not None else FakeAsyncCursor())
    backend._connection = connection

    async def _get_cursor():
        return connection.cursor()

    backend._get_cursor = _get_cursor
    _prepare_logger(backend.logger)
    return backend, connection


def _dml_options(operation=None, table="unit_table") -> ExecutionOptions:
    """DML execution options carrying a structured log context."""
    context = LogContext(operation=operation, object_name=table) if operation else None
    return ExecutionOptions(stmt_type=StatementType.DML, log_context=context)


def _predicate(backend, id_value=1):
    """Build ``id = :value`` for the update/delete option objects."""
    return ComparisonPredicate(
        backend.dialect,
        "eq",
        Column(backend.dialect, "id", table="unit_table"),
        Literal(backend.dialect, id_value),
    )


def _assert_start_and_summary(caplog, sql_fragment, event):
    """Assert one DEBUG start record and one INFO structured summary record."""
    messages = [record.getMessage() for record in caplog.records]
    assert any("Executing SQL" in message and sql_fragment in message for message in messages)
    events = [getattr(record, "event", None) for record in caplog.records]
    assert event in events


# ---------------------------------------------------------------------------
# Error case tables
# ---------------------------------------------------------------------------

_EXECUTE_ERROR_CASES = [
    pytest.param(
        OracleIntegrityError("ORA-00001: unique constraint violated"),
        IntegrityError,
        "ORA-00001",
        id="ora-00001",
    ),
    pytest.param(
        OracleIntegrityError("ORA-01400: cannot insert NULL"),
        IntegrityError,
        "cannot be null",
        id="ora-01400-integrity",
    ),
    pytest.param(
        OracleDatabaseError("ORA-01400: cannot insert NULL"),
        IntegrityError,
        "cannot be null",
        id="ora-01400-database",
    ),
    pytest.param(
        OracleDatabaseError("ORA-01407: cannot update to NULL"),
        IntegrityError,
        "cannot be null",
        id="ora-01407-database",
    ),
    pytest.param(
        OracleDatabaseError("ORA-00060: deadlock detected"),
        DeadlockError,
        "ORA-00060",
        id="ora-00060",
    ),
    pytest.param(
        OracleDatabaseError("ORA-00942: table or view does not exist"),
        DatabaseError,
        "ORA-00942",
        id="ora-00942",
    ),
    pytest.param(
        OracleError("plain driver error"),
        DatabaseError,
        "plain driver error",
        id="generic-oracle-error",
    ),
    pytest.param(
        RuntimeError("driver exploded"),
        QueryError,
        "driver exploded",
        id="unexpected-error",
    ),
]

_HANDLE_ERROR_CASES = [
    pytest.param(
        OracleIntegrityError("ORA-00001: unique constraint violated"),
        IntegrityError,
        "Unique constraint violation",
        id="ora-00001",
    ),
    pytest.param(
        OracleIntegrityError("ORA-01400: cannot insert NULL"),
        IntegrityError,
        "cannot be null",
        id="ora-01400",
    ),
    pytest.param(
        OracleIntegrityError("ORA-01407: cannot update to NULL"),
        IntegrityError,
        "cannot be null",
        id="ora-01407",
    ),
    pytest.param(
        OracleIntegrityError("ORA-02291: integrity constraint violated"),
        IntegrityError,
        "Foreign key constraint violation",
        id="ora-02291",
    ),
    pytest.param(
        OracleIntegrityError("ORA-02292: integrity constraint violated"),
        IntegrityError,
        "Child record exists",
        id="ora-02292",
    ),
    pytest.param(
        OracleIntegrityError("ORA-00999: unclassified integrity error"),
        IntegrityError,
        "ORA-00999",
        id="integrity-other",
    ),
    pytest.param(
        OracleDatabaseError("ORA-01400: cannot insert NULL"),
        IntegrityError,
        "cannot be null",
        id="database-ora-01400",
    ),
    pytest.param(
        OracleDatabaseError("ORA-01407: cannot update to NULL"),
        IntegrityError,
        "cannot be null",
        id="database-ora-01407",
    ),
    pytest.param(
        OracleDatabaseError("ORA-00060: deadlock detected"),
        DeadlockError,
        "ORA-00060",
        id="database-ora-00060",
    ),
    pytest.param(
        OracleDatabaseError("ORA-04020: deadlock detected"),
        DeadlockError,
        "ORA-04020",
        id="database-ora-04020",
    ),
    pytest.param(
        OracleDatabaseError("generic database failure"),
        DatabaseError,
        "generic database failure",
        id="database-generic",
    ),
    pytest.param(
        # oracledb.OperationalError subclasses DatabaseError, so the
        # DatabaseError branch (checked first) handles it.
        OracleOperationalError("ORA-12541: no listener"),
        DatabaseError,
        "ORA-12541",
        id="operational",
    ),
    pytest.param(
        OracleError("base oracle error"),
        DatabaseError,
        "base oracle error",
        id="generic-oracle-error",
    ),
    pytest.param(
        RuntimeError("unexpected"),
        RuntimeError,
        "unexpected",
        id="unexpected",
    ),
]


# ---------------------------------------------------------------------------
# Synchronous backend tests
# ---------------------------------------------------------------------------


class TestExecutionLogging:
    """Execution logging branches of the synchronous Oracle backend."""

    def test_backend_uses_oracle_logger_name(self):
        """Both backend classes resolve to the shared Oracle library logger."""
        backend, _ = make_sync_backend()
        assert backend._get_logger_name() == LOGGER_NAME
        assert backend.logger.name == LOGGER_NAME

    def test_execute_logs_start_and_summary(self, caplog):
        """A DML execute emits one DEBUG start and one INFO summary record."""
        backend, connection = make_sync_backend(FakeCursor(rowcount=1))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            result = backend.execute(
                "INSERT INTO unit_table VALUES (?)",
                (1,),
                options=_dml_options("insert"),
            )

        assert result.affected_rows == 1
        _assert_start_and_summary(caplog, "INSERT INTO unit_table", "dml.insert")
        assert connection.commit_count == 1
        assert connection.cursor_obj.closed is True

    def test_execute_unwraps_sensitive_parameters(self, caplog):
        """``SensitiveValue`` is unwrapped before binding and before logging."""
        backend, connection = make_sync_backend()
        params = (SensitiveValue("secret"), 7)
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            backend.execute(
                "INSERT INTO unit_table VALUES (?, ?)",
                params,
                options=_dml_options("insert"),
            )

        recorded = connection.cursor_obj.executed[-1]
        assert recorded[1] == ("secret", 7)
        messages = [record.getMessage() for record in caplog.records]
        assert not any("SensitiveValue" in message for message in messages)

    @pytest.mark.parametrize("error,expected,message_part", _EXECUTE_ERROR_CASES)
    def test_execute_error_mapping(self, error, expected, message_part, caplog):
        """Execute re-raises driver errors through the backend error taxonomy."""
        backend, _ = make_sync_backend(FakeCursor(error=error))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(expected) as raised:
                backend.execute(
                    "INSERT INTO unit_table VALUES (?)",
                    (1,),
                    options=_dml_options("insert"),
                )

        assert message_part in str(raised.value)
        messages = [record.getMessage() for record in caplog.records]
        assert any("exception will be re-raised" in message for message in messages)

    @pytest.mark.parametrize("error,expected,message_part", _HANDLE_ERROR_CASES)
    def test_handle_error_branches(self, error, expected, message_part):
        """``_handle_error`` maps every documented ORA code family."""
        backend, _ = make_sync_backend()
        with pytest.raises(expected) as raised:
            backend._handle_error(error)

        assert message_part in str(raised.value)

    def test_execute_many_logs_batch(self, caplog):
        """A batch emits a start record with parameter-set count and a summary."""
        backend, connection = make_sync_backend(FakeCursor(rowcount=1))
        params_list = [(SensitiveValue("first"),), (SensitiveValue("second"),)]
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            result = backend.execute_many(
                "INSERT INTO unit_table VALUES (?)",
                params_list,
                options=_dml_options("batch", "unit_table"),
            )

        assert result.affected_rows == 2
        assert connection.cursor_obj.executed[-1][1] == ("second",)
        messages = [record.getMessage() for record in caplog.records]
        assert any("parameter_sets: 2" in message for message in messages)
        events = [getattr(record, "event", None) for record in caplog.records]
        assert "dml.batch" in events

    def test_execute_many_reraises_non_oracle_error(self, caplog):
        """A non-driver exception in a batch is logged and re-raised unchanged."""
        backend, _ = make_sync_backend(FakeCursor(error=RuntimeError("batch exploded")))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(RuntimeError, match="batch exploded"):
                backend.execute_many(
                    "INSERT INTO unit_table VALUES (?)",
                    [(1,)],
                    options=_dml_options("batch", "unit_table"),
                )

        messages = [record.getMessage() for record in caplog.records]
        assert any("exception will be re-raised" in message for message in messages)

    def test_execute_many_default_options_and_driver_error(self, caplog):
        """Batches default to DML options and wrap Oracle errors as DatabaseError."""
        backend, _ = make_sync_backend(FakeCursor(rowcount=1))
        result = backend.execute_many("INSERT INTO unit_table VALUES (?)", [(1,)])
        assert result.affected_rows == 1

        failing, _ = make_sync_backend(FakeCursor(error=OracleError("batch driver error")))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(DatabaseError, match="batch driver error"):
                failing.execute_many("INSERT INTO unit_table VALUES (?)", [(1,)])

        messages = [record.getMessage() for record in caplog.records]
        assert any("exception will be re-raised" in message for message in messages)

    def test_insert_returning_paths(self, caplog):
        """Insert covers supplied-RETURNING, RETURNING INTO and plain paths."""
        backend, _ = make_sync_backend(FakeCursor(rowcount=1))

        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            supplied = backend.insert(
                InsertOptions(
                    table="unit_table",
                    data={"id": 1, "name": "supplied"},
                    returning_columns=["id"],
                )
            )
            returning = backend.insert(
                InsertOptions(
                    table="unit_table",
                    data={"name": "logged"},
                    returning_columns=["id"],
                )
            )
            plain = backend.insert(InsertOptions(table="unit_table", data={"name": "plain"}))

        assert supplied.data == [{"id": 1}]
        assert returning.data == [{"id": 1}]
        assert plain.data is None
        _assert_start_and_summary(caplog, "INSERT INTO", "dml.insert")

    def test_update_and_delete_returning_paths(self, caplog):
        """Update/delete cover both the RETURNING INTO and plain paths."""
        backend, _ = make_sync_backend(FakeCursor(rowcount=1))
        where = _predicate(backend)

        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            updated = backend.update(
                UpdateOptions(
                    table="unit_table",
                    data={"name": "updated"},
                    where=where,
                    returning_columns=["id"],
                )
            )
            deleted = backend.delete(
                DeleteOptions(table="unit_table", where=where, returning_columns=["id"])
            )
            plain_update = backend.update(
                UpdateOptions(table="unit_table", data={"name": "plain"}, where=where)
            )

        assert updated.data == [{"id": 1}]
        assert deleted.data == [{"id": 1}]
        assert plain_update.data is None
        _assert_start_and_summary(caplog, 'UPDATE "', "dml.update")

    def test_returning_error_mapping(self, caplog):
        """RETURNING INTO failures are logged; driver errors become DatabaseError."""
        backend, _ = make_sync_backend(FakeCursor(error=OracleError("returning failure")))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(DatabaseError, match="returning failure"):
                backend.insert(
                    InsertOptions(
                        table="unit_table",
                        data={"name": "failing"},
                        returning_columns=["id"],
                    )
                )

        backend, _ = make_sync_backend(FakeCursor(error=RuntimeError("returning exploded")))
        with pytest.raises(RuntimeError, match="returning exploded"):
            backend.delete(
                DeleteOptions(
                    table="unit_table",
                    where=_predicate(backend),
                    returning_columns=["id"],
                )
            )

        messages = [record.getMessage() for record in caplog.records]
        assert any("exception will be re-raised" in message for message in messages)

    def test_bulk_insert_logging_and_returning(self, caplog):
        """Bulk insert emits batch start/summary and reads output variables."""
        backend, _ = make_sync_backend(FakeCursor(rowcount=1))

        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            returned = backend.bulk_insert(
                BulkInsertOptions(
                    table="unit_table",
                    columns=["id", "name"],
                    rows=[[1, "first"], [2, "second"]],
                    returning_columns=["id"],
                )
            )
            plain = backend.bulk_insert(
                BulkInsertOptions(table="unit_table", columns=["id", "name"], rows=[[3, "third"]])
            )

        assert returned.data == [{"id": 1}, {"id": 1}]
        assert plain.data is None
        messages = [record.getMessage() for record in caplog.records]
        assert any("parameter_sets: 2" in message for message in messages)
        events = [getattr(record, "event", None) for record in caplog.records]
        assert "dml.batch" in events

    def test_bulk_insert_error_mapping(self, caplog):
        """Bulk insert applies the same NOT NULL / driver error mapping."""
        cases = [
            (OracleIntegrityError("ORA-01400: cannot insert NULL"), IntegrityError),
            (OracleIntegrityError("ORA-00999: integrity error"), IntegrityError),
            (OracleDatabaseError("ORA-01407: cannot update to NULL"), IntegrityError),
            (OracleDatabaseError("ORA-00060: deadlock detected"), DeadlockError),
            (OracleDatabaseError("generic database failure"), DatabaseError),
            (OracleError("bulk driver error"), DatabaseError),
            (RuntimeError("bulk exploded"), RuntimeError),
        ]
        for error, expected in cases:
            backend, _ = make_sync_backend(FakeCursor(error=error))
            with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
                with pytest.raises(expected):
                    backend.bulk_insert(
                        BulkInsertOptions(
                            table="unit_table",
                            columns=["id"],
                            rows=[[1]],
                        )
                    )

    def test_executescript_logs_summary_and_wraps_errors(self, caplog):
        """A script run logs a structured summary; driver errors are wrapped."""
        backend, _ = make_sync_backend()
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            backend.executescript("CREATE TABLE unit_table (id NUMBER);")

        messages = [record.getMessage() for record in caplog.records]
        assert any("SQL script executed" in message for message in messages)

        failing, _ = make_sync_backend(FakeCursor(error=OracleError("script failed")))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(DatabaseError, match="script failed"):
                failing.executescript("CREATE TABLE unit_table (id NUMBER);")

    def test_connect_failure_is_wrapped(self, monkeypatch, caplog):
        """A failed listener handshake is logged at DEBUG and wrapped."""
        backend = OracleBackend(connection_config=_connection_config())
        _prepare_logger(backend.logger)

        def _raise_connect(**kwargs):
            raise OracleError("ORA-12541: cannot connect")

        monkeypatch.setattr(oracledb, "connect", _raise_connect)
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(ConnectionError, match="Failed to connect to Oracle"):
                backend.connect()

        messages = [record.getMessage() for record in caplog.records]
        assert any("will re-raise" in message for message in messages)

    def test_transaction_requires_connection(self):
        """A backend without a connection refuses transaction work loudly."""
        backend = OracleBackend(connection_config=_connection_config())
        _prepare_logger(backend.logger)
        manager = OracleTransactionManager(backend, backend.logger)

        with pytest.raises(TransactionError, match="No valid connection"):
            manager._ensure_connection_ready()

    def test_operational_error_branch_is_reachable(self, caplog):
        """A driver OperationalError maps to OperationalError on every path.

        ORA-12541 (no listener) is a transient network failure, so the error
        taxonomy maps it to OperationalError rather than the generic
        DatabaseError fallback, even though ``oracledb.OperationalError``
        subclasses ``oracledb.DatabaseError``.
        """
        backend, _ = make_sync_backend()
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(OperationalError, match="ORA-12541"):
                backend._handle_error(OracleOperationalError("ORA-12541: no listener"))

            failing, _ = make_sync_backend(
                FakeCursor(error=OracleOperationalError("ORA-12541: execute"))
            )
            with pytest.raises(OperationalError, match="ORA-12541"):
                failing.execute(
                    "INSERT INTO unit_table VALUES (?)",
                    (1,),
                    options=_dml_options("insert"),
                )

            bulk, _ = make_sync_backend(
                FakeCursor(error=OracleOperationalError("ORA-12541: bulk"))
            )
            with pytest.raises(OperationalError, match="ORA-12541"):
                bulk.bulk_insert(
                    BulkInsertOptions(table="unit_table", columns=["id"], rows=[[1]])
                )

        messages = [record.getMessage() for record in caplog.records]
        assert any("Operational error (will re-raise)" in message for message in messages)

    def test_transaction_lifecycle_logs(self):
        """Begin/commit/rollback/savepoint paths execute their log records."""
        backend, connection = make_sync_backend()
        manager = OracleTransactionManager(backend, backend.logger)

        manager._do_begin()
        assert manager.state is TransactionState.ACTIVE

        manager._do_commit()
        assert manager.state is TransactionState.COMMITTED

        manager._do_rollback()
        assert manager.state is TransactionState.ROLLED_BACK

        manager._do_create_savepoint("sp_one")
        manager._do_release_savepoint("sp_one")
        manager._do_rollback_savepoint("sp_one")

        assert connection.commit_count == 1
        assert connection.rollback_count == 1
        assert connection.cursor_obj.executed == [
            ("SAVEPOINT sp_one", None),
            ("ROLLBACK TO SAVEPOINT sp_one", None),
        ]

    def test_transaction_isolation_and_mode_statements(self):
        """SERIALIZABLE and READ ONLY issue their SET TRANSACTION statements."""
        backend, connection = make_sync_backend()
        manager = OracleTransactionManager(backend, backend.logger)
        manager._isolation_level = IsolationLevel.SERIALIZABLE
        manager._transaction_mode = TransactionMode.READ_ONLY

        manager._do_begin()

        assert connection.cursor_obj.executed == [
            ("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", None),
            ("SET TRANSACTION READ ONLY", None),
        ]

    def test_transaction_error_branches(self, caplog):
        """Every transaction failure logs its DEBUG note before re-raising."""
        backend, connection = make_sync_backend()
        manager = OracleTransactionManager(backend, backend.logger)
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            manager._isolation_level = IsolationLevel.SERIALIZABLE
            connection.cursor_error = RuntimeError("cursor unavailable")
            with pytest.raises(TransactionError, match="Failed to begin transaction"):
                manager._do_begin()

            connection.commit_error = RuntimeError("commit failed")
            with pytest.raises(TransactionError, match="Failed to commit transaction"):
                manager._do_commit()

            connection.rollback_error = RuntimeError("rollback failed")
            with pytest.raises(TransactionError, match="Failed to rollback transaction"):
                manager._do_rollback()

            with pytest.raises(TransactionError, match="Failed to create savepoint"):
                manager._do_create_savepoint("sp_error")

            with pytest.raises(TransactionError, match="Failed to rollback to savepoint"):
                manager._do_rollback_savepoint("sp_error")

        messages = [record.getMessage() for record in caplog.records]
        assert sum("(will re-raise)" in message for message in messages) >= 5


# ---------------------------------------------------------------------------
# Asynchronous backend tests (same method names as the sync class)
# ---------------------------------------------------------------------------


class TestAsyncExecutionLogging:
    """Execution logging branches of the asynchronous Oracle backend."""

    def test_backend_uses_oracle_logger_name(self):
        """The async backend shares the sync backend's library logger."""
        backend, _ = make_async_backend()
        assert backend._get_logger_name() == LOGGER_NAME
        assert backend.logger.name == LOGGER_NAME

    @pytest.mark.asyncio
    async def test_execute_logs_start_and_summary(self, caplog):
        """Awaited DML emits the same DEBUG start and INFO summary records."""
        backend, connection = make_async_backend(FakeAsyncCursor(rowcount=1))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            result = await backend.execute(
                "INSERT INTO unit_table VALUES (?)",
                (1,),
                options=_dml_options("insert"),
            )

        assert result.affected_rows == 1
        _assert_start_and_summary(caplog, "INSERT INTO unit_table", "dml.insert")
        assert connection.commit_count == 1
        assert connection.cursor_obj.closed is True

    @pytest.mark.asyncio
    async def test_execute_unwraps_sensitive_parameters(self, caplog):
        """Async binding and logging also receive unwrapped values."""
        backend, connection = make_async_backend()
        params = (SensitiveValue("secret"), 7)
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            await backend.execute(
                "INSERT INTO unit_table VALUES (?, ?)",
                params,
                options=_dml_options("insert"),
            )

        recorded = connection.cursor_obj.executed[-1]
        assert recorded[1] == ("secret", 7)
        messages = [record.getMessage() for record in caplog.records]
        assert not any("SensitiveValue" in message for message in messages)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error,expected,message_part", _EXECUTE_ERROR_CASES)
    async def test_execute_error_mapping(self, error, expected, message_part, caplog):
        """Async execute maps driver errors through the same taxonomy."""
        backend, _ = make_async_backend(FakeAsyncCursor(error=error))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(expected) as raised:
                await backend.execute(
                    "INSERT INTO unit_table VALUES (?)",
                    (1,),
                    options=_dml_options("insert"),
                )

        assert message_part in str(raised.value)
        messages = [record.getMessage() for record in caplog.records]
        assert any("exception will be re-raised" in message for message in messages)

    @pytest.mark.parametrize("error,expected,message_part", _HANDLE_ERROR_CASES)
    def test_handle_error_branches(self, error, expected, message_part):
        """The async backend has the same ``_handle_error`` contract."""
        backend, _ = make_async_backend()
        with pytest.raises(expected) as raised:
            backend._handle_error(error)

        assert message_part in str(raised.value)

    @pytest.mark.asyncio
    async def test_execute_many_logs_batch(self, caplog):
        """Async batches log the parameter-set count and the batch summary."""
        backend, connection = make_async_backend(FakeAsyncCursor(rowcount=1))
        params_list = [(SensitiveValue("first"),), (SensitiveValue("second"),)]
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            result = await backend.execute_many(
                "INSERT INTO unit_table VALUES (?)",
                params_list,
                options=_dml_options("batch", "unit_table"),
            )

        assert result.affected_rows == 2
        assert connection.cursor_obj.executed[-1][1] == ("second",)
        messages = [record.getMessage() for record in caplog.records]
        assert any("parameter_sets: 2" in message for message in messages)
        events = [getattr(record, "event", None) for record in caplog.records]
        assert "dml.batch" in events

    @pytest.mark.asyncio
    async def test_execute_many_reraises_non_oracle_error(self, caplog):
        """A non-driver exception in an async batch propagates unchanged."""
        backend, _ = make_async_backend(FakeAsyncCursor(error=RuntimeError("batch exploded")))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(RuntimeError, match="batch exploded"):
                await backend.execute_many(
                    "INSERT INTO unit_table VALUES (?)",
                    [(1,)],
                    options=_dml_options("batch", "unit_table"),
                )

        messages = [record.getMessage() for record in caplog.records]
        assert any("exception will be re-raised" in message for message in messages)

    @pytest.mark.asyncio
    async def test_execute_many_default_options_and_driver_error(self, caplog):
        """Async batches default to DML options and wrap Oracle errors."""
        backend, _ = make_async_backend(FakeAsyncCursor(rowcount=1))
        result = await backend.execute_many("INSERT INTO unit_table VALUES (?)", [(1,)])
        assert result.affected_rows == 1

        failing, _ = make_async_backend(FakeAsyncCursor(error=OracleError("batch driver error")))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(DatabaseError, match="batch driver error"):
                await failing.execute_many("INSERT INTO unit_table VALUES (?)", [(1,)])

        messages = [record.getMessage() for record in caplog.records]
        assert any("exception will be re-raised" in message for message in messages)

    @pytest.mark.asyncio
    async def test_insert_returning_paths(self, caplog):
        """Async insert covers supplied-RETURNING and RETURNING INTO paths."""
        backend, _ = make_async_backend(FakeAsyncCursor(rowcount=1))

        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            supplied = await backend.insert(
                InsertOptions(
                    table="unit_table",
                    data={"id": 1, "name": "supplied"},
                    returning_columns=["id"],
                )
            )
            returning = await backend.insert(
                InsertOptions(
                    table="unit_table",
                    data={"name": "logged"},
                    returning_columns=["id"],
                )
            )
            plain = await backend.insert(InsertOptions(table="unit_table", data={"name": "plain"}))

        assert supplied.data == [{"id": 1}]
        assert returning.data == [{"id": 1}]
        assert plain.data is None
        _assert_start_and_summary(caplog, "INSERT INTO", "dml.insert")

    @pytest.mark.asyncio
    async def test_update_and_delete_returning_paths(self, caplog):
        """Async update/delete cover the RETURNING INTO and plain paths."""
        backend, _ = make_async_backend(FakeAsyncCursor(rowcount=1))
        where = _predicate(backend)

        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            updated = await backend.update(
                UpdateOptions(
                    table="unit_table",
                    data={"name": "updated"},
                    where=where,
                    returning_columns=["id"],
                )
            )
            deleted = await backend.delete(
                DeleteOptions(table="unit_table", where=where, returning_columns=["id"])
            )
            plain_update = await backend.update(
                UpdateOptions(table="unit_table", data={"name": "plain"}, where=where)
            )

        assert updated.data == [{"id": 1}]
        assert deleted.data == [{"id": 1}]
        assert plain_update.data is None
        _assert_start_and_summary(caplog, 'UPDATE "', "dml.update")

    @pytest.mark.asyncio
    async def test_returning_error_mapping(self, caplog):
        """Async RETURNING failures are logged and mapped like sync ones."""
        backend, _ = make_async_backend(FakeAsyncCursor(error=OracleError("returning failure")))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(DatabaseError, match="returning failure"):
                await backend.insert(
                    InsertOptions(
                        table="unit_table",
                        data={"name": "failing"},
                        returning_columns=["id"],
                    )
                )

        backend, _ = make_async_backend(FakeAsyncCursor(error=RuntimeError("returning exploded")))
        with pytest.raises(RuntimeError, match="returning exploded"):
            await backend.delete(
                DeleteOptions(
                    table="unit_table",
                    where=_predicate(backend),
                    returning_columns=["id"],
                )
            )

        messages = [record.getMessage() for record in caplog.records]
        assert any("exception will be re-raised" in message for message in messages)

    @pytest.mark.asyncio
    async def test_bulk_insert_logging_and_returning(self, caplog):
        """Async bulk insert reads output variables and logs batch records."""
        backend, _ = make_async_backend(FakeAsyncCursor(rowcount=1))

        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            returned = await backend.bulk_insert(
                BulkInsertOptions(
                    table="unit_table",
                    columns=["id", "name"],
                    rows=[[1, "first"], [2, "second"]],
                    returning_columns=["id"],
                )
            )
            plain = await backend.bulk_insert(
                BulkInsertOptions(table="unit_table", columns=["id", "name"], rows=[[3, "third"]])
            )

        assert returned.data == [{"id": 1}, {"id": 1}]
        assert plain.data is None
        messages = [record.getMessage() for record in caplog.records]
        assert any("parameter_sets: 2" in message for message in messages)
        events = [getattr(record, "event", None) for record in caplog.records]
        assert "dml.batch" in events

    @pytest.mark.asyncio
    async def test_bulk_insert_error_mapping(self, caplog):
        """Async bulk insert applies the same NOT NULL / driver mapping."""
        cases = [
            (OracleIntegrityError("ORA-01400: cannot insert NULL"), IntegrityError),
            (OracleIntegrityError("ORA-00999: integrity error"), IntegrityError),
            (OracleDatabaseError("ORA-01407: cannot update to NULL"), IntegrityError),
            (OracleDatabaseError("ORA-00060: deadlock detected"), DeadlockError),
            (OracleDatabaseError("generic database failure"), DatabaseError),
            (OracleError("bulk driver error"), DatabaseError),
            (RuntimeError("bulk exploded"), RuntimeError),
        ]
        for error, expected in cases:
            backend, _ = make_async_backend(FakeAsyncCursor(error=error))
            with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
                with pytest.raises(expected):
                    await backend.bulk_insert(
                        BulkInsertOptions(
                            table="unit_table",
                            columns=["id"],
                            rows=[[1]],
                        )
                    )

    @pytest.mark.asyncio
    async def test_executescript_logs_summary_and_wraps_errors(self, caplog):
        """Async script execution logs its summary and wraps driver errors."""
        backend, _ = make_async_backend()
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            await backend.executescript("CREATE TABLE unit_table (id NUMBER);")

        messages = [record.getMessage() for record in caplog.records]
        assert any("SQL script executed (async)" in message for message in messages)

        failing, _ = make_async_backend(FakeAsyncCursor(error=OracleError("script failed")))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(DatabaseError, match="script failed"):
                await failing.executescript("CREATE TABLE unit_table (id NUMBER);")

    @pytest.mark.asyncio
    async def test_connect_failure_is_wrapped(self, monkeypatch, caplog):
        """An async handshake failure is logged at DEBUG and wrapped."""
        backend = AsyncOracleBackend(connection_config=_connection_config())
        _prepare_logger(backend.logger)

        async def _raise_connect(**kwargs):
            raise OracleError("ORA-12541: cannot connect")

        monkeypatch.setattr(oracledb, "connect_async", _raise_connect)
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(ConnectionError, match="Failed to connect to Oracle"):
                await backend.connect()

        messages = [record.getMessage() for record in caplog.records]
        assert any("will re-raise" in message for message in messages)

    @pytest.mark.asyncio
    async def test_transaction_requires_connection(self):
        """An async manager without a connection refuses transaction work."""
        manager = AsyncOracleTransactionManager(None, logging.getLogger(LOGGER_NAME))
        with pytest.raises(TransactionError, match="No valid connection"):
            await manager._ensure_connection_ready()

    @pytest.mark.asyncio
    async def test_operational_error_branch_is_reachable(self, caplog):
        """ORA-12541 maps to OperationalError on every async execution path."""
        backend, _ = make_async_backend()
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            with pytest.raises(OperationalError, match="ORA-12541"):
                backend._handle_error(OracleOperationalError("ORA-12541: no listener"))

            failing, _ = make_async_backend(
                FakeAsyncCursor(error=OracleOperationalError("ORA-12541: execute"))
            )
            with pytest.raises(OperationalError, match="ORA-12541"):
                await failing.execute(
                    "INSERT INTO unit_table VALUES (?)",
                    (1,),
                    options=_dml_options("insert"),
                )

            bulk, _ = make_async_backend(
                FakeAsyncCursor(error=OracleOperationalError("ORA-12541: bulk"))
            )
            with pytest.raises(OperationalError, match="ORA-12541"):
                await bulk.bulk_insert(
                    BulkInsertOptions(table="unit_table", columns=["id"], rows=[[1]])
                )

        messages = [record.getMessage() for record in caplog.records]
        assert any("Operational error (will re-raise)" in message for message in messages)

    @pytest.mark.asyncio
    async def test_transaction_lifecycle_logs(self):
        """Async begin/commit/rollback/savepoint log records execute."""
        connection = FakeAsyncConnection(FakeAsyncCursor())
        manager = AsyncOracleTransactionManager(connection, logging.getLogger(LOGGER_NAME))

        await manager._do_begin()
        assert manager.state is TransactionState.ACTIVE

        await manager._do_commit()
        assert manager.state is TransactionState.COMMITTED

        await manager._do_rollback()
        assert manager.state is TransactionState.ROLLED_BACK

        await manager._do_create_savepoint("sp_one")
        await manager._do_release_savepoint("sp_one")
        await manager._do_rollback_savepoint("sp_one")

        assert connection.commit_count == 1
        assert connection.rollback_count == 1
        assert connection.cursor_obj.executed == [
            ("SAVEPOINT sp_one", None),
            ("ROLLBACK TO SAVEPOINT sp_one", None),
        ]

    @pytest.mark.asyncio
    async def test_transaction_isolation_and_mode_statements(self):
        """Async SERIALIZABLE / READ ONLY issue their SET TRANSACTION statements."""
        connection = FakeAsyncConnection(FakeAsyncCursor())
        manager = AsyncOracleTransactionManager(connection, logging.getLogger(LOGGER_NAME))
        manager._isolation_level = IsolationLevel.SERIALIZABLE
        manager._transaction_mode = TransactionMode.READ_ONLY

        await manager._do_begin()

        assert connection.cursor_obj.executed == [
            ("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", None),
            ("SET TRANSACTION READ ONLY", None),
        ]

    @pytest.mark.asyncio
    async def test_transaction_error_branches(self, caplog):
        """Every async transaction failure logs its DEBUG note first."""
        connection = FakeAsyncConnection(FakeAsyncCursor())
        manager = AsyncOracleTransactionManager(connection, logging.getLogger(LOGGER_NAME))
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            manager._isolation_level = IsolationLevel.SERIALIZABLE
            connection.cursor_error = RuntimeError("cursor unavailable")
            with pytest.raises(TransactionError, match="Failed to begin async transaction"):
                await manager._do_begin()

            connection.commit_error = RuntimeError("commit failed")
            with pytest.raises(TransactionError, match="Failed to commit async transaction"):
                await manager._do_commit()

            connection.rollback_error = RuntimeError("rollback failed")
            with pytest.raises(TransactionError, match="Failed to rollback async transaction"):
                await manager._do_rollback()

            with pytest.raises(TransactionError, match="Failed to create savepoint"):
                await manager._do_create_savepoint("sp_error")

            with pytest.raises(TransactionError, match="Failed to rollback to savepoint"):
                await manager._do_rollback_savepoint("sp_error")

        messages = [record.getMessage() for record in caplog.records]
        assert sum("(will re-raise)" in message for message in messages) >= 5


# ---------------------------------------------------------------------------
# Logging surface shared by both backends
# ---------------------------------------------------------------------------


class TestOracleLoggingSurface:
    """Logger plumbing and error normalization shared across the package."""

    def test_cli_info_uses_oracle_logger(self):
        """The CLI module binds the same library logger as the backends."""
        assert cli_info.logger.name == LOGGER_NAME

    def test_mixin_handle_error_normalizes_not_null(self):
        """The mixin's error normalizer treats ORA-01400/01407 alike."""
        backend, _ = make_sync_backend()
        for message in (
            "ORA-01400: cannot insert NULL",
            "ORA-01407: cannot update to NULL",
        ):
            with pytest.raises(IntegrityError, match="cannot be null"):
                OracleBackendMixin._handle_error(backend, OracleDatabaseError(message))

    def test_mixin_no_longer_overrides_log(self):
        """Structured logging relies on the inherited ``log`` implementation."""
        assert "log" not in OracleBackendMixin.__dict__
