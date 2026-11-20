# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/show/
# test_oracle_show_functionality_async_offline.py
"""Database-free async tests for ``AsyncOracleShowFunctionality``.

Oracle's ``oracledb`` driver has a native asyncio interface, so the async show
path must be covered just like the synchronous one.  ``sessions()`` is the only
async method on the class; it awaits ``backend.execute`` and maps rows into
``OracleSessionInfo``.  Note that this stub maps fewer fields than the sync
``query_sessions`` helper (no module/action/logon_time/process), which the
tests below assert explicitly.

A real-but-connectionless ``OracleDialect`` is used because the expression
validates the dialect; all I/O goes through an ``AsyncMock``.  No listener,
cursor, connection or network is involved.
"""

import inspect
from unittest.mock import AsyncMock, MagicMock

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.show.functionality import (
    AsyncOracleShowFunctionality,
)
from rhosocial.activerecord.backend.impl.oracle.show.types import OracleSessionInfo


def async_backend(rows):
    """Return a MagicMock backend whose ``execute`` is an awaitable mock."""
    backend = MagicMock()
    backend.dialect = OracleDialect(version=(19, 0, 0))
    result = MagicMock()
    result.data = rows
    backend.execute = AsyncMock(return_value=result)
    return backend


async def test_wrapper_exposes_backend_dialect_and_version():
    backend = async_backend([])

    wrapper = AsyncOracleShowFunctionality(backend, version=(19, 0, 0))

    assert wrapper.dialect is backend.dialect
    assert wrapper._backend is backend
    assert wrapper._version == (19, 0, 0)


def test_only_sessions_is_an_async_method():
    """Document the async surface: ``sessions`` is the sole coroutine method."""
    coroutine_names = {
        name
        for name, member in inspect.getmembers(
            AsyncOracleShowFunctionality, inspect.iscoroutinefunction
        )
    }
    assert coroutine_names == {"sessions"}


async def test_sessions_maps_supported_fields():
    backend = async_backend(
        [
            {
                "sid": 12,
                "serial_num": 345,
                "username": "SCOTT",
                "status": "ACTIVE",
                "machine": "WORKSTATION",
                "program": "python.exe",
            }
        ]
    )
    wrapper = AsyncOracleShowFunctionality(backend)

    sessions = await wrapper.sessions()

    assert len(sessions) == 1
    session = sessions[0]
    assert isinstance(session, OracleSessionInfo)
    assert session.sid == 12
    assert session.serial_num == 345
    assert session.username == "SCOTT"
    assert session.status == "ACTIVE"
    assert session.machine == "WORKSTATION"
    assert session.program == "python.exe"
    # The async stub does not project these columns (unlike the sync helper).
    assert session.module is None
    assert session.action is None
    assert session.logon_time is None
    assert session.process is None


async def test_sessions_maps_every_row():
    backend = async_backend(
        [
            {"sid": 1, "serial_num": 10, "username": "SCOTT", "status": "ACTIVE"},
            {"sid": 2, "serial_num": 20, "username": None, "status": "INACTIVE"},
        ]
    )
    wrapper = AsyncOracleShowFunctionality(backend)

    sessions = await wrapper.sessions()

    assert [session.sid for session in sessions] == [1, 2]
    assert [session.serial_num for session in sessions] == [10, 20]
    assert sessions[1].username is None
    assert all(isinstance(session, OracleSessionInfo) for session in sessions)


async def test_sessions_missing_optional_fields_become_none():
    backend = async_backend([{"sid": 1, "serial_num": 2}])
    wrapper = AsyncOracleShowFunctionality(backend)

    session = (await wrapper.sessions())[0]

    assert session.username is None
    assert session.status is None
    assert session.machine is None
    assert session.program is None


async def test_sessions_empty_rows_return_empty_list():
    backend = async_backend([])
    wrapper = AsyncOracleShowFunctionality(backend)

    assert await wrapper.sessions() == []


async def test_execute_is_awaited_once_with_sql_and_params():
    backend = async_backend([])
    wrapper = AsyncOracleShowFunctionality(backend)

    await wrapper.sessions()

    backend.execute.assert_awaited_once()
    sql, params = backend.execute.await_args.args
    assert "V$SESSION" in sql.upper()
    assert params == ()


async def test_active_only_changes_filter_and_params():
    for active_only, expected_params in ((False, ()), (True, ("ACTIVE",))):
        backend = async_backend([])
        wrapper = AsyncOracleShowFunctionality(backend)

        await wrapper.sessions(active_only=active_only)

        sql, params = backend.execute.await_args.args
        assert "V$SESSION" in sql.upper()
        if active_only:
            assert '"STATUS" = ?' in sql
            assert "IS NOT NULL" not in sql
        else:
            assert "IS NOT NULL" in sql
            assert "?" not in sql
        assert params == expected_params
