# tests/rhosocial/activerecord_oracle_test/feature/backend/dml/test_execute_many.py
"""Synchronous Oracle backend ``execute_many`` batch semantics.

Verifies total affected-row accounting for a batch INSERT, the noop behaviour
for an empty parameter list, and that ``?`` placeholders are rewritten to the
Oracle ``:N`` form inside a batch, mirroring ``test_dml_deep_async.py``.
"""
import uuid

import pytest

from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


def _ddl_options() -> ExecutionOptions:
    return ExecutionOptions(stmt_type=StatementType.DDL)


def _select_options() -> ExecutionOptions:
    return ExecutionOptions(stmt_type=StatementType.SELECT)


def _table_name(base: str = "test_execute_many") -> str:
    return f"{base}_{uuid.uuid4().hex[:8]}"


def _fetch_scalar(backend, sql: str) -> object:
    result = backend.execute(sql, options=_select_options())
    first = (result.data or [])[0]
    if isinstance(first, dict):
        return list(first.values())[0]
    return first[0]


@pytest.fixture
def batch_table(oracle_backend_single):
    table = _table_name()
    oracle_backend_single.execute(
        f"CREATE TABLE {table} (name VARCHAR2(50))", options=_ddl_options()
    )
    yield table
    oracle_backend_single.execute(
        f"DROP TABLE {table} PURGE", options=_ddl_options()
    )


class TestExecuteMany:
    def test_batch_insert_reports_total_affected_rows(
        self, oracle_backend_single, batch_table
    ):
        sql = f"INSERT INTO {batch_table} (name) VALUES (:1)"
        params_list = [(f"row_{i}",) for i in range(5)]
        result = oracle_backend_single.execute_many(sql, params_list)
        assert result.affected_rows == 5, "batch insert should report all 5 affected rows"
        count = _fetch_scalar(
            oracle_backend_single, f"SELECT COUNT(*) AS CNT FROM {batch_table}"
        )
        assert int(count) == 5, "all 5 rows should be persisted"

    def test_empty_params_list_is_noop(self, oracle_backend_single, batch_table):
        oracle_backend_single.execute(
            f"INSERT INTO {batch_table} (name) VALUES (:1)",
            ("seed",),
            options=_ddl_options(),
        )
        before = _fetch_scalar(
            oracle_backend_single, f"SELECT COUNT(*) AS CNT FROM {batch_table}"
        )
        result = oracle_backend_single.execute_many(
            f"INSERT INTO {batch_table} (name) VALUES (:1)", []
        )
        assert result is not None, "execute_many should return a QueryResult even for empty params"
        assert result.affected_rows == 0, "empty batch should affect no rows"
        after = _fetch_scalar(
            oracle_backend_single, f"SELECT COUNT(*) AS CNT FROM {batch_table}"
        )
        assert int(after) == int(before), "empty batch must not change row count"

    def test_qmark_placeholders_are_converted_in_batch(
        self, oracle_backend_single, batch_table
    ):
        sql = f"INSERT INTO {batch_table} (name) VALUES (?)"
        params_list = [(f"qmark_{i}",) for i in range(3)]
        result = oracle_backend_single.execute_many(sql, params_list)
        assert result.affected_rows == 3, "qmark batch should affect 3 rows"
        count = _fetch_scalar(
            oracle_backend_single,
            f"SELECT COUNT(*) AS CNT FROM {batch_table} "
            f"WHERE name LIKE 'qmark_%'",
        )
        assert int(count) == 3, "3 qmark rows should be persisted"