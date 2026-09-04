# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/test_oracle_insert_all_expressions.py
"""Tests for Oracle INSERT ALL / FIRST multi-table insert formatters.

Covers the ``OracleDMLOperationMixin`` ``format_insert_all_statement`` and
``format_insert_first_statement`` formatters: unconditional ``INTO``
clauses, conditional ``WHEN ... THEN INTO`` / ``ELSE INTO`` branches, the
trailing ``SELECT ... FROM src`` query, parameter binding, and the
``(9, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.core import Column, Literal, TableExpression
from rhosocial.activerecord.backend.expression.statements import QueryExpression
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


def build_query(dialect):
    query = QueryExpression(
        dialect,
        select=[Column(dialect, "a"), Column(dialect, "b")],
        from_=TableExpression(dialect, "src"),
    )
    return query.to_sql()[0]


class TestOracleInsertAllCapabilities:
    def test_supports_insert_all(self, dialect):
        assert dialect.supports_insert_all() is True
        assert dialect.supports_insert_first() is True
        assert dialect.supports_multi_table_insert() is True


class TestOracleInsertAllStatement:
    def test_unconditional_into_clauses(self, dialect):
        sql, params = dialect.format_insert_all_statement(
            [
                {"table": "t1", "columns": ["a"], "values": "x"},
                {"table": "t2", "columns": "a", "values": "y"},
            ],
            select_query=build_query(dialect),
        )
        assert sql == (
            "INSERT ALL INTO T1 (A) VALUES (x) INTO T2 (A) VALUES (y) "
            "SELECT a, b FROM SRC"
        )
        assert params == ()

    def test_no_trailing_select(self, dialect):
        sql, params = dialect.format_insert_all_statement(
            [{"table": "t1", "columns": "a", "values": "x"}]
        )
        assert sql == "INSERT ALL INTO T1 (A) VALUES (x)"
        assert params == ()

    def test_multiple_columns(self, dialect):
        sql, params = dialect.format_insert_all_statement(
            [{"table": "t1", "columns": ["a", "b"], "values": "x, y"}]
        )
        assert sql == "INSERT ALL INTO T1 (A, B) VALUES (x, y)"
        assert params == ()

    def test_with_condition_and_else(self, dialect):
        sql, params = dialect.format_insert_all_statement(
            [{"table": "t1", "columns": "a", "values": "x"}],
            select_query=build_query(dialect),
            when_clauses=[
                {"condition": "b > 10", "table": "t3", "columns": "a", "values": "z"}
            ],
            else_clause={"table": "t4", "columns": "a", "values": "w"},
        )
        assert sql == (
            "INSERT ALL INTO T1 (A) VALUES (x) "
            "WHEN b > 10 THEN INTO T3 (A) VALUES (z) "
            "ELSE INTO T4 (A) VALUES (w) SELECT a, b FROM SRC"
        )
        assert params == ()

    def test_expression_values_bind_params(self, dialect):
        sql, params = dialect.format_insert_all_statement(
            [
                {"table": "t1", "columns": "a", "values": Literal(dialect, 1)},
                {"table": "t2", "columns": "a", "values": Literal(dialect, 2)},
            ]
        )
        assert sql == "INSERT ALL INTO T1 (A) VALUES (?) INTO T2 (A) VALUES (?)"
        assert params == (1, 2)

    def test_expression_values_in_when_branch(self, dialect):
        sql, params = dialect.format_insert_all_statement(
            [{"table": "t1", "columns": "a", "values": "x"}],
            select_query=build_query(dialect),
            when_clauses=[
                {
                    "condition": "b > 10",
                    "table": "t3",
                    "columns": "a",
                    "values": Literal(dialect, "z"),
                }
            ],
        )
        assert sql == (
            "INSERT ALL INTO T1 (A) VALUES (x) "
            "WHEN b > 10 THEN INTO T3 (A) VALUES (?) SELECT a, b FROM SRC"
        )
        assert params == ("z",)


class TestOracleInsertFirstStatement:
    def test_basic_first(self, dialect):
        sql, params = dialect.format_insert_first_statement(
            [{"table": "t1", "columns": "a", "values": "x"}],
            select_query=build_query(dialect),
        )
        assert sql == "INSERT FIRST INTO T1 (A) VALUES (x) SELECT a, b FROM SRC"
        assert params == ()

    def test_first_with_condition_and_else(self, dialect):
        sql, params = dialect.format_insert_first_statement(
            [{"table": "t1", "columns": "a", "values": "x"}],
            when_clauses=[
                {"condition": "b > 10", "table": "t3", "columns": "a", "values": "z"}
            ],
            else_clause={"table": "t4", "columns": "a", "values": "w"},
        )
        assert sql == (
            "INSERT FIRST INTO T1 (A) VALUES (x) "
            "WHEN b > 10 THEN INTO T3 (A) VALUES (z) "
            "ELSE INTO T4 (A) VALUES (w)"
        )
        assert params == ()

    def test_first_uses_select_query(self, dialect):
        sql, params = dialect.format_insert_first_statement(
            [
                {"table": "t1", "columns": ["a", "b"], "values": "a, b"},
            ],
            select_query=build_query(dialect),
        )
        assert sql == "INSERT FIRST INTO T1 (A, B) VALUES (a, b) SELECT a, b FROM SRC"
        assert params == ()


class TestOracleMultiTableInsertVersionBoundary:
    def test_insert_all_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        with pytest.raises(UnsupportedFeatureError, match="INSERT ALL"):
            d8.format_insert_all_statement([{"table": "t1", "columns": "a", "values": "x"}])

    def test_insert_first_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        with pytest.raises(UnsupportedFeatureError, match="INSERT FIRST"):
            d8.format_insert_first_statement([{"table": "t1", "columns": "a", "values": "x"}])

    def test_at_9i_works(self):
        d9 = OracleDialect(version=(9, 0, 0))
        sql, params = d9.format_insert_all_statement(
            [{"table": "t1", "columns": "a", "values": "x"}]
        )
        assert sql == "INSERT ALL INTO T1 (A) VALUES (x)"
        assert params == ()
