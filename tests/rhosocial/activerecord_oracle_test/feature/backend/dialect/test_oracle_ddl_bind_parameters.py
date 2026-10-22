# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_ddl_bind_parameters.py
"""Oracle DDL bodies may not carry bind variables.

Oracle's DDL statements never take bind variables. The driver refuses such a
statement before the server sees it -- measured on 18c / 21c / 26ai:
"DPY-4009: 0 positional bind values are required but 1 were provided" -- so
the request is refused by name, with the way out named, instead of being
rendered for the driver to reject.

That leaves the caller with one way forward: build the query with an inlined
literal. Both directions are pinned here, because a case that only ever
exercised the working side could not notice the refusal going away.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    CreateMaterializedViewStatement,
    CreateTableAsStatement,
    Literal,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import MaterializedView, Table
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

ORACLE_19 = (19, 0, 0)


def _dialect():
    return OracleDialect(version=ORACLE_19)


def _query(dialect, value, inline=False):
    return QueryStatement(
        dialect=dialect,
        select=[Literal(dialect, value, inline=inline)],
        from_=Table(dialect, "src"),
    )


class TestCreateTableAsRefusesBoundParameters:
    """``CREATE TABLE ... AS`` with a binding body is refused."""

    def test_a_binding_body_is_refused_by_name(self):
        dialect = _dialect()
        expr = CreateTableAsStatement(
            dialect, Table(dialect, "t"), _query(dialect, 1)
        )
        with pytest.raises(UnsupportedFeatureError) as excinfo:
            expr.to_sql()
        message = str(excinfo.value)
        assert "CREATE TABLE ... AS with bound parameters" in message, message
        assert "DPY-4009" in message, message
        assert "inlined literal" in message, message

    def test_an_inlined_body_renders(self):
        dialect = _dialect()
        expr = CreateTableAsStatement(
            dialect, Table(dialect, "t"), _query(dialect, 1, inline=True)
        )
        sql, params = expr.to_sql()
        assert "SELECT 1" in sql, sql
        assert params == ()


class TestCreateMaterializedViewRefusesBoundParameters:
    """``CREATE MATERIALIZED VIEW`` with a binding body is refused."""

    def test_a_binding_body_is_refused_by_name(self):
        dialect = _dialect()
        expr = CreateMaterializedViewStatement(
            dialect, MaterializedView(dialect, "mv"), _query(dialect, 1)
        )
        with pytest.raises(UnsupportedFeatureError) as excinfo:
            expr.to_sql()
        message = str(excinfo.value)
        assert "CREATE MATERIALIZED VIEW with bound parameters" in message, message
        assert "DPY-4009" in message, message
        assert "inlined literal" in message, message

    def test_an_inlined_body_renders(self):
        dialect = _dialect()
        expr = CreateMaterializedViewStatement(
            dialect, MaterializedView(dialect, "mv"), _query(dialect, 1, inline=True)
        )
        sql, params = expr.to_sql()
        assert "SELECT 1" in sql, sql
        assert params == ()
