# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_materialized_view_expressions.py
"""Tests for Oracle materialized view expressions.

Covers ``CREATE MATERIALIZED VIEW`` (REFRESH / QUERY REWRITE / BUILD
options), ``CREATE MATERIALIZED VIEW LOG`` (WITH ROWID / PRIMARY KEY) and
``DROP MATERIALIZED VIEW`` (PRESERVE TABLE), plus capability switches,
identifier quoting, the ``IF NOT EXISTS`` / ``IF EXISTS`` version gate
(23ai) and the ``(9, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    CreateMaterializedViewExpression,
    QueryExpression,
)
from rhosocial.activerecord.backend.expression.core import Column, TableExpression
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    MaterializedViewBuildMode,
    MaterializedViewRefreshMethod,
    MaterializedViewRefreshTrigger,
    OracleCreateMaterializedViewExpression,
    OracleCreateMaterializedViewLogExpression,
    OracleDropMaterializedViewExpression,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


def build_query(dialect):
    return QueryExpression(
        dialect,
        select=[Column(dialect, "id"), Column(dialect, "name")],
        from_=TableExpression(dialect, "t"),
    )


class TestOracleMaterializedViewCapabilities:
    def test_supports_materialized_view(self, dialect):
        assert dialect.supports_materialized_view() is True
        assert dialect.supports_materialized_view_log() is True
        assert dialect.supports_materialized_view_tablespace() is True


class TestOracleCreateMaterializedViewExpression:
    def test_basic_create(self, dialect):
        expr = OracleCreateMaterializedViewExpression(
            dialect, view_name="mv", query=build_query(dialect)
        )
        sql, params = expr.to_sql()
        assert sql.startswith('CREATE MATERIALIZED VIEW "MV"')
        assert sql.endswith('AS SELECT "ID", "NAME" FROM "T"')
        assert params == ()

    def test_full_options(self, dialect):
        expr = OracleCreateMaterializedViewExpression(
            dialect,
            view_name="mv",
            query=build_query(dialect),
            column_aliases=["id", "name"],
            tablespace="ts_data",
            build_mode=MaterializedViewBuildMode.IMMEDIATE,
            refresh_method=MaterializedViewRefreshMethod.FAST,
            refresh_trigger=MaterializedViewRefreshTrigger.ON_COMMIT,
            query_rewrite=True,
        )
        sql, params = expr.to_sql()
        assert sql == (
            'CREATE MATERIALIZED VIEW "MV" ("ID", "NAME") TABLESPACE "TS_DATA" '
            "BUILD IMMEDIATE REFRESH FAST ON COMMIT ENABLE QUERY REWRITE "
            'AS SELECT "ID", "NAME" FROM "T"'
        )
        assert params == ()

    def test_refresh_complete_on_demand_disable_rewrite(self, dialect):
        expr = OracleCreateMaterializedViewExpression(
            dialect,
            view_name="mv",
            query=build_query(dialect),
            refresh_method=MaterializedViewRefreshMethod.COMPLETE,
            refresh_trigger=MaterializedViewRefreshTrigger.ON_DEMAND,
            query_rewrite=False,
        )
        sql, params = expr.to_sql()
        assert sql == (
            'CREATE MATERIALIZED VIEW "MV" REFRESH COMPLETE ON DEMAND '
            'DISABLE QUERY REWRITE AS SELECT "ID", "NAME" FROM "T"'
        )
        assert params == ()

    def test_refresh_force_and_build_deferred(self, dialect):
        expr = OracleCreateMaterializedViewExpression(
            dialect,
            view_name="mv",
            query=build_query(dialect),
            refresh_method=MaterializedViewRefreshMethod.FORCE,
            build_mode=MaterializedViewBuildMode.DEFERRED,
        )
        sql, params = expr.to_sql()
        assert sql == (
            'CREATE MATERIALIZED VIEW "MV" BUILD DEFERRED REFRESH FORCE '
            'AS SELECT "ID", "NAME" FROM "T"'
        )
        assert params == ()

    def test_core_create_materialized_view_expression(self, dialect):
        expr = CreateMaterializedViewExpression(
            dialect, view_name="mv", query=build_query(dialect), with_data=False
        )
        sql, params = expr.to_sql()
        assert sql.startswith('CREATE MATERIALIZED VIEW "MV"')
        assert "BUILD DEFERRED" in sql
        assert sql.endswith('AS SELECT "ID", "NAME" FROM "T"')
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleCreateMaterializedViewExpression(
            dialect, view_name="Sales_MV", query=build_query(dialect)
        )
        sql, params = expr.to_sql()
        assert sql.startswith('CREATE MATERIALIZED VIEW "SALES_MV"')
        assert params == ()

    def test_invalid_query_type_rejected(self, dialect):
        with pytest.raises(TypeError, match="query must be a BaseExpression"):
            OracleCreateMaterializedViewExpression(dialect, view_name="mv", query="select")

    def test_empty_view_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="view_name must be a non-empty string"):
            OracleCreateMaterializedViewExpression(dialect, view_name="  ", query=build_query(dialect))

    def test_if_not_exists_pre_23ai_raises(self, dialect):
        expr = OracleCreateMaterializedViewExpression(
            dialect, view_name="mv", query=build_query(dialect), if_not_exists=True
        )
        with pytest.raises(UnsupportedFeatureError, match="IF NOT EXISTS"):
            expr.to_sql()

    def test_if_not_exists_23ai(self):
        d23 = OracleDialect(version=(23, 0, 0))
        expr = OracleCreateMaterializedViewExpression(
            d23, view_name="mv", query=build_query(d23), if_not_exists=True
        )
        sql, params = expr.to_sql()
        assert sql.startswith('CREATE MATERIALIZED VIEW IF NOT EXISTS "MV"')
        assert params == ()


class TestOracleCreateMaterializedViewLogExpression:
    def test_with_primary_key(self, dialect):
        expr = OracleCreateMaterializedViewLogExpression(
            dialect, table="orders", with_primary_key=True
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE MATERIALIZED VIEW LOG ON "ORDERS" WITH PRIMARY KEY'
        assert params == ()

    def test_with_rowid(self, dialect):
        expr = OracleCreateMaterializedViewLogExpression(dialect, table="orders", with_rowid=True)
        sql, params = expr.to_sql()
        assert sql == 'CREATE MATERIALIZED VIEW LOG ON "ORDERS" WITH ROWID'
        assert params == ()

    def test_with_both(self, dialect):
        expr = OracleCreateMaterializedViewLogExpression(
            dialect, table="orders", with_rowid=True, with_primary_key=True
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE MATERIALIZED VIEW LOG ON "ORDERS" WITH ROWID, PRIMARY KEY'
        assert params == ()

    def test_with_clause_required(self, dialect):
        with pytest.raises(ValueError, match="requires WITH ROWID and/or WITH PRIMARY KEY"):
            OracleCreateMaterializedViewLogExpression(dialect, table="orders")

    def test_empty_table_rejected(self, dialect):
        with pytest.raises(ValueError, match="table must be a non-empty string"):
            OracleCreateMaterializedViewLogExpression(dialect, table="  ", with_rowid=True)


class TestOracleDropMaterializedViewExpression:
    def test_basic_drop(self, dialect):
        expr = OracleDropMaterializedViewExpression(dialect, view_name="mv")
        sql, params = expr.to_sql()
        assert sql == 'DROP MATERIALIZED VIEW "MV"'
        assert params == ()

    def test_preserve_table(self, dialect):
        expr = OracleDropMaterializedViewExpression(dialect, view_name="mv", preserve_table=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP MATERIALIZED VIEW "MV" PRESERVE TABLE'
        assert params == ()

    def test_if_exists_pre_23ai_raises(self, dialect):
        expr = OracleDropMaterializedViewExpression(dialect, view_name="mv", if_exists=True)
        with pytest.raises(UnsupportedFeatureError, match="IF EXISTS"):
            expr.to_sql()

    def test_if_exists_23ai(self):
        d23 = OracleDialect(version=(23, 0, 0))
        expr = OracleDropMaterializedViewExpression(d23, view_name="mv", if_exists=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP MATERIALIZED VIEW IF EXISTS "MV"'
        assert params == ()

    def test_empty_view_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="view_name must be a non-empty string"):
            OracleDropMaterializedViewExpression(dialect, view_name="  ")


class TestOracleMaterializedViewVersionBoundary:
    def test_create_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreateMaterializedViewExpression(
            d8, view_name="mv", query=build_query(d8)
        )
        with pytest.raises(UnsupportedFeatureError, match="CREATE MATERIALIZED VIEW"):
            expr.to_sql()

    def test_log_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreateMaterializedViewLogExpression(d8, table="t", with_rowid=True)
        with pytest.raises(UnsupportedFeatureError, match="MATERIALIZED VIEW LOG"):
            expr.to_sql()

    def test_drop_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleDropMaterializedViewExpression(d8, view_name="mv")
        with pytest.raises(UnsupportedFeatureError, match="DROP MATERIALIZED VIEW"):
            expr.to_sql()

    def test_at_9i_works(self):
        d9 = OracleDialect(version=(9, 0, 0))
        expr = OracleCreateMaterializedViewExpression(d9, view_name="mv", query=build_query(d9))
        assert expr.to_sql()[0].startswith('CREATE MATERIALIZED VIEW "MV"')
