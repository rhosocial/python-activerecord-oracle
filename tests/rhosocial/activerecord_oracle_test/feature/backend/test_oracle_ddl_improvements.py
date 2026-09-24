# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_ddl_improvements.py
"""Tests for Oracle DDL improvements: capability gating, UnsupportedFeatureError."""
import pytest
from unittest.mock import patch, PropertyMock

from rhosocial.activerecord.base.ddl import TableDDLDeriver
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.expression import (
    Column,
    TableExpression,
    QueryExpression,
    CreateViewExpression,
    DropViewExpression,
)
from rhosocial.activerecord.backend.expression.statements import ViewOptions, ViewCheckOption
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class InheritingTable(ActiveRecord):
    __table_name__ = "inheriting_table"

    id: int

    @classmethod
    def table_inherits(cls):
        return ["parent_a", "parent_b"]


class TablespacedTable(ActiveRecord):
    __table_name__ = "tablespaced_table"

    id: int

    @classmethod
    def table_tablespace(cls):
        return "ts_data"


class TestOracleTableCapabilityGating:
    def test_table_declaration_defaults_are_absent(self):
        class Plain(ActiveRecord):
            __table_name__ = "plain_table_defaults"

            id: int

        expression = TableDDLDeriver(Plain, OracleDialect(version=(19, 0, 0))).create_table()
        assert expression.inherits == []
        assert expression.tablespace is None

    def test_table_inherits_declaration_is_propagated_and_fails_fast(self):
        dialect = OracleDialect(version=(19, 0, 0))
        expression = TableDDLDeriver(InheritingTable, dialect).create_table()

        assert expression.inherits == ["parent_a", "parent_b"]
        assert dialect.supports_table_inheritance() is False
        with pytest.raises(UnsupportedFeatureError, match="INHERITS"):
            expression.to_sql()

    def test_table_tablespace_declaration_is_propagated_and_rendered(self):
        dialect = OracleDialect(version=(19, 0, 0))
        expression = TableDDLDeriver(TablespacedTable, dialect).create_table()

        assert expression.tablespace == "ts_data"
        assert dialect.supports_tablespace_option() is True
        sql, params = expression.to_sql()
        assert 'TABLESPACE "TS_DATA"' in sql
        assert params == ()


class TestOracleViewCapabilityGating:
    """Tests for Oracle VIEW DDL capability gating."""

    def test_create_view_check_option_gated(self):
        """WITH CHECK OPTION must fail fast when the capability is off."""
        dialect = OracleDialect()
        query = QueryExpression(
            dialect, select=[Column(dialect, "id")], from_=TableExpression(dialect, "t")
        )
        expr = CreateViewExpression(
            dialect,
            view_name="v",
            query=query,
            options=ViewOptions(check_option=ViewCheckOption.CASCADED),
        )
        with patch.object(type(dialect), "supports_view_check_option", return_value=False):
            with pytest.raises(UnsupportedFeatureError, match="CHECK OPTION"):
                expr.to_sql()

    def test_create_or_replace_view_supported(self):
        """Oracle supports CREATE OR REPLACE VIEW."""
        dialect = OracleDialect()
        assert dialect.supports_create_or_replace_view() is True

    def test_drop_view_if_exists_not_supported(self):
        """Oracle does not support DROP VIEW IF EXISTS."""
        dialect = OracleDialect()
        assert dialect.supports_if_exists_view() is False

    def test_materialized_view_supported(self):
        """Oracle supports materialized views."""
        dialect = OracleDialect()
        assert dialect.supports_materialized_view() is True


class TestOracleColumnCapabilityGating:
    """Tests for Oracle COLUMN DDL capability gating."""

    def test_foreign_key_on_delete_supported(self):
        """Oracle supports FK ON DELETE."""
        dialect = OracleDialect()
        assert dialect.supports_foreign_key_on_delete() is True

    def test_foreign_key_on_update_not_supported(self):
        """Oracle does not support FK ON UPDATE."""
        dialect = OracleDialect()
        assert dialect.supports_foreign_key_on_update() is False

    def test_check_constraint_supported(self):
        """Oracle supports CHECK constraints."""
        dialect = OracleDialect()
        assert dialect.supports_check_constraint() is True


class TestOracleSchemaCapabilityGating:
    """Tests for Oracle SCHEMA DDL capability gating."""

    def test_create_schema_not_supported(self):
        """Oracle does not support CREATE SCHEMA (uses users)."""
        dialect = OracleDialect()
        assert dialect.supports_create_schema() is False

    def test_drop_schema_not_supported(self):
        """Oracle does not support DROP SCHEMA."""
        dialect = OracleDialect()
        assert dialect.supports_drop_schema() is False
