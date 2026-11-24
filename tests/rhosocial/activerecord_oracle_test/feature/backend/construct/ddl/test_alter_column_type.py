# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_alter_column_type.py
"""A column type change is rendered as Oracle's ``MODIFY (col type)`` clause.

Oracle has no ``SET DATA TYPE`` spelling: changing a column's type is a
redefinition expressed through ``ALTER TABLE ... MODIFY (col type)``. The
backend formatter must route the standard ALTER COLUMN action through that
path so the rejected standard spelling never reaches the server.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import AlterTableStatement
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AlterColumnAction,
    ColumnAlterOperation,
)
from rhosocial.activerecord.backend.expression.types import IntegerType, VarCharType
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestAlterColumnTypeRendering:
    def test_set_data_type_renders_as_modify(self, dialect):
        action = AlterColumnAction(
            dialect,
            "c",
            ColumnAlterOperation.SET_DATA_TYPE,
            new_value=IntegerType(dialect),
        )
        sql, params = action.to_sql()
        assert sql == 'MODIFY ("C" NUMBER(10))'
        assert "SET DATA TYPE" not in sql
        assert params == ()

    def test_set_data_type_in_alter_table_statement(self, dialect):
        action = AlterColumnAction(
            dialect,
            "c",
            ColumnAlterOperation.SET_DATA_TYPE,
            new_value=VarCharType(dialect, length=20),
        )
        stmt = AlterTableStatement(dialect, Table(dialect, "t"), actions=[action])
        sql, params = stmt.to_sql()
        assert sql == 'ALTER TABLE "T" MODIFY ("C" VARCHAR2(20))'
        assert "SET DATA TYPE" not in sql
        assert params == ()

    def test_set_data_type_accepts_raw_operation_spelling(self, dialect):
        action = AlterColumnAction(
            dialect, "c", "SET DATA TYPE", new_value=IntegerType(dialect)
        )
        assert action.to_sql()[0] == 'MODIFY ("C" NUMBER(10))'

    def test_non_type_properties_keep_the_core_gate(self, dialect):
        # SET/DROP DEFAULT and SET/DROP NOT NULL are not offered through this
        # action on Oracle; the delegated core formatter still refuses them.
        action = AlterColumnAction(
            dialect, "c", ColumnAlterOperation.SET_DEFAULT, new_value="'x'"
        )
        with pytest.raises(UnsupportedFeatureError):
            action.to_sql()
