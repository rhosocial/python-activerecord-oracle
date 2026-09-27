# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_column_definition.py
"""Tests for the Oracle-specific column definition expressions."""

import pytest

from rhosocial.activerecord.backend.expression import ColumnDefinition
from rhosocial.activerecord.backend.expression.types import IntegerType
from rhosocial.activerecord.backend.impl.oracle import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleColumnDefinition,
    OracleColumnOptions,
)


@pytest.fixture
def dialect():
    return OracleDialect((19, 0, 0))


def test_derives_generic_column_definition():
    assert ColumnDefinition in OracleColumnDefinition.__mro__


def test_invisible(dialect):
    sql, _ = OracleColumnDefinition(dialect, "x", IntegerType(dialect), invisible=True).to_sql()
    assert sql == '"X" NUMBER(10) INVISIBLE'


def test_generic_column_still_renders_on_oracle(dialect):
    generic = ColumnDefinition(dialect, "x", IntegerType(dialect))
    sql, _ = generic.to_sql()
    assert sql == '"X" NUMBER(10)'


def test_options_select_oracle_column_class():
    assert OracleColumnOptions(invisible=True).column_definition_class() is OracleColumnDefinition


def test_options_apply_to(dialect):
    options = OracleColumnOptions(invisible=True)
    col = OracleColumnDefinition(dialect, "c", IntegerType(dialect))
    options.apply_to(col)
    assert col.invisible is True


def test_options_apply_to_rejects_generic_column(dialect):
    options = OracleColumnOptions(invisible=True)
    generic = ColumnDefinition(dialect, "c", IntegerType(dialect))
    with pytest.raises(TypeError, match="OracleColumnDefinition"):
        options.apply_to(generic)
