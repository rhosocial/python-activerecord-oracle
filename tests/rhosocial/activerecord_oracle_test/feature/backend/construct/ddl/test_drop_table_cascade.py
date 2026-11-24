# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_drop_table_cascade.py
"""Tests for DROP TABLE rendering on Oracle.

Oracle uses the backend-specific ``CASCADE CONSTRAINTS`` form (narrower than
SQL-standard CASCADE) and supports ``PURGE``. ``IF EXISTS`` is a native
statement-level clause from 19c Release Update 19.28 and from 23ai; below
that capability it is refused by name, never silently dropped. The bare
``CASCADE`` / ``RESTRICT`` keywords are illegal and are handled separately.
This test covers:
- ``supports_drop_table_cascade() / restrict()`` are False (bare tokens illegal).
- ``supports_cascade_constraints() / supports_purge_on_drop_table()`` are True.
- ``cascade=True`` renders ``CASCADE CONSTRAINTS`` (with optional ``PURGE``).
- ``restrict=True`` raises ``UnsupportedFeatureError`` (Oracle has no RESTRICT).
- ``if_exists=True`` renders at the floor and refuses below it.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import DropTableStatement
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestDropTableCascadeCapabilities:
    def test_global_cascade_switches_off(self, dialect):
        assert dialect.supports_drop_table_cascade() is False
        assert dialect.supports_drop_table_restrict() is False

    def test_oracle_specific_switches_on(self, dialect):
        assert dialect.supports_cascade_constraints() is True
        assert dialect.supports_purge_on_drop_table() is True


class TestDropTableRendering:
    def test_cascade_true_renders_cascade_constraints(self, dialect):
        expr = DropTableStatement(dialect, table=Table(dialect, "users"), cascade=True)
        sql, params = expr.to_sql()
        assert sql.endswith(" CASCADE CONSTRAINTS")
        assert "PURGE" not in sql
        assert params == ()

    def test_cascade_true_with_purge_option(self, dialect):
        expr = DropTableStatement(
            dialect,
            table=Table(dialect, "users"),
            cascade=True,
            purge=True,
        )
        sql, params = expr.to_sql()
        assert sql.endswith(" CASCADE CONSTRAINTS PURGE")
        assert params == ()

    def test_purge_without_cascade(self, dialect):
        expr = DropTableStatement(dialect, table=Table(dialect, "users"), purge=True)
        sql, params = expr.to_sql()
        assert sql.endswith(" PURGE")
        assert "CASCADE" not in sql
        assert params == ()

    def test_restrict_true_raises(self, dialect):
        expr = DropTableStatement(dialect, table=Table(dialect, "users"), restrict=True)
        with pytest.raises(UnsupportedFeatureError, match="DROP TABLE ... RESTRICT"):
            expr.to_sql()

    def test_unspecified_omits_clause(self, dialect):
        expr = DropTableStatement(dialect, table=Table(dialect, "users"))
        sql, params = expr.to_sql()
        assert "CASCADE" not in sql
        assert "RESTRICT" not in sql
        assert params == ()

    def test_if_exists_renders_from_19_28_ru(self, dialect):
        """The 19c line carries the clause from Release Update 19.28."""
        d1928 = OracleDialect(version=(19, 0, 0), version_full=(19, 28, 0, 0, 0))
        expr = DropTableStatement(d1928, table=Table(d1928, "users"), if_exists=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP TABLE IF EXISTS "USERS"'
        assert params == ()

    def test_if_exists_renders_from_23ai(self):
        d23 = OracleDialect(version=(23, 0, 0))
        expr = DropTableStatement(d23, table=Table(d23, "users"), if_exists=True)
        assert expr.to_sql() == ('DROP TABLE IF EXISTS "USERS"', ())

    def test_if_exists_below_floor_refused(self, dialect):
        """A pre-floor request is refused by name, not silently dropped."""
        expr = DropTableStatement(dialect, table=Table(dialect, "users"), if_exists=True)
        with pytest.raises(UnsupportedFeatureError, match="DROP TABLE IF EXISTS"):
            expr.to_sql()
