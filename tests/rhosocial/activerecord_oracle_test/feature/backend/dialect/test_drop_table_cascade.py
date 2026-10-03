# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_drop_table_cascade.py
"""Tests for DROP TABLE rendering on Oracle.

Oracle uses the backend-specific ``CASCADE CONSTRAINTS`` form (narrower than
SQL-standard CASCADE) and supports ``PURGE``. The dialect never emits an
``IF EXISTS`` token (Oracle lacks the syntax), nor the bare ``CASCADE`` /
``RESTRICT`` keywords. This tests covers:
- ``supports_drop_table_cascade() / restrict()`` are False (bare tokens illegal).
- ``supports_cascade_constraints() / supports_purge_on_drop_table()`` are True.
- ``cascade=True`` renders ``CASCADE CONSTRAINTS`` (with optional ``PURGE``).
- ``cascade=False`` raises ``UnsupportedFeatureError`` (Oracle has no RESTRICT).
- ``cascade=None`` omits the clause; ``if_exists=True`` raises rather than being dropped.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import DropTableExpression
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleDropTableCascadeCapabilities:
    def test_global_cascade_switches_off(self, dialect):
        assert dialect.supports_drop_table_cascade() is False
        assert dialect.supports_drop_table_restrict() is False

    def test_oracle_specific_switches_on(self, dialect):
        assert dialect.supports_cascade_constraints() is True
        assert dialect.supports_purge_on_drop_table() is True


class TestOracleDropTableRendering:
    def test_cascade_true_renders_cascade_constraints(self, dialect):
        from rhosocial.activerecord.backend.expression.core import TableExpression
        expr = DropTableExpression(dialect, table=TableExpression(dialect, "users"), cascade=True)
        sql, params = expr.to_sql()
        assert sql.endswith(" CASCADE CONSTRAINTS")
        assert "PURGE" not in sql
        assert params == ()

    def test_cascade_true_with_purge_option(self, dialect):
        from rhosocial.activerecord.backend.expression.core import TableExpression
        expr = DropTableExpression(
            dialect,
            table=TableExpression(dialect, "users"),
            cascade=True,
            purge=True,
        )
        sql, params = expr.to_sql()
        assert sql.endswith(" CASCADE CONSTRAINTS PURGE")
        assert params == ()

    def test_purge_without_cascade(self, dialect):
        from rhosocial.activerecord.backend.expression.core import TableExpression
        expr = DropTableExpression(dialect, table=TableExpression(dialect, "users"), purge=True)
        sql, params = expr.to_sql()
        assert sql.endswith(" PURGE")
        assert "CASCADE" not in sql
        assert params == ()

    def test_cascade_false_raises(self, dialect):
        from rhosocial.activerecord.backend.expression.core import TableExpression
        expr = DropTableExpression(dialect, table=TableExpression(dialect, "users"), cascade=False)
        with pytest.raises(UnsupportedFeatureError, match="DROP TABLE ... RESTRICT"):
            expr.to_sql()

    def test_cascade_none_omits_clause(self, dialect):
        from rhosocial.activerecord.backend.expression.core import TableExpression
        expr = DropTableExpression(dialect, table=TableExpression(dialect, "users"), cascade=None)
        sql, params = expr.to_sql()
        assert "CASCADE" not in sql
        assert "RESTRICT" not in sql
        assert params == ()

    def test_if_exists_is_refused(self, dialect):
        """Oracle has no IF EXISTS clause, so the flag is refused outright.

        Emitting the statement without the clause would turn a requested
        no-op into a failure on a missing table, which is the opposite of
        what the caller asked for.
        """
        assert dialect.supports_if_exists_table() is False
        from rhosocial.activerecord.backend.expression.core import TableExpression
        expr = DropTableExpression(dialect, table=TableExpression(dialect, "users"), if_exists=True)
        with pytest.raises(UnsupportedFeatureError, match="DROP TABLE IF EXISTS"):
            expr.to_sql()

    def test_if_not_set_renders_plainly(self, dialect):
        from rhosocial.activerecord.backend.expression.core import TableExpression
        expr = DropTableExpression(dialect, table=TableExpression(dialect, "users"))
        sql, params = expr.to_sql()
        assert sql == 'DROP TABLE "USERS"'
        assert params == ()
