# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_dql_capabilities.py
"""Oracle DQL capability tests: WITH TIES, NULLS ordering and Wait lock."""

import pytest

from rhosocial.activerecord.backend.expression import (
    Column,
    ForUpdateClause,
    LimitOffsetClause,
    LockStrength,
    OrderByClause,
    OrderByExpression,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateClause


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 0, 0))


def test_fetch_with_ties_supported(dialect):
    assert dialect.supports_fetch_with_ties() is True


def test_fetch_with_ties_renders(dialect):
    sql, params = dialect.format_limit_offset_clause(LimitOffsetClause(dialect, limit=5, with_ties=True))
    assert sql == "FETCH FIRST ? ROWS WITH TIES"
    assert params == (5,)


def test_nulls_first_last_supported(dialect):
    assert dialect.supports_nulls_first_last() is True
    sql, _ = OrderByClause(
        dialect,
        expressions=[OrderByExpression(dialect, Column(dialect, "name"), nulls_last=True)],
    ).to_sql()
    assert "NULLS LAST" in sql


def test_wait_lock_renders(dialect):
    sql, _ = OracleForUpdateClause(dialect, wait=5).to_sql()
    assert sql == "FOR UPDATE WAIT 5"


def test_share_strength_raises(dialect):
    with pytest.raises(Exception):
        ForUpdateClause(dialect, strength=LockStrength.SHARE).to_sql()
