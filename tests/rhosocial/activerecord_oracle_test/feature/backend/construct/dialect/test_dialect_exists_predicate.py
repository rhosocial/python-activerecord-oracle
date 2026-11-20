# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/dialect/test_dialect_exists_predicate.py
"""Offline rendering checks for Oracle EXISTS (scalar vs boolean context).

Core's ``exists()`` turns an ``ExistsPredicate`` into an aliased scalar select
expression. Oracle has no boolean scalar and rejects ``SELECT EXISTS (...)``
(ORA-00936: missing expression), so the Oracle dialect renders the aliased form
as ``CASE WHEN ... THEN 1 ELSE 0 END``; an unaliased EXISTS must stay a plain
boolean predicate for WHERE. This is the Oracle counterpart of SQL Server's
``044cfb2`` fix.
"""

import pytest

from rhosocial.activerecord.backend.expression import (
    Column,
    ExistsPredicate,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.query_parts import WhereClause
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect() -> OracleDialect:
    return OracleDialect((23, 0, 0))


def _subquery(dialect: OracleDialect) -> QueryStatement:
    return QueryStatement(
        dialect,
        select=[Column(dialect, "id")],
        from_=NamedRelationRef(dialect, Table(dialect, "users")),
    )


def test_scalar_exists_renders_case_when(dialect: OracleDialect) -> None:
    """An aliased (scalar) EXISTS uses the portable integer-boolean form."""
    exists_expr = ExistsPredicate(dialect, _subquery(dialect))
    exists_expr.alias = "_exists"
    query = QueryStatement(
        dialect,
        select=[exists_expr],
        from_=NamedRelationRef(dialect, Table(dialect, "users")),
    )
    sql, params = query.to_sql()
    assert sql == (
        'SELECT CASE WHEN EXISTS (SELECT "ID" FROM "USERS") THEN 1 ELSE 0 END '
        'AS "_EXISTS" FROM "USERS"'
    )
    assert params == ()


def test_where_exists_stays_boolean_predicate(dialect: OracleDialect) -> None:
    """An unaliased EXISTS in WHERE is left as a plain predicate."""
    query = QueryStatement(
        dialect,
        select=[Column(dialect, "id")],
        from_=NamedRelationRef(dialect, Table(dialect, "users")),
        where=WhereClause(dialect, condition=ExistsPredicate(dialect, _subquery(dialect))),
    )
    sql, params = query.to_sql()
    assert sql == 'SELECT "ID" FROM "USERS" WHERE EXISTS (SELECT "ID" FROM "USERS")'
    assert params == ()


def test_negated_scalar_exists(dialect: OracleDialect) -> None:
    """NOT EXISTS keeps its negation in the CASE form."""
    exists_expr = ExistsPredicate(dialect, _subquery(dialect), is_not=True)
    exists_expr.alias = "_exists"
    query = QueryStatement(
        dialect,
        select=[exists_expr],
        from_=NamedRelationRef(dialect, Table(dialect, "users")),
    )
    sql, _ = query.to_sql()
    assert sql.startswith("SELECT CASE WHEN NOT EXISTS (")
