# tests/rhosocial/activerecord_oracle_test/feature/backend/explain/test_oracle_explain_deep.py
"""Backend-local, deep EXPLAIN tests for Oracle.

The shared testsuite contracts only assert that EXPLAIN *can run*. This file
pins what is genuinely backend-specific, using the expression system only:

* Capability gates and the exact SQL rendered by ``format_explain_statement``
  are checked construction-level. Oracle's ``EXPLAIN PLAN FOR`` writes to
  ``PLAN_TABLE`` and yields no client result set, so this backend renders a
  ``SELECT 1 AS EXPLAIN_PLAN FROM DUAL`` fallback and advertises ANALYZE / FORMAT
  as unsupported. The wrapped statement is always built from expression objects
  (``QueryStatement`` / ``NamedRelationRef`` / ``Table`` / ``Column`` / ...) --
  never from a SQL string.
* PLAN_TABLE result *interpretation* is checked through the production
  ``OracleExplainResult.from_rows`` machinery, which maps OPERATION / OPTIONS /
  OBJECT_NAME / ID / PARENT_ID rows into a tree and exposes scan / index / join
  / aggregate evidence.

A live-DB PLAN_TABLE test is deliberately absent. Oracle has no expression type
for the ``EXPLAIN PLAN SET STATEMENT_ID ... FOR`` + ``PLAN_TABLE`` flow, and
neither ``OracleBackend`` nor ``AsyncOracleBackend`` implements ``explain()``;
the local renderer only produces the SELECT-from-DUAL fallback. There is
therefore no way to obtain real plan rows through the public expression API,
and hand-built SQL is forbidden. Real EXPLAIN acceptance against a partitioned
table is already covered by the ``execute/partition`` tests.
"""

from typing import Any, Dict, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.dialect import protocols as dialect_protocols
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    AllColumnsWildcard,
    Column,
    ComparisonPredicate,
    ExplainFormat,
    ExplainOptions,
    ExplainStatement,
    ExplainType,
    Literal,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.query_parts import WhereClause
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
from rhosocial.activerecord.backend.impl.oracle import dialect as oracle_dialect
from rhosocial.activerecord.backend.impl.oracle.explain import (
    OracleExplainResult,
    OracleExplainRow,
)

# --- helpers ---------------------------------------------------------------

_FALLBACK_SQL = "SELECT 1 AS EXPLAIN_PLAN FROM DUAL"


def _dialect() -> oracle_dialect.OracleDialect:
    return oracle_dialect.OracleDialect(version=(23, 0, 0))


def _query(dialect: oracle_dialect.OracleDialect) -> QueryStatement:
    """A fully expression-built SELECT used as the wrapped EXPLAIN statement."""
    return QueryStatement(
        dialect,
        select=[Column(dialect, "id"), Column(dialect, "name")],
        from_=NamedRelationRef(dialect, Table(dialect, "users")),
        where=WhereClause(
            dialect,
            condition=ComparisonPredicate(
                dialect,
                ">",
                Column(dialect, "price"),
                Literal(dialect, 100),
            ),
        ),
    )


def _wildcard_query(dialect: oracle_dialect.OracleDialect) -> QueryStatement:
    """Second expression-built query, used to show SQL is statement-independent."""
    return QueryStatement(
        dialect,
        select=[AllColumnsWildcard(dialect)],
        from_=NamedRelationRef(dialect, Table(dialect, "orders")),
    )


def _render(
    dialect: oracle_dialect.OracleDialect,
    options: Optional[ExplainOptions] = None,
    statement: Optional[QueryStatement] = None,
) -> Tuple[str, tuple]:
    """Render an EXPLAIN expression; returns the dialect's (sql, params)."""
    expr = ExplainStatement(
        dialect,
        statement=statement if statement is not None else _query(dialect),
        options=options,
    )
    return dialect.format_explain_statement(expr)


def _plan_row(
    row_id: int,
    operation: str,
    options: Optional[str] = None,
    object_name: Optional[str] = None,
    parent_id: Optional[int] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """Build one PLAN_TABLE-shaped row with the uppercase keys ``from_rows`` expects."""
    row: Dict[str, Any] = {
        "ID": row_id,
        "OPERATION": operation,
        "OPTIONS": options,
        "OBJECT_NAME": object_name,
        "PARENT_ID": parent_id,
    }
    row.update({key.upper(): value for key, value in extra.items()})
    return row


# --- capability + exact SQL (construction-level, no DB) --------------------


class TestOracleExplainCapabilities:
    """Oracle advertises EXPLAIN PLAN but no ANALYZE / FORMAT / option clauses."""

    def test_supports_explain_plan(self) -> None:
        dialect = _dialect()
        assert isinstance(dialect, dialect_protocols.ExplainSupport)
        assert dialect.supports_explain_plan() is True

    def test_supports_explain_analyze_is_false(self) -> None:
        """Oracle has no EXPLAIN ANALYZE equivalent."""
        assert _dialect().supports_explain_analyze() is False

    @pytest.mark.parametrize(
        "probe",
        [
            "supports_explain_format_text",
            "supports_explain_format_json",
            "supports_explain_format_xml",
            "supports_explain_format_yaml",
            "supports_explain_format_tree",
            "supports_explain_format_traditional",
        ],
    )
    def test_no_explain_format_switch_is_on(self, probe: str) -> None:
        """No FORMAT option is part of Oracle's EXPLAIN PLAN grammar."""
        assert getattr(_dialect(), probe)() is False, probe

    @pytest.mark.parametrize(
        "probe",
        [
            "supports_explain_verbose",
            "supports_explain_costs",
            "supports_explain_buffers",
            "supports_explain_timing",
            "supports_explain_settings",
            "supports_explain_wal",
        ],
    )
    def test_no_explain_option_switch_is_on(self, probe: str) -> None:
        """Oracle's EXPLAIN PLAN statement carries none of these option clauses."""
        assert getattr(_dialect(), probe)() is False, probe

    def test_the_old_format_probe_is_gone(self) -> None:
        """``supports_explain_format`` took a format name; the switches replace it."""
        assert not hasattr(_dialect(), "supports_explain_format")


class TestOracleExplainRendering:
    """Exact fallback SQL for expression-built EXPLAIN statements."""

    @pytest.mark.parametrize(
        "options",
        [None, ExplainOptions()],
        ids=["none", "default"],
    )
    def test_format_explain_statement_emits_exact_fallback(self, options) -> None:
        """An EXPLAIN that asks for no option renders the SELECT-from-DUAL fallback."""
        sql, params = _render(_dialect(), options)
        assert sql == _FALLBACK_SQL, "Oracle EXPLAIN must render the SELECT 1 fallback"
        assert params == (), "the fallback has no parameters"

    @pytest.mark.parametrize(
        "options",
        [
            ExplainOptions(analyze=True),
            ExplainOptions(type=ExplainType.ANALYZE),
            ExplainOptions(format=ExplainFormat.JSON),
            ExplainOptions(format=ExplainFormat.TEXT),
            ExplainOptions(costs=False),
            ExplainOptions(buffers=True),
            ExplainOptions(verbose=True),
            ExplainOptions(settings=True),
            ExplainOptions(wal=True),
        ],
        ids=[
            "analyze",
            "type-analyze",
            "json",
            "text",
            "costs-off",
            "buffers",
            "verbose",
            "settings",
            "wal",
        ],
    )
    def test_format_explain_statement_refuses_unsupported_options(self, options) -> None:
        """An option Oracle's grammar has no clause for is refused, not dropped."""
        with pytest.raises(UnsupportedFeatureError):
            _render(_dialect(), options)

    def test_explain_statement_to_sql_uses_fallback(self) -> None:
        """Expression.to_sql() dispatches through format_explain_statement."""
        dialect = _dialect()
        expr = ExplainStatement(dialect, statement=_query(dialect))
        sql, params = expr.to_sql()
        assert sql == _FALLBACK_SQL
        assert params == ()

    def test_fallback_is_independent_of_wrapped_statement(self) -> None:
        """Two different expression-built queries render the identical fallback."""
        dialect = _dialect()
        assert _render(dialect, statement=_query(dialect)) == _render(
            dialect, statement=_wildcard_query(dialect)
        )


# --- result interpretation, synthetic PLAN_TABLE rows (no DB) --------------


class TestOracleExplainResultInterpretation:
    """Interpret PLAN_TABLE-shaped rows through the production result type."""

    def test_full_scan_is_detected(self) -> None:
        plan = OracleExplainResult.from_rows(
            [
                _plan_row(0, "SELECT STATEMENT"),
                _plan_row(
                    1, "TABLE ACCESS", "FULL", "USERS", parent_id=0, cost=12, cardinality=500
                ),
            ]
        )
        assert plan.has_full_scan()
        scan = plan.find_full_scans()[0]
        assert isinstance(scan, OracleExplainRow)
        assert (scan.operation, scan.options, scan.object_name) == ("TABLE ACCESS", "FULL", "USERS")
        assert plan.get_tables_accessed() == ["USERS"]

    def test_index_access_is_detected(self) -> None:
        plan = OracleExplainResult.from_rows(
            [
                _plan_row(0, "SELECT STATEMENT"),
                _plan_row(1, "TABLE ACCESS", "BY INDEX ROWID", "ORDERS", parent_id=0),
                _plan_row(2, "INDEX", "UNIQUE SCAN", "PK_ORDERS", parent_id=1),
            ]
        )
        assert not plan.has_full_scan()
        assert len(plan.find_index_scans()) == 2
        assert plan.get_indexes_used() == ["PK_ORDERS"]
        assert plan.get_summary()["index_scans"] == 2

    def test_join_and_aggregate_evidence(self) -> None:
        plan = OracleExplainResult.from_rows(
            [
                _plan_row(0, "SELECT STATEMENT"),
                _plan_row(1, "SORT", "AGGREGATE", parent_id=0),
                _plan_row(2, "HASH JOIN", parent_id=1),
                _plan_row(3, "TABLE ACCESS", "FULL", "USERS", parent_id=2),
                _plan_row(4, "TABLE ACCESS", "FULL", "ORDERS", parent_id=2),
            ]
        )
        assert [j.operation for j in plan.find_joins()] == ["HASH JOIN"]
        aggregate = plan.find_operations("SORT")
        assert aggregate and aggregate[0].options == "AGGREGATE"
        assert plan.get_tables_accessed() == ["ORDERS", "USERS"]

    def test_tree_is_rebuilt_from_ids_and_cost_accumulates(self) -> None:
        plan = OracleExplainResult.from_rows(
            [
                _plan_row(0, "SELECT STATEMENT", cost=1),
                _plan_row(1, "TABLE ACCESS", "FULL", "USERS", parent_id=0, cost=10),
            ]
        )
        assert plan.root is not None
        assert plan.root.operation == "SELECT STATEMENT"
        assert [c.operation for c in plan.root.children] == ["TABLE ACCESS"]
        assert plan.get_total_cost() == 11

    def test_partition_columns_are_represented(self) -> None:
        """Partition metadata in PLAN_TABLE maps onto the row type.

        This is NOT a pruning assertion -- it only verifies that the structured
        result preserves the columns a real partitioned plan would report.
        """
        plan = OracleExplainResult.from_rows(
            [
                _plan_row(0, "SELECT STATEMENT"),
                _plan_row(
                    1,
                    "PARTITION RANGE",
                    "SINGLE",
                    parent_id=0,
                    PARTITION_START="1",
                    PARTITION_STOP="1",
                    PARTITION_ID=1,
                ),
            ]
        )
        part = plan.find_operations("PARTITION RANGE")[0]
        assert (part.partition_start, part.partition_stop, part.partition_id) == ("1", "1", 1)
