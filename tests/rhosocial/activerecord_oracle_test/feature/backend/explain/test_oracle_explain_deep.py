# tests/rhosocial/activerecord_oracle_test/feature/backend/explain/test_oracle_explain_deep.py
"""Backend-local, deep EXPLAIN tests for Oracle.

The shared testsuite contracts only assert that EXPLAIN *can run*. This file
interprets what Oracle actually returns, which is deliberately NOT generalized
into the testsuite because dialects disagree on plan shape:

* Oracle's ``EXPLAIN PLAN FOR`` writes rows into ``PLAN_TABLE`` (the same table
  ``DBMS_XPLAN`` reads). Each row exposes ``OPERATION`` / ``OPTIONS`` /
  ``OBJECT_NAME`` / ``ID`` / ``PARENT_ID``; the tree is rebuilt from the ids.
* This backend cannot return that table as a client result set, so
  ``format_explain_statement`` renders a tiny ``SELECT 1 AS EXPLAIN_PLAN FROM
  DUAL`` fallback. ANALYZE and FORMAT are advertised as unsupported and never
  change the emitted SQL -- verified here at construction level (no DB needed).
* ``OracleExplainResult.from_rows`` maps raw PLAN_TABLE rows into the structured
  tree used for interpretation. Scan / index / join / aggregate evidence is
  asserted both on synthetic rows (always runs) and on live rows (CI / local DB).

Partition pruning is intentionally NOT asserted: Oracle does support
declarative partitioning, but a stable "pruned partition" string in PLAN_TABLE
is version- and statistics-dependent, so a fake assertion would be worse than
none. The existing ``execute/partition`` test already covers EXPLAIN acceptance
on a partitioned table; this file only checks that partition metadata columns
are *represented* by the result type, not that pruning happened.
"""

from typing import Any, Dict, List, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.dialect import protocols as dialect_protocols
from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.expression import (
    Column,
    ExplainFormat,
    ExplainOptions,
    ExplainType,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.statements import ExplainStatement
from rhosocial.activerecord.backend.impl.oracle import dialect as oracle_dialect
from rhosocial.activerecord.backend.impl.oracle.explain import (
    OracleExplainResult,
    OracleExplainRow,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# --- helpers ---------------------------------------------------------------

_FALLBACK_SQL = "SELECT 1 AS EXPLAIN_PLAN FROM DUAL"


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


def _triples(rows: List[Dict[str, Any]]) -> List[Tuple[Any, Any, Any]]:
    """Extract (OPERATION, OPTIONS, OBJECT_NAME) from raw PLAN_TABLE rows."""
    return [
        (
            r.get("operation") or r.get("OPERATION"),
            r.get("options") or r.get("OPTIONS"),
            r.get("object_name") or r.get("OBJECT_NAME"),
        )
        for r in rows
    ]


# --- capability + exact SQL (construction-level, no DB) --------------------


class TestOracleExplainCapabilities:
    """Oracle advertises EXPLAIN PLAN but no ANALYZE / FORMAT options."""

    @pytest.fixture
    def dialect(self) -> oracle_dialect.OracleDialect:
        return oracle_dialect.OracleDialect(version=(23, 0, 0))

    def test_supports_explain_plan(self, dialect) -> None:
        assert isinstance(dialect, dialect_protocols.ExplainSupport)
        assert dialect.supports_explain_plan() is True

    def test_supports_explain_analyze_is_false(self, dialect) -> None:
        """Oracle has no EXPLAIN ANALYZE equivalent."""
        assert dialect.supports_explain_analyze() is False

    @pytest.mark.parametrize(
        "format_type",
        [f.value for f in ExplainFormat] + ["DOT", "BASIC", "TREE"],
    )
    def test_supports_explain_format_is_false_for_every_format(self, dialect, format_type) -> None:
        """No FORMAT option is part of Oracle's EXPLAIN PLAN grammar."""
        assert dialect.supports_explain_format(format_type) is False

    @pytest.mark.parametrize(
        "options",
        [
            None,
            ExplainOptions(),
            ExplainOptions(analyze=True),
            ExplainOptions(format=ExplainFormat.JSON),
            ExplainOptions(type=ExplainType.ANALYZE, verbose=True, buffers=True, timing=True),
        ],
        ids=["none", "default", "analyze", "json", "analyze-verbose"],
    )
    def test_format_explain_statement_emits_exact_fallback(self, dialect, options) -> None:
        """Every option combination renders the same SELECT-from-DUAL fallback."""
        statement = QueryStatement(dialect, select=[Column(dialect, "1")])
        expr = ExplainStatement(dialect, statement=statement, options=options)
        sql, params = dialect.format_explain_statement(expr)
        assert sql == _FALLBACK_SQL, "Oracle EXPLAIN must render the SELECT 1 fallback"
        assert params == ()

    def test_explain_statement_to_sql_uses_fallback(self, dialect) -> None:
        """Expression.to_sql() dispatches through format_explain_statement."""
        statement = QueryStatement(dialect, select=[Column(dialect, "1")])
        expr = ExplainStatement(dialect, statement=statement)
        sql, params = expr.to_sql()
        assert sql == _FALLBACK_SQL
        assert params == ()


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


# --- live PLAN_TABLE interpretation (CI / local DB) ------------------------


@pytest.mark.backend
@pytest.mark.xdist_group("oracle_explain_deep")
class TestOracleExplainPlanTableLive:
    """Interpret real PLAN_TABLE rows from a live Oracle server.

    Uses a directory-owned schema (``test_backend_explain``) and a unique
    STATEMENT_ID so the GTT stays session-local and leaves no residue. One tiny
    indexed table is created and dropped within a single test, keeping the
    footprint small for contention-sensitive Oracle CI.
    """

    _TABLE = "AR_EXPLAIN_DEEP_T"
    _STATEMENT_ID = "AR_EXPLAIN_DEEP"

    @pytest.fixture
    def plan_env(self, oracle_backend_single):
        backend = oracle_backend_single
        if not backend.dialect.supports_explain_plan():
            pytest.skip("Oracle dialect does not advertise EXPLAIN PLAN support")

        table = self._TABLE
        sid = self._STATEMENT_ID
        try:
            backend.execute(f"DROP TABLE {table}", ())
        except DatabaseError as exc:
            if "ORA-00942" not in str(exc):
                raise
        backend.execute(f"CREATE TABLE {table} (ID NUMBER PRIMARY KEY, V VARCHAR2(20))", ())
        backend.execute(f"INSERT INTO {table} VALUES (1, 'x')", ())
        try:
            yield backend
        finally:
            backend.execute(f"DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '{sid}'", ())
            backend.execute(f"DROP TABLE {table}", ())

    def _explain(self, backend, sql: str) -> List[Dict[str, Any]]:
        sid = self._STATEMENT_ID
        backend.execute(f"EXPLAIN PLAN SET STATEMENT_ID = '{sid}' FOR {sql}", ())
        rows = (
            backend.execute(
                "SELECT ID, PARENT_ID, OPERATION, OPTIONS, OBJECT_NAME, COST, CARDINALITY "
                f"FROM PLAN_TABLE WHERE STATEMENT_ID = '{sid}' ORDER BY ID",
                (),
                options=ExecutionOptions(stmt_type=StatementType.DQL),
            ).data
            or []
        )
        backend.execute(f"DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '{sid}'", ())
        return list(rows)

    def test_live_plan_table_scan_index_and_aggregate(self, plan_env) -> None:
        backend = plan_env
        table = self._TABLE

        agg = _triples(self._explain(backend, "SELECT COUNT(*) FROM DUAL"))
        assert any(
            op == "SORT" and opts == "AGGREGATE" for op, opts, _ in agg
        ), "COUNT(*) must plan as a SORT AGGREGATE"

        indexed = _triples(self._explain(backend, f"SELECT * FROM {table} WHERE ID = 1"))
        assert any(
            op == "INDEX" and opts and "SCAN" in opts for op, opts, _ in indexed
        ), "a primary-key lookup must plan as an INDEX ... SCAN"
        assert any(
            op == "TABLE ACCESS" and opts == "BY INDEX ROWID" for op, opts, _ in indexed
        ), "the PK lookup must reach the table by index rowid"

        scanned = _triples(self._explain(backend, f"SELECT * FROM {table} WHERE V = 'x'"))
        assert (
            "TABLE ACCESS",
            "FULL",
            table,
        ) in scanned, "a non-indexed predicate must plan as a full TABLE ACCESS"

    def test_live_plan_rows_interpreted_by_result_type(self, plan_env) -> None:
        """Live raw rows (lowercase keys) map through the structured result type."""
        backend = plan_env
        raw = self._explain(backend, f"SELECT * FROM {self._TABLE} WHERE ID = 1")
        assert raw, "EXPLAIN PLAN must write rows into PLAN_TABLE"

        upper = [{key.upper(): value for key, value in row.items()} for row in raw]
        plan = OracleExplainResult.from_rows(upper, statement_id=self._STATEMENT_ID)
        assert plan.rows
        assert plan.find_index_scans(), "live PK plan must expose index-scan evidence"
        assert plan.get_summary()["index_scans"] >= 1
