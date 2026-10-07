# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_master_probe_gates.py
"""Guard: Oracle declares the four master probes core now consults.

Core's c4d6adc gave call sites to three previously-decorative master probes and
added a fourth (``supports_transaction_wait``).  A dialect that does not answer
them starts refusing what it used to render -- or, worse, keeps rendering SQL
the server rejects.  Each probe's answer here is backed by live-server
measurement on 18c/21c/23c (the raw transcript lives in the round's plan
directory):

* ``supports_materialized_cte`` -- False.  Oracle spells CTE materialization as
  the ``/*+ MATERIALIZE */`` hint; ``AS MATERIALIZED`` / ``AS NOT MATERIALIZED``
  are rejected (ORA-00906 on all three versions).  The pair is refused by name.
* ``supports_truncate`` -- True.  ``TRUNCATE TABLE`` is accepted on all three.
* ``supports_with_data_clause`` -- False.  ``WITH [NO] DATA`` is rejected on
  CTAS and on CREATE MATERIALIZED VIEW (ORA-00933 on 18c/21c, ORA-03048 on
  23c); the only population spellings are ``BUILD IMMEDIATE`` / ``BUILD
  DEFERRED``, which the Oracle materialized-view formatter emits.
* ``supports_transaction_wait`` -- False.  ``SET TRANSACTION READ WRITE``
  accepts neither ``WAIT`` nor ``NOWAIT`` nor ``NO WAIT`` (ORA-00933 on
  18c/21c, ORA-03049/ORA-03048 on 23c).  The pair is refused by name.

The guard was run against the unmodified backend first (its RED output is
recorded in the round's plan directory); it is green after the change.
"""

from __future__ import annotations

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.protocols import TransactionControlSupport
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.objects import (
    MaterializedView,
    Table,
)
from rhosocial.activerecord.backend.expression.query_sources import CTEExpression
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    CreateTableAsExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_truncate import (
    TruncateExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_view import (
    CreateMaterializedViewExpression,
    RefreshMaterializedViewExpression,
)
from rhosocial.activerecord.backend.expression.statements.dql import QueryExpression
from rhosocial.activerecord.backend.expression.transaction import (
    BeginTransactionExpression,
    SetTransactionExpression,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


def _dialect() -> OracleDialect:
    return OracleDialect(version=(19, 0, 0))


def _query(dialect: OracleDialect) -> QueryExpression:
    return QueryExpression(dialect, select=[Literal(dialect, 1)])


def _refuse(expr, expected_feature: str) -> UnsupportedFeatureError:
    """Render ``expr`` and return the named refusal it must raise."""
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        expr.to_sql()
    exc = excinfo.value
    assert exc.dialect_name == "Oracle", (
        f"the refusal must be Oracle's own, got dialect_name={exc.dialect_name!r}"
    )
    assert exc.feature_name == expected_feature, (
        f"expected feature {expected_feature!r}, got {exc.feature_name!r}: {exc}"
    )
    return exc


class TestMasterProbesAreDeclared:
    """Every master probe answers a real bool -- never the protocol stub."""

    @pytest.mark.parametrize("version", [(18, 0, 0), (19, 0, 0), (21, 0, 0), (23, 0, 0)])
    def test_materialized_cte_is_declined(self, version):
        d = OracleDialect(version=version)
        assert d.supports_materialized_cte() is False

    @pytest.mark.parametrize("version", [(18, 0, 0), (19, 0, 0), (21, 0, 0), (23, 0, 0)])
    def test_truncate_is_declared(self, version):
        d = OracleDialect(version=version)
        assert d.supports_truncate() is True

    @pytest.mark.parametrize("version", [(18, 0, 0), (19, 0, 0), (21, 0, 0), (23, 0, 0)])
    def test_with_data_clause_is_declined(self, version):
        d = OracleDialect(version=version)
        assert d.supports_with_data_clause() is False

    @pytest.mark.parametrize("version", [(18, 0, 0), (19, 0, 0), (21, 0, 0), (23, 0, 0)])
    def test_transaction_wait_is_declined(self, version):
        d = OracleDialect(version=version)
        assert d.supports_transaction_wait() is False

    def test_every_answer_is_a_bool(self):
        """``isinstance(x, bool)`` catches the protocol stub's ``None``."""
        d = _dialect()
        for probe in (
            "supports_materialized_cte",
            "supports_truncate",
            "supports_with_data_clause",
            "supports_transaction_wait",
        ):
            value = getattr(d, probe)()
            assert isinstance(value, bool), (
                f"{probe}() answered {value!r} ({type(value).__name__}); a probe "
                f"must be a concrete bool, not the protocol stub's None"
            )

    def test_transaction_wait_is_not_the_protocol_stub(self):
        """Oracle must declare the probe, not fall through to the protocol."""
        assert (
            type(_dialect()).supports_transaction_wait
            is not TransactionControlSupport.supports_transaction_wait
        )


class TestMaterializedCTEGate:
    """``AS [NOT] MATERIALIZED`` is refused by name on Oracle."""

    def test_materialized_is_refused_by_name(self):
        d = _dialect()
        _refuse(CTEExpression(d, "c", _query(d), materialized=True), "MATERIALIZED CTE")

    def test_not_materialized_is_refused_by_name(self):
        d = _dialect()
        _refuse(
            CTEExpression(d, "c", _query(d), not_materialized=True),
            "NOT MATERIALIZED CTE",
        )

    def test_unspecified_renders_no_hint(self):
        d = _dialect()
        sql, _params = CTEExpression(d, "c", _query(d)).to_sql()
        assert "MATERIALIZED" not in sql, sql

    def test_probe_is_what_refuses(self):
        """Flip the probe and the same expression renders (a mutation check)."""

        class _Materialized(OracleDialect):
            def supports_materialized_cte(self) -> bool:
                return True

        d = _Materialized(version=(19, 0, 0))
        sql, _params = CTEExpression(d, "c", _query(d), not_materialized=True).to_sql()
        assert "AS NOT MATERIALIZED" in sql, sql


class TestWithDataGate:
    """The SQL-standard ``WITH [NO] DATA`` clause has no Oracle spelling."""

    def test_ctas_with_data_is_refused_by_name(self):
        d = _dialect()
        _refuse(
            CreateTableAsExpression(d, Table(d, "t"), _query(d), with_data=True),
            "WITH DATA",
        )

    def test_ctas_no_data_is_refused_by_name(self):
        """The finding this round closes: no_data must not render WITH NO DATA."""
        d = _dialect()
        _refuse(
            CreateTableAsExpression(d, Table(d, "t"), _query(d), no_data=True),
            "WITH NO DATA",
        )

    def test_ctas_unspecified_renders_plain(self):
        d = _dialect()
        sql, _params = CreateTableAsExpression(d, Table(d, "t"), _query(d)).to_sql()
        assert "WITH" not in sql.upper().replace("CREATE", ""), sql

    def test_mv_create_spells_build_instead(self):
        """The dialect hook: with_data/no_data become BUILD IMMEDIATE/DEFERRED."""
        d = _dialect()
        immediate, _ = CreateMaterializedViewExpression(
            d, MaterializedView(d, "mv"), _query(d), with_data=True
        ).to_sql()
        assert "BUILD IMMEDIATE" in immediate, immediate
        assert "WITH DATA" not in immediate, immediate
        deferred, _ = CreateMaterializedViewExpression(
            d, MaterializedView(d, "mv"), _query(d), no_data=True
        ).to_sql()
        assert "BUILD DEFERRED" in deferred, deferred
        assert "WITH NO DATA" not in deferred, deferred

    def test_mv_create_unspecified_emits_no_build(self):
        d = _dialect()
        sql, _params = CreateMaterializedViewExpression(
            d, MaterializedView(d, "mv"), _query(d)
        ).to_sql()
        assert "BUILD" not in sql, sql

    def test_mv_refresh_with_data_is_refused_by_name(self):
        d = _dialect()
        _refuse(
            RefreshMaterializedViewExpression(d, MaterializedView(d, "mv"), with_data=True),
            "REFRESH MATERIALIZED VIEW WITH DATA",
        )

    def test_mv_refresh_no_data_is_refused_by_name(self):
        d = _dialect()
        _refuse(
            RefreshMaterializedViewExpression(d, MaterializedView(d, "mv"), no_data=True),
            "REFRESH MATERIALIZED VIEW WITH NO DATA",
        )


class TestTruncateGate:
    """The declared ``supports_truncate`` answer is load-bearing."""

    def test_plain_truncate_renders(self):
        d = _dialect()
        sql, _params = TruncateExpression(d, Table(d, "t")).to_sql()
        assert sql == 'TRUNCATE TABLE "T"', sql

    def test_a_dialect_that_declines_truncate_is_refused(self):
        """Flip the probe and the formatter refuses by name, never renders."""

        class _NoTruncate(OracleDialect):
            def supports_truncate(self) -> bool:
                return False

        d = _NoTruncate(version=(19, 0, 0))
        assert d.supports_truncate() is False
        with pytest.raises(UnsupportedFeatureError) as excinfo:
            TruncateExpression(d, Table(d, "t")).to_sql()
        # The flipped subclass names itself in ``name``; what this asserts is
        # that the declared probe is the gate the formatter consults.
        assert excinfo.value.feature_name == "TRUNCATE"
        assert excinfo.value.dialect_name == "_NoTruncate"


class TestTransactionWaitGate:
    """Oracle's BEGIN/SET TRANSACTION have no WAIT / NO WAIT spelling.

    Measured on 18c/21c/23c: ``SET TRANSACTION READ WRITE WAIT``,
    ``... NOWAIT`` and ``... NO WAIT`` are all rejected.  Neither the clause nor
    the spelling exists, so both sides are refused by name rather than dropped.
    """

    def test_begin_wait_is_refused_by_name(self):
        d = _dialect()
        _refuse(BeginTransactionExpression(d, wait=True), "BEGIN WAIT")

    def test_begin_no_wait_is_refused_by_name(self):
        d = _dialect()
        _refuse(BeginTransactionExpression(d, no_wait=True), "BEGIN NO WAIT")

    def test_set_transaction_wait_is_refused_by_name(self):
        d = _dialect()
        _refuse(SetTransactionExpression(d, wait=True), "SET TRANSACTION WAIT")

    def test_set_transaction_no_wait_is_refused_by_name(self):
        d = _dialect()
        _refuse(SetTransactionExpression(d, no_wait=True), "SET TRANSACTION NO WAIT")

    def test_unspecified_still_renders(self):
        d = _dialect()
        sql, _params = SetTransactionExpression(d).to_sql()
        assert sql == "SET TRANSACTION", sql
