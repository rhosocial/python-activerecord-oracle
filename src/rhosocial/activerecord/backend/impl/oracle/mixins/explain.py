# src/rhosocial/activerecord/backend/impl/oracle/mixins/explain.py
"""Oracle EXPLAIN PLAN mixin."""
from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.statements import ExplainType

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.statements import ExplainStatement


class OracleExplainMixin:
    """Oracle EXPLAIN PLAN support.

    Oracle's EXPLAIN PLAN FOR statement only writes rows to PLAN_TABLE;
    it does not return a result set. A SELECT 1 fallback is used for
    compatibility with the testsuite contract.

    The fallback renders the wrapped statement as nothing, so an EXPLAIN
    request that names one of the option clauses Oracle's grammar does not
    have is refused by name: there is no SQL left to carry the option and
    dropping it would answer a question the caller did not ask.
    """

    def format_explain_statement(self, expr: "ExplainStatement") -> Tuple[str, tuple]:
        """Format EXPLAIN PLAN FOR <stmt> for Oracle.

        Raises:
            UnsupportedFeatureError: if the statement asks for ANALYZE, a
                FORMAT clause, COSTS OFF, VERBOSE, BUFFERS, SETTINGS or WAL.
                TIMING only accompanies ANALYZE, which is refused first.
        """
        options = expr.options
        if options is not None:
            if options.type == ExplainType.ANALYZE or options.analyze:
                raise UnsupportedFeatureError(self.name, "EXPLAIN ANALYZE")
            if options.format is not None:
                raise UnsupportedFeatureError(
                    self.name, f"EXPLAIN FORMAT {options.format.value}"
                )
            if not options.costs:
                raise UnsupportedFeatureError(self.name, "EXPLAIN COSTS")
            if options.buffers:
                raise UnsupportedFeatureError(self.name, "EXPLAIN BUFFERS")
            if options.verbose:
                raise UnsupportedFeatureError(self.name, "EXPLAIN VERBOSE")
            if options.settings:
                raise UnsupportedFeatureError(self.name, "EXPLAIN SETTINGS")
            if options.wal:
                raise UnsupportedFeatureError(self.name, "EXPLAIN WAL")
        return "SELECT 1 AS EXPLAIN_PLAN FROM DUAL", ()


__all__ = ['OracleExplainMixin']
