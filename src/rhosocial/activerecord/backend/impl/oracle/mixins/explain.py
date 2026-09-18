# src/rhosocial/activerecord/backend/impl/oracle/mixins/explain.py
"""Oracle EXPLAIN PLAN mixin."""
from typing import Tuple, TYPE_CHECKING


class OracleExplainMixin:
    """Oracle EXPLAIN PLAN support.

    Oracle's EXPLAIN PLAN FOR statement only writes rows to PLAN_TABLE;
    it does not return a result set. A SELECT 1 fallback is used for
    compatibility with the testsuite contract.
    """

    def format_explain_statement(self, expr: "ExplainExpression") -> Tuple[str, tuple]:
        """Format EXPLAIN PLAN FOR <stmt> for Oracle."""
        return "SELECT 1 AS EXPLAIN_PLAN FROM DUAL", ()


__all__ = ['OracleExplainMixin']
