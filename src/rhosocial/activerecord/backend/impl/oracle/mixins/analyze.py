# src/rhosocial/activerecord/backend/impl/oracle/mixins/analyze.py
"""Oracle ANALYZE TABLE formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

from ..expression.analyze import OracleAnalyzeMode

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.analyze import OracleAnalyzeExpression


class OracleAnalyzeMixin:
    """Oracle ``ANALYZE`` capability check and formatter.

    The ``ANALYZE`` statement has existed since early Oracle releases; the
    formatter gates on ``(9, 0, 0)`` per the backend implementation
    contract.
    """

    def supports_analyze(self) -> bool:
        return True

    def format_analyze_statement(
        self, expr: "OracleAnalyzeExpression"
    ) -> Tuple[str, tuple]:
        """Format ``ANALYZE TABLE`` for Oracle.

        Raises:
            TypeError: ``expr.table`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Table`.
                An index would render its own name, so the statistics would be
                gathered on it while the statement named a table.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        from rhosocial.activerecord.backend.expression.objects import Table

        if not isinstance(expr.table, Table):
            raise TypeError(
                f"OracleAnalyzeExpression.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )

        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "ANALYZE TABLE",
                suggestion=(
                    f"Oracle {self.version} does not support ANALYZE; "
                    "it requires Oracle 9i or later."
                ),
            )
        table_sql = expr.table.to_sql()[0]
        parts = [f"ANALYZE TABLE {table_sql}"]
        if (
            expr.mode is OracleAnalyzeMode.ESTIMATE_STATISTICS
            and expr.sample_percent is not None
        ):
            parts.append(f"{expr.mode.value} SAMPLE {expr.sample_percent} PERCENT")
        elif expr.mode is OracleAnalyzeMode.VALIDATE_STRUCTURE and expr.cascade:
            parts.append(f"{expr.mode.value} CASCADE")
        else:
            parts.append(expr.mode.value)
        if expr.into is not None:
            parts.append(f"INTO {self.format_identifier(expr.into)}")
        return " ".join(parts), ()
