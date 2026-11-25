# src/rhosocial/activerecord/backend/impl/oracle/mixins/hint.py
"""Oracle query hint support mixin."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.hint import OracleHintExpression


class OracleHintMixin:
    """Oracle optimizer hint capability checks and formatter."""

    def supports_query_hints(self) -> bool:
        return True

    def supports_parallel_hint(self) -> bool:
        return True

    def supports_index_hint(self) -> bool:
        return True

    def supports_leading_hint(self) -> bool:
        return True

    def supports_optimizer_hints(self) -> bool:
        return True

    def format_oracle_hint_expression(
        self, expr: "OracleHintExpression"
    ) -> Tuple[str, tuple]:
        """Render the hints as an optimizer comment after the keyword.

        A node with no hints renders nothing, so it can be added
        unconditionally; the hint text itself is caller-authored SQL.
        """
        if not expr.hints:
            return ("", ())
        hints_str = " ".join(expr.hints)
        return (f"/*+ {hints_str} */", ())
