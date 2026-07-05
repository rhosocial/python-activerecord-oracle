# src/rhosocial/activerecord/backend/impl/oracle/mixins/expression.py
"""Oracle expression-formatting delegation mixin.

Routes Oracle-specific expression formatters (connect_by, pivot,
unpivot, hint, for_update) back through the expression's own
``to_sql(self)`` dispatch, which is the standard pattern used by
the core ``ExpressionMixin``.
"""

from typing import List, Tuple


class OracleExpressionMixin:
    """Oracle-specific expression formatting that delegates to ``to_sql``."""

    def format_connect_by(self, expr) -> Tuple[str, List]:
        return expr.to_sql(self)

    def format_pivot(self, expr) -> Tuple[str, List]:
        return expr.to_sql(self)

    def format_unpivot(self, expr) -> Tuple[str, List]:
        return expr.to_sql(self)

    def format_hint(self, expr) -> Tuple[str, List]:
        return expr.to_sql(self)

    def format_for_update(self, expr) -> Tuple[str, List]:
        return expr.to_sql(self)