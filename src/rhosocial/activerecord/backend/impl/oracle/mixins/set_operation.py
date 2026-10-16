# src/rhosocial/activerecord/backend/impl/oracle/mixins/set_operation.py
"""Oracle set-operation formatting mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.query_sources import SetOperation

if TYPE_CHECKING:
    pass

#: The node's operation words mapped to the spelling Oracle needs. Oracle has
#: no ``EXCEPT``; the set difference is spelled ``MINUS``. The other three words
#: render with the standard spelling.
_OP_SPELLINGS = {
    "union": "UNION",
    "union_all": "UNION ALL",
    "intersect": "INTERSECT",
    "except": "MINUS",
}


class OracleSetOperationMixin:
    """Oracle-specific set-operation expression formatting & capability checks.

    The generic ``SetOperationMixin`` renders the operator from the node's
    ``op`` word; Oracle spells the set difference ``MINUS`` where the standard
    says ``EXCEPT``, so this mixin maps the word to Oracle's spelling.
    """

    def supports_union(self) -> bool:
        return True

    def supports_union_all(self) -> bool:
        return True

    def supports_intersect(self) -> bool:
        return True

    def supports_except(self) -> bool:
        return True

    def supports_set_operation_order_by(self) -> bool:
        return True

    def supports_set_operation_limit_offset(self) -> bool:
        return self.version >= (12, 0, 0)

    def supports_set_operation_for_update(self) -> bool:
        return False

    def format_set_operation(self, expr: "SetOperation") -> Tuple[str, tuple]:
        """Format a set operation, spelling the set difference as ``MINUS``.

        The node carries the operation as a word (``union`` / ``union_all`` /
        ``intersect`` / ``except``); the wording is Oracle's, not the node's, so
        the node stays language-neutral and every backend maps the same word to
        its own spelling.

        Args:
            expr: The node exposing ``op`` (a word form), ``left`` and
                ``right``.

        Returns:
            Tuple of (SQL string, parameters tuple), carrying the operands'
            parameters in order.
        """
        left_sql, left_params = expr.left.to_sql()
        right_sql, right_params = expr.right.to_sql()
        base_sql = f"{left_sql} {_OP_SPELLINGS[expr.op]} {right_sql}"
        return base_sql, tuple(left_params + right_params)