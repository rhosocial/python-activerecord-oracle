# src/rhosocial/activerecord/backend/impl/oracle/mixins/set_operation.py
"""Oracle set-operation formatting mixin."""

from typing import List, Optional, Tuple


class OracleSetOperationMixin:
    """Oracle-specific set-operation expression formatting & capability checks.

    The generic ``SetOperationMixin`` renders ``EXCEPT``; Oracle uses
    ``MINUS`` instead, so this mixin translates before delegating to
    the parent.
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

    def format_set_operation_expression(
        self,
        left,
        right,
        operation: str,
        alias: Optional[str],
        all_: bool,
        order_by_clause=None,
        limit_offset_clause=None,
        for_update_clause=None,
    ) -> Tuple[str, Tuple]:
        if operation.upper() == "EXCEPT":
            operation = "MINUS"
        return super().format_set_operation_expression(
            left,
            right,
            operation,
            alias,
            all_,
            order_by_clause,
            limit_offset_clause,
            for_update_clause,
        )