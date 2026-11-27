# src/rhosocial/activerecord/backend/impl/oracle/mixins/hierarchical.py
"""Oracle hierarchical query support mixin."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.hierarchical import (
        OracleConnectByExpression,
        OracleConnectByIsCycleExpression,
        OracleConnectByIsLeafExpression,
        OracleConnectByRootExpression,
        OracleLevelExpression,
        OraclePriorExpression,
        OracleSiblingsExpression,
        OracleSysConnectByPathExpression,
    )


class OracleHierarchicalMixin:
    """Oracle CONNECT BY / hierarchical query checks and formatters."""

    def supports_hierarchical_queries(self) -> bool:
        return True

    def supports_connect_by(self) -> bool:
        return True

    def supports_level_pseudo_column(self) -> bool:
        return True

    def supports_connect_by_root(self) -> bool:
        return True

    def supports_sys_connect_by_path(self) -> bool:
        return True

    def format_connect_by_expression(
        self, expr: "OracleConnectByExpression"
    ) -> Tuple[str, tuple]:
        """Render ``[START WITH cond] CONNECT BY [NOCYCLE] cond``.

        The two conditions are rendered by their own nodes, so their bind
        parameters are collected in the order the clauses appear.
        """
        parts = []
        params = []

        if expr.start_with:
            start_sql, start_params = expr.start_with.to_sql()
            parts.append(f"START WITH {start_sql}")
            params.extend(start_params)

        nocycle_str = "NOCYCLE " if expr.nocycle else ""
        if expr.condition:
            cond_sql, cond_params = expr.condition.to_sql()
            parts.append(f"CONNECT BY {nocycle_str}{cond_sql}")
            params.extend(cond_params)

        return (" ".join(parts), tuple(params))

    def format_prior_expression(self, expr: "OraclePriorExpression") -> Tuple[str, tuple]:
        """Render the ``PRIOR`` operator applied to a column."""
        return (f"PRIOR {self.format_identifier(expr.column)}", ())

    def format_connect_by_root_expression(
        self, expr: "OracleConnectByRootExpression"
    ) -> Tuple[str, tuple]:
        """Render the ``CONNECT_BY_ROOT`` operator applied to a column."""
        return (f"CONNECT_BY_ROOT {self.format_identifier(expr.column)}", ())

    def format_sys_connect_by_path_expression(
        self, expr: "OracleSysConnectByPathExpression"
    ) -> Tuple[str, tuple]:
        """Render ``SYS_CONNECT_BY_PATH(column, separator)``.

        The separator is text inside the single-quoted argument, exactly as
        Oracle's grammar spells it; it is not a bind parameter.
        """
        return (
            f"SYS_CONNECT_BY_PATH({self.format_identifier(expr.column)}, '{expr.separator}')",
            (),
        )

    def format_connect_by_is_leaf_expression(
        self, expr: "OracleConnectByIsLeafExpression"
    ) -> Tuple[str, tuple]:
        """Render the ``CONNECT_BY_ISLEAF`` pseudo-column."""
        return ("CONNECT_BY_ISLEAF", ())

    def format_connect_by_is_cycle_expression(
        self, expr: "OracleConnectByIsCycleExpression"
    ) -> Tuple[str, tuple]:
        """Render the ``CONNECT_BY_ISCYCLE`` pseudo-column."""
        return ("CONNECT_BY_ISCYCLE", ())

    def format_level_expression(self, expr: "OracleLevelExpression") -> Tuple[str, tuple]:
        """Render the ``LEVEL`` pseudo-column."""
        return ("LEVEL", ())

    def format_siblings_expression(
        self, expr: "OracleSiblingsExpression"
    ) -> Tuple[str, tuple]:
        """Render ``ORDER SIBLINGS BY col [ASC|DESC], ...``.

        An expression that names no columns renders nothing, and the caller
        is what keeps that empty string out of the statement.
        """
        if not expr.columns:
            return ("", ())

        order_parts = []
        for i, col in enumerate(expr.columns):
            asc = expr.ascending[i] if i < len(expr.ascending) else True
            order_parts.append(
                f"{self.format_identifier(col)} {'ASC' if asc else 'DESC'}"
            )

        return (f"ORDER SIBLINGS BY {', '.join(order_parts)}", ())
