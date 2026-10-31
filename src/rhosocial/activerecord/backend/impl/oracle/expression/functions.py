# src/rhosocial/activerecord/backend/impl/oracle/expression/functions.py
"""Oracle's derived function-call nodes (expression-dialect rules §5).

The generic :class:`~rhosocial.activerecord.backend.expression.core.FunctionCall`
is reserved for user-defined functions, and the clauses these nodes carry --
``WITHIN GROUP``, ``ON OVERFLOW``, ``RETURNING``, ``COLUMNS`` -- are Oracle
grammar the core call contract has no slot for. Each used to travel as a
private attribute hung on a ``FunctionCall`` and reflected back by the
formatter, which is the hidden-state anti-pattern the rules forbid twice over:
the state is invisible to every other reader, and the host it rode on is not
this backend's own node.

Each class here is a backend derived expression in the rules' sense: it keeps
**all** of the core call's parameters (the argument list, ``DISTINCT``, the
alias, the niladic marker, the ``FILTER`` predicate, the window specification)
and adds only the fields Oracle's grammar needs, under their public names. The
function's own name is not a parameter -- the class is the function, so the
kind says which one it is and the constructor injects the name.

The factories in ``impl.oracle.functions`` build these nodes; the renderers
live in ``mixins/functions.py`` under one ``format_oracle_*`` entry point per
kind.
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Union

from rhosocial.activerecord.backend.expression.core import FunctionCall

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.advanced_functions import (
        WindowSpecification,
    )
    from rhosocial.activerecord.backend.expression.bases import (
        BaseExpression,
        SQLPredicate,
    )
    from rhosocial.activerecord.backend.expression.types._base import DataType
    from ..dialect import OracleDialect


class OracleListaggFunction(FunctionCall):
    """``LISTAGG(expr [, delimiter] [ON OVERFLOW ...]) [WITHIN GROUP (...)]``.

    Oracle's string-aggregation aggregate. Two clauses the core call does not
    know belong to it, and they go where Oracle's grammar puts them:
    ``ON OVERFLOW`` inside the argument list (after the delimiter; anywhere
    after ``WITHIN GROUP`` is ORA-00923), and the ``WITHIN GROUP (ORDER BY
    ...)`` sort after the argument list.

    Args:
        dialect: the Oracle dialect instance.
        *args: the expression aggregated, then the delimiter literal.
        within_group_order_by: the ordering inside ``WITHIN GROUP (ORDER BY
            ...)``; ``None`` omits the clause.
        on_overflow: the overflow action Oracle spells after ``ON OVERFLOW``
            (``"TRUNCATE"`` / ``"ERROR"``); ``None`` omits the clause.

    The remaining keyword arguments are the core call's own, unchanged.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        *args: "BaseExpression",
        is_distinct: bool = False,
        alias: Optional[str] = None,
        niladic: bool = False,
        filter_predicate: Optional["SQLPredicate"] = None,
        window_spec: Optional[Union["WindowSpecification", str]] = None,
        is_aggregate: bool = False,
        within_group_order_by: Optional[str] = None,
        on_overflow: Optional[str] = None,
    ):
        super().__init__(
            dialect,
            "LISTAGG",
            *args,
            is_distinct=is_distinct,
            alias=alias,
            niladic=niladic,
            filter_predicate=filter_predicate,
            window_spec=window_spec,
            is_aggregate=is_aggregate,
        )
        self.within_group_order_by = within_group_order_by
        self.on_overflow = on_overflow

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_oracle_listagg_function"


class OraclePercentileContFunction(FunctionCall):
    """``PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY ...)``.

    An ordered-set aggregate: the fraction is the argument, the ordering is
    the clause, and the two never swap places in Oracle's grammar.

    Args:
        dialect: the Oracle dialect instance.
        *args: the percentile fraction literal.
        within_group_order_by: the ordering inside ``WITHIN GROUP (ORDER BY
            ...)``; ``None`` omits the clause.

    The remaining keyword arguments are the core call's own, unchanged.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        *args: "BaseExpression",
        is_distinct: bool = False,
        alias: Optional[str] = None,
        niladic: bool = False,
        filter_predicate: Optional["SQLPredicate"] = None,
        window_spec: Optional[Union["WindowSpecification", str]] = None,
        is_aggregate: bool = False,
        within_group_order_by: Optional[str] = None,
    ):
        super().__init__(
            dialect,
            "PERCENTILE_CONT",
            *args,
            is_distinct=is_distinct,
            alias=alias,
            niladic=niladic,
            filter_predicate=filter_predicate,
            window_spec=window_spec,
            is_aggregate=is_aggregate,
        )
        self.within_group_order_by = within_group_order_by

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_oracle_percentile_cont_function"


class OraclePercentileDiscFunction(FunctionCall):
    """``PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY ...)``.

    The discrete-value mirror of :class:`OraclePercentileContFunction`: the
    same ordered-set shape, a different reduction.

    Args:
        dialect: the Oracle dialect instance.
        *args: the percentile fraction literal.
        within_group_order_by: the ordering inside ``WITHIN GROUP (ORDER BY
            ...)``; ``None`` omits the clause.

    The remaining keyword arguments are the core call's own, unchanged.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        *args: "BaseExpression",
        is_distinct: bool = False,
        alias: Optional[str] = None,
        niladic: bool = False,
        filter_predicate: Optional["SQLPredicate"] = None,
        window_spec: Optional[Union["WindowSpecification", str]] = None,
        is_aggregate: bool = False,
        within_group_order_by: Optional[str] = None,
    ):
        super().__init__(
            dialect,
            "PERCENTILE_DISC",
            *args,
            is_distinct=is_distinct,
            alias=alias,
            niladic=niladic,
            filter_predicate=filter_predicate,
            window_spec=window_spec,
            is_aggregate=is_aggregate,
        )
        self.within_group_order_by = within_group_order_by

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_oracle_percentile_disc_function"


class OracleJsonValueFunction(FunctionCall):
    """``JSON_VALUE(json_doc, path [RETURNING type])``.

    Extracts a scalar from a JSON document. The path is a literal Oracle
    refuses to bind (ORA-40454), so the factory inlines it; the optional
    ``RETURNING`` type names what the scalar is read back as.

    Args:
        dialect: the Oracle dialect instance.
        *args: the JSON document expression, then the path literal.
        returning_type: the ``DataType`` the scalar is returned as; ``None``
            omits the clause.

    The remaining keyword arguments are the core call's own, unchanged.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        *args: "BaseExpression",
        is_distinct: bool = False,
        alias: Optional[str] = None,
        niladic: bool = False,
        filter_predicate: Optional["SQLPredicate"] = None,
        window_spec: Optional[Union["WindowSpecification", str]] = None,
        is_aggregate: bool = False,
        returning_type: Optional["DataType"] = None,
    ):
        super().__init__(
            dialect,
            "JSON_VALUE",
            *args,
            is_distinct=is_distinct,
            alias=alias,
            niladic=niladic,
            filter_predicate=filter_predicate,
            window_spec=window_spec,
            is_aggregate=is_aggregate,
        )
        self.returning_type = returning_type

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_oracle_json_value_function"


class OracleJsonQueryFunction(FunctionCall):
    """``JSON_QUERY(json_doc, path [RETURNING type])``.

    Extracts a JSON object or array from a document -- the same shape
    :class:`OracleJsonValueFunction` has, answering with a document instead
    of a scalar.

    Args:
        dialect: the Oracle dialect instance.
        *args: the JSON document expression, then the path literal.
        returning_type: the ``DataType`` the document is returned as;
            ``None`` omits the clause.

    The remaining keyword arguments are the core call's own, unchanged.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        *args: "BaseExpression",
        is_distinct: bool = False,
        alias: Optional[str] = None,
        niladic: bool = False,
        filter_predicate: Optional["SQLPredicate"] = None,
        window_spec: Optional[Union["WindowSpecification", str]] = None,
        is_aggregate: bool = False,
        returning_type: Optional["DataType"] = None,
    ):
        super().__init__(
            dialect,
            "JSON_QUERY",
            *args,
            is_distinct=is_distinct,
            alias=alias,
            niladic=niladic,
            filter_predicate=filter_predicate,
            window_spec=window_spec,
            is_aggregate=is_aggregate,
        )
        self.returning_type = returning_type

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_oracle_json_query_function"


class OracleJsonTableFunction(FunctionCall):
    """``JSON_TABLE(json_doc, path [COLUMNS (...)])``.

    Queries JSON data as a relational table. The ``COLUMNS`` projection is
    Oracle's own sub-clause and follows the root path inside the call.

    Args:
        dialect: the Oracle dialect instance.
        *args: the JSON document expression, then the root path literal.
        columns: the ``COLUMNS (...)`` projection list; ``None`` omits the
            sub-clause.

    The remaining keyword arguments are the core call's own, unchanged.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        *args: "BaseExpression",
        is_distinct: bool = False,
        alias: Optional[str] = None,
        niladic: bool = False,
        filter_predicate: Optional["SQLPredicate"] = None,
        window_spec: Optional[Union["WindowSpecification", str]] = None,
        is_aggregate: bool = False,
        columns: Optional[str] = None,
    ):
        super().__init__(
            dialect,
            "JSON_TABLE",
            *args,
            is_distinct=is_distinct,
            alias=alias,
            niladic=niladic,
            filter_predicate=filter_predicate,
            window_spec=window_spec,
            is_aggregate=is_aggregate,
        )
        self.columns = columns

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_oracle_json_table_function"


__all__ = [
    "OracleListaggFunction",
    "OraclePercentileContFunction",
    "OraclePercentileDiscFunction",
    "OracleJsonValueFunction",
    "OracleJsonQueryFunction",
    "OracleJsonTableFunction",
]
