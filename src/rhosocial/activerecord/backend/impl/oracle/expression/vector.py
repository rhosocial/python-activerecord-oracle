# src/rhosocial/activerecord/backend/impl/oracle/expression/vector.py
"""Oracle VECTOR data type expression wrappers.

This module defines backend-specific expressions that wrap the mixin
format methods ``format_vector_literal`` and ``format_vector_operand``
into proper :class:`BaseExpression` subclasses, making them composable
within the expression tree and renderable through the unified
:meth:`BaseExpression.to_sql` entry point.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class VectorLiteralExpression(BaseExpression):
    """Expression wrapping ``format_vector_literal``.

    Formats a vector value as an Oracle ``VECTOR`` string literal suitable
    for inline SQL embedding.

    Args:
        dialect: the Oracle dialect instance.
        vec: the vector value — a list/tuple of numbers, a string, or an
            ``oracledb.Vector`` instance.
        dialect_options: reserved for future dialect-specific options.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        vec: Any,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.vec = vec
        self.dialect_options = dialect_options or {}

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_vector_literal"


class VectorOperandExpression(BaseExpression):
    """Expression wrapping ``format_vector_operand``.

    Formats a single vector operand for embedding in SQL, appending the
    bind-parameter value to *params* and returning the placeholder.

    Args:
        dialect: the Oracle dialect instance.
        operand: the vector operand value.
        dialect_options: reserved for future dialect-specific options.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        operand: Any,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.operand = operand
        self.dialect_options = dialect_options or {}

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_vector_operand"
