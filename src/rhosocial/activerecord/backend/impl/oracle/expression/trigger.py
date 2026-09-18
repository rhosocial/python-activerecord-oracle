# src/rhosocial/activerecord/backend/impl/oracle/expression/trigger.py
"""Oracle TRIGGER DDL expression wrappers.

This module defines backend-specific expressions that wrap the mixin
format methods ``format_disable_trigger_statement`` and
``format_enable_trigger_statement`` into proper
:class:`BaseExpression` subclasses, making them composable within the
expression tree and renderable through the unified
:meth:`BaseExpression.to_sql` entry point.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class DisableTriggerExpression(BaseExpression):
    """Expression wrapping ``format_disable_trigger_statement``.

    Renders an ``ALTER TRIGGER <name> DISABLE`` statement.

    Args:
        dialect: the Oracle dialect instance.
        trigger_name: the trigger to disable.
        table_name: optional table name (reserved for future use).
        dialect_options: reserved for future dialect-specific options.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        trigger_name: str,
        table_name: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.trigger_name = trigger_name
        self.table_name = table_name
        self.dialect_options = dialect_options or {}

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_disable_trigger_statement"


class EnableTriggerExpression(BaseExpression):
    """Expression wrapping ``format_enable_trigger_statement``.

    Renders an ``ALTER TRIGGER <name> ENABLE`` statement.

    Args:
        dialect: the Oracle dialect instance.
        trigger_name: the trigger to enable.
        table_name: optional table name (reserved for future use).
        dialect_options: reserved for future dialect-specific options.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        trigger_name: str,
        table_name: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.trigger_name = trigger_name
        self.table_name = table_name
        self.dialect_options = dialect_options or {}

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_enable_trigger_statement"
