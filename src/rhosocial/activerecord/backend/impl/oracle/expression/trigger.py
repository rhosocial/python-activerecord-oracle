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

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import Table, Trigger

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class DisableTriggerExpression(BaseExpression):
    """Expression wrapping ``format_disable_trigger_statement``.

    Renders an ``ALTER TRIGGER <name> DISABLE`` statement.

    Args:
        dialect: the Oracle dialect instance.
        trigger: the trigger to disable, carrying its owner when it is not the
            caller's own.
        table: optional table the trigger fires on.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        trigger: Trigger,
        table: Optional[Table] = None,
    ):
        super().__init__(dialect)
        if not isinstance(trigger, Trigger):
            raise TypeError(
                f"trigger must be a Trigger, got {type(trigger).__name__}"
            )
        if table is not None and not isinstance(table, Table):
            raise TypeError(
                f"table must be a Table, got {type(table).__name__}"
            )
        self.trigger = trigger
        self.table = table

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_disable_trigger_statement"


class EnableTriggerExpression(BaseExpression):
    """Expression wrapping ``format_enable_trigger_statement``.

    Renders an ``ALTER TRIGGER <name> ENABLE`` statement.

    Args:
        dialect: the Oracle dialect instance.
        trigger: the trigger to enable, carrying its owner when it is not the
            caller's own.
        table: optional table the trigger fires on.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        trigger: Trigger,
        table: Optional[Table] = None,
    ):
        super().__init__(dialect)
        if not isinstance(trigger, Trigger):
            raise TypeError(
                f"trigger must be a Trigger, got {type(trigger).__name__}"
            )
        if table is not None and not isinstance(table, Table):
            raise TypeError(
                f"table must be a Table, got {type(table).__name__}"
            )
        self.trigger = trigger
        self.table = table

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_enable_trigger_statement"
