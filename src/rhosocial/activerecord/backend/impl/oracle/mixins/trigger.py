# src/rhosocial/activerecord/backend/impl/oracle/mixins/trigger.py
from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.trigger import DisableTriggerExpression, EnableTriggerExpression


class OracleTriggerMixin(object):
    """Oracle trigger DDL implementation.

    Oracle provides comprehensive trigger support including BEFORE/AFTER row
    and statement level triggers, INSTEAD OF triggers on views (Oracle
    pioneered this feature), compound triggers (11g+), ENABLE/DISABLE controls,
    and DDL/database event (system) triggers.
    """

    def _trigger_name_sql(self, expr) -> str:
        """Render a trigger name through its own object protocol."""
        return expr.trigger.to_sql()[0]

    def _trigger_target_sql(self, expr) -> str:
        """Render the relation a trigger fires on through its own object protocol.

        A trigger's target is a table, so it is rendered as a ``Table`` -- which
        means the owner, when the expression carries one, is quoted by the same
        rules as the trigger's own name.
        """
        return expr.table.to_sql()[0]

    def supports_trigger(self) -> bool:
        """Oracle has supported triggers since ancient versions."""
        return True

    def supports_instead_of_trigger(self) -> bool:
        """Oracle pioneered INSTEAD OF triggers on views."""
        return True

    def supports_compound_trigger(self) -> bool:
        """Oracle 11g+ supports compound triggers."""
        version = getattr(self, 'version', None)
        if version is None:
            return False
        return version >= (11, 0, 0)

    def supports_system_trigger(self) -> bool:
        """Oracle supports DDL/database event (system) triggers."""
        return True

    def supports_disable_trigger(self) -> bool:
        """Oracle supports ENABLE/DISABLE TRIGGER clauses."""
        return True

    def supports_trigger_body_plsql(self) -> bool:
        """Oracle trigger bodies are written in PL/SQL."""
        return True

    def format_create_trigger_statement(self, trigger_expr) -> Tuple[str, tuple]:
        """Format CREATE OR REPLACE TRIGGER statement (Oracle syntax).

        Composes an Oracle trigger DDL from a trigger expression. Oracle
        supports a rich trigger syntax; this method intentionally raises
        NotImplementedError when the supplied expression requires elements
        whose canonical templating depends on yet-to-be-defined helpers.
        """
        if not self.supports_trigger():
            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
            raise UnsupportedFeatureError(self.name, "triggers")

        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

        timing = getattr(trigger_expr, 'timing', None)
        timing_value = timing.value if timing is not None else None

        if timing_value == "INSTEAD OF" and not self.supports_instead_of_trigger():
            raise UnsupportedFeatureError(self.name, "INSTEAD OF triggers")

        parts = ["CREATE OR REPLACE TRIGGER"]
        parts.append(self._trigger_name_sql(trigger_expr))

        if timing_value is not None:
            parts.append(timing_value)

        events = getattr(trigger_expr, 'events', None) or []
        event_values = [e.value for e in events]

        if timing_value == "INSTEAD OF":
            if not event_values:
                raise ValueError("INSTEAD OF trigger requires at least one event")
            parts.append(event_values[0])
            parts.append("ON")
            parts.append(self._trigger_target_sql(trigger_expr))
        else:
            if getattr(trigger_expr, 'update_columns', None):
                if not event_values or event_values[0] != "UPDATE":
                    raise ValueError("UPDATE OF requires UPDATE event")
                parts.append("UPDATE OF")
                parts.append(", ".join(self.format_identifier(c) for c in trigger_expr.update_columns))
            elif event_values:
                parts.append(" OR ".join(event_values))
            parts.append("ON")
            parts.append(self._trigger_target_sql(trigger_expr))

        level = getattr(trigger_expr, 'level', None)
        level_value = level.value if level is not None else None
        if level_value == "FOR EACH STATEMENT" and timing_value != "INSTEAD OF":
            if self.supports_compound_trigger():
                parts.append("COMPOUND TRIGGER")
            else:
                raise NotImplementedError(
                    "Compound (statement-level) trigger templating requires dialect-specific context"
                )
        elif level_value is None or level_value == "FOR EACH ROW":
            if timing_value != "INSTEAD OF":
                parts.append("FOR EACH ROW")

        referencing = getattr(trigger_expr, 'referencing', None)
        if referencing:
            raise NotImplementedError("Oracle REFERENCING clause templating requires dialect-specific context")

        condition = getattr(trigger_expr, 'condition', None)
        if condition:
            raise NotImplementedError("Oracle WHEN clause templating requires dialect-specific context")

        body = getattr(trigger_expr, 'body', None)
        if body is None and getattr(trigger_expr, 'function', None) is None:
            raise NotImplementedError("Oracle trigger body (PL/SQL block) templating requires dialect-specific context")

        if getattr(trigger_expr, 'function', None) is not None:
            parts.append("CALL")
            parts.append(trigger_expr.function.to_sql()[0])
        elif body is not None:
            parts.append("BEGIN")
            parts.append(body)
            parts.append("END;")

        return " ".join(parts), ()

    def format_drop_trigger_statement(self, drop_expr) -> Tuple[str, tuple]:
        """Format DROP TRIGGER statement (Oracle syntax)."""
        if not self.supports_trigger():
            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
            raise UnsupportedFeatureError(self.name, "triggers")

        parts = ["DROP TRIGGER"]

        if getattr(drop_expr, 'if_exists', False):
            raise NotImplementedError("Oracle does not support IF EXISTS on DROP TRIGGER")

        parts.append(self._trigger_name_sql(drop_expr))

        return " ".join(parts), ()

    def format_disable_trigger_statement(self, expr: "DisableTriggerExpression") -> Tuple[str, tuple]:
        """Format ALTER TRIGGER ... DISABLE statement (Oracle syntax)."""
        if not self.supports_disable_trigger():
            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
            raise UnsupportedFeatureError(self.name, "DISABLE TRIGGER")

        parts = ["ALTER TRIGGER", self._trigger_name_sql(expr), "DISABLE"]
        return " ".join(parts), ()

    def format_enable_trigger_statement(self, expr: "EnableTriggerExpression") -> Tuple[str, tuple]:
        """Format ALTER TRIGGER ... ENABLE statement (Oracle syntax)."""
        if not self.supports_disable_trigger():
            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
            raise UnsupportedFeatureError(self.name, "ENABLE TRIGGER")

        parts = ["ALTER TRIGGER", self._trigger_name_sql(expr), "ENABLE"]
        return " ".join(parts), ()
