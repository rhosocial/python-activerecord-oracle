# src/rhosocial/activerecord/backend/impl/oracle/mixins/trigger.py
from typing import Tuple


class OracleTriggerMixin(object):
    """Oracle trigger DDL implementation.

    Oracle provides comprehensive trigger support including BEFORE/AFTER row
    and statement level triggers, INSTEAD OF triggers on views (Oracle
    pioneered this feature), compound triggers (11g+), ENABLE/DISABLE controls,
    and DDL/database event (system) triggers.
    """

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
        parts.append(self.format_identifier(trigger_expr.trigger_name))

        if timing_value is not None:
            parts.append(timing_value)

        events = getattr(trigger_expr, 'events', None) or []
        event_values = [e.value for e in events]

        if timing_value == "INSTEAD OF":
            if not event_values:
                raise ValueError("INSTEAD OF trigger requires at least one event")
            parts.append(event_values[0])
            parts.append("ON")
            parts.append(self.format_identifier(trigger_expr.table_name))
        else:
            if getattr(trigger_expr, 'update_columns', None):
                if not event_values or event_values[0] != "UPDATE":
                    raise ValueError("UPDATE OF requires UPDATE event")
                parts.append("UPDATE OF")
                parts.append(", ".join(self.format_identifier(c) for c in trigger_expr.update_columns))
            elif event_values:
                parts.append(" OR ".join(event_values))
            parts.append("ON")
            parts.append(self.format_identifier(trigger_expr.table_name))

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
        if body is None and getattr(trigger_expr, 'function_name', None) is None:
            raise NotImplementedError("Oracle trigger body (PL/SQL block) templating requires dialect-specific context")

        if getattr(trigger_expr, 'function_name', None):
            parts.append("CALL")
            parts.append(self.format_identifier(trigger_expr.function_name))
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

        parts.append(self.format_identifier(drop_expr.trigger_name))

        return " ".join(parts), ()

    def format_disable_trigger_statement(self, trigger_name, table_name=None) -> Tuple[str, tuple]:
        """Format ALTER TRIGGER ... DISABLE statement (Oracle syntax)."""
        if not self.supports_disable_trigger():
            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
            raise UnsupportedFeatureError(self.name, "DISABLE TRIGGER")

        parts = ["ALTER TRIGGER", self.format_identifier(trigger_name), "DISABLE"]
        return " ".join(parts), ()

    def format_enable_trigger_statement(self, trigger_name, table_name=None) -> Tuple[str, tuple]:
        """Format ALTER TRIGGER ... ENABLE statement (Oracle syntax)."""
        if not self.supports_disable_trigger():
            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
            raise UnsupportedFeatureError(self.name, "ENABLE TRIGGER")

        parts = ["ALTER TRIGGER", self.format_identifier(trigger_name), "ENABLE"]
        return " ".join(parts), ()
