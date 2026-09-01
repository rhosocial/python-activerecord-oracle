# src/rhosocial/activerecord/backend/impl/oracle/mixins/flashback.py
"""Oracle FLASHBACK family formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.flashback import (
        OracleAsOfClause,
        OracleFlashbackTableExpression,
        OraclePurgeExpression,
        OracleVersionsBetweenClause,
    )


class OracleFlashbackMixin:
    """Oracle flashback capability checks and formatters.

    Flashback query (``AS OF`` / ``VERSIONS BETWEEN``), the ``FLASHBACK
    TABLE`` statement and the recycle bin / ``PURGE`` all arrived in Oracle
    10g, so the formatters gate on ``(10, 0, 0)``.
    """

    def supports_flashback_query(self) -> bool:
        return True

    def supports_flashback_table(self) -> bool:
        return True

    def supports_purge(self) -> bool:
        return True

    @staticmethod
    def format_flashback_value(value) -> Tuple[str, tuple]:
        """Render a flashback value as a SQL fragment.

        ``BaseExpression`` values are rendered through ``to_sql()``; plain
        values (typically strings such as ``SYSTIMESTAMP - INTERVAL '1' DAY``)
        are treated as raw SQL fragments.
        """
        if hasattr(value, "to_sql"):
            return value.to_sql()
        return str(value), ()

    def format_as_of_clause(self, expr: "OracleAsOfClause") -> Tuple[str, tuple]:
        if self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "AS OF clause",
                suggestion=(
                    f"Oracle {self.version} does not support flashback "
                    "query; it requires Oracle 10g or later."
                ),
            )
        value_sql, value_params = self.format_flashback_value(expr.value)
        return f"AS OF {expr.mode.value} {value_sql}", value_params

    def format_versions_between_clause(
        self, expr: "OracleVersionsBetweenClause"
    ) -> Tuple[str, tuple]:
        if self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "VERSIONS BETWEEN clause",
                suggestion=(
                    f"Oracle {self.version} does not support flashback "
                    "query; it requires Oracle 10g or later."
                ),
            )
        low_sql, low_params = self.format_flashback_value(expr.low_value)
        high_sql, high_params = self.format_flashback_value(expr.high_value)
        return (
            f"VERSIONS BETWEEN {expr.mode.value} {low_sql} AND {high_sql}",
            tuple(low_params) + tuple(high_params),
        )

    def format_flashback_table_statement(
        self, expr: "OracleFlashbackTableExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "FLASHBACK TABLE",
                suggestion=(
                    f"Oracle {self.version} does not support FLASHBACK "
                    "TABLE; it requires Oracle 10g or later."
                ),
            )
        parts = [f"FLASHBACK TABLE {self.format_identifier(expr.table)}"]
        params: tuple = ()
        if expr.to_before_drop:
            parts.append("TO BEFORE DROP")
            if expr.rename_to:
                parts.append(f"RENAME TO {self.format_identifier(expr.rename_to)}")
        elif expr.to_scn is not None:
            parts.append(f"TO SCN {expr.to_scn}")
        elif expr.to_timestamp is not None:
            ts_sql, ts_params = self.format_flashback_value(expr.to_timestamp)
            parts.append(f"TO TIMESTAMP {ts_sql}")
            params = tuple(ts_params)
        if expr.enable_triggers:
            parts.append("ENABLE TRIGGERS")
        if expr.disable_triggers:
            parts.append("DISABLE TRIGGERS")
        return " ".join(parts), params

    def format_purge_statement(self, expr: "OraclePurgeExpression") -> Tuple[str, tuple]:
        if self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "PURGE",
                suggestion=(
                    f"Oracle {self.version} does not support the recycle "
                    "bin or PURGE; it requires Oracle 10g or later."
                ),
            )
        if expr.object_type.value == "RECYCLEBIN":
            return "PURGE RECYCLEBIN", ()
        return (
            f"PURGE {expr.object_type.value} {self.format_identifier(expr.object_name)}",
            (),
        )
