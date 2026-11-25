# src/rhosocial/activerecord/backend/impl/oracle/suggestion/table_index_change_suggestion.py
"""Oracle's table-index-change suggestion.

Oracle changes indexes with standalone ``CREATE INDEX`` / ``DROP INDEX``
statements (Oracle Database 19c/23ai SQL Language Reference, ``CREATE INDEX``
and ``DROP INDEX``), not with ``ALTER TABLE`` index actions; the core exposes
both statement nodes. The frozen index projection carries the name, columns
and statement options, so a plain create/drop is answered with a real chain.
Options Oracle's grammar cannot spell here -- ``USING <type>``, ``INCLUDE``, a
partial ``WHERE``, ``TABLESPACE`` or ``CONCURRENTLY`` -- and statement-level
``IF [NOT] EXISTS`` below 19c RU 28 / 23ai are declined explicitly rather than
emitted as SQL the server rejects.
"""

from __future__ import annotations

from typing import Any

from rhosocial.activerecord.backend.expression.objects import Index, Table
from rhosocial.activerecord.backend.expression.statements import (
    CreateIndexStatement,
    DropIndexStatement,
)
from rhosocial.activerecord.backend.suggestion import TableIndexChangeSuggestion

from ._change_common import answer, projection_name, projection_params

__all__ = ["OracleTableIndexChangeSuggestionMixin"]


class OracleTableIndexChangeSuggestionMixin:
    """Suggest standalone CREATE/DROP INDEX chains where Oracle can spell them."""

    def suggest_table_index_change(self, point: Any) -> TableIndexChangeSuggestion:
        """Answer with CREATE/DROP INDEX statements, or ``False`` when dishonest.

        Drops precede creates so a same-name redefine becomes a drop followed
        by a create, one statement per action. A projection whose options the
        Oracle grammar or version gate cannot express is declined rather than
        handed to a formatter that would raise ``UnsupportedFeatureError``.
        """
        old = tuple(point.old_indexes)
        new = tuple(point.new_indexes)
        added = [item for item in new if item not in old]
        removed = [item for item in old if item not in new]
        if not added and not removed:
            return answer(point, None)
        table_name = point.table_name
        if not isinstance(table_name, str) or not table_name.strip():
            return answer(point, False)
        if not self.supports_create_index() or not self.supports_drop_index():
            return answer(point, False)
        table = Table(self, table_name)
        operations = []
        for item in removed:
            params = projection_params(item)
            name = projection_name(params)
            if name is None:
                return answer(point, False)
            if_exists = bool(params.get("if_exists"))
            if if_exists and not self.supports_index_if_exists():
                # Statement-level IF EXISTS is a 19c RU 28 / 23ai two-anchor
                # gate (see mixins/if_exists.py); below it the clause is
                # ORA-00922, so decline rather than emit it.
                return answer(point, False)
            if params.get("concurrent") and not self.supports_concurrent_index():
                return answer(point, False)
            operations.append(
                DropIndexStatement(self, Index(self, name), table=table, if_exists=if_exists)
            )
        for item in added:
            params = projection_params(item)
            name = projection_name(params)
            if name is None:
                return answer(point, False)
            columns = params.get("columns")
            if (
                not isinstance(columns, (list, tuple))
                or not columns
                or not all(isinstance(column, str) and column.strip() for column in columns)
            ):
                # Functional/expression columns freeze to param/repr tuples and
                # cannot be reconstructed from the point.
                return answer(point, False)
            # Oracle's CREATE INDEX grammar has no ``USING <type>``, ``INCLUDE``,
            # partial ``WHERE`` or ``TABLESPACE``-per-index clause in this node
            # set; the generic formatter refuses each through its probe.
            if params.get("type") and not self.supports_index_type():
                return answer(point, False)
            if params.get("include_columns") and not self.supports_index_include():
                return answer(point, False)
            if params.get("partial_condition") and not self.supports_partial_index():
                return answer(point, False)
            if params.get("tablespace") and not self.supports_index_tablespace():
                return answer(point, False)
            if params.get("concurrent") and not self.supports_concurrent_index():
                return answer(point, False)
            unique = bool(params.get("unique"))
            if unique and not self.supports_unique_index():
                return answer(point, False)
            if_not_exists = bool(params.get("if_not_exists"))
            if if_not_exists and not self.supports_index_if_not_exists():
                return answer(point, False)
            operations.append(
                CreateIndexStatement(
                    self,
                    Index(self, name),
                    table,
                    list(columns),
                    unique=unique,
                    if_not_exists=if_not_exists,
                )
            )
        return answer(point, tuple(operations))
