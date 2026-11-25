# src/rhosocial/activerecord/backend/impl/oracle/suggestion/table_constraint_change_suggestion.py
"""Oracle's table-constraint-change suggestion.

Oracle spells constraint changes as ``ALTER TABLE ... ADD/DROP CONSTRAINT`` and
the core exposes both as ``AlterTableAction`` nodes. A dropped constraint needs
only its name, which the frozen projection carries, so a pure drop is answered
with a real chain (one ``ALTER TABLE`` statement per constraint, one action per
statement). An added constraint needs its whole declaration, but core's
``_freeze`` drops the branch class (UNIQUE / PRIMARY KEY / CHECK / FOREIGN KEY),
so an ADD cannot be reconstructed from the point; any change that adds a
constraint is declined explicitly instead of guessing a declaration.
"""

from __future__ import annotations

from typing import Any

from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import (
    AlterTableStatement,
    DropTableConstraintAction,
)
from rhosocial.activerecord.backend.suggestion import TableConstraintChangeSuggestion

from ._change_common import answer, projection_name, projection_params

__all__ = ["OracleTableConstraintChangeSuggestionMixin"]


class OracleTableConstraintChangeSuggestionMixin:
    """Suggest real DROP CONSTRAINT chains; decline additions."""

    def suggest_table_constraint_change(self, point: Any) -> TableConstraintChangeSuggestion:
        """Answer with DROP CONSTRAINT statements, or ``False`` when dishonest.

        A constraint present in the old declaration but missing in the new one
        is a drop; its name survives the freeze, so a real
        ``ALTER TABLE ... DROP CONSTRAINT`` chain is returned. A constraint
        present only in the new declaration is an add: the frozen projection
        omits the branch kind, so the ``TableConstraint`` node cannot be rebuilt
        and the whole point is declined (a partial chain would silently leave
        the new constraint unapplied).
        """
        old = tuple(point.old_constraints)
        new = tuple(point.new_constraints)
        added = [item for item in new if item not in old]
        removed = [item for item in old if item not in new]
        if not added and not removed:
            return answer(point, None)
        if added:
            return answer(point, False)
        if not self.supports_drop_constraint():
            return answer(point, False)

        table_name = point.table_name
        if not isinstance(table_name, str) or not table_name.strip():
            return answer(point, False)
        table = Table(self, table_name)
        operations = []
        for item in removed:
            name = projection_name(projection_params(item))
            if name is None:
                # An unnamed constraint cannot be dropped by name on Oracle.
                return answer(point, False)
            operations.append(
                AlterTableStatement(self, table, [DropTableConstraintAction(self, name)])
            )
        return answer(point, tuple(operations))
