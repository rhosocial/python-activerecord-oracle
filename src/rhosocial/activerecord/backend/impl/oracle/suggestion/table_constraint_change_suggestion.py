# src/rhosocial/activerecord/backend/impl/oracle/suggestion/table_constraint_change_suggestion.py
"""Oracle's table-constraint-change suggestion.

Oracle spells constraint changes as ``ALTER TABLE ... ADD/DROP CONSTRAINT`` and
the core exposes both as ``AlterTableAction`` nodes. A dropped constraint needs
only its name, which the frozen projection carries, so a pure drop is answered
with a real chain (one ``ALTER TABLE`` statement per constraint, one action per
statement).

An added constraint needs its whole declaration. Core's ``_freeze`` drops the
branch class (UNIQUE / PRIMARY KEY / CHECK / FOREIGN KEY), which is why a
frozen-only point is declined. The enriched point also carries the model's live
``TableConstraint`` declaration, whose ``branch`` keeps that class; when the
declaration names a kind Oracle can spell (and no trait the backend refuses),
the answer is a real ``AddTableConstraintAction`` chain instead of ``False``.
Kinds Oracle has no spelling for -- ``EXCLUDE`` above all -- and declarations
carrying a clause the probes reject stay declined rather than guessed.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import (
    AddTableConstraintAction,
    AlterTableStatement,
    ConstraintValidation,
    DropTableConstraintAction,
    ReferentialAction,
    TableCheckConstraint,
    TableConstraint,
    TableForeignKeyConstraint,
    TablePrimaryKeyConstraint,
    TableUniqueConstraint,
)
from rhosocial.activerecord.backend.suggestion import TableConstraintChangeSuggestion

from ._change_common import answer, projection_name, projection_params

__all__ = ["OracleTableConstraintChangeSuggestionMixin"]


def _action_word(action: Any) -> str:
    """Normalize a referential action to its SQL word (``""`` when absent)."""
    if action is None:
        return ""
    value = action.value if isinstance(action, ReferentialAction) else str(action)
    return value.strip().upper()


def _declaration_key(declaration: Any) -> Tuple[Any, ...]:
    """An identity richer than ``_freeze``: it keeps the branch class.

    Two constraints differing only by kind (``UNIQUE`` vs ``PRIMARY KEY``)
    freeze to the same projection, but their live declarations must not be
    mistaken for one another when deciding what was added or removed.
    """
    if not isinstance(declaration, TableConstraint):
        return ("repr", repr(declaration))
    branch = declaration.branch
    parts: List[Any] = [type(branch).__name__, declaration.name]
    columns = getattr(branch, "columns", None)
    if isinstance(columns, (list, tuple)):
        parts.append(tuple(str(column) for column in columns))
    if isinstance(branch, TableForeignKeyConstraint):
        referenced, referenced_columns = branch.references
        parts.append(
            (
                getattr(referenced, "name", None),
                tuple(str(column) for column in (referenced_columns or ())),
            )
        )
        parts.append(
            (_action_word(branch.on_delete), _action_word(branch.on_update), str(branch.match_type))
        )
    if isinstance(branch, TableCheckConstraint):
        # A predicate is not value-comparable; its rendered text is the closest
        # stable identity available without inventing an equality protocol.
        try:
            parts.append(branch.condition.to_sql()[0])
        except Exception:  # pragma: no cover - an unbound predicate
            parts.append(repr(branch.condition))
    return tuple(parts)


class OracleTableConstraintChangeSuggestionMixin:
    """Suggest real ADD/DROP CONSTRAINT chains where Oracle can spell them."""

    def _constraint_is_spellable(self, declaration: Any) -> bool:
        """Whether this backend can render the declaration as an ADD constraint.

        The kind itself is decided by real probes on this dialect, not by a
        string comparison: Oracle can spell PRIMARY KEY, UNIQUE, CHECK and
        FOREIGN KEY, but has no EXCLUDE branch (``supports_exclude_constraint``
        is ``False``). Traits whose rendered spelling Oracle refuses -- ``NOT
        VALID``, ``ENFORCED``, FK ``MATCH`` / ``ON UPDATE`` and the three
        unspellable ``ON DELETE`` actions -- are declined by the same probes.
        """
        branch = getattr(declaration, "branch", None)
        if isinstance(branch, TablePrimaryKeyConstraint):
            supported = self.supports_primary_key_constraint()
        elif isinstance(branch, TableUniqueConstraint):
            supported = self.supports_unique_constraint()
        elif isinstance(branch, TableCheckConstraint):
            supported = self.supports_check_constraint()
        elif isinstance(branch, TableForeignKeyConstraint):
            supported = self.supports_foreign_key_constraint()
        else:
            # EXCLUDE and any future branch: Oracle has no grammar to emit.
            return False
        if not supported or not self.supports_add_constraint():
            return False
        if declaration.name and not self.supports_constraint_naming():
            return False
        wants_deferral = (
            declaration.deferrable
            or declaration.not_deferrable
            or declaration.initially_deferred
            or declaration.initially_immediate
        )
        if wants_deferral and not self.supports_deferrable_constraint():
            return False
        if (declaration.enforced or declaration.not_enforced) and not self.supports_constraint_enforced():
            return False
        if declaration.validation is not None:
            # The shared renderer only puts a validation trait on CHECK/FK, and
            # its "NOT VALID" word is not Oracle's "NOVALIDATE".
            if not isinstance(branch, (TableCheckConstraint, TableForeignKeyConstraint)):
                return False
            if (
                declaration.validation is not ConstraintValidation.VALIDATE
                and not self.supports_constraint_novalidate()
            ):
                return False
        if isinstance(branch, TableForeignKeyConstraint):
            if branch.match_type is not None and not self.supports_fk_match():
                return False
            if _action_word(branch.on_update) not in ("", "NO ACTION") and not self.supports_fk_on_update():
                return False
            delete = _action_word(branch.on_delete)
            if delete not in ("", "NO ACTION"):
                probes = {
                    "CASCADE": "supports_fk_referential_action_cascade",
                    "SET NULL": "supports_fk_referential_action_set_null",
                    "RESTRICT": "supports_fk_referential_action_restrict",
                    "SET DEFAULT": "supports_fk_referential_action_set_default",
                }
                probe = probes.get(delete)
                if probe is None or not getattr(self, probe)():
                    return False
        return True

    def _drop_chain(self, point: Any, names: List[Optional[str]]) -> Any:
        """Build one ``ALTER TABLE ... DROP CONSTRAINT`` statement per name."""
        if not self.supports_drop_constraint():
            return answer(point, False)
        table_name = point.table_name
        if not isinstance(table_name, str) or not table_name.strip():
            return answer(point, False)
        table = Table(self, table_name)
        operations = []
        for name in names:
            if not isinstance(name, str) or not name.strip():
                # An unnamed constraint cannot be dropped by name on Oracle.
                return answer(point, False)
            operations.append(
                AlterTableStatement(self, table, [DropTableConstraintAction(self, name)])
            )
        return answer(point, tuple(operations))

    def suggest_table_constraint_change(self, point: Any) -> TableConstraintChangeSuggestion:
        """Answer with ADD/DROP CONSTRAINT statements, or ``False`` when dishonest.

        Live declarations, when the point carries them, are authoritative: a
        constraint whose enriched identity is gone is a drop, and one whose
        enriched identity is new is an add. Without them the legacy frozen-only
        path is kept -- an add is still declined because the branch kind is
        gone. Drops precede adds, one statement per action; if any add cannot be
        spelled, the whole point is declined rather than partially applied.
        """
        old_decls = tuple(getattr(point, "old_constraint_declarations", ()) or ())
        new_decls = tuple(getattr(point, "new_constraint_declarations", ()) or ())

        if old_decls or new_decls:
            old_keys = tuple(_declaration_key(item) for item in old_decls)
            new_keys = tuple(_declaration_key(item) for item in new_decls)
            removed = [item for item in old_decls if _declaration_key(item) not in new_keys]
            added = [item for item in new_decls if _declaration_key(item) not in old_keys]
            if not added and not removed:
                return answer(point, None)
            if added and not all(self._constraint_is_spellable(item) for item in added):
                return answer(point, False)
            table_name = point.table_name
            if not isinstance(table_name, str) or not table_name.strip():
                return answer(point, False)
            operations: List[Any] = []
            if removed:
                drop_names: List[Optional[str]] = [
                    getattr(item, "name", None) for item in removed
                ]
                dropped = self._drop_chain(point, drop_names)
                if dropped.operations is False:
                    return dropped
                operations.extend(dropped.operations)
            table = Table(self, table_name)
            for declaration in added:
                operations.append(
                    AlterTableStatement(
                        self,
                        table,
                        [AddTableConstraintAction(self, declaration)],
                    )
                )
            return answer(point, tuple(operations))

        # Legacy frozen-only point: the branch class was erased by `_freeze`,
        # so an addition cannot be reconstructed from the projection alone.
        old = tuple(point.old_constraints)
        new = tuple(point.new_constraints)
        added = [item for item in new if item not in old]
        removed = [item for item in old if item not in new]
        if not added and not removed:
            return answer(point, None)
        if added:
            return answer(point, False)
        return self._drop_chain(
            point, [projection_name(projection_params(item)) for item in removed]
        )
