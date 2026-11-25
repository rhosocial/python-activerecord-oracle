# src/rhosocial/activerecord/backend/impl/oracle/expression/constraint_state.py
"""Oracle constraint-state ALTER TABLE actions.

Oracle changes a constraint's enforcement and validation state with a
``ENABLE`` / ``DISABLE`` clause and with ``MODIFY CONSTRAINT``; there is no
``ALTER CONSTRAINT`` statement to carry the SQL-standard ``ENFORCED`` /
``NOT ENFORCED`` vocabulary. Measured on 21c XE (see
``mixins/features.py``)::

    ALTER TABLE t ENABLE CONSTRAINT ck
    ALTER TABLE t DISABLE CONSTRAINT ck
    ALTER TABLE t ENABLE VALIDATE CONSTRAINT ck
    ALTER TABLE t ENABLE NOVALIDATE CONSTRAINT ck
    ALTER TABLE t MODIFY CONSTRAINT ck VALIDATE
    ALTER TABLE t MODIFY CONSTRAINT ck NOVALIDATE

The bare ``ALTER TABLE t VALIDATE CONSTRAINT ck`` form is refused by the
server (``ORA-01735``), so validation is only reachable through the
``ENABLE``-prefixed or ``MODIFY``-prefixed spellings.

Core has no node for this vocabulary, so these actions are owned by this
backend; they are never re-exported from core. Constructors collect parameters
only and the dialect formatter owns the state vocabulary and every refusal.
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AlterTableAction,
)

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect

__all__ = [
    "OracleEnableConstraintAction",
    "OracleDisableConstraintAction",
    "OracleModifyConstraintAction",
]


class OracleEnableConstraintAction(AlterTableAction):
    """``ALTER TABLE ... ENABLE [VALIDATE|NOVALIDATE] CONSTRAINT <name>``.

    Args:
        dialect: The Oracle dialect, or ``None`` for a later binding.
        constraint_name: The constraint to enable.
        validate: Emit ``ENABLE VALIDATE`` (rejects existing violations).
        novalidate: Emit ``ENABLE NOVALIDATE`` (accepts existing violations).
    """

    def __init__(
        self,
        dialect: Optional["OracleDialect"],
        constraint_name: str,
        *,
        validate: bool = False,
        novalidate: bool = False,
    ):
        super().__init__(dialect)
        self.constraint_name = constraint_name
        self.validate = validate
        self.novalidate = novalidate

    @property
    def format_method(self) -> str:
        """The dialect formatter that renders this ENABLE action."""
        return "format_enable_constraint_action"


class OracleDisableConstraintAction(AlterTableAction):
    """``ALTER TABLE ... DISABLE [VALIDATE|NOVALIDATE] CONSTRAINT <name>``."""

    def __init__(
        self,
        dialect: Optional["OracleDialect"],
        constraint_name: str,
        *,
        validate: bool = False,
        novalidate: bool = False,
    ):
        super().__init__(dialect)
        self.constraint_name = constraint_name
        self.validate = validate
        self.novalidate = novalidate

    @property
    def format_method(self) -> str:
        """The dialect formatter that renders this DISABLE action."""
        return "format_disable_constraint_action"


class OracleModifyConstraintAction(AlterTableAction):
    """``ALTER TABLE ... MODIFY CONSTRAINT <name> {VALIDATE|NOVALIDATE}``.

    Args:
        dialect: The Oracle dialect, or ``None`` for a later binding.
        constraint_name: The constraint to modify.
        novalidate: Emit ``NOVALIDATE``; the default is ``VALIDATE``.
    """

    def __init__(
        self,
        dialect: Optional["OracleDialect"],
        constraint_name: str,
        *,
        novalidate: bool = False,
    ):
        super().__init__(dialect)
        self.constraint_name = constraint_name
        self.novalidate = novalidate

    @property
    def format_method(self) -> str:
        """The dialect formatter that renders this MODIFY action."""
        return "format_modify_constraint_action"
