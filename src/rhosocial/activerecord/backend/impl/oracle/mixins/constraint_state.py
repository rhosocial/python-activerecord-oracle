# src/rhosocial/activerecord/backend/impl/oracle/mixins/constraint_state.py
"""Oracle constraint-state formatters.

Three ALTER TABLE actions render here, each through its own formatter:

* ``ENABLE [VALIDATE|NOVALIDATE] CONSTRAINT``  (``format_enable_constraint_action``)
* ``DISABLE [VALIDATE|NOVALIDATE] CONSTRAINT`` (``format_disable_constraint_action``)
* ``MODIFY CONSTRAINT ... {VALIDATE|NOVALIDATE}`` (``format_modify_constraint_action``)

Measured on 21c XE, as recorded in :mod:`...mixins.features`: every form above
is accepted, while the bare ``VALIDATE CONSTRAINT`` clause is refused with
``ORA-01735``. The validation switch is therefore expressed only through these
two prefixes; no formatter emits a bare ``VALIDATE CONSTRAINT``.

Each formatter is self-contained -- there is no private renderer shared between
them, so the node/formatter pairing stays one-to-one.
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.constraint_state import (
        OracleDisableConstraintAction,
        OracleEnableConstraintAction,
        OracleModifyConstraintAction,
    )


class OracleConstraintStateMixin:
    """Formatters for the constraint-state actions."""

    def format_enable_constraint_action(
        self, expr: "OracleEnableConstraintAction"
    ) -> Tuple[str, tuple]:
        """Render ``ENABLE [VALIDATE|NOVALIDATE] CONSTRAINT <name>``."""
        self.check_feature_support(  # type: ignore[attr-defined]
            "supports_enable_disable_constraint",
            "ENABLE CONSTRAINT",
            "requires Oracle's ENABLE/DISABLE vocabulary (21c XE measured).",
        )
        if expr.validate and expr.novalidate:
            raise ValueError(
                "validate and novalidate are mutually exclusive on ENABLE CONSTRAINT"
            )
        name = expr.constraint_name
        if not isinstance(name, str) or not name.strip():
            raise ValueError("ENABLE CONSTRAINT requires a non-empty constraint name")
        quoted = self.format_identifier(name)  # type: ignore[attr-defined]
        if expr.novalidate:
            return f"ENABLE NOVALIDATE CONSTRAINT {quoted}", ()
        if expr.validate:
            return f"ENABLE VALIDATE CONSTRAINT {quoted}", ()
        return f"ENABLE CONSTRAINT {quoted}", ()

    def format_disable_constraint_action(
        self, expr: "OracleDisableConstraintAction"
    ) -> Tuple[str, tuple]:
        """Render ``DISABLE [VALIDATE|NOVALIDATE] CONSTRAINT <name>``."""
        self.check_feature_support(  # type: ignore[attr-defined]
            "supports_enable_disable_constraint",
            "DISABLE CONSTRAINT",
            "requires Oracle's ENABLE/DISABLE vocabulary (21c XE measured).",
        )
        if expr.validate and expr.novalidate:
            raise ValueError(
                "validate and novalidate are mutually exclusive on DISABLE CONSTRAINT"
            )
        name = expr.constraint_name
        if not isinstance(name, str) or not name.strip():
            raise ValueError("DISABLE CONSTRAINT requires a non-empty constraint name")
        quoted = self.format_identifier(name)  # type: ignore[attr-defined]
        if expr.novalidate:
            return f"DISABLE NOVALIDATE CONSTRAINT {quoted}", ()
        if expr.validate:
            return f"DISABLE VALIDATE CONSTRAINT {quoted}", ()
        return f"DISABLE CONSTRAINT {quoted}", ()

    def format_modify_constraint_action(
        self, expr: "OracleModifyConstraintAction"
    ) -> Tuple[str, tuple]:
        """Render ``MODIFY CONSTRAINT <name> {VALIDATE|NOVALIDATE}``.

        ``MODIFY`` carries only the validation word; enabling and disabling are
        the other two actions' business.
        """
        self.check_feature_support(  # type: ignore[attr-defined]
            "supports_constraint_enable_disable",
            "MODIFY CONSTRAINT",
            "requires Oracle's constraint-state vocabulary (21c XE measured).",
        )
        name = expr.constraint_name
        if not isinstance(name, str) or not name.strip():
            raise ValueError("MODIFY CONSTRAINT requires a non-empty constraint name")
        quoted = self.format_identifier(name)  # type: ignore[attr-defined]
        word = "NOVALIDATE" if expr.novalidate else "VALIDATE"
        return f"MODIFY CONSTRAINT {quoted} {word}", ()
