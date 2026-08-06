# src/rhosocial/activerecord/backend/impl/oracle/mixins/column.py
from typing import Tuple, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
        AddColumn,
        DropColumn,
        ModifyColumn,
    )


class OracleModifyColumnMixin:
    """Oracle ALTER TABLE column-modification mixin.

    Oracle uses ``ALTER TABLE ... MODIFY (col TYPE [constraint])`` for column
    modifications, ``RENAME COLUMN old TO new`` for renames, and ``DROP COLUMN``
    for removals. There is no direct equivalent of MySQL's ``CHANGE COLUMN``
    (which renames + redefines in a single clause), and positional ``ADD COLUMN``
    is not honored — new columns are always appended to the end of the row.

    Oracle (<= 19c) does **not** support the vendor extensions
    ``ADD COLUMN IF NOT EXISTS``, ``DROP COLUMN IF EXISTS`` or
    ``DROP CONSTRAINT IF EXISTS`` (table-level ``IF EXISTS`` only
    arrives in 19.28+). Requesting any of these modifiers raises
    ``UnsupportedFeatureError``; applications should pre-check
    ``USER_TAB_COLUMNS`` / ``USER_CONSTRAINTS`` instead.
    """

    def supports_add_column_if_not_exists(self) -> bool:
        return False

    def supports_drop_column_if_exists(self) -> bool:
        return False

    def supports_drop_constraint_if_exists(self) -> bool:
        return False

    def supports_modify_column(self) -> bool:
        return True

    def supports_change_column(self) -> bool:
        return False

    def supports_rename_column(self) -> bool:
        return True

    def supports_add_column_position(self) -> bool:
        return False

    def supports_drop_column(self) -> bool:
        return True

    def supports_set_default(self) -> bool:
        return True

    def supports_modify_type_with_data(self) -> bool:
        return True

    def format_modify_column_action(self, action: "ModifyColumn") -> Tuple[str, tuple]:
        """Format MODIFY action for ALTER TABLE.

        Oracle's clause is ``MODIFY (col_name data_type ...)`` — the column
        definition (type, default, constraints) is supplied inside the
        parentheses. Here we emit the basic ``MODIFY (col type)`` form; the
        surrounding ``ALTER TABLE`` wrapper and parentheses grouping are
        applied by the caller.
        """
        col = action.column
        col_sql, col_params = self.format_column_definition(col)
        sql = f"MODIFY ({col_sql})"
        return sql, col_params

    def format_rename_column_action(self, old_name: str, new_name: str) -> Tuple[str, tuple]:
        """Format RENAME COLUMN action for ALTER TABLE."""
        sql = f"RENAME COLUMN {self.format_identifier(old_name)} TO {self.format_identifier(new_name)}"
        return sql, ()

    def format_drop_column_action(
        self, action_or_col_name: Union["DropColumn", str]
    ) -> Tuple[str, tuple]:
        """Format DROP COLUMN action for ALTER TABLE.

        Oracle does not support ``DROP COLUMN IF EXISTS``. The plain form
        is ``DROP COLUMN <col>``; the method accepts both the ``DropColumn``
        action object (new interface) and a bare column name (legacy
        signature) for backward compatibility.
        """
        if hasattr(action_or_col_name, "if_exists") and action_or_col_name.if_exists is True:
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE DROP COLUMN IF EXISTS",
                "Oracle does not support IF EXISTS on DROP COLUMN. "
                "Pre-check USER_TAB_COLUMNS.",
            )
        if isinstance(action_or_col_name, str):
            col_name = action_or_col_name
        else:
            col_name = action_or_col_name.column_name
        sql = f"DROP COLUMN {self.format_identifier(col_name)}"
        return sql, ()

    def format_add_column_action(self, action: "AddColumn") -> Tuple[str, tuple]:
        """Format ADD COLUMN action for ALTER TABLE.

        Oracle does not support ``ADD COLUMN IF NOT EXISTS``. Guard the
        modifier and delegate the plain form to the base implementation
        (``ADD COLUMN <col>``, which Oracle accepts).
        """
        if getattr(action, "if_not_exists", None):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE ADD COLUMN IF NOT EXISTS",
                "Oracle does not support IF NOT EXISTS on ADD COLUMN. "
                "Pre-check USER_TAB_COLUMNS.",
            )
        return super().format_add_column_action(action)

    def format_drop_table_constraint_action(self, action) -> Tuple[str, tuple]:
        """Format DROP CONSTRAINT action for ALTER TABLE.

        Oracle does not support ``DROP CONSTRAINT IF EXISTS``. Guard the
        modifier and delegate the plain form to the base implementation.
        """
        if getattr(action, "if_exists", None):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE DROP CONSTRAINT IF EXISTS",
                "Oracle does not support IF EXISTS on DROP CONSTRAINT. "
                "Pre-check USER_CONSTRAINTS.",
            )
        return super().format_drop_table_constraint_action(action)
