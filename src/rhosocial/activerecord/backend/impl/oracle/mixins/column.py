# src/rhosocial/activerecord/backend/impl/oracle/mixins/column.py
from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
        ModifyColumn,
    )


class OracleModifyColumnMixin:
    """Oracle ALTER TABLE column-modification mixin.

    Oracle uses ``ALTER TABLE ... MODIFY (col TYPE [constraint])`` for column
    modifications, ``RENAME COLUMN old TO new`` for renames, and ``DROP COLUMN``
    for removals. There is no direct equivalent of MySQL's ``CHANGE COLUMN``
    (which renames + redefines in a single clause), and positional ``ADD COLUMN``
    is not honored — new columns are always appended to the end of the row.
    """

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

    def format_drop_column_action(self, col_name: str) -> Tuple[str, tuple]:
        """Format DROP COLUMN action for ALTER TABLE."""
        sql = f"DROP COLUMN {self.format_identifier(col_name)}"
        return sql, ()
