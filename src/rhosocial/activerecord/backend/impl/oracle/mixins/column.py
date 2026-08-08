# src/rhosocial/activerecord/backend/impl/oracle/mixins/column.py
from typing import List, Tuple, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import AlterTableExpression
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

    def format_alter_table_statement(
        self, expr: "AlterTableExpression"
    ) -> Tuple[str, tuple]:
        """Format an ALTER TABLE statement with clean clause joining.

        Mirrors the generic core implementation but joins the rendered action
        clauses without the core's leading-space hack, so a single table-level
        clause renders as ``ALTER TABLE T MOVE`` rather than ``ALTER TABLE T
        MOVE`` with a double space.
        """
        all_params: List = []
        parts = [f"ALTER TABLE {self.format_identifier(expr.table_name)}"]
        action_parts = []
        for action in expr.actions:
            action_part, action_params = action.to_sql()
            action_parts.append(action_part)
            all_params.extend(action_params)
        if action_parts:
            parts.append(", ".join(action_parts))
        return " ".join(parts), tuple(all_params)

    # ----------------------------------------------------------------
    # Table-level clauses (SET UNUSED / MOVE / SHRINK / READ ONLY /
    # ROW MOVEMENT). Rendered as ALTER TABLE actions through the core
    # action-dispatch mechanism: each Oracle-specific action overrides
    # ``to_sql()`` and delegates here.
    # ----------------------------------------------------------------
    def supports_set_unused(self) -> bool:
        return True

    def supports_drop_unused_columns(self) -> bool:
        return True

    def supports_move_table(self) -> bool:
        return True

    def supports_shrink_space(self) -> bool:
        return True

    def supports_read_only(self) -> bool:
        return self.version >= (11, 0, 0)

    def supports_row_movement(self) -> bool:
        return True

    def format_set_unused_action(self, action) -> Tuple[str, tuple]:
        """Format ``SET UNUSED (c1, c2)`` for ALTER TABLE."""
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE SET UNUSED",
                suggestion=(
                    f"Oracle {self.version} does not support SET UNUSED; "
                    "it requires Oracle 9i or later."
                ),
            )
        cols = ", ".join(self.format_identifier(c) for c in action.columns)
        return f"SET UNUSED ({cols})", ()

    def format_drop_unused_columns_action(self, action) -> Tuple[str, tuple]:
        """Format ``DROP UNUSED COLUMNS`` for ALTER TABLE."""
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE DROP UNUSED COLUMNS",
                suggestion=(
                    f"Oracle {self.version} does not support DROP UNUSED "
                    "COLUMNS; it requires Oracle 9i or later."
                ),
            )
        return "DROP UNUSED COLUMNS", ()

    def format_move_table_statement(self, action) -> Tuple[str, tuple]:
        """Format the ``MOVE`` clause for ALTER TABLE."""
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE MOVE",
                suggestion=(
                    f"Oracle {self.version} does not support ALTER TABLE "
                    "MOVE; it requires Oracle 9i or later."
                ),
            )
        return "MOVE", ()

    def format_shrink_space_statement(self, action) -> Tuple[str, tuple]:
        """Format ``SHRINK SPACE [CASCADE]`` for ALTER TABLE."""
        if self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE SHRINK SPACE",
                suggestion=(
                    f"Oracle {self.version} does not support segment "
                    "shrink; it requires Oracle 10g or later."
                ),
            )
        sql = "SHRINK SPACE"
        if getattr(action, "cascade", False):
            sql += " CASCADE"
        return sql, ()

    def format_read_only_statement(self, action) -> Tuple[str, tuple]:
        """Format ``READ ONLY`` / ``READ WRITE`` for ALTER TABLE."""
        if self.version < (11, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE READ ONLY",
                suggestion=(
                    f"Oracle {self.version} does not support READ ONLY; "
                    "it requires Oracle 11.1 or later."
                ),
            )
        return ("READ ONLY" if action.read_only else "READ WRITE"), ()

    def format_row_movement_statement(self, action) -> Tuple[str, tuple]:
        """Format ``ENABLE | DISABLE ROW MOVEMENT`` for ALTER TABLE."""
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE ROW MOVEMENT",
                suggestion=(
                    f"Oracle {self.version} does not support row "
                    "movement; it requires Oracle 9i or later."
                ),
            )
        return ("ENABLE ROW MOVEMENT" if action.enable else "DISABLE ROW MOVEMENT"), ()
