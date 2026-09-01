# oracle/protocols/column_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleModifyColumnSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_modify_column(self) -> bool:
        ...  # pragma: no cover
    def supports_change_column(self) -> bool:
        ...  # pragma: no cover
    def supports_add_column_position(self) -> bool:
        ...  # pragma: no cover
    def supports_set_default(self) -> bool:
        ...  # pragma: no cover
    def supports_modify_type_with_data(self) -> bool:
        ...  # pragma: no cover
    def format_modify_column_action(self, action: 'ModifyColumn') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_rename_column_action(self, old_name: str, new_name: str) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def supports_set_unused(self) -> bool:
        ...  # pragma: no cover
    def supports_drop_unused_columns(self) -> bool:
        ...  # pragma: no cover
    def supports_move_table(self) -> bool:
        ...  # pragma: no cover
    def supports_shrink_space(self) -> bool:
        ...  # pragma: no cover
    def supports_read_only(self) -> bool:
        ...  # pragma: no cover
    def supports_row_movement(self) -> bool:
        ...  # pragma: no cover
    def format_set_unused_action(self, action) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_drop_unused_columns_action(self, action) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_move_table_statement(self, action) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_shrink_space_statement(self, action) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_read_only_statement(self, action) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_row_movement_statement(self, action) -> Tuple[str, tuple]:
        ...  # pragma: no cover
