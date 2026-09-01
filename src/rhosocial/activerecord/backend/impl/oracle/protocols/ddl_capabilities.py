# oracle/protocols/ddl_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleDDLSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def format_column_definition(self, col_def: 'ColumnDefinition', constraint_type=None) -> Tuple[str, List[Any]]:
        ...  # pragma: no cover
    def format_table_constraint(self, t_const: 'TableConstraint', TableConstraintType) -> Tuple[str, List[Any]]:
        ...  # pragma: no cover
