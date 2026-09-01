# oracle/protocols/database_link_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleDatabaseLinkSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_create_database_link(self) -> bool:
        ...  # pragma: no cover
    def supports_drop_database_link(self) -> bool:
        ...  # pragma: no cover
    def format_create_database_link_statement(self, expr: 'OracleCreateDatabaseLinkExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_drop_database_link_statement(self, expr: 'OracleDropDatabaseLinkExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
