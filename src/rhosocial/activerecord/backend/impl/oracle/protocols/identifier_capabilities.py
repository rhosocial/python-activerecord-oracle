# oracle/protocols/identifier_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleIdentifierSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def format_identifier(self, identifier: str) -> str:
        ...  # pragma: no cover
    def format_column(self, name: str, table: Optional[str]=None, alias: Optional[str]=None, schema_name: Optional[str]=None) -> Tuple[str, Tuple]:
        ...  # pragma: no cover
    def format_table(self, table: str, alias: Optional[str]=None, schema_name: Optional[str]=None, dblink: Optional[str]=None, flashback: Optional[Any]=None) -> Tuple[str, Tuple]:
        ...  # pragma: no cover
