# oracle/protocols/pagination_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OraclePaginationSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def format_limit_offset(self, limit: Optional[int]=None, offset: Optional[int]=None) -> Tuple[Optional[str], List[Any]]:
        ...  # pragma: no cover
    def format_limit_offset_clause(self, clause) -> Tuple[str, tuple]:
        ...  # pragma: no cover
