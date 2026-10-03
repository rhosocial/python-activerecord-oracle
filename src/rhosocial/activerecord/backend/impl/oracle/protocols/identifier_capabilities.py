# oracle/protocols/identifier_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.core import Column

from typing import Protocol

class OracleIdentifierSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def format_identifier(self, identifier: str, need_quote: bool = True) -> str:
        ...  # pragma: no cover
    def format_column(self, expr: 'Column') -> Tuple[str, Tuple]:
        ...  # pragma: no cover
    def format_table(self, expr: 'BaseExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
