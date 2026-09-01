# oracle/protocols/synonym_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleSynonymSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_create_synonym(self) -> bool:
        ...  # pragma: no cover
    def supports_drop_synonym(self) -> bool:
        ...  # pragma: no cover
    def format_create_synonym_statement(self, expr: 'OracleCreateSynonymExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_drop_synonym_statement(self, expr: 'OracleDropSynonymExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
