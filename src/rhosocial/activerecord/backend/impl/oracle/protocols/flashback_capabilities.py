# oracle/protocols/flashback_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleFlashbackSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_flashback_query(self) -> bool:
        ...  # pragma: no cover
    def supports_flashback_table(self) -> bool:
        ...  # pragma: no cover
    def supports_purge(self) -> bool:
        ...  # pragma: no cover
    def format_as_of_clause(self, expr: 'OracleAsOfClause') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_versions_between_clause(self, expr: 'OracleVersionsBetweenClause') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_flashback_table_statement(self, expr: 'OracleFlashbackTableExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_purge_statement(self, expr: 'OraclePurgeExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
