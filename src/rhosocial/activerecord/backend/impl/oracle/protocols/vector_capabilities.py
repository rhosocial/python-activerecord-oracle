# oracle/protocols/vector_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleVectorSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_vector_index(self) -> bool:
        ...  # pragma: no cover
    def supports_vector_distance_metric(self, metric: str) -> bool:
        ...  # pragma: no cover
    def format_vector_literal(self, vec: Any) -> str:
        ...  # pragma: no cover
    def format_vector_distance(self, expr: Any) -> Tuple[str, Tuple]:
        ...  # pragma: no cover
    def format_vector_operand(self, operand: Any, params: List[Any]) -> str:
        ...  # pragma: no cover
