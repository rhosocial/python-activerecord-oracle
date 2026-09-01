# oracle/protocols/optimizer_hint_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleOptimizerHintSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_optimizer_hint(self) -> bool:
        ...  # pragma: no cover
    def supports_hint_with_arguments(self) -> bool:
        ...  # pragma: no cover
    def format_optimizer_hint(self, name: str, args: Tuple=(), kwargs: Optional[dict]=None) -> str:
        ...  # pragma: no cover
    def format_multiple_hints(self, *hints: str) -> str:
        ...  # pragma: no cover
