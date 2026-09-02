# oracle/protocols/trigger_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleTriggerSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_compound_trigger(self) -> bool:
        ...  # pragma: no cover
    def supports_system_trigger(self) -> bool:
        ...  # pragma: no cover
    def supports_disable_trigger(self) -> bool:
        ...  # pragma: no cover
    def supports_trigger_body_plsql(self) -> bool:
        ...  # pragma: no cover
    def format_disable_trigger_statement(self, trigger, table=None) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_enable_trigger_statement(self, trigger, table=None) -> Tuple[str, tuple]:
        ...  # pragma: no cover
