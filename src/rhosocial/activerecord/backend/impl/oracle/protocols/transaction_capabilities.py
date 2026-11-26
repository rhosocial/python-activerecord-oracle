# oracle/protocols/transaction_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Protocol

class OracleTransactionSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_isolation_level_read_uncommitted(self) -> bool:
        ...  # pragma: no cover
    def supports_isolation_level_read_committed(self) -> bool:
        ...  # pragma: no cover
    def supports_isolation_level_repeatable_read(self) -> bool:
        ...  # pragma: no cover
    def supports_isolation_level_serializable(self) -> bool:
        ...  # pragma: no cover
