# oracle/protocols/materialized_view_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleMaterializedViewSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_materialized_view_bind_parameters(self) -> bool:
        """Whether this statement's body may carry bind parameters.

        Defaults to ``False``: a DDL statement carries no bind parameters
        unless the dialect says it can, so a dialect that does not answer
        refuses rather than emitting a statement the engine may reject. A
        dialect whose engine accepts them returns ``True``.
        """
        ...  # pragma: no cover

    def supports_materialized_view_log(self) -> bool:
        ...  # pragma: no cover
    def format_create_materialized_view_log_statement(self, expr: 'OracleCreateMaterializedViewLogStatement') -> Tuple[str, tuple]:
        ...  # pragma: no cover
