# oracle/protocols/datetime_op_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleDateTimeSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def format_date_trunc_expression(self, expr: 'Any') -> Tuple[str, Tuple]:
        ...  # pragma: no cover
    def format_interval_expression(self, expr: 'Any') -> Tuple[str, Tuple]:
        ...  # pragma: no cover
    def format_datetime_add_expression(self, expr: 'Any') -> Tuple[str, Tuple]:
        ...  # pragma: no cover
    def format_datetime_subtract_expression(self, expr: 'Any') -> Tuple[str, Tuple]:
        ...  # pragma: no cover
    def format_datetime_diff_expression(self, expr: 'Any') -> Tuple[str, Tuple]:
        ...  # pragma: no cover
