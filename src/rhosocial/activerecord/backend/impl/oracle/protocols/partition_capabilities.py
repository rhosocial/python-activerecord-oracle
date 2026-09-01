# oracle/protocols/partition_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OraclePartitionSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def format_partition_keys(self, keys: Sequence[BaseExpression]) -> str:
        ...  # pragma: no cover
    def format_partition_boundary_value(self, value: Any) -> str:
        ...  # pragma: no cover
    def format_legacy_range(self, expr: 'PartitionClause') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_legacy_list(self, expr: 'PartitionClause') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_legacy_hash(self, expr: 'PartitionClause') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_legacy_range_definition(self, partition: Any) -> str:
        ...  # pragma: no cover
    def format_legacy_list_definition(self, partition: Any) -> str:
        ...  # pragma: no cover
