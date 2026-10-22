# oracle/protocols/table_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleTableSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_create_table_as_bind_parameters(self) -> bool:
        """Whether this statement's body may carry bind parameters.

        Defaults to ``False``: a DDL statement carries no bind parameters
        unless the dialect says it can, so a dialect that does not answer
        refuses rather than emitting a statement the engine may reject. A
        dialect whose engine accepts them returns ``True``.
        """
        ...  # pragma: no cover

    def supports_inline_index(self) -> bool:
        ...  # pragma: no cover
    def supports_storage_engine_option(self) -> bool:
        ...  # pragma: no cover
    def supports_tablespace_option(self) -> bool:
        ...  # pragma: no cover
    def supports_compress_option(self) -> bool:
        ...  # pragma: no cover
    def supports_partition_option(self) -> bool:
        ...  # pragma: no cover
    def supports_iot(self) -> bool:
        ...  # pragma: no cover
    def supports_external_table(self) -> bool:
        ...  # pragma: no cover
    def supports_copyright_compatibility(self) -> bool:
        ...  # pragma: no cover
    def format_table_compression_clause(self, expr: 'TableCompressionClauseExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_tablespace_clause(self, expr: 'TablespaceClauseExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
