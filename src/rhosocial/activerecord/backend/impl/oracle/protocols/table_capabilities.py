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
    def format_table_compression_clause(self, mode: str='BASIC') -> str:
        ...  # pragma: no cover
    def format_tablespace_clause(self, tablespace_name: str) -> str:
        ...  # pragma: no cover
