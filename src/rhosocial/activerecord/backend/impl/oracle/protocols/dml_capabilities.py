# oracle/protocols/dml_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""


from typing import Protocol

class OracleDMLOperationSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_insert_ignore(self) -> bool:
        ...  # pragma: no cover
    def supports_replace_into(self) -> bool:
        ...  # pragma: no cover
    def supports_load_data(self) -> bool:
        ...  # pragma: no cover
    def supports_insert_all(self) -> bool:
        ...  # pragma: no cover
    def supports_returning_into(self) -> bool:
        ...  # pragma: no cover
    def supports_multi_table_insert(self) -> bool:
        ...  # pragma: no cover
    def supports_hint_in_insert(self) -> bool:
        ...  # pragma: no cover
    def supports_insert_first(self) -> bool:
        ...  # pragma: no cover
