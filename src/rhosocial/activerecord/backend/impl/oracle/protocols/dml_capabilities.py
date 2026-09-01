# oracle/protocols/dml_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

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
    def format_insert_all_statement(self, into_clauses: List[dict], select_query: Optional[str]=None, *, when_clauses: Optional[List[dict]]=None, else_clause: Optional[dict]=None, dialect_options: Optional[dict]=None) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_insert_first_statement(self, into_clauses: List[dict], select_query: Optional[str]=None, *, when_clauses: Optional[List[dict]]=None, else_clause: Optional[dict]=None, dialect_options: Optional[dict]=None) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_multi_table_insert_statement(self, keyword: str, into_clauses: List[dict], select_query: Optional[str], *, when_clauses: Optional[List[dict]]=None, else_clause: Optional[dict]=None, dialect_options: Optional[dict]=None) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_insert_into_spec(self, spec: dict, params: List[Any]) -> str:
        ...  # pragma: no cover
