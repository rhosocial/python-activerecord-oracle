# oracle/protocols/routine_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleRoutineSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_create_procedure(self) -> bool:
        ...  # pragma: no cover
    def supports_create_package(self) -> bool:
        ...  # pragma: no cover
    def supports_create_package_body(self) -> bool:
        ...  # pragma: no cover
    def format_parameters(self, parameters) -> str:
        ...  # pragma: no cover
    def format_create_procedure_statement(self, expr: 'OracleCreateProcedureExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_create_package_statement(self, expr: 'OracleCreatePackageExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_create_package_body_statement(self, expr: 'OracleCreatePackageBodyExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_drop_routine_statement(self, expr: 'OracleDropRoutineExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
