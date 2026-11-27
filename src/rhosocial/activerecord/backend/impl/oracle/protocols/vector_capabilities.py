# oracle/protocols/vector_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import TYPE_CHECKING, Protocol, Tuple

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.vector import OracleVectorLiteralExpression, OracleVectorOperandExpression

class OracleVectorSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_vector_index(self) -> bool:
        ...  # pragma: no cover
    def supports_vector_distance_metric_cosine(self) -> bool:
        ...  # pragma: no cover
    def supports_vector_distance_metric_euclidean(self) -> bool:
        ...  # pragma: no cover
    def supports_vector_distance_metric_dot(self) -> bool:
        ...  # pragma: no cover
    def supports_vector_distance_metric_manhattan(self) -> bool:
        ...  # pragma: no cover
    def supports_vector_distance_metric_hamming(self) -> bool:
        ...  # pragma: no cover
    def format_vector_literal_expression(self, expr: 'OracleVectorLiteralExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_vector_operand_expression(self, expr: 'OracleVectorOperandExpression') -> Tuple[str, tuple]:
        ...  # pragma: no cover
