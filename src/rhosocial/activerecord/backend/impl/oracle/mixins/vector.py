# src/rhosocial/activerecord/backend/impl/oracle/mixins/vector.py
from typing import Any, Tuple, List, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.vector import VectorLiteralExpression, VectorOperandExpression


class OracleVectorMixin(object):
    """Oracle vector data type implementation (Oracle 23ai+).

    Oracle 23.4 introduces native VECTOR storage with support for
    HNSW (Hierarchical Navigable Small World) vector indexes and
    In-Memory Aggregation for vector similarity search.
    """

    MAX_VECTOR_DIMENSION = 65535

    SUPPORTED_DISTANCE_METRICS = ('COSINE', 'EUCLIDEAN', 'DOT', 'MANHATTAN', 'HAMMING')

    def supports_vector_type(self) -> bool:
        return getattr(self, 'version', (23, 0, 0)) >= (23, 0, 0)

    def supports_vector_index(self) -> bool:
        return True

    def get_max_vector_dimension(self) -> int:
        return self.MAX_VECTOR_DIMENSION

    def supports_vector_distance_metric(self, metric: str) -> bool:
        if not isinstance(metric, str):
            return False
        return metric.upper() in self.SUPPORTED_DISTANCE_METRICS

    def _format_vector_literal_value(self, vec: Any) -> str:
        """Format a raw vector value as Oracle VECTOR string literal."""
        if vec is None:
            return 'NULL'
        if hasattr(vec, 'to_string'):
            return vec.to_string()
        try:
            import oracledb  # type: ignore
            if isinstance(vec, oracledb.Vector):
                return str(vec)
        except (ImportError, AttributeError):
            pass
        if isinstance(vec, (list, tuple)):
            return '[' + ','.join(str(v) for v in vec) + ']'
        if isinstance(vec, str):
            return vec
        raise TypeError(f"Cannot format vector literal from {type(vec).__name__}")

    def format_vector_literal(self, vec_or_expr: "VectorLiteralExpression | Any") -> Tuple[str, tuple]:
        """Format a vector value as Oracle VECTOR string literal."""
        from ..expression.vector import VectorLiteralExpression
        if isinstance(vec_or_expr, VectorLiteralExpression):
            return self._format_vector_literal_value(vec_or_expr.vec), ()
        return self._format_vector_literal_value(vec_or_expr), ()

    def format_vector_distance(self, expr: Any) -> Tuple[str, Tuple]:
        """Format a vector distance expression.

        Args:
            expr: An object (or dict) describing the distance expression,
                expected to expose ``vector1``, ``vector2`` and ``metric``
                attributes/keys. ``metric`` defaults to ``'COSINE'``.

        Returns:
            Tuple of (sql, params) where params supplies the operand
            placeholders and the metric literal.
        """
        metric = 'COSINE'
        if isinstance(expr, dict):
            vector1 = expr.get('vector1')
            vector2 = expr.get('vector2')
            metric = str(expr.get('metric', metric)).upper()
        else:
            vector1 = getattr(expr, 'vector1', None)
            vector2 = getattr(expr, 'vector2', None)
            metric = str(getattr(expr, 'metric', metric)).upper() or metric

        if not self.supports_vector_distance_metric(metric):
            from rhosocial.activerecord.backend.dialect.exceptions import (
                UnsupportedFeatureError,
            )
            raise UnsupportedFeatureError(
                self.name,
                f"VECTOR distance metric '{metric}' "
                f"(supported: {', '.join(self.SUPPORTED_DISTANCE_METRICS)})"
            )

        params: List[Any] = []
        sql_left = self.format_vector_operand(vector1, params)
        sql_right = self.format_vector_operand(vector2, params)
        sql = f"VECTOR_DISTANCE({sql_left}, {sql_right}, '{metric}')"
        return sql, tuple(params)

    def _format_vector_operand_raw(self, operand: Any, params: List[Any]) -> str:
        """Format a single raw vector operand for embedding in SQL."""
        if operand is None:
            return 'NULL'
        if isinstance(operand, str):
            params.append(operand)
            return '?'
        if hasattr(operand, 'to_string'):
            params.append(operand.to_string())
            return '?'
        if isinstance(operand, (list, tuple)):
            params.append('[' + ','.join(str(v) for v in operand) + ']')
            return '?'
        params.append(operand)
        return '?'

    def format_vector_operand(self, expr: "VectorOperandExpression") -> Tuple[str, Tuple]:
        """Format a VectorOperandExpression into SQL and params."""
        from ..expression.vector import VectorOperandExpression
        if isinstance(expr, VectorOperandExpression):
            params: List[Any] = []
            sql = self._format_vector_operand_raw(expr.operand, params)
            return sql, tuple(params)
        # Legacy raw-value path
        params2: List[Any] = []
        sql = self._format_vector_operand_raw(expr, params2)
        return sql, tuple(params2)
