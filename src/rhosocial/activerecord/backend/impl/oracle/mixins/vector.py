# src/rhosocial/activerecord/backend/impl/oracle/mixins/vector.py
from typing import Any, Dict, Tuple, List, Union, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.vector import OracleVectorLiteralExpression, OracleVectorOperandExpression


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

    #: Metric name -> the switch that answers for it. Oracle's
    #: ``VECTOR_DISTANCE`` grammar names the metric as a string literal, so the
    #: closed set of metrics it accepts is answered by one switch per member.
    _DISTANCE_METRIC_SWITCHES = {
        'COSINE': 'supports_vector_distance_metric_cosine',
        'EUCLIDEAN': 'supports_vector_distance_metric_euclidean',
        'DOT': 'supports_vector_distance_metric_dot',
        'MANHATTAN': 'supports_vector_distance_metric_manhattan',
        'HAMMING': 'supports_vector_distance_metric_hamming',
    }

    def supports_vector_distance_metric_cosine(self) -> bool:
        return 'COSINE' in self.SUPPORTED_DISTANCE_METRICS

    def supports_vector_distance_metric_euclidean(self) -> bool:
        return 'EUCLIDEAN' in self.SUPPORTED_DISTANCE_METRICS

    def supports_vector_distance_metric_dot(self) -> bool:
        return 'DOT' in self.SUPPORTED_DISTANCE_METRICS

    def supports_vector_distance_metric_manhattan(self) -> bool:
        return 'MANHATTAN' in self.SUPPORTED_DISTANCE_METRICS

    def supports_vector_distance_metric_hamming(self) -> bool:
        return 'HAMMING' in self.SUPPORTED_DISTANCE_METRICS

    def _supports_distance_metric(self, metric: Any) -> bool:
        """Ask the per-metric switch; an unknown name has no switch and is unsupported."""
        if not isinstance(metric, str):
            return False
        probe = self._DISTANCE_METRIC_SWITCHES.get(metric.upper())
        return bool(probe) and bool(getattr(self, probe)())

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

    def format_vector_literal_expression(self, expr: "OracleVectorLiteralExpression") -> Tuple[str, tuple]:
        """Format a OracleVectorLiteralExpression as Oracle VECTOR string literal."""
        return self._format_vector_literal_value(expr.vec), ()

    def build_vector_distance(
        self, expr: Union[Dict[str, Any], Any]
    ) -> Tuple[str, tuple]:
        """Format a vector distance expression.

        Args:
            expr: An object (or dict) describing the distance expression,
                declaring ``vector1``, ``vector2`` and ``metric`` attributes
                (or keys). ``metric`` defaults to ``'COSINE'``. An object
                that does not declare the fields fails with AttributeError
                rather than rendering a silent default.

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
            vector1 = expr.vector1
            vector2 = expr.vector2
            metric = str(expr.metric).upper() or metric

        if not self._supports_distance_metric(metric):
            from rhosocial.activerecord.backend.dialect.exceptions import (
                UnsupportedFeatureError,
            )
            raise UnsupportedFeatureError(
                self.name,
                f"VECTOR distance metric '{metric}' "
                f"(supported: {', '.join(self.SUPPORTED_DISTANCE_METRICS)})"
            )

        params: List[Any] = []
        sql_left = self._format_vector_operand_raw(vector1, params)
        sql_right = self._format_vector_operand_raw(vector2, params)
        sql = f"VECTOR_DISTANCE({sql_left}, {sql_right}, '{metric}')"
        return sql, tuple(params)

    def _format_vector_operand_raw(self, operand: Any, params: List[Any]) -> str:
        """Format a single raw vector operand for embedding in SQL."""
        if operand is None:
            return 'NULL'
        if isinstance(operand, str):
            params.append(operand)
            return self.p()
        if hasattr(operand, 'to_string'):
            params.append(operand.to_string())
            return self.p()
        if isinstance(operand, (list, tuple)):
            params.append('[' + ','.join(str(v) for v in operand) + ']')
            return self.p()
        params.append(operand)
        return self.p()

    def format_vector_operand_expression(self, expr: "OracleVectorOperandExpression") -> Tuple[str, tuple]:
        """Format a OracleVectorOperandExpression into SQL and params."""
        params: List[Any] = []
        sql = self._format_vector_operand_raw(expr.operand, params)
        return sql, tuple(params)
