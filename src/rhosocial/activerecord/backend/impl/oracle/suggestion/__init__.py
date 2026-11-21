# src/rhosocial/activerecord/backend/impl/oracle/suggestion/__init__.py
"""Oracle backend suggestion implementations.

The concrete answers to the core ``backend.suggestion`` protocols live here,
separate from the general-purpose dialect mixins. They are composed first on
``OracleDialect`` so no core mixin can shadow their answers.
"""

from .data_types import (
    ORACLE_DATA_TYPE_SUGGESTIONS,
    ORACLE_EXTRA_DATA_TYPE_SUGGESTIONS,
    OracleDataTypeSuggestionsMixin,
)
from .integer_auto_increment import OracleIntegerAutoIncrementColumnSuggestionMixin
from .partition import OraclePartitionSuggestionMixin

__all__ = [
    "ORACLE_DATA_TYPE_SUGGESTIONS",
    "ORACLE_EXTRA_DATA_TYPE_SUGGESTIONS",
    "OracleDataTypeSuggestionsMixin",
    "OracleIntegerAutoIncrementColumnSuggestionMixin",
    "OraclePartitionSuggestionMixin",
]
