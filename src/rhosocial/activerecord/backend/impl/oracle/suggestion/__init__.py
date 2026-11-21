# src/rhosocial/activerecord/backend/impl/oracle/suggestion/__init__.py
"""Oracle backend suggestion implementations.

The concrete answers to the core ``backend.suggestion`` protocols live here,
one mixin per file, separate from the general-purpose dialect mixins. They are
composed first on ``OracleDialect`` so no core mixin can shadow their answers.
"""

from ._common import OraclePartitionSuggestionBase
from .data_type_suggestions import (
    ORACLE_DATA_TYPE_SUGGESTIONS,
    ORACLE_EXTRA_DATA_TYPE_SUGGESTIONS,
    OracleDataTypeSuggestionsMixin,
)
from .integer_auto_increment_column_suggestion import (
    OracleIntegerAutoIncrementColumnSuggestionMixin,
)
from .partition_add_suggestion import OraclePartitionAddSuggestionMixin
from .partition_build_suggestion import OraclePartitionBuildSuggestionMixin
from .partition_drop_suggestion import OraclePartitionDropSuggestionMixin
from .partition_merge_suggestion import OraclePartitionMergeSuggestionMixin
from .partition_retention_suggestion import OraclePartitionRetentionSuggestionMixin
from .partition_split_suggestion import OraclePartitionSplitSuggestionMixin
from .partition_table_alter_suggestion import OraclePartitionTableAlterSuggestionMixin
from .partition_table_drop_suggestion import OraclePartitionTableDropSuggestionMixin

__all__ = [
    "ORACLE_DATA_TYPE_SUGGESTIONS",
    "ORACLE_EXTRA_DATA_TYPE_SUGGESTIONS",
    "OracleDataTypeSuggestionsMixin",
    "OracleIntegerAutoIncrementColumnSuggestionMixin",
    "OraclePartitionAddSuggestionMixin",
    "OraclePartitionBuildSuggestionMixin",
    "OraclePartitionDropSuggestionMixin",
    "OraclePartitionMergeSuggestionMixin",
    "OraclePartitionRetentionSuggestionMixin",
    "OraclePartitionSplitSuggestionMixin",
    "OraclePartitionSuggestionBase",
    "OraclePartitionTableAlterSuggestionMixin",
    "OraclePartitionTableDropSuggestionMixin",
]
