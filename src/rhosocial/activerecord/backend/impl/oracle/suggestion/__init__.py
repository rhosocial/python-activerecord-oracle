# src/rhosocial/activerecord/backend/impl/oracle/suggestion/__init__.py
"""Oracle backend suggestion implementations.

The concrete answers to the core ``backend.suggestion`` protocols live here,
one mixin per file, separate from the general-purpose dialect mixins. They are
composed first on ``OracleDialect`` so no core mixin can shadow their answers.
"""

from ._common import OraclePartitionSuggestionBase
from .column_reorder_suggestion import OracleColumnReorderSuggestionMixin
from .create_table_layout_suggestion import OracleCreateTableLayoutSuggestionMixin
from .general_suggestions import OracleStringConcatenationSuggestionMixin
from .partition_scheme_change_suggestion import OraclePartitionSchemeChangeSuggestionMixin
from .data_type_suggestions import (
    ORACLE_DATA_TYPE_SUGGESTIONS,
    ORACLE_EXTRA_DATA_TYPE_SUGGESTIONS,
    OracleDataTypeSuggestionsMixin,
)
from .generated_value_suggestion import OracleGeneratedValueSuggestionMixin
from .partition_add_suggestion import OraclePartitionAddSuggestionMixin
from .partition_build_suggestion import OraclePartitionBuildSuggestionMixin
from .partition_drop_suggestion import OraclePartitionDropSuggestionMixin
from .partition_merge_suggestion import OraclePartitionMergeSuggestionMixin
from .partition_retention_suggestion import OraclePartitionRetentionSuggestionMixin
from .partition_split_suggestion import OraclePartitionSplitSuggestionMixin
from .partition_table_alter_suggestion import OraclePartitionTableAlterSuggestionMixin
from .partition_table_drop_suggestion import OraclePartitionTableDropSuggestionMixin
from .table_constraint_change_suggestion import OracleTableConstraintChangeSuggestionMixin
from .table_index_change_suggestion import OracleTableIndexChangeSuggestionMixin

__all__ = [
    "ORACLE_DATA_TYPE_SUGGESTIONS",
    "ORACLE_EXTRA_DATA_TYPE_SUGGESTIONS",
    "OracleColumnReorderSuggestionMixin",
    "OracleCreateTableLayoutSuggestionMixin",
    "OracleDataTypeSuggestionsMixin",
    "OracleGeneratedValueSuggestionMixin",
    "OraclePartitionAddSuggestionMixin",
    "OraclePartitionBuildSuggestionMixin",
    "OraclePartitionDropSuggestionMixin",
    "OraclePartitionMergeSuggestionMixin",
    "OraclePartitionRetentionSuggestionMixin",
    "OraclePartitionSchemeChangeSuggestionMixin",
    "OraclePartitionSplitSuggestionMixin",
    "OraclePartitionSuggestionBase",
    "OraclePartitionTableAlterSuggestionMixin",
    "OraclePartitionTableDropSuggestionMixin",
    "OracleStringConcatenationSuggestionMixin",
    "OracleTableConstraintChangeSuggestionMixin",
    "OracleTableIndexChangeSuggestionMixin",
]
