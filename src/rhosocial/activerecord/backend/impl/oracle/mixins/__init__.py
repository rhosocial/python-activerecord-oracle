# src/rhosocial/activerecord/backend/impl/oracle/mixins/__init__.py
"""Oracle dialect mixin classes.

This package aggregates Oracle-specific mixins that, combined with the
core framework mixins, provide the full Oracle dialect surface area.
"""

from .analyze import OracleAnalyzeMixin
from .backend_mixin import OracleBackendMixin
from .collation import OracleCollationMixin
from .column import OracleModifyColumnMixin
from .comment import OracleCommentMixin
from .concurrency import OracleConcurrencyMixin
from .database_link import OracleDatabaseLinkMixin
from .datetime_op import OracleDateTimeMixin
from .ddl import OracleDDLMixin
from .dml import OracleDMLOperationMixin
from .expression import OracleExpressionMixin
from .features import OracleFeaturesMixin
from .flashback import OracleFlashbackMixin
from .functions import OracleFunctionFormatMixin
from .hierarchical import OracleHierarchicalMixin
from .hint import OracleHintMixin
from .identifier import OracleIdentifierMixin
from .index import OracleIndexMixin
from .json import OracleJSONFunctionMixin
from .locking import OracleLockingMixin
from .materialized_view import OracleMaterializedViewMixin
from .optimizer_hint import OracleOptimizerHintMixin
from .pagination import OraclePaginationMixin
from .partition import OraclePartitionMixin
from .partition_lifecycle import OraclePartitionLifecycleMixin
from .pivot import OraclePivotMixin
from .routine import OracleRoutineMixin
from .schema import OracleSchemaMixin
from .sequence import OracleSequenceMixin
from .set_operation import OracleSetOperationMixin
from .spatial import OracleSpatialMixin
from .synonym import OracleSynonymMixin
from .table import OracleTableMixin
from .table_op import OracleTableCapabilityMixin
from .transaction import OracleTransactionMixin
from .trigger import OracleTriggerMixin
from .truncate import OracleTruncateMixin
from .types import OracleTypeSupportMixin
from .vector import OracleVectorMixin
from .view import OracleViewMixin

__all__ = [
    "OracleAnalyzeMixin",
    "OracleBackendMixin",
    "OracleCollationMixin",
    "OracleDateTimeMixin",
    "OracleDDLMixin",
    "OracleExpressionMixin",
    "OracleFeaturesMixin",
    "OracleFunctionFormatMixin",
    "OracleHierarchicalMixin",
    "OracleHintMixin",
    "OracleIdentifierMixin",
    "OracleIndexMixin",
    "OracleJSONFunctionMixin",
    "OracleLockingMixin",
    "OracleMaterializedViewMixin",
    "OracleModifyColumnMixin",
    "OracleCommentMixin",
    "OracleConcurrencyMixin",
    "OracleDatabaseLinkMixin",
    "OracleDMLOperationMixin",
    "OracleFlashbackMixin",
    "OracleOptimizerHintMixin",
    "OraclePaginationMixin",
    "OraclePartitionMixin",
    "OraclePartitionLifecycleMixin",
    "OraclePivotMixin",
    "OracleRoutineMixin",
    "OracleSchemaMixin",
    "OracleSequenceMixin",
    "OracleSetOperationMixin",
    "OracleSpatialMixin",
    "OracleSynonymMixin",
    "OracleTableMixin",
    "OracleTableCapabilityMixin",
    "OracleTransactionMixin",
    "OracleTriggerMixin",
    "OracleTruncateMixin",
    "OracleTypeSupportMixin",
    "OracleVectorMixin",
    "OracleViewMixin",
]
