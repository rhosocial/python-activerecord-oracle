# expression/__init__.py
"""
Oracle-specific SQL expression definitions.

This module provides expression classes for Oracle-specific SQL features
including hierarchical queries (CONNECT BY), PIVOT/UNPIVOT, and query hints.
"""

from .hierarchical import (
    ConnectByExpression, PriorExpression,
    ConnectByRootExpression, SysConnectByPathExpression,
    ConnectByIsLeafExpression, ConnectByIsCycleExpression,
    LevelExpression, SiblingsExpression
)
from .pivot import OraclePivot, OracleUnpivot
from .hint import (
    OracleHintExpression,
    index_hint, index_asc_hint, index_desc_hint,
    full_hint, parallel_hint, parallel_hint_default, no_parallel_hint,
    leading_hint, ordered_hint,
    use_nl_hint, use_hash_hint, use_merge_hint,
    first_rows_hint, all_rows_hint,
    append_hint, cardinality_hint,
    dynamic_sampling_hint, gather_plan_statistics_hint,
    monitor_hint, no_monitor_hint
)
from .locking import (
    OracleForUpdateExpression, OracleForUpdateClause, OracleLockTableStatement,
    for_update, for_update_nowait, for_update_wait, for_update_skip_locked,
)
from .sequence import (
    OracleSequenceValueMode, OracleSequenceValueExpression,
    OracleCreateSequenceStatement, OracleDropSequenceStatement,
)
from .materialized_view import (
    MaterializedViewRefreshMethod, MaterializedViewRefreshTrigger,
    MaterializedViewBuildMode,
    OracleCreateMaterializedViewStatement,
    OracleCreateMaterializedViewLogStatement,
    OracleDropMaterializedViewStatement,
    OracleRefreshMaterializedViewStatement,
    OracleRefreshMethod,
)
from .comment import (
    OracleCommentObjectType, OracleCommentOnStatement,
)
from .flashback import (
    OracleAsOfMode, OracleVersionsBetweenMode,
    OracleAsOfClause, OracleVersionsBetweenClause,
    OracleFlashbackTableStatement,
    OraclePurgeObjectType, OraclePurgeStatement,
)
from .column import (
    OracleColumnDefinition,
    OracleColumnOptions,
)
from .alter_table import (
    OracleSetUnusedColumnsAction, OracleDropUnusedColumnsAction,
    OracleMoveTableAction, OracleShrinkSpaceAction,
    OracleReadOnlyAction, OracleRowMovementAction,
)
from .ddl import (
    OracleCreateSynonymStatement, OracleDropSynonymStatement,
    OracleCreateDatabaseLinkStatement, OracleDropDatabaseLinkStatement,
    OracleRoutineParameterMode, OracleRoutineParameter,
    OracleCreateProcedureStatement, OracleCreateFunctionStatement,
    OracleCreatePackageStatement, OracleCreatePackageBodyStatement,
    OracleDropRoutineObjectType, OracleDropRoutineStatement,
    OracleTypeAttribute, OracleTypeMethod,
    OracleObjectTypeDefinition, OracleSqljTypeDefinition,
    OracleVarrayTypeDefinition, OracleNestedTableTypeDefinition,
    OracleIncompleteTypeDefinition, OracleAlterTypeDependentHandling,
    OracleTypeAlterAction, OracleAlterTypeAddAttributeAction,
    OracleAlterTypeModifyAttributeAction, OracleAlterTypeDropAttributeAction,
    OracleAlterTypeAddMethodAction, OracleAlterTypeDropMethodAction,
    OracleAlterTypeLimitAction, OracleAlterTypeElementTypeAction,
    OracleAlterTypeCompileAction, OracleAlterTypeFinalAction,
    OracleAlterTypeInstantiableAction, OracleAlterTypeResetAction,
    OracleCreateTypeBodyStatement, OracleDropTypeExpression,
    DropTypeBodyStatement, OracleDropTypeBodyExpression,
)
from .analyze import (
    OracleAnalyzeMode, OracleAnalyzeStatement,
)
from .objects import OracleRemoteName, OraclePackage, OracleTable
from .sources import OracleNamedRelationRef
from .vector import VectorLiteralExpression, VectorOperandExpression
from .table import TableCompressionClauseExpression, TablespaceClauseExpression
from .trigger import DisableTriggerStatement, EnableTriggerStatement
from .functions import (
    OracleListaggFunction,
    OraclePercentileContFunction,
    OraclePercentileDiscFunction,
    OracleJsonValueFunction,
    OracleJsonQueryFunction,
    OracleJsonTableFunction,
)

__all__ = [
'OracleColumnDefinition', 'OracleColumnOptions',
# Hierarchical query expressions
'ConnectByExpression', 'PriorExpression',
'ConnectByRootExpression', 'SysConnectByPathExpression',
'ConnectByIsLeafExpression', 'ConnectByIsCycleExpression',
'LevelExpression', 'SiblingsExpression',
    # PIVOT/UNPIVOT expressions
    'OraclePivot', 'OracleUnpivot',
    # Query hints
    'OracleHintExpression',
    'index_hint', 'index_asc_hint', 'index_desc_hint',
    'full_hint', 'parallel_hint', 'parallel_hint_default', 'no_parallel_hint',
    'leading_hint', 'ordered_hint',
    'use_nl_hint', 'use_hash_hint', 'use_merge_hint',
    'first_rows_hint', 'all_rows_hint',
    'append_hint', 'cardinality_hint',
    'dynamic_sampling_hint', 'gather_plan_statistics_hint',
    'monitor_hint', 'no_monitor_hint',
    # Locking expressions
    'OracleForUpdateExpression', 'OracleForUpdateClause', 'OracleLockTableStatement',
    'for_update', 'for_update_nowait', 'for_update_wait', 'for_update_skip_locked',
    # Sequence expressions
    'OracleSequenceValueMode', 'OracleSequenceValueExpression',
    'OracleCreateSequenceStatement', 'OracleDropSequenceStatement',
    # Materialized view expressions
    'MaterializedViewRefreshMethod', 'MaterializedViewRefreshTrigger',
    'MaterializedViewBuildMode',
    'OracleCreateMaterializedViewStatement',
    'OracleCreateMaterializedViewLogStatement',
    'OracleDropMaterializedViewStatement',
    'OracleRefreshMaterializedViewStatement',
    'OracleRefreshMethod',
    # COMMENT ON expressions
    'OracleCommentObjectType', 'OracleCommentOnStatement',
    # FLASHBACK family expressions
    'OracleAsOfMode', 'OracleVersionsBetweenMode',
    'OracleAsOfClause', 'OracleVersionsBetweenClause',
    'OracleFlashbackTableStatement',
    'OraclePurgeObjectType', 'OraclePurgeStatement',
    # ALTER TABLE table-level clause actions
    'OracleSetUnusedColumnsAction', 'OracleDropUnusedColumnsAction',
    'OracleMoveTableAction', 'OracleShrinkSpaceAction',
    'OracleReadOnlyAction', 'OracleRowMovementAction',
    # SYNONYM / DATABASE LINK expressions
    'OracleCreateSynonymStatement', 'OracleDropSynonymStatement',
    'OracleCreateDatabaseLinkStatement', 'OracleDropDatabaseLinkStatement',
    # PL/SQL routine and package expressions
    'OracleRoutineParameterMode', 'OracleRoutineParameter',
    'OracleCreateProcedureStatement', 'OracleCreateFunctionStatement',
    'OracleCreatePackageStatement', 'OracleCreatePackageBodyStatement',
    'OracleDropRoutineObjectType', 'OracleDropRoutineStatement',
    # User-defined TYPE DDL expressions
    'OracleTypeAttribute', 'OracleTypeMethod',
    'OracleObjectTypeDefinition', 'OracleSqljTypeDefinition',
    'OracleVarrayTypeDefinition', 'OracleNestedTableTypeDefinition',
    'OracleIncompleteTypeDefinition', 'OracleAlterTypeDependentHandling',
    'OracleTypeAlterAction', 'OracleAlterTypeAddAttributeAction',
    'OracleAlterTypeModifyAttributeAction', 'OracleAlterTypeDropAttributeAction',
    'OracleAlterTypeAddMethodAction', 'OracleAlterTypeDropMethodAction',
    'OracleAlterTypeLimitAction', 'OracleAlterTypeElementTypeAction',
    'OracleAlterTypeCompileAction', 'OracleAlterTypeFinalAction',
    'OracleAlterTypeInstantiableAction', 'OracleAlterTypeResetAction',
    'OracleCreateTypeBodyStatement', 'OracleDropTypeExpression',
    'DropTypeBodyStatement', 'OracleDropTypeBodyExpression',
    # ANALYZE TABLE expressions
    'OracleAnalyzeMode', 'OracleAnalyzeStatement',
    # Schema objects and named-relation references
    'OracleRemoteName', 'OraclePackage', 'OracleTable', 'OracleNamedRelationRef',
    # Vector expressions
    'VectorLiteralExpression', 'VectorOperandExpression',
    # Table clause expressions
    'TableCompressionClauseExpression', 'TablespaceClauseExpression',
    # Trigger DDL expressions
    'DisableTriggerStatement', 'EnableTriggerStatement',
    # Oracle derived function-call nodes (LISTAGG / PERCENTILE / JSON)
    'OracleListaggFunction', 'OraclePercentileContFunction',
    'OraclePercentileDiscFunction', 'OracleJsonValueFunction',
    'OracleJsonQueryFunction', 'OracleJsonTableFunction',
]
