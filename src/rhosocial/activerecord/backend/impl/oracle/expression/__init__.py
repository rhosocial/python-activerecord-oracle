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
from .pivot import PivotExpression, UnpivotExpression
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
    OracleForUpdateExpression, OracleLockTableExpression,
    for_update, for_update_nowait, for_update_wait, for_update_skip_locked,
)
from .sequence import (
    OracleSequenceValueMode, OracleSequenceValueExpression,
    OracleCreateSequenceExpression, OracleDropSequenceExpression,
)
from .materialized_view import (
    MaterializedViewRefreshMethod, MaterializedViewRefreshTrigger,
    MaterializedViewBuildMode,
    OracleCreateMaterializedViewExpression,
    OracleCreateMaterializedViewLogExpression,
    OracleDropMaterializedViewExpression,
)
from .comment import (
    OracleCommentObjectType, OracleCommentExpression,
)
from .flashback import (
    OracleAsOfMode, OracleVersionsBetweenMode,
    OracleAsOfClause, OracleVersionsBetweenClause,
    OracleFlashbackTableExpression,
    OraclePurgeObjectType, OraclePurgeExpression,
)
from .alter_table import (
    OracleSetUnusedColumnsAction, OracleDropUnusedColumnsAction,
    OracleMoveTableAction, OracleShrinkSpaceAction,
    OracleReadOnlyAction, OracleRowMovementAction,
)
from .ddl import (
    OracleCreateSynonymExpression, OracleDropSynonymExpression,
    OracleCreateDatabaseLinkExpression, OracleDropDatabaseLinkExpression,
    OracleRoutineParameterMode, OracleRoutineParameter,
    OracleCreateProcedureExpression, OracleCreateFunctionExpression,
    OracleCreatePackageExpression, OracleCreatePackageBodyExpression,
    OracleDropRoutineObjectType, OracleDropRoutineExpression,
)
from .analyze import (
    OracleAnalyzeMode, OracleAnalyzeExpression,
)

__all__ = [
# Hierarchical query expressions
'ConnectByExpression', 'PriorExpression',
'ConnectByRootExpression', 'SysConnectByPathExpression',
'ConnectByIsLeafExpression', 'ConnectByIsCycleExpression',
'LevelExpression', 'SiblingsExpression',
    # PIVOT/UNPIVOT expressions
    'PivotExpression', 'UnpivotExpression',
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
    'OracleForUpdateExpression', 'OracleLockTableExpression',
    'for_update', 'for_update_nowait', 'for_update_wait', 'for_update_skip_locked',
    # Sequence expressions
    'OracleSequenceValueMode', 'OracleSequenceValueExpression',
    'OracleCreateSequenceExpression', 'OracleDropSequenceExpression',
    # Materialized view expressions
    'MaterializedViewRefreshMethod', 'MaterializedViewRefreshTrigger',
    'MaterializedViewBuildMode',
    'OracleCreateMaterializedViewExpression',
    'OracleCreateMaterializedViewLogExpression',
    'OracleDropMaterializedViewExpression',
    # COMMENT ON expressions
    'OracleCommentObjectType', 'OracleCommentExpression',
    # FLASHBACK family expressions
    'OracleAsOfMode', 'OracleVersionsBetweenMode',
    'OracleAsOfClause', 'OracleVersionsBetweenClause',
    'OracleFlashbackTableExpression',
    'OraclePurgeObjectType', 'OraclePurgeExpression',
    # ALTER TABLE table-level clause actions
    'OracleSetUnusedColumnsAction', 'OracleDropUnusedColumnsAction',
    'OracleMoveTableAction', 'OracleShrinkSpaceAction',
    'OracleReadOnlyAction', 'OracleRowMovementAction',
    # SYNONYM / DATABASE LINK expressions
    'OracleCreateSynonymExpression', 'OracleDropSynonymExpression',
    'OracleCreateDatabaseLinkExpression', 'OracleDropDatabaseLinkExpression',
    # PL/SQL routine and package expressions
    'OracleRoutineParameterMode', 'OracleRoutineParameter',
    'OracleCreateProcedureExpression', 'OracleCreateFunctionExpression',
    'OracleCreatePackageExpression', 'OracleCreatePackageBodyExpression',
    'OracleDropRoutineObjectType', 'OracleDropRoutineExpression',
    # ANALYZE TABLE expressions
    'OracleAnalyzeMode', 'OracleAnalyzeExpression',
]
