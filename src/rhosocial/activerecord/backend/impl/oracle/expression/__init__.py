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
]
