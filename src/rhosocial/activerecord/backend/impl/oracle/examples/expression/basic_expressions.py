"""
Oracle Expression Examples.

This example demonstrates Oracle-specific SQL expressions:
1. Hierarchical queries (CONNECT BY)
2. PIVOT/UNPIVOT operations
3. Query hints
4. Enhanced FOR UPDATE locking
"""

import sys
sys.path.insert(0, 'src')

print("=" * 60)
print("Oracle Expression Examples")
print("=" * 60)

print("\n" + "-" * 40)
print("1. Hierarchical Query Expressions")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.expression import (
    ConnectByRootExpression,
    SysConnectByPathExpression,
    PriorExpression,
    LevelExpression,
    ConnectByIsLeafExpression,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

dialect = OracleDialect(version=(19, 0, 0))

root_expr = ConnectByRootExpression(dialect, column="employee_id")
sql, params = root_expr.to_sql()
print("CONNECT_BY_ROOT expression:")
print(f"  SQL: {sql}")
print(f"  Params: {params}")

path_expr = SysConnectByPathExpression(dialect, column="name", separator="/")
sql, params = path_expr.to_sql()
print("\nSYS_CONNECT_BY_PATH expression:")
print(f"  SQL: {sql}")
print(f"  Params: {params}")

prior_expr = PriorExpression(dialect, column="manager_id")
sql, params = prior_expr.to_sql()
print("\nPRIOR expression:")
print(f"  SQL: {sql}")
print(f"  Params: {params}")

level_expr = LevelExpression(dialect)
sql, params = level_expr.to_sql()
print("\nLEVEL pseudo-column:")
print(f"  SQL: {sql}")

is_leaf = ConnectByIsLeafExpression(dialect)
sql, params = is_leaf.to_sql()
print("\nCONNECT_BY_ISLEAF:")
print(f"  SQL: {sql}")

print("\n" + "-" * 40)
print("2. PIVOT/UNPIVOT Expressions")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.expression import (
    PivotExpression,
    UnpivotExpression,
)

pivot = PivotExpression(
    dialect,
    aggregate_function="SUM",
    value_column="sales",
    pivot_column="month",
    values=["Jan", "Feb", "Mar"]
)
sql, params = pivot.to_sql()
print("PIVOT expression:")
print(f"  SQL: {sql}")
print(f"  Params: {params}")

unpivot = UnpivotExpression(
    dialect,
    value_column="sales",
    pivot_column="month",
    columns=["jan_sales", "feb_sales", "mar_sales"],
    include_nulls=True
)
sql, params = unpivot.to_sql()
print("\nUNPIVOT expression:")
print(f"  SQL: {sql}")
print(f"  Params: {params}")

print("\n" + "-" * 40)
print("3. Query Hints")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleHintExpression,
    index_hint,
    parallel_hint,
    leading_hint,
    full_hint,
    first_rows_hint,
)

single_hint = OracleHintExpression(dialect, hints=[index_hint("users", "idx_name")])
sql, params = single_hint.to_sql()
print("Single hint:")
print(f"  SQL: {sql}")

multi_hint = OracleHintExpression(dialect, hints=[
    full_hint("users"),
    parallel_hint("users", 4),
])
sql, params = multi_hint.to_sql()
print("\nMultiple hints:")
print(f"  SQL: {sql}")

print("\nHint factory functions:")
print(f"  index_hint('users', 'idx'): {index_hint('users', 'idx')}")
print(f"  parallel_hint('users', 4): {parallel_hint('users', 4)}")
print(f"  leading_hint('users', 'orders'): {leading_hint('users', 'orders')}")
print(f"  first_rows_hint(100): {first_rows_hint(100)}")

print("\n" + "-" * 40)
print("4. Enhanced FOR UPDATE Locking")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression

basic_lock = OracleForUpdateExpression(dialect)
sql, params = basic_lock.to_sql()
print("Basic FOR UPDATE:")
print(f"  SQL: {sql}")

nowait_lock = OracleForUpdateExpression(dialect, nowait=True)
sql, params = nowait_lock.to_sql()
print("\nFOR UPDATE NOWAIT:")
print(f"  SQL: {sql}")

wait_lock = OracleForUpdateExpression(dialect, wait_seconds=10)
sql, params = wait_lock.to_sql()
print("\nFOR UPDATE WAIT 10:")
print(f"  SQL: {sql}")

skip_lock = OracleForUpdateExpression(dialect, skip_locked=True)
sql, params = skip_lock.to_sql()
print("\nFOR UPDATE SKIP LOCKED:")
print(f"  SQL: {sql}")

col_lock = OracleForUpdateExpression(
    dialect,
    columns=["id", "name"],
    nowait=True
)
sql, params = col_lock.to_sql()
print("\nFOR UPDATE OF columns NOWAIT:")
print(f"  SQL: {sql}")

print("\n" + "-" * 40)
print("5. LOCK TABLE Statement")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleLockTableExpression,
    for_update, for_update_nowait, for_update_wait, for_update_skip_locked,
)

lock_table = OracleLockTableExpression(dialect, table="users", mode="EXCLUSIVE")
sql, params = lock_table.to_sql()
print("LOCK TABLE exclusive:")
print(f"  SQL: {sql}")

lock_table_share = OracleLockTableExpression(
    dialect,
    table="accounts", mode="SHARE", nowait=True
)
sql, params = lock_table_share.to_sql()
print("\nLOCK TABLE share NOWAIT:")
print(f"  SQL: {sql}")

print("\nFactory functions:")
basic_for_update = for_update(dialect)
sql, params = basic_for_update.to_sql()
print(f"  for_update(dialect): {sql}")

nowait_factory = for_update_nowait(dialect)
sql, params = nowait_factory.to_sql()
print(f"  for_update_nowait(dialect): {sql}")

wait_factory = for_update_wait(dialect, seconds=5)
sql, params = wait_factory.to_sql()
print(f"  for_update_wait(5): {sql}")

skip_locked_factory = for_update_skip_locked(dialect)
sql, params = skip_locked_factory.to_sql()
print(f"  for_update_skip_locked(dialect): {sql}")

print("\n" + "=" * 60)
print("All expression examples completed successfully!")
print("=" * 60)
