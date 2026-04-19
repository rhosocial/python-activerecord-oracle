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

dialect = OracleDialect()

root_expr = ConnectByRootExpression(column="employee_id")
sql, params = root_expr.to_sql(dialect)
print(f"CONNECT_BY_ROOT expression:")
print(f"  SQL: {sql}")
print(f"  Params: {params}")

path_expr = SysConnectByPathExpression(column="name", separator="/")
sql, params = path_expr.to_sql(dialect)
print(f"\nSYS_CONNECT_BY_PATH expression:")
print(f"  SQL: {sql}")
print(f"  Params: {params}")

prior_expr = PriorExpression(column="manager_id")
sql, params = prior_expr.to_sql(dialect)
print(f"\nPRIOR expression:")
print(f"  SQL: {sql}")
print(f"  Params: {params}")

level_expr = LevelExpression()
sql, params = level_expr.to_sql(dialect)
print(f"\nLEVEL pseudo-column:")
print(f"  SQL: {sql}")

is_leaf = ConnectByIsLeafExpression()
sql, params = is_leaf.to_sql(dialect)
print(f"\nCONNECT_BY_ISLEAF:")
print(f"  SQL: {sql}")

print("\n" + "-" * 40)
print("2. PIVOT/UNPIVOT Expressions")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.expression import (
    PivotExpression,
    UnpivotExpression,
)

pivot = PivotExpression(
    aggregate_function="SUM",
    value_column="sales",
    pivot_column="month",
    values=["Jan", "Feb", "Mar"]
)
sql, params = pivot.to_sql(dialect)
print(f"PIVOT expression:")
print(f"  SQL: {sql}")
print(f"  Params: {params}")

unpivot = UnpivotExpression(
    value_column="sales",
    pivot_column="month",
    columns=["jan_sales", "feb_sales", "mar_sales"],
    include_nulls=True
)
sql, params = unpivot.to_sql(dialect)
print(f"\nUNPIVOT expression:")
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

single_hint = OracleHintExpression(hints=[index_hint("users", "idx_name")])
sql, params = single_hint.to_sql(dialect)
print(f"Single hint:")
print(f"  SQL: {sql}")

multi_hint = OracleHintExpression(hints=[
    full_hint("users"),
    parallel_hint("users", 4),
])
sql, params = multi_hint.to_sql(dialect)
print(f"\nMultiple hints:")
print(f"  SQL: {sql}")

print(f"\nHint factory functions:")
print(f"  index_hint('users', 'idx'): {index_hint('users', 'idx')}")
print(f"  parallel_hint('users', 4): {parallel_hint('users', 4)}")
print(f"  leading_hint('users', 'orders'): {leading_hint('users', 'orders')}")
print(f"  first_rows_hint(100): {first_rows_hint(100)}")

print("\n" + "-" * 40)
print("4. Enhanced FOR UPDATE Locking")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.expression import OracleForUpdateExpression

basic_lock = OracleForUpdateExpression()
sql, params = basic_lock.to_sql(dialect)
print(f"Basic FOR UPDATE:")
print(f"  SQL: {sql}")

nowait_lock = OracleForUpdateExpression(nowait=True)
sql, params = nowait_lock.to_sql(dialect)
print(f"\nFOR UPDATE NOWAIT:")
print(f"  SQL: {sql}")

wait_lock = OracleForUpdateExpression(wait_seconds=10)
sql, params = wait_lock.to_sql(dialect)
print(f"\nFOR UPDATE WAIT 10:")
print(f"  SQL: {sql}")

skip_lock = OracleForUpdateExpression(skip_locked=True)
sql, params = skip_lock.to_sql(dialect)
print(f"\nFOR UPDATE SKIP LOCKED:")
print(f"  SQL: {sql}")

col_lock = OracleForUpdateExpression(
    columns=["id", "name"],
    nowait=True
)
sql, params = col_lock.to_sql(dialect)
print(f"\nFOR UPDATE OF columns NOWAIT:")
print(f"  SQL: {sql}")

print("\n" + "=" * 60)
print("All expression examples completed successfully!")
print("=" * 60)
