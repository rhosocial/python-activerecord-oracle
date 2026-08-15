# expression/hint.py
"""
Oracle query hint expressions.

Query hints provide instructions to the Oracle optimizer
for query execution. They can be used to override
the optimizer's default behavior.

Hints are embedded in comments after SELECT keyword:
    SELECT /*+ INDEX(users idx_name) */ * FROM users

Note: Use hints sparingly and only when necessary.
      The optimizer usually makes good decisions.
"""

from dataclasses import dataclass, field
from typing import List, Tuple, Any


@dataclass
class OracleHintExpression:
    """Oracle query hints expression.
    
    Query hints are embedded in comments after SELECT keyword.
    Multiple hints can be combined.
    
    Example SQL:
        SELECT /*+ INDEX(users idx_name) PARALLEL(4) */ * FROM users
    
    Example usage:
        hint = OracleHintExpression(hints=["INDEX(users idx_name)", "PARALLEL(4)"])
        # or using factory functions
        hint = OracleHintExpression(hints=[index_hint("users", "idx_name"), parallel_hint("users", 4)])
    
    Attributes:
        hints: List of hint strings
    """
    hints: List[str] = field(default_factory=list)
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        """Generate hint SQL."""
        if not self.hints:
            return ("", [])
        hints_str = " ".join(self.hints)
        return (f"/*+ {hints_str} */", [])
    
    def __add__(self, other: 'OracleHintExpression') -> 'OracleHintExpression':
        """Combine two hint expressions."""
        return OracleHintExpression(hints=self.hints + other.hints)
    
    def __bool__(self) -> bool:
        """Return True if any hints are defined."""
        return bool(self.hints)


# Index hints
def index_hint(table: str, index: str) -> str:
    """Generate INDEX hint.
    
    Instructs optimizer to use specific index.
    
    Args:
        table: Table name
        index: Index name
        
    Returns:
        Hint string
        
    Example:
        index_hint("users", "idx_name")  # INDEX(users idx_name)
    """
    return f"INDEX({table} {index})"


def index_asc_hint(table: str, index: str) -> str:
    """Generate INDEX_ASC hint for ascending index scan.
    
    Args:
        table: Table name
        index: Index name
        
    Returns:
        Hint string
    """
    return f"INDEX_ASC({table} {index})"


def index_desc_hint(table: str, index: str) -> str:
    """Generate INDEX_DESC hint for descending index scan.
    
    Args:
        table: Table name
        index: Index name
        
    Returns:
        Hint string
    """
    return f"INDEX_DESC({table} {index})"


def index_ffs_hint(table: str, index: str) -> str:
    """Generate INDEX_FFS hint for fast full index scan.
    
    Args:
        table: Table name
        index: Index name
        
    Returns:
        Hint string
    """
    return f"INDEX_FFS({table} {index})"


def index_ss_hint(table: str, index: str) -> str:
    """Generate INDEX_SS hint for index skip scan.
    
    Args:
        table: Table name
        index: Index name
        
    Returns:
        Hint string
    """
    return f"INDEX_SS({table} {index})"


def no_index_hint(table: str, index: str) -> str:
    """Generate NO_INDEX hint.
    
    Instructs optimizer NOT to use specific index.
    
    Args:
        table: Table name
        index: Index name
        
    Returns:
        Hint string
    """
    return f"NO_INDEX({table} {index})"


# Full table scan hint
def full_hint(table: str) -> str:
    """Generate FULL hint for full table scan.
    
    Instructs optimizer to use full table scan.
    
    Args:
        table: Table name
        
    Returns:
        Hint string
        
    Example:
        full_hint("users")  # FULL(users)
    """
    return f"FULL({table})"


# Parallel execution hints
def parallel_hint(table: str, degree: int) -> str:
    """Generate PARALLEL hint.
    
    Instructs optimizer to use parallel execution.
    
    Args:
        table: Table name
        degree: Parallel degree
        
    Returns:
        Hint string
        
    Example:
        parallel_hint("users", 4)  # PARALLEL(users 4)
    """
    return f"PARALLEL({table} {degree})"


def parallel_hint_default(degree: int) -> str:
    """Generate default PARALLEL hint.
    
    Sets parallel degree for the entire statement.
    
    Args:
        degree: Parallel degree
        
    Returns:
        Hint string
    """
    return f"PARALLEL {degree}"


def no_parallel_hint(table: str) -> str:
    """Generate NO_PARALLEL hint.
    
    Disables parallel execution for a table.
    
    Args:
        table: Table name
        
    Returns:
        Hint string
    """
    return f"NO_PARALLEL({table})"


def pq_distribute_hint(outer: str, inner: str, distribution: str) -> str:
    """Generate PQ_DISTRIBUTE hint for parallel join distribution.
    
    Args:
        outer: Outer table
        inner: Inner table
        distribution: Distribution method (HASH, BROADCAST, PARTITION, etc.)
        
    Returns:
        Hint string
    """
    return f"PQ_DISTRIBUTE({outer} {inner} {distribution})"


# Join order hints
def leading_hint(*tables: str) -> str:
    """Generate LEADING hint for join order.
    
    Specifies the order of tables in join.
    
    Args:
        *tables: Table names in join order
        
    Returns:
        Hint string
        
    Example:
        leading_hint("users", "orders")  # LEADING(users orders)
    """
    return f"LEADING({' '.join(tables)})"


def ordered_hint() -> str:
    """Generate ORDERED hint.
    
    Preserves the join order from FROM clause.
    
    Returns:
        Hint string
    """
    return "ORDERED"


# Join method hints
def use_nl_hint(*tables: str) -> str:
    """Generate USE_NL hint for nested loop joins.
    
    Instructs optimizer to use nested loop join method.
    
    Args:
        *tables: Tables to join with nested loop
        
    Returns:
        Hint string
        
    Example:
        use_nl_hint("orders", "line_items")  # USE_NL(orders line_items)
    """
    return f"USE_NL({' '.join(tables)})"


def use_hash_hint(*tables: str) -> str:
    """Generate USE_HASH hint for hash joins.
    
    Instructs optimizer to use hash join method.
    
    Args:
        *tables: Tables to join with hash join
        
    Returns:
        Hint string
    """
    return f"USE_HASH({' '.join(tables)})"


def use_merge_hint(*tables: str) -> str:
    """Generate USE_MERGE hint for merge joins.
    
    Instructs optimizer to use merge join method.
    
    Args:
        *tables: Tables to join with merge join
        
    Returns:
        Hint string
    """
    return f"USE_MERGE({' '.join(tables)})"


def use_merge_cartesian_hint(*tables: str) -> str:
    """Generate USE_MERGE_CARTESIAN hint.
    
    Instructs optimizer to use merge join with cartesian join.
    
    Args:
        *tables: Tables to join
        
    Returns:
        Hint string
    """
    return f"USE_MERGE_CARTESIAN({' '.join(tables)})"


def no_use_nl_hint(*tables: str) -> str:
    """Generate NO_USE_NL hint.
    
    Prevents nested loop join for specified tables.
    
    Args:
        *tables: Tables to not use nested loop join with
        
    Returns:
        Hint string
    """
    return f"NO_USE_NL({' '.join(tables)})"


def no_use_hash_hint(*tables: str) -> str:
    """Generate NO_USE_HASH hint.
    
    Prevents hash join for specified tables.
    
    Args:
        *tables: Tables to not use hash join with
        
    Returns:
        Hint string
    """
    return f"NO_USE_HASH({' '.join(tables)})"


def no_use_merge_hint(*tables: str) -> str:
    """Generate NO_USE_MERGE hint.
    
    Prevents merge join for specified tables.
    
    Args:
        *tables: Tables to not use merge join with
        
    Returns:
        Hint string
    """
    return f"NO_USE_MERGE({' '.join(tables)})"


# Optimizer goal hints
def first_rows_hint(n: int) -> str:
    """Generate FIRST_ROWS hint.
    
    Optimizes for fast initial response time.
    
    Args:
        n: Number of rows to optimize for
        
    Returns:
        Hint string
        
    Example:
        first_rows_hint(10)  # FIRST_ROWS(10)
    """
    return f"FIRST_ROWS({n})"


def all_rows_hint() -> str:
    """Generate ALL_ROWS hint.
    
    Optimizes for total throughput.
    
    Returns:
        Hint string
    """
    return "ALL_ROWS"


def choose_hint() -> str:
    """Generate CHOOSE hint.
    
    Lets optimizer choose between ALL_ROWS and FIRST_ROWS.
    
    Returns:
        Hint string
    """
    return "CHOOSE"


# Cardinality hint
def cardinality_hint(table: str, cardinality: int) -> str:
    """Generate CARDINALITY hint.
    
    Overrides optimizer's cardinality estimate for a table.
    
    Args:
        table: Table name
        cardinality: Estimated cardinality
        
    Returns:
        Hint string
        
    Example:
        cardinality_hint("users", 1000)  # CARDINALITY(users 1000)
    """
    return f"CARDINALITY({table} {cardinality})"


def opt_param_hint(parameter: str, value: str) -> str:
    """Generate OPT_PARAM hint.
    
    Sets optimizer parameter for the statement.
    
    Args:
        parameter: Parameter name
        value: Parameter value
        
    Returns:
        Hint string
    """
    return f"OPT_PARAM('{parameter}' '{value}')"


# DML hints
def append_hint() -> str:
    """Generate APPEND hint for direct-path INSERT.
    
    Uses direct-path insert for better performance.
    
    Returns:
        Hint string
    """
    return "APPEND"


def no_append_hint() -> str:
    """Generate NO_APPEND hint.
    
    Disables direct-path insert.
    
    Returns:
        Hint string
    """
    return "NO_APPEND"


def parallel_dml_hint(degree: int = None) -> str:
    """Generate PARALLEL_DML hint.
    
    Enables parallel DML operations.
    
    Args:
        degree: Optional parallel degree
        
    Returns:
        Hint string
    """
    if degree:
        return f"PARALLEL_DML({degree})"
    return "PARALLEL_DML"


def no_parallel_dml_hint() -> str:
    """Generate NO_PARALLEL_DML hint.
    
    Disables parallel DML.
    
    Returns:
        Hint string
    """
    return "NO_PARALLEL_DML"


# Dynamic sampling hint
def dynamic_sampling_hint(level: int) -> str:
    """Generate DYNAMIC_SAMPLING hint.
    
    Controls dynamic sampling level.
    
    Args:
        level: Sampling level (0-10)
        
    Returns:
        Hint string
        
    Example:
        dynamic_sampling_hint(4)  # DYNAMIC_SAMPLING 4
    """
    return f"DYNAMIC_SAMPLING {level}"


def dynamic_sampling_hint_table(table: str, level: int) -> str:
    """Generate DYNAMIC_SAMPLING hint for specific table.
    
    Args:
        table: Table name
        level: Sampling level (0-10)
        
    Returns:
        Hint string
    """
    return f"DYNAMIC_SAMPLING({table} {level})"


# Monitoring hints
def gather_plan_statistics_hint() -> str:
    """Generate GATHER_PLAN_STATISTICS hint.
    
    Collects statistics for plan analysis.
    
    Returns:
        Hint string
    """
    return "GATHER_PLAN_STATISTICS"


def monitor_hint() -> str:
    """Generate MONITOR hint.
    
    Enables real-time SQL monitoring.
    
    Returns:
        Hint string
    """
    return "MONITOR"


def no_monitor_hint() -> str:
    """Generate NO_MONITOR hint.
    
    Disables SQL monitoring.
    
    Returns:
        Hint string
    """
    return "NO_MONITOR"


# Result cache hint
def result_cache_hint() -> str:
    """Generate RESULT_CACHE hint.
    
    Caches query result.
    
    Returns:
        Hint string
    """
    return "RESULT_CACHE"


def no_result_cache_hint() -> str:
    """Generate NO_RESULT_CACHE hint.
    
    Disables result caching.
    
    Returns:
        Hint string
    """
    return "NO_RESULT_CACHE"


# Predefined hint sets for common use cases
OPTIMAL_FIRST_ROWS = OracleHintExpression(hints=["FIRST_ROWS(100)"])
OPTIMAL_THROUGHPUT = OracleHintExpression(hints=["ALL_ROWS"])
PARALLEL_QUERY = OracleHintExpression(hints=["PARALLEL(4)"])
GATHER_STATS = OracleHintExpression(hints=["GATHER_PLAN_STATISTICS"])
