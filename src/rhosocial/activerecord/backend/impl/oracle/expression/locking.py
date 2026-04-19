# expression/locking.py
"""
Oracle locking expressions.

Oracle provides multiple FOR UPDATE variants for row-level locking,
enabling fine-grained control over locking behavior.
"""

from dataclasses import dataclass
from typing import Optional, List, Tuple, Any


@dataclass
class OracleForUpdateExpression:
    """Oracle FOR UPDATE locking clause.
    
    Oracle supports multiple variants of FOR UPDATE for row-level locking:
    - FOR UPDATE - Basic locking, waits for locks
    - FOR UPDATE NOWAIT - Fail immediately if rows are locked
    - FOR UPDATE WAIT n - Wait n seconds for locks
    - FOR UPDATE SKIP LOCKED - Skip locked rows (11g+)
    - FOR UPDATE OF columns - Lock specific tables in joins
    
    Example SQL:
        SELECT * FROM users WHERE status = 'A' FOR UPDATE NOWAIT
        SELECT * FROM orders o, order_items i
        WHERE o.id = i.order_id FOR UPDATE OF o.id NOWAIT
    
    Example usage:
        # Basic FOR UPDATE
        lock = OracleForUpdateExpression()
        
        # FOR UPDATE NOWAIT
        lock = OracleForUpdateExpression(nowait=True)
        
        # FOR UPDATE WAIT 10
        lock = OracleForUpdateExpression(wait_seconds=10)
        
        # FOR UPDATE SKIP LOCKED
        lock = OracleForUpdateExpression(skip_locked=True)
        
        # FOR UPDATE OF specific columns
        lock = OracleForUpdateExpression(columns=["id", "name"], nowait=True)
    
    Attributes:
        columns: Optional list of columns to lock (in joins)
        nowait: If True, fail immediately if rows are locked
        wait_seconds: Wait time in seconds (mutually exclusive with nowait)
        skip_locked: If True, skip locked rows (11g+)
    """
    columns: Optional[List[str]] = None
    nowait: bool = False
    wait_seconds: Optional[int] = None
    skip_locked: bool = False
    
    def __post_init__(self):
        # Validate options are mutually exclusive
        options = [self.nowait, self.wait_seconds is not None, self.skip_locked]
        if sum(options) > 1:
            raise ValueError(
                "NOWAIT, WAIT n, and SKIP LOCKED are mutually exclusive"
            )
        if self.wait_seconds is not None and self.wait_seconds < 0:
            raise ValueError("wait_seconds must be non-negative")
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        """Generate FOR UPDATE SQL."""
        parts = ["FOR UPDATE"]
        
        if self.columns:
            cols = ", ".join(dialect.format_identifier(c) for c in self.columns)
            parts.append(f"OF {cols}")
        
        if self.nowait:
            parts.append("NOWAIT")
        elif self.wait_seconds is not None:
            parts.append(f"WAIT {self.wait_seconds}")
        elif self.skip_locked:
            parts.append("SKIP LOCKED")
        
        return (" ".join(parts), [])


def for_update() -> OracleForUpdateExpression:
    """Create basic FOR UPDATE expression.
    
    Returns:
        OracleForUpdateExpression instance
    """
    return OracleForUpdateExpression()


def for_update_nowait(columns: Optional[List[str]] = None) -> OracleForUpdateExpression:
    """Create FOR UPDATE NOWAIT expression.
    
    Args:
        columns: Optional columns to lock
        
    Returns:
        OracleForUpdateExpression instance
    """
    return OracleForUpdateExpression(columns=columns, nowait=True)


def for_update_wait(seconds: int, columns: Optional[List[str]] = None) -> OracleForUpdateExpression:
    """Create FOR UPDATE WAIT n expression.
    
    Args:
        seconds: Number of seconds to wait
        columns: Optional columns to lock
        
    Returns:
        OracleForUpdateExpression instance
    """
    return OracleForUpdateExpression(columns=columns, wait_seconds=seconds)


def for_update_skip_locked(columns: Optional[List[str]] = None) -> OracleForUpdateExpression:
    """Create FOR UPDATE SKIP LOCKED expression.
    
    Args:
        columns: Optional columns to lock
        
    Returns:
        OracleForUpdateExpression instance
    """
    return OracleForUpdateExpression(columns=columns, skip_locked=True)


@dataclass
class OracleLockTableExpression:
    """Oracle LOCK TABLE statement.
    
    Locks an entire table in exclusive or share mode.
    
    Example SQL:
        LOCK TABLE users IN EXCLUSIVE MODE NOWAIT
        LOCK TABLE users IN SHARE MODE
    
    Attributes:
        table: Table name
        mode: Lock mode ('EXCLUSIVE' or 'SHARE')
        nowait: If True, don't wait for lock
    """
    table: str
    mode: str = "EXCLUSIVE"
    nowait: bool = False
    
    def __post_init__(self):
        if self.mode.upper() not in ("EXCLUSIVE", "SHARE", "ROW EXCLUSIVE", "ROW SHARE"):
            raise ValueError(f"Invalid lock mode: {self.mode}")
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        """Generate LOCK TABLE SQL."""
        sql = f"LOCK TABLE {dialect.format_identifier(self.table)} IN {self.mode.upper()} MODE"
        if self.nowait:
            sql += " NOWAIT"
        return (sql, [])
