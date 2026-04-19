# expression/hierarchical.py
"""
Oracle hierarchical query expressions.

Oracle supports hierarchical queries using CONNECT BY clause,
which is unique among major databases. This enables queries
over tree-structured data like organizational hierarchies
or bill of materials.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Any
from rhosocial.activerecord.backend.expression.bases import BaseExpression


@dataclass
class ConnectByExpression(BaseExpression):
    """Oracle CONNECT BY clause for hierarchical queries.
    
    Hierarchical queries retrieve data in a hierarchical order,
    such as organizational structure or bill of materials.
    
    Example SQL:
        SELECT * FROM employees
        START WITH manager_id IS NULL
        CONNECT BY PRIOR employee_id = manager_id
    
    Example usage:
        from rhosocial.activerecord.backend.expression import CompareExpression
        condition = CompareExpression(
            left=PriorExpression(column="employee_id"),
            operator="=",
            right="manager_id"
        )
        connect_by = ConnectByExpression(
            condition=condition,
            start_with=CompareExpression("manager_id", "IS", "NULL")
        )
    
    Attributes:
        condition: The CONNECT BY condition (e.g., PRIOR id = parent_id)
        start_with: Optional START WITH condition
        nocycle: If True, allows cycles in the hierarchy
    """
    condition: BaseExpression = None
    start_with: Optional[BaseExpression] = None
    nocycle: bool = False
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        """Generate SQL for CONNECT BY clause."""
        parts = []
        params = []
        
        if self.start_with:
            start_sql, start_params = self.start_with.to_sql(dialect)
            parts.append(f"START WITH {start_sql}")
            params.extend(start_params)
        
        nocycle_str = "NOCYCLE " if self.nocycle else ""
        if self.condition:
            cond_sql, cond_params = self.condition.to_sql(dialect)
            parts.append(f"CONNECT BY {nocycle_str}{cond_sql}")
            params.extend(cond_params)
        
        return (" ".join(parts), params)


@dataclass
class PriorExpression(BaseExpression):
    """Oracle PRIOR operator for hierarchical queries.
    
    The PRIOR operator references the parent row's value in a
    CONNECT BY condition. It's a unary operator that can be
    applied to a column reference.
    
    Example SQL:
        PRIOR employee_id = manager_id
    
    Example usage:
        prior_expr = PriorExpression(column="employee_id")
    
    Attributes:
        column: The column name to apply PRIOR to
    """
    column: str = ""
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        return (f"PRIOR {dialect.format_identifier(self.column)}", [])


@dataclass
class ConnectByRootExpression(BaseExpression):
    """Oracle CONNECT_BY_ROOT operator for hierarchical queries.
    
    Returns the root row's value for the specified column.
    This is a unary operator that returns a single value.
    
    Example SQL:
        SELECT CONNECT_BY_ROOT employee_id AS root_manager
        FROM employees
        CONNECT BY PRIOR employee_id = manager_id
    
    Example usage:
        root_expr = ConnectByRootExpression(column="employee_id")
    
    Attributes:
        column: The column name to get root value for
    """
    column: str = ""
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        return (f"CONNECT_BY_ROOT {dialect.format_identifier(self.column)}", [])


@dataclass
class SysConnectByPathExpression(BaseExpression):
    """Oracle SYS_CONNECT_BY_PATH function for hierarchical queries.
    
    Generates a path string from root to current node, useful for
    displaying hierarchical paths.
    
    Example SQL:
        SYS_CONNECT_BY_PATH(name, '/') -> '/CEO/VP/Manager'
    
    Example usage:
        path_expr = SysConnectByPathExpression(column="name", separator="/")
    
    Attributes:
        column: The column to include in path
        separator: Path separator character (default: '/')
    """
    column: str = ""
    separator: str = '/'
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        return (
            f"SYS_CONNECT_BY_PATH({dialect.format_identifier(self.column)}, '{self.separator}')",
            []
        )


@dataclass
class ConnectByIsLeafExpression(BaseExpression):
    """Oracle CONNECT_BY_ISLEAF pseudo-column.
    
    Returns 1 if the current row is a leaf in the hierarchy
    (has no children), 0 otherwise. Useful for filtering
    leaf nodes.
    
    Example SQL:
        SELECT * FROM employees
        WHERE CONNECT_BY_ISLEAF = 1
        CONNECT BY PRIOR employee_id = manager_id
    """
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        return ("CONNECT_BY_ISLEAF", [])


@dataclass
class ConnectByIsCycleExpression(BaseExpression):
    """Oracle CONNECT_BY_ISCYCLE pseudo-column.
    
    Returns 1 if the current row has a child that is also
    its ancestor (creating a cycle). Requires NOCYCLE in
    CONNECT BY clause.
    
    Example SQL:
        SELECT employee_id, CONNECT_BY_ISCYCLE
        FROM employees
        CONNECT BY NOCYCLE PRIOR employee_id = manager_id
    """
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        return ("CONNECT_BY_ISCYCLE", [])


@dataclass
class LevelExpression(BaseExpression):
    """Oracle LEVEL pseudo-column for hierarchical queries.
    
    Returns the level number of the current row in the hierarchy.
    Root rows have LEVEL = 1, their children have LEVEL = 2, etc.
    Useful for indentation or limiting depth.
    
    Example SQL:
        SELECT employee_id, LEVEL
        FROM employees
        CONNECT BY PRIOR employee_id = manager_id
        START WITH manager_id IS NULL
    
    Example usage:
        level_expr = LevelExpression()
    """
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        return ("LEVEL", [])


@dataclass
class SiblingsExpression(BaseExpression):
    """Oracle ORDER SIBLINGS BY clause.
    
    Orders siblings at each level of the hierarchy while
    preserving the hierarchical structure.
    
    Example SQL:
        SELECT * FROM employees
        CONNECT BY PRIOR employee_id = manager_id
        ORDER SIBLINGS BY name
    
    Attributes:
        columns: List of column names to order by
        ascending: Sort direction (True for ASC, False for DESC)
    """
    columns: List[str] = field(default_factory=list)
    ascending: List[bool] = field(default_factory=lambda: [True])
    
    def to_sql(self, dialect) -> Tuple[str, List[Any]]:
        if not self.columns:
            return ("", [])
        
        order_parts = []
        for i, col in enumerate(self.columns):
            asc = self.ascending[i] if i < len(self.ascending) else True
            order_parts.append(f"{dialect.format_identifier(col)} {'ASC' if asc else 'DESC'}")
        
        return (f"ORDER SIBLINGS BY {', '.join(order_parts)}", [])
