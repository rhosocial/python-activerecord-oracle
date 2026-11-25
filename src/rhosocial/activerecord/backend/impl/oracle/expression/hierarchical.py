# src/rhosocial/activerecord/backend/impl/oracle/expression/hierarchical.py
"""
Oracle hierarchical query expressions.

Oracle supports hierarchical queries using CONNECT BY clause,
which is unique among major databases. This enables queries
over tree-structured data like organizational hierarchies
or bill of materials.
"""
from __future__ import annotations

from typing import Optional, List, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class ConnectByExpression(BaseExpression):
    """Oracle CONNECT BY clause for hierarchical queries.

    Hierarchical queries retrieve data in a hierarchical order,
    such as organizational structure or bill of materials.

    Example SQL:
        SELECT * FROM employees
        START WITH manager_id IS NULL
        CONNECT BY PRIOR employee_id = manager_id

    Example usage:
        condition = PriorExpression(dialect, column="employee_id")
        connect_by = ConnectByExpression(dialect, condition=condition)

    Args:
        dialect: the Oracle dialect instance.
        condition: The CONNECT BY condition (e.g., PRIOR id = parent_id)
        start_with: Optional START WITH condition
        nocycle: If True, allows cycles in the hierarchy
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        condition: Optional[BaseExpression] = None,
        start_with: Optional[BaseExpression] = None,
        nocycle: bool = False,
    ):
        super().__init__(dialect)
        self.condition = condition
        self.start_with = start_with
        self.nocycle = bool(nocycle)

    @property
    def format_method(self) -> str:
        """The dialect method that assembles the START WITH / CONNECT BY clause."""
        return "format_connect_by_expression"


class PriorExpression(BaseExpression):
    """Oracle PRIOR operator for hierarchical queries.

    The PRIOR operator references the parent row's value in a
    CONNECT BY condition. It's a unary operator that can be
    applied to a column reference.

    Example SQL:
        PRIOR employee_id = manager_id

    Example usage:
        prior_expr = PriorExpression(dialect, column="employee_id")

    Args:
        dialect: the Oracle dialect instance.
        column: The column name to apply PRIOR to
    """

    def __init__(self, dialect: "OracleDialect", column: str = ""):
        super().__init__(dialect)
        self.column = column

    @property
    def format_method(self) -> str:
        """The dialect method that renders this as ``PRIOR <column>``."""
        return "format_prior_expression"


class ConnectByRootExpression(BaseExpression):
    """Oracle CONNECT_BY_ROOT operator for hierarchical queries.

    Returns the root row's value for the specified column.
    This is a unary operator that returns a single value.

    Example SQL:
        SELECT CONNECT_BY_ROOT employee_id AS root_manager
        FROM employees
        CONNECT BY PRIOR employee_id = manager_id

    Example usage:
        root_expr = ConnectByRootExpression(dialect, column="employee_id")

    Args:
        dialect: the Oracle dialect instance.
        column: The column name to get root value for
    """

    def __init__(self, dialect: "OracleDialect", column: str = ""):
        super().__init__(dialect)
        self.column = column

    @property
    def format_method(self) -> str:
        """The dialect method that renders ``CONNECT_BY_ROOT <column>``."""
        return "format_connect_by_root_expression"


class SysConnectByPathExpression(BaseExpression):
    """Oracle SYS_CONNECT_BY_PATH function for hierarchical queries.

    Generates a path string from root to current node, useful for
    displaying hierarchical paths.

    Example SQL:
        SYS_CONNECT_BY_PATH(name, '/') -> '/CEO/VP/Manager'

    Example usage:
        path_expr = SysConnectByPathExpression(dialect, column="name", separator="/")

    Args:
        dialect: the Oracle dialect instance.
        column: The column to include in path
        separator: Path separator character (default: '/')
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        column: str = "",
        separator: str = '/',
    ):
        super().__init__(dialect)
        self.column = column
        self.separator = separator

    @property
    def format_method(self) -> str:
        """The dialect method that renders ``SYS_CONNECT_BY_PATH(col, sep)``."""
        return "format_sys_connect_by_path_expression"


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

    @property
    def format_method(self) -> str:
        """The dialect method that renders the ``CONNECT_BY_ISLEAF`` pseudo-column."""
        return "format_connect_by_is_leaf_expression"


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

    @property
    def format_method(self) -> str:
        """The dialect method that renders the ``CONNECT_BY_ISCYCLE`` pseudo-column."""
        return "format_connect_by_is_cycle_expression"


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
        level_expr = LevelExpression(dialect)
    """

    @property
    def format_method(self) -> str:
        """The dialect method that renders the ``LEVEL`` pseudo-column."""
        return "format_level_expression"


class SiblingsExpression(BaseExpression):
    """Oracle ORDER SIBLINGS BY clause.

    Orders siblings at each level of the hierarchy while
    preserving the hierarchical structure.

    Example SQL:
        SELECT * FROM employees
        CONNECT BY PRIOR employee_id = manager_id
        ORDER SIBLINGS BY name

    Args:
        dialect: the Oracle dialect instance.
        columns: List of column names to order by
        ascending: Sort directions (True for ASC, False for DESC)
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        columns: Optional[List[str]] = None,
        ascending: Optional[List[bool]] = None,
    ):
        super().__init__(dialect)
        self.columns = list(columns) if columns else []
        self.ascending = list(ascending) if ascending is not None else [True]

    @property
    def format_method(self) -> str:
        """The dialect method that renders the ``ORDER SIBLINGS BY`` clause."""
        return "format_siblings_expression"
