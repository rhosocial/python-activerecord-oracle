# explain/types.py
"""
Oracle EXPLAIN PLAN result type definitions.

This module provides dataclasses for representing Oracle EXPLAIN PLAN output
in a structured, tree-like format for easy navigation and analysis.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class OracleExplainRow:
    """Represents a single row in Oracle EXPLAIN PLAN output.
    
    Oracle's EXPLAIN PLAN provides detailed information about how the
    optimizer plans to execute a SQL statement. Each row represents
    an operation in the execution plan tree.
    
    Attributes:
        id: The unique identifier for this operation (PLAN_ID)
        operation: The operation being performed (e.g., TABLE ACCESS, INDEX SCAN)
        options: Additional options for the operation (e.g., FULL, RANGE SCAN)
        object_name: The name of the table, index, or other object
        object_type: The type of object (e.g., TABLE, INDEX)
        object_owner: The owner of the object
        optimizer: The optimizer mode used
        parent_id: The ID of the parent operation (for tree structure)
        depth: The depth level in the execution tree
        cost: The estimated cost of the operation
        cardinality: The estimated number of rows
        bytes: The estimated number of bytes
        time: The estimated execution time
        partition_start: Start partition for partition operations
        partition_stop: End partition for partition operations
        partition_id: Partition identifier
        predicate_info: FILTER_PREDICATES or ACCESS_PREDICATES
        projection: Projected columns
        other: Other information
        other_tag: Tag for other information
        other_xml: XML representation of additional plan information
        children: List of child operations (populated after tree construction)
    """
    id: int
    operation: str
    options: Optional[str] = None
    object_name: Optional[str] = None
    object_type: Optional[str] = None
    object_owner: Optional[str] = None
    optimizer: Optional[str] = None
    parent_id: Optional[int] = None
    depth: int = 0
    cost: Optional[float] = None
    cardinality: Optional[int] = None
    bytes: Optional[int] = None
    time: Optional[str] = None
    partition_start: Optional[str] = None
    partition_stop: Optional[str] = None
    partition_id: Optional[int] = None
    predicate_info: Optional[str] = None
    projection: Optional[str] = None
    other: Optional[str] = None
    other_tag: Optional[str] = None
    other_xml: Optional[str] = None
    children: List['OracleExplainRow'] = field(default_factory=list)
    
    def is_full_scan(self) -> bool:
        """Check if this operation is a full table scan."""
        return (
            self.operation == "TABLE ACCESS" and 
            self.options == "FULL"
        )
    
    def is_index_scan(self) -> bool:
        """Check if this operation uses an index."""
        return (
            self.operation == "INDEX" or
            (self.operation == "TABLE ACCESS" and 
             self.options in ("BY INDEX ROWID", "BY USER ROWID"))
        )
    
    def is_join(self) -> bool:
        """Check if this operation is a join."""
        return self.operation in (
            "NESTED LOOPS", "HASH JOIN", "MERGE JOIN", 
            "JOIN FILTER", "SEMI JOIN", "ANTI JOIN"
        )
    
    def get_total_cost(self) -> float:
        """Calculate total cost including children."""
        total = self.cost or 0
        for child in self.children:
            total += child.get_total_cost()
        return total
    
    def find_operations(self, operation_type: str) -> List['OracleExplainRow']:
        """Find all operations of a specific type in subtree.
        
        Args:
            operation_type: The operation type to search for
            
        Returns:
            List of matching OracleExplainRow instances
        """
        results = []
        if self.operation == operation_type:
            results.append(self)
        for child in self.children:
            results.extend(child.find_operations(operation_type))
        return results
    
    def find_table_accesses(self, table_name: str) -> List['OracleExplainRow']:
        """Find all table access operations for a specific table.
        
        Args:
            table_name: The table name to search for
            
        Returns:
            List of matching OracleExplainRow instances
        """
        results = []
        if (self.operation == "TABLE ACCESS" and 
            self.object_name and 
            self.object_name.upper() == table_name.upper()):
            results.append(self)
        for child in self.children:
            results.extend(child.find_table_accesses(table_name))
        return results
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'id': self.id,
            'operation': self.operation,
            'options': self.options,
            'object_name': self.object_name,
            'object_type': self.object_type,
            'cost': self.cost,
            'cardinality': self.cardinality,
            'bytes': self.bytes,
            'time': self.time,
            'children': [c.to_dict() for c in self.children]
        }


@dataclass
class OracleExplainResult:
    """Represents the complete EXPLAIN PLAN result.
    
    This class holds the complete execution plan for a SQL statement,
    including metadata and the tree structure of operations.
    
    Attributes:
        statement_id: The identifier for this statement plan
        plan_id: The unique plan identifier (Oracle 12c+)
        timestamp: When the plan was generated
        remarks: Any remarks associated with the plan
        rows: Flat list of all operation rows
        root: The root operation (after tree construction)
    """
    statement_id: str
    plan_id: Optional[int] = None
    timestamp: Optional[str] = None
    remarks: Optional[str] = None
    rows: List[OracleExplainRow] = field(default_factory=list)
    root: Optional[OracleExplainRow] = None
    
    def build_tree(self) -> 'OracleExplainResult':
        """Build the tree structure from flat rows.
        
        Constructs parent-child relationships based on parent_id
        and sets the root operation.
        
        Returns:
            self for method chaining
        """
        if not self.rows:
            return self
        
        # Build lookup by id
        lookup: Dict[int, OracleExplainRow] = {r.id: r for r in self.rows}
        
        # Build tree
        for row in self.rows:
            if row.parent_id is not None and row.parent_id in lookup:
                lookup[row.parent_id].children.append(row)
        
        # Find root (parent_id is None or 0)
        self.root = next(
            (r for r in self.rows if r.parent_id is None or r.parent_id == 0),
            self.rows[0] if self.rows else None
        )
        
        return self
    
    def get_operation_count(self) -> int:
        """Return total number of operations."""
        return len(self.rows)
    
    def find_operations(self, operation_type: str) -> List[OracleExplainRow]:
        """Find all operations of a specific type.
        
        Args:
            operation_type: The operation type to search for
            
        Returns:
            List of matching OracleExplainRow instances
        """
        return [r for r in self.rows if r.operation == operation_type]
    
    def find_full_scans(self) -> List[OracleExplainRow]:
        """Find all full table scan operations."""
        return [r for r in self.rows if r.is_full_scan()]
    
    def find_index_scans(self) -> List[OracleExplainRow]:
        """Find all index scan operations."""
        return [r for r in self.rows if r.is_index_scan()]
    
    def find_joins(self) -> List[OracleExplainRow]:
        """Find all join operations."""
        return [r for r in self.rows if r.is_join()]
    
    def get_total_cost(self) -> float:
        """Get the total estimated cost."""
        if self.root:
            return self.root.get_total_cost()
        return sum(r.cost or 0 for r in self.rows)
    
    def has_full_scan(self) -> bool:
        """Check if plan contains any full table scans."""
        return any(r.is_full_scan() for r in self.rows)
    
    def get_tables_accessed(self) -> List[str]:
        """Get list of all tables accessed in the plan."""
        tables = set()
        for r in self.rows:
            if r.operation == "TABLE ACCESS" and r.object_name:
                tables.add(r.object_name)
        return sorted(tables)
    
    def get_indexes_used(self) -> List[str]:
        """Get list of all indexes used in the plan."""
        indexes = set()
        for r in self.rows:
            if r.operation == "INDEX" and r.object_name:
                indexes.add(r.object_name)
        return sorted(indexes)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the execution plan.
        
        Returns:
            Dictionary with plan summary statistics
        """
        return {
            'operation_count': self.get_operation_count(),
            'total_cost': self.get_total_cost(),
            'full_scans': len(self.find_full_scans()),
            'index_scans': len(self.find_index_scans()),
            'joins': len(self.find_joins()),
            'tables_accessed': self.get_tables_accessed(),
            'indexes_used': self.get_indexes_used(),
        }
    
    @classmethod
    def from_rows(cls, rows: List[Dict[str, Any]], 
                  statement_id: str = "DEFAULT") -> 'OracleExplainResult':
        """Create OracleExplainResult from raw query results.
        
        Args:
            rows: List of dictionaries from EXPLAIN PLAN query
            statement_id: The statement identifier
            
        Returns:
            New OracleExplainResult instance
        """
        explain_rows = []
        for row in rows:
            explain_row = OracleExplainRow(
                id=row.get('ID', 0),
                operation=row.get('OPERATION', ''),
                options=row.get('OPTIONS'),
                object_name=row.get('OBJECT_NAME'),
                object_type=row.get('OBJECT_TYPE'),
                object_owner=row.get('OBJECT_OWNER'),
                optimizer=row.get('OPTIMIZER'),
                parent_id=row.get('PARENT_ID'),
                depth=row.get('DEPTH', 0),
                cost=row.get('COST'),
                cardinality=row.get('CARDINALITY'),
                bytes=row.get('BYTES'),
                time=row.get('TIME'),
                partition_start=row.get('PARTITION_START'),
                partition_stop=row.get('PARTITION_STOP'),
                partition_id=row.get('PARTITION_ID'),
                predicate_info=row.get('PREDICATE_INFO') or row.get('ACCESS_PREDICATES') or row.get('FILTER_PREDICATES'),
                projection=row.get('PROJECTION'),
                other=row.get('OTHER'),
                other_tag=row.get('OTHER_TAG'),
                other_xml=row.get('OTHER_XML'),
            )
            explain_rows.append(explain_row)
        
        result = cls(statement_id=statement_id, rows=explain_rows)
        result.build_tree()
        return result
