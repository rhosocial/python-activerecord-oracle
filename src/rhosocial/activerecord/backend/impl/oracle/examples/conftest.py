"""
Example metadata configuration.

This file defines metadata for all examples in this directory.
The inspector reads this file to get title, dialect_protocols, and priority.

Oracle Version Support:
- Minimum: 12c
- Maximum: 23ai

Version-specific features:
- Native JSON type: Oracle 21c+
- VECTOR type: Oracle 23ai+
- FETCH FIRST pagination: Oracle 12c+
- PIVOT/UNPIVOT: Oracle 11g+
- FOR UPDATE SKIP LOCKED: Oracle 11g+
- SDO_GEOMETRY: Oracle 10g+ with Spatial option
"""

EXAMPLES_META = {
    'connection/quickstart.py': {
        'title': 'Connect to Oracle and Execute Queries',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '12c',
        'max_version': '23ai',
    },
    'types/basic_types.py': {
        'title': 'Oracle Type System',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'VECTOR type requires Oracle 23ai+',
    },
    'expression/basic_expressions.py': {
        'title': 'Oracle-Specific Expressions',
        'dialect_protocols': [
            'HierarchicalQuerySupport',
            'PivotSupport',
            'QueryHintSupport',
        ],
        'priority': 10,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'Hierarchical queries, PIVOT, and hints available in all versions',
    },
    'insert/basic.py': {
        'title': 'INSERT Statements',
        'dialect_protocols': ['DMLMixin'],
        'priority': 20,
        'min_version': '12c',
        'max_version': '23ai',
    },
    'insert/upsert.py': {
        'title': 'MERGE-based Upsert (Oracle-style)',
        'dialect_protocols': ['MergeSupport'],
        'priority': 25,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'Oracle uses MERGE INTO ... USING ... since 10g (no native ON CONFLICT)',
    },
    'update/basic.py': {
        'title': 'UPDATE Statements',
        'dialect_protocols': ['DMLMixin'],
        'priority': 21,
        'min_version': '12c',
        'max_version': '23ai',
    },
    'delete/basic.py': {
        'title': 'DELETE Statements',
        'dialect_protocols': ['DMLMixin'],
        'priority': 22,
        'min_version': '12c',
        'max_version': '23ai',
    },
    'query/basic.py': {
        'title': 'Basic SELECT and Expression Queries',
        'dialect_protocols': ['DQLMixin'],
        'priority': 15,
        'min_version': '12c',
        'max_version': '23ai',
    },
    'query/join.py': {
        'title': 'JOIN Queries (INNER/OUTER/CROSS)',
        'dialect_protocols': ['JoinSupport'],
        'priority': 26,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'Oracle 9i+ supports ANSI join syntax',
    },
    'query/aggregate.py': {
        'title': 'Aggregate and GROUP BY Queries',
        'dialect_protocols': ['DQLMixin'],
        'priority': 18,
        'min_version': '12c',
        'max_version': '23ai',
    },
    'query/cte.py': {
        'title': 'WITH / Common Table Expressions',
        'dialect_protocols': ['CTESupport'],
        'priority': 27,
        'min_version': '12c',
        'max_version': '23ai',
    },
    'query/pagination.py': {
        'title': 'Pagination via OFFSET / FETCH FIRST',
        'dialect_protocols': ['PaginationSupport'],
        'priority': 28,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'Oracle 12c+ supports OFFSET n ROWS and FETCH FIRST n ROWS ONLY',
    },
    'query/hierarchy.py': {
        'title': 'Oracle Hierarchical Queries (START WITH / CONNECT BY)',
        'dialect_protocols': ['HierarchicalQuerySupport'],
        'priority': 12,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'Oracle-specific CONNECT BY syntax; LEVEL, SYS_CONNECT_BY_PATH, CONNECT_BY_ROOT.',
    },
    'query/pivot.py': {
        'title': 'Oracle PIVOT / UNPIVOT',
        'dialect_protocols': ['PivotSupport'],
        'priority': 13,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'PIVOT/UNPIVOT available since Oracle 11g.',
    },
    'query/listagg.py': {
        'title': 'Oracle LISTAGG / WITHIN GROUP / PERCENTILE aggregates',
        'dialect_protocols': ['OrderedSetAggregationSupport'],
        'priority': 14,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'LISTAGG 11gR2+ ; PERCENTILE_CONT/DISC ordered-set aggregates.',
    },
    'transaction/basic.py': {
        'title': 'Transaction Context Manager',
        'dialect_protocols': ['TransactionControlSupport'],
        'priority': 16,
        'min_version': '12c',
        'max_version': '23ai',
    },
    'transaction/for_update.py': {
        'title': 'Oracle-enhanced FOR UPDATE (NOWAIT / WAIT / SKIP LOCKED)',
        'dialect_protocols': ['LockingSupport'],
        'priority': 17,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'FOR UPDATE SKIP LOCKED available since Oracle 11g.',
    },
    'types/json_basic.py': {
        'title': 'Oracle JSON Functions (JSON_VALUE / JSON_QUERY / JSON_TABLE)',
        'dialect_protocols': ['JSONSupport'],
        'priority': 11,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'JSON_TABLE since Oracle 12c; native JSON datatype since 21c.',
    },
    'types/spatial_basic.py': {
        'title': 'Oracle Spatial (SDO_GEOMETRY / SDO_RELATE)',
        'dialect_protocols': [],
        'priority': 30,
        'min_version': '12c',
        'max_version': '23ai',
        'note': 'Requires Oracle Spatial option.',
    },
    'types/vector_basic.py': {
        'title': 'Oracle 23ai VECTOR Type and Distance Metrics',
        'dialect_protocols': [],
        'priority': 31,
        'min_version': '23ai',
        'max_version': '23ai',
        'note': 'Requires Oracle 23ai (FREEPDB1 in the reference scenario).',
    },
}
