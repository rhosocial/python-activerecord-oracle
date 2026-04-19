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
}
