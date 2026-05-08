# types/constants.py
"""
Oracle type constants for DDL generation.

This module defines constants for Oracle data types used in
DDL generation and type mapping.
"""

from typing import Dict, Type, Any

# Numeric types
NUMBER = "NUMBER"
FLOAT = "FLOAT"
BINARY_FLOAT = "BINARY_FLOAT"
BINARY_DOUBLE = "BINARY_DOUBLE"

# Character types
VARCHAR2 = "VARCHAR2"
NVARCHAR2 = "NVARCHAR2"
CHAR = "CHAR"
NCHAR = "NCHAR"
CLOB = "CLOB"
NCLOB = "NCLOB"
LONG = "LONG"

# Date/Time types
DATE = "DATE"
TIMESTAMP = "TIMESTAMP"
TIMESTAMP_WITH_TZ = "TIMESTAMP WITH TIME ZONE"
TIMESTAMP_WITH_LTZ = "TIMESTAMP WITH LOCAL TIME ZONE"
INTERVAL_YEAR_TO_MONTH = "INTERVAL YEAR TO MONTH"
INTERVAL_DAY_TO_SECOND = "INTERVAL DAY TO SECOND"

# Binary types
BLOB = "BLOB"
BFILE = "BFILE"
RAW = "RAW"
LONG_RAW = "LONG RAW"

# Row ID types
ROWID_TYPE = "ROWID"
UROWID_TYPE = "UROWID"

# Boolean type (23ai+)
BOOLEAN = "BOOLEAN"

# JSON type (21c+)
JSON_TYPE = "JSON"

# XML type
XMLTYPE = "XMLTYPE"

# Spatial type
SDO_GEOMETRY = "SDO_GEOMETRY"

# Vector type (23ai+)
VECTOR = "VECTOR"

# Size limits
VARCHAR2_MAX_SIZE = 4000
NVARCHAR2_MAX_SIZE = 2000
CHAR_MAX_SIZE = 2000
NCHAR_MAX_SIZE = 1000
RAW_MAX_SIZE = 2000

# Default precision/scale
NUMBER_DEFAULT_PRECISION = 38
NUMBER_DEFAULT_SCALE = 0
TIMESTAMP_DEFAULT_PRECISION = 6
INTERVAL_DAY_PRECISION = 2
INTERVAL_SECOND_PRECISION = 6

# Python to Oracle type mapping
PYTHON_TO_ORACLE_TYPE: Dict[Type, str] = {
    bool: NUMBER,  # Will be NUMBER(1) or BOOLEAN (23ai+)
    int: NUMBER,
    float: BINARY_DOUBLE,
    str: VARCHAR2,
    bytes: BLOB,
}

# More specific Python type to Oracle type mapping with sizes
PYTHON_TO_ORACLE_DDL: Dict[Type, Dict[str, Any]] = {
    bool: {"type": NUMBER, "precision": 1},
    int: {"type": NUMBER},
    float: {"type": BINARY_DOUBLE},
    str: {"type": VARCHAR2, "size": VARCHAR2_MAX_SIZE},
    bytes: {"type": BLOB},
}

# Oracle type categories
NUMERIC_TYPES = {NUMBER, FLOAT, BINARY_FLOAT, BINARY_DOUBLE}
CHARACTER_TYPES = {VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, NCLOB, LONG}
DATETIME_TYPES = {DATE, TIMESTAMP, TIMESTAMP_WITH_TZ, TIMESTAMP_WITH_LTZ}
INTERVAL_TYPES = {INTERVAL_YEAR_TO_MONTH, INTERVAL_DAY_TO_SECOND}
BINARY_TYPES = {BLOB, BFILE, RAW, LONG_RAW}
LOB_TYPES = {CLOB, NCLOB, BLOB, BFILE}

# Types that support DEFAULT values in DDL
DEFAULT_SUPPORTED = NUMERIC_TYPES | CHARACTER_TYPES | DATETIME_TYPES | {BOOLEAN}


def get_oracle_type(python_type: Type, size: int = None,
                    precision: int = None, scale: int = None) -> str:
    """Generate Oracle DDL type from Python type.
    
    Args:
        python_type: Python type to convert
        size: Size for VARCHAR2, CHAR, etc.
        precision: Precision for NUMBER, TIMESTAMP, etc.
        scale: Scale for NUMBER
        
    Returns:
        Oracle DDL type string
    """
    base_type = PYTHON_TO_ORACLE_TYPE.get(python_type, VARCHAR2)
    
    if base_type == NUMBER:
        if precision is not None:
            if scale is not None:
                return f"NUMBER({precision}, {scale})"
            return f"NUMBER({precision})"
        return NUMBER
    
    if base_type in (VARCHAR2, NVARCHAR2, CHAR, NCHAR, RAW):
        size = size or (VARCHAR2_MAX_SIZE if base_type == VARCHAR2 else
                       NVARCHAR2_MAX_SIZE if base_type == NVARCHAR2 else
                       CHAR_MAX_SIZE if base_type == CHAR else
                       NCHAR_MAX_SIZE if base_type == NCHAR else
                       RAW_MAX_SIZE)
        return f"{base_type}({size})"
    
    if base_type == TIMESTAMP:
        precision = precision or TIMESTAMP_DEFAULT_PRECISION
        return f"TIMESTAMP({precision})"
    
    if base_type in LOB_TYPES:
        return f"{base_type} BASICFILE"
    
    return base_type
