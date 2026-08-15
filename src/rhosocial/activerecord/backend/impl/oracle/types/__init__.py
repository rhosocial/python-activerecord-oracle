# types/__init__.py
"""
Oracle-specific type definitions.

This module provides Python type definitions for Oracle-specific data types,
enabling proper type handling and conversion between Python and Oracle.
"""

from .interval import IntervalYearToMonth, IntervalDayToSecond
from .rowid import OracleRowID, OracleURowID, parse_rowid
from .xml import OracleXMLType
from .spatial import SDOGeometry, SDOPoint, SDOGeometryType
from .vector import OracleVector
from .constants import *

__all__ = [
    # Interval types
    'IntervalYearToMonth', 'IntervalDayToSecond',
    # ROWID types
    'OracleRowID', 'OracleURowID', 'parse_rowid',
    # XML type
    'OracleXMLType',
    # Spatial types
    'SDOGeometry', 'SDOPoint', 'SDOGeometryType',
    # Vector type (23ai+)
    'OracleVector',
]
