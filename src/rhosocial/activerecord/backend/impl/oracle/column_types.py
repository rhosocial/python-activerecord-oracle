# src/rhosocial/activerecord/backend/impl/oracle/column_types.py
"""Model-facing import path for Oracle's own column classes.

A column class is an *operation surface*: it says what a value held in a column
can be asked to do. Core narrows by the Python type (``str`` -> StringColumn,
``dict`` -> JSONColumn), and those classes deliberately promise only what every
backend can execute. Oracle carries value families the framework has no core
class for -- ``ROWID`` / ``UROWID`` -- and richer spellings for the ones it
does (``SDO_GEOMETRY``, ``VECTOR``, the interval types).

The classes are SQL expressions, so they are *defined* in the backend's
``expression`` package (:mod:`.expression.column_types`) and re-exported here
for model declarations::

    Annotated[SDOGeometry, UseColumnType(OracleSdoGeometryColumn)]

``ORACLE_EXTRA_COLUMN_TYPES`` in :mod:`.mixins.column_type` keys these by the
value class they answer for, so a field annotated with one of Oracle's own
value types resolves without a declaration.
"""

from .expression.column_types import (
    OracleIntervalColumn,
    OracleRowIdColumn,
    OracleSdoGeometryColumn,
    OracleVectorColumn,
    OracleXmlColumn,
)

__all__ = [
    "OracleSdoGeometryColumn",
    "OracleVectorColumn",
    "OracleIntervalColumn",
    "OracleRowIdColumn",
    "OracleXmlColumn",
]
