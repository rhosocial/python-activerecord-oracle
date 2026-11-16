# src/rhosocial/activerecord/backend/impl/oracle/column_types.py
"""Oracle's own column classes.

A column class is an *operation surface*: it says what a value held in a column
can be asked to do. Core narrows by the Python type (``str`` -> StringColumn,
``dict`` -> JSONColumn), and those classes deliberately promise only what every
backend can execute. Oracle carries value families the framework has no core
class for -- ``ROWID`` / ``UROWID`` -- and richer spellings for the ones it
does (``SDO_GEOMETRY``, ``VECTOR``, the interval types).

This module is where those names live, so a model can choose one explicitly::

    Annotated[SDOGeometry, UseColumnType(OracleSdoGeometryColumn)]

Each class inherits the core class that carries the right operation floor
(``OracleSdoGeometryColumn(GeometryColumn)``), or ``ColumnBase`` when the
family has no core class at all (``ROWID``). Oracle's own operations --
``SDO_*``, the vector distance functions -- are added here as they are
measured; until then the class states the same floor the core class does,
which is the honest minimum rather than a promise the server refuses.

``ORACLE_EXTRA_COLUMN_TYPES`` in :mod:`.mixins.column_type` keys these by the
value class they answer for, so a field annotated with one of Oracle's own
value types resolves without a declaration.
"""

from rhosocial.activerecord.backend.expression.column_types import (
    ColumnBase,
    GeometryColumn,
    IntervalColumn,
    VectorColumn,
    XMLColumn,
)


class OracleSdoGeometryColumn(GeometryColumn):
    """A column holding ``SDO_GEOMETRY``.

    ``SDO_RELATE`` / ``SDO_WITHIN_DISTANCE`` and the rest of the spatial
    operator family belong here; the core class states only the shared column
    surface until they are measured.
    """


class OracleVectorColumn(VectorColumn):
    """A column holding a 23ai+ ``VECTOR``.

    The distance functions (``VECTOR_DISTANCE`` and friends) belong here; the
    core class states only the shared column surface.
    """


class OracleIntervalColumn(IntervalColumn):
    """A column holding ``INTERVAL DAY TO SECOND`` or ``INTERVAL YEAR TO MONTH``.

    Oracle has had these natively since 8i, so the class names the value; the
    interval arithmetic and ``EXTRACT`` spelling are Oracle's and are added
    here as they are measured.
    """


class OracleRowIdColumn(ColumnBase):
    """A column holding ``ROWID`` or ``UROWID``.

    A physical row address is neither a number nor text: it identifies a row
    and nothing else, so the class starts from the column surface.
    """

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this column."""
        return "format_column"


class OracleXmlColumn(XMLColumn):
    """A column holding ``XMLType``.

    Inherits the core XML surface: casting and null tests, but no comparison --
    ``XMLType`` has no equality operator either.
    """


__all__ = [
    "OracleSdoGeometryColumn",
    "OracleVectorColumn",
    "OracleIntervalColumn",
    "OracleRowIdColumn",
    "OracleXmlColumn",
]
