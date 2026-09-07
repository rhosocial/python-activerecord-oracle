# src/rhosocial/activerecord/backend/impl/oracle/ddl_spec.py
"""Oracle-specific DDL feature specs.

Plain declaration objects (no dialect at definition time) recognized by the
Oracle dialect's ``build_spec`` via ``isinstance``. Only the Oracle dialect
claims these specs; every other backend silently ignores them
(``build_spec`` returns ``None``).
"""

from typing import Optional, Sequence, Union

from rhosocial.activerecord.backend.expression.statements.ddl_spec import (
    DDLSpec,
    PartitionSpec,
)


class OraclePartitionBound:
    """A single partition boundary in an Oracle ``PARTITION`` definition.

    ``value`` is a plain scalar (str / int / float / Decimal / date /
    datetime); the special ``"MAXVALUE"`` marker renders as ``MAXVALUE``.
    """

    __slots__ = ("value",)

    def __init__(self, value: Union[str, int, float, "object", None] = None):
        self.value = value

    def __repr__(self) -> str:
        return f"OraclePartitionBound({self.value!r})"


class OraclePartitionDefinitionSpec:
    """A ``PARTITION <name> VALUES ...`` entry for an Oracle partition spec.

    RANGE partitions use ``less_than``; LIST partitions use ``in_values``.
    """

    __slots__ = ("name", "less_than", "in_values")

    def __init__(
        self,
        name: str,
        *,
        less_than: Optional[Sequence[OraclePartitionBound]] = None,
        in_values: Optional[Sequence[Union[OraclePartitionBound, "object"]]] = None,
    ):
        if less_than is not None and in_values is not None:
            raise ValueError("less_than and in_values are mutually exclusive")
        self.name = name
        self.less_than = list(less_than) if less_than is not None else None
        self.in_values = list(in_values) if in_values is not None else None


class OracleRangePartition(PartitionSpec):
    """Oracle ``PARTITION BY RANGE`` declaration.

    Example::

        OracleRangePartition(
            column="created_at",
            partitions=[
                OraclePartitionDefinitionSpec(
                    "p2026", less_than=[OraclePartitionBound(2027)]
                ),
                OraclePartitionDefinitionSpec(
                    "p_max", less_than=[OraclePartitionBound("MAXVALUE")]
                ),
            ],
        )
    """

    __slots__ = ("column", "partitions")

    def __init__(
        self,
        column: str,
        partitions: Optional[Sequence[OraclePartitionDefinitionSpec]] = None,
    ):
        if not column:
            raise ValueError("OracleRangePartition requires a partition column")
        self.column = column
        self.partitions = list(partitions) if partitions is not None else None


class OracleListPartition(PartitionSpec):
    """Oracle ``PARTITION BY LIST`` declaration."""

    __slots__ = ("column", "partitions")

    def __init__(
        self,
        column: str,
        partitions: Optional[Sequence[OraclePartitionDefinitionSpec]] = None,
    ):
        if not column:
            raise ValueError("OracleListPartition requires a partition column")
        self.column = column
        self.partitions = list(partitions) if partitions is not None else None


class OracleHashPartition(PartitionSpec):
    """Oracle ``PARTITION BY HASH`` declaration (optionally with partition count)."""

    __slots__ = ("column", "partitions_count")

    def __init__(self, column: str, partitions_count: Optional[int] = None):
        if not column:
            raise ValueError("OracleHashPartition requires a partition column")
        self.column = column
        self.partitions_count = partitions_count


class OracleIntervalPartition(PartitionSpec):
    """Oracle ``PARTITION BY RANGE ... INTERVAL`` declaration.

    ``interval`` is an interval function expression — either a ready
    ``BaseExpression`` (e.g. ``OracleIntervalFunctionExpression``) or a lazy
    factory ``(dialect) -> BaseExpression``. The convenience factory
    :meth:`monthly` / :meth:`yearly` / :meth:`daily` covers the common cases.
    """

    __slots__ = ("column", "interval")

    def __init__(self, column: str, interval: "object"):
        if not column:
            raise ValueError("OracleIntervalPartition requires a partition column")
        if interval is None or (callable(interval) and not hasattr(interval, "to_sql") and interval is None):
            raise ValueError("OracleIntervalPartition requires an interval expression")
        self.column = column
        self.interval = interval

    @classmethod
    def monthly(cls, column: str, amount: int = 1) -> "OracleIntervalPartition":
        """``NUMTOYMINTERVAL(amount, 'MONTH')`` interval partitioning."""

        def _factory(dialect):
            from .expression.partition import OracleIntervalFunctionExpression

            return OracleIntervalFunctionExpression(
                dialect, "NUMTOYMINTERVAL", amount, "MONTH"
            )

        return cls(column, _factory)

    @classmethod
    def yearly(cls, column: str, amount: int = 1) -> "OracleIntervalPartition":
        """``NUMTOYMINTERVAL(amount, 'YEAR')`` interval partitioning."""

        def _factory(dialect):
            from .expression.partition import OracleIntervalFunctionExpression

            return OracleIntervalFunctionExpression(
                dialect, "NUMTOYMINTERVAL", amount, "YEAR"
            )

        return cls(column, _factory)

    @classmethod
    def daily(cls, column: str, amount: int = 1) -> "OracleIntervalPartition":
        """``NUMTODSINTERVAL(amount, 'DAY')`` interval partitioning."""

        def _factory(dialect):
            from .expression.partition import OracleIntervalFunctionExpression

            return OracleIntervalFunctionExpression(
                dialect, "NUMTODSINTERVAL", amount, "DAY"
            )

        return cls(column, _factory)


class OracleSequenceDefault(DDLSpec):
    """An Oracle sequence-backed column default (``seq.NEXTVAL``).

    Example::

        OracleSequenceDefault(column="id", sequence="users_id_seq")

    Renders as ``DEFAULT users_id_seq.NEXTVAL``.
    """

    __slots__ = ("column", "sequence")

    def __init__(self, column: str, sequence: Optional[str] = None):
        if not column:
            raise ValueError("OracleSequenceDefault requires a column name")
        self.column = column
        self.sequence = sequence