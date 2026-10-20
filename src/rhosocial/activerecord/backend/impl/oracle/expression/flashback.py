# src/rhosocial/activerecord/backend/impl/oracle/expression/flashback.py
"""Oracle FLASHBACK family expressions.

This module defines the backend-specific expressions for Oracle's flashback
technology, available since Oracle 10g:

* ``OracleAsOfClause`` — the ``AS OF { SCN | TIMESTAMP } ...`` clause that
  attaches to a table reference in a ``SELECT`` so the table is read as it
  existed at a past point in time.
* ``OracleVersionsBetweenClause`` — the ``VERSIONS BETWEEN ...`` clause used
  to query the versions of a row over a time/SCN range.
* ``OracleFlashbackTableExpression`` — the ``FLASHBACK TABLE`` statement that
  restores a dropped table from the recycle bin (``TO BEFORE DROP``) or
  rewinds a table to a past SCN/TIMESTAMP.
* ``OraclePurgeExpression`` — the ``PURGE`` statement that permanently
  removes a table/index from the recycle bin, or empties the whole recycle
  bin (``PURGE RECYCLEBIN``).

All expressions delegate SQL generation to the dialect through the public
``format_*`` formatters implemented by ``OracleFlashbackMixin``.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Optional, TYPE_CHECKING, Union

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import Index, Table

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleAsOfMode(str, Enum):
    """Oracle ``AS OF`` clause point-in-time modes."""

    TIMESTAMP = "TIMESTAMP"
    SCN = "SCN"

    #: A member *is* the word it carries, so ``str()`` and f-string
    #: interpolation yield the word rather than ``OracleAsOfMode.TIMESTAMP``.
    #: ``(str, Enum)`` alone does not give that -- ``Enum.__str__`` wins over
    #: the mixin -- so it is stated once here. The word is what a plain-JSON
    #: artifact can encode and what a formatter spells.
    __str__ = str.__str__


class OracleVersionsBetweenMode(str, Enum):
    """Oracle ``VERSIONS BETWEEN`` clause range modes."""

    TIMESTAMP = "TIMESTAMP"
    SCN = "SCN"

    __str__ = str.__str__


class OracleAsOfClause(BaseExpression):
    """Oracle ``AS OF { SCN | TIMESTAMP } ...`` flashback query clause.

    Attaches to a table reference in a ``SELECT`` -- as the ``flashback``
    argument of an
    :class:`~...impl.oracle.expression.sources.OracleNamedRelationRef` -- to
    read the table as it was at a specified SCN or timestamp. It is a clause
    on the statement, not part of the table's name.

    Args:
        dialect: the Oracle dialect instance.
        mode: ``TIMESTAMP`` or ``SCN`` -- the member, or the word it carries.
        value: the timestamp/SCN value. Accepts a ``BaseExpression`` (rendered
            via ``to_sql()``) or a raw SQL fragment string, e.g.
            ``"SYSTIMESTAMP - INTERVAL '1' DAY"``.

    Raises:
        ValueError: if ``mode`` names no member.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        mode: OracleAsOfMode,
        value: Any,
    ):
        super().__init__(dialect)
        self.mode = normalize_as_of_mode(mode)
        self.value = value

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_as_of_clause"


class OracleVersionsBetweenClause(BaseExpression):
    """Oracle ``VERSIONS BETWEEN { SCN | TIMESTAMP } ... AND ...`` clause.

    Attaches to a table reference in a ``SELECT`` to return every version of
    a row over the requested time/SCN range.

    Args:
        dialect: the Oracle dialect instance.
        mode: ``TIMESTAMP`` or ``SCN`` -- the member, or the word it carries.
        low_value: the lower bound of the range.
        high_value: the upper bound of the range.

    Raises:
        ValueError: if ``mode`` names no member.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        mode: OracleVersionsBetweenMode,
        low_value: Any,
        high_value: Any,
    ):
        super().__init__(dialect)
        self.mode = normalize_versions_between_mode(mode)
        self.low_value = low_value
        self.high_value = high_value

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_versions_between_clause"


class OracleFlashbackTableExpression(BaseExpression):
    """Oracle ``FLASHBACK TABLE ...`` statement expression.

    Restores a dropped table from the recycle bin (``TO BEFORE DROP``,
    optionally with ``RENAME TO``) or rewinds a table to a past SCN or
    timestamp. ``ENABLE TRIGGERS`` / ``DISABLE TRIGGERS`` control whether
    the table's triggers fire during the flashback.

    Args:
        dialect: the Oracle dialect instance.
        table: the table to flash back. The object carries its owner, so the
            name is rendered by the dialect rather than concatenated here.
        to_scn: restore to this SCN.
        to_timestamp: restore to this timestamp (a ``BaseExpression`` or raw
            SQL fragment string).
        to_before_drop: restore the table from the recycle bin.
        rename_to: with ``to_before_drop``, rename the restored table.
        enable_triggers: append ``ENABLE TRIGGERS``.
        disable_triggers: append ``DISABLE TRIGGERS``.

    Raises:
        TypeError: if ``table`` is not a :class:`Table`.
        ValueError: if no flashback target clause is
            supplied, if ``rename_to`` is used without ``to_before_drop``, or
            if ``enable_triggers`` and ``disable_triggers`` are both set.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: Table,
        to_scn: Optional[int] = None,
        to_timestamp: Any = None,
        to_before_drop: bool = False,
        rename_to: Optional[str] = None,
        enable_triggers: bool = False,
        disable_triggers: bool = False,
    ):
        super().__init__(dialect)
        if not isinstance(table, Table):
            raise TypeError(
                f"table must be a Table, got {type(table).__name__}"
            )
        targets = sum(
            [
                to_scn is not None,
                to_timestamp is not None,
                bool(to_before_drop),
            ]
        )
        if targets != 1:
            raise ValueError(
                "exactly one of to_scn, to_timestamp or to_before_drop must be set"
            )
        if rename_to is not None and not to_before_drop:
            raise ValueError("rename_to requires to_before_drop")
        if enable_triggers and disable_triggers:
            raise ValueError("enable_triggers and disable_triggers are mutually exclusive")
        self.table = table
        self.to_scn = to_scn
        self.to_timestamp = to_timestamp
        self.to_before_drop = bool(to_before_drop)
        self.rename_to = rename_to
        self.enable_triggers = bool(enable_triggers)
        self.disable_triggers = bool(disable_triggers)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_flashback_table_statement"


class OraclePurgeObjectType(str, Enum):
    """Oracle ``PURGE`` statement target object kinds."""

    TABLE = "TABLE"
    INDEX = "INDEX"
    RECYCLEBIN = "RECYCLEBIN"

    __str__ = str.__str__


def normalize_as_of_mode(mode: Union[str, OracleAsOfMode]) -> OracleAsOfMode:
    """A member passes through; the word it carries becomes that member.

    An artifact carries the *word* -- a plain-JSON artifact has no place for
    an enum member, and a str-Enum member is exactly the str the encoder
    writes -- so a constructor that reads one back gets a word, not a member.
    Refusing the word would make every deserialized node unbuildable, which
    is not a capability answer.
    """
    if isinstance(mode, OracleAsOfMode):
        return mode
    normalized = str(mode).strip().upper()
    if normalized in OracleAsOfMode._value2member_map_:
        return OracleAsOfMode(normalized)
    raise ValueError(
        f"unsupported AS OF mode: {mode!r}; expected one of "
        f"{[member.value for member in OracleAsOfMode]}"
    )


def normalize_versions_between_mode(
    mode: Union[str, OracleVersionsBetweenMode],
) -> OracleVersionsBetweenMode:
    """A member passes through; the word it carries becomes that member."""
    if isinstance(mode, OracleVersionsBetweenMode):
        return mode
    normalized = str(mode).strip().upper()
    if normalized in OracleVersionsBetweenMode._value2member_map_:
        return OracleVersionsBetweenMode(normalized)
    raise ValueError(
        f"unsupported VERSIONS BETWEEN mode: {mode!r}; expected one of "
        f"{[member.value for member in OracleVersionsBetweenMode]}"
    )


def normalize_purge_object_type(
    object_type: Union[str, OraclePurgeObjectType],
) -> OraclePurgeObjectType:
    """A member passes through; the word it carries becomes that member."""
    if isinstance(object_type, OraclePurgeObjectType):
        return object_type
    normalized = str(object_type).strip().upper()
    if normalized in OraclePurgeObjectType._value2member_map_:
        return OraclePurgeObjectType(normalized)
    raise ValueError(
        f"unsupported PURGE object type: {object_type!r}; expected one of "
        f"{[member.value for member in OraclePurgeObjectType]}"
    )


class OraclePurgeExpression(BaseExpression):
    """Oracle ``PURGE`` statement expression.

    Permanently drops a table or index from the recycle bin (``PURGE
    TABLE t`` / ``PURGE INDEX i``) or empties the whole recycle bin
    (``PURGE RECYCLEBIN``).

    Args:
        dialect: the Oracle dialect instance.
        object_type: ``TABLE``, ``INDEX`` or ``RECYCLEBIN``.
        target: the object being purged, carrying its owner; required for
            ``TABLE``/``INDEX`` and must be ``None`` for ``RECYCLEBIN``. Its
            kind must agree with ``object_type``.

    Raises:
        ValueError: if ``object_type`` names no member.
            or ``target`` is not the kind ``object_type`` names.
        ValueError: if ``object_type`` is ``TABLE``/``INDEX`` without a
            target, or ``RECYCLEBIN`` with one.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        object_type: OraclePurgeObjectType,
        target: Optional[object] = None,
    ):
        super().__init__(dialect)
        object_type = normalize_purge_object_type(object_type)
        if object_type is OraclePurgeObjectType.RECYCLEBIN:
            if target is not None:
                raise ValueError("RECYCLEBIN purge does not take a target object")
        else:
            expected = {
                OraclePurgeObjectType.TABLE: Table,
                OraclePurgeObjectType.INDEX: Index,
            }[object_type]
            if not isinstance(target, expected):
                raise TypeError(
                    f"target must be a {expected.__name__}, "
                    f"got {type(target).__name__}"
                )
        self.object_type = object_type
        self.target = target

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_purge_statement"
