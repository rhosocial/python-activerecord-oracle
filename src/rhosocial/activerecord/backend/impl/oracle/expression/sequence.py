# src/rhosocial/activerecord/backend/impl/oracle/expression/sequence.py
"""Oracle sequence value and DDL expressions.

This module defines backend-specific expressions for Oracle sequences:

* ``OracleSequenceValueExpression`` — the ``seq.NEXTVAL`` / ``seq.CURRVAL``
  pseudo-column value expression (Oracle's substitute for the SQL-standard
  ``NEXT VALUE FOR``).
* ``OracleCreateSequenceExpression`` — ``CREATE SEQUENCE ...`` with the
  Oracle option set (START WITH / INCREMENT BY / MINVALUE / MAXVALUE /
  CYCLE | NOCYCLE / CACHE | NOCACHE / ORDER | NOORDER).
* ``OracleDropSequenceExpression`` — ``DROP SEQUENCE ...``.

All expressions delegate SQL generation to the dialect through the public
``format_*`` formatters implemented by ``OracleSequenceMixin``.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleSequenceValueMode(Enum):
    """Oracle sequence pseudo-column access modes."""

    NEXTVAL = "NEXTVAL"
    CURRVAL = "CURRVAL"


class OracleSequenceValueExpression(BaseExpression):
    """Oracle ``seq.NEXTVAL`` / ``seq.CURRVAL`` value expression.

    Used wherever a sequence-generated value is needed, e.g.
    ``SELECT seq.NEXTVAL FROM dual`` or
    ``INSERT INTO t (id) VALUES (seq.NEXTVAL)``.

    Args:
        dialect: the Oracle dialect instance.
        sequence: the sequence name.
        mode: access mode; ``NEXTVAL`` (default) or ``CURRVAL``.

    Raises:
        ValueError: if ``sequence`` is empty.
        TypeError: if ``mode`` is not an :class:`OracleSequenceValueMode`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        sequence: str,
        mode: OracleSequenceValueMode = OracleSequenceValueMode.NEXTVAL,
    ):
        super().__init__(dialect)
        if not isinstance(sequence, str) or not sequence.strip():
            raise ValueError("sequence must be a non-empty string")
        if not isinstance(mode, OracleSequenceValueMode):
            raise TypeError(
                "mode must be an OracleSequenceValueMode value, "
                f"got {type(mode).__name__}"
            )
        self.sequence = sequence
        self.mode = mode

    def to_sql(self) -> SQLQueryAndParams:
        if self.mode is OracleSequenceValueMode.NEXTVAL:
            return self.dialect.format_nextval(self)
        return self.dialect.format_currval(self)


class OracleCreateSequenceExpression(BaseExpression):
    """Oracle ``CREATE SEQUENCE ...`` DDL expression.

    Oracle renders sequences with the ``START WITH``/``INCREMENT BY`` option
    set. ``cycle``/``order`` default to ``None`` so the corresponding
    ``NOCYCLE``/``NOORDER`` clauses are omitted unless explicitly requested;
    ``cache=0`` renders the explicit ``NOCACHE`` clause.

    Args:
        dialect: the Oracle dialect instance.
        sequence_name: name of the sequence to create.
        if_not_exists: if True, emit ``IF NOT EXISTS`` (Oracle 23ai+).
        start: ``START WITH`` value.
        increment: ``INCREMENT BY`` value.
        minvalue: ``MINVALUE`` value.
        maxvalue: ``MAXVALUE`` value.
        cycle: ``CYCLE`` when True, ``NOCYCLE`` when False, omitted when None.
        cache: ``CACHE n`` for a positive int, ``NOCACHE`` for ``0``,
            omitted when None.
        order: ``ORDER`` when True, ``NOORDER`` when False, omitted when None.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``sequence_name`` is empty, or ``cache`` is negative.
        TypeError: if ``cache`` is provided but is not an int.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        sequence_name: str,
        if_not_exists: bool = False,
        start: Optional[int] = None,
        increment: Optional[int] = None,
        minvalue: Optional[int] = None,
        maxvalue: Optional[int] = None,
        cycle: Optional[bool] = None,
        cache: Optional[int] = None,
        order: Optional[bool] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(sequence_name, str) or not sequence_name.strip():
            raise ValueError("sequence_name must be a non-empty string")
        if cache is not None:
            if not isinstance(cache, int) or isinstance(cache, bool):
                raise TypeError(
                    "cache must be an int, "
                    f"got {type(cache).__name__}"
                )
            if cache < 0:
                raise ValueError(f"cache must be non-negative, got {cache}")
        self.sequence_name = sequence_name
        self.if_not_exists = bool(if_not_exists)
        self.start = start
        self.increment = increment
        self.minvalue = minvalue
        self.maxvalue = maxvalue
        self.cycle = cycle
        self.cache = cache
        self.order = order
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_create_sequence_statement(self)


class OracleDropSequenceExpression(BaseExpression):
    """Oracle ``DROP SEQUENCE ...`` DDL expression.

    Args:
        dialect: the Oracle dialect instance.
        sequence_name: name of the sequence to drop.
        if_exists: if True, emit ``IF EXISTS`` (Oracle 23ai+).
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``sequence_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        sequence_name: str,
        if_exists: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(sequence_name, str) or not sequence_name.strip():
            raise ValueError("sequence_name must be a non-empty string")
        self.sequence_name = sequence_name
        self.if_exists = bool(if_exists)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_drop_sequence_statement(self)
