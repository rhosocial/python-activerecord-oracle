# src/rhosocial/activerecord/backend/impl/oracle/expression/analyze.py
"""Oracle ANALYZE TABLE statement expressions.

This module defines the backend-specific expression for the Oracle
``ANALYZE`` statement, which collects or validates table statistics:

* ``OracleAnalyzeMode`` — the allowed operation modes (``COMPUTE
  STATISTICS``, ``ESTIMATE STATISTICS``, ``VALIDATE STRUCTURE``,
  ``LIST CHAINED ROWS``, ``DELETE SYSTEM STATISTICS``).
* ``OracleAnalyzeStatement`` — ``ANALYZE TABLE t <mode>`` with the
  ``SAMPLE n PERCENT`` (estimate) and ``CASCADE`` (validate) options.

The expression delegates SQL generation to the dialect through the public
``format_analyze_statement`` formatter implemented by
``OracleAnalyzeMixin``.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional, TYPE_CHECKING, Union

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import Table

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleAnalyzeMode(str, Enum):
    """Oracle ``ANALYZE TABLE`` operation modes."""

    __str__ = str.__str__

    COMPUTE_STATISTICS = "COMPUTE STATISTICS"
    ESTIMATE_STATISTICS = "ESTIMATE STATISTICS"
    VALIDATE_STRUCTURE = "VALIDATE STRUCTURE"
    LIST_CHAINED_ROWS = "LIST CHAINED ROWS"
    DELETE_SYSTEM_STATISTICS = "DELETE SYSTEM STATISTICS"


def normalize_analyze_mode(
    mode: Union[str, OracleAnalyzeMode],
) -> OracleAnalyzeMode:
    """A member passes through; the word it carries becomes that member.

    The artifact carries the word, so a constructor reading one back gets a
    str and must be able to spell the member from it.
    """
    if isinstance(mode, OracleAnalyzeMode):
        return mode
    normalized = str(mode).strip().upper()
    if normalized in OracleAnalyzeMode._value2member_map_:
        return OracleAnalyzeMode(normalized)
    raise ValueError(
        f"unsupported ANALYZE mode: {mode!r}; expected one of "
        f"{[member.value for member in OracleAnalyzeMode]}"
    )


class OracleAnalyzeStatement(BaseExpression):
    """Oracle ``ANALYZE TABLE t <mode>`` statement expression.

    Args:
        dialect: the Oracle dialect instance.
        table: the table (or table-partition) to analyze. The object carries
            its owner, so the name is rendered by the dialect rather than
            concatenated here.
        mode: the operation mode; one of the :class:`OracleAnalyzeMode`
            values.
        sample_percent: with ``ESTIMATE STATISTICS``, the sampling ratio
            rendered as ``SAMPLE n PERCENT``.
        cascade: with ``VALIDATE STRUCTURE``, recurse into dependent
            structures (``CASCADE``).
        into: with ``LIST CHAINED ROWS``, the table to receive the chained
            row list (``INTO ...``).

    Raises:
        ValueError: if ``sample_percent`` is used
            outside ``ESTIMATE STATISTICS``, ``cascade`` is used outside
            ``VALIDATE STRUCTURE``, or ``into`` is used outside
            ``LIST CHAINED ROWS``.
        TypeError: if ``table`` is not a :class:`Table`, or ``sample_percent``
            is not an int.
        ValueError: if ``mode`` names no member, if ``sample_percent`` is used
            outside ``ESTIMATE STATISTICS``, ``cascade`` is used outside
            ``VALIDATE STRUCTURE``, or ``into`` is used outside
            ``LIST CHAINED ROWS``.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: Table,
        mode: Union[str, OracleAnalyzeMode],
        sample_percent: Optional[int] = None,
        cascade: bool = False,
        into: Optional[str] = None,
    ):
        super().__init__(dialect)
        if not isinstance(table, Table):
            raise TypeError(
                f"table must be a Table, got {type(table).__name__}"
            )
        mode = normalize_analyze_mode(mode)
        if sample_percent is not None:
            if not isinstance(sample_percent, int) or isinstance(sample_percent, bool):
                raise TypeError(
                    "sample_percent must be an int, "
                    f"got {type(sample_percent).__name__}"
                )
            if sample_percent <= 0:
                raise ValueError("sample_percent must be a positive integer")
            if mode is not OracleAnalyzeMode.ESTIMATE_STATISTICS:
                raise ValueError("sample_percent requires ESTIMATE STATISTICS mode")
        if cascade and mode is not OracleAnalyzeMode.VALIDATE_STRUCTURE:
            raise ValueError("cascade requires VALIDATE STRUCTURE mode")
        if into is not None:
            if not isinstance(into, str) or not into.strip():
                raise ValueError("into must be a non-empty string")
            if mode is not OracleAnalyzeMode.LIST_CHAINED_ROWS:
                raise ValueError("into requires LIST CHAINED ROWS mode")
        self.table = table
        self.mode = mode
        self.sample_percent = sample_percent
        self.cascade = bool(cascade)
        self.into = into

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_analyze_statement"
