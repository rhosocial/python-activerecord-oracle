# src/rhosocial/activerecord/backend/impl/oracle/expression/analyze.py
"""Oracle ANALYZE TABLE statement expressions.

This module defines the backend-specific expression for the Oracle
``ANALYZE`` statement, which collects or validates table statistics:

* ``OracleAnalyzeMode`` — the allowed operation modes (``COMPUTE
  STATISTICS``, ``ESTIMATE STATISTICS``, ``VALIDATE STRUCTURE``,
  ``LIST CHAINED ROWS``, ``DELETE SYSTEM STATISTICS``).
* ``OracleAnalyzeExpression`` — ``ANALYZE TABLE t <mode>`` with the
  ``SAMPLE n PERCENT`` (estimate) and ``CASCADE`` (validate) options.

The expression delegates SQL generation to the dialect through the public
``format_analyze_statement`` formatter implemented by
``OracleAnalyzeMixin``.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleAnalyzeMode(Enum):
    """Oracle ``ANALYZE TABLE`` operation modes."""

    COMPUTE_STATISTICS = "COMPUTE STATISTICS"
    ESTIMATE_STATISTICS = "ESTIMATE STATISTICS"
    VALIDATE_STRUCTURE = "VALIDATE STRUCTURE"
    LIST_CHAINED_ROWS = "LIST CHAINED ROWS"
    DELETE_SYSTEM_STATISTICS = "DELETE SYSTEM STATISTICS"


class OracleAnalyzeExpression(BaseExpression):
    """Oracle ``ANALYZE TABLE t <mode>`` statement expression.

    Args:
        dialect: the Oracle dialect instance.
        table: name of the table (or table-partition) to analyze.
        mode: the operation mode; one of the :class:`OracleAnalyzeMode`
            values.
        sample_percent: with ``ESTIMATE STATISTICS``, the sampling ratio
            rendered as ``SAMPLE n PERCENT``.
        cascade: with ``VALIDATE STRUCTURE``, recurse into dependent
            structures (``CASCADE``).
        into: with ``LIST CHAINED ROWS``, the table to receive the chained
            row list (``INTO ...``).
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``table`` is empty, ``sample_percent`` is used
            outside ``ESTIMATE STATISTICS``, ``cascade`` is used outside
            ``VALIDATE STRUCTURE``, or ``into`` is used outside
            ``LIST CHAINED ROWS``.
        TypeError: if ``mode`` is not an :class:`OracleAnalyzeMode`, or
            ``sample_percent`` is not an int.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        table: str,
        mode: OracleAnalyzeMode,
        sample_percent: Optional[int] = None,
        cascade: bool = False,
        into: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(table, str) or not table.strip():
            raise ValueError("table must be a non-empty string")
        if not isinstance(mode, OracleAnalyzeMode):
            raise TypeError(
                "mode must be an OracleAnalyzeMode value, "
                f"got {type(mode).__name__}"
            )
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
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_analyze_statement(self)
