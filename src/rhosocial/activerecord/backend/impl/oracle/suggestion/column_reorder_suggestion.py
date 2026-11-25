# src/rhosocial/activerecord/backend/impl/oracle/suggestion/column_reorder_suggestion.py
"""Oracle's column-reorder suggestion.

Oracle's ``ALTER TABLE`` grammar (Oracle Database 19c/23ai SQL Language
Reference, ``ALTER TABLE``) has no clause that repositions a column: ``MODIFY``
changes a column's definition, never its position in the row. Physical column
order is fixed at ``CREATE TABLE`` time, so a reorder means a table rebuild --
create the new table in the desired order, copy the data, drop and rename. No
rebuild node is exposed here, so this backend declines explicitly rather than
fabricating SQL the server would reject.
"""

from __future__ import annotations

from typing import Any

from rhosocial.activerecord.backend.suggestion import ColumnReorderSuggestion

from ._change_common import answer

__all__ = ["OracleColumnReorderSuggestionMixin"]


class OracleColumnReorderSuggestionMixin:
    """Decline column reorder: Oracle can only reorder by rebuilding."""

    def suggest_column_reorder(self, point: Any) -> ColumnReorderSuggestion:
        """Answer ``False``: a reorder needs a rebuild, which has no node here.

        The frozen point still carries the table name and old/new column
        orders, but Oracle has no in-place column-position DDL. The explicit
        "unsupported" state (``False``) tells the caller to handle this as a
        manual rebuild instead of silently skipping it as "no action".
        """
        return answer(point, False)
