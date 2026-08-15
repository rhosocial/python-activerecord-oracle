# src/rhosocial/activerecord/backend/impl/oracle/mixins/pagination.py
"""Oracle pagination (LIMIT/OFFSET) formatting mixin."""

from typing import Any, List, Optional, Tuple

from rhosocial.activerecord.backend.expression.bases import ToSQLProtocol


class OraclePaginationMixin:
    """Oracle-specific LIMIT / OFFSET clause formatting.

    Oracle 12c+ uses FETCH FIRST / OFFSET syntax;
    pre-12c uses ROWNUM-based pagination.
    """

    def format_limit_offset(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Tuple[Optional[str], List[Any]]:
        params: List[Any] = []
        sql_parts: List[str] = []

        if self.version >= (12, 0, 0):
            if offset is not None:
                sql_parts.append(f"OFFSET {offset} ROWS")
            if limit is not None:
                sql_parts.append(f"FETCH FIRST {limit} ROWS ONLY")
            else:
                sql_parts.append("FETCH FIRST ROWS ONLY")
        else:
            pass

        if not sql_parts:
            return None, []
        return " ".join(sql_parts), params

    def format_limit_offset_clause(self, clause) -> Tuple[str, tuple]:
        """Format LIMIT and OFFSET clause for Oracle."""
        all_params: List[Any] = []
        sql_parts: List[str] = []

        if self.version >= (12, 0, 0):
            if clause.offset is not None:
                if isinstance(clause.offset, ToSQLProtocol):
                    offset_sql, offset_params = clause.offset.to_sql()
                    sql_parts.append(f"OFFSET {offset_sql} ROWS")
                    all_params.extend(offset_params)
                else:
                    sql_parts.append(f"OFFSET {self.get_parameter_placeholder()} ROWS")
                    all_params.append(clause.offset)

            if clause.limit is not None:
                if isinstance(clause.limit, ToSQLProtocol):
                    limit_sql, limit_params = clause.limit.to_sql()
                    sql_parts.append(f"FETCH FIRST {limit_sql} ROWS ONLY")
                    all_params.extend(limit_params)
                else:
                    sql_parts.append(f"FETCH FIRST {self.get_parameter_placeholder()} ROWS ONLY")
                    all_params.append(clause.limit)
        else:
            if clause.limit is not None:
                if isinstance(clause.limit, ToSQLProtocol):
                    limit_sql, limit_params = clause.limit.to_sql()
                    sql_parts.append(f"ROWNUM <= {limit_sql}")
                    all_params.extend(limit_params)
                else:
                    sql_parts.append(f"ROWNUM <= {self.get_parameter_placeholder()}")
                    all_params.append(clause.limit)

        return " ".join(sql_parts), tuple(all_params)