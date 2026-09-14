# src/rhosocial/activerecord/backend/impl/oracle/mixins/table.py
from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.table import TableCompressionClauseExpression, TablespaceClauseExpression


class OracleTableMixin(object):
    """Oracle table feature introspection mixin.

    This mixin exposes Oracle-specific table capabilities (capability flags)
    and helpers for composing Oracle-only DDL clauses such as tablespace
    and compression. It is intentionally a *capability/feature* mixin and
    does NOT duplicate the canonical CREATE TABLE formatters that already
    live on the dialect (``format_create_table_statement`` and
    ``format_column_definition``); those remain the source of truth
    for emitting full DDL, including partition clauses (rendered via
    ``expr.partition``) and table-level constraints (rendered via
    ``format_table_constraint``).

    Oracle specifics captured here:

    * No ``CREATE TABLE ... LIKE`` syntax — schema cloning must use CTAS
      (``CREATE TABLE t AS SELECT ...``).
    * No inline index definitions inside ``CREATE TABLE`` — indexes are
      created via separate ``CREATE INDEX`` statements.
    * No MySQL-style ``ENGINE=`` clause; physical placement is controlled
      through tablespaces (``TABLESPACE ...``).
    * Advanced Compression Option (``COMPRESS FOR ...`` / ``NOCOMPRESS``).
    * Index-Organized Tables (``ORGANIZATION INDEX``).
    * Global Temporary Tables (``GLOBAL TEMPORARY``).
    * External tables (``EXTERNAL ORACLE_LOADER``).
    """

    def supports_create_table_like(self) -> bool:
        """Oracle has no CREATE TABLE LIKE; use CTAS instead."""
        return False

    def supports_inline_index(self) -> bool:
        """Indexes must be defined via a separate CREATE INDEX statement."""
        return False

    def supports_storage_engine_option(self) -> bool:
        """Oracle has no ENGINE= concept; TABLESPACE is handled separately."""
        return False

    def supports_tablespace_option(self) -> bool:
        """Oracle supports CREATE TABLE ... TABLESPACE ...."""
        return True

    def supports_compress_option(self) -> bool:
        """Oracle Advanced Compression Option: COMPRESS/NOCOMPRESS, OLTP COMPRESS."""
        return True

    def supports_partition_option(self) -> bool:
        """Partitioning is wired via OraclePartitionMixin in dialect.py."""
        return True

    def supports_iot(self) -> bool:
        """Index-Organized Tables via ORGANIZATION INDEX are supported."""
        return True

    def supports_temporary_table(self) -> bool:
        """GLOBAL TEMPORARY tables are supported."""
        return True

    def supports_external_table(self) -> bool:
        """EXTERNAL ORACLE_LOADER tables are supported."""
        return True

    def supports_copyright_compatibility(self) -> bool:
        """Oracle has no copyright-compatibility mode (e.g. MySQL forks)."""
        return False

    def format_table_compression_clause(self, expr: "TableCompressionClauseExpression") -> Tuple[str, tuple]:
        """Format a TableCompressionClauseExpression into SQL and params."""
        from ..expression.table import TableCompressionClauseExpression
        if isinstance(expr, TableCompressionClauseExpression):
            mode = expr.mode
        else:
            mode = expr
        if mode is None:
            return "NOCOMPRESS", ()
        normalized = str(mode).strip().upper()
        if normalized == "NONE" or normalized == "":
            return "NOCOMPRESS", ()
        return f"COMPRESS FOR {normalized}", ()

    def format_tablespace_clause(self, expr: "TablespaceClauseExpression") -> Tuple[str, tuple]:
        """Format a TablespaceClauseExpression into SQL and params."""
        from ..expression.table import TablespaceClauseExpression
        if isinstance(expr, TablespaceClauseExpression):
            tablespace_name = expr.tablespace_name
        else:
            tablespace_name = expr
        return f"TABLESPACE {self.format_identifier(tablespace_name)}", ()

    def format_table_compression_clause_expression(
        self, expr: "TableCompressionClauseExpression"
    ) -> Tuple[str, tuple]:
        """Alias for format_table_compression_clause."""
        return self.format_table_compression_clause(expr)

    def format_tablespace_clause_expression(
        self, expr: "TablespaceClauseExpression"
    ) -> Tuple[str, tuple]:
        """Alias for format_tablespace_clause."""
        return self.format_tablespace_clause(expr)
