# src/rhosocial/activerecord/backend/impl/oracle/mixins/table.py


class OracleTableMixin(object):
    """Oracle table feature introspection mixin.

    This mixin exposes Oracle-specific table capabilities (capability flags)
    and helpers for composing Oracle-only DDL clauses such as tablespace
    and compression. It is intentionally a *capability/feature* mixin and
    does NOT duplicate the canonical CREATE TABLE formatters that already
    live on the dialect (``format_create_table_statement`` and
    ``format_column_definition_oracle``); those remain the source of truth
    for emitting full DDL, including partition clauses (rendered via
    ``expr.partition``) and table-level constraints (rendered via
    ``format_table_constraint_oracle``).

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

    def supports_table_like_syntax(self) -> bool:
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

    def format_table_compression_clause(self, mode: str = 'BASIC') -> str:
        """Compose an Oracle table-compression clause.

        Passing ``mode='none'`` (case-insensitive) emits ``NOCOMPRESS``;
        any other value becomes ``COMPRESS FOR <MODE>`` (uppercased), e.g.
        ``COMPRESS FOR OLTP`` or ``COMPRESS FOR QUERY LOW``.

        See: Oracle Advanced Compression Option reference.
        """
        if mode is None:
            return "NOCOMPRESS"
        normalized = str(mode).strip().upper()
        if normalized == "NONE" or normalized == "":
            return "NOCOMPRESS"
        return f"COMPRESS FOR {normalized}"

    def format_tablespace_clause(self, tablespace_name: str) -> str:
        """Compose an Oracle TABLESPACE clause.

        The identifier is always quoted via ``format_identifier`` to honor
        the dialect's quoting configuration.
        """
        return f"TABLESPACE {self.format_identifier(tablespace_name)}"
