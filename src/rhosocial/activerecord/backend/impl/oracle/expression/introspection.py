# src/rhosocial/activerecord/backend/impl/oracle/expression/introspection.py
"""Oracle schema-object queries the core catalog does not model.

Tables, columns, indexes, foreign keys, views and triggers are shared by every
engine, so core states their questions. The families here are Oracle's own and
have no core object:

* **primary keys** -- the key is a constraint, so the columns come from
  ``ALL_CONSTRAINTS`` joined to ``ALL_CONS_COLUMNS`` rather than from the
  index listing core reads;
* **sequences** -- ``ALL_SEQUENCES``, whose owner column is
  ``SEQUENCE_OWNER`` rather than the ``OWNER`` the synonym view uses;
* **synonyms** -- ``ALL_SYNONYMS``, which mixes private and ``PUBLIC`` rows in
  one view;
* **packages** -- ``ALL_OBJECTS`` filtered to ``OBJECT_TYPE='PACKAGE'``, which
  picks the specification and not the body;
* **partitions** -- ``ALL_TAB_PARTITIONS`` joined to ``ALL_PART_TABLES`` for
  the strategy, the subpartitioning type and the interval expression;
* **standalone procedures and functions** -- ``ALL_PROCEDURES`` joined to
  ``ALL_OBJECTS`` and ``ALL_ARGUMENTS`` for the signature;
* **types** -- ``ALL_TYPES`` and ``ALL_COLL_TYPES`` joined to
  ``ALL_OBJECTS`` and ``ALL_TYPE_ATTRS`` for the declared shape;
* **constraints** -- ``ALL_CONSTRAINTS`` joined to ``ALL_CONS_COLUMNS``.

Each question here is an expression statement rendered by the dialect, in the
house style of this package: the tree states the projection, the filter and
the ordering, and the dialect only spells it (org rules §16.2). Nothing is
assembled as text, so a value travels as a bind parameter and never appears
inside the SQL string.

Dictionary views are named with ``name_need_quote=False``: Oracle spells them
upper case, and quoting would ask for a table literally of that name.

Example:
    >>> expr = OracleSequenceListQueryStatement(dialect, owner="HR")
    >>> sql, params = expr.to_sql()
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.introspection import (
    IntrospectionExpression,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect.base import SQLDialectBase

__all__ = [
    "OracleConstraintListQueryStatement",
    "OraclePackageListQueryStatement",
    "OraclePartitionListQueryStatement",
    "OraclePrimaryKeyQueryStatement",
    "OracleRoutineListQueryStatement",
    "OracleSequenceInfoQueryStatement",
    "OracleSequenceListQueryStatement",
    "OracleSynonymInfoQueryStatement",
    "OracleSynonymListQueryStatement",
    "OracleTypeListQueryStatement",
]


class OraclePrimaryKeyQueryStatement(IntrospectionExpression):
    """The columns of one table's primary key, in key order.

    The key is a constraint, not an index, so the question reads
    ``ALL_CONSTRAINTS`` (the constraint of type ``P``) and joins
    ``ALL_CONS_COLUMNS`` for its columns. Both halves are joined on the
    constraint name *and* the owner, because a same-named constraint in
    another schema must not contribute a column.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        *,
        table_name: str,
        schema: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            table_name: The table whose key columns are wanted.
            schema: The owner (Oracle's schema) of that table.
        """
        super().__init__(dialect, schema=schema)
        self.table_name: str = table_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_primary_key_query_statement"


class OracleSequenceListQueryStatement(IntrospectionExpression):
    """The sequences one owner has, oldest name first.

    ``ALL_SEQUENCES`` rather than ``USER_SEQUENCES`` so that *owner* is a real
    filter rather than "whoever happens to be connected": a caller inspecting
    another schema gets that schema's sequences or an empty list, never the
    connecting user's.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        owner: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            owner: The schema whose sequences are wanted. Folded to upper
                case by the caller, because Oracle's dictionary does.
        """
        super().__init__(dialect, schema=owner)
        self.owner: str = owner

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_sequence_list_query_statement"


class OracleSequenceInfoQueryStatement(IntrospectionExpression):
    """One sequence, named, or nothing at all.

    The name and the owner are both part of the question: a sequence is owned,
    so the same name in two schemas is two objects.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        sequence_name: str,
        owner: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            sequence_name: The sequence to read.
            owner: The schema owning it.
        """
        super().__init__(dialect, schema=owner)
        self.owner: str = owner
        self.sequence_name: str = sequence_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_sequence_info_query_statement"


class OracleSynonymListQueryStatement(IntrospectionExpression):
    """The synonyms one owner has, in name order.

    ``PUBLIC`` is a legitimate owner: the dictionary records global aliases
    that way, and they are never mixed into a schema's private list, because a
    caller inspecting one schema should not silently receive every alias on
    the instance.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        owner: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            owner: The owner whose synonyms are wanted, ``"PUBLIC"`` included.
        """
        super().__init__(dialect, schema=owner)
        self.owner: str = owner

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_synonym_list_query_statement"


class OracleSynonymInfoQueryStatement(IntrospectionExpression):
    """One synonym, named."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        synonym_name: str,
        owner: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            synonym_name: The synonym to read.
            owner: The owner whose synonym it is.
        """
        super().__init__(dialect, schema=owner)
        self.owner: str = owner
        self.synonym_name: str = synonym_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_synonym_info_query_statement"


class OraclePackageListQueryStatement(IntrospectionExpression):
    """The PL/SQL package specifications one owner has.

    ``OBJECT_TYPE='PACKAGE'`` selects the specification only. ``PACKAGE BODY``
    is a separate type for the same name, so filtering it out is what stops a
    package being counted twice.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        owner: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            owner: The schema whose packages are wanted.
        """
        super().__init__(dialect, schema=owner)
        self.owner: str = owner

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_package_list_query_statement"


class OraclePartitionListQueryStatement(IntrospectionExpression):
    """The partitions of one table, in declaration order.

    The partition rows live in ``ALL_TAB_PARTITIONS``; the strategy, the
    subpartitioning type and the interval expression live in
    ``ALL_PART_TABLES``, which is joined on the owner and table name. The
    tablespace's block size is read rather than assumed, so
    ``SIZE_BYTES`` is the partition's real footprint on a database whose
    block size is not 8 KiB.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        *,
        table_name: str,
        schema: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            table_name: The partitioned table.
            schema: The owner of that table.
        """
        super().__init__(dialect, schema=schema)
        self.table_name: str = table_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_partition_list_query_statement"


class OracleRoutineListQueryStatement(IntrospectionExpression):
    """The standalone procedures or functions one owner has.

    ``routine_kind`` is ``PROCEDURE`` or ``FUNCTION``. ``ALL_PROCEDURES``
    carries one row per routine property set, ``ALL_OBJECTS`` the compile
    status and timestamps, and ``ALL_ARGUMENTS`` the signature. The
    packaged subprograms are excluded by filtering ``PROCEDURE_NAME`` to
    null; they belong to their package, not to this family.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        *,
        owner: str,
        routine_kind: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            owner: The schema whose routines are wanted.
            routine_kind: ``PROCEDURE`` or ``FUNCTION``.
        """
        super().__init__(dialect, schema=owner)
        self.owner: str = owner
        self.routine_kind: str = str(routine_kind).upper()

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_routine_list_query_statement"


class OracleTypeListQueryStatement(IntrospectionExpression):
    """The named types one owner has.

    Object types come from ``ALL_TYPES`` and named collections (nested
    tables and VARRAYs) from ``ALL_COLL_TYPES``. The two are a ``UNION ALL``
    because a collection type may appear in both views, and the object side
    subtracts the collection names so one type is reported once.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        *,
        owner: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            owner: The schema whose types are wanted.
        """
        super().__init__(dialect, schema=owner)
        self.owner: str = owner

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_type_list_query_statement"


class OracleConstraintListQueryStatement(IntrospectionExpression):
    """The constraints of one table, in name and column order.

    ``ALL_CONSTRAINTS`` holds the declaration; ``ALL_CONS_COLUMNS`` holds
    the columns, joined on the constraint name and owner so a same-named
    constraint in another schema cannot contribute a column.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        *,
        table_name: str,
        schema: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            table_name: The constrained table.
            schema: The owner of that table.
        """
        super().__init__(dialect, schema=schema)
        self.table_name: str = table_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_constraint_list_query_statement"
