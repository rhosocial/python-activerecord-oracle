# src/rhosocial/activerecord/backend/impl/oracle/expression/introspection.py
"""Oracle schema-object queries the core catalog does not model.

Tables, columns, indexes, foreign keys, views and triggers are shared by every
engine, so core states their questions. Three families are Oracle's own and
have no core object:

* **sequences** -- ``ALL_SEQUENCES``, whose owner column is
  ``SEQUENCE_OWNER`` rather than the ``OWNER`` the synonym view uses;
* **synonyms** -- ``ALL_SYNONYMS``, which mixes private and ``PUBLIC`` rows in
  one view;
* **packages** -- ``ALL_OBJECTS`` filtered to ``OBJECT_TYPE='PACKAGE'``, which
  picks the specification and not the body.

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
    "OracleSequenceListQueryStatement",
    "OracleSequenceInfoQueryStatement",
    "OracleSynonymListQueryStatement",
    "OracleSynonymInfoQueryStatement",
    "OraclePackageListQueryStatement",
]


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
