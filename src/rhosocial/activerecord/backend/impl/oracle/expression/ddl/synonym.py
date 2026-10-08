# src/rhosocial/activerecord/backend/impl/oracle/expression/ddl/synonym.py
"""Oracle SYNONYM DDL expressions.

This module defines the backend-specific expressions for Oracle synonyms,
which provide a transparent alias for another schema object:

* ``OracleCreateSynonymExpression`` — ``CREATE [PUBLIC] SYNONYM s FOR
  [schema.]table``.
* ``OracleDropSynonymExpression`` — ``DROP [PUBLIC] SYNONYM s`` (optionally
  ``FORCE``).

Each takes the objects it names rather than a name and an owner. A synonym and
its target are two different catalogue entries that may well live in different
owners, and carrying each as its own object is the only way the two owners can
be told apart; passing a single ``schema_name`` beside both names made the
synonym's owner and its target's owner the same string by construction.

All expressions delegate SQL generation to the dialect through the public
``format_*`` formatters implemented by ``OracleSynonymMixin``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import Synonym, Table

if TYPE_CHECKING:  # pragma: no cover
    from ...dialect import OracleDialect


class OracleCreateSynonymExpression(BaseExpression):
    """Oracle ``CREATE [PUBLIC] SYNONYM ... FOR ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        synonym: the synonym to create, carrying its owner if it is not the
            caller's own.
        table: the object the synonym points to, carrying its own owner.
        public: create a PUBLIC synonym (shared by all users) instead of a
            private one.

    Raises:
        TypeError: if ``synonym`` is not a ``Synonym`` or ``table`` is not a
            ``Table``.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        synonym: Synonym,
        table: Table,
        public: bool = False,
    ):
        super().__init__(dialect)
        if not isinstance(synonym, Synonym):
            raise TypeError(
                f"synonym must be a Synonym, got {type(synonym).__name__}"
            )
        if not isinstance(table, Table):
            raise TypeError(
                f"table must be a Table, got {type(table).__name__}"
            )
        self.synonym = synonym
        self.table = table
        self.public = bool(public)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_create_synonym_statement"


class OracleDropSynonymExpression(BaseExpression):
    """Oracle ``DROP [PUBLIC] SYNONYM ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        synonym: the synonym to drop, carrying its owner if it is not the
            caller's own.
        public: drop the PUBLIC synonym.
        force: append ``FORCE`` to drop the synonym even when it has
            dependents.

    Raises:
        TypeError: if ``synonym`` is not a ``Synonym``.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        synonym: Synonym,
        public: bool = False,
        force: bool = False,
    ):
        super().__init__(dialect)
        if not isinstance(synonym, Synonym):
            raise TypeError(
                f"synonym must be a Synonym, got {type(synonym).__name__}"
            )
        self.synonym = synonym
        self.public = bool(public)
        self.force = bool(force)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_drop_synonym_statement"
