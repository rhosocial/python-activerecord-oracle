# src/rhosocial/activerecord/backend/impl/oracle/expression/ddl/synonym.py
"""Oracle SYNONYM DDL expressions.

This module defines the backend-specific expressions for Oracle synonyms,
which provide a transparent alias for another schema object:

* ``OracleCreateSynonymExpression`` — ``CREATE [PUBLIC] SYNONYM s FOR
  [schema.]table``.
* ``OracleDropSynonymExpression`` — ``DROP [PUBLIC] SYNONYM s`` (optionally
  ``FORCE``).

All expressions delegate SQL generation to the dialect through the public
``format_*`` formatters implemented by ``OracleSynonymMixin``.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleCreateSynonymExpression(BaseExpression):
    """Oracle ``CREATE [PUBLIC] SYNONYM ... FOR ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        synonym_name: name of the synonym to create.
        table_name: name of the object the synonym points to.
        schema_name: optional schema qualifier of the target object.
        public: create a PUBLIC synonym (shared by all users) instead of a
            private one.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``synonym_name`` or ``table_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        synonym_name: str,
        table_name: str,
        schema_name: Optional[str] = None,
        public: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(synonym_name, str) or not synonym_name.strip():
            raise ValueError("synonym_name must be a non-empty string")
        if not isinstance(table_name, str) or not table_name.strip():
            raise ValueError("table_name must be a non-empty string")
        self.synonym_name = synonym_name
        self.table_name = table_name
        self.schema_name = schema_name
        self.public = bool(public)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_create_synonym_statement(self)


class OracleDropSynonymExpression(BaseExpression):
    """Oracle ``DROP [PUBLIC] SYNONYM ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        synonym_name: name of the synonym to drop.
        public: drop the PUBLIC synonym.
        force: append ``FORCE`` to drop the synonym even when it has
            dependents.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``synonym_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        synonym_name: str,
        public: bool = False,
        force: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(synonym_name, str) or not synonym_name.strip():
            raise ValueError("synonym_name must be a non-empty string")
        self.synonym_name = synonym_name
        self.public = bool(public)
        self.force = bool(force)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_drop_synonym_statement(self)
