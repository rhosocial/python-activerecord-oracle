# src/rhosocial/activerecord/backend/impl/oracle/expression/ddl/database_link.py
"""Oracle DATABASE LINK DDL expressions.

This module defines the backend-specific expressions for Oracle database
links, which enable cross-database/instance queries:

* ``OracleCreateDatabaseLinkExpression`` — ``CREATE [SHARED] [PUBLIC]
  DATABASE LINK dl CONNECT TO u IDENTIFIED BY pwd USING 'conn_str'``.
* ``OracleDropDatabaseLinkExpression`` — ``DROP [PUBLIC] DATABASE LINK dl``.

Remote table references are expressed with the ``@dblink`` suffix, supported
through ``OracleIdentifierMixin.format_table(..., dblink=...)``.

All expressions delegate SQL generation to the dialect through the public
``format_*`` formatters implemented by ``OracleDatabaseLinkMixin``.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleCreateDatabaseLinkExpression(BaseExpression):
    """Oracle ``CREATE [SHARED] [PUBLIC] DATABASE LINK ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        link_name: name of the database link to create.
        user: user name used to connect to the remote database.
        identified_by: password of the remote user.
        using: connect string (usually an Oracle Net service name) that
            locates the remote database.
        public: create a PUBLIC database link instead of a private one.
        shared: create a SHARED database link used by multiple sessions
            through the same connection.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``link_name`` is empty, or if only one of ``user`` /
            ``identified_by`` is supplied.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        link_name: str,
        user: Optional[str] = None,
        identified_by: Optional[str] = None,
        using: Optional[str] = None,
        public: bool = False,
        shared: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(link_name, str) or not link_name.strip():
            raise ValueError("link_name must be a non-empty string")
        if (user is None) != (identified_by is None):
            raise ValueError("user and identified_by must be supplied together")
        self.link_name = link_name
        self.user = user
        self.identified_by = identified_by
        self.using = using
        self.public = bool(public)
        self.shared = bool(shared)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_create_database_link_statement(self)


class OracleDropDatabaseLinkExpression(BaseExpression):
    """Oracle ``DROP [PUBLIC] DATABASE LINK ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        link_name: name of the database link to drop.
        public: drop the PUBLIC database link.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``link_name`` is empty.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        link_name: str,
        public: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(link_name, str) or not link_name.strip():
            raise ValueError("link_name must be a non-empty string")
        self.link_name = link_name
        self.public = bool(public)
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_drop_database_link_statement(self)
