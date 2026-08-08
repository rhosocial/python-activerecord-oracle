# src/rhosocial/activerecord/backend/impl/oracle/expression/ddl/routine.py
"""Oracle PL/SQL routine and package DDL expressions.

This module defines backend-specific expressions for Oracle stored
procedures, functions and packages:

* ``OracleRoutineParameterMode`` / ``OracleRoutineParameter`` — formal
  parameter declarations (name, optional ``IN``/``OUT``/``IN OUT`` mode,
  data type).
* ``OracleCreateProcedureExpression`` — ``CREATE [OR REPLACE] PROCEDURE
  p (x IN NUMBER) AS ...``.
* ``OracleCreateFunctionExpression`` — ``CREATE [OR REPLACE] FUNCTION
  f (x NUMBER) RETURN NUMBER AS ...``.
* ``OracleCreatePackageExpression`` — ``CREATE [OR REPLACE] PACKAGE pk
  AS ...``.
* ``OracleCreatePackageBodyExpression`` — ``CREATE [OR REPLACE] PACKAGE
  BODY pk AS ...``.
* ``OracleDropRoutineObjectType`` / ``OracleDropRoutineExpression`` —
  ``DROP PROCEDURE/FUNCTION/PACKAGE [BODY] ...``.

Routine bodies are passed through verbatim as raw PL/SQL strings; the
``AS`` / ``IS`` separator is chosen through the ``keyword`` argument.

All expressions delegate SQL generation to the dialect through the public
``format_*`` formatters implemented by ``OracleRoutineMixin``.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleRoutineParameterMode(Enum):
    """Oracle routine parameter direction modes."""

    IN = "IN"
    OUT = "OUT"
    IN_OUT = "IN OUT"


class OracleRoutineParameter:
    """A formal parameter declaration for an Oracle routine.

    Args:
        name: the parameter name.
        data_type: the SQL data type, e.g. ``NUMBER`` or ``VARCHAR2(10)``.
        mode: ``IN``, ``OUT`` or ``IN OUT``; ``None`` renders no mode
            clause (Oracle defaults to ``IN``).

    Raises:
        ValueError: if ``name`` or ``data_type`` is empty.
        TypeError: if ``mode`` is not an :class:`OracleRoutineParameterMode`.
    """

    def __init__(
        self,
        name: str,
        data_type: str,
        mode: Optional[OracleRoutineParameterMode] = None,
    ):
        if not isinstance(name, str) or not name.strip():
            raise ValueError("parameter name must be a non-empty string")
        if not isinstance(data_type, str) or not data_type.strip():
            raise ValueError("parameter data_type must be a non-empty string")
        if mode is not None and not isinstance(mode, OracleRoutineParameterMode):
            raise TypeError(
                "mode must be an OracleRoutineParameterMode value, "
                f"got {type(mode).__name__}"
            )
        self.name = name
        self.data_type = data_type
        self.mode = mode


class OracleCreateProcedureExpression(BaseExpression):
    """Oracle ``CREATE [OR REPLACE] PROCEDURE ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        procedure_name: name of the procedure to create.
        body: the PL/SQL body as a raw string, e.g. ``"BEGIN NULL; END;"``.
        parameters: optional list of :class:`OracleRoutineParameter`.
        or_replace: emit ``OR REPLACE`` (default True).
        keyword: the ``AS`` / ``IS`` separator between the signature and
            the body.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``procedure_name`` or ``body`` is empty, or
            ``keyword`` is not ``AS``/``IS``.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        procedure_name: str,
        body: str,
        parameters: Optional[List[OracleRoutineParameter]] = None,
        or_replace: bool = True,
        keyword: str = "AS",
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(procedure_name, str) or not procedure_name.strip():
            raise ValueError("procedure_name must be a non-empty string")
        if not isinstance(body, str) or not body.strip():
            raise ValueError("body must be a non-empty string")
        if keyword not in ("AS", "IS"):
            raise ValueError("keyword must be 'AS' or 'IS'")
        self.procedure_name = procedure_name
        self.body = body
        self.parameters = list(parameters) if parameters else []
        self.or_replace = bool(or_replace)
        self.keyword = keyword
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_create_procedure_statement(self)


class OracleCreateFunctionExpression(BaseExpression):
    """Oracle ``CREATE [OR REPLACE] FUNCTION ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        function_name: name of the function to create.
        return_type: the SQL return data type, e.g. ``NUMBER``.
        body: the PL/SQL body as a raw string.
        parameters: optional list of :class:`OracleRoutineParameter`.
        or_replace: emit ``OR REPLACE`` (default True).
        return_keyword: ``RETURN`` (Oracle syntax) or ``RETURNS`` spelling.
        keyword: the ``AS`` / ``IS`` separator between the signature and
            the body.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``function_name``, ``return_type`` or ``body`` is
            empty, or ``keyword`` / ``return_keyword`` is invalid.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        function_name: str,
        return_type: str,
        body: str,
        parameters: Optional[List[OracleRoutineParameter]] = None,
        or_replace: bool = True,
        return_keyword: str = "RETURN",
        keyword: str = "AS",
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(function_name, str) or not function_name.strip():
            raise ValueError("function_name must be a non-empty string")
        if not isinstance(return_type, str) or not return_type.strip():
            raise ValueError("return_type must be a non-empty string")
        if not isinstance(body, str) or not body.strip():
            raise ValueError("body must be a non-empty string")
        if return_keyword not in ("RETURN", "RETURNS"):
            raise ValueError("return_keyword must be 'RETURN' or 'RETURNS'")
        if keyword not in ("AS", "IS"):
            raise ValueError("keyword must be 'AS' or 'IS'")
        self.function_name = function_name
        self.return_type = return_type
        self.body = body
        self.parameters = list(parameters) if parameters else []
        self.or_replace = bool(or_replace)
        self.return_keyword = return_keyword
        self.keyword = keyword
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_create_function_statement(self)


class OracleCreatePackageExpression(BaseExpression):
    """Oracle ``CREATE [OR REPLACE] PACKAGE ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        package_name: name of the package to create.
        body: the package specification as a raw string, e.g. ``"PROCEDURE
            p (x NUMBER);"``.
        or_replace: emit ``OR REPLACE`` (default True).
        keyword: the ``AS`` / ``IS`` separator before the body.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``package_name`` or ``body`` is empty, or ``keyword``
            is not ``AS``/``IS``.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        package_name: str,
        body: str,
        or_replace: bool = True,
        keyword: str = "AS",
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(package_name, str) or not package_name.strip():
            raise ValueError("package_name must be a non-empty string")
        if not isinstance(body, str) or not body.strip():
            raise ValueError("body must be a non-empty string")
        if keyword not in ("AS", "IS"):
            raise ValueError("keyword must be 'AS' or 'IS'")
        self.package_name = package_name
        self.body = body
        self.or_replace = bool(or_replace)
        self.keyword = keyword
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_create_package_statement(self)


class OracleCreatePackageBodyExpression(BaseExpression):
    """Oracle ``CREATE [OR REPLACE] PACKAGE BODY ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        package_name: name of the package whose body to create.
        body: the package body as a raw string.
        or_replace: emit ``OR REPLACE`` (default True).
        keyword: the ``AS`` / ``IS`` separator before the body.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``package_name`` or ``body`` is empty, or ``keyword``
            is not ``AS``/``IS``.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        package_name: str,
        body: str,
        or_replace: bool = True,
        keyword: str = "AS",
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(package_name, str) or not package_name.strip():
            raise ValueError("package_name must be a non-empty string")
        if not isinstance(body, str) or not body.strip():
            raise ValueError("body must be a non-empty string")
        if keyword not in ("AS", "IS"):
            raise ValueError("keyword must be 'AS' or 'IS'")
        self.package_name = package_name
        self.body = body
        self.or_replace = bool(or_replace)
        self.keyword = keyword
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_create_package_body_statement(self)


class OracleDropRoutineObjectType(Enum):
    """Oracle PL/SQL object kinds accepted by ``DROP``."""

    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    PACKAGE = "PACKAGE"
    PACKAGE_BODY = "PACKAGE BODY"


class OracleDropRoutineExpression(BaseExpression):
    """Oracle ``DROP PROCEDURE/FUNCTION/PACKAGE [BODY] ...`` expression.

    Args:
        dialect: the Oracle dialect instance.
        object_type: the PL/SQL object kind to drop.
        object_name: the name of the object to drop.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``object_name`` is empty.
        TypeError: if ``object_type`` is not an
            :class:`OracleDropRoutineObjectType`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        object_type: OracleDropRoutineObjectType,
        object_name: str,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(object_type, OracleDropRoutineObjectType):
            raise TypeError(
                "object_type must be an OracleDropRoutineObjectType value, "
                f"got {type(object_type).__name__}"
            )
        if not isinstance(object_name, str) or not object_name.strip():
            raise ValueError("object_name must be a non-empty string")
        self.object_type = object_type
        self.object_name = object_name
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_drop_routine_statement(self)
