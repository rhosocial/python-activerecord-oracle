# src/rhosocial/activerecord/backend/impl/oracle/expression/comment.py
"""Oracle COMMENT ON statement expressions.

This module defines the backend-specific expression for the Oracle
``COMMENT ON`` statement, which attaches free-form text comments to schema
objects. Oracle has no inline column-comment clause (unlike MySQL's
``COMMENT '...'``); comments are always issued through a standalone
``COMMENT ON`` statement.

* ``OracleCommentExpression`` — ``COMMENT ON {TABLE|COLUMN|...} obj IS
  'text'``.

The expression delegates SQL generation to the dialect through the public
``format_comment_statement`` formatter implemented by
``OracleCommentMixin``.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLQueryAndParams

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleCommentObjectType(Enum):
    """Oracle schema object kinds accepted by ``COMMENT ON``."""

    TABLE = "TABLE"
    COLUMN = "COLUMN"
    VIEW = "VIEW"
    INDEX = "INDEX"
    SEQUENCE = "SEQUENCE"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    PACKAGE = "PACKAGE"
    PACKAGE_BODY = "PACKAGE BODY"
    TRIGGER = "TRIGGER"
    MATERIALIZED_VIEW = "MATERIALIZED VIEW"
    TYPE = "TYPE"
    SYNONYM = "SYNONYM"


class OracleCommentExpression(BaseExpression):
    """Oracle ``COMMENT ON ... IS ...`` statement expression.

    Args:
        dialect: the Oracle dialect instance.
        object_type: the schema object kind to comment on.
        object_name: the object name; for ``COLUMN`` this is normally
            ``table.column``.
        comment: the comment text. ``None`` renders ``IS NULL``, which
            removes any existing comment from the object.
        dialect_options: reserved for future dialect-specific options.

    Raises:
        ValueError: if ``object_name`` is empty.
        TypeError: if ``object_type`` is not an
            :class:`OracleCommentObjectType`.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        object_type: OracleCommentObjectType,
        object_name: str,
        comment: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not isinstance(object_type, OracleCommentObjectType):
            raise TypeError(
                "object_type must be an OracleCommentObjectType value, "
                f"got {type(object_type).__name__}"
            )
        if not isinstance(object_name, str) or not object_name.strip():
            raise ValueError("object_name must be a non-empty string")
        self.object_type = object_type
        self.object_name = object_name
        self.comment = comment
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> SQLQueryAndParams:
        return self.dialect.format_comment_statement(self)
