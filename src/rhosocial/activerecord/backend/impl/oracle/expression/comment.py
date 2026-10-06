# src/rhosocial/activerecord/backend/impl/oracle/expression/comment.py
"""Oracle COMMENT ON statement expressions.

This module defines the backend-specific expression for the Oracle
``COMMENT ON`` statement, which attaches free-form text comments to schema
objects. Oracle has no inline column-comment clause (unlike MySQL's
``COMMENT '...'``); comments are always issued through a standalone
``COMMENT ON`` statement.

* ``OracleCommentExpression`` — ``COMMENT ON {TABLE|COLUMN|...} obj IS
  'text'``.

The object being annotated is held as the object it is, so an owner is quoted
by the same rules as everywhere else in the backend. A column is named by a
``column`` beside the object rather than being a kind of its own: a column's
identity is scoped to its table, not to the catalogue, so
``COMMENT ON COLUMN "SCOTT"."ORDERS"."ID"`` is the table ``SCOTT.ORDERS`` with
the column ``ID``, not an object called ``ORDERS.ID``.

The expression delegates SQL generation to the dialect through the public
``format_comment_statement`` formatter implemented by
``OracleCommentMixin``.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import (
    Function,
    Index,
    MaterializedView,
    Procedure,
    Sequence,
    SchemaObject,
    Synonym,
    Table,
    Trigger,
    Type,
    View,
)

from .objects import OraclePackage

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


# The object kind each COMMENT ON keyword names, so the statement cannot claim
# to annotate a ``VIEW`` while holding a ``Table``. Both PACKAGE keywords name
# the same kind -- a package body is not a separate catalogue entry from its
# specification.
_COMMENT_OBJECT_KINDS = {
    OracleCommentObjectType.TABLE: Table,
    OracleCommentObjectType.COLUMN: Table,
    OracleCommentObjectType.VIEW: View,
    OracleCommentObjectType.INDEX: Index,
    OracleCommentObjectType.SEQUENCE: Sequence,
    OracleCommentObjectType.PROCEDURE: Procedure,
    OracleCommentObjectType.FUNCTION: Function,
    OracleCommentObjectType.PACKAGE: OraclePackage,
    OracleCommentObjectType.PACKAGE_BODY: OraclePackage,
    OracleCommentObjectType.TRIGGER: Trigger,
    OracleCommentObjectType.MATERIALIZED_VIEW: MaterializedView,
    OracleCommentObjectType.TYPE: Type,
    OracleCommentObjectType.SYNONYM: Synonym,
}


class OracleCommentExpression(BaseExpression):
    """Oracle ``COMMENT ON ... IS ...`` statement expression.

    Args:
        dialect: the Oracle dialect instance.
        object_type: the schema object kind to comment on. For ``COLUMN`` this
            names the *table* the column belongs to, since a column has no
            namespace of its own.
        object: the object being annotated, carrying its owner when it is not
            the caller's own.
        comment: the comment text. ``None`` renders ``IS NULL``, which
            removes any existing comment from the object.
        column: the column, when ``object_type`` is ``COLUMN``.

    Raises:
        TypeError: if ``object_type`` is not an
            :class:`OracleCommentObjectType`, if ``object`` is not a
            :class:`SchemaObject`, or its kind disagrees with
            ``object_type``.
        ValueError: if ``column`` is given without ``COLUMN``, or
            ``COLUMN`` is named without one.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        object_type: OracleCommentObjectType,
        object: SchemaObject,
        comment: Optional[str] = None,
        column: Optional[str] = None,
    ):
        super().__init__(dialect)
        if not isinstance(object_type, OracleCommentObjectType):
            raise TypeError(
                "object_type must be an OracleCommentObjectType value, "
                f"got {type(object_type).__name__}"
            )
        expected = _COMMENT_OBJECT_KINDS[object_type]
        if not isinstance(object, expected):
            raise TypeError(
                f"{object_type.value} names a {expected.__name__}, "
                f"got {type(object).__name__}"
            )
        if object_type is OracleCommentObjectType.COLUMN:
            if column is None:
                raise ValueError("COLUMN comment requires a column name")
        elif column is not None:
            raise ValueError(
                f"{object_type.value} comment does not take a column name"
            )
        if column is not None and (
            not isinstance(column, str) or not column.strip()
        ):
            raise ValueError("column must be a non-empty string or None")
        self.object_type = object_type
        self.object = object
        self.comment = comment
        self.column = column

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_comment_statement"
