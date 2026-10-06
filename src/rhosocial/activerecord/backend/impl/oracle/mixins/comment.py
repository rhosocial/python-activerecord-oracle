# src/rhosocial/activerecord/backend/impl/oracle/mixins/comment.py
"""Oracle COMMENT ON formatter mixin."""

from typing import Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class OracleCommentMixin:
    """Oracle ``COMMENT ON`` capability check and formatter.

    ``COMMENT ON`` has existed since early Oracle releases; the formatter
    gates on ``(9, 0, 0)`` per the backend implementation contract. Oracle
    stores comments on schema objects (and columns) through a standalone
    statement, never through an inline column clause.
    """

    def supports_comment_on(self) -> bool:
        """Whether standalone ``COMMENT ON`` statements are supported.

        Oracle annotates schema objects through the standalone statement;
        always ``True`` (the version gate lives in
        :meth:`format_comment_statement`).
        """
        return True

    def format_comment_statement(
        self, expr
    ) -> Tuple[str, tuple]:
        """Format a standalone ``COMMENT ON`` statement.

        Accepts the Oracle expression and the core
        :class:`~rhosocial.activerecord.backend.expression.statements.CommentOnExpression`,
        which carry the same fields.

        The annotated object is rendered by its own protocol, and a column --
        which has no namespace of its own -- is appended to it, because Oracle's
        ``COMMENT ON COLUMN`` names a column of the object named before it.

        Raises:
            TypeError: ``expr.object`` is not a ``SchemaObject``. The annotated
                object renders itself, so a value of another kind would be named
                by whatever protocol that value carries.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        from rhosocial.activerecord.backend.expression.objects import SchemaObject

        if not isinstance(expr.object, SchemaObject):
            raise TypeError(
                f"{type(expr).__name__}.object must be a SchemaObject, "
                f"got {type(expr.object).__name__}"
            )

        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "COMMENT ON",
                suggestion=(
                    f"Oracle {self.version} does not support COMMENT ON; "
                    "it requires Oracle 9i or later."
                ),
            )
        object_sql = expr.object.to_sql()[0]
        column = getattr(expr, "column", None)
        if column is not None:
            object_sql = f"{object_sql}.{self.format_identifier(column)}"
        object_type = getattr(expr.object_type, "value", expr.object_type)
        head = f"COMMENT ON {object_type} {object_sql} IS"
        if expr.comment is None:
            return f"{head} NULL", ()
        escaped = self._escape_sql_string(expr.comment)
        return f"{head} '{escaped}'", ()
