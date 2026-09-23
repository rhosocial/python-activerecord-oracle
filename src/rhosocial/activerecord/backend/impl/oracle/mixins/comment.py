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

        Accepts any comment expression carrying ``object_type``,
        ``object_name`` and ``comment``.
        """
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "COMMENT ON",
                suggestion=(
                    f"Oracle {self.version} does not support COMMENT ON; "
                    "it requires Oracle 9i or later."
                ),
            )
        # Qualified names (schema.table[.column]) are quoted segment-by-segment
        # so dotted COLUMN targets stay valid Oracle references.
        object_sql = ".".join(
            self.format_identifier(part) for part in expr.object_name.split(".")
        )
        object_type = getattr(expr.object_type, "value", expr.object_type)
        head = f"COMMENT ON {object_type} {object_sql} IS"
        if expr.comment is None:
            return f"{head} NULL", ()
        escaped = self._escape_sql_string(expr.comment)
        return f"{head} '{escaped}'", ()
