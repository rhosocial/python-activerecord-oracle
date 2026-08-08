# src/rhosocial/activerecord/backend/impl/oracle/mixins/comment.py
"""Oracle COMMENT ON formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.comment import OracleCommentExpression


class OracleCommentMixin:
    """Oracle ``COMMENT ON`` capability check and formatter.

    ``COMMENT ON`` has existed since early Oracle releases; the formatter
    gates on ``(9, 0, 0)`` per the backend implementation contract. Oracle
    stores comments on schema objects (and columns) through a standalone
    statement, never through an inline column clause.
    """

    def supports_comment(self) -> bool:
        return True

    def format_comment_statement(
        self, expr: "OracleCommentExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "COMMENT ON",
                suggestion=(
                    f"Oracle {self.version} does not support COMMENT ON; "
                    "it requires Oracle 9i or later."
                ),
            )
        object_sql = self.format_identifier(expr.object_name)
        head = f"COMMENT ON {expr.object_type.value} {object_sql} IS"
        if expr.comment is None:
            return f"{head} NULL", ()
        escaped = self._escape_sql_string(expr.comment)
        return f"{head} '{escaped}'", ()
