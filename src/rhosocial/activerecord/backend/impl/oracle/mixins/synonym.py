# src/rhosocial/activerecord/backend/impl/oracle/mixins/synonym.py
"""Oracle SYNONYM DDL formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.ddl.synonym import (
        OracleCreateSynonymExpression,
        OracleDropSynonymExpression,
    )


class OracleSynonymMixin:
    """Oracle synonym capability checks and formatters.

    Synonyms have existed since early Oracle releases; the formatters gate
    on ``(9, 0, 0)`` per the backend implementation contract.
    """

    def supports_create_synonym(self) -> bool:
        return True

    def supports_drop_synonym(self) -> bool:
        return True

    def format_create_synonym_statement(
        self, expr: "OracleCreateSynonymExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE SYNONYM",
                suggestion=(
                    f"Oracle {self.version} does not support synonyms; "
                    "they require Oracle 9i or later."
                ),
            )
        parts = ["CREATE"]
        if expr.public:
            parts.append("PUBLIC")
        parts.append("SYNONYM")
        parts.append(self.format_identifier(expr.synonym_name))
        parts.append("FOR")
        if expr.schema_name:
            target = (
                f"{self.format_identifier(expr.schema_name)}."
                f"{self.format_identifier(expr.table)}"
            )
        else:
            target = self.format_identifier(expr.table)
        parts.append(target)
        return " ".join(parts), ()

    def format_drop_synonym_statement(
        self, expr: "OracleDropSynonymExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "DROP SYNONYM",
                suggestion=(
                    f"Oracle {self.version} does not support synonyms; "
                    "they require Oracle 9i or later."
                ),
            )
        parts = ["DROP"]
        if expr.public:
            parts.append("PUBLIC")
        parts.append("SYNONYM")
        parts.append(self.format_identifier(expr.synonym_name))
        if expr.force:
            parts.append("FORCE")
        return " ".join(parts), ()
