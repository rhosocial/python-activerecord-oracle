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

    Both sides of the statement are catalogue objects, so each is rendered by
    its own protocol: the synonym as a :class:`Synonym`, its target as a
    :class:`Table`. Rendering one through an object and the other through a bare
    identifier would make the two halves of one statement follow different
    quoting rules.
    """

    def supports_create_synonym(self) -> bool:
        return True

    def supports_drop_synonym(self) -> bool:
        return True

    def format_create_synonym_statement(
        self, expr: "OracleCreateSynonymExpression"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE [PUBLIC] SYNONYM`` for Oracle.

        Raises:
            TypeError: ``expr.synonym`` is not a ``Synonym``, or ``expr.table``
                is not a ``Table``. Each side renders through its own object
                protocol, so a wrong kind on either side would produce a
                well-formed synonym naming the wrong thing.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        from rhosocial.activerecord.backend.expression.objects import (
            Synonym,
            Table,
        )

        if not isinstance(expr.synonym, Synonym):
            raise TypeError(
                f"OracleCreateSynonymExpression.synonym must be a Synonym, "
                f"got {type(expr.synonym).__name__}"
            )
        if not isinstance(expr.table, Table):
            raise TypeError(
                f"OracleCreateSynonymExpression.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )

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
        parts.append(expr.synonym.to_sql()[0])
        parts.append("FOR")
        parts.append(expr.table.to_sql()[0])
        return " ".join(parts), ()

    def format_drop_synonym_statement(
        self, expr: "OracleDropSynonymExpression"
    ) -> Tuple[str, tuple]:
        """Format ``DROP [PUBLIC] SYNONYM`` for Oracle.

        Raises:
            TypeError: ``expr.synonym`` is not a ``Synonym``. Any other object
                renders its own name, so the statement would drop something else
                and name a synonym.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        from rhosocial.activerecord.backend.expression.objects import Synonym

        if not isinstance(expr.synonym, Synonym):
            raise TypeError(
                f"OracleDropSynonymExpression.synonym must be a Synonym, "
                f"got {type(expr.synonym).__name__}"
            )

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
        parts.append(expr.synonym.to_sql()[0])
        if expr.force:
            parts.append("FORCE")
        return " ".join(parts), ()
