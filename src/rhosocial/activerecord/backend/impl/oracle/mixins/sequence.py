# src/rhosocial/activerecord/backend/impl/oracle/mixins/sequence.py
"""Oracle sequence value and DDL formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.statements import (
        CreateSequenceExpression,
        DropSequenceExpression,
    )


class OracleSequenceMixin:
    """Oracle sequence management capability checks and formatters.

    Sequences are an ancient Oracle feature; the formatters here gate on
    ``(9, 0, 0)`` per the backend implementation contract. Oracle accesses
    sequence values through the ``NEXTVAL`` / ``CURRVAL`` pseudo-columns
    (``seq.NEXTVAL``), not the SQL-standard ``NEXT VALUE FOR``.
    """

    def supports_create_sequence(self) -> bool:
        return True

    def supports_drop_sequence(self) -> bool:
        return True

    def format_nextval(self, expr) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "sequence NEXTVAL",
                suggestion=(
                    f"Oracle {self.version} does not support sequence "
                    "pseudo-columns; NEXTVAL requires Oracle 9i or later."
                ),
            )
        return f"{self.format_identifier(expr.sequence)}.NEXTVAL", ()

    def format_currval(self, expr) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "sequence CURRVAL",
                suggestion=(
                    f"Oracle {self.version} does not support sequence "
                    "pseudo-columns; CURRVAL requires Oracle 9i or later."
                ),
            )
        return f"{self.format_identifier(expr.sequence)}.CURRVAL", ()

    def format_create_sequence_statement(
        self, expr: "CreateSequenceExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE SEQUENCE",
                suggestion=(
                    f"Oracle {self.version} does not support sequences; "
                    "sequences require Oracle 9i or later."
                ),
            )
        parts = ["CREATE SEQUENCE"]
        if getattr(expr, "if_not_exists", False):
            if self.version < (23, 0, 0):
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE SEQUENCE IF NOT EXISTS",
                    suggestion=(
                        f"Oracle {self.version} does not support IF NOT "
                        "EXISTS; graceful DDL requires Oracle 23ai or later."
                    ),
                )
            parts.append("IF NOT EXISTS")
        parts.append(self.format_identifier(expr.sequence_name))
        if expr.start is not None:
            parts.append(f"START WITH {expr.start}")
        if expr.increment is not None:
            parts.append(f"INCREMENT BY {expr.increment}")
        if expr.minvalue is not None:
            parts.append(f"MINVALUE {expr.minvalue}")
        if expr.maxvalue is not None:
            parts.append(f"MAXVALUE {expr.maxvalue}")
        if expr.cycle is not None:
            parts.append("CYCLE" if expr.cycle else "NOCYCLE")
        if expr.cache is not None:
            parts.append(f"CACHE {expr.cache}" if expr.cache else "NOCACHE")
        if expr.order is not None:
            parts.append("ORDER" if expr.order else "NOORDER")
        if getattr(expr, "owned_by", None) is not None:
            raise UnsupportedFeatureError(
                self.name,
                "CREATE SEQUENCE ... OWNED BY",
                suggestion=(
                    "Oracle sequences are not owned by a table column; use "
                    "a BEFORE INSERT trigger or a 12c identity column instead."
                ),
            )
        return " ".join(parts), ()

    def format_drop_sequence_statement(
        self, expr: "DropSequenceExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "DROP SEQUENCE",
                suggestion=(
                    f"Oracle {self.version} does not support sequences; "
                    "sequences require Oracle 9i or later."
                ),
            )
        parts = ["DROP SEQUENCE"]
        if getattr(expr, "if_exists", False):
            if self.version < (23, 0, 0):
                raise UnsupportedFeatureError(
                    self.name,
                    "DROP SEQUENCE IF EXISTS",
                    suggestion=(
                        f"Oracle {self.version} does not support IF EXISTS; "
                        "graceful DDL requires Oracle 23ai or later."
                    ),
                )
            parts.append("IF EXISTS")
        parts.append(self.format_identifier(expr.sequence_name))
        return " ".join(parts), ()
