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

    Sequences are an ancient Oracle feature; the master switch and every
    option probe below carry the version boundary, so the formatters no longer
    hard-code one. Oracle accesses sequence values through the ``NEXTVAL`` /
    ``CURRVAL`` pseudo-columns (``seq.NEXTVAL``), not the SQL-standard
    ``NEXT VALUE FOR``.

    Oracle 8i and earlier have no sequence object at all, so
    :meth:`supports_sequence` is a version check rather than an unconditional
    ``True``; ``CREATE`` / ``DROP`` / ``ALTER SEQUENCE`` inherit that answer.
    ``IF NOT EXISTS`` / ``IF EXISTS`` arrived in 23ai. Every other option --
    ``START WITH``, ``INCREMENT BY``, ``MINVALUE``, ``MAXVALUE``, ``CYCLE``,
    ``CACHE`` and ``ORDER`` -- has been accepted since 9i. ``OWNED BY`` has no
    Oracle form at all.
    """

    def supports_sequence(self) -> bool:
        """Oracle supports sequence objects from 9i onward."""
        return self.version >= (9, 0, 0)

    def supports_create_sequence(self) -> bool:
        return self.supports_sequence()

    def supports_drop_sequence(self) -> bool:
        return self.supports_sequence()

    def supports_alter_sequence(self) -> bool:
        return self.supports_sequence()

    def supports_sequence_if_not_exists(self) -> bool:
        """``CREATE SEQUENCE IF NOT EXISTS`` arrived in Oracle 23ai."""
        return self.version >= (23, 0, 0)

    def supports_sequence_if_exists(self) -> bool:
        """``DROP SEQUENCE IF EXISTS`` arrived in Oracle 23ai."""
        return self.version >= (23, 0, 0)

    def supports_sequence_start(self) -> bool:
        return True

    def supports_sequence_increment(self) -> bool:
        return True

    def supports_sequence_minvalue(self) -> bool:
        return True

    def supports_sequence_maxvalue(self) -> bool:
        return True

    def supports_sequence_cycle(self) -> bool:
        return True

    def supports_sequence_cache(self) -> bool:
        return True

    def supports_sequence_order(self) -> bool:
        return True

    def supports_sequence_owned_by(self) -> bool:
        """Oracle has no ``OWNED BY`` clause on sequences."""
        return False

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
        """Format ``CREATE SEQUENCE`` for Oracle.

        Raises:
            TypeError: ``expr.sequence`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Sequence`.
                A table would render its own name, producing a well-formed
                CREATE SEQUENCE over that table's name.
            UnsupportedFeatureError: The Oracle version is below 9i, IF NOT
                EXISTS was asked for below 23ai, or OWNED BY was requested --
                which Oracle has no form of.
        """
        from rhosocial.activerecord.backend.expression.objects import Sequence

        if not isinstance(expr.sequence, Sequence):
            raise TypeError(
                f"{type(expr).__name__}.sequence must be a Sequence, "
                f"got {type(expr.sequence).__name__}"
            )

        if not self.supports_sequence():
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
            if not self.supports_sequence_if_not_exists():
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE SEQUENCE IF NOT EXISTS",
                    suggestion=(
                        f"{self.name} does not support CREATE SEQUENCE "
                        "IF NOT EXISTS."
                    ),
                )
            parts.append("IF NOT EXISTS")
        parts.append(expr.sequence.to_sql()[0])
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
            if not self.supports_sequence_owned_by():
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
        """Format ``DROP SEQUENCE`` for Oracle.

        Raises:
            TypeError: ``expr.sequence`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Sequence`.
                A table would render its own name, so the statement would drop a
                table and name a sequence.
            UnsupportedFeatureError: The Oracle version is below 9i, or IF
                EXISTS was asked for below 23ai.
        """
        from rhosocial.activerecord.backend.expression.objects import Sequence

        if not isinstance(expr.sequence, Sequence):
            raise TypeError(
                f"{type(expr).__name__}.sequence must be a Sequence, "
                f"got {type(expr.sequence).__name__}"
            )

        if not self.supports_sequence():
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
            if not self.supports_sequence_if_exists():
                raise UnsupportedFeatureError(
                    self.name,
                    "DROP SEQUENCE IF EXISTS",
                    suggestion=(
                        f"{self.name} does not support DROP SEQUENCE "
                        "IF EXISTS."
                    ),
                )
            parts.append("IF EXISTS")
        parts.append(expr.sequence.to_sql()[0])
        return " ".join(parts), ()
