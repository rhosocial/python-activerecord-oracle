# src/rhosocial/activerecord/backend/impl/oracle/mixins/sequence.py
"""Oracle sequence value and DDL formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.statements import (
        AlterSequenceExpression,
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

    ``START WITH`` is a ``CREATE SEQUENCE`` option only: Oracle refuses it on
    ``ALTER SEQUENCE`` (``ORA-02283``), so :meth:`supports_alter_sequence_start`
    is ``False`` and the ALTER formatter refuses the expression's ``start``
    field. The clause that resets a sequence's current value on ALTER is
    ``RESTART START WITH n`` -- not core's ``RESTART WITH n`` -- and
    :meth:`format_alter_sequence_statement` is where that spelling lives.
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
        """``START WITH`` is a ``CREATE SEQUENCE`` clause in Oracle.

        This describes the clause that sets the *initial* value when the
        sequence is created. It does not describe ``ALTER SEQUENCE``: Oracle
        answers ``ORA-02283: cannot alter starting sequence number`` for
        ``ALTER SEQUENCE ... START WITH``, so the ALTER formatter consults
        :meth:`supports_alter_sequence_start` instead.
        """
        return True

    def supports_alter_sequence_start(self) -> bool:
        """Whether ``ALTER SEQUENCE`` accepts a ``START WITH`` clause.

        This describes a different clause from the one
        :meth:`supports_sequence_start` describes. That probe answers for the
        ``CREATE SEQUENCE ... START WITH`` clause, which sets the *initial*
        value, and says nothing about ALTER. Oracle creates a sequence with
        ``START WITH`` but refuses to change the starting value later
        (``ORA-02283: cannot alter starting sequence number``), so the CREATE
        probe cannot stand in for this one and this answers ``False``.

        To reset a sequence's current value on ALTER, Oracle spells the clause
        ``RESTART START WITH n`` -- not core's ``RESTART WITH n`` -- which is
        why :meth:`format_alter_sequence_statement` emits that for the
        expression's ``restart`` field.

        The default direction is ``False``, deliberately: a probe that answered
        ``True`` by default would let a formatter emit ``START WITH`` on ALTER
        and hand the server SQL it rejects, whereas ``False`` fails closed.
        """
        return False

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

        The option clauses -- ``MINVALUE``, ``MAXVALUE``, ``CYCLE`` /
        ``NOCYCLE``, ``CACHE`` / ``NOCACHE``, ``ORDER`` / ``NOORDER`` -- are
        each gated on the probe for that option before the clause is emitted,
        exactly as :meth:`format_alter_sequence_statement` gates them. A probe
        answering ``False`` refuses the option rather than dropping it, so the
        declaration governs the rendering instead of decorating it.

        Raises:
            TypeError: ``expr.sequence`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Sequence`.
                A table would render its own name, producing a well-formed
                CREATE SEQUENCE over that table's name.
            UnsupportedFeatureError: The Oracle version is below 9i, IF NOT
                EXISTS was asked for below 23ai, OWNED BY was requested --
                which Oracle has no form of -- or an option Oracle does not
                model was requested.
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
            if not self.supports_sequence_minvalue():
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE SEQUENCE MINVALUE",
                    suggestion=(
                        f"{self.name} does not support the MINVALUE sequence "
                        "option."
                    ),
                )
            parts.append(f"MINVALUE {expr.minvalue}")
        if expr.maxvalue is not None:
            if not self.supports_sequence_maxvalue():
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE SEQUENCE MAXVALUE",
                    suggestion=(
                        f"{self.name} does not support the MAXVALUE sequence "
                        "option."
                    ),
                )
            parts.append(f"MAXVALUE {expr.maxvalue}")
        if expr.cycle is not None:
            if expr.cycle:
                if not self.supports_sequence_cycle():
                    raise UnsupportedFeatureError(
                        self.name,
                        "CREATE SEQUENCE CYCLE",
                        suggestion=(
                            f"{self.name} does not support the CYCLE sequence "
                            "option."
                        ),
                    )
                parts.append("CYCLE")
            elif self.supports_sequence_cycle():
                # NOCYCLE is Oracle's spelling of the default.
                parts.append("NOCYCLE")
        if expr.cache is not None:
            if not self.supports_sequence_cache():
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE SEQUENCE CACHE",
                    suggestion=(
                        f"{self.name} does not support the CACHE sequence "
                        "option."
                    ),
                )
            parts.append(f"CACHE {expr.cache}" if expr.cache else "NOCACHE")
        if expr.order is not None:
            if not self.supports_sequence_order():
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE SEQUENCE ORDER",
                    suggestion=(
                        f"{self.name} does not support the ORDER sequence "
                        "option."
                    ),
                )
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

    def format_alter_sequence_statement(
        self, expr: "AlterSequenceExpression"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER SEQUENCE`` for Oracle.

        Oracle's ALTER grammar differs from the SQL standard core renders in
        two ways, so this formatter is Oracle's own:

        * The clause that resets the current value is
          ``RESTART START WITH n``, not core's ``RESTART WITH n``. The server
          answers ``ORA-03048: SQL reserved word 'WITH' is not syntactically
          valid`` for the core spelling and accepts ``RESTART START WITH n``
          (and the bare ``RESTART``). This is a spelling difference, not a
          missing clause: the expression's ``restart`` field is renderable.
        * ``START WITH`` is a ``CREATE SEQUENCE`` option only. Oracle refuses it
          on ALTER with ``ORA-02283: cannot alter starting sequence number``, so
          a request for the expression's ``start`` field is refused through
          :meth:`supports_alter_sequence_start` rather than rendered.

        The remaining clauses -- ``INCREMENT BY``, ``MINVALUE``, ``MAXVALUE``,
        ``CACHE`` / ``NOCACHE``, ``CYCLE`` / ``NOCYCLE``, ``ORDER`` /
        ``NOORDER`` -- are each gated on the probe for that option before the
        clause is emitted, exactly as the CREATE formatter does. ``MINVALUE`` is
        not gated off: Oracle accepts it on ALTER, and a value Oracle rejects
        (``ORA-04007``) is a value constraint, not a missing clause.

        Raises:
            TypeError: ``expr.sequence`` is not a
                :class:`~rhosocial.activerecord.backend.expression.objects.Sequence`.
                A table would render its own name, producing a well-formed
                ALTER SEQUENCE over that table's name.
            UnsupportedFeatureError: The Oracle version is below 9i, ``start``
                was requested (Oracle has no ALTER form of it), or an option
                Oracle does not model was requested.
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
                "ALTER SEQUENCE",
                suggestion=(
                    f"Oracle {self.version} does not support sequences; "
                    "sequences require Oracle 9i or later."
                ),
            )
        parts = [f"ALTER SEQUENCE {expr.sequence.to_sql()[0]}"]

        if expr.restart is not None:
            # Oracle spells this RESTART START WITH, not the standard
            # RESTART WITH; the core formatter's spelling is a syntax error.
            parts.append(f"RESTART START WITH {expr.restart}")
        if expr.start is not None:
            if not self.supports_alter_sequence_start():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER SEQUENCE START",
                    suggestion=(
                        "Oracle cannot alter a sequence's starting number; use "
                        "RESTART START WITH to reset its current value instead."
                    ),
                )
            parts.append(f"START WITH {expr.start}")
        if expr.increment is not None:
            if not self.supports_sequence_increment():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER SEQUENCE INCREMENT",
                    suggestion=(
                        f"{self.name} does not support the INCREMENT BY "
                        "sequence option."
                    ),
                )
            parts.append(f"INCREMENT BY {expr.increment}")
        if expr.minvalue is not None:
            if not self.supports_sequence_minvalue():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER SEQUENCE MINVALUE",
                    suggestion=(
                        f"{self.name} does not support the MINVALUE sequence "
                        "option."
                    ),
                )
            parts.append(f"MINVALUE {expr.minvalue}")
        if expr.maxvalue is not None:
            if not self.supports_sequence_maxvalue():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER SEQUENCE MAXVALUE",
                    suggestion=(
                        f"{self.name} does not support the MAXVALUE sequence "
                        "option."
                    ),
                )
            parts.append(f"MAXVALUE {expr.maxvalue}")
        if expr.cycle is not None:
            if expr.cycle:
                if not self.supports_sequence_cycle():
                    raise UnsupportedFeatureError(
                        self.name,
                        "ALTER SEQUENCE CYCLE",
                        suggestion=(
                            f"{self.name} does not support the CYCLE sequence "
                            "option."
                        ),
                    )
                parts.append("CYCLE")
            elif self.supports_sequence_cycle():
                # NOCYCLE is Oracle's spelling of the default.
                parts.append("NOCYCLE")
        if expr.cache is not None:
            if not self.supports_sequence_cache():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER SEQUENCE CACHE",
                    suggestion=(
                        f"{self.name} does not support the CACHE sequence "
                        "option."
                    ),
                )
            parts.append(f"CACHE {expr.cache}" if expr.cache else "NOCACHE")
        if expr.order is not None:
            if not self.supports_sequence_order():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER SEQUENCE ORDER",
                    suggestion=(
                        f"{self.name} does not support the ORDER sequence "
                        "option."
                    ),
                )
            parts.append("ORDER" if expr.order else "NOORDER")
        if expr.owned_by is not None:
            if not self.supports_sequence_owned_by():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER SEQUENCE OWNED BY",
                    suggestion=(
                        "Oracle sequences are not owned by a table column; use "
                        "a BEFORE INSERT trigger or a 12c identity column "
                        "instead."
                    ),
                )

        return " ".join(parts), ()
