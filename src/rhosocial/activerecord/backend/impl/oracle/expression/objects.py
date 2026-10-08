# src/rhosocial/activerecord/backend/impl/oracle/expression/objects.py
"""Oracle schema objects -- the catalogue side of a name.

The core :mod:`rhosocial.activerecord.backend.expression.objects` package
answers "which object, in which namespace" with three fixed slots: catalog,
schema and name. Oracle fills two of them: ``schema_name`` is the owner, and
``catalog_name`` is always ``None`` because Oracle has no namespace above the
owner. That asymmetry is declared, not assumed -- the dialect answers ``True``
to ``supports_schema_qualification()`` and leaves ``supports_catalog()`` false,
which is what makes a stray ``catalog_name`` an error instead of a silently
dropped slot.

What Oracle adds on top of the core slots is the ``@"DBLINK"`` suffix:

    SELECT * FROM scott.orders@world

``@world`` is part of *the name Oracle resolves*, not a clause bolted on
afterwards, so it belongs here as a first-class field rather than in a
formatter. It is rendered by
:meth:`~...impl.oracle.mixins.namespace.OracleNamespaceMixin.format_table_object`,
so every statement that names a remote table picks it up for free.

Deliberately **not** modelled here is the flashback clause (``AS OF SCN 123``
/ ``VERSIONS BETWEEN ...``). A flashback clause says *when* to read a
relation, which is a property of the statement being built, not of the object
in the catalogue; two queries against the same table differ only by their
flashback clause, yet they name one and the same table. It therefore travels
on the query side -- see
:class:`~...impl.oracle.expression.sources.OracleNamedRelationRef`.
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.objects import RoutineObject, Table

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase

__all__ = ["OracleRemoteName", "OraclePackage", "OracleTable"]


class OracleRemoteName:
    """Adds Oracle's ``@dblink`` suffix to a schema object's name.

    Mixed in **before** the core object class so that ``__init__`` and
    ``identity`` chain upwards::

        class OracleTable(OracleRemoteName, Table): ...

    The suffix participates in equality and hashing, because two references
    that differ only by dblink resolve to different objects and must never
    compare equal.

    The field is validated exactly like a namespace slot: ``None`` or a
    non-empty string. An empty string is rejected rather than treated as
    absent, since ``name`` and ``name@`` are different names.

    The signature spells out every parameter rather than accepting ``*args`` and
    ``**slots``. That is not a style preference: both the serializer's
    ``get_params`` and its ``_reconstruct`` read
    ``inspect.signature(cls.__init__)``, and this class *is* that ``__init__`` for
    every class built on it. Behind ``*args`` / ``**slots`` the serializer found
    two parameters named ``args`` and ``slots``, could resolve neither, warned,
    and emitted ``{"dblink": None, "dblink_need_quote": True}`` -- the name, the
    owner and the three quoting flags silently gone. Reconstructing that payload
    then passed the dialect as a keyword into ``**slots``, where
    :class:`SchemaObject` could not see it, and raised for a missing positional
    argument. So the round-trip was lossy on the way out and broken on the way
    back, and the expression matrix is what found it: nothing in this backend had
    ever serialised an :class:`OracleTable`.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        *,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        catalog_need_quote: bool = True,
        schema_need_quote: bool = True,
        name_need_quote: bool = True,
        dblink: Optional[str] = None,
        dblink_need_quote: bool = True,
    ) -> None:
        """Record the object's identity plus the database link it is read through.

        Args:
            dialect: The dialect that will render this object.
            name: The object's own name, unqualified.
            catalog_name: Outermost namespace, or ``None``. Oracle has none, and
                a value here is refused when the name is rendered.
            schema_name: The owner.
            catalog_need_quote: Whether the catalog is quoted when rendered.
            schema_need_quote: Whether the owner is quoted when rendered.
            name_need_quote: Whether the name is quoted when rendered.
            dblink: Database link appended as ``@"dblink"``, or ``None`` for a
                local object.
            dblink_need_quote: Whether the link name is quoted when rendered.

        Raises:
            ValueError: ``name`` is empty, a namespace slot is empty, or
                ``dblink`` is neither ``None`` nor a non-empty string.
        """
        super().__init__(
            dialect,
            name,
            catalog_name=catalog_name,
            schema_name=schema_name,
            catalog_need_quote=catalog_need_quote,
            schema_need_quote=schema_need_quote,
            name_need_quote=name_need_quote,
        )
        self.dblink = self._check_slot(dblink, "dblink")
        self.dblink_need_quote = dblink_need_quote

    @staticmethod
    def _check_slot(value: Optional[str], label: str) -> Optional[str]:
        """Validate a namespace-shaped slot the way the core object validates its own.

        ``None`` means "this level is not carried"; anything else must be a
        non-empty string. An empty string is rejected rather than treated as
        absent, because ``""`` and ``None`` render differently and only the
        caller knows which was meant.
        """
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} must be a non-empty string or None")
        return value

    def identity(self) -> tuple:
        """The comparable payload, widened with the link the name is read through."""
        return super().identity() + (self.dblink, self.dblink_need_quote)

    def get_params(self) -> dict:
        """The serializable payload, widened with the link the name is read through.

        The base's introspective default now sees every field, because this
        class spells its parameters out (see the class docstring for what
        happened when it did not). What it cannot see is that ``dblink`` and
        ``dblink_need_quote`` are added *here* rather than declared further up:
        ``Table.__init__`` does not name them, and the convention resolves a
        parameter against the attribute of the same name on *this* class, so both
        are found -- but only because they are stored under exactly those names.
        Stating that here means the pair is not an accident of naming.
        """
        return {**super().get_params(), "dblink": self.dblink}


class OracleTable(OracleRemoteName, Table):
    """A table that may live behind a database link.

    Oracle resolves ``schema.table@link`` as a single name, so the link is
    part of the table's identity and this object renders as
    ``"SCHEMA"."TABLE"@"LINK"``. Everything else -- what a table *is*, how a
    ``FROM`` clause reads it -- is inherited unchanged, so an ``OracleTable``
    is usable everywhere a core ``Table`` is.
    """


class OraclePackage(RoutineObject):
    """A PL/SQL package: the container a procedure or function lives in.

    The core object tree gives a package no kind of its own, and rightly so:
    Oracle is the only engine that has one. It also means
    :class:`~rhosocial.activerecord.backend.expression.objects.RoutineObject`
    declares no ``format_method`` of its own, being the shared kind a
    procedure and a function agree on while differing from. A package is
    neither -- it is not callable -- so it needs its own kind here, and it is
    rendered through
    :meth:`~...impl.oracle.mixins.routine.OracleRoutineMixin.format_package_object`
    like every other named object in the backend.
    """

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this package."""
        return "format_package_object"
