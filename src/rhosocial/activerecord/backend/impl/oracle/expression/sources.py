# src/rhosocial/activerecord/backend/impl/oracle/expression/sources.py
"""Oracle row sources -- the query side of a name.

The core tree keeps two things apart on purpose:

* :mod:`...expression.objects` answers *which object, in which namespace* --
  a catalogue identity, with no syntax position and no alias;
* :mod:`...expression.sources` answers *where in the ``FROM`` clause* -- a
  row source, which may carry an alias and engine-specific clauses.

A flashback clause (``AS OF SCN 1234567``, ``VERSIONS BETWEEN ...``) belongs
to the second group: it changes when a relation is read, not which relation it
is. ``SELECT * FROM orders`` and ``SELECT * FROM orders AS OF SCN 42`` name
one table, so the clause cannot live on the table. Oracle therefore adds it to
the reference, and the dialect appends it when the reference is rendered.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from rhosocial.activerecord.backend.expression.sources import NamedRelationRef

__all__ = ["OracleNamedRelationRef"]


class OracleNamedRelationRef(NamedRelationRef):
    """A ``FROM`` reference that may read the relation as of a past moment.

    Identical to the core :class:`NamedRelationRef` except for one added
    clause: ``flashback`` is an :class:`~...expression.flashback.OracleAsOfClause`
    or :class:`~...expression.flashback.OracleVersionsBetweenClause` rendered
    between the relation's name and its alias::

        "SCOTT"."ORDERS" AS OF SCN 1234567 AS o

    The clause is **not** part of the relation's identity, so it is not part
    of this reference's ``relation`` object -- see
    :mod:`...expression.objects` for why the ``@dblink`` suffix is handled the
    opposite way.
    """

    __slots__ = ("flashback",)

    def __init__(
        self,
        dialect: Any,
        relation: Any,
        temporal_options: Optional[Dict[str, Any]] = None,
        alias: Optional[str] = None,
        alias_need_quote: bool = True,
        flashback: Optional[Any] = None,
    ) -> None:
        """Point this reference at a relation, optionally reading it in the past.

        Args:
            dialect: The dialect that will render this reference.
            relation: The relation being read. Must be a
                :class:`~rhosocial.activerecord.backend.expression.objects.RelationObject`
                -- typically an
                :class:`~...impl.oracle.expression.objects.OracleTable` when the
                table is remote.
            temporal_options: SQL-standard ``FOR SYSTEM_TIME`` options, which
                Oracle does not implement; kept for interface compatibility.
            alias: Name this source is known by inside the query.
            alias_need_quote: Whether the alias is quoted when rendered.
            flashback: Oracle flashback clause to append after the name, or
                ``None`` to read the relation as it is now.

        Raises:
            TypeError: ``relation`` is not a relation object.
        """
        super().__init__(
            dialect,
            relation,
            temporal_options=temporal_options,
            alias=alias,
            alias_need_quote=alias_need_quote,
        )
        self.flashback = flashback
