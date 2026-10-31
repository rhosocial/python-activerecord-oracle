# src/rhosocial/activerecord/backend/impl/oracle/introspection/types.py
"""Oracle-specific introspection value objects.

Core's :mod:`rhosocial.activerecord.backend.introspection.types` describes
what every backend can answer about -- tables, columns, indexes and the
like. The objects below are Oracle's own: a sequence, a synonym and a
package are things the data dictionary holds and the core vocabulary has
no shape for, so they get their own frozen value objects here rather than
being bent into a table or a "miscellaneous" dict.

They are frozen for the same reason the extracted rows are: they describe
a database, and a caller that edits one is editing its own copy of an
answer. Reads are cached by the introspector, so a mutable result would
also be a shared result.
"""

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.introspection import SequenceInfo


@dataclass(frozen=True)
class OracleSequenceInfo:
    """One row of ``ALL_SEQUENCES``, as Oracle reports it.

    Attributes:
        name: Sequence name, upper case as Oracle stores it.
        owner: Schema (Oracle's "owner") the sequence belongs to.
        min_value: Lower bound the sequence can hand out.
        max_value: Upper bound the sequence can hand out.
        increment: Step between successive values (``INCREMENT_BY``).
        cycle: Whether the sequence wraps to ``min_value`` after
            ``max_value`` (``CYCLE_FLAG = 'Y'``).
        cache_size: How many values the instance pre-allocates per
            request (``CACHE_SIZE``; 0 means ``NOCACHE``).
        order: Whether values are guaranteed to be handed out in request
            order (``ORDER_FLAG = 'Y'``; needs a single-instance setup).
        last_number: The **next** value the sequence would hand out, not
            the last one it handed out.

            This is the single most misread column in the view. Oracle
            allocates values in cache blocks, so ``LAST_NUMBER`` is the
            high-water mark of the *current* cache: with
            ``CACHE 20`` and 3 values consumed, it reads 20 -- the next
            value the instance would hand out after the block is
            exhausted -- not 3 and not the 4th. Under ``NOCACHE`` the two
            readings coincide; under ``CACHE`` they do not, and callers
            that treat it as "last used" will silently believe the
            sequence is further ahead than it is.
    """

    name: str
    owner: str
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    increment: Optional[int] = None
    cycle: bool = False
    cache_size: Optional[int] = None
    order: bool = False
    last_number: Optional[int] = None

    def to_core(self) -> "SequenceInfo":
        """The cross-dialect minimum this answer carries.

        Core's ``SequenceInfo`` names the smallest shape every engine with
        sequences answers, and this is the projection onto it: ``owner``
        lands on ``schema``, ``min_value``/``max_value`` on
        ``minimum_value``/``maximum_value``, ``last_number`` on
        ``last_value`` (with the semantics its docstring warns about: the
        *next* value the cache would hand out, not the last one used).
        ``start_value`` is ``None`` because ``ALL_SEQUENCES`` reports no
        START_WITH -- the declaration's start is history the view does not
        keep -- and ``order`` stays here: it is a deployment guarantee the
        common minimum has no field for
        (``.claude/rules/introspection.md`` §5).
        """
        from rhosocial.activerecord.backend.introspection import SequenceInfo

        return SequenceInfo(
            name=self.name,
            schema=self.owner,
            start_value=None,
            minimum_value=self.min_value,
            maximum_value=self.max_value,
            increment=self.increment,
            cycle=self.cycle,
            cache_size=self.cache_size,
            last_value=self.last_number,
        )


@dataclass(frozen=True)
class OracleSynonymInfo:
    """One row of ``ALL_SYNONYMS``, as Oracle reports it.

    Attributes:
        name: Synonym name (``SYNONYM_NAME``).
        owner: Schema that owns the synonym. Public synonyms are owned by
            ``PUBLIC``, which is a name in this view rather than a schema
            in the usual sense -- see
            :meth:`~..introspector.OracleIntrospectorMixin.list_synonyms`.
        table_owner: Schema of the object the synonym points at.
        table_name: Name of the object the synonym points at. For a
            synonym over a database link this is the object *at the far
            end*, not a local one.
        db_link: Name of the database link the synonym resolves through,
            or ``None`` for a purely local synonym.
        is_public: ``True`` when the synonym belongs to ``PUBLIC`` and is
            therefore visible to every user without a schema prefix.
    """

    name: str
    owner: str
    table_owner: str
    table_name: str
    db_link: Optional[str] = None
    is_public: bool = False


@dataclass(frozen=True)
class OraclePackageInfo:
    """The specification row of a PL/SQL package in ``ALL_OBJECTS``.

    A package has two rows in ``ALL_OBJECTS`` -- ``PACKAGE`` for the
    specification and ``PACKAGE BODY`` for the implementation -- and this
    value object is the specification row only. That is the row that
    decides whether the package *exists*; a body can be missing or
    invalid without the specification going away.

    Attributes:
        name: Package name (``OBJECT_NAME``).
        owner: Schema the package belongs to.
        status: ``VALID`` or ``INVALID``, as compiled.
        created_time: Creation timestamp, stringified by the parser so the
            value survives JSON/CSV rendering unchanged.
        last_ddl_time: Last DDL timestamp, same treatment. This is when
            the specification was last (re)compiled or altered.
    """

    name: str
    owner: str
    status: str
    created_time: Optional[str] = None
    last_ddl_time: Optional[str] = None
