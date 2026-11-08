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
also be a shared result. The one exception is
:class:`OracleColumnDataProfile`, which *extends* a core dataclass and
therefore cannot be frozen; it says so where it is declared.
"""

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.introspection.types import ColumnDataProfile

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


# Deliberately *not* frozen, unlike the three types above: it inherits core's
# ``ColumnDataProfile``, which is a mutable dataclass, and Python refuses to
# build a frozen dataclass on top of a non-frozen one. Consistency with the
# backend's other value objects is worth less than the family fitting the
# slot core reserved for it -- a caller holding ``ColumnDataProfile`` must be
# able to treat this as one, and subclassing is how that is spelled.
@dataclass
class OracleColumnDataProfile(ColumnDataProfile):
    """A column profile carrying the extra facts ``ALL_TAB_COLUMNS`` supplies.

    Named fields rather than ``extra`` entries: these are Oracle's own
    statistics columns, and a consumer that knows it asked Oracle should read
    them off the object instead of guessing at dictionary keys.

    ``sample_size`` is ``SAMPLE_SIZE``: the rows that were sampled **and were
    not null**. Oracle counts the nulls separately in ``NUM_NULLS`` and leaves
    them out of this figure, so it is *not* the number of rows the figures
    describe -- the two parts added together are. Core's ``row_count`` carries
    that total, because a profile's row count must be the population its
    fractions were derived from; ``sample_size`` keeps the dictionary's own
    number beside it. The distinction is worth having: a column whose
    ``sample_size`` is 400 while ``row_count`` is 1000 is 60% null, and a
    column whose ``sample_size`` is 1000 with the same ``row_count`` is not.

    ``histogram`` is ``HISTOGRAM`` -- ``NONE``, ``FREQUENCY``,
    ``HEIGHT BALANCED``, ``TOP-FREQUENCY`` or ``HYBRID``. It matters because
    it decides how exact ``distinct_count`` is: under a ``FREQUENCY``
    histogram every distinct value has a bucket of its own and ``NUM_DISTINCT``
    is an exact count; without one it is an estimate derived from a sample.
    Core has no slot for this, and leaving it out would make two profiles
    with very different confidence look identical.

    ``data_type`` is the declared type as the dictionary spells it
    (``NUMBER``, ``DATE``, ``VARCHAR2``, ...). It is here for the one thing
    this profile cannot answer: ``min_value``/``max_value`` are always
    ``None`` on Oracle because ``LOW_VALUE``/``HIGH_VALUE`` are RAW, so a
    reader looking for an extreme needs to know which type it would have
    been before deciding whether to decode the RAW itself.
    """

    #: Rows sampled that were not null (``SAMPLE_SIZE``). The rows the
    #: profile describes are this plus ``num_nulls``, which core's
    #: ``row_count`` carries.
    sample_size: Optional[int] = None
    #: The histogram kind Oracle built for this column (``HISTOGRAM``).
    histogram: Optional[str] = None
    #: The declared type, in the dictionary's own spelling (``DATA_TYPE``).
    data_type: Optional[str] = None
