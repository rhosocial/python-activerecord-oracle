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

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

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


class OraclePartitionStrategy(str, Enum):
    """The partitioning strategies ``ALL_PART_TABLES`` reports.

    Oracle states the strategy in two columns: ``PARTITIONING_TYPE`` names
    the top level (RANGE / LIST / HASH) and ``SUBPARTITIONING_TYPE`` names
    the second level of a composite table. Interval partitioning is RANGE
    with an ``INTERVAL`` expression rather than a declaration of its own,
    but it is a distinct answer for a caller deciding how a table grows, so
    it gets a member here. ``COMPOSITE`` likewise means "the table has a
    subpartitioning type", not a value Oracle stores in one column.
    """

    __str__ = str.__str__

    RANGE = "RANGE"
    LIST = "LIST"
    HASH = "HASH"
    COMPOSITE = "COMPOSITE"
    INTERVAL = "INTERVAL"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class OraclePartitionInfo:
    """One row of ``ALL_TAB_PARTITIONS``, as Oracle reports it.

    Attributes:
        owner: Table owner (Oracle's schema).
        table_name: The partitioned table.
        partition_name: The partition's name.
        strategy: The strategy derived from ``ALL_PART_TABLES`` -- RANGE,
            LIST, HASH, COMPOSITE or INTERVAL. A table that is both
            subpartitioned and interval-partitioned answers ``COMPOSITE``
            (its structure) and carries the interval expression in
            :attr:`interval`.
        partition_position: 1-based declaration order of the partition.
        high_value: The partition's upper bound, exactly as the dictionary
            spells it. ``LONG`` in the view, so no SELECT can compare it or
            decode it into a Python value; it travels as the text Oracle
            stores. For a MAXVALUE partition the text starts with
            ``MAXVALUE`` (see :attr:`is_max_value`).
        high_value_length: ``HIGH_VALUE_LENGTH``, the length of that text.
        tablespace_name: The tablespace the partition's segment lives in.
        subpartition_count: ``SUBPARTITION_COUNT``; zero for a table that
            has no subpartitions.
        subpartition_strategy: ``SUBPARTITIONING_TYPE`` when the table is
            composite, otherwise ``None``.
        interval: ``ALL_PART_TABLES.INTERVAL`` for an interval-partitioned
            table -- the expression Oracle uses to create the next partition
            -- otherwise ``None``.
        row_count: ``NUM_ROWS``, the optimiser's row estimate.
        size_bytes: ``BLOCKS`` multiplied by the tablespace's own
            ``BLOCK_SIZE``; ``None`` when either factor is unreadable.
        compression: ``COMPRESSION`` as the dictionary spells it.
        segment_created: ``SEGMENT_CREATED``.
        read_only: ``READ_ONLY``.
        extra: Engine facts not given a named slot above.
    """

    owner: str
    table_name: str
    partition_name: str
    strategy: OraclePartitionStrategy
    partition_position: Optional[int] = None
    high_value: Optional[str] = None
    high_value_length: Optional[int] = None
    tablespace_name: Optional[str] = None
    subpartition_count: Optional[int] = None
    subpartition_strategy: Optional[str] = None
    interval: Optional[str] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    compression: Optional[str] = None
    segment_created: Optional[str] = None
    read_only: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_max_value(self) -> bool:
        """Whether the upper bound is Oracle's unbounded ``MAXVALUE``.

        ``HIGH_VALUE`` is padded text rather than a value, so the test is on
        the leading word, not on an equality: a MAXVALUE partition reports
        ``"MAXVALUE"`` or ``"MAXVALUE "`` depending on the partition's
        position.
        """
        return (self.high_value or "").strip().upper().startswith("MAXVALUE")


@dataclass(frozen=True)
class OracleRoutineParameter:
    """One ``ALL_ARGUMENTS`` row for a standalone procedure or function.

    Attributes:
        name: ``ARGUMENT_NAME``. ``None`` for a function's return row, which
            is position 0.
        position: 1-based parameter position, or 0 for a function's return.
        data_type: The declared type as the dictionary spells it, with
            precision/length folded in when the argument carries them.
        mode: ``IN_OUT`` -- IN, OUT or IN/OUT. ``None`` for a return row.
        defaulted: ``DEFAULTED = 'Y'``.
        default_value: ``DEFAULT_VALUE``; Oracle reserves this column for
            future use, so it is normally ``None``.
        type_owner: ``TYPE_OWNER`` for a user-defined argument type.
        type_name: ``TYPE_NAME`` for a user-defined argument type.
        precision: ``DATA_PRECISION``.
        scale: ``DATA_SCALE``.
        char_length: ``CHAR_LENGTH`` for character arguments.
    """

    name: Optional[str]
    position: int
    data_type: Optional[str] = None
    mode: Optional[str] = None
    defaulted: bool = False
    default_value: Optional[str] = None
    type_owner: Optional[str] = None
    type_name: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    char_length: Optional[int] = None


@dataclass(frozen=True)
class OracleRoutineInfo:
    """One standalone PL/SQL procedure or function.

    ``ALL_PROCEDURES`` answers the routine's properties, ``ALL_OBJECTS`` its
    compile status and timestamps, and ``ALL_ARGUMENTS`` its parameters. The
    three are joined on the object and subprogram identifiers. A routine
    with no arguments still appears: ``ALL_ARGUMENTS`` omits such routines
    from 12.1.0.2 on, so the join is a ``LEFT`` one and the absence of a
    parameter row is an answer rather than a missing routine.

    ``comment`` is always ``None``. Oracle's ``COMMENT ON`` statement has no
    procedure or function form (``COMMENT`` covers tables, columns, views,
    materialized views, operators, indextypes, editions, mining models and
    audit policies only), and the dictionary view that carries browser
    comments (``ALL_TAB_COMMENTS``) describes tables and views. PL/SQL
    comments live in the source text, not in a queryable column, so the slot
    is present and honestly empty rather than approximated from source.

    Attributes:
        owner: Schema that owns the routine.
        name: Routine name (``OBJECT_NAME``).
        kind: ``PROCEDURE`` or ``FUNCTION``.
        object_id: ``OBJECT_ID``.
        subprogram_id: ``SUBPROGRAM_ID``; distinguishes overloads.
        status: ``VALID`` or ``INVALID`` from ``ALL_OBJECTS``.
        authid: ``AUTHID`` -- ``DEFINER`` or ``CURRENT_USER``.
        deterministic: ``DETERMINISTIC = 'YES'``.
        pipelined: ``PIPELINED = 'YES'``.
        aggregate: ``AGGREGATE = 'YES'``.
        parallel: ``PARALLEL = 'YES'``.
        result_cache: ``RESULT_CACHE = 'YES'``.
        interface: ``INTERFACE`` -- ``YES`` for an ODCI table function.
        impl_type_name: ``IMPLTYPENAME``, the implementation type, if any.
        polymorphic: ``POLYMORPHIC`` -- ROW, TABLE or LEAF for a polymorphic
            table function, otherwise ``None``.
        overload: ``OVERLOAD``, the overload identifier when one is set.
        parameters: The arguments, in position order. Empty for a routine
            with no arguments.
        return_type: The function's return type, or ``None`` for a procedure
            (and ``None`` for a function whose return row the server did not
            report).
        comment: Always ``None``; see the class docstring.
        created_time: ``CREATED`` from ``ALL_OBJECTS``, stringified.
        last_ddl_time: ``LAST_DDL_TIME`` from ``ALL_OBJECTS``, stringified.
        extra: Engine facts not given a named slot above.
    """

    owner: str
    name: str
    kind: str
    object_id: Optional[int] = None
    subprogram_id: Optional[int] = None
    status: Optional[str] = None
    authid: Optional[str] = None
    deterministic: bool = False
    pipelined: bool = False
    aggregate: bool = False
    parallel: bool = False
    result_cache: bool = False
    interface: Optional[str] = None
    impl_type_name: Optional[str] = None
    polymorphic: Optional[str] = None
    overload: Optional[str] = None
    parameters: List[OracleRoutineParameter] = field(default_factory=list)
    return_type: Optional[str] = None
    comment: Optional[str] = None
    created_time: Optional[str] = None
    last_ddl_time: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OracleTypeAttributeInfo:
    """One attribute of a named object type.

    Attributes:
        name: ``ATTR_NAME``.
        data_type: ``ATTR_TYPE_NAME`` as the dictionary spells it, with the
            declared length/precision folded in when it carries them.
        type_owner: ``ATTR_TYPE_OWNER`` for a user-defined attribute type.
        type_mod: ``ATTR_TYPE_MOD`` -- ``REF`` or ``POINTER``.
        length: ``LENGTH`` for character attributes.
        precision: ``PRECISION`` for numeric attributes.
        scale: ``SCALE`` for numeric attributes.
        charset: ``CHARACTER_SET_NAME``.
        position: ``ATTR_NO``, the declared position.
        inherited: ``INHERITED = 'YES'`` for an attribute of a supertype.
        char_used: ``CHAR_USED`` -- ``B`` or ``C``.
    """

    name: str
    data_type: Optional[str] = None
    type_owner: Optional[str] = None
    type_mod: Optional[str] = None
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    charset: Optional[str] = None
    position: Optional[int] = None
    inherited: bool = False
    char_used: Optional[str] = None


@dataclass(frozen=True)
class OracleTypeInfo:
    """One named object type, collection type or nested table type.

    Object types come from ``ALL_TYPES``; named collections (nested tables
    and VARRAYs) come from ``ALL_COLL_TYPES``. Both sides are joined to
    ``ALL_OBJECTS`` for compile status and timestamps and to
    ``ALL_TYPE_ATTRS`` for object attributes, so one answer carries the
    declared shape rather than only a name.

    Attributes:
        owner: Schema that owns the type.
        name: Type name (``TYPE_NAME``).
        kind: ``OBJECT``, ``NESTED TABLE``, ``VARRAY`` or ``COLLECTION``.
            Derived from ``TYPECODE`` and ``ALL_COLL_TYPES.COLL_TYPE``:
            Oracle records a nested table as collection type ``TABLE`` and a
            VARRAY as ``VARYING ARRAY``.
        type_code: ``TYPECODE`` (``OBJECT``, ``COLLECTION``, ...).
        attributes_count: ``ATTRIBUTES`` from ``ALL_TYPES``.
        methods_count: ``METHODS`` from ``ALL_TYPES``.
        predefined: ``PREDEFINED = 'YES'``.
        incomplete: ``INCOMPLETE = 'YES'``.
        final: ``FINAL`` when the view reports one; ``None`` for a
            collection, whose view has no such column.
        instantiable: ``INSTANTIABLE``, same treatment as ``final``.
        supertype_owner: ``SUPERTYPE_OWNER`` for a subtype.
        supertype_name: ``SUPERTYPE_NAME`` for a subtype.
        collection_type: ``COLL_TYPE`` for a collection type.
        element_type: ``ELEM_TYPE_NAME`` for a collection type.
        element_type_owner: ``ELEM_TYPE_OWNER`` for a collection type.
        element_type_length: ``LENGTH`` of a collection's character
            elements.
        element_type_precision: ``PRECISION`` of a collection's numeric
            elements.
        element_type_scale: ``SCALE`` of a collection's numeric elements.
        element_type_charset: ``CHARACTER_SET_NAME`` of a collection's
            character elements.
        upper_bound: ``UPPER_BOUND`` for a VARRAY (``None`` for a nested
            table).
        status: ``VALID`` or ``INVALID`` from ``ALL_OBJECTS``.
        created_time: ``CREATED``, stringified.
        last_ddl_time: ``LAST_DDL_TIME``, stringified.
        attributes: The object type's attributes in declared order. Empty
            for a collection, and for an object type whose attributes the
            server did not report.
        extra: Engine facts not given a named slot above.
    """

    owner: str
    name: str
    kind: str
    type_code: Optional[str] = None
    attributes_count: Optional[int] = None
    methods_count: Optional[int] = None
    predefined: bool = False
    incomplete: bool = False
    final: Optional[bool] = None
    instantiable: Optional[bool] = None
    supertype_owner: Optional[str] = None
    supertype_name: Optional[str] = None
    collection_type: Optional[str] = None
    element_type: Optional[str] = None
    element_type_owner: Optional[str] = None
    element_type_length: Optional[int] = None
    element_type_precision: Optional[int] = None
    element_type_scale: Optional[int] = None
    element_type_charset: Optional[str] = None
    upper_bound: Optional[int] = None
    status: Optional[str] = None
    created_time: Optional[str] = None
    last_ddl_time: Optional[str] = None
    attributes: List[OracleTypeAttributeInfo] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OracleConstraintInfo:
    """One constraint of a table, from ``ALL_CONSTRAINTS``.

    Primary keys, unique keys, check constraints and foreign keys all live
    in one dictionary view, so this family is the complete constraint
    listing rather than a restatement of the index or foreign-key families.
    The two dedicated families stay -- an index answers how a key is
    enforced, a foreign key answers its referenced detail -- and this one
    answers the declaration itself.

    Attributes:
        owner: Schema that owns the constraint.
        table_name: The constrained table (or view).
        name: ``CONSTRAINT_NAME``.
        constraint_type: ``CONSTRAINT_TYPE`` -- ``P``, ``U``, ``C``, ``R``,
            ``V``, ``O``, ``H``, ``F`` or ``S``.
        status: ``ENABLED`` or ``DISABLED``.
        validated: ``VALIDATED`` or ``NOT VALIDATED``.
        generated: ``USER NAME`` or ``GENERATED NAME``.
        deferrable: ``DEFERRABLE`` or ``NOT DEFERRABLE``.
        deferred: ``DEFERRED`` or ``IMMEDIATE``.
        rely: ``RELY`` is set (query-rewrite trusts a not-validated
            constraint).
        bad: ``BAD`` is set (the constraint names a century ambiguously).
        columns: Constraint columns in ``POSITION`` order. Check constraints
            normally carry none; a multi-column key carries one entry per
            column.
        search_condition: ``SEARCH_CONDITION`` for a check constraint, as
            the dictionary spells it (``LONG``).
        index_name: ``INDEX_NAME`` for a primary-key or unique constraint.
        referenced_owner: ``R_OWNER`` for a foreign key.
        referenced_constraint_name: ``R_CONSTRAINT_NAME`` for a foreign key.
        delete_rule: ``DELETE_RULE`` for a foreign key.
        extra: ``LAST_CHANGE``, ``INVALID`` and ``VIEW_RELATED``, which are
            engine facts without a common slot.
    """

    owner: str
    table_name: str
    name: str
    constraint_type: str
    status: Optional[str] = None
    validated: Optional[str] = None
    generated: Optional[str] = None
    deferrable: Optional[str] = None
    deferred: Optional[str] = None
    rely: bool = False
    bad: bool = False
    columns: List[str] = field(default_factory=list)
    search_condition: Optional[str] = None
    index_name: Optional[str] = None
    referenced_owner: Optional[str] = None
    referenced_constraint_name: Optional[str] = None
    delete_rule: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)
