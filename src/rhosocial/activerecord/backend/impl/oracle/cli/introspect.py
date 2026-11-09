# src/rhosocial/activerecord/backend/impl/oracle/cli/introspect.py
"""introspect subcommand - Database introspection.

Oracle introspect includes the schema-object types 'sequences', 'synonyms',
'packages', 'partitions', 'procedures', 'functions', 'types' and
'constraints' in addition to the standard types, and 'triggers' from the
standard set -- a family the dialect has always declared and the command
line could not ask for.

The operational and data families the core introspector answers are offered
too: a type is advertised exactly when a dialect capability switch answers
for it, and the switch is read against the connected server, so a family
Oracle cannot answer *here* is refused rather than run and answered with an
empty listing. That refusal is most of the eight: on today's servers only the
two data families pass the gate -- column profiles read ``ALL_TAB_COLUMNS``
and the value listing is a grouped scan -- while the other six are declared
gaps the roundtrip suite pins, and are refused with exit 2. DDL extraction is
not offered as a type at all: its switch answers ``False`` permanently, so an
entry would be refused on every server.

The two data families are also why ``source`` is a visible column in their
output. Oracle is the only engine whose profile is an *estimate* (the
statistics the last gathering left) while its value listing is an *aggregate*
(an exact count paid for with a full scan). The profile's rows carry the
``source`` the parser labels them with -- ``estimate``, or ``unknown`` for a
table never analysed -- and the listing's rows get ``source='aggregate'``
added here: a reader comparing the two tables of one run must be able to
tell a sampled estimate from a counted scan, and the two must not render as
one homogeneous answer.

NAME means different things per type, and two of those meanings point in
opposite directions: for 'triggers' it is a table, for 'views' it is the view
itself. The rules live in ``_NAME_RULE`` and are applied by one validator,
``_validate_name``, before anything connects -- the branches consume the name
rather than re-deciding whether it was required, so a type added without a
rule is a type nobody has decided for.
"""

import argparse
import asyncio
import sys
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, Dict, List

from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.backend.async_backend import AsyncOracleBackend
from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.introspection.types import ProfileSource

from .connection import add_connection_args, resolve_connection_config_from_args
from .output import OUTPUT_CHOICES, create_provider

#: CLI type -> the dialect capability switch that answers for it.
#:
#: The advertised list is derived from this mapping, so a family cannot be
#: added in one place and forgotten in the other. A type whose switch is False
#: on the *connected* server is refused when handled rather than run and
#: answered with an empty list: "this server cannot answer" and "this schema
#: has none" are different answers.
#:
#: ``supports_trigger_introspection`` is the entry this backend declared while
#: ``triggers`` was missing from the CLI: the dialect already answered for it,
#: and the whole chain ``format_trigger_list_query_statement`` ->
#: ``parse_triggers`` was wired, so only the word was absent. ``partitions``
#: reuses the existing ``supports_partition_metadata_introspection`` rather
#: than growing a second spelling for the same capability.
#:
#: The operational and data families the core introspector answers are gated
#: through their switches the same way. Six of the eight probes answer False
#: on every Oracle today -- the statements would have to read GV$/V$
#: performance views or call DBMS_METADATA, and the dialect layer has no slot
#: for either (the roundtrip suite pins each as a declared gap) -- so those
#: types are refused with exit 2 rather than run into a formatter that does
#: not exist. The two that answer are the data families, and they answer
#: *differently*: the profile is an estimate read from ``ALL_TAB_COLUMNS``,
#: the value listing a grouped scan of the rows. That difference is a column
#: in the output, not a comment -- see ``_value_sample_rows``.
#: DDL extraction is not in the table: its switch is permanently False, so a
#: ``ddl`` type would be an entry no server could ever satisfy.
_TYPE_CAPABILITIES = {
    "tables": "supports_table_introspection",
    "views": "supports_view_introspection",
    "columns": "supports_column_introspection",
    "indexes": "supports_index_introspection",
    "foreign-keys": "supports_foreign_key_introspection",
    "constraints": "supports_constraint_introspection",
    "triggers": "supports_trigger_introspection",
    "partitions": "supports_partition_metadata_introspection",
    "sequences": "supports_sequence_introspection",
    "synonyms": "supports_synonym_introspection",
    "packages": "supports_package_introspection",
    "procedures": "supports_procedure_introspection",
    "functions": "supports_function_introspection",
    "types": "supports_type_introspection",
    "unused-indexes": "supports_unused_indexes_detection",
    "table-stats": "supports_table_stats",
    "runtime": "supports_runtime_stats",
    "index-stats": "supports_index_stats",
    "dependencies": "supports_object_dependencies",
    "column-profiles": "supports_column_data_profile",
    "value-samples": "supports_column_value_sample",
    "join-coverage": "supports_join_coverage",
    "database": "supports_database_info",
}

INTROSPECT_TYPES = list(_TYPE_CAPABILITIES)

#: What NAME means for each type, and whether it is accepted at all.
#:
#: The table-backed families and the object-backed ones point in opposite
#: directions -- ``triggers users`` asks for the triggers *on* users, while
#: ``views users`` asks for the view *called* users -- so the meaning is
#: written down once, here, and printed by ``--help`` rather than left to be
#: reconstructed from the branches.
#:
#: ``packages`` has no single-object answer (``list_packages`` exists, there is
#: no ``get_package_info``), and ``tables`` deliberately has no single-table
#: type: ``get_table_info`` fills columns, indexes and foreign keys, which is
#: the aggregate removed on 2026-10-27 coming back through another door. A name
#: for either is refused, never dropped -- a dropped name looks like a filter
#: that matched everything.
#:
#: Of the eight operational and data families, the four counter listings answer
#: for the whole schema (a name is refused, same as ``tables``);
#: ``dependencies`` takes an optional one that narrows the graph to one
#: object's edges; and ``column-profiles`` and ``value-samples`` are
#: one-table questions (required), with ``value-samples`` naming its column on
#: ``--column``. ``join-coverage`` is required too -- NAME is the *left* table
#: of the pair -- because ``name`` means "the table" everywhere else in this
#: command, and spelling the left side as a flag would make the one type whose
#: question is about two objects read as though it took none.
#:
#: The order is the order ``_TYPE_CAPABILITIES`` declares and the parser and
#: ``--help`` print, and the two tables cover the same keys: a type offered
#: without a rule is a type whose NAME decision nobody has made.
_NAME_RULE = {
    "columns": "required",
    "indexes": "required",
    "foreign-keys": "required",
    "constraints": "required",
    "triggers": "optional",
    "partitions": "required",
    "views": "optional",
    "sequences": "optional",
    "synonyms": "optional",
    "procedures": "optional",
    "functions": "optional",
    "types": "optional",
    "tables": "forbidden",
    "packages": "forbidden",
    "unused-indexes": "forbidden",
    "table-stats": "forbidden",
    "runtime": "forbidden",
    "index-stats": "forbidden",
    "dependencies": "optional",
    "column-profiles": "required",
    "value-samples": "required",
    "join-coverage": "required",
    "database": "forbidden",
}

#: The provenance the CLI writes into every value-listing row. The profile's
#: rows carry their own ``source`` (the parser labels them ``estimate`` or
#: ``unknown``); core's ``ColumnValueFrequency`` has no such field, because an
#: engine keeping a most-common-values list would need a different word. This
#: dialect's listing is a ``GROUP BY`` over the rows -- an exact count, paid
#: for with a full scan -- so the label is written here, at the only layer
#: that knows how the answer in front of the reader was produced.

#: Said over the value listing, on stderr so the machine formats on stdout
#: stay parseable. The profile beside it costs one dictionary read however
#: large the table is; this one reads every row -- a ``GROUP BY`` with
#: ``COUNT(*)``, bounded by ``FETCH FIRST`` -- and on a large table that can
#: run for a long time. The rows carry ``source='aggregate'`` themselves; this
#: is the sentence that says why the command is slow rather than leaving the
#: wait unexplained.


def _validate_name(args: argparse.Namespace) -> None:
    """Apply the NAME rule table, once, instead of leaving it to each branch.

    A ``required`` name that is missing is an incomplete invocation and leaves
    with 1, the code a missing ``--database`` gets. A name the type has no
    question for is a request this command declines and leaves with 2. Both
    halves come from ``_NAME_RULE`` above: the branches used to carry the first
    one -- fourteen spellings of "Table name is required" across the two chains
    -- and a branch that forgot would have sent ``None`` into a call that reads
    it as a filter over everything.

    ``join-coverage`` is the one type whose NAME is the *left* table of a pair,
    so its sentence says so rather than calling it a table. That is the wording
    of the noun, not a second decision about whether the name is required --
    the rule is read from the table either way.
    """
    rule = _NAME_RULE.get(args.type)
    if rule is None:
        return
    if rule == "required" and not args.name:
        noun = "Left table name" if args.type == "join-coverage" else "Table name"
        print(
            f"Error: {noun} is required for '{args.type}' introspection",
            file=sys.stderr,
        )
        sys.exit(1)
    if rule == "forbidden" and args.name is not None:
        print(f"Error: {args.type} introspection takes no name", file=sys.stderr)
        sys.exit(2)


def _refuse_unused_arguments(args: argparse.Namespace) -> None:
    """Refuse a flag the chosen type has no question for.

    The same defect as a silently dropped name, one flag over: ``--column``
    beside ``introspect tables`` would be ignored and the listing printed as
    if the column had been part of the question. Only the families whose
    probe takes a column read the flag, so only they may be given it.

    The flags are read with ``getattr`` because the handler is also driven
    with hand-built namespaces (the offline tests), which carry no flags at
    all -- an absent attribute is "not given", not an error.
    """
    column = getattr(args, "column", None)
    right_table = getattr(args, "right_table", None)
    right_column = getattr(args, "right_column", None)

    if column is not None and args.type not in ("value-samples", "join-coverage"):
        print(
            f"Error: '{args.type}' does not take --column; "
            "use 'value-samples' or 'join-coverage'",
            file=sys.stderr,
        )
        sys.exit(2)
    if (right_table is not None or right_column is not None) and args.type != "join-coverage":
        print(
            f"Error: '{args.type}' does not take --right-table or --right-column; "
            "use 'join-coverage'",
            file=sys.stderr,
        )
        sys.exit(2)


def _require_capability(backend: Any, args: argparse.Namespace) -> None:
    """Refuse a type the connected server cannot answer.

    The dialect is adapted before this runs, so the switches answer for the
    server in front of us rather than for the newest release this backend
    knows.
    """
    switch = _TYPE_CAPABILITIES.get(args.type)
    if switch is None:
        return
    dialect = getattr(backend, "dialect", None)
    if dialect is None:
        # A stub standing in for the backend (the offline CLI tests) has no
        # dialect to ask; a real one always does.
        return
    supported = getattr(dialect, switch, None)
    if supported is None or supported():
        return
    print(
        f"Error: {args.type} introspection is not supported by this server",
        file=sys.stderr,
    )
    sys.exit(2)


def create_parser(subparsers):
    """Create the introspect subcommand parser."""
    parser = subparsers.add_parser(
        'introspect',
        help='Database introspection',
        epilog="""Examples:
  # List all tables
  %(prog)s tables

  # List all views, or describe one view
  %(prog)s views
  %(prog)s views active_customers

  # Get column details
  %(prog)s columns users

  # List the triggers on one table, or every trigger in the schema
  %(prog)s triggers users
  %(prog)s triggers

  # List sequences (Oracle specific), or describe one
  %(prog)s sequences
  %(prog)s sequences customers_seq

  # List the private synonyms of the connecting user, or describe one
  %(prog)s synonyms
  %(prog)s synonyms customers

  # List PL/SQL packages with their compile status
  %(prog)s packages

  # List the partitions of one table, with strategy, bounds and tablespace
  %(prog)s partitions orders

  # List constraints, standalone procedures, functions and named types
  %(prog)s constraints orders
  %(prog)s procedures
  %(prog)s procedures calculate_tax
  %(prog)s functions
  %(prog)s types
  %(prog)s types address_t

  # Operational readings, each refused (exit 2) on a server whose dialect
  # cannot answer it -- on Oracle today that is all four, until the
  # performance-view statements are written:
  %(prog)s unused-indexes
  %(prog)s table-stats
  %(prog)s runtime
  %(prog)s index-stats

  # The dependency graph, or one object's dependants
  %(prog)s dependencies
  %(prog)s dependencies customers

  # Data profiles. The profile reads ALL_TAB_COLUMNS -- an estimate of the
  # last statistics gathering, vintage in collected_at. The value listing
  # scans the rows -- exact, and paid for with a full scan. Each row says
  # which it is in its source column.
  %(prog)s column-profiles customers
  %(prog)s value-samples customers --column status

  # Measure how two columns' value domains overlap
  %(prog)s join-coverage orders --column customer_id --right-table customers --right-column id

  # Output as JSON
  %(prog)s tables -o json

What NAME means depends on the type:

  columns NAME, indexes NAME, foreign-keys NAME, constraints NAME,
  partitions NAME, column-profiles NAME, value-samples NAME
                        NAME is the table whose sub-objects to list or
                        profile (required)
  join-coverage NAME    NAME is the left table of the pair (required);
                        --column names its column, --right-table and
                        --right-column the other side
  triggers [NAME]       NAME is the table whose triggers to list (optional);
                        it is the table, never the trigger
  views [NAME]          NAME is the view to describe (optional)
  sequences [NAME]      NAME is the sequence to describe (optional)
  synonyms [NAME]       NAME is the synonym to describe (optional)
  procedures [NAME]     NAME is the standalone procedure to describe
                        (optional); package subprograms are not in this
                        family
  functions [NAME]      NAME is the standalone function to describe
                        (optional)
  types [NAME]          NAME is the named type to describe (optional)
  dependencies [NAME]   NAME narrows the graph to one object's edges
                        (optional); omit it for the whole graph
  tables, packages, database, unused-indexes, table-stats, runtime,
  index-stats           take no NAME; passing one is refused (exit 2)

Exit status: 1 for a runtime failure (connection, query, anything unexpected)
and for an object that does not exist; 2 when the request cannot be served
here (an unsupported type on this server, a NAME the type cannot use, or a
flag the type does not read).

Owners: the schema is the user the connection authenticates as (--user), so
there is no separate owner option. PUBLIC synonyms are not listed: Oracle
records them under the owner PUBLIC, which is a name in ALL_SYNONYMS rather
than a schema this run connects as.
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '-o', '--output',
        choices=OUTPUT_CHOICES,
        default='table',
        help='Output format (default: table)',
    )

    add_connection_args(parser)

    parser.add_argument(
        '--rich-ascii',
        action='store_true',
        help='Use ASCII characters for rich table borders.',
    )

    # The candidate list is the keys of _TYPE_CAPABILITIES: only this backend
    # knows whether Oracle has packages, and the parser answers before any
    # connection exists, so the list cannot come from a live dialect.
    parser.add_argument(
        "type",
        choices=INTROSPECT_TYPES,
        help="Introspection type",
    )
    parser.add_argument(
        "name",
        nargs="?",
        default=None,
        help="Table or object name; what it means depends on the type",
    )
    # The families that ask about a column or a second table carry the names
    # the probe needs. They stay flags rather than more positionals because
    # ``name`` already means "the table" everywhere else, and
    # ``value-samples customers.email`` would read as a dotted object name.
    parser.add_argument(
        "--column",
        default=None,
        help="Column name: the column to sample for value-samples, and "
        "the left column for join-coverage",
    )
    parser.add_argument(
        "--right-table",
        default=None,
        help="Right-hand table for join-coverage",
    )
    parser.add_argument(
        "--right-column",
        default=None,
        help="Right-hand column for join-coverage",
    )
    parser.add_argument(
        "--include-system",
        action="store_true",
        help="Include system tables in output",
    )

    return parser


def handle(args):
    """Handle the introspect subcommand."""
    # A NAME or a flag the type cannot use is refused before anything is
    # opened: both are argument errors, and no server could change the
    # answer to either.
    _validate_name(args)
    _refuse_unused_arguments(args)

    provider = create_provider(args.output, ascii_borders=args.rich_ascii)

    config = resolve_connection_config_from_args(args)

    if args.is_async:
        backend = AsyncOracleBackend(connection_config=config)
        asyncio.run(_handle_introspect_async(args, backend, provider))
    else:
        backend = OracleBackend(connection_config=config)
        _handle_introspect_sync(args, backend, provider)


def _serialize_for_output(obj: Any) -> Any:
    """Serialize object for JSON output, handling non-serializable types."""
    if obj is None:
        return None
    if hasattr(obj, 'model_dump'):
        try:
            result = obj.model_dump(mode='json')
            return _serialize_for_output(result)
        except TypeError:
            result = obj.model_dump()
            return _serialize_for_output(result)
    if is_dataclass(obj) and not isinstance(obj, type):
        return {k: _serialize_for_output(v) for k, v in asdict(obj).items()}
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, dict):
        return {k: _serialize_for_output(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize_for_output(item) for item in obj]
    if isinstance(obj, (str, int, float, bool)):
        return obj
    return str(obj)


def _value_sample_rows(samples: List[Any]) -> List[Dict[str, Any]]:
    """The value listing as rows, each carrying how it was produced.

    Oracle is the one engine here whose profile and its value listing are
    produced *differently* -- the profile is an estimate read from
    ``ALL_TAB_COLUMNS``, the listing a grouped scan of the rows -- and the
    profile's rows say so in the ``source`` field the parser fills. The
    listing's rows have no such field: core's ``ColumnValueFrequency``
    carries the value and its count, not how they were counted, because an
    engine keeping a most-common-values list would need a different word.
    So the column is added here, at the only layer that knows how the answer
    in front of the reader was produced. Without it the two tables of one
    run would read as the same kind of answer, and a sampled estimate next
    to a counted scan is exactly the pair a reader must not blur.
    """
    rows: List[Dict[str, Any]] = []
    for sample in samples or []:
        row = _serialize_for_output(sample)
        if isinstance(row, dict):
            row["source"] = ProfileSource.AGGREGATE.value
        rows.append(row)
    return rows


def _handle_introspect_sync(args, backend: OracleBackend, provider):
    """Handle introspect subcommand synchronously."""
    try:
        backend.connect()
        # Adapt before asking anything: the dialect carries the version its
        # version-gated answers depend on, and ``introspect database`` reports
        # that same version. Without this the dialect is unadapted and every
        # gate answers from no measured number.
        backend.introspect_and_adapt()
        _require_capability(backend, args)
        introspector = backend.introspector

        if args.type == "tables":
            tables = introspector.list_tables(
                schema=None,
                include_system=args.include_system
            )
            data = _serialize_for_output(tables)
            provider.display_results(data, title="Tables")

        elif args.type == "views":
            if args.name:
                view = introspector.get_view_info(args.name, schema=None)
                if view is None:
                    print(f"Error: View '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(_serialize_for_output([view]), title=f"View {args.name}")
            else:
                views = introspector.list_views(schema=None)
                data = _serialize_for_output(views)
                provider.display_results(data, title="Views")

        elif args.type == "columns":
            # ``_validate_name`` has already refused a run without a name: it
            # is required here, so ``args.name`` is the table to ask about.
            columns = introspector.list_columns(args.name, schema=None)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            indexes = introspector.list_indexes(args.name, schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            fks = introspector.list_foreign_keys(args.name, schema=None)
            data = _serialize_for_output(fks)
            provider.display_results(data, title=f"Foreign Keys of {args.name}")

        elif args.type == "triggers":
            # NAME is the table here, not the trigger: the same meaning it has
            # for columns/indexes/foreign-keys. Omitting it lists every trigger
            # in the schema. `triggers NAME` is therefore "the triggers on one
            # table", never "the trigger called NAME".
            triggers = introspector.list_triggers(table_name=args.name, schema=None)
            data = _serialize_for_output(triggers)
            provider.display_results(data, title="Triggers")

        elif args.type == "sequences":
            if args.name:
                sequence = introspector.get_sequence_info(args.name, owner=None)
                if sequence is None:
                    print(f"Error: Sequence '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([sequence]), title=f"Sequence {args.name}"
                )
            else:
                sequences = introspector.list_sequences(owner=None)
                data = _serialize_for_output(sequences)
                provider.display_results(data, title="Sequences")

        elif args.type == "synonyms":
            if args.name:
                synonym = introspector.get_synonym_info(args.name, owner=None)
                if synonym is None:
                    print(f"Error: Synonym '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([synonym]), title=f"Synonym {args.name}"
                )
            else:
                synonyms = introspector.list_synonyms(owner=None)
                data = _serialize_for_output(synonyms)
                provider.display_results(data, title="Synonyms")

        elif args.type == "packages":
            packages = introspector.list_packages(owner=None)
            data = _serialize_for_output(packages)
            provider.display_results(data, title="Packages")

        elif args.type == "partitions":
            # NAME is the table, as it is for columns/indexes/constraints.
            partitions = introspector.list_partitions(args.name, schema=None)
            data = _serialize_for_output(partitions)
            provider.display_results(data, title=f"Partitions of {args.name}")

        elif args.type == "constraints":
            constraints = introspector.list_constraints(args.name, schema=None)
            data = _serialize_for_output(constraints)
            provider.display_results(data, title=f"Constraints of {args.name}")

        elif args.type == "procedures":
            if args.name:
                procedure = introspector.get_procedure_info(args.name, owner=None)
                if procedure is None:
                    print(f"Error: Procedure '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([procedure]), title=f"Procedure {args.name}"
                )
            else:
                procedures = introspector.list_procedures(owner=None)
                data = _serialize_for_output(procedures)
                provider.display_results(data, title="Procedures")

        elif args.type == "functions":
            if args.name:
                function = introspector.get_function_info(args.name, owner=None)
                if function is None:
                    print(f"Error: Function '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([function]), title=f"Function {args.name}"
                )
            else:
                functions = introspector.list_functions(owner=None)
                data = _serialize_for_output(functions)
                provider.display_results(data, title="Functions")

        elif args.type == "types":
            if args.name:
                type_info = introspector.get_type_info(args.name, owner=None)
                if type_info is None:
                    print(f"Error: Type '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([type_info]), title=f"Type {args.name}"
                )
            else:
                types = introspector.list_types(owner=None)
                data = _serialize_for_output(types)
                provider.display_results(data, title="Types")

        # ---- the operational families -------------------------------------
        # These read the statistics collector or the performance views rather
        # than the catalog, so each is a listing of the whole connection's
        # scope and none of them takes a NAME -- except ``dependencies``,
        # where a name narrows the graph to one object's edges ("what breaks
        # if I drop this") instead of asking a different family, the same way
        # a trigger's table does. On a server whose probe answered False the
        # gate above has already refused; the branch exists so the day a
        # probe answers True the question is wired, not missing.

        elif args.type == "unused-indexes":
            indexes = introspector.list_unused_indexes(schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title="Unused Indexes")

        elif args.type == "table-stats":
            stats = introspector.list_table_usage_stats(schema=None)
            data = _serialize_for_output(stats)
            provider.display_results(data, title="Table Usage Stats")

        elif args.type == "runtime":
            metrics = introspector.list_runtime_metrics(schema=None)
            data = _serialize_for_output(metrics)
            provider.display_results(data, title="Runtime Metrics")

        elif args.type == "index-stats":
            stats = introspector.list_index_usage_stats(schema=None)
            data = _serialize_for_output(stats)
            provider.display_results(data, title="Index Usage Stats")

        elif args.type == "dependencies":
            dependencies = introspector.list_object_dependencies(
                object_name=args.name, schema=None
            )
            data = _serialize_for_output(dependencies)
            title = f"Dependencies of {args.name}" if args.name else "Object Dependencies"
            provider.display_results(data, title=title)

        # ---- the one-object families --------------------------------------
        # The profiles of one table or one column of it: NAME is the table,
        # and the column travels on its own flag -- ``value-samples
        # customers.email`` would read as a dotted object name, and NAME
        # already means "the table" everywhere else.

        elif args.type == "column-profiles":
            # The rows carry ``source`` from the parser -- ``estimate`` with
            # the vintage in ``collected_at``, or ``unknown`` for a table
            # never analysed -- so the listing answers "how was this
            # produced" without a note beside it.
            profiles = introspector.list_column_data_profiles(args.name, schema=None)
            data = _serialize_for_output(profiles)
            provider.display_results(data, title=f"Column profiles of {args.name}")

        elif args.type == "value-samples":
            if not args.column:
                print(
                    "Error: --column is required for 'value-samples' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            # The scan warning goes to stderr so the machine formats on
            # stdout stay parseable; the rows themselves carry
            # ``source='aggregate'`` (added by _value_sample_rows), which is
            # the column that tells this answer from the profile's estimate.
            # The row bound is the introspector's own: this command has no
            # ``--limit`` to carry a number the caller asked for, so the
            # default the statement's ``FETCH FIRST`` applies is the one that
            # runs rather than a figure restated here.
            print(
                "Note: value samples are aggregated from the rows themselves "
                "(source='aggregate') -- a grouped scan of the whole table, exact "
                "and paid for in full.",
                file=sys.stderr,
            )
            samples = introspector.sample_column_values(args.name, args.column, schema=None)
            data = _value_sample_rows(samples)
            provider.display_results(data, title=f"Values of {args.name}.{args.column}")

        elif args.type == "join-coverage":
            if not args.column:
                print(
                    "Error: --column is required for 'join-coverage' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not args.right_table or not args.right_column:
                print(
                    "Error: --right-table and --right-column are required for "
                    "'join-coverage' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            coverage = introspector.estimate_join_coverage(
                args.name, args.column, args.right_table, args.right_column, schema=None
            )
            if coverage is None:
                # A pair with nothing to match answers "no measurement",
                # which is not zero overlap; printing a row of nulls would
                # read as a measured zero.
                print(
                    f"Error: the overlap of {args.name}.{args.column} and "
                    f"{args.right_table}.{args.right_column} cannot be measured",
                    file=sys.stderr,
                )
                sys.exit(1)
            data = _serialize_for_output([coverage])
            provider.display_results(data, title="Join Coverage")

        elif args.type == "database":
            info = introspector.get_database_info()
            data = _serialize_for_output(info)
            provider.display_results([data], title="Database Info")

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error during introspection: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if backend._connection:
            backend.disconnect()


async def _handle_introspect_async(args, backend: AsyncOracleBackend, provider):
    """Handle introspect subcommand asynchronously."""
    try:
        await backend.connect()
        # The same adaptation the sync path performs, awaited before the first
        # question is asked.
        await backend.introspect_and_adapt()
        _require_capability(backend, args)
        introspector = backend.introspector

        if args.type == "tables":
            tables = await introspector.list_tables(
                schema=None,
                include_system=args.include_system
            )
            data = _serialize_for_output(tables)
            provider.display_results(data, title="Tables")

        elif args.type == "views":
            if args.name:
                view = await introspector.get_view_info(args.name, schema=None)
                if view is None:
                    print(f"Error: View '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(_serialize_for_output([view]), title=f"View {args.name}")
            else:
                views = await introspector.list_views(schema=None)
                data = _serialize_for_output(views)
                provider.display_results(data, title="Views")

        elif args.type == "columns":
            # The same rule as the sync path: ``_validate_name`` has already
            # refused a run without a name, so it is the table to ask about.
            columns = await introspector.list_columns(args.name, schema=None)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            indexes = await introspector.list_indexes(args.name, schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            fks = await introspector.list_foreign_keys(args.name, schema=None)
            data = _serialize_for_output(fks)
            provider.display_results(data, title=f"Foreign Keys of {args.name}")

        elif args.type == "triggers":
            # Same meaning as in the sync path: NAME is the table.
            triggers = await introspector.list_triggers(table_name=args.name, schema=None)
            data = _serialize_for_output(triggers)
            provider.display_results(data, title="Triggers")

        elif args.type == "sequences":
            if args.name:
                sequence = await introspector.get_sequence_info(args.name, owner=None)
                if sequence is None:
                    print(f"Error: Sequence '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([sequence]), title=f"Sequence {args.name}"
                )
            else:
                sequences = await introspector.list_sequences(owner=None)
                data = _serialize_for_output(sequences)
                provider.display_results(data, title="Sequences")

        elif args.type == "synonyms":
            if args.name:
                synonym = await introspector.get_synonym_info(args.name, owner=None)
                if synonym is None:
                    print(f"Error: Synonym '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([synonym]), title=f"Synonym {args.name}"
                )
            else:
                synonyms = await introspector.list_synonyms(owner=None)
                data = _serialize_for_output(synonyms)
                provider.display_results(data, title="Synonyms")

        elif args.type == "packages":
            packages = await introspector.list_packages(owner=None)
            data = _serialize_for_output(packages)
            provider.display_results(data, title="Packages")

        elif args.type == "partitions":
            partitions = await introspector.list_partitions(args.name, schema=None)
            data = _serialize_for_output(partitions)
            provider.display_results(data, title=f"Partitions of {args.name}")

        elif args.type == "constraints":
            constraints = await introspector.list_constraints(args.name, schema=None)
            data = _serialize_for_output(constraints)
            provider.display_results(data, title=f"Constraints of {args.name}")

        elif args.type == "procedures":
            if args.name:
                procedure = await introspector.get_procedure_info(args.name, owner=None)
                if procedure is None:
                    print(f"Error: Procedure '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([procedure]), title=f"Procedure {args.name}"
                )
            else:
                procedures = await introspector.list_procedures(owner=None)
                data = _serialize_for_output(procedures)
                provider.display_results(data, title="Procedures")

        elif args.type == "functions":
            if args.name:
                function = await introspector.get_function_info(args.name, owner=None)
                if function is None:
                    print(f"Error: Function '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([function]), title=f"Function {args.name}"
                )
            else:
                functions = await introspector.list_functions(owner=None)
                data = _serialize_for_output(functions)
                provider.display_results(data, title="Functions")

        elif args.type == "types":
            if args.name:
                type_info = await introspector.get_type_info(args.name, owner=None)
                if type_info is None:
                    print(f"Error: Type '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                provider.display_results(
                    _serialize_for_output([type_info]), title=f"Type {args.name}"
                )
            else:
                types = await introspector.list_types(owner=None)
                data = _serialize_for_output(types)
                provider.display_results(data, title="Types")

        # The async twins of the nine branches above: same method, same
        # argument and same title, awaited. They are written out rather than
        # shared with the sync chain because the executor is the only thing
        # that differs, and a shared body would have to carry an ``await``
        # flag that reads worse than the duplicate.

        elif args.type == "unused-indexes":
            indexes = await introspector.list_unused_indexes(schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title="Unused Indexes")

        elif args.type == "table-stats":
            stats = await introspector.list_table_usage_stats(schema=None)
            data = _serialize_for_output(stats)
            provider.display_results(data, title="Table Usage Stats")

        elif args.type == "runtime":
            metrics = await introspector.list_runtime_metrics(schema=None)
            data = _serialize_for_output(metrics)
            provider.display_results(data, title="Runtime Metrics")

        elif args.type == "index-stats":
            stats = await introspector.list_index_usage_stats(schema=None)
            data = _serialize_for_output(stats)
            provider.display_results(data, title="Index Usage Stats")

        elif args.type == "dependencies":
            dependencies = await introspector.list_object_dependencies(
                object_name=args.name, schema=None
            )
            data = _serialize_for_output(dependencies)
            title = f"Dependencies of {args.name}" if args.name else "Object Dependencies"
            provider.display_results(data, title=title)

        elif args.type == "column-profiles":
            profiles = await introspector.list_column_data_profiles(args.name, schema=None)
            data = _serialize_for_output(profiles)
            provider.display_results(data, title=f"Column profiles of {args.name}")

        elif args.type == "value-samples":
            if not args.column:
                print(
                    "Error: --column is required for 'value-samples' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            print(
                "Note: value samples are aggregated from the rows themselves "
                "(source='aggregate') -- a grouped scan of the whole table, exact "
                "and paid for in full.",
                file=sys.stderr,
            )
            samples = await introspector.sample_column_values(args.name, args.column, schema=None)
            data = _value_sample_rows(samples)
            provider.display_results(data, title=f"Values of {args.name}.{args.column}")

        elif args.type == "join-coverage":
            if not args.column:
                print(
                    "Error: --column is required for 'join-coverage' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not args.right_table or not args.right_column:
                print(
                    "Error: --right-table and --right-column are required for "
                    "'join-coverage' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            coverage = await introspector.estimate_join_coverage(
                args.name, args.column, args.right_table, args.right_column, schema=None
            )
            if coverage is None:
                print(
                    f"Error: the overlap of {args.name}.{args.column} and "
                    f"{args.right_table}.{args.right_column} cannot be measured",
                    file=sys.stderr,
                )
                sys.exit(1)
            data = _serialize_for_output([coverage])
            provider.display_results(data, title="Join Coverage")

        elif args.type == "database":
            info = await introspector.get_database_info()
            data = _serialize_for_output(info)
            provider.display_results([data], title="Database Info")

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error during introspection: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if backend._connection:
            await backend.disconnect()
