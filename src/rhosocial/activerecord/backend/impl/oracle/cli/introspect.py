# src/rhosocial/activerecord/backend/impl/oracle/cli/introspect.py
"""introspect subcommand - Database introspection.

Oracle introspect includes the schema-object types 'sequences', 'synonyms'
and 'packages' in addition to the standard types, and 'triggers' from the
standard set -- a family the dialect has always declared and the command
line could not ask for.

NAME means different things per type, and two of those meanings point in
opposite directions: for 'triggers' it is a table, for 'views' it is the view
itself. The rules live in ``_NAME_RULES`` and are printed by ``--help``.
"""

import argparse
import asyncio
import sys
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.backend.async_backend import AsyncOracleBackend
from rhosocial.activerecord.backend.errors import ConnectionError, QueryError

from .connection import add_connection_args, resolve_connection_config_from_args
from .output import create_provider

OUTPUT_CHOICES = ['table', 'json', 'csv', 'tsv']

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
#: ``parse_triggers`` was wired, so only the word was absent.
_TYPE_CAPABILITIES = {
    "tables": "supports_table_introspection",
    "views": "supports_view_introspection",
    "columns": "supports_column_introspection",
    "indexes": "supports_index_introspection",
    "foreign-keys": "supports_foreign_key_introspection",
    "triggers": "supports_trigger_introspection",
    "sequences": "supports_sequence_introspection",
    "synonyms": "supports_synonym_introspection",
    "packages": "supports_package_introspection",
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
_NAME_RULES = {
    "columns": "required",
    "indexes": "required",
    "foreign-keys": "required",
    "triggers": "optional",
    "views": "optional",
    "sequences": "optional",
    "synonyms": "optional",
    "tables": "forbidden",
    "packages": "forbidden",
    "database": "forbidden",
}


def _refuse_unusable_name(args) -> None:
    """Refuse a NAME the type cannot use instead of ignoring it."""
    if args.name is None:
        return
    if _NAME_RULES.get(args.type) != "forbidden":
        return
    print(f"Error: {args.type} introspection takes no name", file=sys.stderr)
    sys.exit(2)


def _require_capability(backend, args) -> None:
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

  # Output as JSON
  %(prog)s tables -o json

What NAME means depends on the type:

  columns NAME, indexes NAME, foreign-keys NAME
                        NAME is the table whose sub-objects to list
                        (required)
  triggers [NAME]       NAME is the table whose triggers to list (optional);
                        it is the table, never the trigger
  views [NAME]          NAME is the view to describe (optional)
  sequences [NAME]      NAME is the sequence to describe (optional)
  synonyms [NAME]       NAME is the synonym to describe (optional)
  tables, packages, database
                        take no NAME; passing one is refused (exit 2)

Exit status: 1 for a runtime failure (connection, query, anything unexpected)
and for an object that does not exist; 2 when the request cannot be served
here (an unsupported type on this server, or a NAME the type cannot use).

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
    parser.add_argument(
        "--include-system",
        action="store_true",
        help="Include system tables in output",
    )

    return parser


def handle(args):
    """Handle the introspect subcommand."""
    # A NAME the type cannot use is refused before anything is opened: it is
    # an argument error, and no server could change the answer to it.
    _refuse_unusable_name(args)

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
            if not args.name:
                print("Error: Table name is required for 'columns' introspection", file=sys.stderr)
                sys.exit(1)
            columns = introspector.list_columns(args.name, schema=None)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            if not args.name:
                print("Error: Table name is required for 'indexes' introspection", file=sys.stderr)
                sys.exit(1)
            indexes = introspector.list_indexes(args.name, schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            if not args.name:
                print("Error: Table name is required for 'foreign-keys' introspection", file=sys.stderr)
                sys.exit(1)
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
            if not args.name:
                print("Error: Table name is required for 'columns' introspection", file=sys.stderr)
                sys.exit(1)
            columns = await introspector.list_columns(args.name, schema=None)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            if not args.name:
                print("Error: Table name is required for 'indexes' introspection", file=sys.stderr)
                sys.exit(1)
            indexes = await introspector.list_indexes(args.name, schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            if not args.name:
                print("Error: Table name is required for 'foreign-keys' introspection", file=sys.stderr)
                sys.exit(1)
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
