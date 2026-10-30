# src/rhosocial/activerecord/backend/impl/oracle/cli/introspect.py
"""introspect subcommand - Database introspection.

Oracle introspect includes the schema-object types 'sequences', 'synonyms'
and 'packages' in addition to the standard types, and 'triggers' from the
standard set -- a family the dialect has always declared and the command
line could not ask for.
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

  # List all views
  %(prog)s views

  # Get column details
  %(prog)s columns users

  # List the triggers on one table, or every trigger in the schema
  %(prog)s triggers users
  %(prog)s triggers

  # List sequences (Oracle specific)
  %(prog)s sequences

  # List the private synonyms of the connecting user
  %(prog)s synonyms

  # List PL/SQL packages with their compile status
  %(prog)s packages

  # Output as JSON
  %(prog)s tables -o json

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
        help="Table/view name (required for some types)",
    )
    parser.add_argument(
        "--include-system",
        action="store_true",
        help="Include system tables in output",
    )

    return parser


def handle(args):
    """Handle the introspect subcommand."""
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
            sequences = introspector.list_sequences(owner=None)
            data = _serialize_for_output(sequences)
            provider.display_results(data, title="Sequences")

        elif args.type == "synonyms":
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
            sequences = await introspector.list_sequences(owner=None)
            data = _serialize_for_output(sequences)
            provider.display_results(data, title="Sequences")

        elif args.type == "synonyms":
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
