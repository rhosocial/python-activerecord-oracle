# src/rhosocial/activerecord/backend/impl/oracle/cli/status.py
"""status subcommand - Display Oracle server status overview.

Oracle status includes tablespaces and users sections in addition
to the standard sections.
"""

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

from rhosocial.activerecord.backend.impl.oracle import OracleBackend, AsyncOracleBackend
from rhosocial.activerecord.backend.errors import ConnectionError, QueryError

from .connection import add_connection_args, resolve_connection_config_from_args
from .output import create_provider, RICH_AVAILABLE

OUTPUT_CHOICES = ['table', 'json', 'csv', 'tsv']

STATUS_TYPES = ["all", "config", "performance", "connections", "storage", "tablespaces", "users"]


def create_parser(subparsers):
    """Create the status subcommand parser."""
    parser = subparsers.add_parser(
        'status',
        help='Display server status overview',
        epilog="""Examples:
  # Show complete status
  %(prog)s all

  # Show configuration only
  %(prog)s config

  # Show tablespace information (Oracle specific)
  %(prog)s tablespaces

  # Output as JSON
  %(prog)s all -o json
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
        '-v', '--verbose',
        action='count',
        default=0,
        help='Increase verbosity for additional columns.',
    )

    parser.add_argument(
        '--rich-ascii',
        action='store_true',
        help='Use ASCII characters for rich table borders.',
    )

    parser.add_argument(
        "type",
        nargs="?",
        default="all",
        choices=STATUS_TYPES,
        help="Status type (default: all)",
    )

    return parser


def handle(args):
    """Handle the status subcommand."""
    provider = create_provider(args.output, ascii_borders=args.rich_ascii)

    config = resolve_connection_config_from_args(args)

    if args.use_async:
        backend = AsyncOracleBackend(connection_config=config)
        asyncio.run(_handle_status_async(args, backend, provider))
    else:
        backend = OracleBackend(connection_config=config)
        _handle_status_sync(args, backend, provider)


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


def _format_size(size_bytes: int) -> str:
    """Format byte size to human-readable string."""
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def _handle_status_sync(args, backend: OracleBackend, provider):
    """Handle status subcommand synchronously."""
    from rhosocial.activerecord.backend.introspection.status import StatusCategory

    try:
        backend.connect()
        backend.introspect_and_adapt()

        status_introspector = backend.introspector.status
        status_type = args.type

        if status_type == "all":
            status = status_introspector.get_overview()
            effective_output = args.output
            if effective_output in ("csv", "tsv"):
                effective_output = "json"

            if effective_output == "json" or not RICH_AVAILABLE:
                print(json.dumps(_serialize_for_output(status), indent=2))
            else:
                _display_status_rich(status, args.verbose)
        elif status_type == "config":
            config_items = status_introspector.list_configuration(StatusCategory.CONFIGURATION)
            data = _serialize_for_output(config_items)
            provider.display_results(data, title="Configuration")
        elif status_type == "performance":
            perf_items = status_introspector.list_configuration(StatusCategory.PERFORMANCE)
            data = _serialize_for_output(perf_items)
            provider.display_results(data, title="Performance")
        elif status_type == "connections":
            conn_info = status_introspector.get_connection_info()
            data = _serialize_for_output(conn_info)
            provider.display_results([data], title="Connections")
        elif status_type == "storage":
            storage_info = status_introspector.get_storage_info()
            data = _serialize_for_output(storage_info)
            provider.display_results([data], title="Storage")
        elif status_type == "tablespaces":
            tablespaces = status_introspector.list_tablespaces() if hasattr(status_introspector, 'list_tablespaces') else []
            data = _serialize_for_output(tablespaces)
            provider.display_results(data, title="Tablespaces")
        elif status_type == "users":
            users = status_introspector.list_users()
            data = _serialize_for_output(users)
            provider.display_results(data, title="Users")

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error during status retrieval: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if backend._connection:
            backend.disconnect()


async def _handle_status_async(args, backend: AsyncOracleBackend, provider):
    """Handle status subcommand asynchronously."""
    from rhosocial.activerecord.backend.introspection.status import StatusCategory

    try:
        await backend.connect()
        await backend.introspect_and_adapt()

        status_introspector = backend.introspector.status
        status_type = args.type

        if status_type == "all":
            status = await status_introspector.get_overview()
            effective_output = args.output
            if effective_output in ("csv", "tsv"):
                effective_output = "json"

            if effective_output == "json" or not RICH_AVAILABLE:
                print(json.dumps(_serialize_for_output(status), indent=2))
            else:
                _display_status_rich(status, args.verbose)
        elif status_type == "config":
            config_items = await status_introspector.list_configuration(StatusCategory.CONFIGURATION)
            data = _serialize_for_output(config_items)
            provider.display_results(data, title="Configuration")
        elif status_type == "performance":
            perf_items = await status_introspector.list_configuration(StatusCategory.PERFORMANCE)
            data = _serialize_for_output(perf_items)
            provider.display_results(data, title="Performance")
        elif status_type == "connections":
            conn_info = await status_introspector.get_connection_info()
            data = _serialize_for_output(conn_info)
            provider.display_results([data], title="Connections")
        elif status_type == "storage":
            storage_info = await status_introspector.get_storage_info()
            data = _serialize_for_output(storage_info)
            provider.display_results([data], title="Storage")
        elif status_type == "tablespaces":
            tablespaces = await status_introspector.list_tablespaces() if hasattr(status_introspector, 'list_tablespaces') else []
            data = _serialize_for_output(tablespaces)
            provider.display_results(data, title="Tablespaces")
        elif status_type == "users":
            users = await status_introspector.list_users()
            data = _serialize_for_output(users)
            provider.display_results(data, title="Users")

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error during status retrieval: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if backend._connection:
            await backend.disconnect()


def _display_status_rich(status: Any, verbose: int = 0):
    """Display status using rich console.

    Oracle-specific rich display includes tablespaces section.
    """
    from rich.console import Console

    console = Console(force_terminal=True)

    console.print("\n[bold cyan]Oracle Server Status[/bold cyan]\n")

    if hasattr(status, 'server_version'):
        console.print(f"[bold]Version:[/bold] {status.server_version}")
    if hasattr(status, 'server_vendor'):
        console.print(f"[bold]Vendor:[/bold] {status.server_vendor}")

    if hasattr(status, 'session') and status.session:
        session = status.session
        console.print()
        console.print("[bold green]Session[/bold green]")
        if session.user:
            console.print(f" [bold]User:[/bold] {session.user}")
        if session.database:
            console.print(f" [bold]Database:[/bold] {session.database}")
        if session.host:
            console.print(f" [bold]Host:[/bold] {session.host}")

    console.print()
