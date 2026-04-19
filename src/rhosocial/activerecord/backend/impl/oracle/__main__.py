# src/rhosocial/activerecord/backend/impl/oracle/__main__.py
"""
Oracle backend command-line interface.

Provides SQL execution and database information display capabilities.

Usage:
    python -m rhosocial.activerecord.backend.impl.oracle <command> [options]

Commands:
    info        Display Oracle environment information
    query       Execute SQL queries
    introspect  Database introspection
    status      Display session status
"""

import argparse
import asyncio
import inspect
import json
import logging
import os
import sys
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Dict, List, Any, Tuple

from . import OracleBackend, AsyncOracleBackend
from .config import OracleConnectionConfig
from .dialect import OracleDialect
from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.output import (
    JsonOutputProvider, CsvOutputProvider, TsvOutputProvider
)

try:
    from rich.logging import RichHandler
    from rhosocial.activerecord.backend.output_rich import RichOutputProvider
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    RichOutputProvider = None

logger = logging.getLogger(__name__)

INTROSPECT_TYPES = [
    "tables", "views", "table", "columns",
    "indexes", "foreign-keys", "sequences", "database"
]

STATUS_TYPES = ["all", "config", "performance", "connections", "storage", "tablespaces", "users"]


def _serialize_for_output(obj: Any) -> Any:
    """Serialize object for JSON output."""
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


def parse_args():
    """Parse command line arguments."""
    parent_parser = argparse.ArgumentParser(add_help=False)
    
    parent_parser.add_argument(
        "--host",
        default=os.getenv("ORACLE_HOST", "localhost"),
        help="Database host (env: ORACLE_HOST, default: localhost)",
    )
    parent_parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("ORACLE_PORT", "1521")),
        help="Database port (env: ORACLE_PORT, default: 1521)",
    )
    parent_parser.add_argument(
        "--service",
        default=os.getenv("ORACLE_SERVICE", "ORCL"),
        help="Oracle service name (env: ORACLE_SERVICE, default: ORCL)",
    )
    parent_parser.add_argument(
        "--user",
        default=os.getenv("ORACLE_USER", "system"),
        help="Database user (env: ORACLE_USER, default: system)",
    )
    parent_parser.add_argument(
        "--password",
        default=os.getenv("ORACLE_PASSWORD", ""),
        help="Database password (env: ORACLE_PASSWORD)",
    )
    parent_parser.add_argument(
        "--use-async",
        action="store_true",
        help="Use asynchronous backend",
    )
    parent_parser.add_argument(
        "--version",
        type=str,
        default=None,
        help='Oracle version to simulate (e.g., "19.0.0", "21.0.0")',
    )
    parent_parser.add_argument(
        "-o", "--output",
        choices=["table", "json", "csv", "tsv"],
        default="table",
        help='Output format. Defaults to "table" if rich is installed.',
    )
    parent_parser.add_argument(
        "--log-level",
        default="INFO",
        help="Set logging level (e.g., DEBUG, INFO)",
    )
    parent_parser.add_argument(
        "--rich-ascii",
        action="store_true",
        help="Use ASCII characters for rich table borders.",
    )
    parent_parser.add_argument(
        "--mode",
        choices=["thin", "thick"],
        default="thin",
        help="Oracle client mode (thin or thick)",
    )

    parser = argparse.ArgumentParser(
        description="Execute SQL queries against an Oracle backend.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity. -v for families, -vv for details.",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser(
        "info",
        help="Display Oracle environment information",
        parents=[parent_parser],
    )

    query_parser = subparsers.add_parser(
        "query",
        help="Execute SQL query",
        parents=[parent_parser],
    )
    query_parser.add_argument(
        "sql",
        nargs="?",
        default=None,
        help="SQL query to execute. If not provided, reads from --file or stdin.",
    )
    query_parser.add_argument(
        "-f", "--file",
        default=None,
        help="Path to a file containing SQL to execute.",
    )

    introspect_parser = subparsers.add_parser(
        "introspect",
        help="Database introspection commands",
        parents=[parent_parser],
        epilog="""Examples:
  # List all tables
  %(prog)s tables

  # List all views
  %(prog)s views

  # Get detailed table info
  %(prog)s table users

  # Get column details
  %(prog)s columns users

  # Output as JSON
  %(prog)s tables -o json
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    introspect_parser.add_argument(
        "type",
        choices=INTROSPECT_TYPES,
        help="Introspection type",
    )
    introspect_parser.add_argument(
        "name",
        nargs="?",
        default=None,
        help="Table/view name (required for table, columns, indexes, foreign-keys)",
    )
    introspect_parser.add_argument(
        "--owner",
        default=None,
        help="Owner/schema name",
    )
    introspect_parser.add_argument(
        "--include-system",
        action="store_true",
        help="Include system tables in output",
    )

    status_parser = subparsers.add_parser(
        "status",
        help="Display server status overview",
        parents=[parent_parser],
        epilog="""Examples:
  # Show complete status
  %(prog)s all

  # Show configuration only
  %(prog)s config

  # Show tablespace information
  %(prog)s tablespaces

  # Output as JSON
  %(prog)s all -o json
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    status_parser.add_argument(
        "type",
        nargs="?",
        default="all",
        choices=STATUS_TYPES,
        help="Status type (default: all)",
    )

    return parser.parse_args()


def get_provider(args):
    """Factory function to get the correct output provider."""
    output_format = args.output
    if output_format == "table" and not RICH_AVAILABLE:
        output_format = "json"

    if output_format == "table" and RICH_AVAILABLE:
        from rich.console import Console
        return RichOutputProvider(console=Console(), ascii_borders=args.rich_ascii)
    if output_format == "json":
        return JsonOutputProvider()
    if output_format == "csv":
        return CsvOutputProvider()
    if output_format == "tsv":
        return TsvOutputProvider()

    return JsonOutputProvider()


def parse_version(version_str: str) -> Tuple[int, int, int]:
    """Parse version string like '19.0.0' to tuple."""
    parts = version_str.split('.')
    major = int(parts[0]) if len(parts) > 0 else 19
    minor = int(parts[1]) if len(parts) > 1 else 0
    patch = int(parts[2]) if len(parts) > 2 else 0
    return (major, minor, patch)


def handle_info(args, provider: Any):
    """Handle info subcommand."""
    is_connected = False
    dialect = None
    version_display = None

    version = parse_version(args.version) if args.version else (19, 0, 0)
    dialect = OracleDialect(version=version)
    version_display = f"{version[0]}.{version[1]}.{version[2]}"

    try:
        config = OracleConnectionConfig(
            host=args.host,
            port=args.port,
            service_name=args.service,
            username=args.user,
            password=args.password,
        )
        backend = OracleBackend(connection_config=config)
        backend.connect()
        backend.introspect_and_adapt()
        
        dialect = backend.dialect
        version_tuple = backend.get_server_version()
        if version_tuple:
            version_display = f"{version_tuple[0]}.{version_tuple[1]}.{version_tuple[2]}"
        is_connected = True
        backend.disconnect()
    except Exception as e:
        logger.warning("Could not connect to database: %s", e)
        logger.warning("Using default values for dialect information.")

    info = {
        "database": {
            "type": "oracle",
            "version": version_display,
            "version_tuple": list(dialect.version),
            "connected": is_connected,
        },
        "features": {
            "hierarchical_queries": dialect.supports_hierarchical_queries(),
            "pivot": dialect.supports_pivot(),
            "unpivot": dialect.supports_unpivot(),
            "query_hints": dialect.supports_query_hints(),
            "native_json": dialect.supports_native_json(),
            "boolean_type": dialect.supports_boolean_type(),
            "vector_type": dialect.supports_vector_type(),
            "json_duality": dialect.supports_json_duality(),
        },
        "locking": {
            "for_update": dialect.supports_for_update(),
            "for_update_nowait": dialect.supports_for_update_nowait(),
            "for_update_wait": dialect.supports_for_update_wait(),
            "for_update_skip_locked": dialect.supports_for_update_skip_locked(),
        },
        "pagination": {
            "fetch_first": version >= (12, 0, 0),
            "rownum": True,
        },
    }

    if args.output == "json" or not RICH_AVAILABLE:
        print(json.dumps(info, indent=2))
    else:
        _display_info_rich(info, version_display, is_connected)

    return info


def _display_info_rich(info: Dict, version_display: str, is_connected: bool):
    """Display info using rich console."""
    from rich.console import Console
    
    console = Console(force_terminal=True)
    
    console.print("\n[bold cyan]Oracle Environment Information[/bold cyan]\n")
    
    if is_connected:
        console.print(f"[bold]Oracle Version:[/bold] {version_display} [dim](from actual connection)[/dim]\n")
    else:
        console.print(f"[bold]Oracle Version:[/bold] {version_display} [yellow](default value - no database connection)[/yellow]\n")
    
    console.print("[bold green]Features:[/bold green]")
    features = info.get("features", {})
    for name, supported in features.items():
        status = "[green][OK][/green]" if supported else "[red][X][/red]"
        name_display = name.replace("_", " ").title()
        console.print(f"  {status} {name_display}")
    
    console.print("\n[bold green]Locking:[/bold green]")
    locking = info.get("locking", {})
    for name, supported in locking.items():
        status = "[green][OK][/green]" if supported else "[red][X][/red]"
        name_display = name.replace("_", " ").title()
        console.print(f"  {status} {name_display}")
    
    console.print("\n[bold green]Pagination:[/bold green]")
    pagination = info.get("pagination", {})
    for name, supported in pagination.items():
        status = "[green][OK][/green]" if supported else "[red][X][/red]"
        name_display = name.replace("_", " ").title()
        console.print(f"  {status} {name_display}")
    
    console.print()


def execute_query_sync(sql_query: str, backend: OracleBackend, provider: Any):
    """Execute a SQL query synchronously."""
    try:
        backend.connect()
        provider.display_query(sql_query, is_async=False)
        result = backend.execute(sql_query)

        if not result:
            provider.display_no_result_object()
        else:
            provider.display_success(result.affected_rows, result.duration)
            if result.data:
                provider.display_results(result.data)
            else:
                provider.display_no_data()

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        provider.display_unexpected_error(e, is_async=False)
        sys.exit(1)
    finally:
        if backend._connection:
            backend.disconnect()
            provider.display_disconnect(is_async=False)


async def execute_query_async(sql_query: str, backend: AsyncOracleBackend, provider: Any):
    """Execute a SQL query asynchronously."""
    try:
        await backend.connect()
        provider.display_query(sql_query, is_async=True)
        result = await backend.execute(sql_query)

        if not result:
            provider.display_no_result_object()
        else:
            provider.display_success(result.affected_rows, result.duration)
            if result.data:
                provider.display_results(result.data)
            else:
                provider.display_no_data()

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        provider.display_unexpected_error(e, is_async=True)
        sys.exit(1)
    finally:
        if backend._connection:
            await backend.disconnect()
            provider.display_disconnect(is_async=True)


def handle_introspect_sync(args, backend: OracleBackend, provider: Any):
    """Handle introspect subcommand synchronously."""
    try:
        backend.connect()
        introspector = backend.introspector

        if args.type == "tables":
            tables = introspector.list_tables(
                schema=args.owner,
                include_system=args.include_system
            )
            data = _serialize_for_output(tables)
            provider.display_results(data, title="Tables")

        elif args.type == "views":
            views = introspector.list_views(schema=args.owner)
            data = _serialize_for_output(views)
            provider.display_results(data, title="Views")

        elif args.type == "table":
            if not args.name:
                print("Error: Table name is required for 'table' introspection", file=sys.stderr)
                sys.exit(1)
            info = introspector.get_table_info(args.name, schema=args.owner)
            if info:
                provider.display_results(_serialize_for_output(info.columns), title=f"Columns of {args.name}")
                if info.indexes:
                    provider.display_results(_serialize_for_output(info.indexes), title=f"Indexes of {args.name}")
                if info.foreign_keys:
                    provider.display_results(_serialize_for_output(info.foreign_keys), title=f"Foreign Keys of {args.name}")
            else:
                print(f"Error: Table '{args.name}' not found", file=sys.stderr)
                sys.exit(1)

        elif args.type == "columns":
            if not args.name:
                print("Error: Table name is required for 'columns' introspection", file=sys.stderr)
                sys.exit(1)
            columns = introspector.list_columns(args.name, schema=args.owner)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            if not args.name:
                print("Error: Table name is required for 'indexes' introspection", file=sys.stderr)
                sys.exit(1)
            indexes = introspector.list_indexes(args.name, schema=args.owner)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            if not args.name:
                print("Error: Table name is required for 'foreign-keys' introspection", file=sys.stderr)
                sys.exit(1)
            fks = introspector.list_foreign_keys(args.name, schema=args.owner)
            data = _serialize_for_output(fks)
            provider.display_results(data, title=f"Foreign Keys of {args.name}")

        elif args.type == "sequences":
            sequences = introspector.list_sequences(schema=args.owner)
            data = _serialize_for_output(sequences)
            provider.display_results(data, title="Sequences")

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


def handle_status_sync(args, backend: OracleBackend, provider: Any):
    """Handle status subcommand synchronously."""
    try:
        backend.connect()
        backend.introspect_and_adapt()
        
        status_introspector = backend.introspector.status
        status_type = args.type

        if status_type == "all":
            status = status_introspector.get_overview()
            if args.output == "json" or not RICH_AVAILABLE:
                print(json.dumps(_serialize_for_output(status), indent=2))
            else:
                _display_status_rich(status, args.verbose)
        elif status_type == "config":
            from rhosocial.activerecord.backend.introspection.status import StatusCategory
            config_items = status_introspector.list_configuration(StatusCategory.CONFIGURATION)
            data = _serialize_for_output(config_items)
            provider.display_results(data, title="Configuration")
        elif status_type == "performance":
            from rhosocial.activerecord.backend.introspection.status import StatusCategory
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


def _display_status_rich(status, verbose: int = 0):
    """Display status using rich console."""
    from rich.console import Console
    from rich.table import Table

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
            console.print(f"  [bold]User:[/bold] {session.user}")
        if session.database:
            console.print(f"  [bold]Database:[/bold] {session.database}")
        if session.host:
            console.print(f"  [bold]Host:[/bold] {session.host}")

    console.print()


def _format_size(size_bytes: int) -> str:
    """Format byte size to human-readable string."""
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def main():
    """Main entry point."""
    args = parse_args()

    if args.command is None:
        print("Error: Please specify a command: 'info', 'query', 'introspect', or 'status'", file=sys.stderr)
        print("Use --help for more information.", file=sys.stderr)
        sys.exit(1)

    provider = get_provider(args)

    if args.command == "info":
        handle_info(args, provider)
        return

    if args.command == "introspect":
        config = OracleConnectionConfig(
            host=args.host,
            port=args.port,
            service_name=args.service,
            username=args.user,
            password=args.password,
        )

        if args.use_async:
            backend = AsyncOracleBackend(connection_config=config)
            print("Async introspection not yet implemented", file=sys.stderr)
            sys.exit(1)
        else:
            backend = OracleBackend(connection_config=config)
            handle_introspect_sync(args, backend, provider)
        return

    if args.command == "status":
        config = OracleConnectionConfig(
            host=args.host,
            port=args.port,
            service_name=args.service,
            username=args.user,
            password=args.password,
        )

        if args.use_async:
            backend = AsyncOracleBackend(connection_config=config)
            print("Async status not yet implemented", file=sys.stderr)
            sys.exit(1)
        else:
            backend = OracleBackend(connection_config=config)
            handle_status_sync(args, backend, provider)
        return

    if args.command == "query":
        numeric_level = getattr(logging, args.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {args.log_level}")

        if RICH_AVAILABLE:
            from rich.console import Console
            handler = RichHandler(
                rich_tracebacks=True,
                show_path=False,
                console=Console(stderr=True),
            )
            logging.basicConfig(
                level=numeric_level,
                format="%(message)s",
                datefmt="[%X]",
                handlers=[handler],
            )
        else:
            logging.basicConfig(
                level=numeric_level,
                format="%(asctime)s - %(levelname)s - %(message)s",
                stream=sys.stderr,
            )

        provider.display_greeting()

        sql_source = None
        if args.sql:
            sql_source = args.sql
        elif args.file:
            try:
                with open(args.file, "r", encoding="utf-8") as f:
                    sql_source = f.read()
            except FileNotFoundError:
                logger.error(f"Error: File not found at {args.file}")
                sys.exit(1)
        elif not sys.stdin.isatty():
            sql_source = sys.stdin.read()

        if not sql_source:
            print("Error: No SQL query provided. Use SQL argument, --file, or stdin.", file=sys.stderr)
            sys.exit(1)

        config = OracleConnectionConfig(
            host=args.host,
            port=args.port,
            service_name=args.service,
            username=args.user,
            password=args.password,
        )

        if args.use_async:
            backend = AsyncOracleBackend(connection_config=config)
            asyncio.run(execute_query_async(sql_source, backend, provider))
        else:
            backend = OracleBackend(connection_config=config)
            execute_query_sync(sql_source, backend, provider)


if __name__ == "__main__":
    main()
