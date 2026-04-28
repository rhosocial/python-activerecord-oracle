# src/rhosocial/activerecord/backend/impl/oracle/cli/info.py
"""info subcommand - Display Oracle environment information.

info can optionally connect to a database for version introspection,
falling back to --version flag when no connection is available.
"""

import argparse
import json
import logging
from typing import Any, Dict, Tuple

from .connection import add_connection_args, add_version_arg, resolve_connection_config_from_args
from .output import create_provider, RICH_AVAILABLE

logger = logging.getLogger(__name__)

OUTPUT_CHOICES = ['table', 'json']


def create_parser(subparsers):
    """Create the info subcommand parser."""
    parser = subparsers.add_parser(
        'info',
        help='Display Oracle environment information',
        epilog="""Examples:
  # Show info using default version (19.0.0)
  %(prog)s

  # Show info for a specific version
  %(prog)s --version 21.0.0

  # Show info from actual database connection
  %(prog)s --host localhost --service XEPDB1 --user system --password secret

  # Output as JSON
  %(prog)s -o json
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
    add_version_arg(parser)

    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='Increase verbosity. -v for families, -vv for details.',
    )

    parser.add_argument(
        '--rich-ascii',
        action='store_true',
        help='Use ASCII characters for rich table borders.',
    )

    return parser


def parse_version(version_str: str) -> Tuple[int, int, int]:
    """Parse version string like '19.0.0' to tuple."""
    parts = version_str.split('.')
    major = int(parts[0]) if len(parts) > 0 else 19
    minor = int(parts[1]) if len(parts) > 1 else 0
    patch = int(parts[2]) if len(parts) > 2 else 0
    return (major, minor, patch)


def handle(args):
    """Handle the info subcommand."""
    provider = create_provider(args.output, ascii_borders=args.rich_ascii)

    is_connected = False
    dialect = None
    version_display = None

    named_conn = getattr(args, "named_connection", None)
    if named_conn or (args.host and args.service):
        try:
            from rhosocial.activerecord.backend.impl.oracle import OracleBackend
            config = resolve_connection_config_from_args(args)
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
            logger.warning("Could not connect to database for introspection: %s", e)
            logger.warning("Using default values for dialect information.")

    if dialect is None:
        actual_version = args.version
        if actual_version:
            version = parse_version(actual_version)
        else:
            version = (19, 0, 0)
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect(version=version)
        version_display = f"{version[0]}.{version[1]}.{version[2]}"

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
            "fetch_first": dialect.version >= (12, 0, 0),
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
        console.print(f" {status} {name_display}")

    console.print("\n[bold green]Locking:[/bold green]")
    locking = info.get("locking", {})
    for name, supported in locking.items():
        status = "[green][OK][/green]" if supported else "[red][X][/red]"
        name_display = name.replace("_", " ").title()
        console.print(f" {status} {name_display}")

    console.print("\n[bold green]Pagination:[/bold green]")
    pagination = info.get("pagination", {})
    for name, supported in pagination.items():
        status = "[green][OK][/green]" if supported else "[red][X][/red]"
        name_display = name.replace("_", " ").title()
        console.print(f" {status} {name_display}")

    console.print()
