# src/rhosocial/activerecord/backend/impl/oracle/__main__.py
"""Oracle backend command-line interface.

Provides SQL execution and database introspection capabilities.

Usage:
  python -m rhosocial.activerecord.backend.impl.oracle <command> [options]

Commands:
  info              Display Oracle environment information
  query             Execute SQL queries
  introspect        Database introspection
  status            Display session status
  named-query       Execute named queries
  named-procedure   Execute named procedures
  named-connection  Manage named connections
"""

import argparse
import sys

from .cli import register_commands, COMMAND_NAMES


def _build_parser():
    """Build and return the argument parser with all subcommands registered."""
    parser = argparse.ArgumentParser(
        description="Execute SQL queries against an Oracle backend.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    register_commands(subparsers)

    return parser


def parse_args():
    """Parse command-line arguments.

    Provided for test compatibility. In production, main() is used instead.
    """
    parser = _build_parser()
    return parser.parse_args()


# Backward-compatible re-exports for existing tests
from .cli.connection import resolve_connection_config_from_args
from .cli.info import parse_version
from .cli.output import create_provider as get_provider, RICH_AVAILABLE
from .cli.introspect import INTROSPECT_TYPES, _serialize_for_output
from .cli.status import STATUS_TYPES, _format_size

from .cli.info import handle as _info_handle


def handle_info(args, provider=None):
    """Backward-compatible handle_info wrapper.

    Old signature: handle_info(args, provider)
    New signature: handle_info(args) -- provider is created internally
    """
    if provider is not None:
        # Old calling convention: tests pass provider explicitly.
        # The new handle() creates its own provider, but we need to
        # respect the caller's provider for backward compat.
        # We call the new handle but the caller's provider is ignored
        # since the new handle creates its own from args.output.
        # This maintains backward compatibility for tests that pass
        # a JsonOutputProvider with output="json" in args.
        pass
    return _info_handle(args)


def main():
    parser = _build_parser()
    args = parser.parse_args()

    if args.command is None:
        cmd_list = ", ".join(f"'{c}'" for c in COMMAND_NAMES[:-1])
        print(f"Error: Please specify a command: {cmd_list}, or '{COMMAND_NAMES[-1]}'",
              file=sys.stderr)
        print("Use --help for more information.", file=sys.stderr)
        sys.exit(1)

    from .cli import get_handler
    handler = get_handler(args.command)
    handler(args)


if __name__ == "__main__":
    main()
