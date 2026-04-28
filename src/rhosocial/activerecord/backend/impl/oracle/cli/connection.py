# src/rhosocial/activerecord/backend/impl/oracle/cli/connection.py
"""Connection argument parsing and backend creation for Oracle CLI."""

import os

from .output import RICH_AVAILABLE


def add_connection_args(parser):
    """Add Oracle connection arguments to a subcommand parser.

    Each subcommand that needs a database connection calls this.
    """
    parser.add_argument(
        "--host",
        default=os.getenv("ORACLE_HOST", "localhost"),
        help="Database host (env: ORACLE_HOST, default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("ORACLE_PORT", "1521")),
        help="Database port (env: ORACLE_PORT, default: 1521)",
    )
    parser.add_argument(
        "--service",
        default=os.getenv("ORACLE_SERVICE", "ORCL"),
        help="Oracle service name (env: ORACLE_SERVICE, default: ORCL)",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("ORACLE_USER", "system"),
        help="Database user (env: ORACLE_USER, default: system)",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("ORACLE_PASSWORD", ""),
        help="Database password (env: ORACLE_PASSWORD)",
    )
    parser.add_argument(
        "--use-async",
        action="store_true",
        help="Use asynchronous backend",
    )
    parser.add_argument(
        "--named-connection",
        dest="named_connection",
        metavar="QUALIFIED_NAME",
        help="Named connection from Python module (e.g., myapp.connections.prod_db).",
    )
    parser.add_argument(
        "--conn-param",
        action="append",
        metavar="KEY=VALUE",
        default=[],
        dest="connection_params",
        help="Connection parameter override for named connection. Can be specified multiple times.",
    )
    parser.add_argument(
        "--mode",
        choices=["thin", "thick"],
        default="thin",
        help="Oracle client mode (thin or thick)",
    )


def add_version_arg(parser):
    """Add --version argument (used only by info subcommand)."""
    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help='Oracle version to simulate (e.g., "19.0.0", "21.0.0"). Default: auto-detect.',
    )


def create_connection_parent_parser():
    """Create a parent parser with connection and output arguments.

    Used by shared CLI helpers (named-query, named-procedure) that
    require a parent_parser containing connection parameters.
    """
    import argparse
    parent = argparse.ArgumentParser(add_help=False)
    add_connection_args(parent)
    # Output parameters
    parent.add_argument(
        "-o", "--output",
        choices=["table", "json", "csv", "tsv"],
        default="table",
        help='Output format. Defaults to "table" if rich is installed.',
    )
    parent.add_argument(
        "--rich-ascii",
        action="store_true",
        help="Use ASCII characters for rich table borders.",
    )
    return parent


def resolve_connection_config_from_args(args):
    """Resolve Oracle connection config from parsed args.

    Priority order:
    1. --named-connection + --conn-param
    2. Explicit connection parameters (--host, --port, etc.)
    3. Default values
    """
    from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
    from rhosocial.activerecord.backend.named_connection.cli import parse_params
    from rhosocial.activerecord.backend.named_connection import NamedConnectionResolver

    named_conn = getattr(args, "named_connection", None)
    conn_params = getattr(args, "connection_params", [])

    if conn_params:
        conn_params = parse_params(conn_params)
    else:
        conn_params = {}

    if named_conn:
        resolver = NamedConnectionResolver(named_conn).load()
        if conn_params:
            return resolver.resolve(conn_params)
        return resolver.resolve({})

    # Fallback to explicit connection parameters
    return OracleConnectionConfig(
        host=args.host or "localhost",
        port=args.port or 1521,
        service_name=args.service,
        username=args.user,
        password=args.password,
    )


def create_backend(args):
    """Create, connect, and introspect an Oracle backend from parsed args."""
    from rhosocial.activerecord.backend.impl.oracle import OracleBackend
    config = resolve_connection_config_from_args(args)
    backend = OracleBackend(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    return backend
