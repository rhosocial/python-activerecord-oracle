# src/rhosocial/activerecord/backend/impl/oracle/cli/connection.py
"""Connection argument parsing and backend creation for Oracle CLI."""

import os



# The placeholder a connection argument carries when neither the command line
# nor an ORACLE_* variable named one. Kept here, next to the defaults that
# produce it, so `connection_named` below and `add_connection_args` cannot drift
# apart. A command that can report without a database needs to tell "nobody named
# a connection" from "named localhost:1521/ORCL", and these values are the only
# thing that distinguishes them.
_CONNECTION_PLACEHOLDERS = {
    "host": "localhost",
    "port": 1521,
    "service": "ORCL",
    "user": "system",
    "password": "",
}


def add_connection_args(parser):
    """Add Oracle connection arguments to a subcommand parser.

    Each subcommand that needs a database connection calls this.

    Every field carries a placeholder when nothing named one, which is why
    ``connection_named`` exists: a subcommand that can also run without a
    database has no other way to tell an omitted connection from a real one.
    """
    parser.add_argument(
        "--host",
        default=os.getenv("ORACLE_HOST", _CONNECTION_PLACEHOLDERS["host"]),
        help="Database host (env: ORACLE_HOST, default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("ORACLE_PORT", _CONNECTION_PLACEHOLDERS["port"])),
        help="Database port (env: ORACLE_PORT, default: 1521)",
    )
    parser.add_argument(
        "--service",
        "--database",
        dest="service",
        default=os.getenv("ORACLE_SERVICE", _CONNECTION_PLACEHOLDERS["service"]),
        help="Oracle service name (env: ORACLE_SERVICE, default: ORCL). "
        "--database is an alias for --service.",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("ORACLE_USER", _CONNECTION_PLACEHOLDERS["user"]),
        help="Database user (env: ORACLE_USER, default: system)",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("ORACLE_PASSWORD", _CONNECTION_PLACEHOLDERS["password"]),
        help="Database password (env: ORACLE_PASSWORD)",
    )
    parser.add_argument(
        "--ssl",
        choices=["auto", "require", "verify-ca", "verify-full", "disabled"],
        default="auto",
        help="SSL mode (env: ORACLE_SSL, default: auto)",
    )
    parser.add_argument(
        "--async",
        action="store_true",
        dest="is_async",
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


def connection_named(args) -> bool:
    """Whether the caller named a connection, by flag or by environment.

    True when ``--named-connection`` was given, or when any connection field
    holds something other than the placeholder it defaults to. False for a bare
    invocation, which left every field on its placeholder.

    The placeholders are why this exists. ``add_connection_args`` must give
    every field a default — ``query`` and ``status`` need a usable target when
    nothing is passed — so the parsed values alone cannot distinguish
    ``--host localhost`` from no flag at all. A subcommand that can also report
    without a database has to make that call, and it used to make it by opening
    a session: bare ``info`` connected to localhost's ORCL on every run, against
    a service that exists only where someone installed one, and reported the
    failure instead of the panel it was asked for. The one thing this cannot see
    is a connection named *exactly* the placeholder values, which is declined
    here; pass a real service, or set ``ORACLE_SERVICE``, to ask for one.
    """
    if getattr(args, "named_connection", None):
        return True
    return any(
        getattr(args, field, placeholder) != placeholder
        for field, placeholder in _CONNECTION_PLACEHOLDERS.items()
    )


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
    from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
    config = resolve_connection_config_from_args(args)
    backend = OracleBackend(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    return backend
