# src/rhosocial/activerecord/backend/impl/oracle/cli/info.py
"""info subcommand - Display Oracle environment information.

info connects to a database only when one was named — ``--named-connection``,
explicit connection flags, or an ``ORACLE_*`` environment variable — and reads
the server version from it. Given nothing to connect to it reports what it can
be told and marks the rest "not known": the panel is a report, not a probe, and
it must not open a session against a placeholder service, nor die asking a
release it never learned which features exist.
"""

import argparse
import inspect
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from rhosocial.activerecord.backend.dialect.exceptions import DialectNotAdaptedException

from .connection import (
    add_connection_args,
    add_version_arg,
    connection_named,
    resolve_connection_config_from_args,
)
from .output import create_provider, RICH_AVAILABLE

logger = logging.getLogger(__name__)

OUTPUT_CHOICES = ['table', 'json']


def handle(args):
    """Handle the info subcommand."""
    provider = create_provider(args.output, ascii_borders=args.rich_ascii)

    is_connected = False
    dialect = None
    version_display = None

    if connection_named(args):
        try:
            from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
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
        # Offline mode. The version is whatever the caller stated and nothing
        # else: the previous ``(19, 0, 0)`` here reported 19c for a server this
        # process never spoke to, and every ``supports_*`` below it answered on
        # that invented number. ``None`` leaves the dialect unadapted, so the
        # gates that need a release answer "not known" below rather than
        # claiming one nobody named.
        actual_version = args.version
        version = parse_version(actual_version) if actual_version else None
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        dialect = OracleDialect(version=version)
        version_display = (
            f"{version[0]}.{version[1]}.{version[2]}" if version else "unknown"
        )

    # ``dialect.version`` raises ``DialectNotAdaptedException`` when no version
    # was stated and none could be read, and an info panel is not the place to
    # die on that. Resolve it once: the panel then reports the version as
    # ``None`` and every version-derived flag as ``None`` too, which reads as
    # "not known" rather than as "no" — the same distinction the gates make.
    try:
        effective_version: Optional[Tuple[int, ...]] = tuple(dialect.version)
    except Exception:
        effective_version = None

    def _since(threshold: Tuple[int, int, int]) -> Optional[bool]:
        if effective_version is None:
            return None
        return effective_version >= threshold

    def _gate(method) -> Optional[bool]:
        """Ask one capability gate, reporting "not known" when it cannot answer.

        Oracle's release-derived gates answer by reading ``dialect.version``,
        which refuses on a dialect that was never adapted, and an offline panel
        is exactly that state: there is no server to read a release from and the
        caller named none. Calling such a gate directly turned the refusal into a
        ``DialectNotAdaptedException`` out of ``info``, which is why this helper
        exists. Only the gates that genuinely need a release are affected — the
        rest answer as they always did, so the panel still says ``True`` for a
        capability Oracle has had since 11g.
        """
        try:
            return method()
        except DialectNotAdaptedException:
            return None

    info = {
        "database": {
            "type": "oracle",
            "version": version_display,
            "version_tuple": list(effective_version) if effective_version else None,
            "connected": is_connected,
        },
        "features": {
            "hierarchical_queries": _gate(dialect.supports_hierarchical_queries),
            "pivot": _gate(dialect.supports_pivot),
            "unpivot": _gate(dialect.supports_unpivot),
            "query_hints": _gate(dialect.supports_query_hints),
            "native_json": _gate(dialect.supports_native_json),
            "boolean_type": _gate(dialect.supports_boolean_type),
            "vector_type": _gate(dialect.supports_vector_type),
            "json_duality": _gate(dialect.supports_json_duality),
        },
        "locking": {
            "for_update": _gate(dialect.supports_for_update),
            "for_update_nowait": _gate(dialect.supports_for_update_nowait),
            "for_update_wait": _gate(dialect.supports_for_update_wait),
            "for_update_skip_locked": _gate(dialect.supports_for_update_skip_locked),
        },
        "pagination": {
            "fetch_first": _since((12, 0, 0)),
            "rownum": True,
        },
        "protocols": {},
    }

    # Protocol family support probe (consistent with other backends)
    for group_name, protocols in PROTOCOL_FAMILY_GROUPS.items():
        info["protocols"][group_name] = _build_protocol_info(
            dialect, group_name, protocols, args.verbose
        )

    if args.output == "json" or not RICH_AVAILABLE:
        print(json.dumps(info, indent=2))
    else:
        # The renderer's second parameter is the verbosity, so the count
        # collected above is what belongs there. An earlier version of this call
        # passed ``version_display`` in that position instead, which bound a
        # string to ``verbose`` and raised
        # ``TypeError: '>=' not supported between instances of 'str' and 'int'``
        # on the panel's own first comparison — so the default table output
        # could never render at all, while ``-o json`` was unaffected because
        # it takes the branch above and never reaches the renderer.
        _display_info_rich(info, args.verbose, version_display, is_connected)

    return info


from rhosocial.activerecord.backend.dialect.protocols import (
    WindowFunctionSupport,
    CTESupport,
    FilterClauseSupport,
    ReturningSupport,
    UpsertSupport,
    LateralJoinSupport,
    JoinSupport,
    JSONSupport,
    ExplainSupport,
    GraphSupport,
    SetOperationSupport,
    ViewObjectSupport,
    TableObjectSupport,
    TruncateSupport,
    GeneratedColumnSupport,
    TriggerObjectSupport,
    RoutineObjectSupport,
    AdvancedGroupingSupport,
    ArraySupport,
    ILIKESupport,
    IndexObjectSupport,
    LockingSupport,
    MergeSupport,
    OrderedSetAggregationSupport,
    QualifyClauseSupport,
    NamespaceSupport,
    SequenceObjectSupport,
    TemporalTableSupport,
)

from rhosocial.activerecord.backend.impl.oracle.protocols.trigger_capabilities import (
    OracleTriggerSupport,
)
from rhosocial.activerecord.backend.impl.oracle.protocols.table_capabilities import (
    OracleTableSupport,
)
from rhosocial.activerecord.backend.impl.oracle.protocols.column_capabilities import (
    OracleModifyColumnSupport,
)



PROTOCOL_FAMILY_GROUPS: Dict[str, list] = {
    "Query Features": [
        WindowFunctionSupport,
        CTESupport,
        FilterClauseSupport,
        SetOperationSupport,
        AdvancedGroupingSupport,
    ],
    "JOIN Support": [JoinSupport, LateralJoinSupport],
    "Data Types": [JSONSupport, ArraySupport],
    "DML Features": [
        ReturningSupport,
        UpsertSupport,
        MergeSupport,
        OrderedSetAggregationSupport,
    ],
    "Transaction & Locking": [LockingSupport, TemporalTableSupport],
    "Query Analysis": [ExplainSupport, GraphSupport, QualifyClauseSupport],
    "DDL - Table": [TableObjectSupport, TruncateSupport, GeneratedColumnSupport],
    "DDL - View": [ViewObjectSupport],
    "DDL - Schema & Index": [NamespaceSupport, IndexObjectSupport],
    "DDL - Sequence & Trigger": [
        SequenceObjectSupport,
        TriggerObjectSupport,
        RoutineObjectSupport,
    ],
    "String Matching": [ILIKESupport],
    "Oracle-specific": [
        OracleTriggerSupport,
        OracleTableSupport,
        OracleModifyColumnSupport,
    ],
}

# Group names whose protocols this dialect implements itself rather than
# inheriting from core. The renderer tags these so a reader can tell an
# Oracle-native capability from a core protocol every backend answers. Derived
# from the groups above rather than restated, so adding a dialect-native
# protocol cannot leave the label behind.
DIALECT_SPECIFIC_GROUPS = {
    name
    for name, protocols in PROTOCOL_FAMILY_GROUPS.items()
    if all(
        getattr(protocol, "__module__", "").startswith(
            "rhosocial.activerecord.backend.impl.oracle."
        )
        for protocol in protocols
    )
}

# Candidate arguments probed for the ``supports_*`` methods that require one.
# ``check_protocol_support`` reports a method not listed here as unsupported
# without calling it, so an entry has to name a method this dialect actually
# takes an argument for. ``supports_explain_format`` is currently the only one
# that does; the JSON-function and spatial probes that used to sit here named
# MySQL protocols, and neither could ever be reached: Oracle declares no
# ``supports_json_function`` at all, and its ``supports_spatial_type`` takes no
# argument, so the ``elif method_name in SUPPORT_METHOD_ALL_ARGS`` branch was
# never entered for either key.
SUPPORT_METHOD_ALL_ARGS: Dict[str, List[str]] = {
    "supports_explain_format": ["TEXT", "JSON", "TREE", "XML", "YAML", "DOT"],
}


def create_parser(subparsers):
    """Create the info subcommand parser."""
    parser = subparsers.add_parser(
        "info",
        help="Display Oracle environment information",
        epilog="""Examples:
  # Show info without connecting (no version is assumed)
  %(prog)s

  # Show info for a specific version
  %(prog)s --version 19.0.0

  # Show info from actual database connection
  %(prog)s --host localhost --service XEPDB1 --user system --password secret

  # Output as JSON
  %(prog)s -o json

  # Detailed protocol support
  %(prog)s -vv
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Output format (only table and json; info has nested output)
    parser.add_argument(
        "-o",
        "--output",
        choices=OUTPUT_CHOICES,
        default="table",
        help="Output format (default: table)",
    )

    # Connection arguments (optional; info can work without a database)
    add_connection_args(parser)

    # Version override
    add_version_arg(parser)

    # Verbosity
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity. -v for families, -vv for details.",
    )

    # Rich display options
    parser.add_argument(
        "--rich-ascii",
        action="store_true",
        help="Use ASCII characters for rich table borders.",
    )

    return parser




# ---------------------------------------------------------------------------
# Internal helper functions
# ---------------------------------------------------------------------------


def get_protocol_support_methods(protocol_class: type) -> List[str]:
    """Get all support check methods from a protocol class.

    Supports both 'supports_*' and 'is_*_available' naming patterns.
    """
    methods = []
    for name, member in inspect.getmembers(protocol_class):
        is_supports = name.startswith("supports_")
        is_available = name.startswith("is_") and name.endswith("_available")
        if callable(member) and (is_supports or is_available):
            methods.append(name)
    return sorted(methods)


def check_protocol_support(dialect, protocol_class: type) -> Dict[str, Any]:
    """Check all support methods for a protocol against the dialect.

    For methods requiring parameters, tests all possible arguments.

    Returns:
        Dict with method names as keys. For no-arg methods: bool value.
        For methods with parameters: dict with 'supported', 'total', 'args' keys.
    """
    results = {}
    methods = get_protocol_support_methods(protocol_class)
    for method_name in methods:
        if hasattr(dialect, method_name):
            try:
                method = getattr(dialect, method_name)
                # Check if method requires arguments (beyond self)
                sig = inspect.signature(method)
                params = [p for p in sig.parameters.values() if p.default == inspect.Parameter.empty]
                required_params = [p for p in params if p.name != "self"]

                if len(required_params) == 0:
                    result = method()
                    results[method_name] = bool(result)
                elif method_name in SUPPORT_METHOD_ALL_ARGS:
                    all_args = SUPPORT_METHOD_ALL_ARGS[method_name]
                    arg_results = {}
                    for arg in all_args:
                        try:
                            arg_results[arg] = bool(method(arg))
                        except Exception:
                            arg_results[arg] = False
                    supported_count = sum(1 for v in arg_results.values() if v)
                    results[method_name] = {
                        "supported": supported_count,
                        "total": len(all_args),
                        "args": arg_results,
                    }
                else:
                    results[method_name] = False
            except Exception:
                results[method_name] = False
        else:
            results[method_name] = False
    return results


def parse_version(version_str: str) -> Tuple[int, int, int]:
    """Parse version string like '19.0.0' to tuple."""
    parts = version_str.split(".")
    major = int(parts[0]) if len(parts) > 0 else 0
    minor = int(parts[1]) if len(parts) > 1 else 0
    patch = int(parts[2]) if len(parts) > 2 else 0
    return (major, minor, patch)


def _calculate_protocol_stats(support_methods: Dict[str, Any]) -> Tuple[int, int]:
    """Calculate supported and total counts from support methods.

    Args:
        support_methods: Dict with method names as keys.
            For no-arg methods: bool value.
            For methods with parameters: dict with 'supported', 'total', 'args' keys.

    Returns:
        Tuple of (supported_count, total_count)
    """
    supported_count = 0
    total_count = 0
    for value in support_methods.values():
        if isinstance(value, dict):
            supported_count += value["supported"]
            total_count += value["total"]
        else:
            total_count += 1
            if value:
                supported_count += 1
    return supported_count, total_count


def _build_protocol_info(dialect, group_name: str, protocols: List[type], verbose: int) -> Dict[str, Dict[str, Any]]:
    """Build protocol support information for a single group.

    Args:
        dialect: OracleDialect instance to check against
        group_name: Name of the protocol group
        protocols: List of protocol classes in this group
        verbose: Verbosity level for output detail

    Returns:
        Dict mapping protocol names to their support statistics
    """
    group_info = {}
    for protocol in protocols:
        protocol_name = protocol.__name__
        support_methods = check_protocol_support(dialect, protocol)
        supported_count, total_count = _calculate_protocol_stats(support_methods)

        percentage = round(supported_count / total_count * 100, 1) if total_count > 0 else 0

        if verbose >= 2:
            group_info[protocol_name] = {
                "supported": supported_count,
                "total": total_count,
                "percentage": percentage,
                "methods": support_methods,
            }
        else:
            group_info[protocol_name] = {
                "supported": supported_count,
                "total": total_count,
                "percentage": percentage,
            }
    return group_info


def _get_status_style(pct: float) -> Tuple[str, str]:
    """Get color and symbol based on percentage."""
    if pct == 100:
        return "green", "[OK]"
    elif pct >= 50:
        return "yellow", "[~]"
    elif pct > 0:
        return "red", "[~]"
    else:
        return "red", "[X]"


def _format_method_display(method: str) -> str:
    """Format method name for display."""
    return method.replace("supports_", "").replace("_", " ").replace("is_", "").replace("_available", "")


def _display_method_details(console: Any, method: str, value: Any) -> None:
    """Display detailed method support information."""
    method_display = _format_method_display(method)

    if isinstance(value, dict):
        console.print(f"        [dim]{method_display}:[/dim]")
        for arg, supported in value.get("args", {}).items():
            m_status = "[green][OK][/green]" if supported else "[red][X][/red]"
            console.print(f"            {m_status} {arg}")
    else:
        m_status = "[green][OK][/green]" if value else "[red][X][/red]"
        console.print(f"        {m_status} {method_display}")


def _display_protocol_item(console: Any, protocol_name: str, stats: Dict[str, Any], verbose: int) -> None:
    """Display a single protocol's support information."""
    pct = stats["percentage"]
    color, symbol = _get_status_style(pct)

    bar_len = 20
    filled = int(pct / 100 * bar_len)
    progress_bar = "#" * filled + "-" * (bar_len - filled)

    sup = stats["supported"]
    tot = stats["total"]
    console.print(
        f"    [{color}]{symbol}[/{color}] {protocol_name}: [{color}]{progress_bar}[/{color}] {pct:.0f}% ({sup}/{tot})"
    )

    if verbose >= 2 and "methods" in stats:
        for method, value in stats["methods"].items():
            _display_method_details(console, method, value)


def _display_protocol_group(console: Any, group_name: str, protocols: Dict[str, Any], verbose: int) -> None:
    """Display a protocol group's support information."""
    if group_name in DIALECT_SPECIFIC_GROUPS:
        console.print(f"\n  [bold underline]{group_name}:[/bold underline] [dim](dialect-specific)[/dim]")
    else:
        console.print(f"\n  [bold underline]{group_name}:[/bold underline]")

    for protocol_name, stats in protocols.items():
        _display_protocol_item(console, protocol_name, stats, verbose)


def _display_info_rich(info: Dict, verbose: int, version_display: str, is_connected: bool = True):
    """Display info using rich console."""
    from rich.console import Console

    console = Console(force_terminal=True)

    # This renderer is the Oracle CLI's own output; it was pasted in from MySQL
    # with the brand strings still in it, so the Oracle panel used to title
    # itself "MySQL Environment Information" and report a "MySQL Version".
    console.print("\n[bold cyan]Oracle Environment Information[/bold cyan]\n")

    if is_connected:
        console.print(f"[bold]Oracle Version:[/bold] {version_display} [dim](from actual connection)[/dim]\n")
    else:
        console.print(
            f"[bold]Oracle Version:[/bold] {version_display} [yellow](no database connection)[/yellow]\n"
        )

    label = "Detailed" if verbose >= 2 else "Family Overview"
    console.print(f"[bold green]Protocol Support ({label}):[/bold green]")

    for group_name, protocols in info["protocols"].items():
        _display_protocol_group(console, group_name, protocols, verbose)

    console.print()
