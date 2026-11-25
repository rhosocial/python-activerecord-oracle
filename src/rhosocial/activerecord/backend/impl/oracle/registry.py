# src/rhosocial/activerecord/backend/impl/oracle/registry.py
"""The Oracle backend's expression registry.

Oracle owns the kinds its own expression package declares. It does not own
core's kinds and it does not list any other backend's: the overlap with core is
expressed by composition — ``base=core_registry()`` — and there is no
cross-backend merge anywhere.

The registry is built on first use, cached, and frozen. Freezing is what lets
the dialect hand the same table to every reader without one caller's
registration changing what another caller resolves. Its contents are the
classes the package declares: the walk admits a concrete expression class only
when it resolves a ``format_method``, so a node that names the dialect
formatter rendering it is a kind with no separate registration to keep in step.
"""

from rhosocial.activerecord.backend.expression.registry import ExpressionRegistry
from rhosocial.activerecord.backend.expression.serialization import (
    core_registry,
    registry_for_packages,
)

#: The Oracle backend's own expression package.
_ORACLE_EXPRESSION_PACKAGE = "rhosocial.activerecord.backend.impl.oracle.expression"

_backend_registry = None


def backend_registry() -> ExpressionRegistry:
    """Oracle's registry: core's kinds plus Oracle's own, frozen and cached.

    What this backend can read is the Oracle expression package's kinds — the
    statements, predicates and clause nodes Oracle alone declares. Core's kinds
    are present because the base was ``core_registry()``: they are the overlap
    every dialect accepts, and Oracle's trees are built from them. No other
    backend's kinds are in this table, so a SQLite, dummy or MySQL kind is
    refused by name rather than rendered by the wrong dialect.

    Returns:
        The frozen registry ``OracleDialect.artifact_registry()`` exposes.
    """
    global _backend_registry
    if _backend_registry is None:
        # Build into a local and publish only on success: a half-populated
        # instance cached under the module global would make every later call
        # fail with a confusing "no such kind" instead of the real cause.
        _backend_registry = registry_for_packages(
            _ORACLE_EXPRESSION_PACKAGE,
            base=core_registry(),
        )
    return _backend_registry
