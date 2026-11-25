# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/dialect/test_artifact_registry.py
"""Oracle publishes its own artifact registry, and it is the only one it reads.

Core f8de2b36 split artifact names from the process-global table: ``core_registry()``
holds core's kinds and is frozen, a backend composes its own on top of it
(``backend_registry()``), and ``SQLDialectBase.artifact_registry()`` is what a
read through a dialect resolves against. Oracle overrides that method with its
own composition, so the facts worth stating directly are:

* the dialect resolves a kind only Oracle declares, and reads it back;
* another backend's kind is refused by name, not silently rendered;
* the published table is one cached, frozen instance — a caller who needs raw
  SQL opts in on a copy, never on the shared table.
"""

import pytest

from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.operators import RawSQL
from rhosocial.activerecord.backend.expression.registry import (
    RegistryError,
    RegistryReason,
)
from rhosocial.activerecord.backend.expression.serialization import (
    deserialize,
    serialize,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
    OracleCoalescePartitionExpression,
)
from rhosocial.activerecord.backend.impl.oracle.registry import backend_registry
from rhosocial.activerecord.backend.impl.sqlite.expression.vacuum import (
    SQLiteVacuumStatement,
)

ORACLE_KIND = "oracle_coalesce_partition_expression"
# Named through the class rather than typed out: the point is that this kind is
# another backend's, so it has to be the kind SQLite actually derives.
FOREIGN_KIND = SQLiteVacuumStatement.kind()


def test_dialect_registry_resolves_oracle_proprietary_kind():
    """Oracle's table names the kinds only Oracle's package declares."""
    dialect = OracleDialect(version=(19, 0, 0))
    registry = dialect.artifact_registry()

    assert ORACLE_KIND in registry.node_kinds()
    assert registry.resolve_node(ORACLE_KIND) is OracleCoalescePartitionExpression


def test_dialect_registry_reads_back_an_oracle_proprietary_node():
    """A read through the dialect's table rebuilds the Oracle node."""
    dialect = OracleDialect(version=(19, 0, 0))
    expression = OracleCoalescePartitionExpression(dialect, Table(dialect, "t"))

    restored = deserialize(serialize(expression), dialect, registry=dialect.artifact_registry())
    assert type(restored) is OracleCoalescePartitionExpression
    assert serialize(restored) == serialize(expression)


def test_foreign_backend_kind_is_refused():
    """No other backend's kind is in Oracle's table, so it is refused by name."""
    dialect = OracleDialect(version=(19, 0, 0))
    registry = dialect.artifact_registry()

    assert FOREIGN_KIND not in registry.node_kinds()
    with pytest.raises(RegistryError) as caught:
        registry.resolve_node(FOREIGN_KIND)
    assert caught.value.reason is RegistryReason.KIND_NOT_REGISTERED
    assert FOREIGN_KIND in str(caught.value)


def test_core_kind_resolves_through_the_oracle_table():
    """Core's kinds are the overlap: every dialect accepts them."""
    dialect = OracleDialect(version=(19, 0, 0))
    registry = dialect.artifact_registry()

    assert registry.resolve_node("column").__module__ == (
        "rhosocial.activerecord.backend.expression.core"
    )
    assert "integer_value" in registry.node_kinds()


def test_backend_registry_is_cached_frozen_and_shared():
    """One frozen instance, handed out by every dialect and every import."""
    registry = backend_registry()
    dialect = OracleDialect(version=(19, 0, 0))

    assert registry is backend_registry()
    assert registry is dialect.artifact_registry()
    assert registry.is_frozen

    with pytest.raises(RegistryError) as caught:
        registry.register_node(OracleCoalescePartitionExpression)
    assert caught.value.reason is RegistryReason.REGISTRY_FROZEN


def test_raw_sql_opt_in_is_a_copy_not_the_shared_table():
    """A caller trusting raw SQL widens its own twin, never the published table."""
    registry = backend_registry()
    twin = registry.twin()
    assert not twin.is_frozen

    twin.register_node(RawSQL)
    assert "raw_sql" in twin.node_kinds()
    assert "raw_sql" not in registry.node_kinds()
    assert "raw_sql" not in backend_registry().node_kinds()


def test_formatter_declaring_nodes_are_auto_discovered():
    """A node is a kind because it names a formatter, not because it was listed.

    The registry walk admits a concrete expression class only when it resolves
    a ``format_method``; adding Oracle's hierarchical, hint, sequence and SHOW
    nodes to the table is therefore a consequence of each class naming the
    dialect formatter that renders it. No manual registration exists for them,
    and the kinds resolve back to the classes that declared the names.
    """
    from rhosocial.activerecord.backend.impl.oracle.expression.hierarchical import (
        LevelExpression,
        PriorExpression,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.hint import (
        OracleHintExpression,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.sequence import (
        OracleSequenceValueExpression,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.show import (
        OracleQueryLocksExpression,
    )

    registry = backend_registry()
    kinded = (
        ("level_expression", LevelExpression),
        ("prior_expression", PriorExpression),
        ("oracle_hint_expression", OracleHintExpression),
        ("oracle_sequence_value_expression", OracleSequenceValueExpression),
        ("oracle_query_locks_expression", OracleQueryLocksExpression),
    )
    for kind, cls in kinded:
        assert kind in registry.node_kinds()
        assert registry.resolve_node(kind) is cls
        assert isinstance(cls.format_method, property)

