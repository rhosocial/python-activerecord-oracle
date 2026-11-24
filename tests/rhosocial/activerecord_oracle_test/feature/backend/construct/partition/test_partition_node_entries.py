# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/partition/test_partition_node_entries.py
"""Core partition nodes dispatch to Oracle's renderers through their own entry.

Core's ``PartitionDefinitionExpression`` / ``SubpartitionDefinitionExpression``
name a formatter via ``format_method``; a backend that implements the rendering
under a differently-named method leaves the node unreachable. These cases pin
that ``node.to_sql()`` reaches Oracle's implementation.
"""

import pytest

from rhosocial.activerecord.backend.expression.statements.ddl_partition import (
    PartitionDefinitionExpression,
    SubpartitionDefinitionExpression,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionValue,
)


@pytest.fixture
def dialect():
    return OracleDialect((19, 0, 0))


def test_partition_definition_node_renders(dialect):
    definition = PartitionDefinitionExpression(
        dialect, "p0", less_than=(OraclePartitionValue(dialect, 100),)
    )
    sql, params = definition.to_sql()
    assert sql == 'PARTITION "P0" VALUES LESS THAN (100)'
    assert params == ()


def test_subpartition_definition_node_renders(dialect):
    subpartition = SubpartitionDefinitionExpression(dialect, "sp0")
    sql, params = subpartition.to_sql()
    assert sql == 'SUBPARTITION "SP0"'
    assert params == ()
