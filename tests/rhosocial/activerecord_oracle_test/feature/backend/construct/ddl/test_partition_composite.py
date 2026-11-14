# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_partition_composite.py
"""Oracle composite partition expression audit.

These tests pin the two-level contract the backend exposes:

* the top-level strategy is per table (``OraclePartitionByRange`` / ``List``
  / ``Hash``), and
* the subpartition strategy is **one table-level** ``OracleSubpartitionClause``;
  a top-level partition may vary only its definitions or its HASH count.

They also cover the renderer additions that make per-partition variation
expressible: ``SUBPARTITIONS n`` on a partition definition and
``SUBPARTITION TEMPLATE (...)`` on the table-level clause.
"""

from datetime import date

import pytest

from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionByHash,
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
    OracleSubpartitionClause,
    OracleSubpartitionDefinition,
    OracleSubpartitionStrategy,
)


def _dialect(version=(23, 1, 0)) -> OracleDialect:
    return OracleDialect(version=version)


class TestCompositeRangeHash:
    """RANGE-HASH composite partitioning expression construction."""

    def test_hash_count_is_table_level(self):
        d = _dialect()
        clause = OraclePartitionByRange(
            d,
            [Column(d, "CREATED_AT")],
            subpartition_by=OracleSubpartitionClause(
                d,
                OracleSubpartitionStrategy.HASH,
                keys=[Column(d, "ID")],
                count=2,
            ),
            partitions=[
                OraclePartitionDefinition(
                    name="P1",
                    less_than=[OraclePartitionValue(d, date(2026, 2, 1))],
                ),
            ],
        )
        sql, params = clause.to_sql()
        assert "PARTITION BY RANGE" in sql
        assert 'SUBPARTITION BY HASH ("ID") SUBPARTITIONS 2' in sql
        assert params == ()

    def test_per_partition_count_override(self):
        d = _dialect()
        clause = OraclePartitionByRange(
            d,
            [Column(d, "CREATED_AT")],
            subpartition_by=OracleSubpartitionClause(
                d,
                OracleSubpartitionStrategy.HASH,
                keys=[Column(d, "ID")],
                count=2,
            ),
            partitions=[
                OraclePartitionDefinition(
                    name="P1",
                    less_than=[OraclePartitionValue(d, date(2026, 2, 1))],
                    subpartitions_count=3,
                ),
                OraclePartitionDefinition(
                    name="P2",
                    less_than=[OraclePartitionMaxValue(d)],
                ),
            ],
        )
        sql, _ = clause.to_sql()
        assert (
            'PARTITION "P1" VALUES LESS THAN (DATE \'2026-02-01\') SUBPARTITIONS 3'
            in sql
        )
        assert 'PARTITION "P2" VALUES LESS THAN (MAXVALUE)' in sql
        # The table-level count stays; only P1 overrides it.
        assert "SUBPARTITIONS 2" in sql

    def test_mixed_partition_count_and_definitions(self):
        d = _dialect()
        clause = OraclePartitionByHash(
            d,
            [Column(d, "ID")],
            partitions=[
                OraclePartitionDefinition(
                    name="P0",
                    subpartitions_count=3,
                ),
                OraclePartitionDefinition(
                    name="P1",
                    subpartition_definitions=[
                        OracleSubpartitionDefinition(
                            name="P1_A",
                            in_values=[OraclePartitionValue(d, "A")],
                        ),
                    ],
                ),
            ],
            subpartition_by=OracleSubpartitionClause(
                d,
                OracleSubpartitionStrategy.LIST,
                keys=[Column(d, "REGION")],
            ),
        )
        sql, _ = clause.to_sql()
        assert 'PARTITION "P0" SUBPARTITIONS 3' in sql
        assert 'PARTITION "P1" (SUBPARTITION "P1_A" VALUES (\'A\'))' in sql


class TestCompositeRangeList:
    """RANGE-LIST composite partitioning: explicit, per-partition definitions."""

    def test_per_partition_definition_counts_differ(self):
        d = _dialect()
        clause = OraclePartitionByRange(
            d,
            [Column(d, "CREATED_AT")],
            subpartition_by=OracleSubpartitionClause(
                d,
                OracleSubpartitionStrategy.LIST,
                keys=[Column(d, "REGION")],
            ),
            partitions=[
                OraclePartitionDefinition(
                    name="P1",
                    less_than=[OraclePartitionValue(d, date(2026, 2, 1))],
                    subpartition_definitions=[
                        OracleSubpartitionDefinition(
                            name="P1_E",
                            in_values=[OraclePartitionValue(d, "EAST")],
                        ),
                        OracleSubpartitionDefinition(
                            name="P1_W",
                            in_values=[OraclePartitionValue(d, "WEST")],
                        ),
                    ],
                ),
                OraclePartitionDefinition(
                    name="P2",
                    less_than=[OraclePartitionValue(d, date(2026, 3, 1))],
                    subpartition_definitions=[
                        OracleSubpartitionDefinition(
                            name="P2_E",
                            in_values=[OraclePartitionValue(d, "EAST")],
                        ),
                    ],
                ),
            ],
        )
        sql, _ = clause.to_sql()
        assert 'SUBPARTITION "P1_E" VALUES (\'EAST\')' in sql
        assert 'SUBPARTITION "P1_W" VALUES (\'WEST\')' in sql
        assert 'SUBPARTITION "P2_E" VALUES (\'EAST\')' in sql
        # P1 has two definitions, P2 one: Oracle accepts the variation
        # because the strategy (LIST) is still table level.
        assert sql.count("SUBPARTITION") > 3

    def test_template_definitions_render(self):
        d = _dialect()
        clause = OracleSubpartitionClause(
            d,
            OracleSubpartitionStrategy.LIST,
            keys=[Column(d, "REGION")],
            templates=[
                OracleSubpartitionDefinition(
                    name="SP_E",
                    in_values=[OraclePartitionValue(d, "EAST")],
                ),
                OracleSubpartitionDefinition(
                    name="SP_W",
                    in_values=[OraclePartitionValue(d, "WEST")],
                ),
            ],
        )
        sql, params = clause.to_sql()
        assert "SUBPARTITION BY LIST" in sql
        assert "SUBPARTITION TEMPLATE" in sql
        assert 'SUBPARTITION "SP_E" VALUES (\'EAST\')' in sql
        assert params == ()


class TestSubpartitionDefinitionValidation:
    """Renderer-level guard rails for per-partition overrides."""

    def test_count_and_definitions_are_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            OraclePartitionDefinition(
                name="P1",
                less_than=[OraclePartitionValue(_dialect(), 1)],
                subpartitions_count=2,
                subpartition_definitions=[
                    OracleSubpartitionDefinition(name="SP1"),
                ],
            )

    def test_count_must_be_positive(self):
        with pytest.raises(ValueError, match="positive integer"):
            OraclePartitionDefinition(
                name="P1",
                less_than=[OraclePartitionValue(_dialect(), 1)],
                subpartitions_count=0,
            )

    def test_count_rejects_bool(self):
        with pytest.raises(TypeError, match="must be an int"):
            OraclePartitionDefinition(
                name="P1",
                less_than=[OraclePartitionValue(_dialect(), 1)],
                subpartitions_count=True,
            )

    def test_subpartition_template_still_renders_for_hash(self):
        d = _dialect()
        clause = OracleSubpartitionClause(
            d,
            OracleSubpartitionStrategy.HASH,
            keys=[Column(d, "ID")],
            templates=[
                OracleSubpartitionDefinition(name="SP1"),
                OracleSubpartitionDefinition(name="SP2"),
            ],
        )
        sql, _ = clause.to_sql()
        assert "SUBPARTITION BY HASH" in sql
        assert "SUBPARTITION TEMPLATE" in sql
        assert "SUBPARTITIONS" not in sql

    def test_hash_template_and_count_are_rejected(self):
        """Oracle refuses ``SUBPARTITIONS n`` next to a template (ORA-00922)."""
        d = _dialect()
        with pytest.raises(ValueError, match="mutually exclusive"):
            OracleSubpartitionClause(
                d,
                OracleSubpartitionStrategy.HASH,
                keys=[Column(d, "ID")],
                count=2,
                templates=[
                    OracleSubpartitionDefinition(name="SP1"),
                    OracleSubpartitionDefinition(name="SP2"),
                ],
            )
