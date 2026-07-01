# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_partition_phase5bcd.py
"""Phase 5 sub-phases B/C/D tests: INTERVAL / REFERENCE / COMPOSITE.

Verifies expression construction (SQL shape + parameter order) and
capability gating for the Oracle-specific partition strategies. Real
Oracle execution is covered in ``test_oracle_partition_phase5_real.py``.
"""

import pytest

from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.operators import RawSQLExpression
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OracleIntervalPartitionClause,
    OraclePartitionByHash,
    OraclePartitionByList,
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
    OracleReferencePartitionClause,
    OracleSubpartitionClause,
    OracleSubpartitionDefinition,
    OracleSubpartitionStrategy,
)


def _dialect(version=(23, 1, 0)) -> OracleDialect:
    return OracleDialect(version=version)


# ---------------------------------------------------------------------------
# INTERVAL (sub-phase B)
# ---------------------------------------------------------------------------


class TestOracleIntervalPartition:
    """Oracle INTERVAL partitioning (11g+) expression construction."""

    def test_interval_basic(self):
        d = _dialect()
        c = OracleIntervalPartitionClause(
            d, [Column(d, "created_at")],
            interval=RawSQLExpression(d, "NUMTOYMINTERVAL(1, 'YEAR')"),
            partitions=[
                OraclePartitionDefinition(name="p0", less_than=[OraclePartitionValue(d, "2026-01-01")]),
            ],
        )
        sql, params = c.to_sql()
        assert sql == (
            " PARTITION BY RANGE (created_at) "
            "INTERVAL (NUMTOYMINTERVAL(1, 'YEAR')) "
            "(PARTITION P0 VALUES LESS THAN ('2026-01-01'))"
        )
        assert params == ()

    def test_interval_requires_single_key_column(self):
        d = _dialect()
        with pytest.raises(ValueError, match="exactly one"):
            OracleIntervalPartitionClause(
                d, [Column(d, "a"), Column(d, "b")],
                interval=RawSQLExpression(d, "x"),
                partitions=[OraclePartitionDefinition(name="p0", less_than=[OraclePartitionValue(d, 1)])],
            )

    def test_interval_requires_seed_partition(self):
        d = _dialect()
        with pytest.raises(ValueError, match="seed partition"):
            OracleIntervalPartitionClause(
                d, [Column(d, "created_at")],
                interval=RawSQLExpression(d, "x"),
                partitions=[],
            )

    def test_interval_rejects_parameterized_expression(self):
        """INTERVAL expression must not produce bind parameters (Oracle DDL)."""
        d = _dialect()
        c = OracleIntervalPartitionClause(
            d, [Column(d, "created_at")],
            interval=Literal(d, "x"),  # Literal -> parameterized
            partitions=[OraclePartitionDefinition(name="p0", less_than=[OraclePartitionValue(d, 1)])],
        )
        with pytest.raises(ValueError, match="bind parameters"):
            c.to_sql()

    def test_interval_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        c = OracleIntervalPartitionClause(
            d, [Column(d, "created_at")],
            interval=RawSQLExpression(d, "x"),
            partitions=[OraclePartitionDefinition(name="p0", less_than=[OraclePartitionValue(d, 1)])],
        )
        with pytest.raises(UnsupportedFeatureError):
            c.to_sql()

    def test_interval_capability_gating(self):
        # INTERVAL partitioning baseline is Oracle 11g (11.1.0).
        assert _dialect((11, 1, 0)).supports_interval_partitioning() is True
        assert _dialect((11, 0, 0)).supports_interval_partitioning() is False
        assert _dialect((10, 9, 0)).supports_interval_partitioning() is False

    def test_interval_must_be_baseexpression(self):
        d = _dialect()
        with pytest.raises(TypeError):
            OracleIntervalPartitionClause(
                d, [Column(d, "created_at")],
                interval="not an expression",  # type: ignore[arg-type]
                partitions=[OraclePartitionDefinition(name="p0", less_than=[OraclePartitionValue(d, 1)])],
            )


# ---------------------------------------------------------------------------
# REFERENCE (sub-phase C)
# ---------------------------------------------------------------------------


class TestOracleReferencePartition:
    """Oracle REFERENCE partitioning (11g+) expression construction."""

    def test_reference_basic(self):
        d = _dialect()
        r = OracleReferencePartitionClause(d, "fk_orders_customer")
        sql, params = r.to_sql()
        assert sql == " PARTITION BY REFERENCE (FK_ORDERS_CUSTOMER)"
        assert params == ()

    def test_reference_identifier_quoted(self):
        d = _dialect()
        r = OracleReferencePartitionClause(d, "my_fk")
        sql, _ = r.to_sql()
        assert "MY_FK" in sql

    def test_reference_empty_constraint_raises(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OracleReferencePartitionClause(d, "")

    def test_reference_non_string_constraint_raises(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OracleReferencePartitionClause(d, None)  # type: ignore[arg-type]

    def test_reference_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        r = OracleReferencePartitionClause(d, "fk_x")
        with pytest.raises(UnsupportedFeatureError):
            r.to_sql()

    def test_reference_capability_gating(self):
        # REFERENCE partitioning baseline is Oracle 11g (11.1.0).
        assert _dialect((11, 1, 0)).supports_reference_partitioning() is True
        assert _dialect((11, 0, 0)).supports_reference_partitioning() is False
        assert _dialect((10, 9, 0)).supports_reference_partitioning() is False

    def test_reference_has_no_keys(self):
        """REFERENCE partitioning has no partition key columns."""
        d = _dialect()
        r = OracleReferencePartitionClause(d, "fk_x")
        assert r.keys == []


# ---------------------------------------------------------------------------
# COMPOSITE / subpartitioning (sub-phase D)
# ---------------------------------------------------------------------------


class TestOracleCompositePartition:
    """Oracle composite partitioning (subpartitioning) expression construction."""

    def test_range_hash_composite(self):
        d = _dialect()
        sub = OracleSubpartitionClause(
            d, OracleSubpartitionStrategy.HASH, keys=[Column(d, "id")], count=2
        )
        c = OraclePartitionByRange(
            d, [Column(d, "created_at")],
            subpartition_by=sub,
            partitions=[
                OraclePartitionDefinition(
                    name="p1",
                    less_than=[OraclePartitionValue(d, "2026-01-01")],
                ),
                OraclePartitionDefinition(
                    name="p2",
                    less_than=[OraclePartitionMaxValue(d)],
                ),
            ],
        )
        sql, params = c.to_sql()
        assert "PARTITION BY RANGE (created_at)" in sql
        assert "SUBPARTITION BY HASH (id) SUBPARTITIONS 2" in sql
        assert "PARTITION P1 VALUES LESS THAN ('2026-01-01')" in sql
        assert "PARTITION P2 VALUES LESS THAN (MAXVALUE)" in sql
        assert params == ()

    def test_range_hash_composite_with_explicit_subpartitions(self):
        d = _dialect()
        sub = OracleSubpartitionClause(
            d, OracleSubpartitionStrategy.HASH, keys=[Column(d, "id")], count=2
        )
        c = OraclePartitionByRange(
            d, [Column(d, "created_at")],
            subpartition_by=sub,
            partitions=[
                OraclePartitionDefinition(
                    name="p1",
                    less_than=[OraclePartitionValue(d, "2026-01-01")],
                    subpartition_definitions=[
                        OracleSubpartitionDefinition(name="p1_sub1"),
                        OracleSubpartitionDefinition(name="p1_sub2"),
                    ],
                ),
                OraclePartitionDefinition(
                    name="p2",
                    less_than=[OraclePartitionValue(d, "2027-01-01")],
                ),
            ],
        )
        sql, _ = c.to_sql()
        assert "(SUBPARTITION P1_SUB1, SUBPARTITION P1_SUB2)" in sql

    def test_list_hash_composite(self):
        d = _dialect()
        sub = OracleSubpartitionClause(
            d, OracleSubpartitionStrategy.HASH, keys=[Column(d, "id")], count=3
        )
        c = OraclePartitionByList(
            d, [Column(d, "region")],
            subpartition_by=sub,
            partitions=[
                OraclePartitionDefinition(
                    name="p_east",
                    in_values=[OraclePartitionValue(d, "EAST")],
                ),
            ],
        )
        sql, _ = c.to_sql()
        assert "PARTITION BY LIST (region)" in sql
        assert "SUBPARTITION BY HASH (id) SUBPARTITIONS 3" in sql

    def test_subpartition_by_list_template(self):
        d = _dialect()
        sub = OracleSubpartitionClause(
            d, OracleSubpartitionStrategy.LIST, keys=[Column(d, "region")]
        )
        sql, _ = sub.to_sql()
        assert sql == " SUBPARTITION BY LIST (region)"

    def test_subpartition_by_range_template(self):
        d = _dialect()
        sub = OracleSubpartitionClause(
            d, OracleSubpartitionStrategy.RANGE, keys=[Column(d, "id")]
        )
        sql, _ = sub.to_sql()
        assert sql == " SUBPARTITION BY RANGE (id)"

    def test_subpartition_count_must_be_positive(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OracleSubpartitionClause(
                d, OracleSubpartitionStrategy.HASH, keys=[Column(d, "id")], count=0
            )

    def test_subpartition_count_rejects_bool(self):
        d = _dialect()
        with pytest.raises(TypeError):
            OracleSubpartitionClause(
                d, OracleSubpartitionStrategy.HASH, keys=[Column(d, "id")], count=True
            )

    def test_subpartition_strategy_must_be_enum(self):
        d = _dialect()
        with pytest.raises(TypeError):
            OracleSubpartitionClause(d, "HASH", keys=[Column(d, "id")])  # type: ignore[arg-type]

    def test_subpartition_definition_with_less_than(self):
        d = _dialect()
        sub_def = OracleSubpartitionDefinition(
            name="sub1", less_than=[OraclePartitionValue(d, 100)]
        )
        sql, _ = d.format_subpartition_definition(sub_def)
        assert sql == "SUBPARTITION SUB1 VALUES LESS THAN (100)"

    def test_subpartition_definition_with_in_values(self):
        d = _dialect()
        sub_def = OracleSubpartitionDefinition(
            name="sub1", in_values=[OraclePartitionValue(d, "EAST")]
        )
        sql, _ = d.format_subpartition_definition(sub_def)
        assert sql == "SUBPARTITION SUB1 VALUES ('EAST')"

    def test_subpartition_definition_name_required(self):
        with pytest.raises(ValueError):
            OracleSubpartitionDefinition(name="")

    def test_subpartitioning_capability_gating(self):
        assert _dialect((11, 0, 0)).supports_subpartitioning() is True
        assert _dialect((10, 0, 0)).supports_subpartitioning() is False

    def test_subpartitioning_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        sub = OracleSubpartitionClause(
            d, OracleSubpartitionStrategy.HASH, keys=[Column(d, "id")], count=2
        )
        with pytest.raises(UnsupportedFeatureError):
            sub.to_sql()

    def test_hash_composite_with_subpartition(self):
        """HASH top-level with HASH subpartitioning (HASH-HASH composite)."""
        d = _dialect()
        sub = OracleSubpartitionClause(
            d, OracleSubpartitionStrategy.HASH, keys=[Column(d, "region")], count=2
        )
        c = OraclePartitionByHash(
            d, [Column(d, "id")], partitions_count=4, subpartition_by=sub
        )
        sql, _ = c.to_sql()
        assert "PARTITION BY HASH (id)" in sql
        assert "SUBPARTITION BY HASH (region) SUBPARTITIONS 2" in sql
        assert sql.endswith("PARTITIONS 4")

    def test_subpartition_by_rejects_non_clause(self):
        """subpartition_by must be an OracleSubpartitionClause."""
        d = _dialect()
        with pytest.raises(TypeError):
            OraclePartitionByRange(
                d, [Column(d, "id")],
                subpartition_by="not a clause",  # type: ignore[arg-type]
            )
