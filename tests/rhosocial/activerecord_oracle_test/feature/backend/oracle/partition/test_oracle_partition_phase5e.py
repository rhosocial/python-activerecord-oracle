# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/partition/test_oracle_partition_phase5e.py
"""Phase 5 sub-phase E tests: partition maintenance statements.

Verifies expression construction (SQL shape + parameter order) and
capability gating for the Oracle partition maintenance expressions
(ADD / DROP / SPLIT / MERGE / EXCHANGE / MOVE / TRUNCATE PARTITION).

Real Oracle execution is covered in ``test_oracle_partition_phase5_real.py``.
"""

import pytest

from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
)
from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
    OracleAddPartitionExpression,
    OracleDropPartitionExpression,
    OracleExchangePartitionExpression,
    OracleMergePartitionsExpression,
    OracleMovePartitionExpression,
    OracleSplitPartitionExpression,
    OracleTruncatePartitionExpression,
)


def _dialect(version=(23, 1, 0)) -> OracleDialect:
    return OracleDialect(version=version)


# ---------------------------------------------------------------------------
# ADD PARTITION
# ---------------------------------------------------------------------------


class TestOracleAddPartition:
    def test_add_range_partition(self):
        d = _dialect()
        e = OracleAddPartitionExpression(
            d, "orders",
            OraclePartitionDefinition(name="p3", less_than=[OraclePartitionValue(d, 200)]),
        )
        sql, params = e.to_sql()
        assert sql == "ALTER TABLE ORDERS ADD PARTITION P3 VALUES LESS THAN (200)"
        assert params == ()

    def test_add_list_partition(self):
        d = _dialect()
        e = OracleAddPartitionExpression(
            d, "orders",
            OraclePartitionDefinition(name="p_south", in_values=[OraclePartitionValue(d, "SOUTH")]),
        )
        sql, _ = e.to_sql()
        assert sql == "ALTER TABLE ORDERS ADD PARTITION P_SOUTH VALUES ('SOUTH')"

    def test_add_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        e = OracleAddPartitionExpression(
            d, "orders",
            OraclePartitionDefinition(name="p3", less_than=[OraclePartitionValue(d, 200)]),
        )
        with pytest.raises(UnsupportedFeatureError):
            e.to_sql()

    def test_add_rejects_non_definition(self):
        d = _dialect()
        with pytest.raises(TypeError):
            OracleAddPartitionExpression(d, "orders", "not a definition")  # type: ignore[arg-type]

    def test_add_rejects_empty_table(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OracleAddPartitionExpression(
                d, "",
                OraclePartitionDefinition(name="p3", less_than=[OraclePartitionValue(d, 200)]),
            )


# ---------------------------------------------------------------------------
# DROP PARTITION
# ---------------------------------------------------------------------------


class TestOracleDropPartition:
    def test_drop_basic(self):
        d = _dialect()
        e = OracleDropPartitionExpression(d, "orders", "p2")
        sql, params = e.to_sql()
        assert sql == "ALTER TABLE ORDERS DROP PARTITION P2"
        assert params == ()

    def test_drop_with_update_indexes(self):
        d = _dialect()
        e = OracleDropPartitionExpression(d, "orders", "p2", update_indexes=True)
        sql, _ = e.to_sql()
        assert sql == "ALTER TABLE ORDERS DROP PARTITION P2 UPDATE INDEXES"

    def test_drop_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        e = OracleDropPartitionExpression(d, "orders", "p2")
        with pytest.raises(UnsupportedFeatureError):
            e.to_sql()

    def test_drop_rejects_empty_partition_name(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OracleDropPartitionExpression(d, "orders", "")


# ---------------------------------------------------------------------------
# SPLIT PARTITION (Oracle-specific)
# ---------------------------------------------------------------------------


class TestOracleSplitPartition:
    def test_split_basic(self):
        d = _dialect()
        e = OracleSplitPartitionExpression(
            d, "orders", "p_max",
            at_values=[OraclePartitionValue(d, 200)],
            new_partitions=[
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionValue(d, 200)]),
                OraclePartitionDefinition(name="p_max", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        sql, params = e.to_sql()
        assert sql == (
            "ALTER TABLE ORDERS SPLIT PARTITION P_MAX AT (200) INTO "
            "(PARTITION P2, PARTITION P_MAX)"
        )
        assert params == ()

    def test_split_requires_two_new_partitions(self):
        d = _dialect()
        with pytest.raises(ValueError, match="exactly 2"):
            OracleSplitPartitionExpression(
                d, "orders", "p_max",
                at_values=[OraclePartitionValue(d, 200)],
                new_partitions=[
                    OraclePartitionDefinition(name="p2", less_than=[OraclePartitionValue(d, 200)]),
                ],
            )

    def test_split_requires_at_values(self):
        d = _dialect()
        with pytest.raises(ValueError, match="at_values"):
            OracleSplitPartitionExpression(
                d, "orders", "p_max",
                at_values=[],
                new_partitions=[
                    OraclePartitionDefinition(name="p2", less_than=[OraclePartitionValue(d, 200)]),
                    OraclePartitionDefinition(name="p3", less_than=[OraclePartitionMaxValue(d)]),
                ],
            )

    def test_split_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        e = OracleSplitPartitionExpression(
            d, "orders", "p_max",
            at_values=[OraclePartitionValue(d, 200)],
            new_partitions=[
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionValue(d, 200)]),
                OraclePartitionDefinition(name="p3", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        with pytest.raises(UnsupportedFeatureError):
            e.to_sql()


# ---------------------------------------------------------------------------
# MERGE PARTITIONS
# ---------------------------------------------------------------------------


class TestOracleMergePartitions:
    def test_merge_basic(self):
        d = _dialect()
        e = OracleMergePartitionsExpression(
            d, "orders", ["p1", "p2"],
            OraclePartitionDefinition(name="p12", less_than=[OraclePartitionValue(d, 200)]),
        )
        sql, params = e.to_sql()
        assert sql == (
            "ALTER TABLE ORDERS MERGE PARTITIONS P1, P2 INTO "
            "PARTITION P12"
        )
        assert params == ()

    def test_merge_requires_two_partition_names(self):
        d = _dialect()
        with pytest.raises(ValueError, match="exactly 2"):
            OracleMergePartitionsExpression(
                d, "orders", ["p1"],
                OraclePartitionDefinition(name="p12", less_than=[OraclePartitionValue(d, 200)]),
            )

    def test_merge_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        e = OracleMergePartitionsExpression(
            d, "orders", ["p1", "p2"],
            OraclePartitionDefinition(name="p12", less_than=[OraclePartitionValue(d, 200)]),
        )
        with pytest.raises(UnsupportedFeatureError):
            e.to_sql()


# ---------------------------------------------------------------------------
# EXCHANGE PARTITION
# ---------------------------------------------------------------------------


class TestOracleExchangePartition:
    def test_exchange_basic(self):
        d = _dialect()
        e = OracleExchangePartitionExpression(d, "orders", "p1", "orders_p1_staging")
        sql, params = e.to_sql()
        assert sql == (
            "ALTER TABLE ORDERS EXCHANGE PARTITION P1 WITH TABLE "
            "ORDERS_P1_STAGING WITH VALIDATION"
        )
        assert params == ()

    def test_exchange_with_options(self):
        d = _dialect()
        e = OracleExchangePartitionExpression(
            d, "orders", "p1", "orders_p1_staging",
            including_indexes=True, with_validation=False,
        )
        sql, _ = e.to_sql()
        assert sql == (
            "ALTER TABLE ORDERS EXCHANGE PARTITION P1 WITH TABLE "
            "ORDERS_P1_STAGING INCLUDING INDEXES WITHOUT VALIDATION"
        )

    def test_exchange_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        e = OracleExchangePartitionExpression(d, "orders", "p1", "staging")
        with pytest.raises(UnsupportedFeatureError):
            e.to_sql()

    def test_exchange_rejects_empty_with_table(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OracleExchangePartitionExpression(d, "orders", "p1", "")


# ---------------------------------------------------------------------------
# MOVE PARTITION (Oracle-specific)
# ---------------------------------------------------------------------------


class TestOracleMovePartition:
    def test_move_basic(self):
        d = _dialect()
        e = OracleMovePartitionExpression(d, "orders", "p1")
        sql, params = e.to_sql()
        assert sql == "ALTER TABLE ORDERS MOVE PARTITION P1"
        assert params == ()

    def test_move_with_tablespace(self):
        d = _dialect()
        e = OracleMovePartitionExpression(d, "orders", "p1", tablespace_name="users")
        sql, _ = e.to_sql()
        assert sql == "ALTER TABLE ORDERS MOVE PARTITION P1 TABLESPACE USERS"

    def test_move_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        e = OracleMovePartitionExpression(d, "orders", "p1")
        with pytest.raises(UnsupportedFeatureError):
            e.to_sql()


# ---------------------------------------------------------------------------
# TRUNCATE PARTITION
# ---------------------------------------------------------------------------


class TestOracleTruncatePartition:
    def test_truncate_basic(self):
        d = _dialect()
        e = OracleTruncatePartitionExpression(d, "orders", "p1")
        sql, params = e.to_sql()
        assert sql == "ALTER TABLE ORDERS TRUNCATE PARTITION P1"
        assert params == ()

    def test_truncate_with_update_indexes(self):
        d = _dialect()
        e = OracleTruncatePartitionExpression(d, "orders", "p1", update_indexes=True)
        sql, _ = e.to_sql()
        assert sql == "ALTER TABLE ORDERS TRUNCATE PARTITION P1 UPDATE INDEXES"

    def test_truncate_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        e = OracleTruncatePartitionExpression(d, "orders", "p1")
        with pytest.raises(UnsupportedFeatureError):
            e.to_sql()

    def test_truncate_rejects_empty_partition_name(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OracleTruncatePartitionExpression(d, "orders", "")


# ---------------------------------------------------------------------------
# Capability gating summary
# ---------------------------------------------------------------------------


class TestOracleMaintenanceCapabilityGating:
    """All Oracle maintenance statements are gated for 11g+."""

    @pytest.mark.parametrize("method", [
        "supports_add_partition",
        "supports_drop_partition",
        "supports_truncate_partition",
        "supports_reorganize_partition",
        "supports_split_partition",
        "supports_merge_partition",
        "supports_exchange_partition",
        "supports_move_partition",
    ])
    def test_supported_for_11g(self, method):
        d = _dialect((11, 0, 0))
        assert getattr(d, method)() is True

    @pytest.mark.parametrize("method", [
        "supports_add_partition",
        "supports_drop_partition",
        "supports_truncate_partition",
        "supports_reorganize_partition",
        "supports_split_partition",
        "supports_merge_partition",
        "supports_exchange_partition",
        "supports_move_partition",
    ])
    def test_unsupported_pre_11g(self, method):
        d = OracleDialect(version=(10, 0, 0))
        assert getattr(d, method)() is False

    def test_attach_detach_not_applicable(self):
        d = _dialect()
        assert d.supports_attach_partition() is False
        assert d.supports_detach_partition() is False
