# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_partition_phase4.py
"""Phase 4 tests for Oracle table partitioning (RANGE/LIST/HASH).

Originally written against the Phase 4 generic ``PartitionClause`` +
``dialect_options`` path; migrated to the structured backend-specific
``OraclePartitionByRange`` / ``...ByList`` / ``...ByHash`` API with
``OraclePartitionDefinition`` boundary definitions. The untyped
``dialect_options`` bag is no longer read by Oracle.

Covers:

* Expression construction: ``to_sql()`` produces the expected Oracle SQL
  shape and parameter order for RANGE / LIST / HASH.
* Protocol conformance: ``OracleDialect`` satisfies ``PartitionSupport``
  and ``OraclePartitionSupport``.
* Capability gating: ``supports_*`` methods reflect Oracle version.
* ``format_create_table_statement`` appends the PARTITION BY clause.
* Error paths: unsupported version, invalid method, malformed definitions.
* Rejection of the generic core ``PartitionClause`` expression.
"""

import pytest

from rhosocial.activerecord.backend.dialect.protocols import PartitionSupport
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.statements import (
    PartitionClause,
    PartitionStrategy,
)
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionByHash,
    OraclePartitionByList,
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
)
from rhosocial.activerecord.backend.impl.oracle.protocols.partition import (
    OraclePartitionSupport,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dialect(version=(19, 0, 0)) -> OracleDialect:
    return OracleDialect(version=version)


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------

class TestOraclePartitionProtocolConformance:
    """Verify OracleDialect satisfies the partition protocols."""

    def test_dialect_is_partition_support(self):
        d = _dialect()
        assert isinstance(d, PartitionSupport)

    def test_dialect_is_oracle_partition_support(self):
        d = _dialect()
        assert isinstance(d, OraclePartitionSupport)

    @pytest.mark.parametrize("method", [
        "supports_table_partitioning",
        "supports_partitioned_table_creation",
        "supports_partition_metadata_introspection",
        "supports_range_table_partitioning",
        "supports_list_table_partitioning",
        "supports_hash_table_partitioning",
        "supports_subpartitioning",
        "supports_add_partition",
        "supports_drop_partition",
        "supports_truncate_partition",
        "supports_reorganize_partition",
        "supports_attach_partition",
        "supports_detach_partition",
        "format_partition_clause",
    ])
    def test_protocol_method_present(self, method):
        d = _dialect()
        assert hasattr(d, method), f"Missing PartitionSupport method: {method}"


# ---------------------------------------------------------------------------
# Capability gating
# ---------------------------------------------------------------------------

class TestOraclePartitionCapabilityGating:
    """supports_* methods must reflect Oracle version."""

    @pytest.mark.parametrize("version", [(11, 0, 0), (12, 0, 0), (18, 0, 0), (19, 0, 0), (21, 0, 0), (23, 0, 0)])
    def test_supported_versions(self, version):
        d = OracleDialect(version=version)
        assert d.supports_table_partitioning() is True
        assert d.supports_partitioned_table_creation() is True
        assert d.supports_range_table_partitioning() is True
        assert d.supports_list_table_partitioning() is True
        assert d.supports_hash_table_partitioning() is True

    def test_pre_11g_not_supported(self):
        d = OracleDialect(version=(10, 0, 0))
        assert d.supports_table_partitioning() is False
        assert d.supports_partitioned_table_creation() is False
        assert d.supports_range_table_partitioning() is False
        assert d.supports_list_table_partitioning() is False
        assert d.supports_hash_table_partitioning() is False

    def test_phase5_maintenance_operations_supported(self):
        """Phase 5 implements partition maintenance statements for 11g+."""
        d = _dialect()
        # Composite partitioning is supported for 11g+.
        assert d.supports_subpartitioning() is True
        # Maintenance statements are supported for 11g+.
        assert d.supports_add_partition() is True
        assert d.supports_drop_partition() is True
        assert d.supports_truncate_partition() is True
        assert d.supports_reorganize_partition() is True
        # Oracle uses EXCHANGE instead of ATTACH/DETACH.
        assert d.supports_attach_partition() is False
        assert d.supports_detach_partition() is False
        # Oracle-specific maintenance statements.
        assert d.supports_split_partition() is True
        assert d.supports_merge_partition() is True
        assert d.supports_exchange_partition() is True
        assert d.supports_move_partition() is True

    def test_phase5_oracle_strategies_supported(self):
        """Phase 5 adds INTERVAL and REFERENCE partitioning for 11g+."""
        d = _dialect()
        assert d.supports_interval_partitioning() is True
        assert d.supports_reference_partitioning() is True

    def test_pre_11g_format_partition_raises_unsupported(self):
        d = OracleDialect(version=(10, 0, 0))
        clause = OraclePartitionByHash(d, [Column(d, "id")], partitions_count=2)
        with pytest.raises(UnsupportedFeatureError):
            clause.to_sql()


# ---------------------------------------------------------------------------
# Expression construction: RANGE
# ---------------------------------------------------------------------------

class TestOracleRangePartitionClause:
    """RANGE PARTITION BY clause SQL generation."""

    def test_range_with_literal_and_maxvalue(self):
        d = _dialect()
        clause = OraclePartitionByRange(
            d, [Column(d, "age")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 18)]),
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        sql, params = clause.to_sql()
        assert sql == (
            " PARTITION BY RANGE (\"AGE\") "
            "(PARTITION \"P1\" VALUES LESS THAN (18), "
            "PARTITION \"P2\" VALUES LESS THAN (MAXVALUE))"
        )
        # Oracle DDL does not accept bind variables; values are inlined.
        assert params == ()

    def test_range_multi_column(self):
        d = _dialect()
        clause = OraclePartitionByRange(
            d, [Column(d, "a"), Column(d, "b")],
            partitions=[
                OraclePartitionDefinition(
                    name="p1",
                    less_than=[OraclePartitionValue(d, 1), OraclePartitionValue(d, 100)],
                ),
                OraclePartitionDefinition(
                    name="p2",
                    less_than=[OraclePartitionMaxValue(d), OraclePartitionMaxValue(d)],
                ),
            ],
        )
        sql, params = clause.to_sql()
        assert sql.startswith(' PARTITION BY RANGE ("A", "B") ')
        assert 'PARTITION "P1" VALUES LESS THAN (1, 100)' in sql
        assert 'PARTITION "P2" VALUES LESS THAN (MAXVALUE, MAXVALUE)' in sql
        assert params == ()

    def test_range_string_boundary_escaped(self):
        """String boundary values are escaped and quoted, not bound."""
        d = _dialect()
        clause = OraclePartitionByRange(
            d, [Column(d, "code")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, "M")]),
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        sql, params = clause.to_sql()
        assert "VALUES LESS THAN ('M')" in sql
        assert params == ()

    def test_range_no_partitions(self):
        """RANGE without explicit partition definitions is valid Oracle syntax."""
        d = _dialect()
        clause = OraclePartitionByRange(d, [Column(d, "age")])
        sql, params = clause.to_sql()
        assert sql == ' PARTITION BY RANGE ("AGE")'
        assert params == ()

    def test_range_missing_less_than_raises(self):
        d = _dialect()
        clause = OraclePartitionByRange(
            d, [Column(d, "age")],
            partitions=[OraclePartitionDefinition(name="p1")],
        )
        with pytest.raises(ValueError, match="less_than"):
            clause.to_sql()

    def test_range_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        clause = OraclePartitionByRange(
            d, [Column(d, "age")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        with pytest.raises(UnsupportedFeatureError, match="table partitioning"):
            clause.to_sql()


# ---------------------------------------------------------------------------
# Expression construction: LIST
# ---------------------------------------------------------------------------

class TestOracleListPartitionClause:
    """LIST PARTITION BY clause SQL generation."""

    def test_list_single_column(self):
        d = _dialect()
        clause = OraclePartitionByList(
            d, [Column(d, "region")],
            partitions=[
                OraclePartitionDefinition(
                    name="p_east",
                    in_values=[OraclePartitionValue(d, "EAST"), OraclePartitionValue(d, "NORTH")],
                ),
                OraclePartitionDefinition(name="p_west", in_values=[OraclePartitionValue(d, "WEST")]),
            ],
        )
        sql, params = clause.to_sql()
        assert sql == (
            " PARTITION BY LIST (\"REGION\") "
            "(PARTITION \"P_EAST\" VALUES ('EAST', 'NORTH'), "
            "PARTITION \"P_WEST\" VALUES ('WEST'))"
        )
        assert params == ()

    def test_list_no_partitions(self):
        d = _dialect()
        clause = OraclePartitionByList(d, [Column(d, "region")])
        sql, params = clause.to_sql()
        assert sql == ' PARTITION BY LIST ("REGION")'
        assert params == ()

    def test_list_missing_in_values_raises(self):
        d = _dialect()
        clause = OraclePartitionByList(
            d, [Column(d, "region")],
            partitions=[OraclePartitionDefinition(name="p1")],
        )
        with pytest.raises(ValueError, match="in_values"):
            clause.to_sql()


# ---------------------------------------------------------------------------
# Expression construction: HASH
# ---------------------------------------------------------------------------

class TestOracleHashPartitionClause:
    """HASH PARTITION BY clause SQL generation."""

    def test_hash_with_partitions_count(self):
        d = _dialect()
        clause = OraclePartitionByHash(d, [Column(d, "id")], partitions_count=4)
        sql, params = clause.to_sql()
        assert sql == ' PARTITION BY HASH ("ID") PARTITIONS 4'
        assert params == ()

    def test_hash_multi_column(self):
        d = _dialect()
        clause = OraclePartitionByHash(d, [Column(d, "a"), Column(d, "b")], partitions_count=8)
        sql, params = clause.to_sql()
        assert sql == ' PARTITION BY HASH ("A", "B") PARTITIONS 8'
        assert params == ()

    def test_hash_without_count(self):
        """HASH without partitions_count omits the PARTITIONS clause."""
        d = _dialect()
        clause = OraclePartitionByHash(d, [Column(d, "id")])
        sql, params = clause.to_sql()
        assert sql == ' PARTITION BY HASH ("ID")'
        assert params == ()

    def test_hash_invalid_count_raises(self):
        d = _dialect()
        with pytest.raises(ValueError, match="positive integer"):
            OraclePartitionByHash(d, [Column(d, "id")], partitions_count=0)

    def test_hash_negative_count_raises(self):
        d = _dialect()
        with pytest.raises(ValueError, match="positive integer"):
            OraclePartitionByHash(d, [Column(d, "id")], partitions_count=-3)

    def test_hash_bool_count_raises(self):
        d = _dialect()
        with pytest.raises(TypeError, match="partitions_count"):
            OraclePartitionByHash(d, [Column(d, "id")], partitions_count=True)


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

class TestOraclePartitionClauseErrors:
    """Error path coverage for the structured expressions."""

    def test_generic_partition_clause_rejected(self):
        """The generic core PartitionClause (dialect_options bag) is rejected."""
        d = _dialect()
        clause = PartitionClause(d, PartitionStrategy.HASH, [Column(d, "id")])
        with pytest.raises(TypeError, match="Oracle-specific partition clause"):
            clause.to_sql()

    def test_range_partition_definition_wrong_type(self):
        d = _dialect()
        clause = OraclePartitionByRange(
            d, [Column(d, "age")], partitions=["not-a-definition"],
        )
        with pytest.raises(TypeError, match="OraclePartitionDefinition"):
            clause.to_sql()

    def test_list_partition_definition_wrong_type(self):
        d = _dialect()
        clause = OraclePartitionByList(
            d, [Column(d, "region")], partitions=[42],
        )
        with pytest.raises(TypeError, match="OraclePartitionDefinition"):
            clause.to_sql()

    def test_boundary_value_wrong_type(self):
        d = _dialect()
        clause = OraclePartitionByRange(
            d, [Column(d, "age")],
            partitions=[OraclePartitionDefinition(name="p1", less_than=[123])],
        )
        with pytest.raises(TypeError, match="OraclePartitionValue"):
            clause.to_sql()

    def test_less_than_wrong_container(self):
        d = _dialect()
        clause = OraclePartitionByRange(
            d, [Column(d, "age")],
            partitions=[OraclePartitionDefinition(name="p1", less_than="MAXVALUE")],
        )
        with pytest.raises(TypeError, match="list or tuple"):
            clause.to_sql()
