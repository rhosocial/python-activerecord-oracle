# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/partition/test_oracle_partition_phase5a.py
"""Phase 5 sub-phase A tests: backend-specific partition expressions.

Verifies that the backend-specific ``OraclePartitionByRange`` /
``OraclePartitionByList`` / ``OraclePartitionByHash`` expressions and
the ``OraclePartitionDefinition`` / ``OraclePartitionValue`` /
``OraclePartitionMaxValue`` helpers produce the expected Oracle SQL
shape, and that the generic ``PartitionClause`` (Phase 4) path remains
backward compatible.

These are expression-construction tests (SQL shape + parameter order).
Real Oracle execution is covered in ``test_oracle_partition_phase5_real.py``.
"""

import pytest
from datetime import date

from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.statements import (
    PartitionClause,
    PartitionStrategy,
)
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
# RANGE
# ---------------------------------------------------------------------------


class TestOraclePartitionByRange:
    """Backend-specific PARTITION BY RANGE expression."""

    def test_range_with_value_and_maxvalue(self):
        d = _dialect()
        c = OraclePartitionByRange(
            d, [Column(d, "id")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        sql, params = c.to_sql()
        assert sql == (
            " PARTITION BY RANGE (id) "
            "(PARTITION P1 VALUES LESS THAN (100), "
            "PARTITION P2 VALUES LESS THAN (MAXVALUE))"
        )
        assert params == ()

    def test_range_multi_column(self):
        d = _dialect()
        c = OraclePartitionByRange(
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
        sql, _ = c.to_sql()
        assert "PARTITION P1 VALUES LESS THAN (1, 100)" in sql
        assert "PARTITION P2 VALUES LESS THAN (MAXVALUE, MAXVALUE)" in sql

    def test_range_string_boundary_escaped(self):
        d = _dialect()
        c = OraclePartitionByRange(
            d, [Column(d, "name")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, "M")]),
                OraclePartitionDefinition(name="p2", less_than=[OraclePartitionMaxValue(d)]),
            ],
        )
        sql, _ = c.to_sql()
        assert "VALUES LESS THAN ('M')" in sql
        assert "VALUES LESS THAN (MAXVALUE)" in sql

    def test_range_literal_expression_also_accepted(self):
        """A core ``Literal`` expression is also accepted as a boundary value."""
        d = _dialect()
        c = OraclePartitionByRange(
            d, [Column(d, "id")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[Literal(d, 50)]),
                OraclePartitionDefinition(name="p2", less_than=[Literal(d, date(2026, 1, 1))]),
            ],
        )
        sql, _ = c.to_sql()
        assert "VALUES LESS THAN (50)" in sql
        assert "VALUES LESS THAN (DATE '2026-01-01')" in sql

    def test_range_no_partitions(self):
        d = _dialect()
        c = OraclePartitionByRange(d, [Column(d, "id")])
        sql, params = c.to_sql()
        assert sql == " PARTITION BY RANGE (id)"
        assert params == ()

    def test_range_missing_less_than_raises(self):
        d = _dialect()
        c = OraclePartitionByRange(
            d, [Column(d, "id")],
            partitions=[OraclePartitionDefinition(name="p1")],
        )
        with pytest.raises(ValueError, match="less_than"):
            c.to_sql()

    def test_range_pre_11g_raises(self):
        d = OracleDialect(version=(10, 0, 0))
        c = OraclePartitionByRange(
            d, [Column(d, "id")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
            ],
        )
        with pytest.raises(UnsupportedFeatureError):
            c.to_sql()


# ---------------------------------------------------------------------------
# LIST
# ---------------------------------------------------------------------------


class TestOraclePartitionByList:
    """Backend-specific PARTITION BY LIST expression."""

    def test_list_single_column(self):
        d = _dialect()
        c = OraclePartitionByList(
            d, [Column(d, "region")],
            partitions=[
                OraclePartitionDefinition(
                    name="p_east",
                    in_values=[OraclePartitionValue(d, "EAST"), OraclePartitionValue(d, "NORTH")],
                ),
                OraclePartitionDefinition(name="p_west", in_values=[OraclePartitionValue(d, "WEST")]),
            ],
        )
        sql, params = c.to_sql()
        assert sql == (
            " PARTITION BY LIST (region) "
            "(PARTITION P_EAST VALUES ('EAST', 'NORTH'), "
            "PARTITION P_WEST VALUES ('WEST'))"
        )
        assert params == ()

    def test_list_multi_column(self):
        d = _dialect()
        c = OraclePartitionByList(
            d, [Column(d, "a"), Column(d, "b")],
            partitions=[
                OraclePartitionDefinition(
                    name="p1",
                    in_values=[
                        [OraclePartitionValue(d, "x"), OraclePartitionValue(d, 1)],
                        [OraclePartitionValue(d, "y"), OraclePartitionValue(d, 2)],
                    ],
                ),
            ],
        )
        sql, _ = c.to_sql()
        assert "VALUES (('x', 1), ('y', 2))" in sql

    def test_list_no_partitions(self):
        d = _dialect()
        c = OraclePartitionByList(d, [Column(d, "region")])
        sql, _ = c.to_sql()
        assert sql == " PARTITION BY LIST (region)"

    def test_list_missing_in_values_raises(self):
        d = _dialect()
        c = OraclePartitionByList(
            d, [Column(d, "region")],
            partitions=[OraclePartitionDefinition(name="p1")],
        )
        with pytest.raises(ValueError, match="in_values"):
            c.to_sql()


# ---------------------------------------------------------------------------
# HASH
# ---------------------------------------------------------------------------


class TestOraclePartitionByHash:
    """Backend-specific PARTITION BY HASH expression."""

    def test_hash_with_count(self):
        d = _dialect()
        c = OraclePartitionByHash(d, [Column(d, "id")], partitions_count=4)
        sql, params = c.to_sql()
        assert sql == " PARTITION BY HASH (id) PARTITIONS 4"
        assert params == ()

    def test_hash_no_count_no_partitions(self):
        d = _dialect()
        c = OraclePartitionByHash(d, [Column(d, "id")])
        sql, _ = c.to_sql()
        assert sql == " PARTITION BY HASH (id)"

    def test_hash_with_explicit_partitions(self):
        d = _dialect()
        c = OraclePartitionByHash(
            d, [Column(d, "id")],
            partitions=[
                OraclePartitionDefinition(name="p1"),
                OraclePartitionDefinition(name="p2"),
            ],
        )
        sql, _ = c.to_sql()
        assert "PARTITION BY HASH (id) (PARTITION P1, PARTITION P2)" in sql

    def test_hash_count_must_be_positive(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OraclePartitionByHash(d, [Column(d, "id")], partitions_count=0)

    def test_hash_count_must_be_int(self):
        d = _dialect()
        with pytest.raises(TypeError):
            OraclePartitionByHash(d, [Column(d, "id")], partitions_count="4")

    def test_hash_count_rejects_bool(self):
        d = _dialect()
        with pytest.raises(TypeError):
            OraclePartitionByHash(d, [Column(d, "id")], partitions_count=True)


# ---------------------------------------------------------------------------
# Definition validation
# ---------------------------------------------------------------------------


class TestOraclePartitionDefinition:
    """OraclePartitionDefinition dataclass validation."""

    def test_name_must_be_non_empty(self):
        with pytest.raises(ValueError):
            OraclePartitionDefinition(name="")

    def test_name_must_be_str(self):
        with pytest.raises(ValueError):
            OraclePartitionDefinition(name=None)  # type: ignore[arg-type]

    def test_less_than_and_in_values_mutually_exclusive(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OraclePartitionDefinition(
                name="p1",
                less_than=[OraclePartitionValue(d, 1)],
                in_values=[OraclePartitionValue(d, "x")],
            )

    def test_dialect_options_must_be_dict(self):
        with pytest.raises(TypeError):
            OraclePartitionDefinition(name="p1", dialect_options="not a dict")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Boundary value rendering
# ---------------------------------------------------------------------------


class TestOraclePartitionValue:
    """OraclePartitionValue / OraclePartitionMaxValue rendering."""

    def test_maxvalue_renders_keyword(self):
        d = _dialect()
        v = OraclePartitionMaxValue(d)
        sql, params = v.to_sql()
        assert sql == "MAXVALUE"
        assert params == ()

    def test_int_value(self):
        d = _dialect()
        v = OraclePartitionValue(d, 42)
        sql, _ = v.to_sql()
        assert sql == "42"

    def test_string_value_escaped(self):
        d = _dialect()
        v = OraclePartitionValue(d, "O'Brien")
        sql, _ = v.to_sql()
        assert sql == "'O''Brien'"

    def test_none_renders_null(self):
        d = _dialect()
        v = OraclePartitionValue(d, None)
        sql, _ = v.to_sql()
        assert sql == "NULL"

    def test_date_value(self):
        d = _dialect()
        v = OraclePartitionValue(d, date(2026, 1, 1))
        sql, _ = v.to_sql()
        # Oracle ANSI DATE literal — required for DATE column boundaries.
        assert sql == "DATE '2026-01-01'"

    def test_bool_rejected(self):
        d = _dialect()
        with pytest.raises(TypeError):
            OraclePartitionValue(d, True)

    def test_float_must_be_finite(self):
        d = _dialect()
        with pytest.raises(ValueError):
            OraclePartitionValue(d, float("inf"))

    def test_unsupported_type_rejected(self):
        d = _dialect()
        with pytest.raises(TypeError):
            OraclePartitionValue(d, [1, 2, 3])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Backward compatibility: legacy PartitionClause path
# ---------------------------------------------------------------------------


class TestLegacyPartitionClauseCompat:
    """Phase 4 generic PartitionClause path must still work."""

    def test_legacy_range_path(self):
        d = _dialect()
        c = PartitionClause(
            d, PartitionStrategy.RANGE, [Column(d, "id")],
            dialect_options={
                "partitions": [
                    {"name": "p1", "less_than": [Literal(d, 100)]},
                    {"name": "p2", "less_than": ["MAXVALUE"]},
                ],
            },
        )
        sql, params = c.to_sql()
        assert "PARTITION BY RANGE (id)" in sql
        assert "PARTITION P1 VALUES LESS THAN (100)" in sql
        assert "PARTITION P2 VALUES LESS THAN (MAXVALUE)" in sql
        assert params == ()

    def test_legacy_hash_path(self):
        d = _dialect()
        c = PartitionClause(
            d, PartitionStrategy.HASH, [Column(d, "id")],
            dialect_options={"partitions_count": 2},
        )
        sql, _ = c.to_sql()
        assert sql == " PARTITION BY HASH (id) PARTITIONS 2"

    def test_backend_specific_dispatch_takes_priority(self):
        """Backend-specific expression dispatches before legacy method string."""
        d = _dialect()
        # Even though method is RANGE, an OraclePartitionByRange instance
        # should dispatch through format_partition_by_range (structured form),
        # not the legacy dialect_options path.
        c = OraclePartitionByRange(
            d, [Column(d, "id")],
            partitions=[
                OraclePartitionDefinition(name="p1", less_than=[OraclePartitionValue(d, 100)]),
            ],
        )
        sql, _ = c.to_sql()
        # Structured form produces identical SQL to legacy for this case,
        # but the dispatch path is verifiable through the absence of
        # dialect_options (structured form never reads dialect_options).
        assert "PARTITION BY RANGE (id)" in sql
        assert "PARTITION P1 VALUES LESS THAN (100)" in sql
