# tests/rhosocial/activerecord_oracle_test/feature/backend/ddl/test_ddl_spec_protocol.py
"""Oracle DDL feature-spec claiming tests (``build_spec``).

Covers the Oracle-specific Specs (RANGE / LIST / HASH / INTERVAL partitions,
sequence defaults) and the generic Specs on the Oracle dialect:

- Oracle partition Specs translate to the Oracle partition expression layer
  with correct inline DDL (no bind parameters).
- ``OracleSequenceDefault`` translates to ``DEFAULT seq.NEXTVAL``.
- Generic Specs still translate via the core ``DDLSpecBuildingMixin``.
- Foreign Specs (``PartitionSpec`` marker, unknown objects) return ``None``.
"""

import pytest

from rhosocial.activerecord.base import (
    CheckSpec,
    PartitionSpec,
    PrimaryKeySpec,
    UniqueSpec,
)
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.ddl_spec import (
    OracleHashPartition,
    OracleIntervalPartition,
    OracleListPartition,
    OraclePartitionBound,
    OraclePartitionDefinitionSpec,
    OracleRangePartition,
    OracleSequenceDefault,
)


@pytest.fixture
def dialect():
    d = OracleDialect()
    d.version = (19, 0, 0)
    return d


class TestProtocolConformance:
    def test_build_spec_returns_none_for_unknown(self, dialect):
        assert dialect.build_spec(object()) is None

    def test_build_spec_returns_none_for_base_partition_marker(self, dialect):
        assert dialect.build_spec(PartitionSpec()) is None


class TestGenericSpecTranslation:
    def test_unique_spec(self, dialect):
        result = dialect.build_spec(UniqueSpec(["a", "b"], name="uq_ab"))
        assert result.columns == ["a", "b"]

    def test_check_spec_lazy(self, dialect):
        result = dialect.build_spec(
            CheckSpec(lambda d: Column(d, "age") >= 18, name="ck_age")
        )
        assert result.check_condition is not None

    def test_primary_key_single(self, dialect):
        result = dialect.build_spec(PrimaryKeySpec(["id"]))
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnConstraint,
            ColumnConstraintType,
        )
        assert isinstance(result, ColumnConstraint)
        assert result.constraint_type == ColumnConstraintType.PRIMARY_KEY


class TestOraclePartitionSpecs:
    def test_range_partition(self, dialect):
        spec = OracleRangePartition(
            "created_at",
            [
                OraclePartitionDefinitionSpec(
                    "p2026", less_than=[OraclePartitionBound(2027)]
                ),
                OraclePartitionDefinitionSpec(
                    "p_max", less_than=[OraclePartitionBound("MAXVALUE")]
                ),
            ],
        )
        expr = dialect.build_spec(spec)
        assert type(expr).__name__ == "OraclePartitionByRange"
        sql, params = expr.to_sql()
        assert "PARTITION BY RANGE" in sql
        assert "VALUES LESS THAN (2027)" in sql
        assert "MAXVALUE" in sql
        assert params == ()  # Oracle DDL takes no bind variables

    def test_list_partition(self, dialect):
        spec = OracleListPartition(
            "region",
            [OraclePartitionDefinitionSpec("p_east", in_values=[OraclePartitionBound("EAST")])],
        )
        expr = dialect.build_spec(spec)
        assert type(expr).__name__ == "OraclePartitionByList"
        sql, _ = expr.to_sql()
        assert "PARTITION BY LIST" in sql
        assert "VALUES ('EAST')" in sql

    def test_hash_partition(self, dialect):
        expr = dialect.build_spec(OracleHashPartition("id", partitions_count=4))
        assert type(expr).__name__ == "OraclePartitionByHash"
        sql, _ = expr.to_sql()
        assert "PARTITION BY HASH" in sql
        assert "PARTITIONS 4" in sql

    def test_interval_partition_monthly(self, dialect):
        expr = dialect.build_spec(OracleIntervalPartition.monthly("created_at"))
        assert type(expr).__name__ == "OracleIntervalPartitionClause"
        sql, params = expr.to_sql()
        assert "INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))" in sql
        assert params == ()  # inline literals, no binds

    def test_interval_partition_yearly(self, dialect):
        expr = dialect.build_spec(OracleIntervalPartition.yearly("created_at", 2))
        sql, _ = expr.to_sql()
        assert "NUMTOYMINTERVAL(2, 'YEAR')" in sql

    def test_interval_partition_daily(self, dialect):
        expr = dialect.build_spec(OracleIntervalPartition.daily("ts"))
        sql, _ = expr.to_sql()
        assert "NUMTODSINTERVAL(1, 'DAY')" in sql

    def test_interval_partition_rejects_missing_interval(self, dialect):
        with pytest.raises(ValueError):
            dialect.build_spec(OracleIntervalPartition("created_at", None))


class TestOracleSequenceDefault:
    def test_sequence_default(self, dialect):
        result = dialect.build_spec(OracleSequenceDefault("id", "users_id_seq"))
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnConstraint,
            ColumnConstraintType,
        )
        assert isinstance(result, ColumnConstraint)
        assert result.constraint_type == ColumnConstraintType.DEFAULT
        sql, _ = result.default_value.to_sql()
        assert sql == "USERS_ID_SEQ.NEXTVAL"


class TestBuildSpecFeedsCreateTable:
    def test_partition_spec_feeds_create_table(self, dialect):
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnDefinition,
            CreateTableExpression,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType

        part = dialect.build_spec(
            OracleRangePartition(
                "created_at",
                [
                    OraclePartitionDefinitionSpec(
                        "p2026", less_than=[OraclePartitionBound(2027)]
                    )
                ],
            )
        )
        pk = dialect.build_spec(PrimaryKeySpec(["id"]))
        cols = [
            ColumnDefinition("id", IntegerType(), constraints=[pk]),
            ColumnDefinition("created_at", IntegerType()),
        ]
        expr = CreateTableExpression(
            dialect=dialect,
            table="orders",
            columns=cols,
            partition=part,
        )
        sql, _ = expr.to_sql()
        assert "PARTITION BY RANGE" in sql


class TestModelIntegration:
    def test_model_spec_constraints(self, dialect):
        from rhosocial.activerecord.model import ActiveRecord

        class T(ActiveRecord):
            __table_name__ = "t"
            __table_constraints__ = [
                UniqueSpec(columns=["a", "b"], name="uq_ab"),
            ]
            a: int
            b: int

        expr = T.generate_create_table(dialect)
        sql, _ = expr.to_sql()
        assert "UNIQUE" in sql

    def test_model_partition_spec(self, dialect):
        from rhosocial.activerecord.model import ActiveRecord

        class Orders(ActiveRecord):
            __table_name__ = "orders"
            __table_partition__ = [
                OracleRangePartition(
                    "created_at",
                    [
                        OraclePartitionDefinitionSpec(
                            "p2026", less_than=[OraclePartitionBound(2027)]
                        )
                    ],
                ),
            ]
            created_at: int

        expr = Orders.generate_create_table(dialect)
        assert expr.partition is not None
        sql, _ = expr.to_sql()
        assert "PARTITION BY RANGE" in sql

    def test_model_unclaimed_partition_is_ignored(self):
        from rhosocial.activerecord.backend.impl.sqlite.dialect import SQLiteDialect
        from rhosocial.activerecord.model import ActiveRecord

        class Orders(ActiveRecord):
            __table_name__ = "orders"
            __table_partition__ = [
                OracleRangePartition(
                    "created_at",
                    [
                        OraclePartitionDefinitionSpec(
                            "p2026", less_than=[OraclePartitionBound(2027)]
                        )
                    ],
                ),
            ]
            created_at: int

        # The same model on SQLite: Oracle partition spec unclaimed → plain table.
        expr = Orders.generate_create_table(SQLiteDialect())
        assert expr.partition is None