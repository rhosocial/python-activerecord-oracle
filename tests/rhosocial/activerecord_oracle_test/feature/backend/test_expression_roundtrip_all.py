# tests/rhosocial/activerecord_oracle_test/feature/backend/test_expression_roundtrip_all.py
import pytest

from rhosocial.activerecord.testsuite.utils.expression import (
    collect_expression_classes,
    make_instance,
    register_all,
    register_special_constructor,
    roundtrip_expression,
    sql_consistent,
)

ORACLE_EXPR_PKG = "rhosocial.activerecord.backend.impl.oracle.expression"

CLASSES = collect_expression_classes(ORACLE_EXPR_PKG)
register_all(CLASSES)


def _register_oracle_specials():

    def analyze_expression(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.analyze import (
            OracleAnalyzeMode, OracleAnalyzeExpression,
        )
        return OracleAnalyzeExpression(d, table="t", mode=OracleAnalyzeMode.COMPUTE_STATISTICS)

    def comment_expression(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.comment import (
            OracleCommentObjectType, OracleCommentExpression,
        )
        return OracleCommentExpression(
            d, object_type=OracleCommentObjectType.TABLE, object_name="t", comment="test"
        )

    def as_of_clause(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.flashback import (
            OracleAsOfMode, OracleAsOfClause,
        )
        return OracleAsOfClause(d, mode=OracleAsOfMode.TIMESTAMP, value="SYSTIMESTAMP")

    def versions_between_clause(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.flashback import (
            OracleVersionsBetweenMode, OracleVersionsBetweenClause,
        )
        return OracleVersionsBetweenClause(
            d, mode=OracleVersionsBetweenMode.TIMESTAMP,
            low_value="SYSTIMESTAMP - 1", high_value="SYSTIMESTAMP",
        )

    def flashback_table_expression(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.flashback import (
            OracleFlashbackTableExpression,
        )
        return OracleFlashbackTableExpression(d, table="t", to_before_drop=True, rename_to="t2")

    def purge_expression(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.flashback import (
            OraclePurgeObjectType, OraclePurgeExpression,
        )
        return OraclePurgeExpression(d, object_type=OraclePurgeObjectType.TABLE, object_name="t")

    def materialized_view_log_expression(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.materialized_view import (
            OracleCreateMaterializedViewLogExpression,
        )
        return OracleCreateMaterializedViewLogExpression(d, table="t", with_rowid=True)

    def drop_routine_expression(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.ddl.routine import (
            OracleDropRoutineObjectType, OracleDropRoutineExpression,
        )
        return OracleDropRoutineExpression(
            d, object_type=OracleDropRoutineObjectType.PROCEDURE, object_name="p"
        )

    def subpartition_clause(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
            OracleSubpartitionStrategy, OracleSubpartitionClause,
        )
        return OracleSubpartitionClause(d, strategy=OracleSubpartitionStrategy.HASH, count=4)

    def partition_clause(d):
        from rhosocial.activerecord.backend.expression.core import Column
        from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
            OraclePartitionClause,
        )
        return OraclePartitionClause(d, "RANGE", [Column(d, "id")])

    def partition_by_range(d):
        from rhosocial.activerecord.backend.expression.core import Column
        from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
            OraclePartitionByRange,
        )
        return OraclePartitionByRange(d, [Column(d, "id")])

    def partition_by_list(d):
        from rhosocial.activerecord.backend.expression.core import Column
        from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
            OraclePartitionByList,
        )
        return OraclePartitionByList(d, [Column(d, "id")])

    def partition_by_hash(d):
        from rhosocial.activerecord.backend.expression.core import Column
        from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
            OraclePartitionByHash,
        )
        return OraclePartitionByHash(d, [Column(d, "id")], partitions_count=4)

    def interval_partition_clause(d):
        from rhosocial.activerecord.backend.expression.core import Column, Literal
        from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
            OracleIntervalPartitionClause,
            OraclePartitionDefinition,
            OraclePartitionValue,
        )
        pdef = OraclePartitionDefinition(
            name="p1", less_than=[OraclePartitionValue(d, 100)]
        )
        return OracleIntervalPartitionClause(
            d,
            [Column(d, "created_at")],
            interval=Literal(d, "INTERVAL '1' MONTH"),
            partitions=[pdef],
        )

    def add_partition_expression(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
            OracleAddPartitionExpression,
        )
        from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
            OraclePartitionDefinition, OraclePartitionMaxValue,
        )
        pdef = OraclePartitionDefinition(
            name="p_new", less_than=[OraclePartitionMaxValue(d)]
        )
        return OracleAddPartitionExpression(d, table="t", partition=pdef)

    def split_partition_expression(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
            OracleSplitPartitionExpression,
        )
        from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
            OraclePartitionDefinition, OraclePartitionValue, OraclePartitionMaxValue,
        )
        p1 = OraclePartitionDefinition(
            name="p1", less_than=[OraclePartitionValue(d, 100)]
        )
        p2 = OraclePartitionDefinition(
            name="p2", less_than=[OraclePartitionMaxValue(d)]
        )
        return OracleSplitPartitionExpression(
            d, table="t", partition_name="p_old", at_values=[100],
            new_partitions=[p1, p2],
        )

    def merge_partitions_expression(d):
        from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
            OracleMergePartitionsExpression,
        )
        from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
            OraclePartitionDefinition, OraclePartitionMaxValue,
        )
        into = OraclePartitionDefinition(
            name="p_merged", less_than=[OraclePartitionMaxValue(d)]
        )
        return OracleMergePartitionsExpression(
            d, table="t", partition_names=["p1", "p2"], into_partition=into
        )

    register_special_constructor("analyze.OracleAnalyzeExpression", analyze_expression)
    register_special_constructor("comment.OracleCommentExpression", comment_expression)
    register_special_constructor("flashback.OracleAsOfClause", as_of_clause)
    register_special_constructor("flashback.OracleVersionsBetweenClause", versions_between_clause)
    register_special_constructor("flashback.OracleFlashbackTableExpression", flashback_table_expression)
    register_special_constructor("flashback.OraclePurgeExpression", purge_expression)
    register_special_constructor("materialized_view.OracleCreateMaterializedViewLogExpression", materialized_view_log_expression)
    register_special_constructor("ddl.routine.OracleDropRoutineExpression", drop_routine_expression)
    register_special_constructor("partition.OracleSubpartitionClause", subpartition_clause)
    register_special_constructor("partition.OraclePartitionClause", partition_clause)
    register_special_constructor("partition.OraclePartitionByRange", partition_by_range)
    register_special_constructor("partition.OraclePartitionByList", partition_by_list)
    register_special_constructor("partition.OraclePartitionByHash", partition_by_hash)
    register_special_constructor(
        "partition.OracleIntervalPartitionClause", interval_partition_clause
    )
    register_special_constructor("partition_lifecycle.OracleAddPartitionExpression", add_partition_expression)
    register_special_constructor("partition_lifecycle.OracleSplitPartitionExpression", split_partition_expression)
    register_special_constructor("partition_lifecycle.OracleMergePartitionsExpression", merge_partitions_expression)


_register_oracle_specials()


@pytest.fixture(scope="function")
def oracle_dialect():
    from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
    dialect = OracleDialect()
    dialect.version = (19, 0, 0)
    return dialect


@pytest.fixture(params=[fqn for fqn in sorted(CLASSES)], ids=sorted(CLASSES))
def oracle_expr_case(request, oracle_dialect):
    fqn = request.param
    cls = CLASSES[fqn]
    instance, source = make_instance(cls, oracle_dialect)
    if instance is None:
        pytest.skip(f"{fqn}: {source}")
    return fqn, instance


class TestOracleExpressionRoundtrip:
    """All constructible Oracle expression classes round-trip losslessly."""

    def test_get_params_roundtrip(self, oracle_expr_case, oracle_dialect):
        fqn, instance = oracle_expr_case
        roundtrip_expression(fqn, instance, oracle_dialect)

    def test_to_sql_consistent(self, oracle_expr_case, oracle_dialect):
        fqn, instance = oracle_expr_case
        sql_consistent(fqn, instance, oracle_dialect)