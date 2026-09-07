# src/rhosocial/activerecord/backend/impl/oracle/mixins/ddl_spec.py
"""Oracle ``build_spec`` implementation (DDL feature-spec claiming).

Composed into ``OracleDialect``. Claims the Oracle-specific Specs defined in
``..ddl_spec`` via ``isinstance`` and translates them into the Oracle
partition / sequence expression layer. All other Specs fall through to the
generic ``DDLSpecBuildingMixin`` base translation.
"""

from typing import Any, Optional

from rhosocial.activerecord.backend.dialect.mixins.ddl_spec import DDLSpecBuildingMixin
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.statements.ddl_spec import (
    DDLSpec,
    PartitionSpec,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    ColumnConstraint,
    ColumnConstraintType,
)

from ..ddl_spec import (
    OracleHashPartition,
    OracleIntervalPartition,
    OracleListPartition,
    OraclePartitionBound,
    OraclePartitionDefinitionSpec,
    OracleRangePartition,
    OracleSequenceDefault,
)
from ..expression.partition import (
    OracleIntervalPartitionClause,
    OraclePartitionByHash,
    OraclePartitionByList,
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
)
from ..expression.sequence import (
    OracleSequenceValueExpression,
)


def _bound_expression(dialect, bound: "OraclePartitionBound"):
    """Translate a plain boundary to the Oracle partition value expression."""
    if bound.value == "MAXVALUE":
        return OraclePartitionMaxValue(dialect)
    return OraclePartitionValue(dialect, bound.value)


def _definition_expression(dialect, spec: "OraclePartitionDefinitionSpec"):
    """Translate a plain partition definition to ``OraclePartitionDefinition``."""
    less_than = None
    in_values = None
    if spec.less_than is not None:
        less_than = [_bound_expression(dialect, b) for b in spec.less_than]
    elif spec.in_values is not None:
        in_values = [_bound_expression(dialect, b) for b in spec.in_values]
    return OraclePartitionDefinition(
        name=spec.name,
        less_than=less_than,
        in_values=in_values,
    )


class OracleDDLSpecMixin(DDLSpecBuildingMixin):
    """Oracle-specific ``build_spec`` claiming and translation."""

    def build_spec(self, spec: "DDLSpec") -> Optional[Any]:
        """Claim Oracle Specs; otherwise defer to the generic build."""
        if isinstance(spec, OracleRangePartition):
            return self._build_oracle_range_partition(spec)
        if isinstance(spec, OracleListPartition):
            return self._build_oracle_list_partition(spec)
        if isinstance(spec, OracleHashPartition):
            return self._build_oracle_hash_partition(spec)
        if isinstance(spec, OracleIntervalPartition):
            return self._build_oracle_interval_partition(spec)
        if isinstance(spec, OracleSequenceDefault):
            return self._build_oracle_sequence_default(spec)
        return super().build_spec(spec)

    def _build_oracle_range_partition(self, spec: "OracleRangePartition"):
        return OraclePartitionByRange(
            self,
            keys=[Column(self, spec.column)],
            partitions=(
                [_definition_expression(self, d) for d in spec.partitions]
                if spec.partitions else None
            ),
        )

    def _build_oracle_list_partition(self, spec: "OracleListPartition"):
        return OraclePartitionByList(
            self,
            keys=[Column(self, spec.column)],
            partitions=(
                [_definition_expression(self, d) for d in spec.partitions]
                if spec.partitions else None
            ),
        )

    def _build_oracle_hash_partition(self, spec: "OracleHashPartition"):
        return OraclePartitionByHash(
            self,
            keys=[Column(self, spec.column)],
            partitions_count=spec.partitions_count,
        )

    def _build_oracle_interval_partition(self, spec: "OracleIntervalPartition"):
        """INTERVAL partitioning requires a seed partition; synthesize a
        ``p_first`` seed when none given.

        The interval is a ready ``BaseExpression`` (e.g.
        ``OracleIntervalFunctionExpression``) or a lazy
        ``(dialect) -> BaseExpression`` factory, resolved here.
        """
        from ..expression.partition import OraclePartitionDefinition as _Def

        interval = self._resolve_predicate(spec.interval)
        partitions = [_Def(name="p_first", less_than=[OraclePartitionMaxValue(self)])]
        return OracleIntervalPartitionClause(
            self,
            keys=[Column(self, spec.column)],
            interval=interval,
            partitions=partitions,
        )

    def _build_oracle_sequence_default(self, spec: "OracleSequenceDefault"):
        """Translate a sequence default Spec to a DEFAULT column constraint."""
        sequence = spec.sequence or f"{spec.column}_seq"
        value = OracleSequenceValueExpression(self, sequence)
        return ColumnConstraint(
            constraint_type=ColumnConstraintType.DEFAULT,
            name=None,
            default_value=value,
        )

    def _build_partition_spec(self, spec: "PartitionSpec"):
        """Unclaimed partition Specs silently return ``None``."""
        return None