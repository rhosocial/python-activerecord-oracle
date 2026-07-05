# src/rhosocial/activerecord/backend/impl/oracle/dialect.py
"""Oracle backend SQL dialect implementation.

Assembles the full Oracle dialect surface by composing generic mixins
(provided by the core framework) together with Oracle-specific mixins
that override version-gated capability checks and syntax formatters.
"""
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
from rhosocial.activerecord.backend.dialect.protocols import (
    SQLXMLSupport,
    SQLXMLParsingSupport,
    SQLXMLSerializationSupport,
    SQLXMLConstructionSupport,
    SQLXMLAggregationSupport,
    SQLXMLQueryingSupport,
    CollationSupport,
    CTESupport,
    FilterClauseSupport,
    WindowFunctionSupport,
    JSONSupport,
    ReturningSupport,
    AdvancedGroupingSupport,
    ArraySupport,
    ExplainSupport,
    GraphSupport,
    GraphTableSupport,
    LockingSupport,
    MergeSupport,
    OrderedSetAggregationSupport,
    QualifyClauseSupport,
    TemporalTableSupport,
    UpsertSupport,
    LateralJoinSupport,
    WildcardSupport,
    JoinSupport,
    ViewSupport,
    SchemaSupport,
    IndexSupport,
    SequenceSupport,
    TableSupport,
    IntrospectionSupport,
    SetOperationSupport,
    TruncateSupport,
    ConstraintSupport,
    TransactionControlSupport,
    SQLFunctionSupport,
)
from rhosocial.activerecord.backend.dialect.mixins import (
    SQLXMLMixin,
    CollationMixin,
    CTEMixin,
    FilterClauseMixin,
    WindowFunctionMixin,
    JSONMixin,
    ReturningMixin,
    AdvancedGroupingMixin,
    ArrayMixin,
    ExplainMixin,
    GraphMixin,
    GraphTableMixin,
    LockingMixin,
    MergeMixin,
    OrderedSetAggregationMixin,
    QualifyClauseMixin,
    TemporalTableMixin,
    UpsertMixin,
    LateralJoinMixin,
    JoinMixin,
    SetOperationMixin,
    TruncateMixin,
    ViewMixin,
    SchemaMixin,
    IndexMixin,
    SequenceMixin,
    TableMixin,
    ConstraintMixin,
    IntrospectionMixin,
    DDLColumnMixin,
    IdentifierMixin,
    PredicateMixin,
    ExpressionMixin,
    DateTimeMixin,
    DQLMixin,
    DMLMixin,
    PartitionMixin,
    GraphTableMixin,
)
from .mixins import (
    OracleCollationMixin,
    OracleDateTimeMixin,
    OracleDDLMixin,
    OracleDMLOperationMixin,
    OracleExpressionMixin,
    OracleFeaturesMixin,
    OracleFunctionFormatMixin,
    OracleHierarchicalMixin,
    OracleHintMixin,
    OracleIdentifierMixin,
    OracleIndexMixin,
    OracleJSONFunctionMixin,
    OracleLockingMixin,
    OracleModifyColumnMixin,
    OracleOptimizerHintMixin,
    OraclePaginationMixin,
    OraclePartitionMixin,
    OraclePartitionLifecycleMixin,
    OraclePivotMixin,
    OracleSchemaMixin,
    OracleSequenceMixin,
    OracleSetOperationMixin,
    OracleSpatialMixin,
    OracleTableMixin,
    OracleTableCapabilityMixin,
    OracleTransactionMixin,
    OracleTriggerMixin,
    OracleTruncateMixin,
    OracleTypeSupportMixin,
    OracleVectorMixin,
    OracleViewMixin,
)
from .protocols.partition import OraclePartitionSupport

if TYPE_CHECKING:
    pass


class OracleDialect(
    SQLDialectBase,
    # ================================================================
    # Oracle-specific overrides – listed BEFORE the generic mixins they
    # override so that MRO resolves to the Oracle version first.
    # ================================================================
    OracleCollationMixin,
    OracleDDLMixin,
    OracleDateTimeMixin,
    OracleDMLOperationMixin,
    OracleExpressionMixin,
    OracleFeaturesMixin,
    OracleFunctionFormatMixin,
    OracleHierarchicalMixin,
    OracleHintMixin,
    OracleIdentifierMixin,
    OracleIndexMixin,
    OracleJSONFunctionMixin,
    OracleLockingMixin,
    OracleModifyColumnMixin,
    OracleOptimizerHintMixin,
    OraclePaginationMixin,
    OraclePartitionMixin,
    OraclePartitionLifecycleMixin,
    OraclePivotMixin,
    OracleSchemaMixin,
    OracleSequenceMixin,
    OracleSetOperationMixin,
    OracleSpatialMixin,
    OracleTableMixin,
    OracleTableCapabilityMixin,
    OracleTransactionMixin,
    OracleTriggerMixin,
    OracleTruncateMixin,
    OracleTypeSupportMixin,
    OracleVectorMixin,
    OracleViewMixin,
    # ================================================================
    # Generic fallback mixins – defaults that Oracle-specific mixins
    # above can override.
    # ================================================================
    SQLXMLMixin,
    CollationMixin,
    CTEMixin,
    FilterClauseMixin,
    WindowFunctionMixin,
    JSONMixin,
    ReturningMixin,
    AdvancedGroupingMixin,
    ArrayMixin,
    ExplainMixin,
    GraphMixin,
    LockingMixin,
    MergeMixin,
    OrderedSetAggregationMixin,
    QualifyClauseMixin,
    TemporalTableMixin,
    UpsertMixin,
    LateralJoinMixin,
    JoinMixin,
    SetOperationMixin,
    TruncateMixin,
    ViewMixin,
    SchemaMixin,
    IndexMixin,
    SequenceMixin,
    TableMixin,
    ConstraintMixin,
    IntrospectionMixin,
    DDLColumnMixin,
    IdentifierMixin,
    PredicateMixin,
    ExpressionMixin,
    DateTimeMixin,
    DQLMixin,
    DMLMixin,
    PartitionMixin,
    GraphTableMixin,
    # ================================================================
    # Protocols (type-annotation guarentee only)
    # ================================================================
    SQLXMLSupport,
    SQLXMLParsingSupport,
    SQLXMLSerializationSupport,
    SQLXMLConstructionSupport,
    SQLXMLAggregationSupport,
    SQLXMLQueryingSupport,
    CollationSupport,
    CTESupport,
    FilterClauseSupport,
    WindowFunctionSupport,
    JSONSupport,
    ReturningSupport,
    AdvancedGroupingSupport,
    ArraySupport,
    ExplainSupport,
    GraphSupport,
    GraphTableSupport,
    LockingSupport,
    MergeSupport,
    OrderedSetAggregationSupport,
    QualifyClauseSupport,
    TemporalTableSupport,
    UpsertSupport,
    LateralJoinSupport,
    WildcardSupport,
    JoinSupport,
    SetOperationSupport,
    TruncateSupport,
    ViewSupport,
    SchemaSupport,
    IndexSupport,
    SequenceSupport,
    TableSupport,
    ConstraintSupport,
    IntrospectionSupport,
    TransactionControlSupport,
    SQLFunctionSupport,
    OraclePartitionSupport,
):
    """Oracle dialect implementation that adapts to the Oracle version.

    All SQL generation and capability-check logic lives in dedicated
    mixin classes (see ``.mixins`` package). This class is a thin
    composition skeleton that wires them together.
    """

    def __init__(self, version: Optional[Tuple[int, int, int]] = None):
        """Initialise the dialect with an optional version tuple.

        Args:
            version: ``(major, minor, patch)`` tuple, e.g. ``(23, 4, 0)``.
                When ``None``, the version must be obtained later via
                :meth:`backend.introspect_and_adapt`.
        """
        super().__init__()
        if version is not None:
            self.version = version

    def get_parameter_placeholder(self, position: int = 0) -> str:
        """Return the positional placeholder ``?``.

        OracleBackend.execute() renumbers ``?`` placeholders to
        ``:1, :2, ...`` at execution time.
        """
        return "?"

    def get_server_version(self) -> Tuple[int, int, int]:
        """Return the configured Oracle version tuple."""
        return self.version

    def format_identifier(self, identifier: str) -> str:
        """Format identifier for Oracle (uppercase, no quoting)."""
        return identifier.upper()
