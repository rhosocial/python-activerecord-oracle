# src/rhosocial/activerecord/backend/impl/oracle/dialect.py
"""Oracle backend SQL dialect implementation.

Assembles the full Oracle dialect surface by composing generic mixins
(provided by the core framework) together with Oracle-specific mixins
that override version-gated capability checks and syntax formatters.
"""
from typing import Any, Optional, Tuple, TYPE_CHECKING

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
    UserDefinedTypeSupport,
)
from rhosocial.activerecord.backend.dialect.mixins import (
    SQLXMLMixin,
    CollationMixin,
    CTEMixin,

    WindowFunctionMixin,
    JSONMixin,

    ArrayMixin,
    ExplainMixin,
    GraphMixin,
    GraphTableMixin,

    MergeMixin,

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
    PredicateMixin,
    ExpressionMixin,
    DateTimeMixin,
    DQLMixin,
    DMLMixin,
    PartitionMixin,
    UserDefinedTypeMixin,
)
from .mixins import (
    OracleAnalyzeMixin,
    OracleCollationMixin,
    OracleCommentMixin,
    OracleDatabaseLinkMixin,
    OracleDateTimeMixin,
    OracleDDLMixin,
    OracleDMLOperationMixin,
    OracleExpressionMixin,
    OracleFeaturesMixin,
    OracleFlashbackMixin,
    OracleFunctionFormatMixin,
    OracleHierarchicalMixin,
    OracleHintMixin,
    OracleIndexMixin,
    OracleIntrospectionMixin,
    OracleJSONFunctionMixin,
    OracleLockingMixin,
    OracleModifyColumnMixin,
    OracleMaterializedViewMixin,
    OracleOptimizerHintMixin,
    OraclePaginationMixin,
    OraclePartitionMixin,
    OraclePartitionLifecycleMixin,
    OraclePivotMixin,
    OracleRoutineMixin,
    OracleSchemaMixin,
    OracleSequenceMixin,
    OracleSetOperationMixin,
    OracleSpatialMixin,
    OracleSynonymMixin,
    OracleTableMixin,
    OracleTableCapabilityMixin,
    OracleTransactionMixin,
    OracleTriggerMixin,
    OracleTruncateMixin,
    OracleTypeSupportMixin,
    OracleTypeSuggestionMixin,
    OracleTypeDDLMixin,
    OracleVectorMixin,
    OracleViewMixin,
    OracleIdentifierMixin,
    OracleExplainMixin,
)
from .protocols.partition import OraclePartitionSupport
from .protocols.ddl_type import OracleTypeDDLSupport
from .reserved_words import ORACLE_RESERVED_WORDS

if TYPE_CHECKING:
    pass


class OracleDialect(
    SQLDialectBase,
    # ================================================================
    # Oracle-specific overrides – listed BEFORE the generic mixins they
    # override so that MRO resolves to the Oracle version first.
    # ================================================================
    OracleAnalyzeMixin,
    OracleCollationMixin,
    OracleDDLMixin,
    OracleTypeDDLMixin,
    UserDefinedTypeMixin,
    OracleCommentMixin,
    OracleDatabaseLinkMixin,
    OracleDateTimeMixin,
    OracleDMLOperationMixin,
    OracleExpressionMixin,
    OracleFeaturesMixin,
    OracleFlashbackMixin,
    OracleFunctionFormatMixin,
    OracleHierarchicalMixin,
    OracleHintMixin,
    OracleIndexMixin,
    OracleIntrospectionMixin,
    OracleJSONFunctionMixin,
    OracleLockingMixin,
    OracleModifyColumnMixin,
    OracleMaterializedViewMixin,
    OracleOptimizerHintMixin,
    OraclePaginationMixin,
    OraclePartitionMixin,
    OraclePartitionLifecycleMixin,
    OraclePivotMixin,
    OracleRoutineMixin,
    OracleSchemaMixin,
    OracleSequenceMixin,
    OracleSetOperationMixin,
    OracleSpatialMixin,
    OracleSynonymMixin,
    OracleTableMixin,
    OracleTableCapabilityMixin,
    OracleTransactionMixin,
    OracleTriggerMixin,
    OracleTruncateMixin,
    OracleTypeSupportMixin,
    OracleTypeSuggestionMixin,
    OracleVectorMixin,
    OracleViewMixin,
    OracleIdentifierMixin,
    OracleExplainMixin,
    # ================================================================
    # Generic fallback mixins – defaults that Oracle-specific mixins
    # above can override.
    # ================================================================
    SQLXMLMixin,
    CollationMixin,
    CTEMixin,

    WindowFunctionMixin,
    JSONMixin,

    ArrayMixin,
    ExplainMixin,
    GraphMixin,

    MergeMixin,

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
    PredicateMixin,
    ExpressionMixin,
    DateTimeMixin,
    DQLMixin,
    DMLMixin,
    PartitionMixin,
    GraphTableMixin,
    # ================================================================
    # Protocols (type-annotation guarantee only)
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
    OracleTypeDDLSupport,
    UserDefinedTypeSupport,
):
    """Oracle dialect implementation that adapts to the Oracle version.

    All SQL generation and capability-check logic lives in dedicated
    mixin classes (see ``.mixins`` package). This class is a thin
    composition skeleton that wires them together.
    """

    def __init__(
        self,
        version: Optional[Tuple[int, int, int]] = None,
        *,
        version_full: Optional[Tuple[int, ...]] = None,
        ru_version: Optional[int] = None,
    ):
        """Initialise the dialect with an optional version tuple.

        Args:
            version: ``(major, minor, patch)`` tuple, e.g. ``(23, 4, 0)``.
                When ``None``, the version must be obtained later via
                :meth:`backend.introspect_and_adapt`.
        """
        super().__init__()
        self._reserved_words = ORACLE_RESERVED_WORDS
        self._version_full = self._normalize_version_full(version_full)
        self._ru_version = self._normalize_ru_version(ru_version)
        if version is not None:
            self.version = version

    @staticmethod
    def _normalize_version_full(
        value: Optional[Tuple[int, ...]],
    ) -> Optional[Tuple[int, ...]]:
        if value is None:
            return None
        if isinstance(value, str):
            value = tuple(
                int(part)
                for part in value.strip().split(".")
                if part.strip().isdigit()
            )
        if not isinstance(value, (tuple, list)):
            raise TypeError("version_full must be a tuple or string")
        result = tuple(int(part) for part in value)
        return result or None

    @staticmethod
    def _normalize_ru_version(value: Optional[int]) -> Optional[int]:
        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError("ru_version must be an int")
        return value

    @property
    def version_full(self) -> Optional[Tuple[int, ...]]:
        return self._version_full

    @version_full.setter
    def version_full(self, value: Optional[Tuple[int, ...]]) -> None:
        self._version_full = self._normalize_version_full(value)

    @property
    def ru_version(self) -> Optional[int]:
        if self._ru_version is not None:
            return self._ru_version
        if self._version_full and len(self._version_full) >= 2:
            return self._version_full[1]
        return None

    @ru_version.setter
    def ru_version(self, value: Optional[int]) -> None:
        self._ru_version = self._normalize_ru_version(value)

    def format_identifier(self, identifier: str, need_quote: bool = True) -> str:
        """Format identifier for Oracle with double-quote quoting and uppercasing."""
        if not need_quote:
            if self.is_reserved_word(identifier):
                import warnings
                from rhosocial.activerecord.backend.warnings import IdentifierQuotingWarning
                warnings.warn(
                    f"Identifier '{identifier}' is a reserved word in {self.name} "
                    f"and may cause SQL errors without quoting.",
                    IdentifierQuotingWarning,
                    stacklevel=2,
                )
            return identifier
        escaped = identifier.replace('"', '""')
        return f'"{escaped.upper()}"'

    def get_parameter_placeholder(self, position: int = 0) -> str:
        """Return the positional placeholder ``?``.

        OracleBackend.execute() renumbers ``?`` placeholders to
        ``:1, :2, ...`` at execution time.
        """
        return "?"

    def get_server_version(self) -> Tuple[int, int, int]:
        """Return the configured Oracle version tuple."""
        return self.version

    def create_schema_differ(self) -> Any:
        """Return the Oracle schema differ for this dialect."""
        from rhosocial.activerecord.backend.impl.oracle.schema.differ import (
            OracleSchemaDiffer,
        )

        return OracleSchemaDiffer()
