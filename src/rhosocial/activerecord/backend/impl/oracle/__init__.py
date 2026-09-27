# src/rhosocial/activerecord/backend/impl/oracle/__init__.py
"""Oracle backend implementation for the Python ORM.

This module provides:
- Oracle synchronous backend with connection management and query execution
- Oracle asynchronous backend with async/await support (via oracledb thin mode)
- Oracle-specific connection configuration
- Type mapping and value conversion
- Transaction management with savepoint support (sync and async)
- Oracle dialect and expression handling

Architecture:
- OracleBackend: Synchronous implementation using oracledb
- AsyncOracleBackend: Asynchronous implementation using oracledb (thin mode)
- Independent from ORM frameworks - uses only native drivers

Subpackages:
- explain: EXPLAIN result types
- types: Oracle-specific type definitions
- expression: Oracle-specific SQL expressions (including DDL DataType subclasses)
- functions: Oracle function factories
- mixins: Feature-specific mixin classes
- schema: Schema differ support"""

# Public names are resolved on first access rather than at package-execution
# time. Importing this package used to import every submodule eagerly, which
# pulled in the DB driver through ``.backend`` and made the expression classes
# unreachable without it: Python executes a parent package's ``__init__`` before
# any submodule, so ``impl.<name>.expression`` could not be imported on its own.
# The exports themselves are unchanged -- attribute access, ``__all__``, ``dir()``
# and ``from ... import *`` all behave as before.
from typing import TYPE_CHECKING

#: Public name -> (submodule that defines it, attribute name within it).
#: The two differ when the original import was aliased.
_EXPORTS = {
    "AsyncOracleBackend": (".async_backend", "AsyncOracleBackend"),
    "AsyncOracleTransactionManager": (".async_transaction", "AsyncOracleTransactionManager"),
    "DIRECT_COMPATIBLE_CASTS": (".type_compatibility", "DIRECT_COMPATIBLE_CASTS"),
    "DropTypeBodyExpression": (".expression.ddl.type", "DropTypeBodyExpression"),
    "OracleAlterTypeAddAttributeAction": (".expression.ddl.type", "OracleAlterTypeAddAttributeAction"),
    "OracleAlterTypeAddMethodAction": (".expression.ddl.type", "OracleAlterTypeAddMethodAction"),
    "OracleAlterTypeAttributeAction": (".expression.ddl.type", "OracleAlterTypeAttributeAction"),
    "OracleAlterTypeCompileAction": (".expression.ddl.type", "OracleAlterTypeCompileAction"),
    "OracleAlterTypeDependentHandling": (".expression.ddl.type", "OracleAlterTypeDependentHandling"),
    "OracleAlterTypeDropAttributeAction": (".expression.ddl.type", "OracleAlterTypeDropAttributeAction"),
    "OracleAlterTypeDropMethodAction": (".expression.ddl.type", "OracleAlterTypeDropMethodAction"),
    "OracleAlterTypeElementTypeAction": (".expression.ddl.type", "OracleAlterTypeElementTypeAction"),
    "OracleAlterTypeFinalAction": (".expression.ddl.type", "OracleAlterTypeFinalAction"),
    "OracleAlterTypeInstantiableAction": (".expression.ddl.type", "OracleAlterTypeInstantiableAction"),
    "OracleAlterTypeLimitAction": (".expression.ddl.type", "OracleAlterTypeLimitAction"),
    "OracleAlterTypeMethodAction": (".expression.ddl.type", "OracleAlterTypeMethodAction"),
    "OracleAlterTypeModifyAttributeAction": (".expression.ddl.type", "OracleAlterTypeModifyAttributeAction"),
    "OracleAlterTypeModifyLimitAction": (".expression.ddl.type", "OracleAlterTypeModifyLimitAction"),
    "OracleAlterTypeResetAction": (".expression.ddl.type", "OracleAlterTypeResetAction"),
    "OracleBackend": (".backend", "OracleBackend"),
    "OracleBackendMixin": (".mixins", "OracleBackendMixin"),
    "OracleBigIntType": (".expression.types", "OracleBigIntType"),
    "OracleBlobType": (".expression.types", "OracleBlobType"),
    "OracleBooleanAdapter": (".adapters", "OracleBooleanAdapter"),
    "OracleBytesAdapter": (".adapters", "OracleBytesAdapter"),
    "OracleCharType": (".expression.types", "OracleCharType"),
    "OracleClobType": (".expression.types", "OracleClobType"),
    "OracleCollation": (".collation", "OracleCollation"),
    "OracleCompileTypeAction": (".expression.ddl.type", "OracleCompileTypeAction"),
    "OracleConcurrencyMixin": (".mixins", "OracleConcurrencyMixin"),
    "OracleConnectionConfig": (".config", "OracleConnectionConfig"),
    "OracleCreateTypeBodyExpression": (".expression.ddl.type", "OracleCreateTypeBodyExpression"),
    "OracleDateAdapter": (".adapters", "OracleDateAdapter"),
    "OracleDateTimeAdapter": (".adapters", "OracleDateTimeAdapter"),
    "OracleDecimalAdapter": (".adapters", "OracleDecimalAdapter"),
    "OracleDialect": (".dialect", "OracleDialect"),
    "OracleDropTypeBodyExpression": (".expression.ddl.type", "OracleDropTypeBodyExpression"),
    "OracleDropTypeExpression": (".expression.ddl.type", "OracleDropTypeExpression"),
    "OracleEnumAdapter": (".adapters", "OracleEnumAdapter"),
    "OracleExplainResult": (".explain", "OracleExplainResult"),
    "OracleExplainRow": (".explain", "OracleExplainRow"),
    "OracleIncompleteTypeDefinition": (".expression.ddl.type", "OracleIncompleteTypeDefinition"),
    "OracleIntegerType": (".expression.types", "OracleIntegerType"),
    "OracleIntervalAdapter": (".adapters", "OracleIntervalAdapter"),
    "OracleJSONAdapter": (".adapters", "OracleJSONAdapter"),
    "OracleLongRawType": (".expression.types", "OracleLongRawType"),
    "OracleLongType": (".expression.types", "OracleLongType"),
    "OracleNClobType": (".expression.types", "OracleNClobType"),
    "OracleNVarChar2Type": (".expression.types", "OracleNVarChar2Type"),
    "OracleNestedTableTypeDefinition": (".expression.ddl.type", "OracleNestedTableTypeDefinition"),
    "OracleObjectTypeDefinition": (".expression.ddl.type", "OracleObjectTypeDefinition"),
    "OracleRawType": (".expression.types", "OracleRawType"),
    "OracleRowIDAdapter": (".adapters", "OracleRowIDAdapter"),
    "OracleSDOGeometryAdapter": (".adapters", "OracleSDOGeometryAdapter"),
    "OracleSchemaDiffer": (".schema", "OracleSchemaDiffer"),
    "OracleSetTypeFinalAction": (".expression.ddl.type", "OracleSetTypeFinalAction"),
    "OracleSetTypeInstantiableAction": (".expression.ddl.type", "OracleSetTypeInstantiableAction"),
    "OracleSmallIntType": (".expression.types", "OracleSmallIntType"),
    "OracleSqljTypeDefinition": (".expression.ddl.type", "OracleSqljTypeDefinition"),
    "OracleStringAdapter": (".adapters", "OracleStringAdapter"),
    "OracleTimeAdapter": (".adapters", "OracleTimeAdapter"),
    "OracleTransactionManager": (".transaction", "OracleTransactionManager"),
    "OracleTransactionMixin": (".mixins", "OracleTransactionMixin"),
    "OracleTypeAlterAction": (".expression.ddl.type", "OracleTypeAlterAction"),
    "OracleTypeAttribute": (".expression.ddl.type", "OracleTypeAttribute"),
    "OracleTypeDependentHandling": (".expression.ddl.type", "OracleTypeDependentHandling"),
    "OracleTypeMethod": (".expression.ddl.type", "OracleTypeMethod"),
    "OracleTypeSupportMixin": (".mixins", "OracleTypeSupportMixin"),
    "OracleUUIDAdapter": (".adapters", "OracleUUIDAdapter"),
    "OracleVarChar2Type": (".expression.types", "OracleVarChar2Type"),
    "OracleVarrayTypeDefinition": (".expression.ddl.type", "OracleVarrayTypeDefinition"),
    "OracleVectorAdapter": (".adapters", "OracleVectorAdapter"),
    "OracleXMLAdapter": (".adapters", "OracleXMLAdapter"),
    "OracleXmlType": (".expression.types", "OracleXmlType"),
    "VERSION_FULL_QUERY": (".version", "VERSION_FULL_QUERY"),
    "VERSION_QUERY": (".version", "VERSION_QUERY"),
    "check_cast_compatibility": (".type_compatibility", "check_cast_compatibility"),
    "get_compatible_types": (".type_compatibility", "get_compatible_types"),
    "oracle_adapters": (".adapters", "oracle_adapters"),
    "ru_from_version_full": (".version", "ru_from_version_full"),
}

if TYPE_CHECKING:  # pragma: no cover - re-exports for type checkers
    from .backend import OracleBackend
    from .async_backend import AsyncOracleBackend
    from .config import OracleConnectionConfig
    from .collation import OracleCollation
    from .dialect import OracleDialect
    from .transaction import OracleTransactionManager
    from .async_transaction import AsyncOracleTransactionManager
    from .adapters import (
        OracleBooleanAdapter,
        OracleDateTimeAdapter,
        OracleDateAdapter,
        OracleTimeAdapter,
        OracleDecimalAdapter,
        OracleJSONAdapter,
        OracleBytesAdapter,
        OracleStringAdapter,
        OracleUUIDAdapter,
        OracleEnumAdapter,
        OracleIntervalAdapter,
        OracleRowIDAdapter,
        OracleXMLAdapter,
        OracleSDOGeometryAdapter,
        OracleVectorAdapter,
        oracle_adapters,
    )
    from .explain import OracleExplainResult, OracleExplainRow
    from .mixins import (
        OracleTransactionMixin,
        OracleBackendMixin,
        OracleConcurrencyMixin,
        OracleTypeSupportMixin,
    )
    from .expression.types import (
        OracleBigIntType,
        OracleBlobType,
        OracleCharType,
        OracleClobType,
        OracleIntegerType,
        OracleLongRawType,
        OracleLongType,
        OracleNClobType,
        OracleNVarChar2Type,
        OracleRawType,
        OracleSmallIntType,
        OracleVarChar2Type,
        OracleXmlType,
    )
    from .expression.ddl.type import (
        DropTypeBodyExpression,
        OracleAlterTypeAddAttributeAction,
        OracleAlterTypeAddMethodAction,
        OracleAlterTypeCompileAction,
        OracleAlterTypeDependentHandling,
        OracleTypeDependentHandling,
        OracleAlterTypeAttributeAction,
        OracleAlterTypeMethodAction,
        OracleAlterTypeModifyLimitAction,
        OracleCompileTypeAction,
        OracleSetTypeFinalAction,
        OracleSetTypeInstantiableAction,
        OracleAlterTypeDropAttributeAction,
        OracleAlterTypeDropMethodAction,
        OracleAlterTypeElementTypeAction,
        OracleAlterTypeFinalAction,
        OracleAlterTypeInstantiableAction,
        OracleAlterTypeLimitAction,
        OracleAlterTypeModifyAttributeAction,
        OracleAlterTypeResetAction,
        OracleCreateTypeBodyExpression,
        OracleDropTypeBodyExpression,
        OracleDropTypeExpression,
        OracleIncompleteTypeDefinition,
        OracleNestedTableTypeDefinition,
        OracleObjectTypeDefinition,
        OracleSqljTypeDefinition,
        OracleTypeAlterAction,
        OracleTypeAttribute,
        OracleTypeMethod,
        OracleVarrayTypeDefinition,
    )
    from .version import VERSION_FULL_QUERY, VERSION_QUERY, ru_from_version_full
    from .schema import OracleSchemaDiffer
    from .type_compatibility import (
        DIRECT_COMPATIBLE_CASTS,
        check_cast_compatibility,
        get_compatible_types,
    )

__all__ = [
    "OracleBackend",
    "AsyncOracleBackend",
    "OracleConnectionConfig",
    "OracleDialect",
    "OracleCollation",
    "OracleTransactionManager",
    "AsyncOracleTransactionManager",
    "OracleBooleanAdapter",
    "OracleDateTimeAdapter",
    "OracleDateAdapter",
    "OracleTimeAdapter",
    "OracleDecimalAdapter",
    "OracleJSONAdapter",
    "OracleBytesAdapter",
    "OracleStringAdapter",
    "OracleUUIDAdapter",
    "OracleEnumAdapter",
    "OracleIntervalAdapter",
    "OracleRowIDAdapter",
    "OracleXMLAdapter",
    "OracleSDOGeometryAdapter",
    "OracleVectorAdapter",
    "oracle_adapters",
    "OracleExplainResult",
    "OracleExplainRow",
    "OracleTransactionMixin",
    "OracleBackendMixin",
    "OracleConcurrencyMixin",
    "OracleTypeSupportMixin",
    "OracleIntegerType",
    "OracleSmallIntType",
    "OracleBigIntType",
    "OracleVarChar2Type",
    "OracleNVarChar2Type",
    "OracleCharType",
    "OracleClobType",
    "OracleNClobType",
    "OracleLongType",
    "OracleXmlType",
    "OracleRawType",
    "OracleLongRawType",
    "OracleBlobType",
    "OracleTypeAttribute",
    "OracleTypeMethod",
    "OracleObjectTypeDefinition",
    "OracleSqljTypeDefinition",
    "OracleVarrayTypeDefinition",
    "OracleNestedTableTypeDefinition",
    "OracleIncompleteTypeDefinition",
    "OracleAlterTypeDependentHandling",
    "OracleTypeDependentHandling",
    "OracleAlterTypeAttributeAction",
    "OracleAlterTypeMethodAction",
    "OracleAlterTypeModifyLimitAction",
    "OracleCompileTypeAction",
    "OracleSetTypeFinalAction",
    "OracleSetTypeInstantiableAction",
    "OracleTypeAlterAction",
    "OracleAlterTypeAddAttributeAction",
    "OracleAlterTypeModifyAttributeAction",
    "OracleAlterTypeDropAttributeAction",
    "OracleAlterTypeAddMethodAction",
    "OracleAlterTypeDropMethodAction",
    "OracleAlterTypeLimitAction",
    "OracleAlterTypeElementTypeAction",
    "OracleAlterTypeCompileAction",
    "OracleAlterTypeFinalAction",
    "OracleAlterTypeInstantiableAction",
    "OracleAlterTypeResetAction",
    "OracleCreateTypeBodyExpression",
    "OracleDropTypeExpression",
    "DropTypeBodyExpression",
    "OracleDropTypeBodyExpression",
    "VERSION_FULL_QUERY",
    "VERSION_QUERY",
    "ru_from_version_full",
    "OracleSchemaDiffer",
    "DIRECT_COMPATIBLE_CASTS",
    "check_cast_compatibility",
    "get_compatible_types",
]


def __getattr__(name: str):
    """Resolve a public name by importing its defining submodule once."""
    target = _EXPORTS.get(name)
    if target is not None:
        import importlib

        module_name, attr = target
        value = getattr(importlib.import_module(module_name, __name__), attr)
        globals()[name] = value  # cache, so __getattr__ runs at most once per name
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(_EXPORTS))
