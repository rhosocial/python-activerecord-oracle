# src/rhosocial/activerecord/backend/impl/oracle/expression/ddl/__init__.py
"""Oracle DDL expression sub-package.

Hosts Oracle-specific DDL expressions that do not map onto a core statement
class, e.g. synonyms and database links.
"""

from .database_link import (
    OracleCreateDatabaseLinkStatement,
    OracleDropDatabaseLinkStatement,
)
from .routine import (
    OracleCreateFunctionStatement,
    OracleCreatePackageBodyStatement,
    OracleCreatePackageStatement,
    OracleCreateProcedureStatement,
    OracleDropRoutineStatement,
    OracleDropRoutineObjectType,
    OracleRoutineParameter,
    OracleRoutineParameterMode,
)
from .synonym import (
    OracleCreateSynonymStatement,
    OracleDropSynonymStatement,
)
from .type import (
    DropTypeBodyStatement,
    OracleAlterTypeAddAttributeAction,
    OracleAlterTypeAddMethodAction,
    OracleAlterTypeCompileAction,
    OracleAlterTypeDependentHandling,
    OracleAlterTypeDropAttributeAction,
    OracleAlterTypeDropMethodAction,
    OracleAlterTypeElementTypeAction,
    OracleAlterTypeFinalAction,
    OracleAlterTypeInstantiableAction,
    OracleAlterTypeLimitAction,
    OracleAlterTypeModifyAttributeAction,
    OracleAlterTypeResetAction,
    OracleCreateTypeBodyStatement,
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

__all__ = [
    "OracleCreateSynonymStatement",
    "OracleDropSynonymStatement",
    "OracleCreateDatabaseLinkStatement",
    "OracleDropDatabaseLinkStatement",
    "OracleRoutineParameterMode",
    "OracleRoutineParameter",
    "OracleCreateProcedureStatement",
    "OracleCreateFunctionStatement",
    "OracleCreatePackageStatement",
    "OracleCreatePackageBodyStatement",
    "OracleDropRoutineObjectType",
    "OracleDropRoutineStatement",
    "OracleTypeAttribute",
    "OracleTypeMethod",
    "OracleObjectTypeDefinition",
    "OracleSqljTypeDefinition",
    "OracleVarrayTypeDefinition",
    "OracleNestedTableTypeDefinition",
    "OracleIncompleteTypeDefinition",
    "OracleAlterTypeDependentHandling",
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
    "OracleCreateTypeBodyStatement",
    "OracleDropTypeExpression",
    "DropTypeBodyStatement",
    "OracleDropTypeBodyExpression",
]
