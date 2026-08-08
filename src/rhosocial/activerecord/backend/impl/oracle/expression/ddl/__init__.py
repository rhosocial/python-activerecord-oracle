# src/rhosocial/activerecord/backend/impl/oracle/expression/ddl/__init__.py
"""Oracle DDL expression sub-package.

Hosts Oracle-specific DDL expressions that do not map onto a core statement
class, e.g. synonyms and database links.
"""

from .database_link import (
    OracleCreateDatabaseLinkExpression,
    OracleDropDatabaseLinkExpression,
)
from .synonym import (
    OracleCreateSynonymExpression,
    OracleDropSynonymExpression,
)

__all__ = [
    "OracleCreateSynonymExpression",
    "OracleDropSynonymExpression",
    "OracleCreateDatabaseLinkExpression",
    "OracleDropDatabaseLinkExpression",
]
