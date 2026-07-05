# src/rhosocial/activerecord/backend/impl/oracle/show/expressions.py
"""
Oracle data dictionary expression classes.

Each expression class collects parameters and delegates SQL generation
to the OracleShowDialectMixin's compose_query_* methods.

Inheritance pattern mirrors MySQL's ShowExpression:
- OracleQueryExpression is the base, accepting an OracleDialect.
- Subclasses collect params via fluent setters (schema, owner, like_pattern).
- to_sql() delegates to the dialect's compose_query_* method.

Bound-parameter placeholders use Oracle's native colon prefix (":1", ":name"),
as emitted by the dialect mixin.
"""

from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import (
    BaseExpression,
    SQLQueryAndParams,
)

if TYPE_CHECKING:
    from ...dialect import OracleDialect


class OracleQueryExpression(BaseExpression):
    """Base class for Oracle data dictionary query expressions.

    Subclasses wrap a SELECT against Oracle data dictionary views
    (USER_/ALL_/DBA_/V$) and expose fluent setters.
    """

    def __init__(self, dialect: "OracleDialect"):
        super().__init__(dialect)
        self._schema: Optional[str] = None
        self._owner: Optional[str] = None
        self._name_pattern: Optional[str] = None

    def schema(self, name: str) -> "OracleQueryExpression":
        """Set schema/owner prefix (USER_/ALL_/DBA_ scope selection)."""
        self._schema = name
        return self

    def owner(self, name: str) -> "OracleQueryExpression":
        """Filter objects by owner (ALL_/DBA_ views)."""
        self._owner = name
        return self

    def like(self, pattern: str) -> "OracleQueryExpression":
        """Apply a LIKE-style object name filter."""
        self._name_pattern = pattern
        return self

    def get_params(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if self._schema is not None:
            params["schema"] = self._schema
        if self._owner is not None:
            params["owner"] = self._owner
        if self._name_pattern is not None:
            params["name_pattern"] = self._name_pattern
        return params

    def to_sql(self) -> SQLQueryAndParams:
        raise NotImplementedError("Subclasses must implement to_sql()")

    # Convenience for type checker
    @property
    def dialect_(self) -> "OracleDialect":
        return self._dialect  # type: ignore[return-value]


class OracleQuerySessionsExpression(OracleQueryExpression):
    """Query V$SESSION for active sessions."""

    def __init__(self, dialect: "OracleDialect", active_only: bool = False):
        super().__init__(dialect)
        self._active_only = active_only

    def active_only(self, value: bool = True) -> "OracleQuerySessionsExpression":
        self._active_only = value
        return self

    def get_params(self) -> Dict[str, Any]:
        p = super().get_params()
        p["active_only"] = self._active_only
        return p

    def to_sql(self) -> SQLQueryAndParams:
        return self._dialect.compose_query_sessions_sql(self)  # type: ignore[attr-defined]


class OracleQueryRunningSQLExpression(OracleQueryExpression):
    """Query V$SQL for currently running SQL statements."""

    def __init__(self, dialect: "OracleDialect", limit: int = 50):
        super().__init__(dialect)
        self._limit = limit

    def limit(self, value: int) -> "OracleQueryRunningSQLExpression":
        self._limit = value
        return self

    def get_params(self) -> Dict[str, Any]:
        p = super().get_params()
        p["limit"] = self._limit
        return p

    def to_sql(self) -> SQLQueryAndParams:
        return self._dialect.compose_query_running_sql_sql(self)  # type: ignore[attr-defined]


class OracleQueryDatabaseInfoExpression(OracleQueryExpression):
    """Query V$DATABASE for database details."""

    def to_sql(self) -> SQLQueryAndParams:
        return self._dialect.compose_query_database_info_sql(self)  # type: ignore[attr-defined]


class OracleQueryInstanceInfoExpression(OracleQueryExpression):
    """Query V$INSTANCE for instance details."""

    def to_sql(self) -> SQLQueryAndParams:
        return self._dialect.compose_query_instance_info_sql(self)  # type: ignore[attr-defined]


class OracleQueryObjectsExpression(OracleQueryExpression):
    """Query USER_/ALL_/DBA_OBJECTS for valid objects."""

    def __init__(self, dialect: "OracleDialect", object_type: Optional[str] = None):
        super().__init__(dialect)
        self._object_type = object_type
        self._include_invalid: bool = False

    def object_type(self, value: str) -> "OracleQueryObjectsExpression":
        self._object_type = value
        return self

    def include_invalid(self, value: bool = True) -> "OracleQueryObjectsExpression":
        self._include_invalid = value
        return self

    def get_params(self) -> Dict[str, Any]:
        p = super().get_params()
        if self._object_type is not None:
            p["object_type"] = self._object_type
        p["include_invalid"] = self._include_invalid
        return p

    def to_sql(self) -> SQLQueryAndParams:
        return self._dialect.compose_query_objects_sql(self)  # type: ignore[attr-defined]


class OracleQueryLocksExpression(OracleQueryExpression):
    """Query V$LOCK for current lock state."""

    def to_sql(self) -> SQLQueryAndParams:
        return self._dialect.compose_query_locks_sql(self)  # type: ignore[attr-defined]


class OracleQueryWaitEventsExpression(OracleQueryExpression):
    """Query V$SESSION_WAIT for active wait events."""

    def to_sql(self) -> SQLQueryAndParams:
        return self._dialect.compose_query_wait_events_sql(self)  # type: ignore[attr-defined]


class OracleQueryNlsParametersExpression(OracleQueryExpression):
    """Query NLS_SESSION_PARAMETERS / NLS_DATABASE_PARAMETERS."""

    def __init__(self, dialect: "OracleDialect", scope: str = "SESSION"):
        super().__init__(dialect)
        self._scope = scope.upper()

    def scope(self, value: str) -> "OracleQueryNlsParametersExpression":
        self._scope = value.upper()
        return self

    def get_params(self) -> Dict[str, Any]:
        p = super().get_params()
        p["scope"] = self._scope
        return p

    def to_sql(self) -> SQLQueryAndParams:
        return self._dialect.compose_query_nls_parameters_sql(self)  # type: ignore[attr-defined]


__all__ = [
    "OracleQueryExpression",
    "OracleQuerySessionsExpression",
    "OracleQueryRunningSQLExpression",
    "OracleQueryDatabaseInfoExpression",
    "OracleQueryInstanceInfoExpression",
    "OracleQueryObjectsExpression",
    "OracleQueryLocksExpression",
    "OracleQueryWaitEventsExpression",
    "OracleQueryNlsParametersExpression",
]
