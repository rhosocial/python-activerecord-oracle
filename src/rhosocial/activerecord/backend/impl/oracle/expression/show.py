# src/rhosocial/activerecord/backend/impl/oracle/expression/show.py
"""
Oracle data dictionary expression classes.

Each expression class collects parameters and delegates SQL generation
to the OracleShowDialectMixin's compose_query_* methods.

Inheritance pattern mirrors MySQL's ShowExpression:
- OracleQueryExpression is the base, accepting an OracleDialect.
- Subclasses collect params via constructor keyword arguments.

Bound-parameter placeholders use Oracle's native colon prefix (":1", ":name"),
as emitted by the dialect mixin.

Every parameter is a constructor field, stored under its own name and read
back by ``get_params()`` -- the introspection-based default reads a
parameter off ``self.<parameter name>``, so a field stored as ``_name``
while the constructor declares ``name`` makes the class unserialisable and
the dialect's ``params.get("name")`` read nothing. There are no fluent
setters here either: a builder that patches a node after construction has
no parameter an artifact can name (expression-dialect §7).
"""

from typing import Any, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import (
    BaseExpression,
    SQLQueryAndParams,
)

if TYPE_CHECKING:  # pragma: no cover
    from ..dialect import OracleDialect


class OracleQueryExpression(BaseExpression):
    """Base class for Oracle data dictionary query expressions.

    Subclasses wrap a SELECT against Oracle data dictionary views
    (USER_/ALL_/DBA_/V$) and take their filters as constructor arguments.

    Args:
        dialect: the Oracle dialect instance.
        schema: the schema the query is scoped to.
        owner: the owner filter (ALL_/DBA_ views).
        name_pattern: a LIKE-style object name filter.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        schema: Optional[str] = None,
        owner: Optional[str] = None,
        name_pattern: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.schema: Optional[str] = schema
        self.owner: Optional[str] = owner
        self.name_pattern: Optional[str] = name_pattern

    def to_sql(self) -> SQLQueryAndParams:
        raise NotImplementedError("Subclasses must implement to_sql()")

    # Convenience for type checker
    @property
    def dialect_(self) -> "OracleDialect":
        return self._dialect  # type: ignore[return-value]


class OracleQuerySessionsExpression(OracleQueryExpression):
    """Query V$SESSION for active sessions."""

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        active_only: bool = False,
        schema: Optional[str] = None,
        owner: Optional[str] = None,
        name_pattern: Optional[str] = None,
    ):
        super().__init__(
            dialect, schema=schema, owner=owner, name_pattern=name_pattern
        )
        self.active_only: bool = active_only

    def to_sql(self) -> SQLQueryAndParams:
        return self._dialect.compose_query_sessions_sql(self)  # type: ignore[attr-defined]


class OracleQueryRunningSQLExpression(OracleQueryExpression):
    """Query V$SQL for currently running SQL statements."""

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        limit: int = 50,
        schema: Optional[str] = None,
        owner: Optional[str] = None,
        name_pattern: Optional[str] = None,
    ):
        super().__init__(
            dialect, schema=schema, owner=owner, name_pattern=name_pattern
        )
        self.limit: int = limit

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

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        object_type: Optional[str] = None,
        include_invalid: bool = False,
        schema: Optional[str] = None,
        owner: Optional[str] = None,
        name_pattern: Optional[str] = None,
    ):
        super().__init__(
            dialect, schema=schema, owner=owner, name_pattern=name_pattern
        )
        self.object_type: Optional[str] = object_type
        self.include_invalid: bool = include_invalid

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

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        scope: str = "SESSION",
        schema: Optional[str] = None,
        owner: Optional[str] = None,
        name_pattern: Optional[str] = None,
    ):
        super().__init__(
            dialect, schema=schema, owner=owner, name_pattern=name_pattern
        )
        self.scope: str = scope.upper()

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
