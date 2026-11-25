# src/rhosocial/activerecord/backend/impl/oracle/expression/show.py
"""
Oracle data dictionary expression classes.

Each expression class collects parameters; SQL generation is declared as a
``format_method`` that the OracleShowDialectMixin implements by delegating to
its ``compose_query_*`` methods.

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

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

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

    @property
    def format_method(self) -> str:
        """The dialect method that composes the V$SESSION query."""
        return "format_oracle_query_sessions_expression"


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

    @property
    def format_method(self) -> str:
        """The dialect method that composes the V$SQL query."""
        return "format_oracle_query_running_sql_expression"


class OracleQueryDatabaseInfoExpression(OracleQueryExpression):
    """Query V$DATABASE for database details."""

    @property
    def format_method(self) -> str:
        """The dialect method that composes the V$DATABASE query."""
        return "format_oracle_query_database_info_expression"


class OracleQueryInstanceInfoExpression(OracleQueryExpression):
    """Query V$INSTANCE for instance details."""

    @property
    def format_method(self) -> str:
        """The dialect method that composes the V$INSTANCE query."""
        return "format_oracle_query_instance_info_expression"


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

    @property
    def format_method(self) -> str:
        """The dialect method that composes the object-catalogue query."""
        return "format_oracle_query_objects_expression"


class OracleQueryLocksExpression(OracleQueryExpression):
    """Query V$LOCK for current lock state."""

    @property
    def format_method(self) -> str:
        """The dialect method that composes the V$LOCK query."""
        return "format_oracle_query_locks_expression"


class OracleQueryWaitEventsExpression(OracleQueryExpression):
    """Query V$SESSION_WAIT for active wait events."""

    @property
    def format_method(self) -> str:
        """The dialect method that composes the V$SESSION_WAIT query."""
        return "format_oracle_query_wait_events_expression"


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

    @property
    def format_method(self) -> str:
        """The dialect method that composes the NLS parameter query."""
        return "format_oracle_query_nls_parameters_expression"


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
