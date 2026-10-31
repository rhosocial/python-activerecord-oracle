# src/rhosocial/activerecord/backend/impl/oracle/show/dialect.py
"""Oracle SHOW-style data dictionary query composition.

Oracle has no ``SHOW`` statement, so what MySQL answers with ``SHOW ...`` is
answered here by reading the data dictionary views (``V$SESSION``, ``V$SQL``,
``V$DATABASE``, ...). Each ``compose_query_*`` method builds that read as an
expression tree and renders it -- projections are columns and function calls,
filters are predicate nodes, ordering is an ``OrderByClause`` -- so the SQL is
spelled by the nodes rather than assembled from string fragments here (org
rules §16.2).

Every filter value is a bind parameter, including the row limit of the
``V$SQL`` read: the limit is caller-supplied data, and Oracle accepts a bind in
``FETCH FIRST``. Placeholders are whatever
:meth:`~..dialect.OracleDialect.get_parameter_placeholder` says (``?``); the
backend renumbers them to ``:N`` at execution.

The view names are written bare (``name_need_quote=False``). Oracle stores them
in upper case and folds an unquoted identifier the same way, while a quoted
``"V$SESSION"`` would ask for an object literally called that -- which is not
what the dictionary calls it.
"""
from typing import Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ..expression.show import (
        OracleQuerySessionsExpression,
        OracleQueryRunningSQLExpression,
        OracleQueryDatabaseInfoExpression,
        OracleQueryInstanceInfoExpression,
        OracleQueryObjectsExpression,
        OracleQueryLocksExpression,
        OracleQueryWaitEventsExpression,
        OracleQueryNlsParametersExpression,
    )

#: ``TO_CHAR`` format for the timestamp columns these reads project. Rendered
#: here as a literal so the value is SQL data rather than a Python format.
_TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS"


class OracleShowDialectMixin:
    """Expression-tree composition for Oracle data dictionary reads.

    Each method accepts the corresponding ``OracleQuery*Expression``, reads its
    parameters through the node fields, and returns a ``(sql, params)`` tuple
    with the binds in the order they appear in the SQL.
    """

    # ============================================================
    # Shared building blocks
    # ============================================================

    def _show_column(
        self,
        name: str,
        table: Optional[str] = None,
        alias: Optional[str] = None,
        need_quote: bool = True,
    ):
        """A dictionary column, qualified when the read is joined or aliased."""
        from rhosocial.activerecord.backend.expression import Column

        return Column(
            self, name, table, alias=alias, name_need_quote=need_quote
        )

    def _show_relation(self, name: str, alias: Optional[str] = None):
        """A dynamic/dictionary view as a ``FROM`` source, named bare."""
        from rhosocial.activerecord.backend.expression import NamedRelationRef
        from rhosocial.activerecord.backend.expression.objects.relation import Table

        return NamedRelationRef(
            self,
            Table(self, name, name_need_quote=False),
            alias=alias,
            alias_need_quote=False,
        )

    def _show_equals(self, left, right):
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        return ComparisonPredicate(self, "eq", left, right)

    def _show_all_of(self, *predicates):
        """The conjunction of the given predicates, or ``None`` when there are none."""
        from rhosocial.activerecord.backend.expression import LogicalPredicate

        kept = [p for p in predicates if p is not None]
        if not kept:
            return None
        return LogicalPredicate(self, "AND", *kept)

    def _show_where(self, *predicates):
        from rhosocial.activerecord.backend.expression import WhereClause

        condition = self._show_all_of(*predicates)
        return None if condition is None else WhereClause(self, condition)

    def _show_timestamp(self, column: str, table: Optional[str], alias: str):
        """``TO_CHAR(col, 'YYYY-MM-DD HH24:MI:SS') AS alias``.

        The format string is inline: it is a format declaration in a syntax
        position, not a value the caller supplied.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, Literal

        return FunctionCall(
            self,
            "TO_CHAR",
            self._show_column(column, table),
            Literal(self, _TIMESTAMP_FORMAT, inline=True),
            alias=alias,
        )

    # ============================================================
    # Sessions / Processes (V$SESSION, V$PROCESS)
    # ============================================================

    def compose_query_sessions_sql(
        self, expr: "OracleQuerySessionsExpression"
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression import (
            Literal, OrderByClause, QueryStatement,
        )
        from rhosocial.activerecord.backend.expression.predicates import IsNullPredicate

        params = expr.get_params()
        active_only = params.get("active_only", False)

        if active_only:
            # A named-session filter: the caller asked for the sessions that are
            # actually working, so the state is compared to a bound value.
            condition = self._show_equals(
                self._show_column("status", "s"), Literal(self, "ACTIVE")
            )
        else:
            # Everything the current user can see, which excludes the background
            # processes that carry no username.
            condition = IsNullPredicate(
                self, self._show_column("username", "s"), is_not=True
            )

        return QueryStatement(
            self,
            select=[
                self._show_column("sid", "s", "sid"),
                self._show_column("serial#", "s", "serial_num", need_quote=False),
                self._show_column("username", "s", "username"),
                self._show_column("status", "s", "status"),
                self._show_column("machine", "s", "machine"),
                self._show_column("program", "s", "program"),
                self._show_column("module", "s", "module"),
                self._show_column("action", "s", "action"),
                self._show_timestamp("logon_time", "s", "logon_time"),
                self._show_column("process", "s", "process"),
            ],
            from_=self._show_relation("v$session", "s"),
            where=self._show_where(condition),
            order_by=OrderByClause(self, [self._show_column("sid", "s")]),
        ).to_sql()

    def compose_query_process_sql(self) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression import (
            OrderByClause, QueryStatement,
        )

        return QueryStatement(
            self,
            select=[
                self._show_column("spid", "p", "spid"),
                self._show_column("pid", "p", "pid"),
                self._show_column("program", "p", "program"),
                self._show_column("background", "p", "background"),
            ],
            from_=self._show_relation("v$process", "p"),
            order_by=OrderByClause(self, [self._show_column("pid", "p")]),
        ).to_sql()

    # ============================================================
    # Running SQL (V$SQL)
    # ============================================================

    def compose_query_running_sql_sql(
        self, expr: "OracleQueryRunningSQLExpression"
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression import (
            ArithmeticOperation, LimitOffsetClause, Literal, OrderByClause,
            QueryStatement,
        )

        params = expr.get_params()
        limit = int(params.get("limit", 50))
        if limit < 1:
            limit = 1

        def seconds(column: str, alias: str):
            """``col / 1000000 AS alias`` -- the view stores microseconds."""
            return ArithmeticOperation(
                self,
                "div",
                self._show_column(column),
                Literal(self, 1000000, inline=True),
            ).as_(alias)

        return QueryStatement(
            self,
            select=[
                self._show_column("sql_id", alias="sql_id"),
                self._show_column("child_number", alias="child_number"),
                self._show_column("sql_text", alias="sql_text"),
                self._show_column("executions", alias="executions"),
                seconds("elapsed_time", "elapsed_seconds"),
                seconds("cpu_time", "cpu_seconds"),
                self._show_column("buffer_gets", alias="buffer_gets"),
                self._show_column("disk_reads", alias="disk_reads"),
                self._show_column("rows_processed", alias="rows_processed"),
                self._show_column("parsing_schema_name", alias="parsing_schema_name"),
                self._show_timestamp("last_active_time", None, "last_active_time"),
            ],
            from_=self._show_relation("v$sql"),
            where=self._show_where(
                self._show_greater_than(
                    self._show_column("executions"), Literal(self, 0, inline=True)
                )
            ),
            order_by=OrderByClause(
                self, [(self._show_column("elapsed_time"), "DESC")]
            ),
            limit_offset=LimitOffsetClause(self, limit=limit),
        ).to_sql()

    def _show_greater_than(self, left, right):
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        return ComparisonPredicate(self, "gt", left, right)

    # ============================================================
    # Database / Instance info (V$DATABASE, V$INSTANCE)
    # ============================================================

    def compose_query_database_info_sql(
        self, expr: "OracleQueryDatabaseInfoExpression"
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression import QueryStatement

        return QueryStatement(
            self,
            select=[
                self._show_column("name", alias="name"),
                self._show_column("dbid", alias="dbid"),
                self._show_timestamp("created", None, "created"),
                self._show_column("log_mode", alias="log_mode"),
                self._show_column("open_mode", alias="open_mode"),
                self._show_column("platform_name", alias="platform_name"),
                self._show_column("database_role", alias="role"),
            ],
            from_=self._show_relation("v$database"),
        ).to_sql()

    def compose_query_instance_info_sql(
        self, expr: "OracleQueryInstanceInfoExpression"
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression import QueryStatement

        return QueryStatement(
            self,
            select=[
                self._show_column("instance_name", alias="instance_name"),
                self._show_column("instance_number", alias="instance_num"),
                self._show_column("host_name", alias="host_name"),
                self._show_column("version", alias="version"),
                self._show_column("status", alias="status"),
                self._show_timestamp("startup_time", None, "startup_time"),
                self._show_column("archiver", alias="archiver"),
            ],
            from_=self._show_relation("v$instance"),
        ).to_sql()

    # ============================================================
    # Objects (USER_/ALL_/DBA_OBJECTS)
    # ============================================================

    def compose_query_objects_sql(
        self, expr: "OracleQueryObjectsExpression"
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression import (
            LikePredicate, LikeOperator, Literal, OrderByClause, QueryStatement,
        )

        params = expr.get_params()
        include_invalid = params.get("include_invalid", False)
        object_type = params.get("object_type")
        name_pattern = params.get("name_pattern")

        # DBA_ requires privileges; ALL_ is what an owner filter needs, and
        # USER_OBJECTS is the connected schema's own objects when no filter is
        # given. The choice is stated here rather than probed.
        view = "all_objects" if params.get("owner") else "user_objects"

        conditions = []
        if not include_invalid:
            # ``VALID`` is a fixed state name, not caller data.
            conditions.append(
                self._show_equals(
                    self._show_column("status"), Literal(self, "VALID", inline=True)
                )
            )
        if params.get("owner") is not None:
            conditions.append(
                self._show_equals(
                    self._show_column("owner"), Literal(self, params["owner"])
                )
            )
        if object_type is not None:
            conditions.append(
                self._show_equals(
                    self._show_column("object_type"), Literal(self, object_type)
                )
            )
        if name_pattern is not None:
            conditions.append(
                LikePredicate(
                    self,
                    LikeOperator.LIKE,
                    self._show_column("object_name"),
                    Literal(self, name_pattern),
                )
            )

        return QueryStatement(
            self,
            select=[
                self._show_column("owner", alias="owner"),
                self._show_column("object_name", alias="object_name"),
                self._show_column("object_type", alias="object_type"),
                self._show_column("object_id", alias="object_id"),
                self._show_timestamp("created", None, "created"),
                self._show_timestamp("last_ddl_time", None, "last_ddl_time"),
                self._show_column("status", alias="status"),
                self._show_column("temporary", alias="temporary"),
            ],
            from_=self._show_relation(view),
            where=self._show_where(*conditions),
            order_by=OrderByClause(
                self, [self._show_column("object_type"), self._show_column("object_name")]
            ),
        ).to_sql()

    # ============================================================
    # Locks (V$LOCK)
    # ============================================================

    def compose_query_locks_sql(
        self, expr: "OracleQueryLocksExpression"
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression import (
            OrderByClause, QueryStatement,
        )

        return QueryStatement(
            self,
            select=[
                self._show_column("sid", alias="sid"),
                self._show_column("type", alias="lock_type"),
                self._show_column("id1", alias="id1"),
                self._show_column("id2", alias="id2"),
                self._show_column("lmode", alias="lmode"),
                self._show_column("request", alias="request"),
                self._show_column("block", alias="block"),
            ],
            from_=self._show_relation("v$lock"),
            order_by=OrderByClause(self, [self._show_column("sid")]),
        ).to_sql()

    # ============================================================
    # Wait events (V$SESSION_WAIT)
    # ============================================================

    def compose_query_wait_events_sql(
        self, expr: "OracleQueryWaitEventsExpression"
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression import (
            OrderByClause, QueryStatement,
        )

        return QueryStatement(
            self,
            select=[
                self._show_column("sid", "w", "sid"),
                self._show_column("seq#", "w", "seq_num", need_quote=False),
                self._show_column("event", "w", "event"),
                self._show_column("p1text", "w", "p1text"),
                self._show_column("p1", "w", "p1"),
                self._show_column("wait_class", "w", "wait_class"),
                self._show_column("wait_time", "w", "wait_time"),
                self._show_column("seconds_in_wait", "w", "seconds_in_wait"),
                self._show_column("state", "w", "state"),
            ],
            from_=self._show_relation("v$session_wait", "w"),
            order_by=OrderByClause(self, [self._show_column("sid", "w")]),
        ).to_sql()

    # ============================================================
    # NLS parameters
    # ============================================================

    def compose_query_nls_parameters_sql(
        self, expr: "OracleQueryNlsParametersExpression"
    ) -> Tuple[str, tuple]:
        """The NLS read, from the session-scope or database-scope view.

        The view is chosen by the requested scope; the two are the same read of
        different relations, so the projection and ordering are shared.
        """
        from rhosocial.activerecord.backend.expression import (
            OrderByClause, QueryStatement,
        )

        params = expr.get_params()
        scope = str(params.get("scope", "SESSION")).upper()
        view = (
            "nls_database_parameters"
            if scope == "DATABASE"
            else "nls_session_parameters"
        )
        return QueryStatement(
            self,
            select=[
                self._show_column("parameter", alias="parameter"),
                self._show_column("value", alias="value"),
            ],
            from_=self._show_relation(view),
            order_by=OrderByClause(self, [self._show_column("parameter")]),
        ).to_sql()

    def _select_objects_view(self, owner: Optional[str]) -> str:
        """Choose USER_/ALL_/DBA_OBJECTS based on filter intent.

        Retained as the named statement of the choice; the composition above
        reads the same decision, and the two agree because both ask the same
        question of the same field.
        """
        if owner is None:
            return "user_objects"
        return "all_objects"


__all__ = ["OracleShowDialectMixin"]
