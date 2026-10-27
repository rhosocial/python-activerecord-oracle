# src/rhosocial/activerecord/backend/impl/oracle/mixins/introspection.py
"""Oracle dialect introspection capability declaration and query formatting.

Implements the ``format_*_query`` methods consumed by the core introspector
base class via the Expression→Dialect path. All queries target the Oracle
data dictionary (``ALL_TABLES``, ``ALL_TAB_COLUMNS``, ``ALL_INDEXES``, ...)
filtered by the current schema owner.

Each query is an expression tree -- projections are ``Column`` /
``FunctionCall`` nodes, filters are predicate nodes, ordering is an
``OrderByClause`` -- so what a query asks is a structure rather than text
reassembled one clause at a time. The dialect only spells the tree
(org rules §16.2).

Two spellings are deliberate. Dictionary views are named with
``name_need_quote=False``: Oracle's dictionary names are already upper case, and
a quoted ``"V$PARAMETER"`` would ask for a table literally called that, which
does not exist. And a table qualifier travels in ``Column``'s ``table`` field
rather than inside the column name, so a joined read renders
``"T"."TABLE_NAME"`` instead of one dotted identifier.
"""
from typing import List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.introspection.types import IntrospectionScope
    from rhosocial.activerecord.backend.expression.introspection import (
        DatabaseInfoQueryStatement,
        TableListQueryStatement,
        ColumnInfoQueryStatement,
        IndexInfoQueryStatement,
        ForeignKeyQueryStatement,
        ViewListQueryStatement,
        ViewInfoQueryStatement,
        TriggerListQueryStatement,
    )
    from ...expression.introspection import (
        OraclePackageListQueryStatement,
        OraclePrimaryKeyQueryStatement,
        OracleSequenceInfoQueryStatement,
        OracleSequenceListQueryStatement,
        OracleSynonymInfoQueryStatement,
        OracleSynonymListQueryStatement,
    )

_SYSTEM_OWNERS = (
    "SYS", "SYSTEM", "XDB", "MDSYS", "CTXSYS", "ORDSYS", "OUTLN", "DBSNMP",
    "APPQOSSYS", "AUDSYS", "GSMADMIN_INTERNAL", "LBACSYS", "OLAPSYS",
    "ORDDATA", "WMSYS",
)

#: The six positions every side of the table-list query projects, in order.
#: Both sides of the ``UNION ALL`` name the same aliases in the same sequence,
#: which is what makes the compound query legal and the outer projection able to
#: address them by name.
_TABLE_LIST_PROJECTION = (
    "TABLE_NAME", "TABLE_TYPE", "COMMENTS", "NUM_ROWS", "DATA_LENGTH",
    "LAST_ANALYZED",
)


class OracleIntrospectionMixin:
    """Oracle introspection capabilities backed by the data dictionary."""

    # ========== Capability Detection ==========

    def supports_introspection(self) -> bool:
        return True

    def supports_database_info(self) -> bool:
        return True

    def supports_table_introspection(self) -> bool:
        return True

    def supports_column_introspection(self) -> bool:
        return True

    def supports_index_introspection(self) -> bool:
        return True

    def supports_foreign_key_introspection(self) -> bool:
        return True

    def supports_view_introspection(self) -> bool:
        return True

    def supports_trigger_introspection(self) -> bool:
        return True

    def get_supported_introspection_scopes(self) -> List["IntrospectionScope"]:
        from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

        return [
            IntrospectionScope.DATABASE,
            IntrospectionScope.TABLE,
            IntrospectionScope.COLUMN,
            IntrospectionScope.INDEX,
            IntrospectionScope.FOREIGN_KEY,
            IntrospectionScope.VIEW,
            IntrospectionScope.TRIGGER,
        ]

    # ========== Shared building blocks ==========

    def _relation(self, name: str, alias: Optional[str] = None):
        """A data-dictionary view as a ``FROM`` source.

        The name is written bare. Oracle stores these names in upper case and
        folds an unquoted identifier to the same, so the bare spelling resolves;
        quoting would instead ask for that exact name, and ``V$...``/``NLS_...``
        names are not identifiers a quoted reference can reach.
        """
        from rhosocial.activerecord.backend.expression import NamedRelationRef
        from rhosocial.activerecord.backend.expression.objects.relation import Table

        return NamedRelationRef(
            self,
            Table(self, name, name_need_quote=False),
            alias=alias,
            alias_need_quote=False,
        )

    def _column(self, name: str, table: Optional[str] = None, alias: Optional[str] = None):
        """A dictionary column, qualified by a table alias when the read is joined."""
        from rhosocial.activerecord.backend.expression import Column

        return Column(self, name, table, alias=alias)

    def _equals(self, left, right):
        """``left = right`` as a predicate node."""
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        return ComparisonPredicate(self, "eq", left, right)

    def _all_of(self, *predicates):
        """The conjunction of the given predicates, or ``None`` when there are none."""
        from rhosocial.activerecord.backend.expression import LogicalPredicate

        kept = [p for p in predicates if p is not None]
        if not kept:
            return None
        return LogicalPredicate(self, "AND", *kept)

    def _where(self, *predicates):
        """A ``WhereClause`` over the conjunction, or ``None`` for an unfiltered read."""
        from rhosocial.activerecord.backend.expression import WhereClause

        condition = self._all_of(*predicates)
        return None if condition is None else WhereClause(self, condition)

    def _owner_predicate(self, schema: str, owner_column, include_system: bool):
        """The owner narrowing every dictionary read here applies.

        A named schema is an equality against a **bound** value -- the caller's
        schema is request data and never SQL text. With no schema, the system
        owners are excluded through ``NOT IN`` over the same bound list, in the
        same order, that the concatenated form bound.
        """
        from rhosocial.activerecord.backend.expression import (
            InPredicate, Literal, LogicalPredicate,
        )

        if schema:
            return self._equals(owner_column, Literal(self, str(schema).upper()))
        if include_system:
            return None
        return LogicalPredicate(
            self,
            "NOT",
            InPredicate(self, owner_column, Literal(self, _SYSTEM_OWNERS)),
        )

    def _constant(self, value, alias: str, data_type):
        """A constant projection carrying *alias* and a declared type.

        A ``Literal`` has no name to give -- a literal is the one node with
        nothing to alias -- so the constant is cast to the type the position
        declares and the cast carries the alias. Declaring the type is also what
        lets the two sides of a ``UNION ALL`` agree: Oracle wants the branches to
        agree on type, and an untyped ``NULL`` leaves that to inference.
        """
        from rhosocial.activerecord.backend.expression import Literal, cast

        return cast(self, Literal(self, value, inline=True), data_type).as_(alias)

    def _not_a_materialized_view(self, schema: str, include_system: bool):
        """The predicate that keeps materialized views off the table side.

        ``ALL_TABLES`` describes a materialized view too: an MV has a table
        segment, so it is listed with the ordinary tables under object type
        ``TABLE``. The listing reads MVs from a side of their own, so the table
        side subtracts them by name -- otherwise each MV would answer twice,
        once as a table and once as itself. The names come from a subquery
        filtered by the same owner predicate as every other dictionary read,
        and travel as binds.
        """
        from rhosocial.activerecord.backend.expression import (
            InPredicate, LogicalPredicate, QueryStatement, Subquery,
        )

        materialized_view_names = Subquery(
            self,
            QueryStatement(
                self,
                select=[self._column("mview_name", "mv")],
                from_=self._relation("all_mviews", "mv"),
                where=self._where(
                    self._owner_predicate(
                        schema, self._column("OWNER", "mv"), include_system
                    )
                ),
            ),
        )
        return LogicalPredicate(
            self,
            "NOT",
            InPredicate(self, self._column("table_name", "t"), materialized_view_names),
        )

    # ========== Query Formatting ==========

    def format_database_info_query_statement(
        self, expr: "DatabaseInfoQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format database information query (NLS settings).

        Four scalar subreads of ``V$NLS_PARAMETERS`` projected from ``DUAL``.
        Each subread is a ``QueryStatement`` used as a value -- which is what a
        scalar subquery is -- rather than text spliced into an outer string.
        """
        from rhosocial.activerecord.backend.expression import (
            Literal, QueryStatement, Subquery,
        )

        settings = (
            ("CHARSET", "NLS_CHARACTERSET"),
            ("NCHAR_CHARSET", "NLS_NCHAR_CHARACTERSET"),
            ("LANGUAGE", "NLS_LANGUAGE"),
            ("TERRITORY", "NLS_TERRITORY"),
        )
        projections = [
            Subquery(
                self,
                QueryStatement(
                    self,
                    select=[self._column("value")],
                    from_=self._relation("v$nls_parameters"),
                    where=self._where(
                        self._equals(
                            self._column("parameter"),
                            Literal(self, parameter, inline=True),
                        )
                    ),
                ),
                alias=alias,
            )
            for alias, parameter in settings
        ]
        return QueryStatement(
            self, select=projections, from_=self._relation("dual")
        ).to_sql()

    def format_table_list_query_statement(self, expr: "TableListQueryStatement") -> Tuple[str, tuple]:
        """Format table (and optionally view) list query.

        Each side of the answer knows what it is reading, and says so in
        ``TABLE_TYPE`` rather than leaving every row to be called a base table:
        ordinary tables come from ``ALL_TABLES`` left-joined to
        ``ALL_TAB_COMMENTS`` for the comment, views from ``ALL_VIEWS`` and
        materialized views from ``ALL_MVIEWS``. ``ALL_TABLES`` also lists
        materialized views -- an MV is stored as a table segment -- so they are
        subtracted from the table side by name, or one MV would be reported
        twice: once as a table and once as the MV it is.

        All sides name the same six aliases in the same order. When more than
        one is wanted they are a ``UNION ALL`` and the outer query sorts the
        result, because ordering a compound query belongs to the statement
        around it.
        """
        from rhosocial.activerecord.backend.expression import (
            ArithmeticOperation, JoinClause, JoinType, Literal, OrderByClause,
            QueryStatement, SetOperation, Subquery,
        )
        from rhosocial.activerecord.backend.expression.query_sources import (
            SetOperationKind,
        )
        from rhosocial.activerecord.backend.expression.types import (
            DateType, DecimalType, VarCharType,
        )

        params = expr.get_params()
        schema = params.get("schema") or ""
        include_views = params.get("include_views", True)
        include_system = params.get("include_system", False)
        table_type = (params.get("table_type") or "").upper()

        want_tables = table_type not in ("VIEW", "MATERIALIZED VIEW")
        want_views = (include_views or table_type == "VIEW") and table_type != "MATERIALIZED VIEW"
        want_mviews = (
            include_views or table_type == "MATERIALIZED VIEW"
        ) and table_type != "VIEW"

        sides = []
        if want_tables:
            comment_join = JoinClause(
                self,
                left_table=self._relation("all_tables", "t"),
                right_table=self._relation("all_tab_comments", "c"),
                join_type=JoinType.LEFT,
                condition=self._all_of(
                    self._equals(
                        self._column("table_name", "t"), self._column("table_name", "c")
                    ),
                    self._equals(self._column("owner", "t"), self._column("owner", "c")),
                ),
            )
            sides.append(
                (
                    [
                        self._column("table_name", "t", "TABLE_NAME"),
                        self._constant("TABLE", "TABLE_TYPE", VarCharType(self, 30)),
                        self._column("comments", "c", "COMMENTS"),
                        self._column("num_rows", "t", "NUM_ROWS"),
                        ArithmeticOperation(
                            self,
                            "mul",
                            self._column("blocks", "t"),
                            Literal(self, 8192, inline=True),
                        ).as_("DATA_LENGTH"),
                        self._column("last_analyzed", "t", "LAST_ANALYZED"),
                    ],
                    comment_join,
                    self._where(
                        self._owner_predicate(
                            schema, self._column("OWNER", "t"), include_system
                        ),
                        self._not_a_materialized_view(schema, include_system),
                    ),
                )
            )
        if want_views:
            sides.append(
                (
                    [
                        self._column("view_name", "v", "TABLE_NAME"),
                        self._constant("VIEW", "TABLE_TYPE", VarCharType(self, 30)),
                        self._constant(None, "COMMENTS", VarCharType(self, 4000)),
                        self._constant(None, "NUM_ROWS", DecimalType(self)),
                        self._constant(None, "DATA_LENGTH", DecimalType(self)),
                        self._constant(None, "LAST_ANALYZED", DateType(self)),
                    ],
                    self._relation("all_views", "v"),
                    self._where(
                        self._owner_predicate(
                            schema, self._column("OWNER", "v"), include_system
                        )
                    ),
                )
            )
        if want_mviews:
            sides.append(
                (
                    [
                        self._column("mview_name", "mv", "TABLE_NAME"),
                        self._constant("MATERIALIZED VIEW", "TABLE_TYPE", VarCharType(self, 30)),
                        self._constant(None, "COMMENTS", VarCharType(self, 4000)),
                        self._constant(None, "NUM_ROWS", DecimalType(self)),
                        self._constant(None, "DATA_LENGTH", DecimalType(self)),
                        self._constant(None, "LAST_ANALYZED", DateType(self)),
                    ],
                    self._relation("all_mviews", "mv"),
                    self._where(
                        self._owner_predicate(
                            schema, self._column("OWNER", "mv"), include_system
                        )
                    ),
                )
            )

        first = QueryStatement(
            self, select=sides[0][0], from_=sides[0][1], where=sides[0][2]
        )
        if len(sides) == 1:
            return QueryStatement(
                self,
                select=sides[0][0],
                from_=sides[0][1],
                where=sides[0][2],
                order_by=OrderByClause(self, [self._column("TABLE_NAME")]),
            ).to_sql()

        # More than one side: each becomes a query over its own relation, their
        # UNION ALL is the FROM of the statement that sorts the result. The
        # compound query itself carries no ordering -- Oracle applies one
        # ORDER BY to the whole compound, which is why it belongs here rather
        # than to any one side.
        #
        # The set operation is wrapped in a ``Subquery`` because Oracle requires
        # a derived table to carry an alias (``ORA-00907`` otherwise), and the
        # alias is also what the outer projection is allowed to name.
        compound = first
        for projection, source, where in sides[1:]:
            compound = SetOperation(
                self,
                op=SetOperationKind.UNION_ALL,
                left=compound,
                right=QueryStatement(self, select=projection, from_=source, where=where),
            )
        return QueryStatement(
            self,
            select=[self._column(alias) for alias in _TABLE_LIST_PROJECTION],
            from_=Subquery(
                self,
                compound,
                alias="TABLE_LIST",
            ),
            order_by=OrderByClause(self, [self._column("TABLE_NAME")]),
        ).to_sql()

    def format_column_info_query_statement(
        self, expr: "ColumnInfoQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format column information query."""
        from rhosocial.activerecord.backend.expression import (
            Literal, OrderByClause, QueryStatement,
        )

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema", "")

        projection = [
            self._column(name, alias=name)
            for name in (
                "column_name", "data_type", "data_type_mod", "data_type_owner",
                "data_length", "data_precision", "data_scale", "nullable",
                "column_id", "default_length", "data_default", "num_distinct",
                "low_value", "high_value", "density", "num_nulls", "num_buckets",
                "last_analyzed", "sample_size", "character_set_name",
                "char_col_decl_length", "global_stats", "user_stats",
                "data_upgraded", "avg_col_len", "char_length", "char_used",
                "v80_fmt_image", "histogram", "default_on_null",
                "identity_column", "evaluation_edition", "unusable_before",
                "unusable_beginning", "collation",
            )
        ]
        return QueryStatement(
            self,
            select=projection,
            from_=self._relation("all_tab_columns"),
            where=self._where(
                self._equals(
                    self._column("table_name"), Literal(self, str(table_name).upper())
                ),
                self._equals(
                    self._column("owner"), Literal(self, str(schema).upper())
                ),
            ),
            order_by=OrderByClause(self, [self._column("column_id")]),
        ).to_sql()

    def format_primary_key_query_statement(
        self, expr: "OraclePrimaryKeyQueryStatement"
    ) -> Tuple[str, tuple]:
        """The columns of one table's primary key, in key order.

        The key is a constraint, so the two dictionary views are joined on the
        constraint name *and* the owner: a same-named constraint in another
        schema must not contribute a column to this table's key.
        """
        from rhosocial.activerecord.backend.expression import (
            JoinClause, JoinType, Literal, OrderByClause, QueryStatement,
        )

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema", "")

        key_join = JoinClause(
            self,
            left_table=self._relation("all_constraints", "cons"),
            right_table=self._relation("all_cons_columns", "cols"),
            join_type=JoinType.JOIN,
            condition=self._all_of(
                self._equals(
                    self._column("constraint_name", "cons"),
                    self._column("constraint_name", "cols"),
                ),
                self._equals(
                    self._column("owner", "cons"), self._column("owner", "cols")
                ),
            ),
        )
        return QueryStatement(
            self,
            select=[self._column("column_name", "cols")],
            from_=key_join,
            where=self._where(
                self._equals(
                    self._column("table_name", "cons"),
                    Literal(self, str(table_name).upper()),
                ),
                self._equals(
                    self._column("owner", "cons"), Literal(self, str(schema).upper())
                ),
                self._equals(
                    self._column("constraint_type", "cons"),
                    Literal(self, "P", inline=True),
                ),
            ),
            order_by=OrderByClause(self, [self._column("position", "cols")]),
        ).to_sql()

    def format_index_info_query_statement(self, expr: "IndexInfoQueryStatement") -> Tuple[str, tuple]:
        """Format index information query.

        The primary key is a constraint, not an index, so whether an index is
        *the* primary key is answered by ``ALL_CONSTRAINTS`` -- the constraint
        of type ``P`` names the index that enforces it in ``INDEX_NAME``. It is
        a left join, because an ordinary index has no such constraint row, and
        it is narrowed to ``'P'`` so the join cannot multiply the rows of one
        index that also backs a unique constraint. The joined
        ``CONSTRAINT_TYPE`` is projected as ``CONSTRAINT_TYPE`` precisely so the
        parser can tell the two apart.
        """
        from rhosocial.activerecord.backend.expression import (
            JoinClause, JoinType, Literal, OrderByClause, QueryStatement,
        )

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema", "")

        index_join = JoinClause(
            self,
            left_table=self._relation("all_indexes", "i"),
            right_table=self._relation("all_ind_columns", "ic"),
            join_type=JoinType.JOIN,
            condition=self._all_of(
                self._equals(
                    self._column("index_name", "i"), self._column("index_name", "ic")
                ),
                self._equals(self._column("owner", "i"), self._column("index_owner", "ic")),
            ),
        )
        with_primary_key = JoinClause(
            self,
            left_table=index_join,
            right_table=self._relation("all_constraints", "cons"),
            join_type=JoinType.LEFT,
            condition=self._all_of(
                self._equals(
                    self._column("index_name", "cons"), self._column("index_name", "i")
                ),
                self._equals(self._column("owner", "cons"), self._column("owner", "i")),
                self._equals(
                    self._column("constraint_type", "cons"),
                    Literal(self, "P", inline=True),
                ),
            ),
        )
        return QueryStatement(
            self,
            select=[
                self._column("index_name", "i", "INDEX_NAME"),
                self._column("index_type", "i", "INDEX_TYPE"),
                self._column("uniqueness", "i", "UNIQUENESS"),
                self._column("constraint_type", "cons", "CONSTRAINT_TYPE"),
                self._column("column_name", "ic", "COLUMN_NAME"),
                self._column("column_position", "ic", "COLUMN_POSITION"),
                self._column("descend", "ic", "DESCEND"),
            ],
            from_=with_primary_key,
            where=self._where(
                self._equals(
                    self._column("table_owner", "i"), Literal(self, str(schema).upper())
                ),
                self._equals(
                    self._column("table_name", "i"), Literal(self, str(table_name).upper())
                ),
            ),
            order_by=OrderByClause(
                self,
                [self._column("index_name", "i"), self._column("column_position", "ic")],
            ),
        ).to_sql()

    def format_foreign_key_query_statement(
        self, expr: "ForeignKeyQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format foreign key information query."""
        from rhosocial.activerecord.backend.expression import (
            JoinClause, JoinType, Literal, OrderByClause, QueryStatement,
        )

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema", "")

        # Four relations, joined in the order the constraint graph walks them:
        # the constraint, its own columns, the constraint it references, and
        # that constraint's columns.
        constraint_columns = JoinClause(
            self,
            left_table=self._relation("all_constraints", "cons"),
            right_table=self._relation("all_cons_columns", "cols"),
            join_type=JoinType.JOIN,
            condition=self._all_of(
                self._equals(
                    self._column("constraint_name", "cons"),
                    self._column("constraint_name", "cols"),
                ),
                self._equals(self._column("owner", "cons"), self._column("owner", "cols")),
            ),
        )
        with_referenced = JoinClause(
            self,
            left_table=constraint_columns,
            right_table=self._relation("all_constraints", "ref_cons"),
            join_type=JoinType.JOIN,
            condition=self._all_of(
                self._equals(
                    self._column("r_constraint_name", "cons"),
                    self._column("constraint_name", "ref_cons"),
                ),
                self._equals(
                    self._column("r_owner", "cons"), self._column("owner", "ref_cons")
                ),
            ),
        )
        all_four = JoinClause(
            self,
            left_table=with_referenced,
            right_table=self._relation("all_cons_columns", "ref_cols"),
            join_type=JoinType.JOIN,
            condition=self._all_of(
                self._equals(
                    self._column("constraint_name", "ref_cons"),
                    self._column("constraint_name", "ref_cols"),
                ),
                self._equals(
                    self._column("owner", "ref_cons"), self._column("owner", "ref_cols")
                ),
                self._equals(
                    self._column("position", "cols"), self._column("position", "ref_cols")
                ),
            ),
        )
        return QueryStatement(
            self,
            select=[
                self._column("constraint_name", "cons", "CONSTRAINT_NAME"),
                self._column("delete_rule", "cons", "DELETE_RULE"),
                self._column("table_name", "ref_cons", "REFERENCED_TABLE_NAME"),
                self._column("column_name", "cols", "COLUMN_NAME"),
                self._column("column_name", "ref_cols", "REFERENCED_COLUMN_NAME"),
            ],
            from_=all_four,
            where=self._where(
                self._equals(
                    self._column("owner", "cons"), Literal(self, str(schema).upper())
                ),
                self._equals(
                    self._column("table_name", "cons"), Literal(self, str(table_name).upper())
                ),
                self._equals(
                    self._column("constraint_type", "cons"),
                    Literal(self, "R", inline=True),
                ),
            ),
            order_by=OrderByClause(
                self,
                [self._column("constraint_name", "cons"), self._column("position", "cols")],
            ),
        ).to_sql()

    def format_view_list_query_statement(self, expr: "ViewListQueryStatement") -> Tuple[str, tuple]:
        """Format view list query."""
        from rhosocial.activerecord.backend.expression import (
            OrderByClause, QueryStatement,
        )

        params = expr.get_params()
        schema = params.get("schema") or ""
        include_system = params.get("include_system", False)

        return QueryStatement(
            self,
            select=[
                self._column("view_name", alias="VIEW_NAME"),
                self._column("text_vc", alias="TEXT_VC"),
                self._column("read_only", alias="READ_ONLY"),
            ],
            from_=self._relation("all_views"),
            where=self._where(
                self._owner_predicate(schema, self._column("OWNER"), include_system)
            ),
            order_by=OrderByClause(self, [self._column("view_name")]),
        ).to_sql()

    def format_view_info_query_statement(self, expr: "ViewInfoQueryStatement") -> Tuple[str, tuple]:
        """Format single view information query."""
        from rhosocial.activerecord.backend.expression import (
            Literal, QueryStatement,
        )

        params = expr.get_params()
        view_name = params.get("view_name", "")
        schema = params.get("schema", "")

        return QueryStatement(
            self,
            select=[
                self._column("view_name", alias="VIEW_NAME"),
                self._column("text_vc", alias="VIEW_DEFINITION"),
                self._column("read_only", alias="READ_ONLY"),
            ],
            from_=self._relation("all_views"),
            where=self._where(
                self._equals(
                    self._column("owner"), Literal(self, str(schema).upper())
                ),
                self._equals(
                    self._column("view_name"), Literal(self, str(view_name).upper())
                ),
            ),
        ).to_sql()

    def format_trigger_list_query_statement(
        self, expr: "TriggerListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format trigger list query."""
        from rhosocial.activerecord.backend.expression import (
            Literal, OrderByClause, QueryStatement,
        )

        params = expr.get_params()
        schema = params.get("schema") or ""
        table_name = params.get("table_name")

        table_predicate = None
        if table_name:
            table_predicate = self._equals(
                self._column("table_name"), Literal(self, str(table_name).upper())
            )
        return QueryStatement(
            self,
            select=[
                self._column("trigger_name", alias="TRIGGER_NAME"),
                self._column("trigger_type", alias="TRIGGER_TYPE"),
                self._column("triggering_event", alias="TRIGGERING_EVENT"),
                self._column("table_name", alias="TABLE_NAME"),
                self._column("trigger_body", alias="TRIGGER_BODY"),
            ],
            from_=self._relation("all_triggers"),
            where=self._where(
                self._owner_predicate(schema, self._column("OWNER"), include_system=False),
                table_predicate,
            ),
            order_by=OrderByClause(self, [self._column("trigger_name")]),
        ).to_sql()

    # ------------------------------------------------------------------ #
    # Schema objects: sequences, synonyms, packages
    #
    # Three families core has no object for. They are stated here as trees in
    # the same shape as the table-list read above, so the dialect spells them
    # and no value is ever part of the text.
    # ------------------------------------------------------------------ #

    #: The five positions every sequence read projects, in dictionary order.
    _SEQUENCE_PROJECTION = (
        "sequence_owner", "sequence_name", "min_value", "max_value",
        "increment_by", "cycle_flag", "order_flag", "cache_size", "last_number",
    )

    #: The five positions every synonym read projects.
    _SYNONYM_PROJECTION = (
        "owner", "synonym_name", "table_owner", "table_name", "db_link",
    )

    #: The positions every package read projects. ``created`` is here
    #: because the parser reads it: dropping a projected column would quietly
    #: turn ``created_time`` into ``None`` rather than fail. ``owner`` is here
    #: for the same reason -- a package is an owned object, and a listing that
    #: does not ask for the owner reports an empty one.
    _PACKAGE_PROJECTION = (
        "owner", "object_name", "object_type", "status", "created", "last_ddl_time",
    )

    def format_sequence_list_query_statement(
        self, expr: "OracleSequenceListQueryStatement"
    ) -> Tuple[str, tuple]:
        """The sequences one owner has, in name order."""
        from rhosocial.activerecord.backend.expression import (
            Literal, OrderByClause, QueryStatement,
        )

        params = expr.get_params()
        owner = params.get("owner") or ""
        return QueryStatement(
            self,
            select=[self._column(c) for c in self._SEQUENCE_PROJECTION],
            from_=self._relation("all_sequences"),
            where=self._where(
                self._equals(self._column("sequence_owner"), Literal(self, owner))
            ),
            order_by=OrderByClause(self, [self._column("sequence_name")]),
        ).to_sql()

    def format_sequence_info_query_statement(
        self, expr: "OracleSequenceInfoQueryStatement"
    ) -> Tuple[str, tuple]:
        """One sequence, narrowed by name and owner."""
        from rhosocial.activerecord.backend.expression import Literal, QueryStatement

        params = expr.get_params()
        owner = params.get("owner") or ""
        sequence_name = params.get("sequence_name", "")
        return QueryStatement(
            self,
            select=[self._column(c) for c in self._SEQUENCE_PROJECTION],
            from_=self._relation("all_sequences"),
            where=self._where(
                self._equals(self._column("sequence_name"), Literal(self, sequence_name)),
                self._equals(self._column("sequence_owner"), Literal(self, owner)),
            ),
        ).to_sql()

    def format_synonym_list_query_statement(
        self, expr: "OracleSynonymListQueryStatement"
    ) -> Tuple[str, tuple]:
        """The synonyms one owner has, public ones included when asked for."""
        from rhosocial.activerecord.backend.expression import (
            Literal, OrderByClause, QueryStatement,
        )

        owner = expr.get_params().get("owner") or ""
        return QueryStatement(
            self,
            select=[self._column(c) for c in self._SYNONYM_PROJECTION],
            from_=self._relation("all_synonyms"),
            where=self._where(
                self._equals(self._column("owner"), Literal(self, owner))
            ),
            order_by=OrderByClause(self, [self._column("synonym_name")]),
        ).to_sql()

    def format_synonym_info_query_statement(
        self, expr: "OracleSynonymInfoQueryStatement"
    ) -> Tuple[str, tuple]:
        """One synonym, narrowed by name and owner."""
        from rhosocial.activerecord.backend.expression import Literal, QueryStatement

        params = expr.get_params()
        owner = params.get("owner") or ""
        synonym_name = params.get("synonym_name", "")
        return QueryStatement(
            self,
            select=[self._column(c) for c in self._SYNONYM_PROJECTION],
            from_=self._relation("all_synonyms"),
            where=self._where(
                self._equals(self._column("synonym_name"), Literal(self, synonym_name)),
                self._equals(self._column("owner"), Literal(self, owner)),
            ),
        ).to_sql()

    def format_package_list_query_statement(
        self, expr: "OraclePackageListQueryStatement"
    ) -> Tuple[str, tuple]:
        """The PL/SQL package specifications one owner has.

        ``OBJECT_TYPE`` is a fact about what is being asked for rather than
        caller data, so it rides the tree as an inline literal and stops the
        ``PACKAGE BODY`` row for the same name from being counted twice.
        """
        from rhosocial.activerecord.backend.expression import (
            Literal, OrderByClause, QueryStatement,
        )

        owner = expr.get_params().get("owner") or ""
        return QueryStatement(
            self,
            select=[self._column(c) for c in self._PACKAGE_PROJECTION],
            from_=self._relation("all_objects"),
            where=self._where(
                self._equals(self._column("owner"), Literal(self, owner)),
                self._equals(
                    self._column("object_type"),
                    Literal(self, "PACKAGE", inline=True),
                ),
            ),
            order_by=OrderByClause(self, [self._column("object_name")]),
        ).to_sql()
