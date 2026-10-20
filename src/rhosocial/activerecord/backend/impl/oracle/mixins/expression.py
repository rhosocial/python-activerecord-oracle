# src/rhosocial/activerecord/backend/impl/oracle/mixins/expression.py
"""Oracle expression-formatting delegation mixin.

Routes Oracle-specific expression formatters (connect_by, pivot,
unpivot, hint, for_update) back through the expression's own
``to_sql(self)`` dispatch, which is the standard pattern used by
the core ``ExpressionMixin``.

``format_named_relation`` is the one override here that is not a pure
delegation. The core implementation renders the relation through the
relation's own ``format_<kind>_object``, appends the engine-specific
time-travel clause and then the alias; Oracle adds one thing between the
name and the alias -- the flashback clause.

``format_query_statement`` is the second. Oracle requires a select-list
``*`` that shares the list with other items to be qualified (``t.*``);
this backend binds the qualifier to the ``AllColumnsWildcard`` node and
lets the ordinary wildcard formatter render it, rather than rewriting the
finished SQL text.
"""

import copy
from typing import TYPE_CHECKING, Optional, Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.core import AllColumnsWildcard
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef

from ..expression.sources import OracleNamedRelationRef

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.bases import BaseExpression


class OracleExpressionMixin:
    """Oracle-specific expression formatting that delegates to ``to_sql``."""

    def format_named_relation(self, ref: "BaseExpression") -> Tuple[str, tuple]:
        """Format a ``FROM`` reference to a named relation for Oracle.

        The relation's name, the ``@"DBLINK"`` suffix and the alias all come
        from the shared object-naming path; Oracle adds exactly one thing the
        core has no slot for: the flashback clause ``AS OF SCN 1234567`` /
        ``VERSIONS BETWEEN ...``, which reads the relation as it existed at a
        past moment. That clause belongs to the *statement*, so it travels on
        the reference and is inserted between the name and the alias.

        Alias rendering keeps the core's ``AS`` separator. Oracle accepts
        ``FROM t AS x``, so there is no reason for this backend to be the one
        engine whose ``FROM`` clauses read differently from every other
        backend's for the same input.

        Args:
            ref: The row source to render. An
                :class:`~...impl.oracle.expression.sources.OracleNamedRelationRef`
                may carry a flashback clause; any other named-relation source is
                rendered entirely by the core implementation.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        if not isinstance(ref, OracleNamedRelationRef) or ref.flashback is None:
            return super().format_named_relation(ref)

        name_sql, params = ref.relation.to_sql()
        flashback_sql, flashback_params = ref.flashback.to_sql()
        if ref.alias:
            name_sql = (
                f"{name_sql} {flashback_sql}{self.source_alias_separator()}"
                f"{self.format_identifier(ref.alias, ref.alias_need_quote)}"
            )
        else:
            name_sql = f"{name_sql} {flashback_sql}"
        return name_sql, tuple(params) + tuple(flashback_params)

    def format_query_statement(self, expr: "BaseExpression") -> Tuple[str, tuple]:
        """Oracle SELECT builder that qualifies the select-list wildcard.

        Oracle (unlike PostgreSQL/MySQL/SQLite) rejects ``SELECT *, expr AS x``
        with ``ORA-00923: FROM keyword not found where expected``: a ``*`` that
        shares the select list with another item must be written ``t.*``. That
        is exactly the shape ActiveRecord's ``derived=True`` produces --
        ``select_columns`` starts as one bare ``AllColumnsWildcard`` and the
        derived expressions are appended to it.

        The qualification happens **on the node**, before the statement is
        rendered: each bare wildcard is replaced by the same node bound to the
        FROM source, and the ordinary ``format_all_columns_wildcard`` path
        renders it. The rendered SQL is never parsed or sliced afterwards, so
        a select item whose text contains a comma or an asterisk
        (``CONCAT('x, y')``, a quoted identifier, an inline ``*`` inside a
        function call) cannot be mistaken for a separator or for the wildcard.

        A lone wildcard keeps its bare spelling -- ``SELECT *`` is legal Oracle
        and qualifying it would change nothing for one source while breaking
        ``SELECT *`` over a join.

        Args:
            expr: The ``QueryStatement`` being rendered.

        Returns:
            Tuple of (SQL string, parameters tuple).

        Raises:
            UnsupportedFeatureError: A bare ``*`` shares the select list and
                the FROM clause does not name a single, qualifiable source
                (a join without an alias, several comma-separated sources, no
                source at all). Oracle needs one qualifier there and the
                statement does not say which, so the query is refused instead
                of guessed.
        """
        wildcard_relation = self._select_list_wildcard_relation(expr)
        if wildcard_relation is None:
            return super().format_query_statement(expr)

        # Bind the qualifier to the node and leave the caller's statement
        # untouched: the select list is shared with the query object that
        # built it, and rendering must not rewrite it.
        bound_select = [
            (
                AllColumnsWildcard(expr.dialect, relation=wildcard_relation)
                if isinstance(item, AllColumnsWildcard) and item.relation is None
                else item
            )
            for item in expr.select
        ]
        bound_statement = copy.copy(expr)
        bound_statement.select = bound_select
        return super().format_query_statement(bound_statement)

    def _select_list_wildcard_relation(self, expr: "BaseExpression") -> Optional[NamedRelationRef]:
        """The row source a bare select-list ``*`` must be qualified by.

        Returns ``None`` when no bare wildcard shares the select list -- either
        there is none, or the select list is the wildcard alone, which Oracle
        accepts unqualified.
        """
        shares_select_list = [
            item
            for item in expr.select
            if isinstance(item, AllColumnsWildcard) and item.relation is None
        ]
        if not shares_select_list or len(expr.select) == 1:
            return None
        return self._row_source_qualifier(expr.from_, expr.dialect)

    def _row_source_qualifier(self, from_, dialect) -> NamedRelationRef:
        """Build the reference that names *from_* in a select list.

        The qualifier is the spelling the query itself uses for the source --
        the alias when there is one, the relation's own name otherwise -- so it
        is read off the source node rather than from the rendered FROM text.
        A source is qualified by identity, never by position: when identity is
        not available the caller is told to write the wildcard out, because
        guessing which of several sources a ``*`` spans is exactly what Oracle
        refuses to do.

        Args:
            from_: The statement's FROM clause.
            dialect: The dialect the qualifier is rendered with.

        Returns:
            A ``NamedRelationRef`` naming the single source.

        Raises:
            UnsupportedFeatureError: The FROM clause has no single nameable
                source.
        """
        if isinstance(from_, NamedRelationRef):
            # A flashback clause says *when* the relation is read, not *which*
            # relation it is, so it stays on the FROM reference and out of the
            # qualifier.
            return NamedRelationRef(
                dialect,
                from_.relation,
                alias=from_.alias,
                alias_need_quote=from_.alias_need_quote,
            )
        if isinstance(from_, Table):
            return NamedRelationRef(dialect, from_)
        if isinstance(from_, str):
            return NamedRelationRef(dialect, Table(dialect, from_))

        # Any other single source is still nameable through its alias: a
        # derived table, a VALUES list, a table function or an aliased join
        # renders as ``... <alias>`` and is referred to by that alias alone.
        alias = getattr(from_, "alias", None)
        if alias:
            return NamedRelationRef(
                dialect,
                Table(dialect, alias),
                alias=alias,
                alias_need_quote=getattr(from_, "alias_need_quote", True),
            )

        source = "no FROM source" if from_ is None else f"a {type(from_).__name__} source"
        raise UnsupportedFeatureError(
            self.name,
            f"an unqualified '*' beside other select items over {source}",
            "Oracle needs exactly one table qualifier for '*' and this FROM "
            "clause does not name a single, referenceable relation. Write the "
            "wildcard explicitly, e.g. AllColumnsWildcard(dialect, "
            "relation=NamedRelationRef(dialect, Table(dialect, 't'), alias='t')) "
            "for 't.*'.",
        )
