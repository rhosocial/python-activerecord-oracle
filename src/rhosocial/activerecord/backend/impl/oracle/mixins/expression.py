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
"""

from typing import Tuple, TYPE_CHECKING

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

    def format_connect_by(self, expr: "BaseExpression") -> Tuple[str, tuple]:
        return expr.to_sql()

    def format_pivot(self, expr: "BaseExpression") -> Tuple[str, tuple]:
        return expr.to_sql()

    def format_unpivot(self, expr: "BaseExpression") -> Tuple[str, tuple]:
        return expr.to_sql()

    def format_hint(self, expr: "BaseExpression") -> Tuple[str, tuple]:
        return expr.to_sql()

    def format_for_update(self, expr: "BaseExpression") -> Tuple[str, tuple]:
        return expr.to_sql()

    def format_query_statement(self, expr: "BaseExpression") -> Tuple[str, tuple]:
        """Oracle SELECT builder.

        Oracle (unlike PostgreSQL/MySQL/SQLite) rejects ``SELECT *, expr AS x``
        with ``ORA-00923: FROM keyword not found where expected``. The wildcard
        must be qualified with a table qualifier (``t.*``) whenever it shares
        the select-list with other expressions — which is exactly the shape
        produced by ActiveRecord's ``derived=True`` query option.

        This override runs the core builder, then post-processes the resulting
        SQL by qualifying any bare ``*`` select-item with the FROM source's
        table alias (or name, when no alias is present). The rewrite is only
        applied when a bare ``*`` coexists with other select items, so single-
        column selects and plain ``SELECT *`` statements are untouched.
        """
        sql, params = super().format_query_statement(expr)

        if len(expr.select) <= 1:
            return sql, params

        from_ = getattr(expr, "from_", None)
        qualifier = None
        if from_ is not None and not isinstance(from_, (str, list)):
            qualifier = getattr(from_, "alias", None) or getattr(from_, "name", None)

        if qualifier is None:
            return sql, params

        try:
            upper_sql = sql.upper()
            sel_idx = upper_sql.find("SELECT")
            if sel_idx == -1:
                return sql, params
            # Skip past optional DISTINCT/ALL modifier to reach the select list.
            from_idx = upper_sql.find(" FROM ", sel_idx + 6)
            if from_idx == -1:
                return sql, params
            select_body = sql[sel_idx + 6:from_idx]
            upper_body = select_body.upper()
            items = [s.strip() for s in select_body.split(",")]
            upper_items = [s.strip() for s in upper_body.split(",")]

            wildcard_positions = [
                i for i, item in enumerate(upper_items) if item == "*"
            ]
            if not wildcard_positions:
                return sql, params

            qualifier_sql = self.format_identifier(qualifier)
            for i in wildcard_positions:
                items[i] = f"{qualifier_sql}.*"

            new_select_body = ", ".join(items)
            return sql[:sel_idx + 6] + " " + new_select_body + sql[from_idx:], params
        except Exception:
            return sql, params