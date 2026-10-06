# src/rhosocial/activerecord/backend/impl/oracle/mixins/namespace.py
"""Oracle's override of the one place a qualified name becomes SQL.

The core :class:`~rhosocial.activerecord.backend.dialect.mixins.object_table_name.TableNameMixin`
already does everything Oracle needs for a table: render the namespace slots the
object carries, then append its own name. Oracle's whole difference is one extra
token on the end of the name, the ``@"DBLINK"`` suffix, so the override is
deliberately that small.

What this mixin does **not** do is invent a second path. The suffix belongs
after the name rather than beside it, which is why it is overridden on the table
formatter instead of on the namespace renderer: a database link qualifies *which
server resolves the name*, and Oracle resolves ``"SCOTT"."ORDERS"@"WORLD"`` as
one name, so it cannot be rendered as a prefix ahead of the table's own name.

Oracle is also the one engine in the tree whose namespace shape is the *reverse*
of the core default: it has a schema and no catalog, so the level that sits
outside a schema is always empty. That used to work by accident -- the core's
two-slot renderer simply did not emit the empty one -- and an accident is not a
statement of intent. :meth:`format_qualified_name` therefore states the shape
outright: one level, the owner, then the name, joined by
:attr:`~rhosocial.activerecord.backend.dialect.mixins.schema_namespace.NamespaceMixin.separator`.

The catalog level is not merely absent here, it is *refused*
upstream: :meth:`supports_catalog` is left at its ``False`` default, so
:meth:`~rhosocial.activerecord.backend.dialect.mixins.schema_namespace.NamespaceMixin.validate_namespace`
raises :class:`~rhosocial.activerecord.backend.dialect.exceptions.UnsupportedFeatureError`
for an object that carries one. Dropping it instead would resolve the name
against a different object, and the caller would never learn why.
"""

from typing import List, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.mixins.object_table_name import (
    TableNameMixin,
)
from rhosocial.activerecord.backend.expression.objects import Table

from ..expression.objects import OracleRemoteName

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.objects import SchemaObject

__all__ = ["OracleNamespaceMixin"]


class OracleNamespaceMixin(TableNameMixin):
    """Renders schema objects, including the Oracle ``@dblink`` name suffix."""

    def supports_schema_qualification(self) -> bool:
        """Whether a name may carry an owner: ``"SCOTT"."ORDERS"``.

        The naming-side switch, and the one :meth:`format_qualified_name`
        depends on. It lives here rather than on
        :class:`~...mixins.schema.OracleSchemaMixin` because naming and DDL are
        different questions that an engine can answer differently: Oracle
        namespaces per owner but has no ``CREATE SCHEMA`` at all.
        """
        return True

    def format_qualified_name(self, expr: "SchemaObject") -> Tuple[str, tuple]:
        """Spell the qualified name: the owner, then the name.

        One level, because the owner is Oracle's only namespace. There is no
        catalog branch here at all -- not an ``if`` that is always false -- so
        the shape cannot drift back into two slots when a caller supplies one:
        such an object is refused by :meth:`validate_namespace` before this is
        reached.

        Args:
            expr: The object being named. Its ``schema_name`` is the owner; its
                ``catalog_name`` is ``None`` for every object this dialect
                renders, and one that carries a value never arrives.

        Returns:
            A ``(sql, params)`` tuple; ``params`` is empty, because an
            identifier is never a bind parameter.
        """
        parts: List[str] = []
        if expr.schema_name:
            parts.append(
                self.format_identifier(expr.schema_name, expr.schema_need_quote)
            )
        parts.append(self.format_identifier(expr.name, expr.name_need_quote))
        return self.separator.join(parts), ()

    def format_table_object(self, obj: Table) -> Tuple[str, tuple]:
        """Render a table name, adding ``@"DBLINK"`` when the table is remote.

        Args:
            obj: A table. When it carries a ``dblink``
                (:class:`~...impl.oracle.expression.objects.OracleRemoteName`),
                the rendered name gains the ``@"DBLINK"`` suffix, because in
                Oracle that suffix is part of the name being resolved.

        Returns:
            A ``(sql, params)`` tuple; ``params`` is always empty, since an
            identifier is never a bind parameter.

        Raises:
            TypeError: ``obj`` is not a :class:`Table`. Another catalogue object
                carries its own ``format_method``, so it would render its own
                name and the statement would silently name the wrong object.
            UnsupportedFeatureError: ``obj`` carries a ``catalog_name``.
                Oracle has no namespace above the owner, so this is reported
                rather than silently dropped. ``OracleDialect`` implements
                :class:`~...dialect.protocols.NamespaceSupport` and declares
                ``supports_catalog()`` false, which is how it says so.
        """
        if not isinstance(obj, Table):
            raise TypeError(
                f"format_table_object expects a Table, "
                f"got {type(obj).__name__}"
            )
        name_sql, params = super().format_table_object(obj)
        if isinstance(obj, OracleRemoteName) and obj.dblink:
            name_sql = (
                f"{name_sql}@{self.format_identifier(obj.dblink, obj.dblink_need_quote)}"
            )
        return name_sql, params
