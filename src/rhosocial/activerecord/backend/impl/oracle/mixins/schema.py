# src/rhosocial/activerecord/backend/impl/oracle/mixins/schema.py
"""Oracle schema (owner) support: the DDL switches."""


class OracleSchemaMixin:
    """Oracle schema (owner) capability checks, DDL side only.

    One question lives here: does the engine have schemas as a thing it can
    create and drop? Oracle answers it -- schemas exist, but they come from
    ``CREATE USER``, so there is no ``CREATE SCHEMA`` and no ``DROP SCHEMA``.

    The *other* question -- may a name be qualified with an owner? -- is a
    naming question with a different owner, and it is answered on
    :class:`~...mixins.namespace.OracleNamespaceMixin`, which implements the
    core's :class:`~rhosocial.activerecord.backend.dialect.protocols.NamespaceSupport`.
    Keeping the two apart is not tidiness. MySQL has a schema it cannot create
    and a database it cannot qualify names with, so a single switch cannot say
    both, and Oracle -- which has schemas it cannot create and *can* qualify
    names with -- is that same disagreement pointed the other way.
    """

    def supports_schema(self) -> bool:
        """Oracle namespaces objects per user schema (schema.table qualification)."""
        return True

    def supports_create_schema(self) -> bool:
        """False: schemas come from CREATE USER, not from schema DDL."""
        return False

    def supports_drop_schema(self) -> bool:
        """False: Oracle has no DROP SCHEMA statement."""
        return False

    def supports_schema_if_not_exists(self) -> bool:
        return False

    def supports_schema_if_exists(self) -> bool:
        return False
