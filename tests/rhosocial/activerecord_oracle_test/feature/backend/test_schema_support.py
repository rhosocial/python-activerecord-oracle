# tests/rhosocial/activerecord_oracle_test/feature/backend/test_schema_support.py
"""Tests for the schema capability declared on the Oracle dialect.

Oracle namespaces objects per user schema, so ``supports_schema()`` is True and
a name may be qualified with an owner. However schemas are created implicitly
with users: there is no CREATE/DROP SCHEMA namespace DDL on this server.

Two switches are asserted here and they answer different questions, owned by
two different mixins.
:class:`~rhosocial.activerecord.backend.impl.oracle.mixins.schema.OracleSchemaMixin`
holds the DDL side -- ``supports_schema`` and its siblings: does the engine have
schemas as a thing it can create and drop.
:class:`~rhosocial.activerecord.backend.impl.oracle.mixins.namespace.OracleNamespaceMixin`
holds the naming side -- ``supports_schema_qualification``, on ``NamespaceSupport``:
may a name carry an owner. Oracle answers True to both, and a single switch could
not report that, because MySQL answers the pair the other way round.

The switch placement itself is asserted in
``dialect/test_oracle_namespace_wiring.py``; what follows is the behaviour.
"""
from rhosocial.activerecord.backend.dialect.protocols import NamespaceSupport
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


class TestSchemaCapability:
    """Umbrella flag and granular schema DDL capability bits."""

    def _dialect(self) -> OracleDialect:
        return OracleDialect()

    def test_supports_schema_is_true(self):
        assert self._dialect().supports_schema() is True

    def test_names_may_be_owner_qualified(self):
        assert self._dialect().supports_schema_qualification() is True

    def test_implements_namespace_support_protocol(self):
        assert isinstance(self._dialect(), NamespaceSupport)

    def test_no_namespace_above_the_owner(self):
        """Oracle's only namespace is the owner, and nothing sits above it."""
        d = self._dialect()
        assert d.supports_catalog() is False
        assert d.supports_catalog_qualification() is False

    def test_no_schema_namespace_ddl(self):
        """Schemas follow users; the server has no CREATE/DROP SCHEMA DDL."""
        d = self._dialect()
        assert d.supports_create_schema() is False
        assert d.supports_drop_schema() is False

    def test_no_if_exists_variants(self):
        d = self._dialect()
        assert d.supports_schema_if_not_exists() is False
        assert d.supports_schema_if_exists() is False

    def test_no_cascade_support(self):
        assert self._dialect().supports_schema_cascade() is False
