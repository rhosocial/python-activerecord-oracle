# tests/rhosocial/activerecord_oracle_test/feature/backend/schema/test_schema_support.py
"""Tests for the SchemaSupport capability declared on the Oracle dialect.

Oracle namespaces objects per user schema, so schema-qualified names work and
``supports_schema()`` is True. However schemas are created implicitly with
users: there is no CREATE/DROP SCHEMA namespace DDL on this server.
"""
from rhosocial.activerecord.backend.dialect.protocols import SchemaSupport
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


class TestSchemaCapability:
    """Umbrella flag and granular schema DDL capability bits."""

    def _dialect(self) -> OracleDialect:
        return OracleDialect()

    def test_supports_schema_is_true(self):
        assert self._dialect().supports_schema() is True

    def test_implements_schema_support_protocol(self):
        assert isinstance(self._dialect(), SchemaSupport)

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
