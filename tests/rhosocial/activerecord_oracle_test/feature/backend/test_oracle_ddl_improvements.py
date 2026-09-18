# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_ddl_improvements.py
"""Tests for Oracle DDL improvements: capability gating, UnsupportedFeatureError."""
import pytest
from unittest.mock import patch, PropertyMock

from rhosocial.activerecord.backend.expression import (
    Column,
    TableExpression,
    QueryExpression,
    CreateViewExpression,
    DropViewExpression,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class TestOracleViewCapabilityGating:
    """Tests for Oracle VIEW DDL capability gating."""

    def test_create_or_replace_view_supported(self):
        """Oracle supports CREATE OR REPLACE VIEW."""
        dialect = OracleDialect()
        assert dialect.supports_create_or_replace_view() is True

    def test_drop_view_if_exists_not_supported(self):
        """Oracle does not support DROP VIEW IF EXISTS."""
        dialect = OracleDialect()
        assert dialect.supports_if_exists_view() is False

    def test_materialized_view_supported(self):
        """Oracle supports materialized views."""
        dialect = OracleDialect()
        assert dialect.supports_materialized_view() is True


class TestOracleColumnCapabilityGating:
    """Tests for Oracle COLUMN DDL capability gating."""

    def test_foreign_key_on_delete_supported(self):
        """Oracle supports FK ON DELETE."""
        dialect = OracleDialect()
        assert dialect.supports_foreign_key_on_delete() is True

    def test_foreign_key_on_update_not_supported(self):
        """Oracle does not support FK ON UPDATE."""
        dialect = OracleDialect()
        assert dialect.supports_foreign_key_on_update() is False

    def test_check_constraint_supported(self):
        """Oracle supports CHECK constraints."""
        dialect = OracleDialect()
        assert dialect.supports_check_constraint() is True


class TestOracleSchemaCapabilityGating:
    """Tests for Oracle SCHEMA DDL capability gating."""

    def test_create_schema_not_supported(self):
        """Oracle does not support CREATE SCHEMA (uses users)."""
        dialect = OracleDialect()
        assert dialect.supports_create_schema() is False

    def test_drop_schema_not_supported(self):
        """Oracle does not support DROP SCHEMA."""
        dialect = OracleDialect()
        assert dialect.supports_drop_schema() is False
