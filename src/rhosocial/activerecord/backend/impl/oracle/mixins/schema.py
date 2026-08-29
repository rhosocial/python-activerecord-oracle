# src/rhosocial/activerecord/backend/impl/oracle/mixins/schema.py
"""Oracle schema DDL support mixin."""


class OracleSchemaMixin:
    """Oracle schema management capability checks."""

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