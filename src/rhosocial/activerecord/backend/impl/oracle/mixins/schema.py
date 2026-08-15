# src/rhosocial/activerecord/backend/impl/oracle/mixins/schema.py
"""Oracle schema DDL support mixin."""


class OracleSchemaMixin:
    """Oracle schema management capability checks."""

    def supports_create_schema(self) -> bool:
        return True

    def supports_drop_schema(self) -> bool:
        return True

    def supports_schema_if_not_exists(self) -> bool:
        return False

    def supports_schema_if_exists(self) -> bool:
        return False