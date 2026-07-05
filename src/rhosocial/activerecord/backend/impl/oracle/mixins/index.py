# src/rhosocial/activerecord/backend/impl/oracle/mixins/index.py
"""Oracle index DDL support mixin."""


class OracleIndexMixin:
    """Oracle index management capability checks."""

    def supports_create_index(self) -> bool:
        return True

    def supports_drop_index(self) -> bool:
        return True

    def supports_unique_index(self) -> bool:
        return True

    def supports_index_if_not_exists(self) -> bool:
        return False

    def supports_index_if_exists(self) -> bool:
        return False