# src/rhosocial/activerecord/backend/impl/oracle/mixins/index.py
"""Oracle index DDL support mixin."""

from .if_exists import supports_statement_if_exists


class OracleIndexMixin:
    """Oracle index management capability checks."""

    def supports_create_index(self) -> bool:
        return True

    def supports_drop_index(self) -> bool:
        return True

    def supports_drop_index_on_table(self) -> bool:
        return False

    def supports_unique_index(self) -> bool:
        return True

    def supports_index_if_not_exists(self) -> bool:
        """Whether CREATE INDEX accepts the native ``IF NOT EXISTS``.

        Oracle added it with 19c Release Update 19.28 and ships it in the 23ai
        base release; 21c has no such spelling. See
        :func:`~.if_exists.supports_statement_if_exists`.
        """
        return supports_statement_if_exists(self)

    def supports_index_if_exists(self) -> bool:
        """Whether DROP INDEX accepts the native ``IF EXISTS``.

        Same two anchors as :meth:`supports_index_if_not_exists`.
        """
        return supports_statement_if_exists(self)
