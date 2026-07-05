# src/rhosocial/activerecord/backend/impl/oracle/mixins/hint.py
"""Oracle query hint support mixin."""


class OracleHintMixin:
    """Oracle optimizer hint capability checks."""

    def supports_query_hints(self) -> bool:
        return True

    def supports_parallel_hint(self) -> bool:
        return True

    def supports_index_hint(self) -> bool:
        return True

    def supports_leading_hint(self) -> bool:
        return True

    def supports_optimizer_hints(self) -> bool:
        return True