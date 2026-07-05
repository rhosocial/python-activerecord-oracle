# src/rhosocial/activerecord/backend/impl/oracle/mixins/sequence.py
"""Oracle sequence DDL support mixin."""


class OracleSequenceMixin:
    """Oracle sequence management capability checks."""

    def supports_create_sequence(self) -> bool:
        return True

    def supports_drop_sequence(self) -> bool:
        return True