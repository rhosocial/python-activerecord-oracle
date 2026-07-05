# src/rhosocial/activerecord/backend/impl/oracle/mixins/table_op.py
"""Oracle table DDL support mixin (capability flags for table operations).

The existing ``table.py`` contains ``OracleTableMixin`` which handles
table-level operations. This module provides the remaining ``supports_*``
capability checks that were previously inlined in the monolithic
``dialect.py``.
"""


class OracleTableCapabilityMixin:
    """Oracle table operation capability checks."""

    def supports_if_not_exists_table(self) -> bool:
        return False

    def supports_if_exists_table(self) -> bool:
        return False

    def supports_temporary_table(self) -> bool:
        return True