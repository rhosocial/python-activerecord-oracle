# src/rhosocial/activerecord/backend/impl/oracle/mixins/pivot.py
"""Oracle PIVOT / UNPIVOT support mixin."""


class OraclePivotMixin:
    """Oracle PIVOT / UNPIVOT capability checks (11g+)."""

    def supports_pivot(self) -> bool:
        return self.version >= (11, 0, 0)

    def supports_unpivot(self) -> bool:
        return self.version >= (11, 0, 0)

    def supports_pivot_xml(self) -> bool:
        return self.version >= (11, 0, 0)