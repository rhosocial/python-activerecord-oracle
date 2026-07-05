# src/rhosocial/activerecord/backend/impl/oracle/mixins/hierarchical.py
"""Oracle hierarchical query support mixin."""


class OracleHierarchicalMixin:
    """Oracle CONNECT BY / hierarchical query capability checks."""

    def supports_hierarchical_queries(self) -> bool:
        return True

    def supports_connect_by(self) -> bool:
        return True

    def supports_level_pseudo_column(self) -> bool:
        return True

    def supports_connect_by_root(self) -> bool:
        return True

    def supports_sys_connect_by_path(self) -> bool:
        return True