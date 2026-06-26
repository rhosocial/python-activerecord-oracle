# src/rhosocial/activerecord/backend/impl/oracle/mixins/transaction.py
"""Oracle-specific transaction mixin."""

from typing import Dict

from rhosocial.activerecord.backend.transaction import IsolationLevel


class OracleTransactionMixin:
    """Mixin providing Oracle-specific transaction handling."""

    _ISOLATION_LEVELS: Dict[IsolationLevel, str] = {
        IsolationLevel.READ_COMMITTED: "READ COMMITTED",
        IsolationLevel.SERIALIZABLE: "SERIALIZABLE",
    }

    @classmethod
    def get_isolation_level_string(cls, level: IsolationLevel) -> str:
        return cls._ISOLATION_LEVELS.get(level, "READ COMMITTED")

    def supports_isolation_level(self, level: IsolationLevel) -> bool:
        return level in self._ISOLATION_LEVELS