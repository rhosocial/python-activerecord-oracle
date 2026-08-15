# src/rhosocial/activerecord/backend/impl/oracle/mixins/transaction.py
"""Oracle-specific transaction mixin."""

from typing import Dict, Tuple

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

    def supports_transaction_mode(self) -> bool:
        return True

    def supports_isolation_level_in_begin(self) -> bool:
        return False

    def supports_read_only_transaction(self) -> bool:
        return True

    def supports_deferrable_transaction(self) -> bool:
        return True

    def supports_savepoint(self) -> bool:
        return True

    def format_begin_transaction(self, expr) -> Tuple[str, tuple]:
        return ("", ())

    def format_commit_transaction(self, expr) -> Tuple[str, tuple]:
        return ("COMMIT", ())

    def format_rollback_transaction(self, expr) -> Tuple[str, tuple]:
        params = expr.get_params()
        savepoint = params.get("savepoint")
        if savepoint:
            return (f"ROLLBACK TO SAVEPOINT {self.format_identifier(savepoint)}", ())
        return ("ROLLBACK", ())

    def format_savepoint(self, expr) -> Tuple[str, tuple]:
        return (f"SAVEPOINT {self.format_identifier(expr.name)}", ())

    def format_release_savepoint(self, expr) -> Tuple[str, tuple]:
        return ("", ())

    def format_set_transaction(self, expr) -> Tuple[str, tuple]:
        params = expr.get_params()
        parts = ["SET TRANSACTION"]
        isolation = params.get("isolation_level")
        if isolation:
            parts.append(f"ISOLATION LEVEL {isolation}")
        mode = params.get("mode")
        if mode:
            parts.append(str(mode))
        if params.get("deferrable"):
            parts.append("DEFERRABLE")
        return (" ".join(parts), ())