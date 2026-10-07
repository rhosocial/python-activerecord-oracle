# src/rhosocial/activerecord/backend/impl/oracle/mixins/transaction.py
"""Oracle-specific transaction mixin."""

from typing import Dict, Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
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
        """Format BEGIN for Oracle, which begins transactions implicitly.

        The statement itself renders as the empty string; a requested
        ``deferrable`` / ``not_deferrable`` mode has no Oracle spelling and is
        refused by name instead of being dropped.

        Raises:
            UnsupportedFeatureError: if a deferrable transaction mode was
                requested.
        """
        params = expr.get_params()
        if params.get("deferrable"):
            raise UnsupportedFeatureError(
                self.name,
                "BEGIN DEFERRABLE",
                "Oracle begins transactions implicitly; there is no DEFERRABLE "
                "BEGIN spelling.",
            )
        if params.get("not_deferrable"):
            raise UnsupportedFeatureError(
                self.name,
                "BEGIN NOT DEFERRABLE",
                "Oracle begins transactions implicitly; there is no NOT "
                "DEFERRABLE BEGIN spelling.",
            )
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
        """Format SET TRANSACTION for Oracle.

        ``deferrable`` renders the declared ``DEFERRABLE`` spelling; the
        explicit negative has no Oracle form and is refused by name rather
        than dropped.

        Raises:
            UnsupportedFeatureError: if ``not_deferrable`` was requested.
        """
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
        if params.get("not_deferrable"):
            raise UnsupportedFeatureError(
                self.name,
                "SET TRANSACTION NOT DEFERRABLE",
                "Oracle's SET TRANSACTION has no NOT DEFERRABLE spelling.",
            )
        return (" ".join(parts), ())