# src/rhosocial/activerecord/backend/impl/oracle/mixins/identifier.py
"""Oracle identifier formatting mixin."""


class OracleIdentifierMixin:
    """Oracle identifier formatting with double-quote quoting.

    Oracle folds unquoted identifiers to uppercase. When quoting is
    enabled (the default), identifiers are uppercased, escaped, and
    wrapped in double quotes. When need_quote=False, the identifier
    is returned as-is without quoting or uppercasing.
    """

    def format_identifier(self, identifier: str, need_quote: bool = True) -> str:
        """Format identifier for Oracle with double-quote quoting."""
        if not need_quote:
            if self.is_reserved_word(identifier):
                import warnings
                from rhosocial.activerecord.backend.warnings import IdentifierQuotingWarning
                warnings.warn(
                    f"Identifier '{identifier}' is a reserved word in {self.name} "
                    f"and may cause SQL errors without quoting.",
                    IdentifierQuotingWarning,
                    stacklevel=2,
                )
            return identifier
        escaped = identifier.replace('"', '""')
        return f'"{escaped.upper()}"'


__all__ = ['OracleIdentifierMixin']
