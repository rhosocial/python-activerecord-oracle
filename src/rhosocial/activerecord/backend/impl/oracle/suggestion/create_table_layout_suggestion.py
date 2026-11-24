# src/rhosocial/activerecord/backend/impl/oracle/suggestion/create_table_layout_suggestion.py
"""Oracle's answer to the core create-table layout suggestion protocol."""

from rhosocial.activerecord.backend.suggestion.definitions import CreateTableLayoutSuggestion
from rhosocial.activerecord.backend.suggestion.protocols import CreateTableLayoutSuggestionSupport


class OracleCreateTableLayoutSuggestionMixin(CreateTableLayoutSuggestionSupport):
    """Report how Oracle lays out CREATE TABLE statements.

    Oracle defines indexes in separate ``CREATE INDEX`` statements and has no
    inline table comment, so the layout is ``"split"``. The individual flags
    are read from the dialect's capability probes rather than hard-coded, so
    they stay in sync with the rendering gates.
    """

    def suggest_create_table_layout(self) -> CreateTableLayoutSuggestion:
        inline_index = self.supports_inline_index()
        inline_comment = self.supports_table_comment()
        return CreateTableLayoutSuggestion(
            strategy="single" if inline_index and inline_comment else "split",
            inline_index=inline_index,
            inline_comment=inline_comment,
            state="actions",
        )


__all__ = ["OracleCreateTableLayoutSuggestionMixin"]
