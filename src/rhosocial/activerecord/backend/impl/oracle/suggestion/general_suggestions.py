# src/rhosocial/activerecord/backend/impl/oracle/suggestion/general_suggestions.py
"""Oracle answers to the core's general suggestion protocols."""

from typing import Optional

from rhosocial.activerecord.backend.suggestion.definitions import (
    ConcatenationFunctionSuggestion,
)
from rhosocial.activerecord.backend.suggestion.protocols import (
    StringConcatenationSuggestionSupport,
)


class OracleStringConcatenationSuggestionMixin(StringConcatenationSuggestionSupport):
    """Oracle concatenates strings with the portable ``||`` operator.

    Returning ``function=None`` tells core's ``format_concatenation_operation``
    to use the ``||`` spelling, which is what Oracle's grammar accepts.
    """

    def suggest_concatenation_function(self) -> Optional[str]:
        return ConcatenationFunctionSuggestion(function=None)


__all__ = ["OracleStringConcatenationSuggestionMixin"]
