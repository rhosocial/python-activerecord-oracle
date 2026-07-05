# src/rhosocial/activerecord/backend/impl/oracle/mixins/optimizer_hint.py
from typing import Optional, Tuple


class OracleOptimizerHintMixin(object):
    """Oracle optimizer hint implementation.

    Oracle supports rich optimizer hints via ``/*+ ... */`` comments,
    including INDEX, LEADING, PARALLEL, USE_NL, USE_HASH, HASH_JOIN,
    FIRST_ROWS(n), ALL_ROWS, OPTIMIZER_FEATURES_ENABLE,
    GATHER_PLAN_STATISTICS, and MONITOR.
    """

    def supports_optimizer_hint(self) -> bool:
        return True

    def supports_parallel_hint(self) -> bool:
        return True

    def supports_index_hint(self) -> bool:
        return True

    def supports_leading_hint(self) -> bool:
        return True

    def supports_optimizer_hints(self) -> bool:
        return True

    def supports_hint_with_arguments(self) -> bool:
        return True

    def format_optimizer_hint(
        self,
        name: str,
        args: Tuple = (),
        kwargs: Optional[dict] = None,
    ) -> str:
        """Format a single Oracle optimizer hint ``/*+ NAME(args) */``.

        Keyword arguments are emitted as ``key value`` pairs inside the
        parentheses, following Oracle's hint argument convention.
        """
        parts = [str(a) for a in args]
        if kwargs:
            for key, value in kwargs.items():
                parts.append(f"{key} {value}")
        inner = " ".join(parts)
        if inner:
            return f"/*+ {name.upper()}({inner}) */"
        return f"/*+ {name.upper()} */"

    def format_multiple_hints(self, *hints: str) -> str:
        """Format multiple pre-formatted hints into one comment block."""
        return f"/*+ {' '.join(hints)} */"
