# src/rhosocial/activerecord/backend/impl/oracle/protocols/functions_capabilities.py
"""Capability protocol declarations for Oracle's derived function formatters.

Functional-group principle: every public format_*/supports_* on a backend
mixin is declared here so dialect users can program against the capability
contract. Regenerate via scripts/p7_generate_protocols.py when mixins gain
new public rendering methods.
"""

from typing import TYPE_CHECKING, Tuple

from typing import Protocol

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.bases import BaseExpression


class OracleFunctionFormatSupport(Protocol):
    """Capability protocol for Oracle's derived function-call formatters.

    One entry point per derived node kind in
    ``impl.oracle.expression.functions``; the generic
    ``format_function_call`` remains the base's and needs no declaration
    here.
    """

    def format_oracle_listagg_function(
        self, expr: "BaseExpression"
    ) -> Tuple[str, tuple]:
        ...  # pragma: no cover

    def format_oracle_percentile_cont_function(
        self, expr: "BaseExpression"
    ) -> Tuple[str, tuple]:
        ...  # pragma: no cover

    def format_oracle_percentile_disc_function(
        self, expr: "BaseExpression"
    ) -> Tuple[str, tuple]:
        ...  # pragma: no cover

    def format_oracle_json_value_function(
        self, expr: "BaseExpression"
    ) -> Tuple[str, tuple]:
        ...  # pragma: no cover

    def format_oracle_json_query_function(
        self, expr: "BaseExpression"
    ) -> Tuple[str, tuple]:
        ...  # pragma: no cover

    def format_oracle_json_table_function(
        self, expr: "BaseExpression"
    ) -> Tuple[str, tuple]:
        ...  # pragma: no cover
