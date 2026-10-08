# oracle/protocols/identifier_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.

Note what is deliberately *absent*: identifier rendering itself.
``format_identifier`` is a primitive every dialect provides and no protocol
declares -- a Protocol describes what a consumer may rely on, and a protocol
that merely restates the primitive would be a second, competing definition of
it. Slot-level name rendering is likewise not declared here: each schema object
declares its own ``format_<kind>_object``, and a row source is read through
``format_named_relation``, so a name is rendered by exactly one method per kind
of thing named.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleIdentifierSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def format_column(self, name: str, table: Optional[str]=None, alias: Optional[str]=None, schema_name: Optional[str]=None) -> Tuple[str, Tuple]:
        ...  # pragma: no cover
