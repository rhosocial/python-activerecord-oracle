# oracle/protocols/spatial_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class OracleSpatialSupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def supports_spatial_type(self) -> bool:
        ...  # pragma: no cover
    def supports_spatial_index(self) -> bool:
        ...  # pragma: no cover
    def supports_srs(self) -> bool:
        ...  # pragma: no cover
    def supports_3d_geometry(self) -> bool:
        ...  # pragma: no cover
    def supports_geodetic_index(self) -> bool:
        ...  # pragma: no cover
    def format_spatial_literal(self, geom: Any) -> str:
        ...  # pragma: no cover
    def format_spatial_function(self, name: str, *args) -> str:
        ...  # pragma: no cover
    def format_st_function(self, pg_name: str, *args) -> str:
        ...  # pragma: no cover
    def format_spatial_index_options(self, options) -> str:
        ...  # pragma: no cover
