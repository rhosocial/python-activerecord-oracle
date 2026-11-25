# src/rhosocial/activerecord/backend/impl/oracle/suggestion/_change_common.py
"""Shared readers for the four migration change-point suggestions.

Core freezes a change point's declaration projections with ``_freeze``
(``migration/schema_change.py``): an expression becomes
``("params", ((name, value), ...))`` and everything else is either a primitive
or a ``("repr", text)`` fallback. These readers decode that shape so a
suggestion can decide, from the point alone, whether a real node can be built.
The projections deliberately carry no expression class, so a frozen-only point
cannot reconstruct a concrete branch/type (e.g. an ADD CONSTRAINT) and must be
declined rather than guessed. The enriched points instead carry the model's live
declaration alongside the projection; when a suggestion needs the branch class
it reads that field and still declines a point that does not provide one.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict, Optional

__all__ = ["answer", "projection_name", "projection_params"]


def projection_params(value: Any) -> Optional[Dict[str, Any]]:
    """Decode a ``_freeze`` ``("params", ((k, v), ...))`` projection.

    Returns ``None`` for any other shape -- a primitive, a ``("repr", ...)``
    fallback, or a malformed value -- which is how a suggestion tells that the
    point did not carry a reconstructable declaration.
    """
    if (
        isinstance(value, tuple)
        and len(value) == 2
        and value[0] == "params"
        and isinstance(value[1], tuple)
        and all(isinstance(item, tuple) and len(item) == 2 for item in value[1])
    ):
        return {key: item_value for key, item_value in value[1]}
    return None


def projection_name(params: Optional[Dict[str, Any]]) -> Optional[str]:
    """Return a non-empty declaration name from a decoded projection."""
    if not params:
        return None
    name = params.get("name")
    if isinstance(name, str) and name.strip():
        return name
    return None


def answer(point: Any, operations: Any) -> Any:
    """Return ``point`` with ``operations`` filled, preserving its context.

    The four change-point values are frozen dataclasses, so a new instance is
    returned; a bare label (the legacy shape) has no ``operations`` slot and is
    passed through unchanged.
    """
    if dataclasses.is_dataclass(point):
        return dataclasses.replace(point, operations=operations)
    return point
