# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/mixins/test_shared_render_helper_guard.py
"""Guard: no private render helper is shared by two public formatters.

The backend renders one expression kind per ``format_*`` method. When two
formatters call one private ``_render_*`` / ``_format_*`` helper for part of
their output, two kinds share that part of their rendering, and the next change
to one grammar lands in the other by accident. The fix is to inline the shared
piece into each formatter, which is what ``mixins/functions.py`` did for its
argument assembly: LISTAGG and JSON_TABLE each assemble their own arguments now.

A helper that legitimately serves several formatters is allow-listed, and only
in one of two categories:

* a **pure primitive utility** -- it renders a scalar piece with no grammar of
  its own (a quoting rule, a literal spelling), or
* a **sub-node renderer** -- it renders a node the callers *contain*, through
  that node's own object protocol, so the callers still own their own grammar.

The allow-list is asserted against the scan in both directions: every shared
helper must be allow-listed, and every allow-list entry must still name a helper
that is actually shared. The scan reads the ``mixins`` package's own sources, so
a new shared helper fails the test by name without anyone having to remember
this file.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Dict, Set, Tuple

from rhosocial.activerecord.backend.impl.oracle import mixins as oracle_mixins
from rhosocial.activerecord.backend.impl.oracle.mixins.functions import (
    OracleFunctionFormatMixin,
)

MIXINS_DIR = Path(oracle_mixins.__file__).parent

RENDER_PREFIXES = ("_render_", "_format_")
PUBLIC_PREFIX = "format_"

# Every entry below is a helper the scan finds shared by two or more public
# formatters in one class, with the reason it may stay shared. The reason must
# name one of the two allowed categories.
ALLOW_LIST: Dict[str, str] = {
    "ddl_type.py:OracleTypeDDLMixin._format_type_name": (
        "Sub-node renderer: it delegates to the Type object's own to_sql(), so "
        "the CREATE/ALTER/DROP TYPE formatters all name the same object the "
        "same way instead of each concatenating a name and an owner."
    ),
}
# ``_format_vector_operand_raw`` is deliberately not listed: since the vector
# distance value renderer became ``build_vector_distance`` (it is not a dispatch
# target), only ``format_vector_operand_expression`` remains a public caller.


def _self_calls(node: ast.AST) -> Set[str]:
    """The attribute names this method body calls on ``self``."""
    names: Set[str] = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute):
            target = sub.func.value
            if isinstance(target, ast.Name) and target.id == "self":
                names.add(sub.func.attr)
    return names


def _shared_render_helpers() -> Dict[str, Tuple[str, ...]]:
    """Map ``file:Class.method`` to the public formatters that call it.

    Only direct calls from inside a public ``format_*`` method of the same
    class count: that is the shape where two rendered kinds share rendering.
    """
    shared: Dict[str, Tuple[str, ...]] = {}
    for path in sorted(MIXINS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for cls in ast.walk(tree):
            if not isinstance(cls, ast.ClassDef):
                continue
            methods = [
                node
                for node in cls.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            ]
            public = [
                node
                for node in methods
                if node.name.startswith(PUBLIC_PREFIX) and not node.name.startswith("_")
            ]
            private = [node.name for node in methods if node.name.startswith(RENDER_PREFIXES)]
            for helper in sorted(private):
                callers = sorted(node.name for node in public if helper in _self_calls(node))
                if len(callers) >= 2:
                    shared[f"{path.name}:{cls.name}.{helper}"] = tuple(callers)
    return shared


def test_no_private_render_helper_is_shared_by_two_public_formatters() -> None:
    shared = _shared_render_helpers()
    offenders = sorted(set(shared) - set(ALLOW_LIST))
    assert offenders == [], (
        "A private `_render_*` / `_format_*` helper is called by two or more "
        "public `format_*` methods in the same class, so two expression kinds "
        "share part of their rendering. Inline the piece into each formatter, "
        "or allow-list the helper in ALLOW_LIST with a written reason naming it "
        "a pure primitive utility or a sub-node renderer.\n"
        f"Offenders: {offenders}\n"
        f"Callers: { {name: shared[name] for name in offenders} }"
    )


def test_allow_list_matches_reality() -> None:
    shared = _shared_render_helpers()
    stale = sorted(set(ALLOW_LIST) - set(shared))
    assert stale == [], (
        "The allow-list names helpers that are no longer shared; drop the "
        f"entries so the guard stays exact: {stale}"
    )
    for name, reason in ALLOW_LIST.items():
        assert reason.strip(), f"{name}: the allow-list entry needs a written reason"
        lowered = reason.lower()
        assert any(category in lowered for category in ("primitive", "sub-node")), (
            f"{name}: the reason must name its category -- a pure primitive "
            f"utility or a sub-node renderer -- got {reason!r}"
        )


def test_function_formatters_assemble_their_own_arguments() -> None:
    """The defect this guard was added for: LISTAGG and JSON_TABLE shared
    their argument assembly through ``_render_call_args``."""
    shared = _shared_render_helpers()
    function_entries = {
        name: callers for name, callers in shared.items() if name.startswith("functions.py:")
    }
    assert function_entries == {}, (
        "Two Oracle function formatters share a private render helper again; "
        f"each formatter assembles its own argument list: {function_entries}"
    )
    assert not hasattr(OracleFunctionFormatMixin, "_render_call_args"), (
        "the removed shared argument assembler is back; every derived Oracle "
        "function formatter must assemble its own arguments"
    )
