# tests/rhosocial/activerecord_oracle_test/feature/backend/test_expression_namespace.py
"""Guardrail: every expression must live under the backend's expression namespace.

``BaseExpression`` subclasses render SQL, so they belong in
``rhosocial.activerecord.backend.impl.oracle.expression``. Data types are no
exception: a value-level ``DataType`` belongs in ``expression/types.py`` and a
DDL-level type in ``expression/ddl/type.py``.

The invariant is asserted mechanically rather than left to review, so a class
cannot be reintroduced outside ``expression`` without a test failing.
"""

import importlib
import pkgutil

import pytest

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.impl import oracle

EXPRESSION_NS = oracle.__name__ + ".expression"
BACKEND_NS = oracle.__name__ + "."
TYPE_VALUES_NS = oracle.__name__ + ".type_values"


def _iter_subclasses(cls):
    """Yield every transitive subclass, breadth-first, cycle-safe."""
    seen = {cls}
    queue = [cls]
    while queue:
        current = queue.pop(0)
        for sub in current.__subclasses__():
            if sub not in seen:
                seen.add(sub)
                queue.append(sub)
                yield sub


def _import_backend_modules():
    """Import every non-example backend module; report the ones that fail."""
    failures = []
    for mod in pkgutil.walk_packages(oracle.__path__, prefix=oracle.__name__ + "."):
        if ".examples" in mod.name or ".cli" in mod.name:
            continue  # example/CLI modules have import side effects
        try:
            importlib.import_module(mod.name)
        except Exception as exc:  # pragma: no cover - diagnostic path
            failures.append(f"{mod.name}: {type(exc).__name__}: {exc}")
    return failures


@pytest.fixture(scope="module")
def all_expression_classes():
    """Import every module in the oracle backend, then collect expressions.

    Imports are required so that ``__subclasses__()`` is populated; classes
    that are never imported cannot be discovered by introspection.
    """
    failures = _import_backend_modules()
    assert not failures, "could not import backend modules:\n" + "\n".join(failures)
    return sorted(_iter_subclasses(BaseExpression), key=lambda c: (c.__module__, c.__qualname__))


def test_backend_modules_import_cleanly():
    """Sanity check: every non-example backend module is importable."""
    failures = _import_backend_modules()
    assert not failures, "unimportable modules:\n" + "\n".join(failures)


def test_no_expression_defined_outside_expression_namespace(all_expression_classes):
    """The core invariant: backend expressions are only defined under ``expression``.

    Only classes defined *inside this backend's* package are constrained. Core
    expressions live in ``rhosocial.activerecord.backend.expression`` and
    expressions belonging to other backends are irrelevant here, so both are
    excluded by requiring the module to start with the backend namespace.
    """
    offenders = [
        f"{cls.__module__}.{cls.__qualname__}"
        for cls in all_expression_classes
        if cls.__module__.startswith(BACKEND_NS)
        and not cls.__module__.startswith(EXPRESSION_NS)
    ]
    assert not offenders, (
        "BaseExpression subclasses defined in this backend but outside "
        f"{EXPRESSION_NS}:\n  " + "\n  ".join(sorted(offenders))
    )


def test_type_values_package_holds_no_expressions(all_expression_classes):
    """``type_values`` is the domain value-type layer, so it holds no expressions."""
    offenders = [
        f"{cls.__module__}.{cls.__qualname__}"
        for cls in all_expression_classes
        if cls.__module__.startswith(TYPE_VALUES_NS)
    ]
    assert not offenders, (
        f"expressions leaked into {TYPE_VALUES_NS}:\n  " + "\n  ".join(sorted(offenders))
    )


def test_every_expression_class_is_inspectable(all_expression_classes):
    """Sanity check: the collected set is non-trivial, so the test cannot pass vacuously."""
    assert len(all_expression_classes) > 50, (
        f"only found {len(all_expression_classes)} expression classes; "
        "module discovery is probably broken"
    )


def test_old_types_package_is_gone():
    """The renamed package must not linger under its old name.

    ``types`` is a heavily overloaded name in this series, so a leftover
    ``oracle/types`` package would silently re-open the ambiguity that the
    ``type_values`` rename was meant to remove.
    """
    import importlib.util

    assert importlib.util.find_spec(f"{oracle.__name__}.types") is None, (
        f"{oracle.__name__}.types still exists; it should have been renamed to type_values"
    )
