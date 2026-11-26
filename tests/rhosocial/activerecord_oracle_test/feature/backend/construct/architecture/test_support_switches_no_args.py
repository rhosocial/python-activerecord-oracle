# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/architecture/test_support_switches_no_args.py
"""``supports_*`` is a support switch: no arguments, one bool.

A capability flag answers "does this dialect configuration have capability k",
so its only input is the dialect instance. Anything that needs a name or a kind
is either a separate switch (closed vocabulary) or a format-time validation
(open vocabulary) -- never an argument to ``supports_*``.

This guard walks the backend package's own ``src`` tree. The editable install
exposes the namespace package with ``__file__ = None``, so the directory is
taken from ``__path__``.
"""
import ast
from pathlib import Path

import pytest

import rhosocial.activerecord.backend.impl.oracle as oracle_package


def _src_files():
    """Every module inside this backend's source tree."""
    roots = list(getattr(oracle_package, "__path__", []))
    files = []
    for root in roots:
        for path in Path(root).rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return files


def _required_arguments(func: ast.FunctionDef):
    """Parameters that must be passed, excluding ``self``."""
    args = func.args
    positional = list(args.args)
    required = positional[1 : len(positional) - len(args.defaults)]
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        if default is None:
            required.append(arg)
    return [a.arg for a in required]


def _offenders(tree, label):
    """``supports_*`` methods that cannot be called without an argument."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for item in node.body:
            if not isinstance(item, ast.FunctionDef):
                continue
            if not item.name.startswith("supports_"):
                continue
            missing = _required_arguments(item)
            if missing:
                found.append(f"{label}:{item.lineno} {item.name}({', '.join(missing)})")
    return found


@pytest.mark.parametrize("path", _src_files(), ids=lambda p: p.name)
def test_support_switch_takes_no_required_argument(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    offenders = _offenders(tree, path.name)
    assert not offenders, "\n".join(offenders)


def test_the_scan_reaches_the_source_tree():
    files = _src_files()
    assert len(files) > 50, f"only {len(files)} source files were scanned"
    names = {path.name for path in files}
    assert "dialect.py" in names


def test_the_scan_flags_a_planted_violation(tmp_path):
    probe = tmp_path / "_guard_probe.py"
    probe.write_text(
        "class P:\n"
        "    def supports_thing(self, name):\n"
        "        return False\n",
        encoding="utf-8",
    )
    tree = ast.parse(probe.read_text(encoding="utf-8"))
    assert _offenders(tree, "probe")


def test_the_scan_flags_a_planted_keyword_only_violation(tmp_path):
    probe = tmp_path / "_guard_probe.py"
    probe.write_text(
        "class P:\n"
        "    def supports_thing(self, *, name):\n"
        "        return False\n",
        encoding="utf-8",
    )
    tree = ast.parse(probe.read_text(encoding="utf-8"))
    assert _offenders(tree, "probe")


def test_the_scan_accepts_a_defaulted_parameter(tmp_path):
    probe = tmp_path / "_guard_probe.py"
    probe.write_text(
        "class P:\n"
        "    def supports_thing(self, name=None):\n"
        "        return False\n",
        encoding="utf-8",
    )
    tree = ast.parse(probe.read_text(encoding="utf-8"))
    assert not _offenders(tree, "probe")
