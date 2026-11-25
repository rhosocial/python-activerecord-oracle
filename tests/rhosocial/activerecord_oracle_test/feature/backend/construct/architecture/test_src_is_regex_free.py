# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/architecture/test_src_is_regex_free.py
"""The backend's own source must not use the ``re`` module.

The project rule is blunt: ``src`` never imports or uses ``re``. Catalogue
text is read by character scanning, SQL is built by formatters, and no layer
rewrites SQL or catalogue text with a pattern. Tests may keep regexes, but
only to assert generated SQL.

This guard walks the package's ``src`` tree. The editable install exposes the
namespace package with ``__file__ = None``, so the directory is taken from
``__path__``.
"""
import ast
import tokenize
from pathlib import Path

import pytest

import rhosocial.activerecord.backend.impl.oracle as oracle_package


def _src_files():
    roots = list(getattr(oracle_package, "__path__", []))
    files = []
    for root in roots:
        for path in Path(root).rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return files


def _imports_re(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "re" or alias.name.startswith("re."):
                    return node.lineno
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "re" or module.startswith("re."):
                return node.lineno
    return None


def _re_attribute_lines(path):
    """Scan tokens: ``re`` immediately followed by ``.`` on the same line."""
    found = []
    pending_re = None
    with path.open("rb") as handle:
        for token in tokenize.tokenize(handle.readline):
            if token.type == tokenize.NAME and token.string == "re":
                pending_re = token.start[0]
                continue
            if pending_re is None:
                continue
            if token.type in (tokenize.NL, tokenize.NEWLINE, tokenize.COMMENT):
                pending_re = None
                continue
            if token.type == tokenize.OP and token.string == ".":
                found.append(pending_re)
            pending_re = None
    return found


@pytest.mark.parametrize("path", _src_files(), ids=lambda p: p.name)
def test_src_does_not_import_re(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    line = _imports_re(tree)
    assert line is None, f"{path}:{line} imports the re module"


@pytest.mark.parametrize("path", _src_files(), ids=lambda p: p.name)
def test_src_does_not_use_re_attributes(path):
    lines = _re_attribute_lines(path)
    assert not lines, f"{path}:{lines} uses the re module"


def test_the_scan_reaches_the_source_tree():
    files = _src_files()
    assert len(files) > 50, f"only {len(files)} source files were scanned"
    names = {path.name for path in files}
    assert "_scan.py" in names


def _write_probe(directory, body):
    probe = directory / "_guard_probe.py"
    probe.write_text(body, encoding="utf-8")
    return probe


def test_the_import_scan_flags_a_planted_violation(tmp_path):
    probe = _write_probe(tmp_path, "import re as regex\n")
    tree = ast.parse(probe.read_text(encoding="utf-8"))
    assert _imports_re(tree) is not None


def test_the_attribute_scan_flags_a_planted_violation(tmp_path):
    probe = _write_probe(tmp_path, "def f(text):\n    return re.sub('x', 'y', text)\n")
    assert _re_attribute_lines(probe)


def test_the_scans_accept_clean_source(tmp_path):
    probe = _write_probe(tmp_path, "def f(text):\n    return ' '.join(text.split())\n")
    tree = ast.parse(probe.read_text(encoding="utf-8"))
    assert _imports_re(tree) is None
    assert not _re_attribute_lines(probe)
