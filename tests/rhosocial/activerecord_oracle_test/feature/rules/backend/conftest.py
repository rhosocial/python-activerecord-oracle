# tests/rhosocial/activerecord_oracle_test/feature/rules/backend/conftest.py
"""Fixtures for Oracle's own rule suite.

Oracle is an independent backend repository. Every path is derived from the
installed Oracle expression package, never from a checkout or worktree path;
``source_roots`` names a single root, ``impl/oracle``. The shared rule library
supplies the assertions, this conftest selects the subject.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import List, Tuple

import pytest

from rhosocial.activerecord.testsuite.feature.rules.backend.conftest import *  # noqa: F401,F403
from rhosocial.activerecord.testsuite.feature.rules.backend.conftest import python_files_in
from rhosocial.activerecord.testsuite.pytest_plugin import Declaration

DECLARATION = Declaration(
    uses_database=False,
    parallel=False,
    purpose="Executable assertions of the Oracle backend's formal constraints",
    passes_means="Oracle source obeys the mechanical and structural rule",
    fails_means="Oracle source introduces a forbidden construct or dependency",
)

ORACLE_EXPRESSION_PACKAGE = "rhosocial.activerecord.backend.impl.oracle.expression"


def oracle_expression_package() -> object:
    """The installed Oracle expression package."""
    return importlib.import_module(ORACLE_EXPRESSION_PACKAGE)


def oracle_impl_root() -> Path:
    """The installed ``backend/impl/oracle`` directory."""
    package = oracle_expression_package()
    for search_path in vars(package).get("__path__", ()):
        current = Path(search_path).resolve()
        for candidate in [current] + list(current.parents):
            if candidate.name == "impl" and candidate.parent.name == "backend":
                return candidate / current.relative_to(candidate).parts[0]
    pytest.skip("Oracle impl root is not discoverable from the installed package")


def oracle_project_root() -> Path:
    """Project root inferred from the installed Oracle package layout."""
    for candidate in [oracle_impl_root()] + list(oracle_impl_root().parents):
        if (candidate / "tests").is_dir() and (candidate / "src").is_dir():
            return candidate
    pytest.skip("Oracle project root is not discoverable from the installed package")


@pytest.fixture
def backend_expression_package() -> object:
    """Oracle's expression package."""
    return oracle_expression_package()


@pytest.fixture
def backend_dialect() -> object:
    """An instantiated Oracle dialect."""
    from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

    return OracleDialect()


@pytest.fixture
def backend_prefix() -> str:
    """Oracle's own naming prefix."""
    return "Oracle"


@pytest.fixture
def backend_impl_root() -> Path:
    """The Oracle ``impl`` root used by file-driven shared rules."""
    return oracle_impl_root()


@pytest.fixture
def source_roots() -> List[Tuple[str, Path]]:
    """One root only: the Oracle backend, never core or another backend."""
    return [("oracle", oracle_impl_root())]


@pytest.fixture
def backend_project_root() -> Path:
    """Project root for Oracle's tests."""
    return oracle_project_root()


@pytest.fixture
def backend_test_files(backend_project_root) -> List[Path]:
    """Every Python test file in this project."""
    files = list(python_files_in(backend_project_root / "tests"))
    assert files, f"no test files under {backend_project_root / 'tests'}"
    return files


@pytest.fixture
def backend_matrix_files(backend_test_files) -> List[Path]:
    """Round-trip matrices that declare an UNCONSTRUCTIBLE list."""
    return [
        path for path in backend_test_files if "UNCONSTRUCTIBLE" in path.read_text(encoding="utf-8")
    ]
