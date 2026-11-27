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
from typing import Dict, List, Tuple

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


# ---------------------------------------------------------------------------
# Measured legacy surface, keyed by this repository's own ``source_roots``
# label. Entries are monotonic: removing a call is fine, adding one fails.
# Each reason states both why the target is not an expression object and why
# reading a declared attribute cannot replace the reflection or the literal.
# ---------------------------------------------------------------------------

REFL_CLI = (
    "CLI reads argparse/Namespace keys on a non-expression object; a declared attribute "
    "cannot be used because option names are chosen at runtime"
)
REFL_DRIVER = (
    "backend reads lazy driver/connection attributes on a non-expression object; a declared "
    "attribute cannot be used because the driver creates them at runtime"
)
REFL_ADAPTER = (
    "type adapter probes a driver value object, not an expression; a declared attribute "
    "cannot be used because callers pass arbitrary values"
)
REFL_MIXIN = (
    "mixin probes a pluggable dialect capability on a non-expression object; a declared "
    "attribute cannot be used because providers are optional"
)
REFL_EXAMPLE = (
    "shipped example duck-types a caller-supplied object, not an expression; a declared "
    "attribute cannot be used because its shape varies by caller"
)
REFL_EXPRESSION = (
    "helper reads a runtime-named attribute on a non-node object; a declared attribute cannot "
    "be used because the name is caller-supplied"
)
REFL_FLAG_ALIAS = (
    "merged-flag view reads an alias name chosen at runtime, not a declared expression field; "
    "a declared attribute cannot be used because the alias is dynamic"
)
REFL_CATALOG = (
    "introspector reads Oracle catalog objects, not expressions; a declared attribute cannot "
    "be used because row shapes are driver-defined"
)
REFL_SHOW = (
    "SHOW helper reads a driver-defined result object, not an expression; a declared "
    "attribute cannot be used because its shape is not declared here"
)
REFL_OPTIONS = (
    "suggestion helper reads caller option keys on a non-expression bag; a declared attribute "
    "cannot be used because the keys are dynamic"
)
REFL_DIALECT = (
    "dialect reads an optional flag on a duck-typed node, not an expression field; a declared "
    "attribute cannot be used because foreign nodes omit it"
)
REFL_EXCEPTION = (
    "error mapper reads Oracle driver exception fields, not an expression; a declared "
    "attribute cannot be used because the exception type is driver-defined"
)
REFL_TRANSACTION = (
    "transaction manager reads a lazy connection attribute, not an expression; a declared "
    "attribute cannot be used because the driver supplies it"
)
REFL_VERSION = (
    "version probe reads a driver row attribute, not an expression; a declared attribute "
    "cannot be used because the row shape is driver-defined"
)
SQL_EXAMPLE = (
    "shipped example passes caller-vouched static SQL with bound parameters; no expression "
    "object exists, so a declared attribute cannot carry the literal"
)
SQL_SESSION = (
    "connect sets a fixed NLS parameter on the driver connection; no expression object "
    "exists, so a declared attribute cannot carry the literal"
)
SQL_PING = (
    "liveness probe runs constant SELECT 1 on the driver connection; no expression object "
    "exists, so a declared attribute cannot carry the literal"
)
SQL_CATALOG = (
    "catalog lookup runs a fixed dictionary query with bound names; no expression object "
    "exists, so a declared attribute cannot carry the literal"
)
SQL_PARAM = (
    "concurrency probe reads a fixed V$PARAMETER row; no expression object exists, so a "
    "declared attribute cannot carry the literal"
)
SQL_IDENTIFIER = (
    "escapes a developer-supplied identifier atom before concatenation, not assembled SQL; no "
    "declared node attribute exists because the operand is a caller string"
)

ORACLE_REFLECTION_ALLOWLIST: Dict[Tuple[str, str, str, str], Tuple[int, str]] = {
    ("oracle", "adapters.py", "OracleBytesAdapter", "hasattr"): (1, REFL_ADAPTER),
    ("oracle", "adapters.py", "OracleIntervalAdapter", "hasattr"): (3, REFL_ADAPTER),
    ("oracle", "adapters.py", "OracleJSONAdapter", "hasattr"): (2, REFL_ADAPTER),
    ("oracle", "adapters.py", "OracleRowIDAdapter", "hasattr"): (1, REFL_ADAPTER),
    ("oracle", "adapters.py", "OracleSDOGeometryAdapter", "hasattr"): (1, REFL_ADAPTER),
    ("oracle", "adapters.py", "OracleStringAdapter", "hasattr"): (1, REFL_ADAPTER),
    ("oracle", "adapters.py", "OracleVectorAdapter", "hasattr"): (1, REFL_ADAPTER),
    ("oracle", "adapters.py", "OracleXMLAdapter", "hasattr"): (1, REFL_ADAPTER),
    ("oracle", "backend/async_backend.py", "AsyncOracleBackend", "getattr"): (12, REFL_DRIVER),
    ("oracle", "backend/async_backend.py", "AsyncOracleBackend", "hasattr"): (6, REFL_DRIVER),
    ("oracle", "backend/async_backend.py", "AsyncOracleBackend", "setattr"): (1, REFL_DRIVER),
    ("oracle", "backend/backend.py", "OracleBackend", "getattr"): (12, REFL_DRIVER),
    ("oracle", "backend/backend.py", "OracleBackend", "hasattr"): (7, REFL_DRIVER),
    ("oracle", "backend/backend.py", "OracleBackend", "setattr"): (1, REFL_DRIVER),
    ("oracle", "cli/connection.py", "connection_named", "getattr"): (2, REFL_CLI),
    ("oracle", "cli/connection.py", "resolve_connection_config_from_args", "getattr"): (
        2,
        REFL_CLI,
    ),
    ("oracle", "cli/info.py", "<module>", "getattr"): (1, REFL_CLI),
    ("oracle", "cli/introspect.py", "_refuse_unused_arguments", "getattr"): (3, REFL_CLI),
    ("oracle", "cli/introspect.py", "_require_capability", "getattr"): (2, REFL_CLI),
    ("oracle", "cli/introspect.py", "_serialize_for_output", "hasattr"): (1, REFL_CLI),
    ("oracle", "cli/named_expression.py", "_get_requested_version", "getattr"): (1, REFL_CLI),
    ("oracle", "cli/named_expression.py", "handle", "getattr"): (1, REFL_CLI),
    ("oracle", "cli/named_migration.py", "disconnect", "getattr"): (1, REFL_CLI),
    ("oracle", "cli/named_migration.py", "disconnect_async", "getattr"): (1, REFL_CLI),
    ("oracle", "cli/named_migration.py", "handle", "getattr"): (1, REFL_CLI),
    ("oracle", "cli/named_procedure.py", "handle", "getattr"): (1, REFL_CLI),
    ("oracle", "cli/named_procedure_graph.py", "handle", "getattr"): (1, REFL_CLI),
    ("oracle", "cli/query.py", "handle", "getattr"): (1, REFL_CLI),
    ("oracle", "cli/status.py", "_display_status_rich", "hasattr"): (3, REFL_CLI),
    ("oracle", "cli/status.py", "_handle_status_async", "hasattr"): (1, REFL_CLI),
    ("oracle", "cli/status.py", "_handle_status_sync", "hasattr"): (1, REFL_CLI),
    ("oracle", "cli/status.py", "_serialize_for_output", "hasattr"): (1, REFL_CLI),
    ("oracle", "dialect.py", "OracleDialect", "getattr"): (2, REFL_DIALECT),
    ("oracle", "error_mapping.py", "extract_ora_code", "getattr"): (3, REFL_EXCEPTION),
    ("oracle", "examples/introspection_schema_objects.py", "CountingExecutor", "getattr"): (
        1,
        REFL_EXAMPLE,
    ),
    ("oracle", "examples/introspection_schema_objects.py", "_timed_pass", "getattr"): (
        2,
        REFL_EXAMPLE,
    ),
    ("oracle", "examples/named_migrations/run_basic_async.py", "_table_exists", "getattr"): (
        1,
        REFL_EXAMPLE,
    ),
    ("oracle", "expression/column_types.py", "_require", "getattr"): (1, REFL_EXPRESSION),
    ("oracle", "expression/ddl/type.py", "_ResolvedFlagView", "getattr"): (1, REFL_FLAG_ALIAS),
    ("oracle", "expression/partition.py", "subpartitions_count_of", "getattr"): (
        1,
        REFL_EXPRESSION,
    ),
    ("oracle", "introspection/introspector.py", "OracleIntrospectorMixin", "getattr"): (
        1,
        REFL_CATALOG,
    ),
    ("oracle", "introspection/introspector.py", "OracleIntrospectorMixin", "hasattr"): (
        2,
        REFL_CATALOG,
    ),
    ("oracle", "mixins/backend_mixin.py", "OracleBackendMixin", "getattr"): (3, REFL_MIXIN),
    ("oracle", "mixins/backend_mixin.py", "OracleBackendMixin", "hasattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/collation.py", "OracleCollationMixin", "getattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/column.py", "OracleModifyColumnMixin", "getattr"): (3, REFL_MIXIN),
    ("oracle", "mixins/column.py", "OracleModifyColumnMixin", "hasattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/concurrency.py", "OracleConcurrencyMixin", "getattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/ddl.py", "OracleDDLMixin", "getattr"): (2, REFL_MIXIN),
    ("oracle", "mixins/ddl_type.py", "OracleTypeDDLMixin", "getattr"): (2, REFL_MIXIN),
    ("oracle", "mixins/dml.py", "OracleDMLOperationMixin", "getattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/dml.py", "OracleDMLOperationMixin", "hasattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/expression.py", "OracleExpressionMixin", "getattr"): (2, REFL_MIXIN),
    ("oracle", "mixins/flashback.py", "OracleFlashbackMixin", "hasattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/functions.py", "OracleFunctionFormatMixin", "getattr"): (2, REFL_MIXIN),
    ("oracle", "mixins/if_exists.py", "supports_statement_if_exists", "getattr"): (3, REFL_MIXIN),
    ("oracle", "mixins/json.py", "OracleJSONFunctionMixin", "getattr"): (4, REFL_MIXIN),
    ("oracle", "mixins/locking.py", "OracleLockingMixin", "getattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/spatial.py", "OracleSpatialMixin", "hasattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/trigger.py", "OracleTriggerMixin", "getattr"): (11, REFL_MIXIN),
    ("oracle", "mixins/types.py", "OracleTypeSuggestionMixin", "getattr"): (1, REFL_MIXIN),
    ("oracle", "mixins/types.py", "OracleTypeSupportMixin", "getattr"): (3, REFL_MIXIN),
    ("oracle", "mixins/vector.py", "OracleVectorMixin", "getattr"): (2, REFL_MIXIN),
    ("oracle", "mixins/vector.py", "OracleVectorMixin", "hasattr"): (2, REFL_MIXIN),
    ("oracle", "neutral_adapters.py", "OracleNeutralRangeAdapter", "getattr"): (1, REFL_ADAPTER),
    ("oracle", "neutral_adapters.py", "OracleNeutralVectorAdapter", "hasattr"): (1, REFL_ADAPTER),
    ("oracle", "neutral_adapters.py", "_read_bytes", "getattr"): (1, REFL_ADAPTER),
    ("oracle", "neutral_adapters.py", "_sdo_value", "getattr"): (2, REFL_ADAPTER),
    ("oracle", "show/backend_mixin.py", "AsyncOracleShowMixin", "getattr"): (1, REFL_SHOW),
    ("oracle", "show/backend_mixin.py", "AsyncOracleShowMixin", "hasattr"): (1, REFL_SHOW),
    ("oracle", "show/backend_mixin.py", "OracleShowMixin", "getattr"): (1, REFL_SHOW),
    ("oracle", "show/backend_mixin.py", "OracleShowMixin", "hasattr"): (1, REFL_SHOW),
    ("oracle", "suggestion/_common.py", "OraclePartitionSuggestionBase", "getattr"): (
        2,
        REFL_OPTIONS,
    ),
    ("oracle", "suggestion/_common.py", "_bound_value", "getattr"): (4, REFL_OPTIONS),
    ("oracle", "suggestion/_common.py", "_get", "getattr"): (1, REFL_OPTIONS),
    ("oracle", "suggestion/_common.py", "_word", "getattr"): (3, REFL_OPTIONS),
    (
        "oracle",
        "suggestion/table_constraint_change_suggestion.py",
        "OracleTableConstraintChangeSuggestionMixin",
        "getattr",
    ): (5, REFL_OPTIONS),
    ("oracle", "suggestion/table_constraint_change_suggestion.py", "_declaration_key", "getattr"): (
        2,
        REFL_OPTIONS,
    ),
    ("oracle", "transaction.py", "OracleTransactionManager", "hasattr"): (1, REFL_TRANSACTION),
    ("oracle", "version.py", "select_version_row", "hasattr"): (1, REFL_VERSION),
}

ORACLE_EXEC_ALLOWLIST: Dict[Tuple[str, str, str, str, str], Tuple[int, str]] = {
    (
        "oracle",
        "backend/async_backend.py",
        "_lookup_oracle_data_type_async",
        "execute",
        "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS WH",
    ): (1, SQL_CATALOG),
    (
        "oracle",
        "backend/async_backend.py",
        "_lookup_oracle_data_type_async",
        "execute",
        "SELECT DATA_TYPE FROM USER_TAB_COLUMNS W",
    ): (1, SQL_CATALOG),
    (
        "oracle",
        "backend/async_backend.py",
        "connect",
        "execute",
        "ALTER SESSION SET NLS_DATE_FORMAT = 'YYY",
    ): (1, SQL_SESSION),
    (
        "oracle",
        "backend/async_backend.py",
        "connect",
        "execute",
        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT =",
    ): (1, SQL_SESSION),
    (
        "oracle",
        "backend/async_backend.py",
        "connect",
        "execute",
        "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMA",
    ): (1, SQL_SESSION),
    ("oracle", "backend/async_backend.py", "ping", "execute", "SELECT 1 FROM DUAL"): (1, SQL_PING),
    (
        "oracle",
        "backend/backend.py",
        "_lookup_oracle_data_type",
        "execute",
        "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS WH",
    ): (1, SQL_CATALOG),
    (
        "oracle",
        "backend/backend.py",
        "_lookup_oracle_data_type",
        "execute",
        "SELECT DATA_TYPE FROM USER_TAB_COLUMNS W",
    ): (1, SQL_CATALOG),
    (
        "oracle",
        "backend/backend.py",
        "connect",
        "execute",
        "ALTER SESSION SET NLS_DATE_FORMAT = 'YYY",
    ): (1, SQL_SESSION),
    (
        "oracle",
        "backend/backend.py",
        "connect",
        "execute",
        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT =",
    ): (1, SQL_SESSION),
    (
        "oracle",
        "backend/backend.py",
        "connect",
        "execute",
        "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMA",
    ): (1, SQL_SESSION),
    ("oracle", "backend/backend.py", "ping", "execute", "SELECT 1 FROM DUAL"): (1, SQL_PING),
    (
        "oracle",
        "examples/connection/quickstart.py",
        "<module>",
        "execute",
        "SELECT 'Hello from Oracle!' AS message F",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/connection/quickstart.py",
        "<module>",
        "execute",
        "DROP TABLE quickstart_users PURGE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/connection/quickstart.py",
        "<module>",
        "execute",
        "INSERT INTO quickstart_users (name, stat",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/connection/quickstart.py",
        "<module>",
        "execute",
        "SELECT * FROM quickstart_users ORDER BY ",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/connection/quickstart.py",
        "<module>",
        "execute",
        "SELECT * FROM quickstart_users WHERE nam",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/connection/quickstart.py",
        "<module>",
        "execute",
        "UPDATE quickstart_users SET status = :1 ",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/delete/basic.py",
        "<module>",
        "execute",
        "DELETE FROM delete_demo WHERE status = :",
    ): (1, SQL_EXAMPLE),
    ("oracle", "examples/delete/basic.py", "<module>", "execute", "DELETE FROM delete_demo"): (
        1,
        SQL_EXAMPLE,
    ),
    ("oracle", "examples/delete/basic.py", "<module>", "execute", "DROP TABLE delete_demo PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/delete/basic.py",
        "<module>",
        "execute",
        "INSERT INTO delete_demo (name, status) V",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/delete/basic.py",
        "<module>",
        "execute",
        "SELECT * FROM delete_demo ORDER BY id",
    ): (2, SQL_EXAMPLE),
    ("oracle", "examples/delete/basic.py", "<module>", "execute", "SELECT * FROM delete_demo"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/insert/basic.py",
        "<module>",
        "execute",
        "DROP TABLE eg_insert_demo PURGE",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/insert/basic.py",
        "<module>",
        "execute",
        "INSERT INTO eg_insert_demo (name, score,",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/insert/basic.py",
        "<module>",
        "execute",
        "SELECT * FROM eg_insert_demo ORDER BY id",
    ): (1, SQL_EXAMPLE),
    ("oracle", "examples/insert/upsert.py", "<module>", "execute", "DROP TABLE merge_demo PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/insert/upsert.py",
        "<module>",
        "execute",
        "INSERT INTO merge_demo (sku, stock, pric",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/insert/upsert.py",
        "<module>",
        "execute",
        "SELECT sku, stock, price FROM merge_demo",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/named_migrations/run_basic_async.py",
        "_table_exists",
        "execute",
        "SELECT 1 FROM user_tables WHERE table_na",
    ): (1, SQL_EXAMPLE),
    ("oracle", "examples/query/aggregate.py", "<module>", "execute", "DROP TABLE agg_demo PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/query/aggregate.py",
        "<module>",
        "execute",
        "INSERT INTO agg_demo (dept, role, salary",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/aggregate.py",
        "<module>",
        "execute",
        "SELECT dept, COUNT(*) AS cnt FROM agg_de",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/aggregate.py",
        "<module>",
        "execute",
        "SELECT dept, COUNT(*) AS cnt, SUM(salary",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/aggregate.py",
        "<module>",
        "execute",
        "SELECT dept, MIN(salary) AS min_sal, MAX",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/aggregate.py",
        "<module>",
        "execute",
        "SELECT dept, role, COUNT(*) AS cnt, ROUN",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/basic.py",
        "<module>",
        "execute",
        "DROP TABLE eg_query_basic PURGE",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/basic.py",
        "<module>",
        "execute",
        "INSERT INTO eg_query_basic (product, cat",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/basic.py",
        "<module>",
        "execute",
        "SELECT * FROM eg_query_basic ORDER BY id",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/basic.py",
        "<module>",
        "execute",
        "SELECT category, COUNT(*) AS cnt, AVG(pr",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/basic.py",
        "<module>",
        "execute",
        "SELECT product, price FROM eg_query_basi",
    ): (1, SQL_EXAMPLE),
    ("oracle", "examples/query/cte.py", "<module>", "execute", "DROP TABLE cte_demo PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/query/cte.py",
        "<module>",
        "execute",
        "INSERT INTO cte_demo (parent_id, name) V",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/hierarchy.py",
        "<module>",
        "execute",
        "SELECT LEVEL, LPAD(' ', LEVEL * 2) || na",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/hierarchy.py",
        "<module>",
        "execute",
        "SELECT name, SYS_CONNECT_BY_PATH(name, '",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/hierarchy.py",
        "<module>",
        "execute",
        "DROP TABLE employees PURGE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/hierarchy.py",
        "<module>",
        "execute",
        "INSERT INTO employees (emp_id, name, mgr",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/hierarchy.py",
        "<module>",
        "execute",
        "SELECT emp_id, name, mgr_id FROM employe",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/hierarchy.py",
        "<module>",
        "execute",
        "SELECT name, CONNECT_BY_ROOT name AS roo",
    ): (1, SQL_EXAMPLE),
    ("oracle", "examples/query/join.py", "<module>", "execute", "DROP TABLE join_a PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    ("oracle", "examples/query/join.py", "<module>", "execute", "DROP TABLE join_b PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/query/join.py",
        "<module>",
        "execute",
        "INSERT INTO join_a (id, label) VALUES (:",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/join.py",
        "<module>",
        "execute",
        "INSERT INTO join_b (id, label, a_id) VAL",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/join.py",
        "<module>",
        "execute",
        "SELECT a.label AS a_label, b.label AS b_",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/join.py",
        "<module>",
        "execute",
        "SELECT a.label, b.label AS b_label FROM ",
    ): (3, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/join.py",
        "<module>",
        "execute",
        "SELECT b.label, a.label AS a_label FROM ",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/listagg.py",
        "<module>",
        "execute",
        "SELECT region, LISTAGG(rep, ', ') AS rep",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/listagg.py",
        "<module>",
        "execute",
        "SELECT region, LISTAGG(rep, '; ') WITHIN",
    ): (1, SQL_EXAMPLE),
    ("oracle", "examples/query/listagg.py", "<module>", "execute", "DROP TABLE sales PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/query/listagg.py",
        "<module>",
        "execute",
        "INSERT INTO sales (region, rep, amount) ",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/listagg.py",
        "<module>",
        "execute",
        "SELECT region, COUNT(*) AS num, SUM(amou",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/listagg.py",
        "<module>",
        "execute",
        "SELECT region, PERCENTILE_CONT(0.5) WITH",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/listagg.py",
        "<module>",
        "execute",
        "SELECT region, PERCENTILE_DISC(0.5) WITH",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pagination.py",
        "<module>",
        "execute",
        "DROP TABLE page_demo PURGE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pagination.py",
        "<module>",
        "execute",
        "INSERT INTO page_demo (name, score) VALU",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pagination.py",
        "<module>",
        "execute",
        "SELECT name, score FROM (SELECT name, sc",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pagination.py",
        "<module>",
        "execute",
        "SELECT name, score FROM page_demo ORDER ",
    ): (5, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pagination.py",
        "<module>",
        "execute",
        "UPDATE page_demo SET score = 80 WHERE na",
    ): (2, SQL_EXAMPLE),
    ("oracle", "examples/query/pivot.py", "<module>", "execute", "DROP TABLE pivot_sales PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    ("oracle", "examples/query/pivot.py", "<module>", "execute", "DROP TABLE unpivot_demo PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/query/pivot.py",
        "<module>",
        "execute",
        "INSERT INTO pivot_sales (year, quarter, ",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pivot.py",
        "<module>",
        "execute",
        "INSERT INTO unpivot_demo (year, q1, q2, ",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pivot.py",
        "<module>",
        "execute",
        "SELECT * FROM pivot_sales ORDER BY year,",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pivot.py",
        "<module>",
        "execute",
        "SELECT * FROM pivot_sales PIVOT (SUM(amo",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pivot.py",
        "<module>",
        "execute",
        "SELECT * FROM pivot_sales PIVOT XML (SUM",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/query/pivot.py",
        "<module>",
        "execute",
        "SELECT year, quarter, amount FROM unpivo",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/basic.py",
        "<module>",
        "execute",
        "CREATE TABLE txn_rollback (name VARCHAR2",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/basic.py",
        "<module>",
        "execute",
        "DROP TABLE txn_demo PURGE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/basic.py",
        "<module>",
        "execute",
        "DROP TABLE txn_rollback PURGE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/basic.py",
        "<module>",
        "execute",
        "INSERT INTO txn_demo (name, balance) VAL",
    ): (3, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/basic.py",
        "<module>",
        "execute",
        "INSERT INTO txn_rollback (name) VALUES (",
    ): (2, SQL_EXAMPLE),
    ("oracle", "examples/transaction/basic.py", "<module>", "execute", "SELECT * FROM txn_demo"): (
        3,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/transaction/basic.py",
        "<module>",
        "execute",
        "SELECT name FROM txn_rollback",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/for_update.py",
        "<module>",
        "execute",
        "DROP TABLE lock_demo PURGE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/for_update.py",
        "<module>",
        "execute",
        "INSERT INTO lock_demo (id, name, amount)",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/for_update.py",
        "<module>",
        "execute",
        "SELECT id, name FROM lock_demo FOR UPDAT",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/for_update.py",
        "<module>",
        "execute",
        "SELECT id, name FROM lock_demo WHERE id ",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/for_update.py",
        "<module>",
        "execute",
        "SELECT id, name, amount FROM lock_demo W",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/transaction/for_update.py",
        "<module>",
        "execute",
        "UPDATE lock_demo SET amount = :1 WHERE i",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/json_basic.py",
        "<module>",
        "execute",
        "SELECT JSON_OBJECT('product' VALUE name,",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/json_basic.py",
        "<module>",
        "execute",
        "SELECT name, JSON_QUERY(payload, '$.tags",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/json_basic.py",
        "<module>",
        "execute",
        "SELECT name, JSON_VALUE(payload, '$.stoc",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/json_basic.py",
        "<module>",
        "execute",
        "DROP TABLE json_demo PURGE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/json_basic.py",
        "<module>",
        "execute",
        "INSERT INTO json_demo (name, payload) VA",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/json_basic.py",
        "<module>",
        "execute",
        "SELECT JSON_ARRAY(name, JSON_VALUE(paylo",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/json_basic.py",
        "<module>",
        "execute",
        "SELECT jt.tag_name FROM json_demo, JSON_",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/json_basic.py",
        "<module>",
        "execute",
        "SELECT name FROM json_demo WHERE JSON_EX",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/spatial_basic.py",
        "<module>",
        "execute",
        "DROP TABLE spatial_demo PURGE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/spatial_basic.py",
        "<module>",
        "execute",
        "INSERT INTO spatial_demo (id, name, geom",
    ): (3, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/spatial_basic.py",
        "<module>",
        "execute",
        "SELECT a.name AS loc1, b.name AS loc2 FR",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/spatial_basic.py",
        "<module>",
        "execute",
        "SELECT name FROM spatial_demo WHERE SDO_",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/spatial_basic.py",
        "<module>",
        "execute",
        "SELECT name, SDO_GEOM.SDO_DISTANCE((SELE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/spatial_basic.py",
        "<module>",
        "execute",
        "SELECT name, geom.GET_GTYPE() AS gtype, ",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/vector_basic.py",
        "<module>",
        "execute",
        "DROP TABLE vector_demo PURGE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/vector_basic.py",
        "<module>",
        "execute",
        "INSERT INTO vector_demo (id, name, embed",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/vector_basic.py",
        "<module>",
        "execute",
        "SELECT a.name, b.name, VECTOR_DISTANCE(a",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/vector_basic.py",
        "<module>",
        "execute",
        "SELECT name FROM vector_demo WHERE name ",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/xml_basic.py",
        "<module>",
        "execute",
        "SELECT name, XMLCast(XMLQuery('/order/cu",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/xml_basic.py",
        "<module>",
        "execute",
        "SELECT name, XMLCast(XMLQuery('/order/to",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/xml_basic.py",
        "<module>",
        "execute",
        "SELECT name, XMLExists('/order/status' P",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/types/xml_basic.py",
        "<module>",
        "execute",
        "SELECT name, doc.extract('/order/custome",
    ): (1, SQL_EXAMPLE),
    ("oracle", "examples/types/xml_basic.py", "<module>", "execute", "DROP TABLE xml_demo PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/types/xml_basic.py",
        "<module>",
        "execute",
        "INSERT INTO xml_demo (id, name, doc) VAL",
    ): (2, SQL_EXAMPLE),
    ("oracle", "examples/update/basic.py", "<module>", "execute", "DROP TABLE update_demo PURGE"): (
        1,
        SQL_EXAMPLE,
    ),
    (
        "oracle",
        "examples/update/basic.py",
        "<module>",
        "execute",
        "INSERT INTO update_demo (name, score, st",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/update/basic.py",
        "<module>",
        "execute",
        "SELECT * FROM update_demo ORDER BY id",
    ): (2, SQL_EXAMPLE),
    (
        "oracle",
        "examples/update/basic.py",
        "<module>",
        "execute",
        "UPDATE update_demo SET score = :1 WHERE ",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/update/basic.py",
        "<module>",
        "execute",
        "UPDATE update_demo SET score = :1, statu",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "examples/update/basic.py",
        "<module>",
        "execute",
        "UPDATE update_demo SET status = :1 WHERE",
    ): (1, SQL_EXAMPLE),
    (
        "oracle",
        "mixins/concurrency.py",
        "_fetch_concurrency_hint",
        "execute",
        "SELECT VALUE FROM V$PARAMETER WHERE NAME",
    ): (1, SQL_PARAM),
}

ORACLE_FORMATTER_ALLOWLIST: Dict[Tuple[str, str, str, str, str], Tuple[int, str]] = {
    ("oracle", "dialect.py", "format_identifier", "replace", "name:identifier"): (
        1,
        SQL_IDENTIFIER,
    ),
}


@pytest.fixture
def reflection_allowlist() -> Dict[Tuple[str, str, str, str], Tuple[int, str]]:
    """Shared controls plus Oracle's measured reflection surface."""
    from rhosocial.activerecord.testsuite.feature.rules.backend import (
        test_no_reflection_in_expression_layer as reflection_rules,
    )

    return {**reflection_rules.REFLECTION_ALLOWLIST, **ORACLE_REFLECTION_ALLOWLIST}


@pytest.fixture
def exec_allowlist() -> Dict[Tuple[str, str, str, str, str], Tuple[int, str]]:
    """Shared controls plus Oracle's measured SQL literals."""
    from rhosocial.activerecord.testsuite.feature.rules.backend import (
        test_no_raw_sql_in_src as raw_sql_rules,
    )

    return {**raw_sql_rules.EXEC_ALLOWLIST, **ORACLE_EXEC_ALLOWLIST}


@pytest.fixture
def formatter_allowlist() -> Dict[Tuple[str, str, str, str, str], Tuple[int, str]]:
    """Shared controls plus Oracle's identifier-atom escape."""
    from rhosocial.activerecord.testsuite.feature.rules.backend import (
        test_no_string_sql_manipulation_in_formatters as formatter_rules,
    )

    return {**formatter_rules.FORMATTER_ALLOWLIST, **ORACLE_FORMATTER_ALLOWLIST}


@pytest.fixture
def backend_matrix_files(backend_test_files) -> List[Path]:
    """Round-trip matrices that declare an UNCONSTRUCTIBLE list."""
    return [
        path for path in backend_test_files if "UNCONSTRUCTIBLE" in path.read_text(encoding="utf-8")
    ]
