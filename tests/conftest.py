# tests/conftest.py
"""
Root pytest configuration file for the rhosocial-activerecord-oracle package.

Configures the environment for the testsuite to find backend-specific implementations.
"""
import os
import sys
import pytest

# Set the environment variable for provider registry BEFORE importing providers.
os.environ.setdefault(
    'TESTSUITE_PROVIDER_REGISTRY',
    'providers.registry:provider_registry'
)

# Import providers to register them with the registry.
try:
    import providers  # noqa: F401
except Exception as exc:
    print(f"[conftest] providers import warning: {exc}", file=sys.stderr)


# --- Oracle 18c skip configuration ---
#
# Oracle 18c's AL32UTF8 charset cannot round-trip supplementary-plane
# characters (U+10000+, e.g. most emoji).  The testsuite provides both
# full-Unicode tests and BMP-only tests.  On Oracle 18c we skip the
# full-Unicode tests and keep only the BMP-only tests.

_ORACLE_18C_SKIP_TEST_NAMES = frozenset({
    "test_unicode_multilingual_content_round_trip",
    "test_unicode_emoji_burst_truncated_in_summary",
    "test_unicode_json_fixture_json_field_round_trip",
})

_SKIP_REASON = (
    "Oracle 18c AL32UTF8 cannot round-trip supplementary-plane characters "
    "(U+10000+). BMP-only tests cover Oracle 18c's usable charset."
)


def _detect_oracle_18c() -> bool:
    """Read the scenario YAML directly to detect Oracle 18c."""
    try:
        import yaml
    except ImportError:
        print("[conftest] yaml not available, skipping 18c detection", file=sys.stderr)
        return False

    config_path = os.getenv("ORACLE_SCENARIOS_CONFIG_PATH")
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), "config", "oracle_scenarios.yaml")
    if not config_path or not os.path.exists(config_path):
        print(f"[conftest] no config at {config_path}", file=sys.stderr)
        return False
    try:
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
        scenarios = data.get("scenarios") or {}
        found = any("18c" in name for name in scenarios)
        print(f"[conftest] scenarios={list(scenarios.keys())}, is_18c={found}", file=sys.stderr)
        return found
    except Exception as exc:
        print(f"[conftest] YAML read error: {exc}", file=sys.stderr)
        return False


def pytest_collection_modifyitems(config, items):
    """Skip supplementary-plane Unicode tests on Oracle 18c."""
    if not _detect_oracle_18c():
        return

    skip_marker = pytest.mark.skip(reason=_SKIP_REASON)
    count = 0
    for item in items:
        if item.name in _ORACLE_18C_SKIP_TEST_NAMES:
            item.add_marker(skip_marker)
            count += 1
    print(f"[conftest] Marked {count} tests as skip for Oracle 18c", file=sys.stderr)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment before running tests."""
    yield
