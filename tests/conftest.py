# tests/conftest.py
"""
Root pytest configuration file for the rhosocial-activerecord-oracle package.

Configures the environment for the testsuite to find backend-specific implementations.
"""
import os
import pytest

# Import providers to register them with the registry
import providers

# Set the environment variable for provider registry
os.environ.setdefault(
    'TESTSUITE_PROVIDER_REGISTRY',
    'providers.registry:provider_registry'
)


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


def _has_oracle_18c_scenario() -> bool:
    """Check scenario config file directly for Oracle 18c.

    We read the YAML config ourselves instead of relying on SCENARIO_MAP,
    because the providers package has no __init__.py and scenarios.py's
    module-level registration may not have executed yet at conftest time.
    """
    import yaml

    config_path = os.getenv("ORACLE_SCENARIOS_CONFIG_PATH")
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), "config", "oracle_scenarios.yaml")
    if not config_path or not os.path.exists(config_path):
        return False
    try:
        with open(config_path) as f:
            data = yaml.safe_load(f)
        scenarios = data.get("scenarios", {}) or {}
        return any("18c" in name for name in scenarios)
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    """Skip supplementary-plane Unicode tests on Oracle 18c."""
    has_18c = _has_oracle_18c_scenario()
    print(f"[conftest] _has_oracle_18c_scenario() = {has_18c}, "
          f"ORACLE_SCENARIOS_CONFIG_PATH={os.getenv('ORACLE_SCENARIOS_CONFIG_PATH')}")
    if not has_18c:
        return

    skip_reason = (
        "Oracle 18c AL32UTF8 cannot round-trip supplementary-plane characters "
        "(U+10000+). BMP-only tests cover Oracle 18c's usable charset."
    )
    skip_marker = pytest.mark.skip(reason=skip_reason)

    skipped = []
    for item in items:
        if item.name in _ORACLE_18C_SKIP_TEST_NAMES:
            item.add_marker(skip_marker)
            skipped.append(item.name)
    print(f"[conftest] Marked {len(skipped)} tests as skip: {skipped}")


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment before running tests."""
    yield
