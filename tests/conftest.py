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

# --- Oracle 18c Unicode limitation handling ---
#
# Oracle 18c's AL32UTF8 charset cannot round-trip supplementary-plane
# characters (U+10000+, e.g. most emoji).  The testsuite's full Unicode
# round-trip tests exercise these characters, so they must be skipped on
# Oracle 18c.  Oracle 21c+ handles supplementary characters correctly.
#
# The skip is done here (backend-side) rather than in the shared testsuite,
# keeping the testsuite backend-agnostic.

# Test IDs that exercise supplementary-plane Unicode characters.
_ORACLE_18C_SKIP_TESTS = {
    "test_unicode_multilingual_content_round_trip",
    "test_unicode_emoji_burst_truncated_in_summary",
}


def _is_oracle_18c(config):
    """Check if any active scenario targets Oracle 18c."""
    from providers.scenarios import SCENARIO_MAP
    for name in SCENARIO_MAP:
        if "18c" in name or "18" in name.split("_")[-1]:
            return True
    return False


def pytest_collection_modifyitems(config, items):
    """Skip supplementary-plane Unicode tests on Oracle 18c."""
    if not _is_oracle_18c(config):
        return

    skip_marker = pytest.mark.skip(
        reason=(
            "Oracle 18c's AL32UTF8 charset does not fully support "
            "supplementary-plane emoji (e.g. U+1F950) introduced in "
            "Unicode 10.0+; round-trip corruption expected. "
            "Oracle 21c+ handles these correctly."
        )
    )
    for item in items:
        if item.name in _ORACLE_18C_SKIP_TESTS:
            item.add_marker(skip_marker)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment before running tests."""
    yield
