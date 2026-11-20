# tests/conftest.py
"""Root configuration for the Oracle backend's test tree.

The backend knows its own classes and how a scenario maps to a connection, so
no declaration table is needed here: the scenario is exposed as a parameterized
pytest fixture that the testsuite environment consumes.
"""

import pytest

import support.oracle

_SCENARIOS = [support.oracle.scenario(name) for name in support.oracle.SCENARIOS]


@pytest.fixture(scope="session", params=_SCENARIOS, ids=lambda scenario: scenario.name)
def backend_scenario(request):
    """Return the scenario the tests run against; its id is the scenario name."""
    return request.param


# --- Oracle 18c skip configuration ---
#
# Oracle 18c's AL32UTF8 charset cannot round-trip supplementary-plane
# characters (U+10000+, e.g. most emoji).  The testsuite provides both
# full-Unicode tests and BMP-only tests.  On Oracle 18c we skip the
# full-Unicode tests and keep only the BMP-only tests.

_ORACLE_18C_SKIP_TEST_NAMES = frozenset(
    {
        "test_unicode_multilingual_content_round_trip",
        "test_unicode_emoji_burst_truncated_in_summary",
        "test_unicode_json_fixture_json_field_round_trip",
    }
)

_SKIP_REASON = (
    "Oracle 18c AL32UTF8 cannot round-trip supplementary-plane characters "
    "(U+10000+). BMP-only tests cover Oracle 18c's usable charset."
)


def _detect_oracle_18c() -> bool:
    """Whether any enabled scenario is Oracle 18c."""
    return any("18c" in name for name in support.oracle.SCENARIO_MAP)


def pytest_collection_modifyitems(config, items):
    """Skip tests the enabled scenario's charset cannot round-trip."""
    if not _detect_oracle_18c():
        return

    skip_marker = pytest.mark.skip(reason=_SKIP_REASON)
    for item in items:
        # item.name includes parametrize suffix, e.g. "test_foo[oracle_18c]"
        if any(
            item.name == skip_name or item.name.startswith(skip_name + "[")
            for skip_name in _ORACLE_18C_SKIP_TEST_NAMES
        ):
            item.add_marker(skip_marker)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment before running tests."""
    yield
