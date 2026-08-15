# tests/rhosocial/activerecord_oracle_test/feature/query/conftest.py
"""
Pytest configuration for query feature tests.

This file imports fixtures from the corresponding testsuite, making them
available to the tests in this directory.
"""

# Keep these explicit backend fixture imports: query tests use oracle_backend
# fixtures from the sibling backend test tree, which pytest will not load by
# directory ancestry. The imported names are consumed by pytest fixture lookup.
from rhosocial.activerecord_oracle_test.feature.backend.conftest import (  # noqa: F401
    oracle_backend,
    oracle_backend_single,
)

from rhosocial.activerecord.testsuite.feature.query.conftest import *  # noqa: F403
