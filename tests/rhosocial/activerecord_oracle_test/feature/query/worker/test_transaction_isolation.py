# tests/rhosocial/activerecord_oracle_test/feature/query/worker/test_transaction_isolation.py
"""
Bridge file for transaction isolation worker tests.

Imports tests from testsuite and makes them discoverable by pytest.
"""

# Keep these explicit fixture imports: these bridge modules import testsuite test
# classes directly, so pytest does not load the testsuite worker conftest.py
# by directory ancestry. The imported names are consumed by pytest fixture lookup.
from rhosocial.activerecord.testsuite.feature.query.worker.conftest import (  # noqa: F401
    async_order_fixtures_for_worker,
    order_fixtures_for_worker,
)
from rhosocial.activerecord.testsuite.feature.query.worker.test_transaction_isolation import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.query.worker.test_transaction_isolation_async import *  # noqa: F403

