# tests/rhosocial/activerecord_oracle_test/feature/basic/worker/test_worker_pool_lifecycle.py
"""
Bridge file for worker pool lifecycle tests.

Imports tests from testsuite and makes them discoverable by pytest.
"""

# Keep this explicit fixture import: these bridge modules import testsuite test
# classes directly, so pytest does not load the testsuite worker conftest.py
# by directory ancestry. The imported name is consumed by pytest fixture lookup.
from rhosocial.activerecord.testsuite.feature.basic.worker.conftest import user_class_for_worker  # noqa: F401
from rhosocial.activerecord.testsuite.feature.basic.worker.test_worker_pool_lifecycle import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.basic.worker.test_worker_pool_lifecycle_async import *  # noqa: F403

