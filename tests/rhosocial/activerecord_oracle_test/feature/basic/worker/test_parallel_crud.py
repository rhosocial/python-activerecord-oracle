# tests/rhosocial/activerecord_oracle_test/feature/basic/worker/test_parallel_crud.py
"""
Bridge file for parallel CRUD worker tests.

Imports tests from testsuite and makes them discoverable by pytest.
"""

# Keep these explicit fixture imports: these bridge modules import testsuite test
# classes directly, so pytest does not load the testsuite worker conftest.py
# by directory ancestry. The imported names are consumed by pytest fixture lookup.
from rhosocial.activerecord.testsuite.feature.basic.worker.conftest import (  # noqa: F401
    async_user_class_for_worker,
    user_class_for_worker,
)
from rhosocial.activerecord.testsuite.feature.basic.worker.test_parallel_crud import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.basic.worker.test_parallel_crud_async import *  # noqa: F403

