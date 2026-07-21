# tests/rhosocial/activerecord_oracle_test/feature/mixins/test_optimistic_lock.py
"""
Test optimistic locking functionality for Oracle backend.

This module imports and runs the shared tests from the testsuite package,
ensuring Oracle backend compatibility.
"""

# Import shared tests from testsuite package
from rhosocial.activerecord.testsuite.feature.mixins.test_optimistic_lock import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.mixins.test_optimistic_lock_async import *  # noqa: F403

