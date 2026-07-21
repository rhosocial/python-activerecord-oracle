# tests/rhosocial/activerecord_oracle_test/feature/mixins/test_soft_delete.py
"""
Test soft delete functionality for Oracle backend.

This module imports and runs the shared tests from the testsuite package,
ensuring Oracle backend compatibility.
"""

# Import shared tests from testsuite package
from rhosocial.activerecord.testsuite.feature.mixins.test_soft_delete import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.mixins.test_soft_delete_async import *  # noqa: F403

