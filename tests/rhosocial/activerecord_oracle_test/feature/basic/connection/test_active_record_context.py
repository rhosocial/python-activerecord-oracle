# tests/rhosocial/activerecord_oracle_test/feature/basic/connection/test_active_record_context.py
"""
Basic ActiveRecord Context Test Module for Oracle backend.

This module imports and runs the shared tests from the testsuite package,
ensuring Oracle backend compatibility for connection pool context awareness.
"""


# Import shared tests from testsuite package
from rhosocial.activerecord.testsuite.feature.basic.connection.test_active_record_context import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.basic.connection.test_active_record_context_async import *  # noqa: F403

