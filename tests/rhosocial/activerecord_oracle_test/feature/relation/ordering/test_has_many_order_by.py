# tests/rhosocial/activerecord_oracle_test/feature/relation/ordering/test_has_many_order_by.py
"""
Bridge file for HasMany(order_by=...) contract tests from the testsuite.

This file imports the generic sync and async tests from the testsuite package
and makes them discoverable by pytest in this project's test run.
"""

from rhosocial.activerecord.testsuite.feature.relation.ordering.test_has_many_order_by import *  # noqa: F403
