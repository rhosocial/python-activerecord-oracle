# tests/rhosocial/activerecord_oracle_test/feature/query/test_query_optimization.py
"""
Bridge file for test_query_optimization tests from the testsuite.

This file imports the generic tests from the testsuite package and makes them
discoverable by pytest in this project's test run.
"""
from rhosocial.activerecord.testsuite.feature.query.test_query_optimization import *
from rhosocial.activerecord.testsuite.feature.query.test_query_optimization_async import *  # noqa: F403

