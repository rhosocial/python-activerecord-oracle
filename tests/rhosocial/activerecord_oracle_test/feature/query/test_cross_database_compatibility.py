# tests/rhosocial/activerecord_oracle_test/feature/query/test_cross_database_compatibility.py
"""
Bridge file for test_cross_database_compatibility tests from the testsuite.

This file imports the generic tests from the testsuite package and makes them
discoverable by pytest in this project's test run.
"""
from rhosocial.activerecord.testsuite.feature.query.test_cross_database_compatibility import *
from rhosocial.activerecord.testsuite.feature.query.test_cross_database_compatibility_async import *  # noqa: F403

