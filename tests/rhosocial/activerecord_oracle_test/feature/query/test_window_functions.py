# tests/rhosocial/activerecord_oracle_test/feature/query/test_window_functions.py
"""
Bridge file for test_window_functions tests from the testsuite.

This file imports the generic tests from the testsuite package and makes them
discoverable by pytest in this project's test run.
"""
from rhosocial.activerecord.testsuite.feature.query.test_window_functions import *
from rhosocial.activerecord.testsuite.feature.query.test_window_functions_async import *  # noqa: F403

