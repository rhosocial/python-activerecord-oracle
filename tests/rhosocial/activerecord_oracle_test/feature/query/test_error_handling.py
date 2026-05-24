# tests/rhosocial/activerecord_oracle_test/feature/query/test_error_handling.py
"""
Bridge file for error handling tests from the testsuite.

This file imports the generic tests from the testsuite package and makes them
discoverable by pytest in this project's test run.
"""
from rhosocial.activerecord.testsuite.feature.query.conftest import (
    order_fixtures,
)
from rhosocial.activerecord.testsuite.feature.query.test_error_handling import *
