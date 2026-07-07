# tests/rhosocial/activerecord_oracle_test/feature/query/test_relations_basic.py
"""
Bridge file for test_relations_basic tests from the testsuite.

This file imports the generic tests from the testsuite package and makes them
discoverable by pytest in this project's test run.
"""
from rhosocial.activerecord.testsuite.feature.query.test_relations_basic import *
from rhosocial.activerecord.testsuite.feature.query.test_relations_basic_async import *  # noqa: F403

