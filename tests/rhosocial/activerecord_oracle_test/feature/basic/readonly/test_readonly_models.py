# tests/rhosocial/activerecord_oracle_test/feature/basic/readonly/test_readonly_models.py
"""
Bridge file for read-only model contract tests from the testsuite.

This file imports the generic sync and async tests from the testsuite package
and makes them discoverable by pytest in this project's test run.
"""

from rhosocial.activerecord.testsuite.feature.basic.readonly.test_readonly_models import *  # noqa: F403
