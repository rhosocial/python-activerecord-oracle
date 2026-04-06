# tests/rhosocial/activerecord_oracle_test/feature/basic/test_crud.py
"""
This is a "bridge" file for the basic features test group.

Its purpose is to import the generic tests from the `rhosocial-activerecord-testsuite`
package and make them discoverable by `pytest` within this project's test run.
"""

# Import fixtures from the testsuite
from rhosocial.activerecord.testsuite.feature.basic.conftest import (
    user_class,
    type_case_class,
    type_test_model,
    validated_user_class,
    async_user_class,
    async_type_case_class,
    async_validated_user_class,
)

# Import all test functions from the generic testsuite
from rhosocial.activerecord.testsuite.feature.basic.test_crud import *
