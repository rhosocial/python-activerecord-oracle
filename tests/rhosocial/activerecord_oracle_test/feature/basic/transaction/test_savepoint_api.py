# tests/rhosocial/activerecord_oracle_test/feature/basic/transaction/test_savepoint_api.py
"""
Black-box contracts for savepoint APIs (Oracle backend).
"""
import sys as _sys
print("DEBUG_SAVEPOINT_TEST FILE LOADED", file=_sys.stderr, flush=True)

from rhosocial.activerecord.testsuite.feature.basic.transaction.test_savepoint_api import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.basic.transaction.test_savepoint_api_async import *  # noqa: F403
print("DEBUG_SAVEPOINT_TEST IMPORTS DONE", file=_sys.stderr, flush=True)
