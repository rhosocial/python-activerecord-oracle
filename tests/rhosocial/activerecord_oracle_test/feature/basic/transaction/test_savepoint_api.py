# tests/rhosocial/activerecord_oracle_test/feature/basic/transaction/test_savepoint_api.py
"""
Black-box contracts for savepoint APIs (Oracle backend).
"""
import sys as _sys
print("DEBUG_SAVEPOINT_TEST FILE LOADED", file=_sys.stderr, flush=True)
sys.stderr.flush()

from rhosocial.activerecord.testsuite.feature.basic.transaction.test_savepoint_api import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.basic.transaction.test_savepoint_api_async import *  # noqa: F403
print("DEBUG_SAVEPOINT_TEST IMPORTS DONE", file=_sys.stderr, flush=True)
sys.stderr.flush()

# Wrap TestSavepointCreate.test_savepoint_autoname to log
_orig = TestSavepointCreate.test_savepoint_autoname_is_returned_and_prefixed
def _wrapped(self, *args, **kwargs):
    print("DEBUG_SAVEPOINT_TEST test_method_entered backend=%s" % type(args[0]).__name__ if args else "no_args", file=_sys.stderr, flush=True)
    return _orig(self, *args, **kwargs)
TestSavepointCreate.test_savepoint_autoname_is_returned_and_prefixed = _wrapped

