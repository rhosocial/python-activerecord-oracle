# tests/rhosocial/activerecord_oracle_test/feature/basic/transaction/conftest.py
"""
Pytest configuration for basic transaction feature tests for Oracle backend.
"""
import sys as _sys
print("DEBUG_TX_CONFTEST LOADED module=%s" % __name__, file=_sys.stderr, flush=True)

from rhosocial.activerecord.testsuite.feature.basic.transaction.conftest import *  # noqa: F403
print("DEBUG_TX_CONFTEST IMPORT_DONE", file=_sys.stderr, flush=True)

