# tests/rhosocial/activerecord_oracle_test/feature/cli/conftest.py
"""Use the shared ``cli`` fixture group and minimal test set.

The CLI module bundle the tests address by ``module:attribute`` is built by the
testsuite fixture: the backend keeps no fixture of its own here.
"""
from rhosocial.activerecord.testsuite.feature.cli.conftest import *  # noqa: F401,F403
