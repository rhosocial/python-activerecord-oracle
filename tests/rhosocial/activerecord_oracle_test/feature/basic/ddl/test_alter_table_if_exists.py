# tests/rhosocial/activerecord_oracle_test/feature/basic/ddl/test_alter_table_if_exists.py
"""
ALTER TABLE IF [NOT] EXISTS tests (sync) for the Oracle backend.

Thin bridge that runs the shared testsuite contract against a bare Oracle
dialect. Oracle (<= 19c) rejects all three ``IF [NOT] EXISTS`` modifiers, so
the modifier tests are skipped via ``@requires_protocol`` and the plain forms
are exercised directly.
"""

from rhosocial.activerecord.testsuite.feature.basic.ddl.test_alter_table_if_exists import *  # noqa: F403
