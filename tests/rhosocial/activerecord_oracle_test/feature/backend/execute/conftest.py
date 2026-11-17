# tests/rhosocial/activerecord_oracle_test/feature/backend/execute/conftest.py
"""Declare the execute tree serial per directory.

Every leaf under ``execute/`` owns one logical database (see ``SHARDS.md`` and
``_database_for`` in the parent conftest); its first fixture request drops and
recreates that schema user.  Declaring ``parallel=True`` here is inherited by
every leaf, and the testsuite plugin groups items by their own directory, so
each leaf stays on one worker while different leaves still run in parallel.
"""
from rhosocial.activerecord.testsuite.pytest_plugin import Declaration

DECLARATION = Declaration(
    parallel=True,
    purpose="Backend execute tests that create tables in their directory's own schema.",
    passes_means="statements execute correctly against the directory's live schema",
    fails_means="a statement fails or corrupts the directory's schema",
)
