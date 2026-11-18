# tests/rhosocial/activerecord_oracle_test/feature/backend/execute/conftest.py
"""Declare the execute tree serial per directory.

Every leaf under ``execute/`` owns one logical database (see ``SHARDS.md`` and
``_database_for`` in the parent conftest); its first fixture request drops and
recreates that schema user.  Declaring ``parallel=True`` here is inherited by
every leaf, and the testsuite plugin groups items by their own directory, so
each leaf stays on one worker while different leaves still run in parallel.
"""

import pytest

from rhosocial.activerecord.testsuite.pytest_plugin import Declaration

DECLARATION = Declaration(
    uses_database=True,
    parallel=True,
    purpose="Backend execute tests that create tables in their directory's own schema.",
    passes_means="statements execute correctly against the directory's live schema",
    fails_means="a statement fails or corrupts the directory's schema",
)

# Modules that drop and recreate the SAME AR_CRM / AR_SHOP users must share one
# xdist worker. The testsuite plugin adds a per-directory ``xdist_group`` marker
# on top of the modules' explicit ``oracle_schema_dml`` marker, and xdist joins
# all of a test's group names into its scheduling key -- so crud/, dml_insert/
# and introspection/ land on different workers. One worker then drops a schema
# under another worker's INSERT (ORA-00942) or between CREATE USER and
# CREATE TABLE (ORA-01918). Collapse those tests back to the single shared
# group; every other leaf keeps its own directory group and parallelism.
_SHARED_SCHEMA_GROUP = "oracle_schema_dml"


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items):
    """Pin the AR_* schema-sharing modules to one worker across directories."""
    for item in items:
        nodeid = item.nodeid
        # xdist appends ``@<joined groups>`` to the node id during collection.
        if "@" not in nodeid:
            continue
        base, _, groups = nodeid.rpartition("@")
        if _SHARED_SCHEMA_GROUP not in groups:
            continue
        item._nodeid = f"{base}@{_SHARED_SCHEMA_GROUP}"
        # pytest 9's ``nodeid`` reads ``_id``; xdist's scheduler and the
        # collection report both consume that value.
        item._id = item._nodeid
