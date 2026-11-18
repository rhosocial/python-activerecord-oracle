# tests/rhosocial/activerecord_oracle_test/feature/basic/types_complex/conftest.py
"""Use the shared ``basic/types_complex`` test set, gated on GEOMETRY support.

The shared fixture declares its geometry column through
``CustomType(raw="GEOMETRY")``, which Oracle renders verbatim: DDL *builds*
(the type is a passthrough ``CustomType``), so the shared capability gate --
which only skips when DDL building fails -- never fires, and the server
rejects ``CREATE TABLE`` with ``ORA-00902: invalid datatype``. Oracle has no
``GEOMETRY`` datatype; its spatial type is ``MDSYS.SDO_GEOMETRY`` (and needs
the Spatial option), which the shared, backend-neutral fixture cannot name.
Every wired Oracle scenario rejects the bare type, so this bridge adds the
missing capability gate locally:

* the shared ``schema`` fixture is autouse, so every test in the group builds
  the GEOMETRY table at setup. We override it as opt-in so declaration-only
  tests keep running;
* the tests that actually build or use the tables are skipped with a named
  reason, leaving the declaration-only assertions in place.
"""

from pathlib import Path

import pytest

from rhosocial.activerecord.testsuite.feature.basic.types_complex.conftest import *  # noqa: F401,F403
from rhosocial.activerecord.testsuite.feature.basic.types_complex.conftest import (
    _unsupported_reason,
)

_SKIP_REASON = (
    "Oracle has no GEOMETRY datatype (ORA-00902: invalid datatype): the shared "
    "complex-type fixture declares a CustomType(raw='GEOMETRY') column; "
    "Oracle's spatial type is MDSYS.SDO_GEOMETRY"
)


@pytest.fixture(scope="function")
def schema(database):
    """Opt-in override of the shared autouse fixture.

    Declaration-only tests must not build the GEOMETRY table at setup: Oracle
    rejects it with ORA-00902 before they can assert anything. Tests that need
    the tables request this fixture (directly or through the
    ``complex_*_class`` fixtures) as before.
    """
    why = _unsupported_reason(database.dialect)
    if why is not None:
        pytest.skip(why)
    pytest.skip(_SKIP_REASON)


def pytest_collection_modifyitems(config, items):
    """Skip the GEOMETRY-dependent tests; leave declaration-only tests running."""
    here = Path(__file__).resolve().parent
    for item in items:
        path = Path(str(item.path)).resolve()
        if here not in path.parents:
            continue
        # Only tests that build or use the tables request ``schema`` (directly
        # or through the complex_*_class fixtures). Declaration-only tests do
        # not, so they keep running on Oracle.
        if "schema" not in item.fixturenames:
            continue
        item.add_marker(pytest.mark.skip(reason=_SKIP_REASON))
