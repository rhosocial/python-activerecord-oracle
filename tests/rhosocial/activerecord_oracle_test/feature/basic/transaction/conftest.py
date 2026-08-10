# tests/rhosocial/activerecord_oracle_test/feature/basic/transaction/conftest.py
"""
Pytest configuration for basic transaction feature tests for Oracle backend.
"""

from rhosocial.activerecord.testsuite.feature.basic.transaction.conftest import *  # noqa: F403


# Replace sync_pool_and_model with an oracle-specific override that resets
# ``AsyncTestUser.__backend__`` to ``None`` *before* the provider runs.
# This is a defensive measure for the CI environment where the previous
# async tests (which import the same module) leave ``AsyncTestUser.__backend__``
# pointed at an ``AsyncOracleBackend`` and then somehow contaminate the
# sync fixture's view of ``sync_pool_and_model``.
import pytest as _pytest
from rhosocial.activerecord.testsuite.core.registry import get_provider_registry as _gpr

_PROVIDER_KEY = "feature.basic.transaction.ITransactionBasicProvider"
_FALLBACK_KEY = "feature.basic.connection.IBasicConnectionProvider"


def _oracle_scenarios():
    reg = _gpr()
    cls = reg.get_provider(_PROVIDER_KEY)
    if cls is None:
        cls = reg.get_provider(_FALLBACK_KEY)
    if cls is None:
        return []
    return cls().get_test_scenarios()


_oracle_scenarios_list = _oracle_scenarios()
_ORACLE_SCENARIO_PARAMS = _oracle_scenarios_list if _oracle_scenarios_list else [
    _pytest.param("default", marks=_pytest.mark.skip(reason="No oracle transaction basic provider found"))
]


@_pytest.fixture(scope="function", params=_ORACLE_SCENARIO_PARAMS)
def sync_pool_and_model(request):
    """Oracle-specific override that bypasses the testsuite star-import path.

    Earlier runs saw ``transaction_manager.savepoint()`` return a coroutine
    because the resolved backend was ``AsyncOracleBackend`` instead of the
    sync ``OracleBackend``.  Building the provider explicitly here forces
    a fresh ``OracleBackend`` factory even in the CI environment.
    """
    import logging as _logging
    _log = _logging.getLogger("rhosocial.activerecord.oracle.test.debug")
    _log.error("ORACLE_SYNC_POOL_AND_MODEL entry scenario=%s", request.param)
    scenario = request.param
    reg = _gpr()
    cls = reg.get_provider(_PROVIDER_KEY) or reg.get_provider(_FALLBACK_KEY)
    provider = cls()
    pool, model = provider.setup_sync_pool_and_model(scenario)
    _log.error("ORACLE_SYNC_POOL_AND_MODEL returned pool=%s model=%s",
               type(pool).__name__, type(model).__name__)
    yield pool, model
    provider.cleanup_sync(scenario, pool)

