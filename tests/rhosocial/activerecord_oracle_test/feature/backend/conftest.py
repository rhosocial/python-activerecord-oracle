# tests/rhosocial/activerecord_oracle_test/feature/backend/conftest.py
"""Fixtures for the backend-owned tests.

The backend runs these against a real server. The scenario supplies the
connection parameters; the logical database name is derived from the
directory that owns the test, so two directories never share tables.
"""

import pytest
import pytest_asyncio

import support.oracle

from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.backend.async_backend import (
    AsyncOracleBackend,
)


def get_enabled_scenarios():
    """Return the scenarios this backend enables, as a name -> server map."""
    return support.oracle.get_enabled_scenarios()


def get_scenario_names():
    return list(get_enabled_scenarios().keys())


_PREPARED_DATABASES = set()


def _database_for(request) -> str:
    """Derive the logical database the directory owning the test runs in."""
    parts = list(request.node.path.parts)
    try:
        index = max(i for i, part in enumerate(parts) if part == "backend")
    except ValueError:
        return "test_backend"
    names = [part for part in parts[index + 1 : -1] if part]
    return "test_backend" + "".join("_" + name for name in names)


def _prepare_database(scenario_name: str, database: str) -> None:
    """Create the directory's database once per scenario."""
    key = (scenario_name, database)
    if key in _PREPARED_DATABASES:
        return
    support.oracle.scenario(scenario_name).reset_database(database)
    _PREPARED_DATABASES.add(key)


def get_scenario(name: str, database: str = "test_backend"):
    """Return ``(backend_class, config)`` for a directory's logical database."""
    if name not in get_enabled_scenarios():
        if get_enabled_scenarios():
            name = next(iter(get_enabled_scenarios()))
        else:
            raise ValueError("No scenarios registered")
    _prepare_database(name, database)
    return OracleBackend, support.oracle.scenario(name).make_config(database)


def pytest_collection_modifyitems(config, items):
    """Pin a backend directory's tests to one worker.

    These directories create and drop tables in the directory's own database,
    so two workers must never run tests from the same directory at once.
    Modules that carry an explicit ``xdist_group`` keep their own marker:
    that group spans directories on purpose (the shared AR_CRM/AR_SHOP
    schema users), and xdist concatenates every group name an item carries.
    """
    for item in items:
        parts = item.path.parts
        if "backend" not in parts:
            continue
        if item.get_closest_marker("xdist_group") is not None:
            continue
        item.add_marker(
            pytest.mark.xdist_group("oracle-backend::" + str(item.path.parent))
        )


# --- Provider Logic for Fixtures ---


class BackendFeatureProvider:
    def __init__(self):
        self._backend = None
        self._async_backend = None

    def setup_backend(self, scenario_name: str, database: str):
        if self._backend:
            return self._backend
        backend_class, config = get_scenario(scenario_name, database)
        self._backend = backend_class(connection_config=config)
        self._backend.connect()
        # Introspect and adapt to detect server version and features.
        self._backend.introspect_and_adapt()
        return self._backend

    async def setup_async_backend(self, scenario_name: str, database: str):
        if self._async_backend:
            return self._async_backend
        _, config = get_scenario(scenario_name, database)
        self._async_backend = AsyncOracleBackend(connection_config=config)
        await self._async_backend.connect()
        # Introspect and adapt to detect server version and features.
        await self._async_backend.introspect_and_adapt()
        return self._async_backend

    def cleanup(self):
        if self._backend:
            self._backend.disconnect()
            self._backend = None

    async def async_cleanup(self):
        if self._async_backend:
            await self._async_backend.disconnect()
            self._async_backend = None


# --- Fixtures ---


@pytest.fixture(scope="function", params=get_scenario_names())
def oracle_backend(request):
    scenario_name = request.param
    provider = BackendFeatureProvider()
    backend = provider.setup_backend(scenario_name, _database_for(request))
    yield backend
    provider.cleanup()


@pytest.fixture(scope="function")
def oracle_backend_single(request):
    """Non-parameterized fixture using the first available scenario."""
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No Oracle scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = provider.setup_backend(scenario_name, _database_for(request))
    yield backend
    provider.cleanup()


@pytest_asyncio.fixture(scope="function", params=get_scenario_names())
async def async_oracle_backend(request):
    scenario_name = request.param
    provider = BackendFeatureProvider()
    backend = await provider.setup_async_backend(scenario_name, _database_for(request))
    yield backend
    await provider.async_cleanup()


@pytest_asyncio.fixture(scope="function")
async def async_oracle_backend_single(request):
    """Non-parameterized async fixture using the first available scenario."""
    scenario_names = get_scenario_names()
    if not scenario_names:
        pytest.skip("No Oracle scenarios configured")
    scenario_name = scenario_names[0]
    provider = BackendFeatureProvider()
    backend = await provider.setup_async_backend(scenario_name, _database_for(request))
    yield backend
    await provider.async_cleanup()
