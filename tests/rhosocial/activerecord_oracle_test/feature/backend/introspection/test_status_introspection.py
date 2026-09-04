# tests/rhosocial/activerecord_oracle_test/feature/backend/introspection/test_status_introspection.py
"""Live-server coverage for the Oracle status introspector.

The ``.introspector.status`` surface (overview, configuration, performance,
connection/storage info, users) previously had no direct tests; it was a
noted coverage gap in .claude/plan/2026-08-26/coverage-restoration.md.
"""
import pytest

from providers.scenarios import get_scenario_raw


@pytest.fixture(scope="module")
def connected_backend():
    backend_class, config = get_scenario_raw("oracle_23c")
    backend = backend_class(connection_config=config)
    backend.connect()
    yield backend
    try:
        backend.disconnect()
    except Exception:
        pass


class TestSyncStatusIntrospection:
    def test_overview_reports_version_and_vendor(self, connected_backend):
        overview = connected_backend.introspector.status.get_overview()
        assert overview.server_vendor == "Oracle"
        assert overview.server_version and "Oracle" in overview.server_version

    def test_list_configuration_returns_items(self, connected_backend):
        items = connected_backend.introspector.status.list_configuration()
        assert items, "expected at least one configuration parameter"
        first = items[0]
        assert first.name and first.value is not None

    def test_list_performance_metrics_returns_items(self, connected_backend):
        items = connected_backend.introspector.status.list_performance_metrics()
        assert all(item.name for item in items)

    def test_connection_info_shape(self, connected_backend):
        info = connected_backend.introspector.status.get_connection_info()
        assert info is not None

    def test_storage_info_reports_tablespace(self, connected_backend):
        storage = connected_backend.introspector.status.get_storage_info()
        assert storage is not None

    def test_list_users_contains_system(self, connected_backend):
        users = connected_backend.introspector.status.list_users()
        names = {u.name.upper() for u in users}
        assert "SYSTEM" in names


@pytest.mark.asyncio
async def test_async_status_overview():
    from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

    _, config = get_scenario_raw("oracle_23c")
    backend = AsyncOracleBackend(connection_config=config)
    await backend.connect()
    try:
        overview = await backend.introspector.status.get_overview()
        assert overview.server_vendor == "Oracle"
    finally:
        try:
            await backend.disconnect()
        except Exception:
            pass
