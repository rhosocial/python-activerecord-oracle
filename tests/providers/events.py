# tests/providers/events.py
"""
Concrete implementations of `IEventsSyncProvider` and `IEventsAsyncProvider`
for the Oracle backend.
"""

import os
import sys
import logging
from typing import Type, List

from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType  # noqa: E402
from rhosocial.activerecord.model import ActiveRecord  # noqa: E402

logger = logging.getLogger(__name__)

from rhosocial.activerecord.testsuite.utils import select_fixture  # noqa: E402

from rhosocial.activerecord.testsuite.feature.events.fixtures.models import (  # noqa: E402
    EventTestModel as EventTestModelBase,
    EventTrackingModel as EventTrackingModelBase,
    AsyncEventTestModel as AsyncEventTestModelBase,
)

EventTestModel310 = EventTrackingModel310 = None

if sys.version_info >= (3, 10):
    try:
        from rhosocial.activerecord.testsuite.feature.events.fixtures.models_py310 import (
            EventTestModel as EventTestModel310,
            EventTrackingModel as EventTrackingModel310,
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.10+ fixtures: {e}")

EventTestModel311 = EventTrackingModel311 = None

if sys.version_info >= (3, 11):
    try:
        from rhosocial.activerecord.testsuite.feature.events.fixtures.models_py311 import (
            EventTestModel as EventTestModel311,
            EventTrackingModel as EventTrackingModel311,
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.11+ fixtures: {e}")

EventTestModel312 = EventTrackingModel312 = None

if sys.version_info >= (3, 12):
    try:
        from rhosocial.activerecord.testsuite.feature.events.fixtures.models_py312 import (
            EventTestModel as EventTestModel312,
            EventTrackingModel as EventTrackingModel312,
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.12+ fixtures: {e}")


def _select_model_class(base_cls, py312_cls, py311_cls, py310_cls, model_name: str) -> Type:
    """Select the most appropriate model class for the current Python version."""
    candidates = [c for c in [py312_cls, py311_cls, py310_cls, base_cls] if c is not None]
    selected = select_fixture(*candidates)
    logger.info(f"Selected {model_name}: {selected.__name__} from {selected.__module__}")
    return selected


EventTestModel = _select_model_class(
    EventTestModelBase, EventTestModel312, EventTestModel311, EventTestModel310, "EventTestModel"
)
AsyncEventTestModel = AsyncEventTestModelBase
EventTrackingModel = _select_model_class(
    EventTrackingModelBase, EventTrackingModel312, EventTrackingModel311, EventTrackingModel310, "EventTrackingModel"
)

from rhosocial.activerecord.testsuite.feature.events.interfaces import (  # noqa: E402
    IEventsSyncProvider,
    IEventsAsyncProvider,
)

from .scenarios import get_enabled_scenarios, get_scenario  # noqa: E402


class EventsProviderBase:
    def __init__(self):
        self._scenario_db_files = {}

    @staticmethod
    def _ddl_options() -> ExecutionOptions:
        return ExecutionOptions(stmt_type=StatementType.DDL)

    def get_test_scenarios(self) -> List[str]:
        return list(get_enabled_scenarios().keys())

    def _load_oracle_schema(self, filename: str) -> str:
        schema_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "rhosocial",
            "activerecord_oracle_test",
            "feature",
            "events",
            "schema",
        )
        schema_path = os.path.join(schema_dir, filename)
        with open(schema_path, "r", encoding="utf-8") as f:
            return f.read()


class EventsSyncProvider(EventsProviderBase, IEventsSyncProvider):
    def __init__(self):
        super().__init__()
        self._active_backends: List = []

    def _drop_table_sync(self, backend_instance, table_name: str) -> None:
        try:
            backend_instance.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=self._ddl_options(),
            )
        except Exception:
            pass

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        backend_class, config = get_scenario(scenario_name)
        model_class.configure(config, backend_class)
        backend_instance = model_class.__backend__
        if backend_instance not in self._active_backends:
            self._active_backends.append(backend_instance)
        self._drop_table_sync(backend_instance, table_name)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        backend_instance.execute(schema_sql, options=self._ddl_options())
        return model_class

    def setup_event_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(EventTestModel, scenario_name, "event_tests")

    def setup_event_tracking_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(EventTrackingModel, scenario_name, "event_tracking_models")

    def cleanup_after_test(self, scenario_name: str):
        for backend_instance in self._active_backends:
            try:
                for table_name in ("event_tracking_models", "event_tests"):
                    try:
                        self._drop_table_sync(backend_instance, table_name)
                    except Exception:
                        pass
            finally:
                try:
                    backend_instance.disconnect()
                except Exception:
                    pass
        self._active_backends.clear()


class EventsAsyncProvider(EventsProviderBase, IEventsAsyncProvider):
    def __init__(self):
        super().__init__()
        self._active_async_backends: List = []

    async def _drop_table_async(self, backend_instance, table_name: str) -> None:
        try:
            await backend_instance.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=self._ddl_options(),
            )
        except Exception:
            pass

    async def _setup_async_model(
        self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str
    ) -> Type[ActiveRecord]:
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

        _, config = get_scenario(scenario_name)
        await model_class.configure(config, AsyncOracleBackend)
        backend_instance = model_class.__backend__
        if backend_instance not in self._active_async_backends:
            self._active_async_backends.append(backend_instance)
        await self._drop_table_async(backend_instance, table_name)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        await backend_instance.execute(schema_sql, options=self._ddl_options())
        return model_class

    async def setup_event_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncEventTestModel, scenario_name, "event_tests")

    async def setup_event_tracking_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncEventTestModel, scenario_name, "event_tracking_models")

    async def cleanup_after_test(self, scenario_name: str):
        for backend_instance in self._active_async_backends:
            try:
                for table_name in ("event_tracking_models", "event_tests"):
                    try:
                        await self._drop_table_async(backend_instance, table_name)
                    except Exception:
                        pass
            finally:
                try:
                    await backend_instance.disconnect()
                except Exception:
                    pass
        self._active_async_backends.clear()
