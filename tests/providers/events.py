"""Oracle backend Events provider implementation."""
from typing import Type
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.testsuite.feature.events.interfaces import IEventsProvider
from .scenarios import get_scenario


class EventsProvider(IEventsProvider):
    """Oracle backend implementation for events test."""

    def __init__(self):
        self._active_backends = []

    def get_test_scenarios(self):
        from .scenarios import get_enabled_scenarios
        return list(get_enabled_scenarios().keys())

    def setup_event_test_model(self, scenario_name: str) -> Type[ActiveRecord]:
        # Placeholder implementation
        raise NotImplementedError("Events provider not yet implemented for Oracle")

    def cleanup_after_test(self, scenario_name: str):
        for backend_instance in self._active_backends:
            try:
                backend_instance.disconnect()
            except Exception:
                pass
        self._active_backends.clear()
