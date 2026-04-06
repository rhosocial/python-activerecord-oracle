"""Oracle backend Mixins provider implementation."""
from typing import Type
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.testsuite.feature.mixins.interfaces import IMixinsProvider


class MixinsProvider(IMixinsProvider):
    """Oracle backend implementation for mixins test."""

    def __init__(self):
        self._active_backends = []

    def get_test_scenarios(self):
        from .scenarios import get_enabled_scenarios
        return list(get_enabled_scenarios().keys())

    def setup_timestamp_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError("Mixins provider not yet implemented for Oracle")

    def setup_soft_delete_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError("Mixins provider not yet implemented for Oracle")

    def setup_optimistic_lock_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError("Mixins provider not yet implemented for Oracle")

    def cleanup_after_test(self, scenario_name: str):
        for backend_instance in self._active_backends:
            try:
                backend_instance.disconnect()
            except Exception:
                pass
        self._active_backends.clear()
