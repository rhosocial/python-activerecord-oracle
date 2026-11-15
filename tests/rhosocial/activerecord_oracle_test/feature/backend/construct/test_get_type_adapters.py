# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/test_get_type_adapters.py
"""The backend's adapter inventory is a catalogue, not a recommendation.

Oracle-specific adapters are registered during construction and never need a
server, so the whole file runs without a database. What is pinned here is the
shape of ``get_type_adapters()``: it implements the core protocol, it lists
classes once each, and it answers in the same order every time.
"""

import pytest

from rhosocial.activerecord.backend.impl.oracle.backend import (
    AsyncOracleBackend,
    OracleBackend,
)
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.backend.type_adapter import TypeAdapterCatalogProtocol


def _adapter_class_key(adapter):
    adapter_class = type(adapter)
    return (adapter_class.__module__, adapter_class.__qualname__)


def _config():
    """An inert connection config: construction never opens a socket."""
    return OracleConnectionConfig(
        host="localhost",
        port=1521,
        service_name="UNIT",
        username="unit",
        password="unit",
    )


@pytest.fixture(params=[OracleBackend, AsyncOracleBackend])
def backend(request):
    """A constructed backend: neither branch opens a connection."""
    return request.param(connection_config=_config())


class TestGetTypeAdapters:
    def test_implements_the_catalogue_protocol(self, backend):
        assert isinstance(backend, TypeAdapterCatalogProtocol)

    def test_inventory_is_not_empty(self, backend):
        assert backend.get_type_adapters()

    def test_each_adapter_class_appears_once(self, backend):
        keys = [_adapter_class_key(adapter) for adapter in backend.get_type_adapters()]
        assert len(keys) == len(set(keys))

    def test_order_is_deterministic_and_sorted_by_class(self, backend):
        first = [_adapter_class_key(adapter) for adapter in backend.get_type_adapters()]
        second = [_adapter_class_key(adapter) for adapter in backend.get_type_adapters()]
        assert first == second
        assert first == sorted(first)

    def test_a_new_list_is_returned_on_each_call(self, backend):
        first = backend.get_type_adapters()
        second = backend.get_type_adapters()
        assert first is not second
        assert first == second

    def test_default_adapter_suggestions_are_still_provided(self, backend):
        suggestions = backend.get_default_adapter_suggestions()
        assert isinstance(suggestions, dict)
        assert suggestions
