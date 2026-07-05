# tests/providers/registry.py
"""
Test Provider Registry

This module registers concrete implementations of test suite interfaces.
The registry system allows the test suite to be decoupled from specific
backend implementations, enabling the same tests to run against different
database backends.
"""
from rhosocial.activerecord.testsuite.core.registry import ProviderRegistry
from .basic import BasicProvider
from .events import EventsProvider
from .mixins import MixinsProvider
from .query import QueryProvider
from .relation import RelationProvider
from .composite_pk import CompositePKProvider
from .basic_connection import BasicConnectionProvider
from .query_connection import QueryConnectionProvider
from .crud_benchmark import CrudBenchmarkProvider
from .query_benchmark import QueryBenchmarkProvider
from .mixin_benchmark import MixinBenchmarkProvider
from .transaction_benchmark import TransactionBenchmarkProvider

# Create a single, global instance of the ProviderRegistry.
provider_registry = ProviderRegistry()

# Register the concrete implementations as the providers for the
# interfaces defined in the testsuite.
provider_registry.register("feature.basic.IBasicProvider", BasicProvider)
provider_registry.register("feature.events.IEventsProvider", EventsProvider)
provider_registry.register("feature.mixins.IMixinsProvider", MixinsProvider)
provider_registry.register("feature.query.IQueryProvider", QueryProvider)
provider_registry.register("feature.relation.IRelationProvider", RelationProvider)
provider_registry.register("feature.composite_pk.ICompositePKProvider", CompositePKProvider)
provider_registry.register(
    "feature.basic.connection.IBasicConnectionProvider",
    BasicConnectionProvider,
)
provider_registry.register(
    "feature.query.connection.IQueryConnectionProvider",
    QueryConnectionProvider,
)

# Register benchmark providers.
provider_registry.register("benchmark.crud.ICrudBenchmarkProvider", CrudBenchmarkProvider)
provider_registry.register("benchmark.query.IQueryBenchmarkProvider", QueryBenchmarkProvider)
provider_registry.register("benchmark.mixin.IMixinBenchmarkProvider", MixinBenchmarkProvider)
provider_registry.register(
    "benchmark.transaction.ITransactionBenchmarkProvider",
    TransactionBenchmarkProvider,
)
