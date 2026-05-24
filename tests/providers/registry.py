"""
Test Provider Registry

This module registers concrete implementations of test suite interfaces.
The registry system allows the test suite to be decoupled from specific
backend implementations, enabling the same tests to run against different
database backends.
"""
from rhosocial.activerecord.testsuite.core.registry import ProviderRegistry
from .basic import BasicProvider
from .query import QueryProvider

# Create a single, global instance of the ProviderRegistry.
provider_registry = ProviderRegistry()

# Register the concrete `BasicProvider` as the implementation for the
# `feature.basic.IBasicProvider` interface defined in the testsuite.
provider_registry.register("feature.basic.IBasicProvider", BasicProvider)
provider_registry.register("feature.query.IQueryProvider", QueryProvider)
