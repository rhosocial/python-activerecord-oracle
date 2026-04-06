# tests/conftest.py
"""
Root pytest configuration file for the rhosocial-activerecord-oracle package.

Configures the environment for the testsuite to find backend-specific implementations.
"""
import os
import pytest

# Import providers to register them with the registry
import providers

# Set the environment variable for provider registry
os.environ.setdefault(
    'TESTSUITE_PROVIDER_REGISTRY',
    'providers.registry:provider_registry'
)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment before running tests."""
    yield
