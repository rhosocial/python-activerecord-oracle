# tests/rhosocial/activerecord_oracle_test/feature/relation/conftest.py
"""
Pytest configuration for relation feature tests.

This file imports fixtures from the corresponding testsuite, making them
available to the tests in this directory.
"""

from rhosocial.activerecord.testsuite.feature.relation.conftest import *  # noqa: F403

import pytest_asyncio


@pytest_asyncio.fixture(scope="function", params=SCENARIO_PARAMS_ASYNC)  # noqa: F405
async def async_user_class(request):
    from rhosocial.activerecord.testsuite.core.registry import get_provider_registry

    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider(PROVIDER_KEY_ASYNC)  # noqa: F405
    provider = provider_class()
    model = await provider.setup_user_model(scenario)
    yield model
    await provider.cleanup_after_test(scenario)


@pytest_asyncio.fixture(scope="function", params=SCENARIO_PARAMS_ASYNC)  # noqa: F405
async def async_post_class(request):
    from rhosocial.activerecord.testsuite.core.registry import get_provider_registry

    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider(PROVIDER_KEY_ASYNC)  # noqa: F405
    provider = provider_class()
    model = await provider.setup_post_model(scenario)
    yield model
    await provider.cleanup_after_test(scenario)


@pytest_asyncio.fixture(scope="function", params=SCENARIO_PARAMS_ASYNC)  # noqa: F405
async def async_comment_class(request):
    from rhosocial.activerecord.testsuite.core.registry import get_provider_registry

    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider(PROVIDER_KEY_ASYNC)  # noqa: F405
    provider = provider_class()
    model = await provider.setup_comment_model(scenario)
    yield model
    await provider.cleanup_after_test(scenario)


@pytest_asyncio.fixture(scope="function", params=SCENARIO_PARAMS_ASYNC)  # noqa: F405
async def async_user_post_comment_classes(request):
    from rhosocial.activerecord.testsuite.core.registry import get_provider_registry

    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider(PROVIDER_KEY_ASYNC)  # noqa: F405
    provider = provider_class()
    user = await provider.setup_user_model(scenario)
    post = await provider.setup_post_model(scenario)
    comment = await provider.setup_comment_model(scenario)
    yield user, post, comment
    await provider.cleanup_after_test(scenario)


@pytest_asyncio.fixture(scope="function", params=SCENARIO_PARAMS_ASYNC)  # noqa: F405
async def async_relation_boundary_context(request):
    from rhosocial.activerecord.testsuite.core.registry import get_provider_registry

    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider(PROVIDER_KEY_ASYNC)  # noqa: F405
    provider = provider_class()
    owner, profile, post = await provider.setup_relation_boundary_fixtures(scenario)
    yield provider, scenario, owner, profile, post
    await provider.cleanup_after_test(scenario)
