# tests/providers/crud_benchmark.py
"""Oracle provider for CRUD benchmark tests."""

import sys
from typing import Type

from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.testsuite.benchmark.crud.fixtures.data import (
    make_user_payloads,
    payload_count_for_size,
)
from rhosocial.activerecord.testsuite.benchmark.crud.fixtures.models import (
    AsyncBenchmarkUser as AsyncBenchmarkUserBase,
    BenchmarkUser as BenchmarkUserBase,
)
from rhosocial.activerecord.testsuite.benchmark.crud.interfaces import CrudBenchmarkContext
from rhosocial.activerecord.testsuite.utils import select_fixture

from .scenarios import get_enabled_scenarios, get_scenario

BenchmarkUser310 = AsyncBenchmarkUser310 = None
BenchmarkUser311 = AsyncBenchmarkUser311 = None
BenchmarkUser312 = AsyncBenchmarkUser312 = None

if sys.version_info >= (3, 10):
    from rhosocial.activerecord.testsuite.benchmark.crud.fixtures.models_py310 import (
        AsyncBenchmarkUser as AsyncBenchmarkUser310,
        BenchmarkUser as BenchmarkUser310,
    )

if sys.version_info >= (3, 11):
    from rhosocial.activerecord.testsuite.benchmark.crud.fixtures.models_py311 import (
        AsyncBenchmarkUser as AsyncBenchmarkUser311,
        BenchmarkUser as BenchmarkUser311,
    )

if sys.version_info >= (3, 12):
    from rhosocial.activerecord.testsuite.benchmark.crud.fixtures.models_py312 import (
        AsyncBenchmarkUser as AsyncBenchmarkUser312,
        BenchmarkUser as BenchmarkUser312,
    )


BenchmarkUser = select_fixture(
    *[
        candidate
        for candidate in (BenchmarkUser312, BenchmarkUser311, BenchmarkUser310, BenchmarkUserBase)
        if candidate
    ]
)
AsyncBenchmarkUser = select_fixture(
    *[
        candidate
        for candidate in (
            AsyncBenchmarkUser312,
            AsyncBenchmarkUser311,
            AsyncBenchmarkUser310,
            AsyncBenchmarkUserBase,
        )
        if candidate
    ]
)


class CrudBenchmarkProvider:
    def __init__(self):
        self._active_backends = []
        self._active_async_backends = []

    def get_benchmark_scenarios(self):
        return list(get_enabled_scenarios().keys())

    def setup_benchmark_sync(self, scenario: str, size: str):
        model_class = self._setup_model(BenchmarkUser, scenario)
        payloads = make_user_payloads(payload_count_for_size(size))
        record_ids = []
        for payload in payloads:
            instance = model_class(**payload)
            rows = instance.save()
            if rows != 1 or instance.id is None:
                raise AssertionError("failed to seed sync CRUD benchmark row")
            record_ids.append(instance.id)
        return CrudBenchmarkContext(
            scenario=scenario,
            size=size,
            model_class=model_class,
            payloads=payloads,
            record_ids=record_ids,
            backend_namespace="rhosocial.activerecord.backend.impl.oracle",
            backend_name="oracle",
        )

    def teardown_benchmark_sync(self, scenario: str, context: CrudBenchmarkContext) -> None:
        self._cleanup_sync()

    async def setup_benchmark_async(self, scenario: str, size: str):
        model_class = await self._setup_async_model(AsyncBenchmarkUser, scenario)
        payloads = make_user_payloads(payload_count_for_size(size))
        record_ids = []
        for payload in payloads:
            instance = model_class(**payload)
            rows = await instance.save()
            if rows != 1 or instance.id is None:
                raise AssertionError("failed to seed async CRUD benchmark row")
            record_ids.append(instance.id)
        return CrudBenchmarkContext(
            scenario=scenario,
            size=size,
            model_class=model_class,
            payloads=payloads,
            record_ids=record_ids,
            backend_namespace="rhosocial.activerecord.backend.impl.oracle",
            backend_name="oracle",
        )

    async def teardown_benchmark_async(self, scenario: str, context: CrudBenchmarkContext) -> None:
        await self._cleanup_async()

    def _setup_model(self, model_class: Type[ActiveRecord], scenario: str) -> Type[ActiveRecord]:
        backend_class, config = get_scenario(scenario)
        model_class.configure(config, backend_class)
        self._initialize_schema(model_class.__backend__)
        if model_class.__backend__ not in self._active_backends:
            self._active_backends.append(model_class.__backend__)
        return model_class

    async def _setup_async_model(
        self, model_class: Type[ActiveRecord], scenario: str
    ) -> Type[ActiveRecord]:
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

        _, config = get_scenario(scenario)
        await model_class.configure(config, AsyncOracleBackend)
        await self._initialize_schema_async(model_class.__backend__)
        if model_class.__backend__ not in self._active_async_backends:
            self._active_async_backends.append(model_class.__backend__)
        return model_class

    def _initialize_schema(self, backend):
        options = ExecutionOptions(stmt_type=StatementType.DDL)
        # Oracle thin driver rejects anonymous-PL/SQL blocks via
        # cursor.execute(); delegates to executescript which splits on
        # ``;`` and runs each statement individually.
        backend.executescript(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE benchmark_users PURGE'; "
            "EXCEPTION WHEN OTHERS THEN NULL; END;",
        )
        backend.executescript(self._schema_sql())

    async def _initialize_schema_async(self, backend):
        options = ExecutionOptions(stmt_type=StatementType.DDL)
        # See _initialize_schema above; the async Oracle thin driver has the
        # same multi-statement constraint.
        await backend.executescript(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE benchmark_users PURGE'; "
            "EXCEPTION WHEN OTHERS THEN NULL; END;",
        )
        await backend.executescript(self._schema_sql())

    def _schema_sql(self):
        return """
CREATE TABLE benchmark_users (
    id NUMBER(10) GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    username VARCHAR2(255) NOT NULL,
    email VARCHAR2(255) NOT NULL,
    age INTEGER,
    balance BINARY_DOUBLE DEFAULT 0.0 NOT NULL,
    notes CLOB,
    is_active NUMBER(1) DEFAULT 1 NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""

    def _cleanup_sync(self):
        for backend in self._active_backends:
            try:
                backend.disconnect()
            except Exception:
                pass
        self._active_backends.clear()

    async def _cleanup_async(self):
        for backend in self._active_async_backends:
            try:
                await backend.disconnect()
            except Exception:
                pass
        self._active_async_backends.clear()
