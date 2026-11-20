# tests/benchmark/providers.py
"""Oracle benchmark implementations registered with the shared testsuite.

The testsuite's ``benchmark/*`` contracts obtain a backend implementation from
the global ``benchmarks`` registry.  Registering here (at conftest import time)
is what turns those contracts from "skip: no implementation" into real
measurements against an Oracle service.  Direct ``StorageBackend.execute``
benchmarks stay backend-owned under ``tests/benchmark/backend/`` and do not use
this registry.

Oracle has no database-per-connection the way SQLite has a file: a schema user
*is* the database.  Each feature run therefore gets a fresh, uniquely named
schema user (created via the scenario's ``reset_database``) so runs never share
tables.  DDL is derived from the model via the expression system -- no
hand-built CREATE TABLE SQL.
"""

import uuid
from typing import ClassVar, Optional

from pydantic import EmailStr, Field

from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.base.field_proxy import FieldProxy
from rhosocial.activerecord.field import DefaultTimestampMixin, IntegerPKMixin
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord
from rhosocial.activerecord.testsuite.benchmark.crud.fixtures.data import (
    make_user_payloads,
    payload_count_for_size,
)
from rhosocial.activerecord.testsuite.benchmark.crud.interfaces import CrudBenchmarkContext
from rhosocial.activerecord.testsuite.benchmark.mixin.interfaces import MixinBenchmarkContext
from rhosocial.activerecord.testsuite.benchmark.query.interfaces import QueryBenchmarkContext
from rhosocial.activerecord.testsuite.benchmark.registry import (
    BenchmarkImplementation,
    benchmarks,
)
from rhosocial.activerecord.testsuite.benchmark.transaction.interfaces import (
    TransactionBenchmarkContext,
)

from support.oracle import SCENARIOS, scenario

BACKEND_NAMESPACE = "rhosocial.activerecord.backend.impl.oracle"
BACKEND_NAME = "oracle"

# DDL options used by the derived statements.
DDL = ExecutionOptions(stmt_type=StatementType.DDL)


# --------------------------------------------------------------------------- #
# Minimal benchmark-only models.  Each run uses a fresh schema, so these are
# declared locally instead of reusing feature fixtures.  DDL is derived from
# the model via the expression system (no hand-built SQL).
# --------------------------------------------------------------------------- #
class _BenchUserBase:
    @classmethod
    def table_name(cls):
        return "benchmark_users"

    c: ClassVar[FieldProxy] = FieldProxy()
    id: Optional[int] = None
    username: str
    email: EmailStr
    age: Optional[int] = Field(..., ge=0, le=120)
    balance: float = 0.0
    notes: Optional[str] = None
    is_active: bool = True


class BenchUser(IntegerPKMixin, DefaultTimestampMixin, _BenchUserBase, ActiveRecord):
    pass


class AsyncBenchUser(IntegerPKMixin, DefaultTimestampMixin, _BenchUserBase, AsyncActiveRecord):
    pass


def _schema_name(scenario_name: str, tag: str) -> str:
    """A fresh Oracle schema username (letters/digits/underscore only).

    Oracle identifiers must begin with a letter and contain only alphanumeric
    characters (plus ``_``, ``$``, ``#``); a uuid suffix keeps concurrent/feature
    runs isolated.  ``[:12]`` keeps the name comfortably below the 128-byte
    identifier limit while retaining ample entropy.
    """
    return f"bench_{tag}_{scenario_name}_{uuid.uuid4().hex[:12]}"


def _config_for_scenario(scenario_name: str, tag: str):
    """Create a fresh schema user and return ``(scenario, config)`` for it."""
    sc = scenario(scenario_name)
    name = _schema_name(scenario_name, tag)
    sc.reset_database(name)
    return sc, sc.make_config(name)


async def _seed_async(model, payloads):
    ids = []
    for payload in payloads:
        obj = model(**payload)
        await obj.save()
        ids.append(obj.id)
    return ids


class CrudBenchmarkImpl(BenchmarkImplementation):
    def __init__(self, tag="crud"):
        self.tag = tag
        self._backends = []

    def scenarios(self):
        return list(SCENARIOS)

    def sync_setup(self, scenario_name, size):
        sc, cfg = _config_for_scenario(scenario_name, self.tag)
        BenchUser.configure(cfg, sc.backend_class)
        backend = BenchUser.backend()
        backend.execute(*BenchUser.create_table_statement(backend=backend).to_sql(), options=DDL)
        self._backends.append(backend)
        payloads = make_user_payloads(payload_count_for_size(size))
        ids = []
        for payload in payloads:
            obj = BenchUser(**payload)
            obj.save()
            ids.append(obj.id)
        return CrudBenchmarkContext(
            scenario_name, size, BenchUser, payloads, ids, BACKEND_NAMESPACE, BACKEND_NAME
        )

    def sync_teardown(self, scenario_name, context):
        for backend in self._backends:
            backend.disconnect()
        self._backends.clear()

    async def async_setup(self, scenario_name, size, strategy="context"):
        sc, cfg = _config_for_scenario(scenario_name, self.tag)
        await AsyncBenchUser.configure(cfg, sc.async_backend_class)
        backend = AsyncBenchUser.backend()
        await backend.execute(
            *AsyncBenchUser.create_table_statement(backend=backend).to_sql(), options=DDL
        )
        self._backends.append(backend)
        payloads = make_user_payloads(payload_count_for_size(size))
        ids = await _seed_async(AsyncBenchUser, payloads)
        return CrudBenchmarkContext(
            scenario_name, size, AsyncBenchUser, payloads, ids, BACKEND_NAMESPACE, BACKEND_NAME
        )

    async def async_teardown(self, scenario_name, context):
        for backend in self._backends:
            await backend.disconnect()
        self._backends.clear()


class QueryBenchmarkImpl(CrudBenchmarkImpl):
    """Query benchmark shares the CRUD context shape."""

    def sync_setup(self, scenario_name, size):
        ctx = super().sync_setup(scenario_name, size)
        return QueryBenchmarkContext(
            ctx.scenario,
            ctx.size,
            ctx.model_class,
            ctx.payloads,
            ctx.record_ids,
            ctx.backend_namespace,
            ctx.backend_name,
        )

    async def async_setup(self, scenario_name, size, strategy="context"):
        ctx = await super().async_setup(scenario_name, size, strategy)
        return QueryBenchmarkContext(
            ctx.scenario,
            ctx.size,
            ctx.model_class,
            ctx.payloads,
            ctx.record_ids,
            ctx.backend_namespace,
            ctx.backend_name,
        )


class TransactionBenchmarkImpl(CrudBenchmarkImpl):
    def sync_setup(self, scenario_name, size):
        ctx = super().sync_setup(scenario_name, size)
        return TransactionBenchmarkContext(
            ctx.scenario,
            ctx.size,
            ctx.model_class,
            ctx.payloads,
            ctx.record_ids,
            ctx.backend_namespace,
            ctx.backend_name,
        )

    async def async_setup(self, scenario_name, size, strategy="context"):
        ctx = await super().async_setup(scenario_name, size, strategy)
        return TransactionBenchmarkContext(
            ctx.scenario,
            ctx.size,
            ctx.model_class,
            ctx.payloads,
            ctx.record_ids,
            ctx.backend_namespace,
            ctx.backend_name,
        )


class MixinBenchmarkImpl(CrudBenchmarkImpl):
    def sync_setup(self, scenario_name, size):
        ctx = super().sync_setup(scenario_name, size)
        return MixinBenchmarkContext(
            ctx.scenario,
            ctx.size,
            ctx.model_class,
            ctx.payloads,
            ctx.record_ids,
            ctx.backend_namespace,
            ctx.backend_name,
        )

    async def async_setup(self, scenario_name, size, strategy="context"):
        ctx = await super().async_setup(scenario_name, size, strategy)
        return MixinBenchmarkContext(
            ctx.scenario,
            ctx.size,
            ctx.model_class,
            ctx.payloads,
            ctx.record_ids,
            ctx.backend_namespace,
            ctx.backend_name,
        )


# --------------------------------------------------------------------------- #
# Register.  Imported as a pytest plugin (``-p tests.benchmark.providers``) so
# registration happens before the shared contracts are collected.
# --------------------------------------------------------------------------- #
benchmarks.register("crud", CrudBenchmarkImpl("crud"))
benchmarks.register("query", QueryBenchmarkImpl("query"))
benchmarks.register("transaction", TransactionBenchmarkImpl("transaction"))
benchmarks.register("mixin", MixinBenchmarkImpl("mixin"))
