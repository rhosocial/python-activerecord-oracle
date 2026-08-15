# tests/providers/query_benchmark.py
"""Oracle provider for query benchmark tests."""

from rhosocial.activerecord.testsuite.benchmark.query.interfaces import QueryBenchmarkContext

from .crud_benchmark import CrudBenchmarkProvider


class QueryBenchmarkProvider(CrudBenchmarkProvider):
    def setup_benchmark_sync(self, scenario: str, size: str):
        context = super().setup_benchmark_sync(scenario, size)
        return QueryBenchmarkContext(**context.__dict__)

    async def setup_benchmark_async(self, scenario: str, size: str):
        context = await super().setup_benchmark_async(scenario, size)
        return QueryBenchmarkContext(**context.__dict__)
