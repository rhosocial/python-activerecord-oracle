# tests/providers/transaction_benchmark.py
"""Oracle provider for transaction benchmark tests."""

from rhosocial.activerecord.testsuite.benchmark.transaction.interfaces import (
    TransactionBenchmarkContext,
)

from .crud_benchmark import CrudBenchmarkProvider


class TransactionBenchmarkProvider(CrudBenchmarkProvider):
    def setup_benchmark_sync(self, scenario: str, size: str):
        context = super().setup_benchmark_sync(scenario, size)
        return TransactionBenchmarkContext(**context.__dict__)

    async def setup_benchmark_async(self, scenario: str, size: str):
        context = await super().setup_benchmark_async(scenario, size)
        return TransactionBenchmarkContext(**context.__dict__)
