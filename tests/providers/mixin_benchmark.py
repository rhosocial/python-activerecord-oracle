# tests/providers/mixin_benchmark.py
"""Oracle provider for mixin benchmark tests."""

from rhosocial.activerecord.testsuite.benchmark.mixin.interfaces import MixinBenchmarkContext

from .crud_benchmark import CrudBenchmarkProvider


class MixinBenchmarkProvider(CrudBenchmarkProvider):
    def setup_benchmark_sync(self, scenario: str, size: str):
        context = super().setup_benchmark_sync(scenario, size)
        return MixinBenchmarkContext(**context.__dict__)

    async def setup_benchmark_async(self, scenario: str, size: str):
        context = await super().setup_benchmark_async(scenario, size)
        return MixinBenchmarkContext(**context.__dict__)
