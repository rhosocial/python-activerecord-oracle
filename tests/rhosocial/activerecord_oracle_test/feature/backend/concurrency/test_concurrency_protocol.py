# tests/rhosocial/activerecord_oracle_test/feature/backend/concurrency/test_concurrency_protocol.py
"""Tests for the ConcurrencyAware protocol implementation in the Oracle backend.

Verifies that :class:`OracleBackend` implements the ``ConcurrencyAware``
protocol and returns a concurrency hint via :meth:`get_concurrency_hint`.

The Oracle mixin fetches ``processes`` from ``V$PARAMETER`` during
``connect()`` and derives ``max_concurrency = min(processes, pool_max)``. When
the query is not permitted (no ``V$PARAMETER`` privilege) the hint falls back
to ``None`` (unlimited), so the protocol tests accept either outcome.
"""
from rhosocial.activerecord.backend.protocols import ConcurrencyAware, ConcurrencyHint


class TestOracleConcurrencyAware:
    def test_oracle_backend_implements_protocol(self, oracle_backend_single):
        """Test that OracleBackend implements ConcurrencyAware protocol."""
        assert isinstance(oracle_backend_single, ConcurrencyAware), "OracleBackend must implement ConcurrencyAware"

    def test_oracle_get_concurrency_hint(self, oracle_backend_single):
        """Test that the backend returns a concurrency hint after connect.

        Without ``V$PARAMETER`` privileges the hint may be None, meaning no
        concurrency constraint is known for the server.
        """
        hint = oracle_backend_single.get_concurrency_hint()

        assert hint is None or isinstance(hint, ConcurrencyHint), "refetched hint must remain None or a ConcurrencyHint"
        if hint is not None:
            assert hint.max_concurrency is None or hint.max_concurrency > 0, "max_concurrency must be positive when set"

    def test_oracle_concurrency_hint_reason(self, oracle_backend_single):
        """Test that the hint reason describes the constraint source."""
        hint = oracle_backend_single.get_concurrency_hint()

        if hint is not None:
            assert "processes" in hint.reason or "pool_max" in hint.reason, "hint reason must name its constraint source"

    def test_oracle_hint_after_refetch(self, oracle_backend_single):
        """Test that refetching does not disturb the cached hint type."""
        oracle_backend_single._fetch_concurrency_hint()
        hint = oracle_backend_single.get_concurrency_hint()

        assert hint is None or isinstance(hint, ConcurrencyHint), "refetched hint must remain None or a ConcurrencyHint"