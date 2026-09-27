# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_capability_accuracy.py
"""Oracle capability accuracy: Oracle has no system-versioned temporal tables.

The previous declaration claimed ``supports_temporal_tables() is True`` with
no implementation; Oracle's Flashback / temporal validity features are a
different mechanism and are not modelled here.
"""

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


def test_temporal_tables_unsupported():
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_temporal_tables() is False
