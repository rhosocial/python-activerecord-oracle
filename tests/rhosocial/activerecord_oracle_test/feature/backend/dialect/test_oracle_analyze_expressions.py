# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_analyze_expressions.py
"""Tests for Oracle ANALYZE TABLE expressions.

Covers ``ANALYZE TABLE t COMPUTE STATISTICS`` / ``ESTIMATE STATISTICS
SAMPLE n PERCENT`` / ``VALIDATE STRUCTURE [CASCADE]`` / ``LIST CHAINED
ROWS INTO ...`` / ``DELETE SYSTEM STATISTICS``, option validation and the
``(9, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleAnalyzeExpression,
    OracleAnalyzeMode,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleAnalyzeCapabilities:
    def test_supports_analyze(self, dialect):
        assert dialect.supports_analyze() is True


class TestOracleAnalyzeExpression:
    def test_compute_statistics(self, dialect):
        expr = OracleAnalyzeExpression(dialect, Table(dialect, "t"), OracleAnalyzeMode.COMPUTE_STATISTICS)
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "T" COMPUTE STATISTICS'
        assert params == ()

    def test_estimate_statistics_with_sample(self, dialect):
        expr = OracleAnalyzeExpression(
            dialect, Table(dialect, "t"), OracleAnalyzeMode.ESTIMATE_STATISTICS, sample_percent=5
        )
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "T" ESTIMATE STATISTICS SAMPLE 5 PERCENT'
        assert params == ()

    def test_estimate_statistics_without_sample(self, dialect):
        expr = OracleAnalyzeExpression(dialect, Table(dialect, "t"), OracleAnalyzeMode.ESTIMATE_STATISTICS)
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "T" ESTIMATE STATISTICS'
        assert params == ()

    def test_validate_structure(self, dialect):
        expr = OracleAnalyzeExpression(dialect, Table(dialect, "t"), OracleAnalyzeMode.VALIDATE_STRUCTURE)
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "T" VALIDATE STRUCTURE'
        assert params == ()

    def test_validate_structure_cascade(self, dialect):
        expr = OracleAnalyzeExpression(
            dialect, Table(dialect, "t"), OracleAnalyzeMode.VALIDATE_STRUCTURE, cascade=True
        )
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "T" VALIDATE STRUCTURE CASCADE'
        assert params == ()

    def test_list_chained_rows(self, dialect):
        expr = OracleAnalyzeExpression(dialect, Table(dialect, "t"), OracleAnalyzeMode.LIST_CHAINED_ROWS)
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "T" LIST CHAINED ROWS'
        assert params == ()

    def test_list_chained_rows_into(self, dialect):
        expr = OracleAnalyzeExpression(
            dialect, Table(dialect, "t"), OracleAnalyzeMode.LIST_CHAINED_ROWS, into="chained_rows"
        )
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "T" LIST CHAINED ROWS INTO "CHAINED_ROWS"'
        assert params == ()

    def test_delete_system_statistics(self, dialect):
        expr = OracleAnalyzeExpression(dialect, Table(dialect, "t"), OracleAnalyzeMode.DELETE_SYSTEM_STATISTICS)
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "T" DELETE SYSTEM STATISTICS'
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleAnalyzeExpression(dialect, Table(dialect, "My_Table"), OracleAnalyzeMode.COMPUTE_STATISTICS)
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "MY_TABLE" COMPUTE STATISTICS'
        assert params == ()

    def test_empty_table_name_rejected(self, dialect):
        """The table is an object, so its own construction rejects the name."""
        with pytest.raises(ValueError, match="name must be a non-empty string"):
            OracleAnalyzeExpression(dialect, Table(dialect, "  "), OracleAnalyzeMode.COMPUTE_STATISTICS)

    def test_table_object_required(self, dialect):
        """A bare string cannot say which kind of object is named."""
        with pytest.raises(TypeError, match="table must be a Table"):
            OracleAnalyzeExpression(dialect, "t", OracleAnalyzeMode.COMPUTE_STATISTICS)

    def test_invalid_mode_rejected(self, dialect):
        with pytest.raises(TypeError, match="mode must be an OracleAnalyzeMode"):
            OracleAnalyzeExpression(dialect, Table(dialect, "t"), "COMPUTE STATISTICS")

    def test_sample_with_compute_statistics_rejected(self, dialect):
        with pytest.raises(ValueError, match="sample_percent requires ESTIMATE STATISTICS"):
            OracleAnalyzeExpression(
                dialect, Table(dialect, "t"), OracleAnalyzeMode.COMPUTE_STATISTICS, sample_percent=5
            )

    def test_cascade_with_compute_statistics_rejected(self, dialect):
        with pytest.raises(ValueError, match="cascade requires VALIDATE STRUCTURE"):
            OracleAnalyzeExpression(
                dialect, Table(dialect, "t"), OracleAnalyzeMode.COMPUTE_STATISTICS, cascade=True
            )

    def test_into_with_compute_statistics_rejected(self, dialect):
        with pytest.raises(ValueError, match="into requires LIST CHAINED ROWS"):
            OracleAnalyzeExpression(
                dialect, Table(dialect, "t"), OracleAnalyzeMode.COMPUTE_STATISTICS, into="ct"
            )

    def test_non_int_sample_rejected(self, dialect):
        with pytest.raises(TypeError, match="sample_percent must be an int"):
            OracleAnalyzeExpression(
                dialect, Table(dialect, "t"), OracleAnalyzeMode.ESTIMATE_STATISTICS, sample_percent="5"
            )

    def test_non_positive_sample_rejected(self, dialect):
        with pytest.raises(ValueError, match="sample_percent must be a positive integer"):
            OracleAnalyzeExpression(
                dialect, Table(dialect, "t"), OracleAnalyzeMode.ESTIMATE_STATISTICS, sample_percent=0
            )

    def test_empty_into_rejected(self, dialect):
        with pytest.raises(ValueError, match="into must be a non-empty string"):
            OracleAnalyzeExpression(
                dialect, Table(dialect, "t"), OracleAnalyzeMode.LIST_CHAINED_ROWS, into="  "
            )


class TestOracleAnalyzeVersionBoundary:
    def test_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleAnalyzeExpression(d8, Table(d8, "t"), OracleAnalyzeMode.COMPUTE_STATISTICS)
        with pytest.raises(UnsupportedFeatureError, match="ANALYZE TABLE"):
            expr.to_sql()

    def test_at_9i_works(self):
        d9 = OracleDialect(version=(9, 0, 0))
        expr = OracleAnalyzeExpression(d9, Table(d9, "t"), OracleAnalyzeMode.COMPUTE_STATISTICS)
        sql, params = expr.to_sql()
        assert sql == 'ANALYZE TABLE "T" COMPUTE STATISTICS'
        assert params == ()
