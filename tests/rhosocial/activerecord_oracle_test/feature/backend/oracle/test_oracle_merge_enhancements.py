# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_merge_enhancements.py
"""Tests for Oracle MERGE statement enhancements.

Covers the ``OracleDMLOperationMixin.format_merge_statement`` extensions:
the ``WHEN MATCHED ... DELETE WHERE (cond)`` conditional-delete branch
(10g) and the trailing ``LOG ERRORS INTO ... [REJECT LIMIT n]`` DML
error-logging clause (10g), plus the ``(10, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleMergeCapabilities:
    def test_supports_merge(self, dialect):
        assert dialect.supports_merge_statement() is True


class TestOracleMergeEnhancements:
    def test_basic_merge_unchanged(self, dialect):
        sql, params = dialect.format_merge_statement(
            "T", "SRC", "T.ID = SRC.ID", "T.NAME = SRC.NAME", "ID, NAME", "SRC.ID, SRC.NAME"
        )
        assert sql == (
            "MERGE INTO T t USING (SRC) s ON (T.ID = SRC.ID) "
            "WHEN MATCHED THEN UPDATE SET T.NAME = SRC.NAME "
            "WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SRC.ID, SRC.NAME)"
        )
        assert params == ()

    def test_merge_delete_where(self, dialect):
        sql, params = dialect.format_merge_statement(
            "T", "SRC", "T.ID = SRC.ID", "T.NAME = SRC.NAME", "ID, NAME", "SRC.ID, SRC.NAME",
            delete_where="SRC.FLAG = 1",
        )
        assert sql == (
            "MERGE INTO T t USING (SRC) s ON (T.ID = SRC.ID) "
            "WHEN MATCHED THEN UPDATE SET T.NAME = SRC.NAME DELETE WHERE (SRC.FLAG = 1) "
            "WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SRC.ID, SRC.NAME)"
        )
        assert params == ()

    def test_merge_log_errors_into(self, dialect):
        sql, params = dialect.format_merge_statement(
            "T", "SRC", "T.ID = SRC.ID", "T.NAME = SRC.NAME", "ID, NAME", "SRC.ID, SRC.NAME",
            log_errors_into="ERR$_T",
        )
        assert sql == (
            "MERGE INTO T t USING (SRC) s ON (T.ID = SRC.ID) "
            "WHEN MATCHED THEN UPDATE SET T.NAME = SRC.NAME "
            "WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SRC.ID, SRC.NAME) "
            "LOG ERRORS INTO ERR$_T"
        )
        assert params == ()

    def test_merge_log_errors_reject_limit(self, dialect):
        sql, params = dialect.format_merge_statement(
            "T", "SRC", "T.ID = SRC.ID", "T.NAME = SRC.NAME", "ID, NAME", "SRC.ID, SRC.NAME",
            log_errors_into="ERR$_T",
            reject_limit=25,
        )
        assert sql == (
            "MERGE INTO T t USING (SRC) s ON (T.ID = SRC.ID) "
            "WHEN MATCHED THEN UPDATE SET T.NAME = SRC.NAME "
            "WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SRC.ID, SRC.NAME) "
            "LOG ERRORS INTO ERR$_T REJECT LIMIT 25"
        )
        assert params == ()

    def test_merge_delete_where_and_log_errors(self, dialect):
        sql, params = dialect.format_merge_statement(
            "T", "SRC", "T.ID = SRC.ID", "T.NAME = SRC.NAME", "ID, NAME", "SRC.ID, SRC.NAME",
            delete_where="SRC.FLAG = 1",
            log_errors_into="ERR$_T",
            reject_limit=10,
        )
        assert sql == (
            "MERGE INTO T t USING (SRC) s ON (T.ID = SRC.ID) "
            "WHEN MATCHED THEN UPDATE SET T.NAME = SRC.NAME DELETE WHERE (SRC.FLAG = 1) "
            "WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SRC.ID, SRC.NAME) "
            "LOG ERRORS INTO ERR$_T REJECT LIMIT 10"
        )
        assert params == ()

    def test_merge_delete_where_without_update_branch(self, dialect):
        sql, params = dialect.format_merge_statement(
            "T", "SRC", "T.ID = SRC.ID", "", "", "",
            delete_where="SRC.FLAG = 1",
            log_errors_into="ERR$_T",
            reject_limit=10,
        )
        assert sql == (
            "MERGE INTO T t USING (SRC) s ON (T.ID = SRC.ID) "
            "LOG ERRORS INTO ERR$_T REJECT LIMIT 10"
        )
        assert params == ()


class TestOracleMergeVersionBoundary:
    def test_delete_where_below_10g_raises(self):
        d9 = OracleDialect(version=(9, 2, 0))
        with pytest.raises(UnsupportedFeatureError, match="DELETE WHERE"):
            d9.format_merge_statement(
                "T", "SRC", "T.ID = SRC.ID", "T.NAME = SRC.NAME", "ID", "ID",
                delete_where="SRC.FLAG = 1",
            )

    def test_log_errors_below_10g_raises(self):
        d9 = OracleDialect(version=(9, 2, 0))
        with pytest.raises(UnsupportedFeatureError, match="LOG ERRORS INTO"):
            d9.format_merge_statement(
                "T", "SRC", "T.ID = SRC.ID", "T.NAME = SRC.NAME", "ID", "ID",
                log_errors_into="ERR$_T",
            )

    def test_at_10g_works(self):
        d10 = OracleDialect(version=(10, 0, 0))
        sql, params = d10.format_merge_statement(
            "T", "SRC", "T.ID = SRC.ID", "T.NAME = SRC.NAME", "ID", "ID",
            delete_where="SRC.FLAG = 1",
            log_errors_into="ERR$_T",
            reject_limit=25,
        )
        assert "DELETE WHERE (SRC.FLAG = 1)" in sql
        assert "LOG ERRORS INTO ERR$_T REJECT LIMIT 25" in sql
        assert params == ()
