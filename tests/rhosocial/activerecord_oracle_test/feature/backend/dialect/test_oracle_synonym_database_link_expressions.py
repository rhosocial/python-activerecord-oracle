# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_synonym_database_link_expressions.py
"""Tests for Oracle SYNONYM and DATABASE LINK expressions.

Covers ``CREATE [PUBLIC] SYNONYM`` / ``DROP [PUBLIC] SYNONYM``,
``CREATE [SHARED] [PUBLIC] DATABASE LINK`` / ``DROP DATABASE LINK``, the
``@dblink`` table-reference suffix, and the ``(9, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleCreateDatabaseLinkExpression,
    OracleCreateSynonymExpression,
    OracleDropDatabaseLinkExpression,
    OracleDropSynonymExpression,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleSynonymCapabilities:
    def test_supports_synonym(self, dialect):
        assert dialect.supports_create_synonym() is True
        assert dialect.supports_drop_synonym() is True

    def test_supports_database_link(self, dialect):
        assert dialect.supports_create_database_link() is True
        assert dialect.supports_drop_database_link() is True


class TestOracleCreateSynonymExpression:
    def test_private_synonym(self, dialect):
        expr = OracleCreateSynonymExpression(dialect, synonym_name="s", table_name="t")
        sql, params = expr.to_sql()
        assert sql == 'CREATE SYNONYM "S" FOR "T"'
        assert params == ()

    def test_schema_qualified_target(self, dialect):
        expr = OracleCreateSynonymExpression(
            dialect, synonym_name="s", table_name="t", schema_name="scott"
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE SYNONYM "S" FOR "SCOTT"."T"'
        assert params == ()

    def test_public_synonym(self, dialect):
        expr = OracleCreateSynonymExpression(
            dialect, synonym_name="s", table_name="t", public=True
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE PUBLIC SYNONYM "S" FOR "T"'
        assert params == ()

    def test_public_synonym_with_schema(self, dialect):
        expr = OracleCreateSynonymExpression(
            dialect, synonym_name="s", table_name="emp", schema_name="hr", public=True
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE PUBLIC SYNONYM "S" FOR "HR"."EMP"'
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleCreateSynonymExpression(dialect, synonym_name="My_Syn", table_name="t")
        sql, params = expr.to_sql()
        assert sql == 'CREATE SYNONYM "MY_SYN" FOR "T"'
        assert params == ()

    def test_empty_synonym_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="synonym_name must be a non-empty string"):
            OracleCreateSynonymExpression(dialect, synonym_name="  ", table_name="t")

    def test_empty_table_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="table_name must be a non-empty string"):
            OracleCreateSynonymExpression(dialect, synonym_name="s", table_name="")


class TestOracleDropSynonymExpression:
    def test_private_drop(self, dialect):
        expr = OracleDropSynonymExpression(dialect, synonym_name="s")
        sql, params = expr.to_sql()
        assert sql == 'DROP SYNONYM "S"'
        assert params == ()

    def test_public_drop(self, dialect):
        expr = OracleDropSynonymExpression(dialect, synonym_name="s", public=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP PUBLIC SYNONYM "S"'
        assert params == ()

    def test_drop_force(self, dialect):
        expr = OracleDropSynonymExpression(dialect, synonym_name="s", force=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP SYNONYM "S" FORCE'
        assert params == ()

    def test_empty_synonym_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="synonym_name must be a non-empty string"):
            OracleDropSynonymExpression(dialect, synonym_name="  ")


class TestOracleCreateDatabaseLinkExpression:
    def test_full_clause(self, dialect):
        expr = OracleCreateDatabaseLinkExpression(
            dialect,
            link_name="dl",
            user="u",
            identified_by="pwd",
            using="conn_str",
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE DATABASE LINK "DL" CONNECT TO "U" IDENTIFIED BY "PWD" USING \'conn_str\''
        assert params == ()

    def test_public_link(self, dialect):
        expr = OracleCreateDatabaseLinkExpression(
            dialect, link_name="dl", user="u", identified_by="pwd", public=True
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE PUBLIC DATABASE LINK "DL" CONNECT TO "U" IDENTIFIED BY "PWD"'
        assert params == ()

    def test_shared_link(self, dialect):
        expr = OracleCreateDatabaseLinkExpression(
            dialect, link_name="dl", user="u", identified_by="pwd", shared=True
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE SHARED DATABASE LINK "DL" CONNECT TO "U" IDENTIFIED BY "PWD"'
        assert params == ()

    def test_link_name_only(self, dialect):
        expr = OracleCreateDatabaseLinkExpression(dialect, link_name="dl")
        sql, params = expr.to_sql()
        assert sql == 'CREATE DATABASE LINK "DL"'
        assert params == ()

    def test_connect_string_escaped(self, dialect):
        expr = OracleCreateDatabaseLinkExpression(
            dialect, link_name="dl", user="u", identified_by="pwd", using="it's"
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE DATABASE LINK "DL" CONNECT TO "U" IDENTIFIED BY "PWD" USING \'it\'\'s\''
        assert params == ()

    def test_user_without_password_rejected(self, dialect):
        with pytest.raises(ValueError, match="user and identified_by must be supplied together"):
            OracleCreateDatabaseLinkExpression(dialect, link_name="dl", user="u")

    def test_password_without_user_rejected(self, dialect):
        with pytest.raises(ValueError, match="user and identified_by must be supplied together"):
            OracleCreateDatabaseLinkExpression(dialect, link_name="dl", identified_by="pwd")

    def test_empty_link_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="link_name must be a non-empty string"):
            OracleCreateDatabaseLinkExpression(dialect, link_name="  ")


class TestOracleDropDatabaseLinkExpression:
    def test_private_drop(self, dialect):
        expr = OracleDropDatabaseLinkExpression(dialect, link_name="dl")
        sql, params = expr.to_sql()
        assert sql == 'DROP DATABASE LINK "DL"'
        assert params == ()

    def test_public_drop(self, dialect):
        expr = OracleDropDatabaseLinkExpression(dialect, link_name="dl", public=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP PUBLIC DATABASE LINK "DL"'
        assert params == ()

    def test_empty_link_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="link_name must be a non-empty string"):
            OracleDropDatabaseLinkExpression(dialect, link_name="  ")


class TestOracleDblinkTableReference:
    def test_dblink_suffix(self, dialect):
        sql, params = dialect.format_table("remote_table", dblink="dl")
        assert sql == '"REMOTE_TABLE"@"DL"'
        assert params == ()

    def test_dblink_uppercased(self, dialect):
        sql, params = dialect.format_table("remote_table", dblink="my_dl")
        assert sql == '"REMOTE_TABLE"@"MY_DL"'
        assert params == ()

    def test_dblink_with_schema(self, dialect):
        sql, params = dialect.format_table("remote_table", schema_name="scott", dblink="dl")
        assert sql == '"SCOTT"."REMOTE_TABLE"@"DL"'
        assert params == ()

    def test_dblink_with_alias(self, dialect):
        sql, params = dialect.format_table("remote_table", alias="r", dblink="dl")
        assert sql == '"REMOTE_TABLE"@"DL" "R"'
        assert params == ()

    def test_dblink_through_table_expression_to_sql(self, dialect):
        sql, params = dialect.format_table("remote_table", dblink="dl")
        assert sql == '"REMOTE_TABLE"@"DL"'
        assert params == ()

    def test_table_expression_without_dblink_unchanged(self, dialect):
        sql, params = dialect.format_table("t")
        assert sql == '"T"'
        assert params == ()


class TestOracleSynonymDatabaseLinkVersionBoundary:
    def test_create_synonym_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreateSynonymExpression(d8, synonym_name="s", table_name="t")
        with pytest.raises(UnsupportedFeatureError, match="CREATE SYNONYM"):
            expr.to_sql()

    def test_drop_synonym_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleDropSynonymExpression(d8, synonym_name="s")
        with pytest.raises(UnsupportedFeatureError, match="DROP SYNONYM"):
            expr.to_sql()

    def test_create_database_link_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreateDatabaseLinkExpression(d8, link_name="dl")
        with pytest.raises(UnsupportedFeatureError, match="CREATE DATABASE LINK"):
            expr.to_sql()

    def test_drop_database_link_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleDropDatabaseLinkExpression(d8, link_name="dl")
        with pytest.raises(UnsupportedFeatureError, match="DROP DATABASE LINK"):
            expr.to_sql()

    def test_at_9i_works(self):
        d9 = OracleDialect(version=(9, 0, 0))
        assert OracleCreateSynonymExpression(
            d9, synonym_name="s", table_name="t"
        ).to_sql()[0] == 'CREATE SYNONYM "S" FOR "T"'
        assert OracleCreateDatabaseLinkExpression(
            d9, link_name="dl"
        ).to_sql()[0] == 'CREATE DATABASE LINK "DL"'
