# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_synonym_database_link_expressions.py
"""Tests for Oracle SYNONYM and DATABASE LINK expressions.

Covers ``CREATE [PUBLIC] SYNONYM`` / ``DROP [PUBLIC] SYNONYM``,
``CREATE [SHARED] [PUBLIC] DATABASE LINK`` / ``DROP DATABASE LINK``, the
``@dblink`` name suffix carried on an ``OracleTable``, and the ``(9, 0, 0)``
version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Synonym, Table
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleCreateDatabaseLinkExpression,
    OracleCreateSynonymExpression,
    OracleDropDatabaseLinkExpression,
    OracleDropSynonymExpression,
    OracleNamedRelationRef,
    OracleTable,
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
        expr = OracleCreateSynonymExpression(
            dialect, synonym=Synonym(dialect, "s"), table=Table(dialect, "t")
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE SYNONYM "S" FOR "T"'
        assert params == ()

    def test_schema_qualified_target(self, dialect):
        """The owner rides on the target table; the synonym keeps its own."""
        expr = OracleCreateSynonymExpression(
            dialect,
            synonym=Synonym(dialect, "s"),
            table=Table(dialect, "t", schema_name="scott"),
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE SYNONYM "S" FOR "SCOTT"."T"'
        assert params == ()

    def test_the_two_owners_are_independent(self, dialect):
        """A synonym and its target may live in different owners.

        One ``schema_name`` beside both names could not say so: it had to be the
        same string for the two halves of one statement.
        """
        expr = OracleCreateSynonymExpression(
            dialect,
            synonym=Synonym(dialect, "s", schema_name="app"),
            table=Table(dialect, "t", schema_name="hr"),
        )
        assert expr.to_sql()[0] == 'CREATE SYNONYM "APP"."S" FOR "HR"."T"'

    def test_public_synonym(self, dialect):
        expr = OracleCreateSynonymExpression(
            dialect, synonym=Synonym(dialect, "s"), table=Table(dialect, "t"),
            public=True,
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE PUBLIC SYNONYM "S" FOR "T"'
        assert params == ()

    def test_public_synonym_with_schema(self, dialect):
        expr = OracleCreateSynonymExpression(
            dialect,
            synonym=Synonym(dialect, "s"),
            table=Table(dialect, "emp", schema_name="hr"),
            public=True,
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE PUBLIC SYNONYM "S" FOR "HR"."EMP"'
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleCreateSynonymExpression(
            dialect, synonym=Synonym(dialect, "My_Syn"), table=Table(dialect, "t")
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE SYNONYM "MY_SYN" FOR "T"'
        assert params == ()

    def test_empty_synonym_name_rejected(self, dialect):
        """The name is rejected by the object itself, which owns the slot."""
        with pytest.raises(ValueError, match="name must be a non-empty string"):
            OracleCreateSynonymExpression(
                dialect, synonym=Synonym(dialect, "  "), table=Table(dialect, "t")
            )

    def test_synonym_object_required(self, dialect):
        with pytest.raises(TypeError, match="synonym must be a Synonym"):
            OracleCreateSynonymExpression(
                dialect, synonym="s", table=Table(dialect, "t")
            )

    def test_table_object_required(self, dialect):
        with pytest.raises(TypeError, match="table must be a Table"):
            OracleCreateSynonymExpression(
                dialect, synonym=Synonym(dialect, "s"), table="t"
            )


class TestOracleDropSynonymExpression:
    def test_private_drop(self, dialect):
        expr = OracleDropSynonymExpression(dialect, synonym=Synonym(dialect, "s"))
        sql, params = expr.to_sql()
        assert sql == 'DROP SYNONYM "S"'
        assert params == ()

    def test_public_drop(self, dialect):
        expr = OracleDropSynonymExpression(dialect, synonym=Synonym(dialect, "s"), public=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP PUBLIC SYNONYM "S"'
        assert params == ()

    def test_drop_force(self, dialect):
        expr = OracleDropSynonymExpression(dialect, synonym=Synonym(dialect, "s"), force=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP SYNONYM "S" FORCE'
        assert params == ()

    def test_empty_synonym_name_rejected(self, dialect):
        """The name is rejected by the object itself, which owns the slot."""
        with pytest.raises(ValueError, match="name must be a non-empty string"):
            OracleDropSynonymExpression(dialect, synonym=Synonym(dialect, "  "))


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
    """The ``@"DBLINK"`` suffix is part of the name, so it lives on the object.

    A remote reference is an :class:`OracleTable` carrying ``dblink`, read
    through an :class:`OracleNamedRelationRef`. The suffix is rendered by the
    table's own formatter, which is why every case below goes through an object
    rather than a bare identifier.
    """

    def test_dblink_suffix(self, dialect):
        ref = OracleNamedRelationRef(
            dialect, OracleTable(dialect, "remote_table", dblink="dl")
        )
        sql, params = ref.to_sql()
        assert sql == '"REMOTE_TABLE"@"DL"'
        assert params == ()

    def test_dblink_uppercased(self, dialect):
        ref = OracleNamedRelationRef(
            dialect, OracleTable(dialect, "remote_table", dblink="my_dl")
        )
        sql, params = ref.to_sql()
        assert sql == '"REMOTE_TABLE"@"MY_DL"'
        assert params == ()

    def test_dblink_with_schema(self, dialect):
        ref = OracleNamedRelationRef(
            dialect,
            OracleTable(dialect, "remote_table", schema_name="scott", dblink="dl"),
        )
        sql, params = ref.to_sql()
        assert sql == '"SCOTT"."REMOTE_TABLE"@"DL"'
        assert params == ()

    def test_dblink_with_alias(self, dialect):
        ref = OracleNamedRelationRef(
            dialect, OracleTable(dialect, "remote_table", dblink="dl"), alias="r"
        )
        sql, params = ref.to_sql()
        # The alias keeps the core's ``AS`` separator, as it does for every
        # other backend: Oracle accepts ``FROM t AS r``.
        assert sql == '"REMOTE_TABLE"@"DL" AS "R"'
        assert params == ()

    def test_dblink_rendered_by_the_table_object(self, dialect):
        """The suffix belongs to the name, not to the reference.

        Rendering the table on its own produces the same text, which is the point:
        any statement that names a remote table picks the suffix up without
        knowing anything about database links.
        """
        assert OracleTable(dialect, "remote_table", dblink="dl").to_sql() == (
            '"REMOTE_TABLE"@"DL"', (),
        )

    def test_dblink_part_of_identity(self, dialect):
        """Two references differing only by link must not compare equal."""
        a = OracleTable(dialect, "t", dblink="dl")
        b = OracleTable(dialect, "t", dblink="dl2")
        c = OracleTable(dialect, "t", dblink="dl")
        assert a != b
        assert a == c
        assert hash(a) != hash(b)

    def test_empty_dblink_rejected(self, dialect):
        with pytest.raises(ValueError, match="dblink must be a non-empty string"):
            OracleTable(dialect, "t", dblink="  ")

    def test_table_without_dblink_unchanged(self, dialect):
        sql, params = OracleNamedRelationRef(
            dialect, OracleTable(dialect, "t")
        ).to_sql()
        assert sql == '"T"'
        assert params == ()


class TestOracleSynonymDatabaseLinkVersionBoundary:
    def test_create_synonym_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreateSynonymExpression(d8, synonym=Synonym(d8, "s"), table=Table(d8, "t"))
        with pytest.raises(UnsupportedFeatureError, match="CREATE SYNONYM"):
            expr.to_sql()

    def test_drop_synonym_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleDropSynonymExpression(d8, synonym=Synonym(d8, "s"))
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
            d9, synonym=Synonym(d9, "s"), table=Table(d9, "t")
        ).to_sql()[0] == 'CREATE SYNONYM "S" FOR "T"'
        assert OracleCreateDatabaseLinkExpression(
            d9, link_name="dl"
        ).to_sql()[0] == 'CREATE DATABASE LINK "DL"'
