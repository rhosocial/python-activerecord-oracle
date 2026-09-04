# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/test_oracle_routine_expressions.py
"""Tests for Oracle PL/SQL routine and package DDL expressions.

Covers ``CREATE [OR REPLACE] PROCEDURE`` / ``FUNCTION`` / ``PACKAGE`` /
``PACKAGE BODY`` with parameter lists, return types and verbatim PL/SQL
bodies, the ``DROP PROCEDURE/FUNCTION/PACKAGE [BODY]`` statements, and the
``(9, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleCreateFunctionExpression,
    OracleCreatePackageBodyExpression,
    OracleCreatePackageExpression,
    OracleCreateProcedureExpression,
    OracleDropRoutineExpression,
    OracleDropRoutineObjectType,
    OracleRoutineParameter,
    OracleRoutineParameterMode,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleRoutineCapabilities:
    def test_supports_routines(self, dialect):
        assert dialect.supports_create_procedure() is True
        assert dialect.supports_create_function() is True
        assert dialect.supports_create_package() is True
        assert dialect.supports_create_package_body() is True


class TestOracleCreateProcedureExpression:
    def test_or_replace_with_in_parameter(self, dialect):
        expr = OracleCreateProcedureExpression(
            dialect,
            procedure_name="p",
            body="BEGIN NULL; END;",
            parameters=[OracleRoutineParameter("x", "NUMBER", OracleRoutineParameterMode.IN)],
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE PROCEDURE P (X IN NUMBER) AS BEGIN NULL; END;"
        assert params == ()

    def test_without_or_replace(self, dialect):
        expr = OracleCreateProcedureExpression(
            dialect, procedure_name="p", body="BEGIN NULL; END;", or_replace=False
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE PROCEDURE P AS BEGIN NULL; END;"
        assert params == ()

    def test_is_separator(self, dialect):
        expr = OracleCreateProcedureExpression(
            dialect, procedure_name="p", body="BEGIN NULL; END;", keyword="IS"
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE PROCEDURE P IS BEGIN NULL; END;"
        assert params == ()

    def test_no_parameters_omits_parentheses(self, dialect):
        expr = OracleCreateProcedureExpression(
            dialect, procedure_name="p", body="BEGIN NULL; END;"
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE PROCEDURE P AS BEGIN NULL; END;"
        assert params == ()

    def test_multiple_parameters(self, dialect):
        expr = OracleCreateProcedureExpression(
            dialect,
            procedure_name="p",
            body="BEGIN NULL; END;",
            parameters=[
                OracleRoutineParameter("x", "NUMBER"),
                OracleRoutineParameter("y", "VARCHAR2(10)", OracleRoutineParameterMode.OUT),
                OracleRoutineParameter("z", "NUMBER", OracleRoutineParameterMode.IN_OUT),
            ],
        )
        sql, params = expr.to_sql()
        assert sql == (
            "CREATE OR REPLACE PROCEDURE P (X NUMBER, Y OUT VARCHAR2(10), "
            "Z IN OUT NUMBER) AS BEGIN NULL; END;"
        )
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleCreateProcedureExpression(
            dialect,
            procedure_name="my_proc",
            body="BEGIN NULL; END;",
            parameters=[OracleRoutineParameter("in_x", "number")],
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE PROCEDURE MY_PROC (IN_X NUMBER) AS BEGIN NULL; END;"
        assert params == ()

    def test_body_passed_through_verbatim(self, dialect):
        expr = OracleCreateProcedureExpression(
            dialect,
            procedure_name="p",
            body="BEGIN dbms_output.put_line(x); END;",
        )
        sql, params = expr.to_sql()
        assert sql == (
            "CREATE OR REPLACE PROCEDURE P AS BEGIN dbms_output.put_line(x); END;"
        )
        assert params == ()

    def test_empty_procedure_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="procedure_name must be a non-empty string"):
            OracleCreateProcedureExpression(dialect, procedure_name="  ", body="BEGIN NULL; END;")

    def test_empty_body_rejected(self, dialect):
        with pytest.raises(ValueError, match="body must be a non-empty string"):
            OracleCreateProcedureExpression(dialect, procedure_name="p", body="  ")

    def test_invalid_keyword_rejected(self, dialect):
        with pytest.raises(ValueError, match="keyword must be 'AS' or 'IS'"):
            OracleCreateProcedureExpression(
                dialect, procedure_name="p", body="BEGIN NULL; END;", keyword="WHEN"
            )


class TestOracleRoutineParameter:
    def test_invalid_mode_rejected(self, dialect):
        with pytest.raises(TypeError, match="mode must be an OracleRoutineParameterMode"):
            OracleRoutineParameter("x", "NUMBER", mode="IN")

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError, match="parameter name must be a non-empty string"):
            OracleRoutineParameter("  ", "NUMBER")

    def test_empty_data_type_rejected(self):
        with pytest.raises(ValueError, match="parameter data_type must be a non-empty string"):
            OracleRoutineParameter("x", "  ")


class TestOracleCreateFunctionExpression:
    def test_with_return_type_and_parameter(self, dialect):
        expr = OracleCreateFunctionExpression(
            dialect,
            function_name="f",
            return_type="NUMBER",
            body="BEGIN RETURN x + 1; END;",
            parameters=[OracleRoutineParameter("x", "NUMBER")],
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE FUNCTION F (X NUMBER) RETURN NUMBER AS BEGIN RETURN x + 1; END;"
        assert params == ()

    def test_returns_keyword(self, dialect):
        expr = OracleCreateFunctionExpression(
            dialect,
            function_name="f",
            return_type="NUMBER",
            body="BEGIN RETURN 1; END;",
            return_keyword="RETURNS",
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE FUNCTION F RETURNS NUMBER AS BEGIN RETURN 1; END;"
        assert params == ()

    def test_without_or_replace(self, dialect):
        expr = OracleCreateFunctionExpression(
            dialect,
            function_name="f",
            return_type="NUMBER",
            body="BEGIN RETURN 1; END;",
            or_replace=False,
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE FUNCTION F RETURN NUMBER AS BEGIN RETURN 1; END;"
        assert params == ()

    def test_is_separator(self, dialect):
        expr = OracleCreateFunctionExpression(
            dialect,
            function_name="f",
            return_type="NUMBER",
            body="BEGIN RETURN 1; END;",
            keyword="IS",
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE FUNCTION F RETURN NUMBER IS BEGIN RETURN 1; END;"
        assert params == ()

    def test_empty_return_type_rejected(self, dialect):
        with pytest.raises(ValueError, match="return_type must be a non-empty string"):
            OracleCreateFunctionExpression(
                dialect, function_name="f", return_type="  ", body="BEGIN RETURN 1; END;"
            )

    def test_invalid_return_keyword_rejected(self, dialect):
        with pytest.raises(ValueError, match="return_keyword must be 'RETURN' or 'RETURNS'"):
            OracleCreateFunctionExpression(
                dialect, function_name="f", return_type="NUMBER", body="BEGIN RETURN 1; END;",
                return_keyword="IS",
            )


class TestOracleCreatePackageExpression:
    def test_or_replace_package(self, dialect):
        expr = OracleCreatePackageExpression(
            dialect,
            package_name="pk",
            body="PROCEDURE p (x NUMBER);",
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE PACKAGE PK AS PROCEDURE p (x NUMBER);"
        assert params == ()

    def test_package_with_is(self, dialect):
        expr = OracleCreatePackageExpression(
            dialect, package_name="pk", body="PROCEDURE p (x NUMBER);", keyword="IS"
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE PACKAGE PK IS PROCEDURE p (x NUMBER);"
        assert params == ()

    def test_package_without_or_replace(self, dialect):
        expr = OracleCreatePackageExpression(
            dialect, package_name="pk", body="PROCEDURE p (x NUMBER);", or_replace=False
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE PACKAGE PK AS PROCEDURE p (x NUMBER);"
        assert params == ()

    def test_empty_package_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="package_name must be a non-empty string"):
            OracleCreatePackageExpression(dialect, package_name="  ", body="PROCEDURE p (x NUMBER);")


class TestOracleCreatePackageBodyExpression:
    def test_or_replace_package_body(self, dialect):
        expr = OracleCreatePackageBodyExpression(
            dialect,
            package_name="pk",
            body="PROCEDURE p (x NUMBER) AS BEGIN NULL; END p;",
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE PACKAGE BODY PK AS PROCEDURE p (x NUMBER) AS BEGIN NULL; END p;"
        assert params == ()

    def test_package_body_with_is(self, dialect):
        expr = OracleCreatePackageBodyExpression(
            dialect,
            package_name="pk",
            body="PROCEDURE p (x NUMBER) AS BEGIN NULL; END p;",
            keyword="IS",
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE OR REPLACE PACKAGE BODY PK IS PROCEDURE p (x NUMBER) AS BEGIN NULL; END p;"
        assert params == ()

    def test_package_body_without_or_replace(self, dialect):
        expr = OracleCreatePackageBodyExpression(
            dialect, package_name="pk", body="PROCEDURE p (x NUMBER) AS BEGIN NULL; END p;",
            or_replace=False,
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE PACKAGE BODY PK AS PROCEDURE p (x NUMBER) AS BEGIN NULL; END p;"
        assert params == ()


class TestOracleDropRoutineExpression:
    def test_drop_procedure(self, dialect):
        expr = OracleDropRoutineExpression(
            dialect, OracleDropRoutineObjectType.PROCEDURE, "p"
        )
        sql, params = expr.to_sql()
        assert sql == "DROP PROCEDURE P"
        assert params == ()

    def test_drop_function(self, dialect):
        expr = OracleDropRoutineExpression(
            dialect, OracleDropRoutineObjectType.FUNCTION, "f"
        )
        sql, params = expr.to_sql()
        assert sql == "DROP FUNCTION F"
        assert params == ()

    def test_drop_package(self, dialect):
        expr = OracleDropRoutineExpression(
            dialect, OracleDropRoutineObjectType.PACKAGE, "pk"
        )
        sql, params = expr.to_sql()
        assert sql == "DROP PACKAGE PK"
        assert params == ()

    def test_drop_package_body(self, dialect):
        expr = OracleDropRoutineExpression(
            dialect, OracleDropRoutineObjectType.PACKAGE_BODY, "pk"
        )
        sql, params = expr.to_sql()
        assert sql == "DROP PACKAGE BODY PK"
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleDropRoutineExpression(
            dialect, OracleDropRoutineObjectType.PROCEDURE, "my_proc"
        )
        sql, params = expr.to_sql()
        assert sql == "DROP PROCEDURE MY_PROC"
        assert params == ()

    def test_invalid_object_type_rejected(self, dialect):
        with pytest.raises(TypeError, match="object_type must be an OracleDropRoutineObjectType"):
            OracleDropRoutineExpression(dialect, "PROCEDURE", "p")

    def test_empty_object_name_rejected(self, dialect):
        with pytest.raises(ValueError, match="object_name must be a non-empty string"):
            OracleDropRoutineExpression(dialect, OracleDropRoutineObjectType.PROCEDURE, "  ")


class TestOracleRoutineVersionBoundary:
    def test_create_procedure_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreateProcedureExpression(d8, procedure_name="p", body="BEGIN NULL; END;")
        with pytest.raises(UnsupportedFeatureError, match="CREATE PROCEDURE"):
            expr.to_sql()

    def test_create_function_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreateFunctionExpression(
            d8, function_name="f", return_type="NUMBER", body="BEGIN RETURN 1; END;"
        )
        with pytest.raises(UnsupportedFeatureError, match="CREATE FUNCTION"):
            expr.to_sql()

    def test_create_package_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreatePackageExpression(d8, package_name="pk", body="PROCEDURE p (x NUMBER);")
        with pytest.raises(UnsupportedFeatureError, match="CREATE PACKAGE"):
            expr.to_sql()

    def test_create_package_body_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreatePackageBodyExpression(
            d8, package_name="pk", body="PROCEDURE p (x NUMBER) AS BEGIN NULL; END p;"
        )
        with pytest.raises(UnsupportedFeatureError, match="CREATE PACKAGE BODY"):
            expr.to_sql()

    def test_drop_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleDropRoutineExpression(d8, OracleDropRoutineObjectType.PROCEDURE, "p")
        with pytest.raises(UnsupportedFeatureError, match="DROP PROCEDURE"):
            expr.to_sql()

    def test_at_9i_works(self):
        d9 = OracleDialect(version=(9, 0, 0))
        assert OracleCreateProcedureExpression(
            d9, procedure_name="p", body="BEGIN NULL; END;"
        ).to_sql()[0] == "CREATE OR REPLACE PROCEDURE P AS BEGIN NULL; END;"
        assert OracleCreateFunctionExpression(
            d9, function_name="f", return_type="NUMBER", body="BEGIN RETURN 1; END;"
        ).to_sql()[0] == "CREATE OR REPLACE FUNCTION F RETURN NUMBER AS BEGIN RETURN 1; END;"
        assert OracleDropRoutineExpression(
            d9, OracleDropRoutineObjectType.PACKAGE_BODY, "pk"
        ).to_sql()[0] == "DROP PACKAGE BODY PK"
