# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_expression_fields_match_formatters.py
"""A formatter may not read a field its statement does not carry.

A statement formatter that reads ``expr.schema_name`` needs the expression to
have that attribute. When the formatter was changed to qualify names and the
expression was not given the field, the result is not wrong SQL -- it is an
``AttributeError`` on a statement that can never be built, which is how
SQLServerColumnstoreIndexExpression reached CI.

These were source scans rather than runtime tests. A scan does not work here:
whether the field exists depends on inheritance reaching core, which lives in
another repository, and on **core_kwargs forwarding. Reading the source of this
repository can see neither, so a scan reported defects that were not there --
two were chased down and both were false alarms -- while a field genuinely
removed still passed. Building the statement answers the question the defect
actually asks: does this statement build, and does the schema reach the SQL?
"""
import importlib
import inspect

import pytest

#: Statement fields a formatter may read that some expression classes carry
#: under a different name. Reading these by their own name is the defect.
#: TruncateExpression and the PostgreSQL vacuum/statistics expressions name the
#: field `schema`; the DDL statements name it `schema_name`.
KNOWN_ALIASES = {
    "schema": {"TruncateExpression"},
}


class TestQualifiedStatementsRender:
    """A statement whose formatter qualifies names must build with a schema.

    Checked by building each statement and rendering it, not by scanning
    source. Each case names the statement and how to build it, so adding
    coverage for a newly qualified object type is one entry rather than a new
    mechanism.

    Oracle folds unquoted identifiers to upper case, so the expected SQL below
    carries ``"APP"`` where ``"app"`` was passed in.
    """

    @pytest.fixture
    def dialect(self):
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        return OracleDialect(version=(19, 0, 0))

    def test_create_sequence(self, dialect):
        from rhosocial.activerecord.backend.expression import CreateSequenceExpression

        expr = CreateSequenceExpression(dialect, sequence_name="seq_orders")
        assert expr.to_sql()[0] == (
            'CREATE SEQUENCE "SEQ_ORDERS" NOCYCLE NOORDER'
        ), expr.to_sql()[0]
        qualified = CreateSequenceExpression(
            dialect, sequence_name="seq_orders", schema_name="app"
        )
        assert qualified.to_sql()[0] == (
            'CREATE SEQUENCE "APP"."SEQ_ORDERS" NOCYCLE NOORDER'
        ), qualified.to_sql()[0]

    def test_create_synonym_qualifies_the_target_only(self, dialect):
        """The synonym itself is unqualified; only the table it points at is.

        Worth pinning because it is not obvious from the signature that the
        field lands on the target rather than on the synonym name.
        """
        from rhosocial.activerecord.backend.impl.oracle.expression.ddl.synonym import (
            OracleCreateSynonymExpression,
        )

        expr = OracleCreateSynonymExpression(
            dialect, synonym_name="syn_orders", table_name="orders"
        )
        assert expr.to_sql()[0] == (
            'CREATE SYNONYM "SYN_ORDERS" FOR "ORDERS"'
        ), expr.to_sql()[0]
        qualified = OracleCreateSynonymExpression(
            dialect, synonym_name="syn_orders", table_name="orders", schema_name="app"
        )
        assert qualified.to_sql()[0] == (
            'CREATE SYNONYM "SYN_ORDERS" FOR "APP"."ORDERS"'
        ), qualified.to_sql()[0]

    def test_disable_trigger(self, dialect):
        from rhosocial.activerecord.backend.impl.oracle.expression.trigger import (
            DisableTriggerExpression,
        )

        expr = DisableTriggerExpression(dialect, trigger_name="trg_audit")
        assert expr.to_sql()[0] == (
            'ALTER TRIGGER "TRG_AUDIT" DISABLE'
        ), expr.to_sql()[0]
        qualified = DisableTriggerExpression(
            dialect, trigger_name="trg_audit", schema_name="app"
        )
        assert qualified.to_sql()[0] == (
            'ALTER TRIGGER "APP"."TRG_AUDIT" DISABLE'
        ), qualified.to_sql()[0]

    def test_create_function(self, dialect):
        from rhosocial.activerecord.backend.impl.oracle.expression.ddl.routine import (
            OracleCreateFunctionExpression,
        )

        def build(schema_name=None):
            return OracleCreateFunctionExpression(
                dialect,
                function_name="fn_calc",
                return_type="NUMBER",
                body="RETURN 1;",
                schema_name=schema_name,
            )

        assert build().to_sql()[0] == (
            'CREATE OR REPLACE FUNCTION "FN_CALC" RETURN NUMBER AS RETURN 1;'
        )
        assert build(schema_name="app").to_sql()[0] == (
            'CREATE OR REPLACE FUNCTION "APP"."FN_CALC" RETURN NUMBER AS RETURN 1;'
        )

    def test_create_materialized_view_inherits_the_field_from_core(self, dialect):
        """The case a source scan got wrong in both directions.

        CreateMaterializedViewExpression lives in core and assigns schema_name
        there. A scan of this repository sees the formatter reading the field
        and no assignment at all, so it either misses a field that is there or
        reports one that is not, depending on how it resolves the base. Building
        it settles the question.
        """
        from rhosocial.activerecord.backend.expression import (
            Column,
            CreateMaterializedViewExpression,
            QueryExpression,
        )

        query = QueryExpression(dialect, [Column(dialect, "id")], from_="orders")
        expr = CreateMaterializedViewExpression(
            dialect, view_name="mv_orders", query=query
        )
        assert expr.to_sql()[0] == (
            'CREATE MATERIALIZED VIEW "MV_ORDERS" BUILD IMMEDIATE AS '
            'SELECT "ID" FROM "ORDERS"'
        ), expr.to_sql()[0]
        qualified = CreateMaterializedViewExpression(
            dialect, view_name="mv_orders", query=query, schema_name="app"
        )
        assert qualified.to_sql()[0] == (
            'CREATE MATERIALIZED VIEW "APP"."MV_ORDERS" BUILD IMMEDIATE AS '
            'SELECT "ID" FROM "ORDERS"'
        ), qualified.to_sql()[0]


class TestExpressionSignatures:
    """The expressions this backend's formatters qualify must take the field."""

    @pytest.mark.parametrize(
        "import_path,class_name",
        [
            (
                "rhosocial.activerecord.backend.expression.statements.ddl_sequence",
                "CreateSequenceExpression",
            ),
            (
                "rhosocial.activerecord.backend.impl.oracle.expression.ddl.synonym",
                "OracleCreateSynonymExpression",
            ),
            (
                "rhosocial.activerecord.backend.impl.oracle.expression.trigger",
                "DisableTriggerExpression",
            ),
            (
                "rhosocial.activerecord.backend.impl.oracle.expression.ddl.routine",
                "OracleCreateFunctionExpression",
            ),
            (
                "rhosocial.activerecord.backend.expression.statements.ddl_view",
                "CreateMaterializedViewExpression",
            ),
        ],
    )
    def test_qualified_expression_accepts_schema_name(self, import_path, class_name):
        module = importlib.import_module(import_path)
        cls = getattr(module, class_name)
        params = inspect.signature(cls.__init__).parameters
        assert "schema_name" in params, (
            f"{class_name} is qualified by its formatter, so it needs the "
            f"field; got {list(params)}"
        )
        assert params["schema_name"].default is None, (
            f"{class_name} must default schema_name to None -- None is what "
            f"means unqualified"
        )
