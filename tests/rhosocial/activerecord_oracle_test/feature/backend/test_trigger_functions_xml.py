# tests/rhosocial/activerecord_oracle_test/feature/backend/test_trigger_functions_xml.py
"""Offline snapshot tests for Oracle trigger DDL (mixins/trigger.py),
function-call formatting (mixins/functions.py + functions/*) and the
XMLType helper (types/xml.py).
"""
import pytest
from xml.etree.ElementTree import Element

from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.expression.statements.ddl_trigger import (
    CreateTriggerExpression, DropTriggerExpression, TriggerEvent, TriggerLevel,
    TriggerTiming,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.functions import analytic
from rhosocial.activerecord.backend.impl.oracle.functions import json as json_funcs
from rhosocial.activerecord.backend.impl.oracle.functions import string as string_funcs
from rhosocial.activerecord.backend.impl.oracle.types.xml import OracleXMLType


@pytest.fixture
def dialect() -> OracleDialect:
    return OracleDialect(version=(23, 0, 0))


def _trigger(dialect: OracleDialect, **kwargs) -> CreateTriggerExpression:
    defaults = dict(
        trigger_name="trg", table_name="t",
        timing=TriggerTiming.BEFORE, events=[TriggerEvent.INSERT],
        function_name="proc", level=TriggerLevel.ROW,
    )
    defaults.update(kwargs)
    return CreateTriggerExpression(dialect=dialect, **defaults)


class TestTriggerCapabilities:
    def test_basic_capabilities(self):
        d = OracleDialect(version=(23, 0, 0))
        assert d.supports_trigger()
        assert d.supports_instead_of_trigger()
        assert d.supports_system_trigger()
        assert d.supports_disable_trigger()
        assert d.supports_trigger_body_plsql()

    def test_compound_trigger_version_gate(self):
        assert OracleDialect(version=(11, 1, 0)).supports_compound_trigger()
        assert not OracleDialect(version=(10, 2, 0)).supports_compound_trigger()


class TestCreateTriggerFormatting:
    def test_row_trigger_with_call(self, dialect):
        expr = _trigger(dialect, trigger_name="trg_audit",
                        table_name="customers",
                        events=[TriggerEvent.INSERT, TriggerEvent.UPDATE],
                        function_name="audit_proc")
        sql, params = expr.to_sql()
        assert sql == ("CREATE OR REPLACE TRIGGER TRG_AUDIT BEFORE "
                       "INSERT OR UPDATE ON CUSTOMERS FOR EACH ROW CALL AUDIT_PROC")
        assert params == ()

    def test_update_of_columns(self, dialect):
        expr = _trigger(dialect, table_name="orders",
                        events=[TriggerEvent.UPDATE], function_name="do_it",
                        update_columns=["status", "note"])
        sql, _ = expr.to_sql()
        assert sql == ("CREATE OR REPLACE TRIGGER TRG BEFORE "
                       "UPDATE OF STATUS, NOTE ON ORDERS FOR EACH ROW CALL DO_IT")

    def test_instead_of_trigger_on_view(self, dialect):
        expr = _trigger(dialect, trigger_name="TRG", table_name="v",
                        timing=TriggerTiming.INSTEAD_OF,
                        events=[TriggerEvent.INSERT], function_name="p",
                        level=TriggerLevel.STATEMENT)
        sql, _ = expr.to_sql()
        assert sql == "CREATE OR REPLACE TRIGGER TRG INSTEAD OF INSERT ON V CALL P"

    def test_statement_level_becomes_compound_when_supported(self, dialect):
        expr = _trigger(dialect, level=TriggerLevel.STATEMENT)
        sql, _ = expr.to_sql()
        assert sql == ("CREATE OR REPLACE TRIGGER TRG BEFORE INSERT ON T "
                       "COMPOUND TRIGGER CALL PROC")

    def test_statement_level_before_11g_not_implemented(self):
        old = OracleDialect(version=(10, 2, 0))
        expr = _trigger(old, function_name="proc", level=TriggerLevel.STATEMENT)
        with pytest.raises(NotImplementedError):
            expr.to_sql()

    def test_referencing_clause_not_implemented(self, dialect):
        expr = _trigger(dialect, referencing="REFERENCING NEW AS N")
        with pytest.raises(NotImplementedError):
            expr.to_sql()

    def test_condition_clause_not_implemented(self, dialect):
        expr = _trigger(dialect, condition="1 > 0")
        with pytest.raises(NotImplementedError):
            expr.to_sql()


class TestDropAndToggleTriggers:
    def test_drop_trigger(self, dialect):
        sql, params = DropTriggerExpression(dialect, "trg_x").to_sql()
        assert sql == "DROP TRIGGER TRG_X"
        assert params == ()

    def test_drop_if_exists_rejected(self, dialect):
        expr = DropTriggerExpression(dialect, "trg_x", if_exists=True)
        with pytest.raises(NotImplementedError):
            expr.to_sql()

    def test_disable_and_enable(self, dialect):
        assert dialect.format_disable_trigger_statement("trg_a") == \
            ("ALTER TRIGGER TRG_A DISABLE", ())
        assert dialect.format_enable_trigger_statement("trg_a") == \
            ("ALTER TRIGGER TRG_A ENABLE", ())


class TestOracleFunctionFormatMixin:
    def test_listagg_plain(self, dialect):
        func = analytic.listagg(dialect, "ename", ",")
        assert func.to_sql() == ("LISTAGG(ename, ?)", (",",))

    def test_listagg_distinct_within_group_overflow(self, dialect):
        func = core.FunctionCall(dialect, "LISTAGG", core.Column(dialect, "e"),
                                 core.Literal(dialect, ";"), is_distinct=True)
        func._oracle_within_group = "e"
        func._oracle_on_overflow = "TRUNCATE"
        sql, params = dialect.format_function_call(func)
        assert sql == ("LISTAGG(DISTINCT e, ?) WITHIN GROUP (ORDER BY e) "
                       "ON OVERFLOW TRUNCATE")
        assert params == (";",)

    def test_percentile_cont_and_disc(self, dialect):
        cont = analytic.percentile_cont(dialect, 0.5, "sal")
        assert cont.to_sql() == ("PERCENTILE_CONT(?) WITHIN GROUP (ORDER BY sal)",
                                 (0.5,))
        disc = analytic.percentile_disc(dialect, 0.5, "sal DESC")
        assert disc.to_sql() == ("PERCENTILE_DISC(?) WITHIN GROUP (ORDER BY sal DESC)",
                                 (0.5,))

    def test_json_table_with_columns(self, dialect):
        func = json_funcs.json_table(
            dialect, "doc", "$",
            "ROW PATH 'x' COLUMNS (a VARCHAR2(10))",
        )
        assert func.to_sql() == (
            "JSON_TABLE(doc, ? COLUMNS (ROW PATH 'x' COLUMNS (a VARCHAR2(10))))",
            ("$",),
        )

    def test_json_table_without_columns(self, dialect):
        func = core.FunctionCall(dialect, "JSON_TABLE", core.Column(dialect, "doc"),
                                 core.Literal(dialect, "$"))
        assert func.to_sql() == ("JSON_TABLE(doc, ?)", ("$",))

    def test_plain_function_falls_back_to_generic(self, dialect):
        func = core.FunctionCall(dialect, "UPPER", core.Column(dialect, "name"))
        assert func.to_sql() == ("UPPER(name)", ())

    def test_alias_appended(self, dialect):
        func = core.FunctionCall(dialect, "UPPER", core.Column(dialect, "name"),
                                 alias="U")
        assert func.to_sql() == ("UPPER(name) AS U", ())

    def test_cast_types_wrap_result(self, dialect):
        func = core.FunctionCall(dialect, "MAX", core.Column(dialect, "v"))
        func = func.cast("CLOB")
        assert dialect.format_function_call(func) == ("CAST(MAX(v) AS CLOB)", ())


class TestStringFunctionFactories:
    def test_decode_with_default(self, dialect):
        call = string_funcs.decode_expr(dialect, "status", "A", 1, "B", 2, default=0)
        assert call.to_sql() == ("DECODE(status, ?, ?, ?, ?, ?)",
                                 ("A", 1, "B", 2, 0))

    def test_decode_without_default(self, dialect):
        call = string_funcs.decode_expr(dialect, "status", "A", 1)
        assert call.to_sql() == ("DECODE(status, ?, ?)", ("A", 1))

    def test_regexp_substr(self, dialect):
        basic = string_funcs.regexp_substr(dialect, "name", "[0-9]+")
        assert basic.to_sql() == ("REGEXP_SUBSTR(name, ?, ?, ?)", ("[0-9]+", 1, 1))
        full = string_funcs.regexp_substr(dialect, "name", "[0-9]+", 2, 3, "i")
        assert full.to_sql() == ("REGEXP_SUBSTR(name, ?, ?, ?, ?)",
                                 ("[0-9]+", 2, 3, "i"))

    def test_regexp_instr(self, dialect):
        call = string_funcs.regexp_instr(dialect, "col1", "x", 1, 1, 1)
        assert call.to_sql() == ("REGEXP_INSTR(col1, ?, ?, ?, ?)", ("x", 1, 1, 1))

    def test_regexp_like(self, dialect):
        plain = string_funcs.regexp_like(dialect, "email", ".+@.+")
        assert plain.to_sql() == ("REGEXP_LIKE(email, ?)", (".+@.+",))
        flagged = string_funcs.regexp_like(dialect, "email", ".+@.+", "i")
        assert flagged.to_sql() == ("REGEXP_LIKE(email, ?, ?)", (".+@.+", "i"))

    def test_regexp_replace(self, dialect):
        call = string_funcs.regexp_replace(dialect, "phone", "-", "", 1, 0)
        assert call.to_sql() == ("REGEXP_REPLACE(phone, ?, ?, ?, ?)", ("-", "", 1, 0))

    def test_regexp_count(self, dialect):
        call = string_funcs.regexp_count(dialect, "txt", "a", 1, "i")
        assert call.to_sql() == ("REGEXP_COUNT(txt, ?, ?, ?)", ("a", 1, "i"))


class TestJsonScalarFactories:
    def test_json_value(self, dialect):
        call = json_funcs.json_value(dialect, "doc", "$.a")
        assert call.to_sql() == ("JSON_VALUE(doc, ?)", ("$.a",))

    def test_json_query_and_exists(self, dialect):
        query = json_funcs.json_query(dialect, "doc", "$.a[*]")
        assert query.to_sql() == ("JSON_QUERY(doc, ?)", ("$.a[*]",))
        exists = json_funcs.json_exists(dialect, "doc", "$.a")
        assert exists.to_sql() == ("JSON_EXISTS(doc, ?)", ("$.a",))

    def test_json_value_with_returning_clause(self, dialect):
        call = json_funcs.json_value(dialect, "doc", "$.a", "VARCHAR2(100)")
        assert call.to_sql() == (
            "JSON_VALUE(doc, ? RETURNING VARCHAR2(100))", ("$.a",),
        )

    def test_json_query_with_returning_clause(self, dialect):
        call = json_funcs.json_query(dialect, "doc", "$.a[*]", "CLOB")
        assert call.to_sql() == (
            "JSON_QUERY(doc, ? RETURNING CLOB)", ("$.a[*]",),
        )


VALID_XML = '<root><name>John</name><item id="7">a</item></root>'


class TestOracleXMLType:
    def test_validity_flag(self):
        assert OracleXMLType(VALID_XML).is_valid is True
        assert OracleXMLType("not xml <").is_valid is False

    def test_extract_element_text(self):
        doc = OracleXMLType(VALID_XML)
        # The path namespace starts below the root element.
        assert doc.extract("/name") == "John"
        assert doc.extract("/missing") is None

    def test_extract_attribute(self):
        doc = OracleXMLType(VALID_XML)
        assert doc.extract("/item/@id") == "7"
        assert doc.extract("/name/@id") is None

    def test_extract_empty_path_returns_content(self):
        doc = OracleXMLType(VALID_XML)
        assert doc.extract("/") == VALID_XML

    def test_exists(self):
        doc = OracleXMLType(VALID_XML)
        assert doc.exists("/name") is True
        assert doc.exists("/nope") is False

    def test_to_string_preserves_document(self):
        assert OracleXMLType(VALID_XML).to_string() == VALID_XML
        invalid = OracleXMLType("plain text")
        assert invalid.to_string() == "plain text"

    def test_get_element_and_children(self):
        doc = OracleXMLType(VALID_XML)
        element = doc.get_element("/name")
        assert element is not None and element.tag == "name"
        assert doc.get_element("/missing") is None
        assert len(doc.get_children()) == 2
        assert doc.get_children("/missing") == []

    def test_from_element_round_trip(self):
        source = Element("top")
        doc = OracleXMLType.from_element(source)
        assert doc.content == "<top />"
        assert doc.root is source

    def test_from_dict_nested_structures(self):
        doc = OracleXMLType.from_dict({"a": 1, "b": {"c": ["x", "y"]}, "d": None})
        assert doc.content == ("<root><a>1</a><b><c><item>x</item>"
                               "<item>y</item></c></b><d>None</d></root>")
        assert doc.extract("/a") == "1"
        assert doc.extract("/b/c/item") == "x"

    def test_empty_content_is_invalid_but_inert(self):
        doc = OracleXMLType("")
        assert doc.is_valid is False
        assert doc.extract("/a") is None
