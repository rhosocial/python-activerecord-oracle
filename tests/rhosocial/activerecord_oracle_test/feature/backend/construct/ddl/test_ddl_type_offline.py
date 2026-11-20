# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_ddl_type_offline.py
"""Connectionless unit tests for Oracle user-defined TYPE DDL.

Everything here runs against a real but unbound :class:`OracleDialect`
(no driver, no session).  The two production modules under test are the
expression tree in ``expression/ddl/type.py`` and its capability/formatting
half in ``mixins/ddl_type.py``.  Validation helpers, alias resolution,
capability gates and SQL rendering are all exercised without a database.
"""

from dataclasses import replace
from typing import Any, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import Type
from rhosocial.activerecord.backend.expression.statements import (
    AlterTypeStatement,
    CreateTypeStatement,
    DropTypeStatement,
)
from rhosocial.activerecord.backend.expression.types import (
    IntegerType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.ddl.type import (
    DropTypeBodyStatement,
    OracleAlterTypeAddAttributeAction,
    OracleAlterTypeAddMethodAction,
    OracleAlterTypeCompileAction,
    OracleAlterTypeDependentHandling,
    OracleAlterTypeDropAttributeAction,
    OracleAlterTypeDropMethodAction,
    OracleAlterTypeElementTypeAction,
    OracleAlterTypeFinalAction,
    OracleAlterTypeInstantiableAction,
    OracleAlterTypeLimitAction,
    OracleAlterTypeModifyAttributeAction,
    OracleAlterTypeResetAction,
    OracleCreateTypeBodyStatement,
    OracleDropTypeExpression,
    OracleIncompleteTypeDefinition,
    OracleNestedTableTypeDefinition,
    OracleObjectTypeDefinition,
    OracleSqljTypeDefinition,
    OracleTypeAttribute,
    OracleTypeMethod,
    OracleVarrayTypeDefinition,
    _coerce_attribute,
    _coerce_attributes,
    _coerce_dependent_handling,
    _coerce_method,
    _coerce_methods,
    _has_top_level_comma,
    _normalize_flag,
    _normalize_invoker_rights,
    _normalize_keyword,
    _normalize_method_kind,
    _normalize_sqlj_using,
    _validate_accessible_by,
    _validate_accessor,
    _validate_fragment,
    _validate_method_name,
    _validate_method_parameter,
    _validate_method_parameters,
    _validate_name,
    _validate_object_external_names,
    _validate_optional_name,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def dialect() -> OracleDialect:
    return OracleDialect(version=(19, 0, 0), version_full=(19, 28, 0, 0, 0))


def _type(dialect: OracleDialect, name: str = "sample_t") -> Type:
    return Type(dialect, name)


class _BoundedType(BaseExpression):
    """A tiny expression that renders a fragment carrying a bind parameter.

    Used to drive the ``BaseExpression`` branch of ``_format_type_value`` where
    a render that still has bind parameters must be refused.
    """

    @property
    def format_method(self) -> str:
        return "format_noop"

    def to_sql(self, dialect: Optional["OracleDialect"] = None) -> Tuple[str, tuple]:
        return "SOME_TYPE(?)", (1,)


# ===========================================================================
# expression/ddl/type.py -- validation helpers
# ===========================================================================


class TestNameAndFragmentHelpers:
    @pytest.mark.parametrize("value", ["", "   ", None, 123])
    def test_validate_name_rejects_bad_values(self, value: Any) -> None:
        with pytest.raises(ValueError, match="must be a non-empty string"):
            _validate_name(value, "field")

    def test_validate_name_accepts_valid(self) -> None:
        _validate_name("ABC", "field")
        _validate_name("  trimmed  ", "field")

    @pytest.mark.parametrize("value", ["", "   ", None, 5])
    def test_validate_fragment_rejects_empty(self, value: Any) -> None:
        with pytest.raises(ValueError, match="non-empty string"):
            _validate_fragment(value, "field")

    @pytest.mark.parametrize("token", [";", "--", "/*", "*/", "a; DROP", "x /*c*/"])
    def test_validate_fragment_rejects_sql_tokens(self, token: str) -> None:
        with pytest.raises(ValueError, match="must not contain SQL statement"):
            _validate_fragment(token, "field")

    def test_validate_fragment_trims_valid(self) -> None:
        assert _validate_fragment("  NUMBER(10)  ", "field") == "NUMBER(10)"

    def test_validate_optional_name(self) -> None:
        assert _validate_optional_name(None, "field") is None
        assert _validate_optional_name(" base_t ", "field") == " base_t "
        with pytest.raises(ValueError):
            _validate_optional_name("  ", "field")


class TestFlagAndKeywordHelpers:
    def test_normalize_flag_negative_alias(self) -> None:
        assert _normalize_flag(None, True, "final") is False
        assert _normalize_flag(None, False, "final") is True

    def test_normalize_flag_positive(self) -> None:
        assert _normalize_flag(True, None, "final") is True
        assert _normalize_flag(None, None, "final") is None

    def test_normalize_flag_mutual_exclusion(self) -> None:
        with pytest.raises(ValueError, match="mutually exclusive"):
            _normalize_flag(True, True, "final")

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, None),
            (True, True),
            (False, False),
            ("invoker rights", True),
            ("  INVOKER   RIGHTS  ", True),
        ],
    )
    def test_normalize_invoker_rights(self, value: Any, expected: Optional[bool]) -> None:
        assert _normalize_invoker_rights(value) is expected

    @pytest.mark.parametrize("value", ["DEFINER", "DROP TABLE", 1, "invoker"])
    def test_normalize_invoker_rights_rejects_bad(self, value: Any) -> None:
        with pytest.raises(ValueError, match="invoker_rights"):
            _normalize_invoker_rights(value)

    @pytest.mark.parametrize("keyword", ["as", "AS", "Is", "IS"])
    def test_normalize_keyword(self, keyword: str) -> None:
        assert _normalize_keyword(keyword, "keyword") == keyword.upper()

    @pytest.mark.parametrize("keyword", ["", "ON", "as is", None])
    def test_normalize_keyword_rejects_bad(self, keyword: Any) -> None:
        with pytest.raises(ValueError, match="must be 'AS' or 'IS'"):
            _normalize_keyword(keyword, "keyword")


class TestAccessorHelpers:
    @pytest.mark.parametrize(
        "value",
        ["trusted", "APP.TRUSTED", '"Quoted"', "PROCEDURE trusted", "PACKAGE app.pkg"],
    )
    def test_validate_accessor_accepts(self, value: str) -> None:
        assert _validate_accessor(value) == value

    @pytest.mark.parametrize(
        "value",
        ["", "   ", "a; b", "a--b", "a(b)", "bad-name"],
    )
    def test_validate_accessor_rejects(self, value: str) -> None:
        with pytest.raises(ValueError):
            _validate_accessor(value)

    def test_validate_accessible_by_forms(self) -> None:
        assert _validate_accessible_by(None) is None
        assert _validate_accessible_by("pkg") == ("pkg",)
        assert _validate_accessible_by(["a", "b"]) == ("a", "b")

    @pytest.mark.parametrize("value", [[], 123, "", ["a", ""]])
    def test_validate_accessible_by_rejects(self, value: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            _validate_accessible_by(value)


class TestSqljAndMethodHelpers:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("sqldata", "SQLData"),
            ("CUSTOMDATUM", "CustomDatum"),
            ("oraData", "OraData"),
        ],
    )
    def test_normalize_sqlj_using(self, value: str, expected: str) -> None:
        assert _normalize_sqlj_using(value) == expected

    @pytest.mark.parametrize("value", ["", "  ", "JDBC", None])
    def test_normalize_sqlj_using_rejects_bad(self, value: Any) -> None:
        with pytest.raises(ValueError):
            _normalize_sqlj_using(value)

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, None),
            ("MEMBER", "MEMBER FUNCTION"),
            ("static", "STATIC FUNCTION"),
            ("constructor", "CONSTRUCTOR FUNCTION"),
            ("MAP", "MAP MEMBER FUNCTION"),
            ("ORDER MEMBER", "ORDER MEMBER FUNCTION"),
            ("MEMBER PROCEDURE", "MEMBER PROCEDURE"),
            ("STATIC PROCEDURE", "STATIC PROCEDURE"),
        ],
    )
    def test_normalize_method_kind(self, value: Any, expected: Any) -> None:
        assert _normalize_method_kind(value) == expected

    @pytest.mark.parametrize("value", ["", "   ", "WEIRD", 42])
    def test_normalize_method_kind_rejects_bad(self, value: Any) -> None:
        with pytest.raises(ValueError):
            _normalize_method_kind(value)

    @pytest.mark.parametrize("name", ["display", "get_value", '"Quoted"'])
    def test_validate_method_name_ok(self, name: str) -> None:
        assert _validate_method_name(name) == name

    @pytest.mark.parametrize("name", ["", "a(b)", "a-b", "a.b"])
    def test_validate_method_name_rejects(self, name: str) -> None:
        with pytest.raises(ValueError):
            _validate_method_name(name)

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("x NUMBER", False),
            ("x NUMBER, y NUMBER", True),
            ("f(x, y)", False),
            ("f(x), y", True),
            ("'a,b'", False),
            ('"a,b"', False),
            ("f(x", True),
            ("x)", True),
        ],
    )
    def test_has_top_level_comma(self, value: str, expected: bool) -> None:
        assert _has_top_level_comma(value) is expected


class TestMethodParameterHelpers:
    def test_validate_method_parameter_passthrough_expression(self) -> None:
        expr = IntegerType()
        assert _validate_method_parameter(expr) is expr

    @pytest.mark.parametrize("value", ["", "  ", 3])
    def test_validate_method_parameter_rejects_bad(self, value: Any) -> None:
        with pytest.raises(ValueError):
            _validate_method_parameter(value)

    @pytest.mark.parametrize("value", ["x NUMBER, y NUMBER", "a; b", "a /* c */"])
    def test_validate_method_parameter_rejects_unsafe(self, value: str) -> None:
        with pytest.raises(ValueError):
            _validate_method_parameter(value)

    def test_validate_method_parameters_variants(self) -> None:
        expr = IntegerType()
        assert _validate_method_parameters(None) is None
        assert _validate_method_parameters(expr) is expr
        assert _validate_method_parameters("x NUMBER") == "x NUMBER"
        assert _validate_method_parameters(["a", "b"]) == ["a", "b"]

    @pytest.mark.parametrize("value", [123, {"a": 1}])
    def test_validate_method_parameters_rejects_bad(self, value: Any) -> None:
        with pytest.raises(TypeError):
            _validate_method_parameters(value)


class TestCoercionHelpers:
    @pytest.mark.parametrize(
        "value",
        [
            {"id": "NUMBER"},
            {"name": "id", "data_type": "NUMBER"},
            ("id", "NUMBER"),
            ["id", "NUMBER"],
            "id NUMBER",
        ],
    )
    def test_coerce_attribute_forms(self, value: Any) -> None:
        attr = _coerce_attribute(value)
        assert attr.name == "id"
        assert attr.data_type == "NUMBER"

    @pytest.mark.parametrize("value", ["id", 42, ("id", "NUMBER", "x", "y")])
    def test_coerce_attribute_rejects_bad(self, value: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            _coerce_attribute(value)

    def test_coerce_attribute_without_type(self) -> None:
        attr = _coerce_attribute("id", require_type=False)
        assert attr.name == "id"
        assert attr.data_type is None

    def test_coerce_attributes_forms(self) -> None:
        assert _coerce_attributes(None) == []
        assert len(_coerce_attributes("id NUMBER")) == 1
        assert len(_coerce_attributes(["a NUMBER", "b NUMBER"])) == 2
        assert len(_coerce_attributes(("id", "NUMBER"))) == 1

    @pytest.mark.parametrize("value", [123, {"a": 1, "b": 2, "c": 3}])
    def test_coerce_attributes_rejects_bad(self, value: Any) -> None:
        with pytest.raises((TypeError, ValueError)):
            _coerce_attributes(value)

    @pytest.mark.parametrize(
        "value",
        ["MEMBER FUNCTION f RETURN NUMBER", ("MEMBER FUNCTION f RETURN NUMBER",), {"declaration": "x"}],
    )
    def test_coerce_method_forms(self, value: Any) -> None:
        assert isinstance(_coerce_method(value), OracleTypeMethod)

    @pytest.mark.parametrize("value", [42, ("a", "b", "c")])
    def test_coerce_method_rejects_bad(self, value: Any) -> None:
        with pytest.raises(TypeError):
            _coerce_method(value)

    def test_coerce_methods_forms(self) -> None:
        assert _coerce_methods(None) == []
        assert len(_coerce_methods("x")) == 1
        assert len(_coerce_methods(["x", "y"])) == 2

    @pytest.mark.parametrize("value", [42, {"a": 1}])
    def test_coerce_methods_rejects_bad(self, value: Any) -> None:
        with pytest.raises(TypeError):
            _coerce_methods(value)

    def test_validate_object_external_names_rejects_attribute(self) -> None:
        attrs = [OracleTypeAttribute("id", "NUMBER", external_name="java_id")]
        with pytest.raises(ValueError, match="SQLJ object types"):
            _validate_object_external_names(attrs, [])

    def test_validate_object_external_names_rejects_method(self) -> None:
        method = OracleTypeMethod("MEMBER FUNCTION f RETURN NUMBER EXTERNAL NAME 'x'")
        with pytest.raises(ValueError, match="SQLJ object types"):
            _validate_object_external_names([], [method])

    def test_coerce_dependent_handling_aliases(self) -> None:
        assert _coerce_dependent_handling("cascade including data") is (
            OracleAlterTypeDependentHandling.CASCADE_INCLUDING_TABLE_DATA
        )
        assert _coerce_dependent_handling("convert to substitutable") is (
            OracleAlterTypeDependentHandling.CASCADE_CONVERT_TO_SUBSTITUTABLE
        )
        assert _coerce_dependent_handling(None) is None

    @pytest.mark.parametrize("value", ["", "   ", 5, "MAKE IT SO"])
    def test_coerce_dependent_handling_rejects_bad(self, value: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            _coerce_dependent_handling(value)


# ===========================================================================
# expression/ddl/type.py -- expression classes
# ===========================================================================


class TestAttributeAndMethodClasses:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"name": ""},
            {"name": "id", "not_null": "yes"},
            {"name": "id", "external_name": "a; b"},
        ],
    )
    def test_attribute_validation(self, kwargs: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            OracleTypeAttribute(**kwargs)

    def test_attribute_external_name_is_validated(self) -> None:
        attr = OracleTypeAttribute("id", "NUMBER", external_name=" java_id ")
        assert "java_id" in attr.external_name

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"kind": "MEMBER FUNCTION", "name": "f"},
            {"kind": "MEMBER PROCEDURE", "name": "f", "return_type": "NUMBER"},
            {"kind": "CONSTRUCTOR FUNCTION", "name": "f", "return_type": "NUMBER"},
            {"declaration": "x", "kind": "MEMBER FUNCTION"},
            {"declaration": "x IS BEGIN NULL; END;"},
            {"kind": "MEMBER FUNCTION", "name": "bad-name", "return_type": "NUMBER"},
        ],
    )
    def test_method_validation(self, kwargs: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            OracleTypeMethod(**kwargs)

    def test_method_procedure_no_return_ok(self) -> None:
        method = OracleTypeMethod(kind="MEMBER PROCEDURE", name="refresh")
        assert method.kind == "MEMBER PROCEDURE"

    def test_method_constructor_self_as_result_ok(self) -> None:
        method = OracleTypeMethod(
            kind="CONSTRUCTOR FUNCTION",
            name="create",
            return_type="SELF AS RESULT",
        )
        assert method.return_type == "SELF AS RESULT"

    def test_method_require_return_type_false(self) -> None:
        method = OracleTypeMethod(
            kind="MEMBER FUNCTION",
            name="f",
            require_return_type=False,
        )
        assert method.return_type is None

    def test_method_structured_fields_coerced(self) -> None:
        method = OracleTypeMethod(
            kind="member",
            name="f",
            parameters=["x NUMBER", "y NUMBER"],
            return_type="NUMBER",
        )
        assert method.kind == "MEMBER FUNCTION"
        assert method.parameters == ["x NUMBER", "y NUMBER"]


class TestVarrayAndCollectionDefinitions:
    def test_varray_size_aliases(self) -> None:
        by_size = OracleVarrayTypeDefinition(None, "NUMBER", 10)
        by_max = OracleVarrayTypeDefinition(None, "NUMBER", max_size=11)
        by_length = OracleVarrayTypeDefinition(None, "NUMBER", max_length=12)
        by_size_alias = OracleVarrayTypeDefinition(None, "NUMBER", size=13)
        assert by_size.resolved_size_limit == 10
        assert by_max.resolved_size_limit == 11
        assert by_length.resolved_size_limit == 12
        assert by_size_alias.resolved_size_limit == 13

    def test_varray_swapped_int_arguments(self) -> None:
        defn = OracleVarrayTypeDefinition(None, 10, "NUMBER")
        assert defn.element_type == "NUMBER"
        assert defn.resolved_size_limit == 10

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"element_type": None},
            {"element_type": "NUMBER"},
            {"element_type": "NUMBER", "size_limit": 0},
            {"element_type": "NUMBER", "size_limit": True},
            {"element_type": "NUMBER", "size_limit": "10"},
            {"element_type": "NUMBER", "size_limit": 10, "max_size": 20},
            {"element_type": "NUMBER", "size_limit": 10, "force": "yes"},
            {"element_type": "NUMBER", "size_limit": 10, "not_null": 1},
        ],
    )
    def test_varray_validation(self, kwargs: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            OracleVarrayTypeDefinition(None, **kwargs)

    def test_varray_resolved_persistable(self) -> None:
        assert OracleVarrayTypeDefinition(None, "NUMBER", 5).resolved_persistable is None
        assert OracleVarrayTypeDefinition(
            None, "NUMBER", 5, persistable=True
        ).resolved_persistable is True

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"element_type": None},
            {"element_type": "NUMBER", "force": 1},
            {"element_type": "NUMBER", "not_null": 1},
        ],
    )
    def test_nested_table_validation(self, kwargs: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            OracleNestedTableTypeDefinition(None, **kwargs)


class TestObjectAndSqljDefinitions:
    def test_object_definition_flags(self, dialect: OracleDialect) -> None:
        defn = OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            final=True,
            not_instantiable=True,
            not_persistable=True,
        )
        assert defn.resolved_final is True
        assert defn.resolved_instantiable is False
        assert defn.resolved_persistable is False
        assert defn.definition_kind == "oracle.object"

    def test_object_definition_is_subtype(self, dialect: OracleDialect) -> None:
        defn = OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            under="base_t",
        )
        assert defn.under == "base_t"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"attributes": None},
            {"attributes": []},
            {"attributes": [OracleTypeAttribute("id", "NUMBER", not_null=True)]},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "force": 1},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "keyword": "ON"},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "authid": "PUBLIC"},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "editionable": True, "noneditionable": True},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "editionable": 1},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "sharing": "a; b"},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "oid": "x -- y"},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "default_collation": "x /* y */"},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "final": True, "not_final": True},
            {"attributes": [OracleTypeAttribute("id", "NUMBER")], "authid": "DEFINER", "invoker_rights": True},
        ],
    )
    def test_object_definition_validation(self, dialect: OracleDialect, kwargs: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            OracleObjectTypeDefinition(dialect, **kwargs)

    def test_sqlj_using_type_alias(self, dialect: OracleDialect) -> None:
        defn = OracleSqljTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            external_name="Person",
            language="JAVA",
            using_type="SQLData",
        )
        assert defn.normalized_using_clause == "SQLData"

    def test_sqlj_java_class_alias(self, dialect: OracleDialect) -> None:
        defn = OracleSqljTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            java_class="Person",
            language="JAVA",
            using_clause="SQLData",
        )
        assert defn.external_name == "Person"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {
                "attributes": [OracleTypeAttribute("id", "NUMBER")],
                "external_name": "Person",
                "language": "JAVA",
                "using_clause": "SQLData",
                "java_class": "Other",
            },
            {
                "attributes": [OracleTypeAttribute("id", "NUMBER")],
                "external_name": "Person",
                "language": "JAVA",
                "using_clause": "CustomDatum",
                "using_type": "SQLData",
            },
            {
                "attributes": [OracleTypeAttribute("id", "NUMBER")],
                "external_name": "Person",
                "language": "JAVA",
                "using_type": "   ",
            },
            {
                "attributes": [OracleTypeAttribute("id", "NUMBER")],
                "external_name": "x; y",
                "language": "JAVA",
                "using_clause": "SQLData",
            },
        ],
    )
    def test_sqlj_definition_validation(self, dialect: OracleDialect, kwargs: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            OracleSqljTypeDefinition(dialect, **kwargs)

    def test_incomplete_definition(self, dialect: OracleDialect) -> None:
        assert OracleIncompleteTypeDefinition(dialect).definition_kind == "oracle.incomplete"


class TestAlterActionClasses:
    @pytest.mark.parametrize("handling", ["INVALIDATE", "CASCADE", "cascade including data"])
    def test_dependent_handling_normalization(self, handling: str) -> None:
        action = OracleAlterTypeFinalAction(
            None, final=True, dependent_handling=handling
        )
        assert action.dependent_handling is not None

    @pytest.mark.parametrize(
        "factory",
        [
            lambda d: OracleAlterTypeAddAttributeAction(d),
            lambda d: OracleAlterTypeAddAttributeAction(d, "id", "NUMBER", attribute_name="id"),
            lambda d: OracleAlterTypeDropAttributeAction(d),
            lambda d: OracleAlterTypeDropAttributeAction(d, "id", names="id"),
            lambda d: OracleAlterTypeAddMethodAction(d, "x", declaration="x"),
            lambda d: OracleAlterTypeDropMethodAction(d, "x", name="x"),
            lambda d: OracleAlterTypeLimitAction(d, 2, new_limit=3),
            lambda d: OracleAlterTypeLimitAction(d, 0),
            lambda d: OracleAlterTypeElementTypeAction(d, "X", new_element_type="Y"),
            lambda d: OracleAlterTypeElementTypeAction(d),
            lambda d: OracleAlterTypeCompileAction(d, target="BODY", compile_target="SPECIFICATION"),
            lambda d: OracleAlterTypeCompileAction(d, target="OTHER"),
            lambda d: OracleAlterTypeCompileAction(d, debug=1),
            lambda d: OracleAlterTypeFinalAction(d),
            lambda d: OracleAlterTypeInstantiableAction(d),
            lambda d: OracleAlterTypeAddAttributeAction(d, "id", "NUMBER", force=1),
        ],
    )
    def test_alter_action_validation(self, dialect: OracleDialect, factory: Any) -> None:
        with pytest.raises((ValueError, TypeError)):
            factory(dialect)

    def test_modify_is_add_subclass(self, dialect: OracleDialect) -> None:
        action = OracleAlterTypeModifyAttributeAction(dialect, "id", "NUMBER")
        assert action.action_kind == "oracle.attribute.modify"
        assert action.resolved_attributes[0].name == "id"

    def test_drop_attribute_names_spelling(self, dialect: OracleDialect) -> None:
        action = OracleAlterTypeDropAttributeAction(dialect, names=["a", "b"])
        assert [a.name for a in action.resolved_attributes] == ["a", "b"]

    def test_limit_aliases(self, dialect: OracleDialect) -> None:
        assert OracleAlterTypeLimitAction(dialect, limit=5).resolved_limit == 5
        assert OracleAlterTypeLimitAction(dialect, size_limit=6).resolved_limit == 6
        assert OracleAlterTypeLimitAction(dialect, new_limit=7).resolved_limit == 7

    def test_element_type_alias(self, dialect: OracleDialect) -> None:
        action = OracleAlterTypeElementTypeAction(dialect, new_element_type="NUMBER")
        assert action.resolved_element_type == "NUMBER"

    def test_compile_target_alias(self, dialect: OracleDialect) -> None:
        action = OracleAlterTypeCompileAction(dialect, compile_target="body")
        assert action.resolved_target == "BODY"

    def test_final_and_instantiable_mandatory(self, dialect: OracleDialect) -> None:
        assert OracleAlterTypeFinalAction(dialect, not_final=True).resolved_final is False
        assert OracleAlterTypeInstantiableAction(
            dialect, is_instantiable=True
        ).resolved_instantiable is True

    def test_force_requires_cascade_handling(self, dialect: OracleDialect) -> None:
        action = OracleAlterTypeFinalAction(
            dialect,
            final=True,
            force=True,
            dependent_handling=OracleAlterTypeDependentHandling.INVALIDATE,
        )
        with pytest.raises(ValueError, match="force requires a CASCADE"):
            dialect._format_dependent_clause(action)

    def test_dependent_clause_without_handling(self, dialect: OracleDialect) -> None:
        action = OracleAlterTypeFinalAction(dialect, final=True, force=True)
        assert dialect._format_dependent_clause(action) == "CASCADE FORCE"


class TestStatementExpressions:
    def test_create_type_body_validation(self, dialect: OracleDialect) -> None:
        with pytest.raises((TypeError, AttributeError)):
            OracleCreateTypeBodyStatement(dialect, "not_a_type", "body")
        with pytest.raises(ValueError):
            OracleCreateTypeBodyStatement(dialect, _type(dialect), "  ")
        with pytest.raises(ValueError):
            OracleCreateTypeBodyStatement(
                dialect, _type(dialect), "body", if_not_exists=True, or_replace=True
            )
        with pytest.raises(ValueError):
            OracleCreateTypeBodyStatement(
                dialect, _type(dialect), "body", editionable=True, noneditionable=True
            )

    def test_create_type_body_format_method(self, dialect: OracleDialect) -> None:
        stmt = OracleCreateTypeBodyStatement(dialect, _type(dialect), "BEGIN NULL; END;")
        assert stmt.format_method == "format_create_type_body_statement"

    def test_drop_type_expression_validation(self, dialect: OracleDialect) -> None:
        with pytest.raises(ValueError):
            OracleDropTypeExpression(dialect, _type(dialect), force=True, validate=True)
        with pytest.raises(TypeError):
            OracleDropTypeExpression(dialect, _type(dialect), force=1)

    def test_drop_type_body_validation(self, dialect: OracleDialect) -> None:
        with pytest.raises((TypeError, AttributeError)):
            DropTypeBodyStatement(dialect, "not_a_type")
        stmt = DropTypeBodyStatement(dialect, _type(dialect))
        assert stmt.format_method == "format_drop_type_body_statement"


# ===========================================================================
# mixins/ddl_type.py -- capability gates
# ===========================================================================


class TestCapabilityGates:
    @pytest.mark.parametrize(
        "version,expected",
        [
            ((8, 0, 0), False),
            ((9, 0, 0), True),
            ((12, 2, 0), True),
            ((19, 0, 0), True),
            ((23, 0, 0), True),
            (None, False),
        ],
    )
    def test_has_type_support(self, version: Any, expected: bool) -> None:
        dialect = OracleDialect(version=version)
        assert dialect.supports_type_objects() is expected
        assert dialect.supports_create_type() is expected
        assert dialect.supports_alter_type() is expected
        assert dialect.supports_drop_type() is expected

    @pytest.mark.parametrize(
        "version,expected",
        [((12, 2, 0), False), ((17, 0, 0), False), ((18, 0, 0), True), ((23, 0, 0), True)],
    )
    def test_persistable_gate(self, version: Any, expected: bool) -> None:
        assert OracleDialect(version=version).supports_type_persistable() is expected

    @pytest.mark.parametrize(
        "version,expected",
        [
            ((8, 0, 0), False),
            ((9, 0, 0), True),
            ((23, 0, 0), True),
        ],
    )
    def test_other_type_gates(self, version: Any, expected: bool) -> None:
        dialect = OracleDialect(version=version)
        for name in (
            "supports_create_type_or_replace",
            "supports_multiple_type_alter_actions",
            "supports_create_type_body",
            "supports_drop_type_body",
            "supports_drop_type_force",
            "supports_drop_type_validate",
            "supports_type_attribute_actions",
            "supports_type_method_actions",
            "supports_type_limit_actions",
            "supports_type_compile",
            "supports_type_final",
            "supports_type_instantiable",
            "supports_sqlj_type",
        ):
            assert getattr(dialect, name)() is expected, name

    def test_supports_type_definition(self, dialect: OracleDialect) -> None:
        for defn_type in (
            OracleObjectTypeDefinition,
            OracleSqljTypeDefinition,
            OracleVarrayTypeDefinition,
            OracleNestedTableTypeDefinition,
            OracleIncompleteTypeDefinition,
        ):
            assert dialect.supports_type_definition(defn_type) is True
        assert dialect.supports_type_definition(str) is False
        assert dialect.supports_type_definition(object) is False

    def test_supports_type_alter_action(self, dialect: OracleDialect) -> None:
        for action_type in (
            OracleAlterTypeAddAttributeAction,
            OracleAlterTypeModifyAttributeAction,
            OracleAlterTypeDropAttributeAction,
            OracleAlterTypeAddMethodAction,
            OracleAlterTypeDropMethodAction,
            OracleAlterTypeLimitAction,
            OracleAlterTypeElementTypeAction,
            OracleAlterTypeCompileAction,
            OracleAlterTypeFinalAction,
            OracleAlterTypeInstantiableAction,
            OracleAlterTypeResetAction,
        ):
            assert dialect.supports_type_alter_action(action_type) is True
        assert dialect.supports_type_alter_action(str) is False
        assert dialect.supports_type_alter_action(object) is False

    def test_supported_type_definitions_tuple(self, dialect: OracleDialect) -> None:
        assert dialect.supported_type_definitions() == dialect._TYPE_DEFINITION_TYPES

    @pytest.mark.parametrize(
        "version,version_full,ru,expected",
        [
            ((19, 0, 0), None, None, False),
            ((19, 0, 0), (19, 27, 0, 0, 0), None, False),
            ((19, 0, 0), (19, 28, 0, 0, 0), None, True),
            ((19, 0, 0), (19, 28, 0, 0, 0), 27, False),
            ((19, 0, 0), (19, 28, 0, 0, 0), 28, True),
            ((19, 0, 0), (18, 28, 0, 0, 0), None, False),
            ((19, 28, 0), None, None, False),
            ((21, 3, 0), (21, 3, 0, 0, 0), None, False),
            ((23, 0, 0), None, None, True),
            ((26, 1, 0), None, None, True),
        ],
    )
    def test_if_exists_gate(
        self,
        version: Any,
        version_full: Any,
        ru: Any,
        expected: bool,
    ) -> None:
        dialect = OracleDialect(version=version, version_full=version_full, ru_version=ru)
        for name in (
            "supports_create_type_if_not_exists",
            "supports_alter_type_if_exists",
            "supports_drop_type_if_exists",
            "supports_create_type_body_if_not_exists",
            "supports_drop_type_body_if_exists",
        ):
            assert getattr(dialect, name)() is expected, name


# ===========================================================================
# mixins/ddl_type.py -- formatting
# ===========================================================================


class TestFormattingPrimitives:
    def test_format_type_value_string(self, dialect: OracleDialect) -> None:
        assert dialect._format_type_value("  NUMBER(10)  ") == "NUMBER(10)"

    def test_format_type_value_expression(self, dialect: OracleDialect) -> None:
        assert dialect._format_type_value(IntegerType(dialect=dialect)) == "NUMBER(10)"

    def test_format_type_value_with_params_rejected(self, dialect: OracleDialect) -> None:
        with pytest.raises(ValueError, match="without bind parameters"):
            dialect._format_type_value(_BoundedType(dialect))

    @pytest.mark.parametrize("value", ["", "  ", 5])
    def test_format_type_value_bad_type(self, dialect: OracleDialect, value: Any) -> None:
        with pytest.raises(TypeError):
            dialect._format_type_value(value)

    @pytest.mark.parametrize("value", ["x; y", "x -- y", "x /* y */"])
    def test_format_type_value_tokens(self, dialect: OracleDialect, value: str) -> None:
        with pytest.raises(ValueError):
            dialect._format_type_value(value)

    def test_format_type_reference_qualified(self, dialect: OracleDialect) -> None:
        assert dialect._format_type_reference("app.sample_t") == '"APP"."SAMPLE_T"'

    def test_format_type_reference_expression(self, dialect: OracleDialect) -> None:
        assert dialect._format_type_reference(IntegerType(dialect=dialect)) == "NUMBER(10)"

    def test_format_type_reference_quoted_passthrough(self, dialect: OracleDialect) -> None:
        assert dialect._format_type_reference('"MixedCase"') == '"MixedCase"'

    @pytest.mark.parametrize("value", ["", 5, "x; y"])
    def test_format_type_reference_bad(self, dialect: OracleDialect, value: Any) -> None:
        with pytest.raises((TypeError, ValueError)):
            dialect._format_type_reference(value)

    def test_format_external_name_escapes_quotes(self, dialect: OracleDialect) -> None:
        assert dialect._format_external_name("O'Reilly") == "EXTERNAL NAME 'O''Reilly'"

    def test_format_attribute_expression_type(self, dialect: OracleDialect) -> None:
        attr = OracleTypeAttribute("id", IntegerType(dialect=dialect))
        assert dialect._format_attribute(attr) == '"ID" NUMBER(10)'

    def test_format_attribute_no_data_type(self, dialect: OracleDialect) -> None:
        with pytest.raises(ValueError, match="data_type is required"):
            dialect._format_attribute(OracleTypeAttribute("id"))

    def test_format_attribute_not_null_rejected(self, dialect: OracleDialect) -> None:
        attr = replace(OracleTypeAttribute("id", "NUMBER"), not_null=True)
        with pytest.raises(ValueError, match="NOT NULL"):
            dialect._format_attribute(attr)

    def test_format_method_expression_parameter(self, dialect: OracleDialect) -> None:
        method = OracleTypeMethod(
            kind="MEMBER FUNCTION",
            name="f",
            parameters=[VarCharType(dialect=dialect, length=10)],
            return_type="NUMBER",
        )
        assert "VARCHAR2(10)" in dialect._format_method(method)

    def test_format_method_require_return_type_false(self, dialect: OracleDialect) -> None:
        method = OracleTypeMethod(
            kind="MEMBER FUNCTION", name="f", require_return_type=False
        )
        with pytest.raises(ValueError, match="return_type is required"):
            dialect._format_method(method)

    def test_format_drop_method_declaration(self, dialect: OracleDialect) -> None:
        method = OracleTypeMethod(
            declaration="MEMBER FUNCTION f(x NUMBER) RETURN NUMBER"
        )
        rendered = dialect._format_drop_method(method)
        assert rendered == "MEMBER FUNCTION f(x NUMBER)"

    def test_format_drop_method_bare_declaration(self, dialect: OracleDialect) -> None:
        method = OracleTypeMethod(declaration="refresh_stats")
        assert dialect._format_drop_method(method) == "MEMBER FUNCTION refresh_stats"

    def test_format_drop_method_requires_name_or_declaration(
        self, dialect: OracleDialect
    ) -> None:
        method = OracleTypeMethod(kind="MEMBER FUNCTION", name="f", require_return_type=False)
        # name path works, so no error; a nameless structured method is impossible
        assert "f" in dialect._format_drop_method(method)

    def test_format_flags_all(self, dialect: OracleDialect) -> None:
        expr = OracleObjectTypeDefinition(
            dialect,
            [OracleTypeAttribute("id", "NUMBER")],
            final=True,
            not_instantiable=True,
            not_persistable=True,
        )
        assert dialect._format_flags(expr) == ["FINAL", "NOT INSTANTIABLE", "NOT PERSISTABLE"]

    def test_format_common_prefix(self, dialect: OracleDialect) -> None:
        expr = OracleObjectTypeDefinition(
            dialect,
            [OracleTypeAttribute("id", "NUMBER")],
            force=True,
            authid="definer",
            sharing="METADATA LINK",
            default_collation="BINARY_CI",
            accessible_by="PROCEDURE trusted",
        )
        prefix = dialect._format_common_prefix(expr)
        assert "FORCE" in prefix
        assert "AUTHID DEFINER" in prefix
        assert "METADATA LINK" in prefix
        assert "DEFAULT COLLATION BINARY_CI" in prefix
        assert "ACCESSIBLE BY (PROCEDURE trusted)" in prefix

    def test_format_common_prefix_invoker_rights(self, dialect: OracleDialect) -> None:
        expr = OracleObjectTypeDefinition(
            dialect,
            [OracleTypeAttribute("id", "NUMBER")],
            invoker_rights=True,
        )
        assert "AUTHID CURRENT_USER" in dialect._format_common_prefix(expr)


class TestDefinitionRendering:
    def test_object_definition_with_methods(self, dialect: OracleDialect) -> None:
        defn = OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            methods=[OracleTypeMethod("MEMBER FUNCTION display RETURN VARCHAR2")],
        )
        sql = CreateTypeStatement(dialect, _type(dialect), defn).to_sql()[0]
        assert "AS OBJECT" in sql
        assert "MEMBER FUNCTION display RETURN VARCHAR2" in sql

    def test_object_definition_under_and_oid(self, dialect: OracleDialect) -> None:
        defn = OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            under="app.base_t",
            oid="O'ID",
        )
        sql = CreateTypeStatement(dialect, _type(dialect), defn).to_sql()[0]
        assert "OID 'O''ID'" in sql
        assert 'UNDER "APP"."BASE_T"' in sql

    def test_varray_persistable_render(self, dialect: OracleDialect) -> None:
        defn = OracleVarrayTypeDefinition(dialect, "NUMBER", 4, persistable=True)
        sql = CreateTypeStatement(dialect, _type(dialect), defn).to_sql()[0]
        assert "AS VARRAY(4) OF (NUMBER) PERSISTABLE" in sql

    def test_nested_table_not_null(self, dialect: OracleDialect) -> None:
        defn = OracleNestedTableTypeDefinition(dialect, "NUMBER", not_null=True)
        sql = CreateTypeStatement(dialect, _type(dialect), defn).to_sql()[0]
        assert "AS TABLE OF NUMBER NOT NULL" in sql

    def test_sqlj_definition_render(self, dialect: OracleDialect) -> None:
        defn = OracleSqljTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER", "java_id")],
            external_name="Person",
            language="JAVA",
            using_clause="CustomDatum",
        )
        sql = CreateTypeStatement(dialect, _type(dialect), defn).to_sql()[0]
        assert "EXTERNAL NAME 'Person'" in sql
        assert "LANGUAGE JAVA" in sql
        assert "USING CustomDatum" in sql
        assert "EXTERNAL NAME 'java_id'" in sql

    def test_incomplete_definition_renders_just_name(self, dialect: OracleDialect) -> None:
        defn = OracleIncompleteTypeDefinition(dialect)
        sql = CreateTypeStatement(dialect, _type(dialect), defn).to_sql()[0]
        assert sql == 'CREATE TYPE "SAMPLE_T"'

    def test_create_type_editionable(self, dialect: OracleDialect) -> None:
        defn = OracleObjectTypeDefinition(
            dialect,
            [OracleTypeAttribute("id", "NUMBER")],
            editionable=True,
        )
        sql = CreateTypeStatement(dialect, _type(dialect), defn, or_replace=True).to_sql()[0]
        assert sql.startswith("CREATE OR REPLACE EDITIONABLE TYPE")

    def test_create_type_noneditionable(self, dialect: OracleDialect) -> None:
        defn = OracleObjectTypeDefinition(
            dialect,
            [OracleTypeAttribute("id", "NUMBER")],
            noneditionable=True,
        )
        sql = CreateTypeStatement(dialect, _type(dialect), defn).to_sql()[0]
        assert sql.startswith("CREATE NONEDITIONABLE TYPE")

    def test_create_type_with_params_rejected(self, dialect: OracleDialect) -> None:
        defn = OracleObjectTypeDefinition(
            dialect, [OracleTypeAttribute("id", _BoundedType(dialect))]
        )
        with pytest.raises(ValueError, match="without bind parameters"):
            CreateTypeStatement(dialect, _type(dialect), defn).to_sql()

    def test_format_type_definition_unsupported_version(self) -> None:
        old = OracleDialect(version=(8, 0, 0))
        defn = OracleIncompleteTypeDefinition(old)
        with pytest.raises(UnsupportedFeatureError, match="TYPE definition"):
            old.format_type_definition(defn)

    def test_format_type_definition_unsupported_kind(self, dialect: OracleDialect) -> None:
        from rhosocial.activerecord.backend.expression.statements.ddl_type import (
            TypeDefinition,
        )

        class ForeignDefinition(TypeDefinition):
            definition_kind = "foreign"

        with pytest.raises(UnsupportedFeatureError, match="foreign"):
            dialect.format_type_definition(ForeignDefinition(dialect))


class TestStatementFormatting:
    def test_create_type_error_paths(self) -> None:
        old = OracleDialect(version=(8, 0, 0))
        defn = OracleIncompleteTypeDefinition(old)
        with pytest.raises(UnsupportedFeatureError, match="CREATE TYPE"):
            CreateTypeStatement(old, _type(old), defn).to_sql()

    def test_create_type_if_not_exists_with_force(self, dialect: OracleDialect) -> None:
        defn = OracleVarrayTypeDefinition(dialect, "NUMBER", 4, force=True)
        with pytest.raises(ValueError, match="FORCE"):
            CreateTypeStatement(
                dialect, _type(dialect), defn, if_not_exists=True
            ).to_sql()

    def test_alter_type_single_actions(self, dialect: OracleDialect) -> None:
        cases = [
            (OracleAlterTypeFinalAction(dialect, not_final=True), "NOT FINAL"),
            (OracleAlterTypeInstantiableAction(dialect, instantiable=True), "INSTANTIABLE"),
            (OracleAlterTypeLimitAction(dialect, 9), "MODIFY LIMIT 9"),
            (OracleAlterTypeElementTypeAction(dialect, "NUMBER"), "MODIFY ELEMENT TYPE NUMBER"),
            (OracleAlterTypeCompileAction(dialect), "COMPILE"),
            (OracleAlterTypeResetAction(dialect), "RESET"),
        ]
        for action, fragment in cases:
            sql = AlterTypeStatement(dialect, _type(dialect), [action]).to_sql()[0]
            assert fragment in sql

    def test_alter_type_compile_options(self, dialect: OracleDialect) -> None:
        action = OracleAlterTypeCompileAction(
            dialect,
            debug=True,
            target="BODY",
            reuse_settings=True,
            compiler_parameters=["PLSQL_OPTIMIZE_LEVEL=2"],
        )
        sql = AlterTypeStatement(dialect, _type(dialect), [action]).to_sql()[0]
        assert "COMPILE DEBUG BODY PLSQL_OPTIMIZE_LEVEL=2 REUSE SETTINGS" in sql

    def test_alter_type_compile_rejects_dependent_handling(self, dialect: OracleDialect) -> None:
        action = OracleAlterTypeCompileAction(
            dialect, dependent_handling=OracleAlterTypeDependentHandling.CASCADE
        )
        with pytest.raises(ValueError, match="COMPILE does not accept"):
            dialect.format_type_alter_action(action)

    def test_alter_type_reset_rejects_dependent_handling(self, dialect: OracleDialect) -> None:
        action = OracleAlterTypeResetAction(dialect, force=True)
        with pytest.raises(ValueError, match="RESET does not accept"):
            dialect.format_type_alter_action(action)

    def test_alter_type_multiple_actions_rejected(self) -> None:
        old = OracleDialect(version=(8, 0, 0))
        action = OracleAlterTypeFinalAction(old, final=True)
        with pytest.raises(UnsupportedFeatureError):
            AlterTypeStatement(old, _type(old), [action, action]).to_sql()

    def test_alter_type_mixed_families_rejected(self, dialect: OracleDialect) -> None:
        actions = [
            OracleAlterTypeFinalAction(dialect, final=True),
            OracleAlterTypeLimitAction(dialect, 4),
        ]
        with pytest.raises(UnsupportedFeatureError, match="mixed ALTER TYPE"):
            AlterTypeStatement(dialect, _type(dialect), actions).to_sql()

    def test_alter_type_multiple_limit_actions_rejected(self, dialect: OracleDialect) -> None:
        actions = [
            OracleAlterTypeLimitAction(dialect, 4),
            OracleAlterTypeLimitAction(dialect, 5),
        ]
        with pytest.raises(UnsupportedFeatureError, match="multiple collection.limit"):
            AlterTypeStatement(dialect, _type(dialect), actions).to_sql()

    def test_drop_type_force_validate(self, dialect: OracleDialect) -> None:
        assert OracleDropTypeExpression(dialect, _type(dialect), force=True).to_sql()[0] == (
            'DROP TYPE "SAMPLE_T" FORCE'
        )
        assert OracleDropTypeExpression(dialect, _type(dialect), validate=True).to_sql()[0] == (
            'DROP TYPE "SAMPLE_T" VALIDATE'
        )

    def test_drop_type_if_exists(self, dialect: OracleDialect) -> None:
        sql = DropTypeStatement(dialect, _type(dialect), if_exists=True).to_sql()[0]
        assert sql == 'DROP TYPE IF EXISTS "SAMPLE_T"'

    def test_create_type_body_versions(self, dialect: OracleDialect) -> None:
        body = OracleCreateTypeBodyStatement(dialect, _type(dialect), "BEGIN NULL; END;")
        assert body.to_sql()[0].startswith("CREATE TYPE BODY")
        replace = OracleCreateTypeBodyStatement(
            dialect, _type(dialect), "BEGIN NULL; END;", or_replace=True
        )
        assert replace.to_sql()[0].startswith("CREATE OR REPLACE TYPE BODY")

    def test_drop_type_body(self, dialect: OracleDialect) -> None:
        sql = DropTypeBodyStatement(dialect, _type(dialect), if_exists=True).to_sql()[0]
        assert sql == 'DROP TYPE BODY IF EXISTS "SAMPLE_T"'

    def test_drop_type_body_unsupported_version(self) -> None:
        old = OracleDialect(version=(8, 0, 0))
        stmt = DropTypeBodyStatement(old, _type(old))
        with pytest.raises(UnsupportedFeatureError, match="DROP TYPE BODY"):
            stmt.to_sql()
