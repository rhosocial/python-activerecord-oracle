# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_type_ddl.py
"""Oracle TYPE DDL SQL and capability tests."""

from typing import Any, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.mixins import UserDefinedTypeMixin
from rhosocial.activerecord.backend.dialect.protocols import UserDefinedTypeSupport
from rhosocial.activerecord.backend.expression.serialization import deserialize, serialize
from rhosocial.activerecord.backend.expression.statements import (
    AlterTypeExpression,
    CreateTypeExpression,
    DropTypeExpression,
)
from rhosocial.activerecord.backend.impl.oracle.async_backend import AsyncOracleBackend
from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.mixins import OracleTypeDDLMixin
from rhosocial.activerecord.backend.impl.oracle.expression.ddl.type import (
    DropTypeBodyExpression,
    OracleAlterTypeAddAttributeAction,
    OracleAlterTypeAddMethodAction,
    OracleAlterTypeCompileAction,
    OracleAlterTypeDependentHandling,
    OracleAlterTypeDropAttributeAction,
    OracleAlterTypeDropMethodAction,
    OracleAlterTypeFinalAction,
    OracleAlterTypeInstantiableAction,
    OracleAlterTypeLimitAction,
    OracleAlterTypeModifyAttributeAction,
    OracleCreateTypeBodyExpression,
    OracleDropTypeExpression,
    OracleIncompleteTypeDefinition,
    OracleNestedTableTypeDefinition,
    OracleObjectTypeDefinition,
    OracleSqljTypeDefinition,
    OracleTypeAttribute,
    OracleTypeMethod,
    OracleVarrayTypeDefinition,
)
from rhosocial.activerecord.backend.impl.oracle.protocols import OracleTypeDDLSupport


def _dialect(
    version: Tuple[int, int, int] = (19, 0, 0),
    version_full: Optional[Tuple[int, ...]] = None,
) -> OracleDialect:
    return OracleDialect(version=version, version_full=version_full)


def test_type_protocol_and_definition_sql() -> None:
    dialect = _dialect(version_full=(19, 28, 0, 0, 0))
    assert issubclass(OracleTypeDDLSupport, UserDefinedTypeSupport)
    assert isinstance(dialect, UserDefinedTypeSupport)
    assert isinstance(dialect, OracleTypeDDLSupport)
    mro = OracleDialect.__mro__
    assert mro.index(OracleTypeDDLMixin) < mro.index(UserDefinedTypeMixin)
    assert mro.index(OracleTypeDDLSupport) < mro.index(UserDefinedTypeSupport)

    definition = OracleObjectTypeDefinition(
        dialect,
        attributes=[
            OracleTypeAttribute("id", "NUMBER(10)"),
            OracleTypeAttribute("name", "VARCHAR2(100)"),
        ],
        methods=[OracleTypeMethod("MEMBER FUNCTION display RETURN VARCHAR2")],
        not_final=True,
        instantiable=False,
    )
    create = CreateTypeExpression(dialect, "person_t", definition, or_replace=True)
    sql, params = create.to_sql()
    assert sql == (
        'CREATE OR REPLACE TYPE "PERSON_T" AS OBJECT ('
        '"ID" NUMBER(10), "NAME" VARCHAR2(100), '
        'MEMBER FUNCTION display RETURN VARCHAR2) NOT FINAL NOT INSTANTIABLE'
    )
    assert params == ()


def test_definition_option_validation_and_accessible_by() -> None:
    dialect = _dialect()
    definition = OracleObjectTypeDefinition(
        dialect,
        attributes=[OracleTypeAttribute("id", "NUMBER")],
        accessible_by=["PROCEDURE trusted", "PACKAGE APP.TRUSTED"],
    )
    assert CreateTypeExpression(dialect, "secured_t", definition).to_sql()[0] == (
        'CREATE TYPE "SECURED_T" ACCESSIBLE BY (PROCEDURE trusted, '
        'PACKAGE APP.TRUSTED) AS OBJECT ("ID" NUMBER)'
    )
    with pytest.raises(ValueError):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            under="base_t",
            persistable=True,
        )
    subtype = OracleObjectTypeDefinition(
        dialect,
        attributes=[OracleTypeAttribute("id", "NUMBER")],
        under="base_t",
    )
    assert "PERSISTABLE" not in CreateTypeExpression(dialect, "sub_t", subtype).to_sql()[0]
    with pytest.raises(ValueError):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            accessible_by=[],
        )
    with pytest.raises(ValueError):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            external_name="id",
        )
    with pytest.raises(ValueError):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            accessible_by="(PROCEDURE trusted)",
        )
    with pytest.raises(ValueError):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER", external_name="id")],
        )
    with pytest.raises(ValueError):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            methods=[OracleTypeMethod("MEMBER FUNCTION f RETURN NUMBER", "f() return int")],
        )


def test_invoker_rights_is_validated_and_rendered_canonically() -> None:
    dialect = _dialect()
    for invoker_rights in (True, "invoker rights"):
        definition = OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            invoker_rights=invoker_rights,
        )
        sql = CreateTypeExpression(dialect, "secure_t", definition).to_sql()[0]
        assert "AUTHID CURRENT_USER" in sql
        assert "INVOKER RIGHTS" not in sql
    with pytest.raises(ValueError, match="invoker_rights"):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            invoker_rights="DROP TABLE person_t",
        )
    with pytest.raises(ValueError, match="mutually exclusive"):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            authid="DEFINER",
            invoker_rights=True,
        )
    definition.invoker_rights = "INVALID"
    with pytest.raises(ValueError, match="invoker_rights"):
        CreateTypeExpression(dialect, "secure_t", definition).to_sql()


def test_structured_method_kind_return_type_and_name_validation() -> None:
    with pytest.raises(ValueError, match="method kind"):
        OracleTypeMethod(name="value", return_type="NUMBER")
    with pytest.raises(ValueError, match="requires return_type"):
        OracleTypeMethod(kind="MEMBER FUNCTION", name="value")
    with pytest.raises(ValueError, match="must not define return_type"):
        OracleTypeMethod(kind="MEMBER PROCEDURE", name="refresh", return_type="NUMBER")
    with pytest.raises(ValueError, match="Oracle identifier"):
        OracleTypeMethod(kind="MEMBER FUNCTION", name="value() return int", return_type="NUMBER")


def test_collection_sqlj_and_incomplete_sql() -> None:
    dialect = _dialect(version_full=(19, 28, 0, 0, 0))
    varray = OracleVarrayTypeDefinition(dialect, "NUMBER", 10, not_null=True)
    nested = OracleNestedTableTypeDefinition(dialect, "VARCHAR2(30)", not_persistable=True)
    sqlj = OracleSqljTypeDefinition(
        dialect,
        attributes=[OracleTypeAttribute("name", "VARCHAR2(30)", "name")],
        methods=[OracleTypeMethod("MEMBER FUNCTION id RETURN NUMBER", "id() return int")],
        external_name="Person",
        language="JAVA",
        using_clause="SQLData",
    )
    incomplete = OracleIncompleteTypeDefinition(dialect)

    assert CreateTypeExpression(dialect, "numbers_t", varray).to_sql() == (
        'CREATE TYPE "NUMBERS_T" AS VARRAY(10) OF NUMBER NOT NULL'
    )
    assert CreateTypeExpression(dialect, "names_t", nested).to_sql() == (
        'CREATE TYPE "NAMES_T" AS TABLE OF (VARCHAR2(30)) NOT PERSISTABLE'
    )
    assert CreateTypeExpression(dialect, "person_java", sqlj).to_sql() == (
        'CREATE TYPE "PERSON_JAVA" AS OBJECT EXTERNAL NAME \'Person\' LANGUAGE JAVA '
        'USING SQLData ("NAME" VARCHAR2(30) EXTERNAL NAME \'name\', '
        'MEMBER FUNCTION id RETURN NUMBER EXTERNAL NAME \'id() return int\')'
    )
    with pytest.raises(ValueError, match="EXTERNAL NAME"):
        OracleSqljTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            language="JAVA",
            using_clause="SQLData",
        )
    with pytest.raises(ValueError, match="LANGUAGE JAVA"):
        OracleSqljTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            external_name="Person",
            using_clause="SQLData",
        )
    with pytest.raises(ValueError, match="USING"):
        OracleSqljTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            external_name="Person",
            language="JAVA",
        )
    assert CreateTypeExpression(dialect, "forward_t", incomplete).to_sql() == (
        'CREATE TYPE "FORWARD_T"'
    )


def test_persistable_clause_requires_oracle_18c() -> None:
    legacy = _dialect(version=(12, 2, 0))
    supported = _dialect(version=(18, 0, 0))
    legacy_definition = OracleVarrayTypeDefinition(legacy, "NUMBER", 4, persistable=True)
    supported_definition = OracleVarrayTypeDefinition(
        supported,
        "NUMBER",
        4,
        not_persistable=True,
    )

    assert legacy.supports_type_persistable() is False
    assert supported.supports_type_persistable() is True
    with pytest.raises(UnsupportedFeatureError, match="PERSISTABLE"):
        CreateTypeExpression(legacy, "legacy_numbers_t", legacy_definition).to_sql()
    assert "NOT PERSISTABLE" in CreateTypeExpression(
        supported,
        "numbers_t",
        supported_definition,
    ).to_sql()[0]


def test_alter_actions_and_dependent_handling() -> None:
    dialect = _dialect(version_full=(19, 28, 0, 0, 0))
    actions = [
        OracleAlterTypeAddAttributeAction(
            dialect,
            OracleTypeAttribute("created_at", "TIMESTAMP"),
            dependent_handling=OracleAlterTypeDependentHandling.CASCADE,
        ),
        OracleAlterTypeAddAttributeAction(
            dialect,
            OracleTypeAttribute("tag", "VARCHAR2(20)"),
        ),
    ]
    alter = AlterTypeExpression(dialect, "person_t", actions)
    sql, params = alter.to_sql()
    assert sql == (
        'ALTER TYPE "PERSON_T" ADD ATTRIBUTE ("CREATED_AT" TIMESTAMP, '
        '"TAG" VARCHAR2(20)) CASCADE'
    )
    assert params == ()

    method_actions = [
        OracleAlterTypeAddMethodAction(dialect, "MEMBER FUNCTION label RETURN VARCHAR2"),
        OracleAlterTypeDropMethodAction(
            dialect,
            name="old_label",
            kind="MEMBER FUNCTION",
            parameters=[],
            dependent_handling=OracleAlterTypeDependentHandling.CASCADE,
        ),
    ]
    method_sql = AlterTypeExpression(dialect, "person_t", method_actions).to_sql()[0]
    assert method_sql == (
        'ALTER TYPE "PERSON_T" ADD MEMBER FUNCTION label RETURN VARCHAR2, '
        'DROP MEMBER FUNCTION old_label() CASCADE'
    )
    with pytest.raises(UnsupportedFeatureError):
        AlterTypeExpression(
            dialect,
            "person_t",
            [actions[0], method_actions[0]],
        ).to_sql()
    with pytest.raises(UnsupportedFeatureError):
        AlterTypeExpression(
            dialect,
            "person_t",
            [
                actions[0],
                OracleAlterTypeModifyAttributeAction(
                    dialect,
                    OracleTypeAttribute("name", "VARCHAR2(200)"),
                ),
            ],
        ).to_sql()

    drop_attribute = OracleAlterTypeDropAttributeAction(dialect, "created_at")
    drop_method = OracleAlterTypeDropMethodAction(
        dialect,
        name="label",
        kind="MEMBER FUNCTION",
        parameters=[],
    )
    compile_action = OracleAlterTypeCompileAction(dialect, target="BODY", debug=True)
    final_action = OracleAlterTypeFinalAction(dialect, not_final=True)
    instantiable_action = OracleAlterTypeInstantiableAction(dialect, not_instantiable=True)
    limit_action = OracleAlterTypeLimitAction(dialect, 20)
    assert drop_attribute.to_sql()[0].startswith("DROP ATTRIBUTE")
    assert drop_method.to_sql()[0] == "DROP MEMBER FUNCTION label()"
    assert compile_action.to_sql()[0] == "COMPILE DEBUG BODY"
    assert final_action.to_sql()[0] == "NOT FINAL"
    assert instantiable_action.to_sql()[0] == "NOT INSTANTIABLE"
    assert limit_action.to_sql()[0] == "MODIFY LIMIT 20"


def test_body_and_force_validate_drop_are_separate_expressions() -> None:
    dialect = _dialect(version_full=(19, 28, 0, 0, 0))
    body = OracleCreateTypeBodyExpression(
        dialect,
        "person_t",
        "MEMBER FUNCTION display RETURN VARCHAR2 IS BEGIN RETURN name; END;",
        or_replace=True,
    )
    force_drop = OracleDropTypeExpression(dialect, "person_t", force=True)
    validate_drop = OracleDropTypeExpression(dialect, "person_t", validate=True)
    body_drop = DropTypeBodyExpression(dialect, "person_t", if_exists=True)

    assert body.to_sql()[0].startswith("CREATE OR REPLACE TYPE BODY")
    assert force_drop.to_sql()[0] == 'DROP TYPE "PERSON_T" FORCE'
    assert validate_drop.to_sql()[0] == 'DROP TYPE "PERSON_T" VALIDATE'
    assert body_drop.to_sql()[0] == 'DROP TYPE BODY IF EXISTS "PERSON_T"'
    with pytest.raises(ValueError):
        OracleDropTypeExpression(dialect, "person_t", force=True, validate=True)


def test_if_clauses_require_reliable_19_28_ru_source() -> None:
    base = _dialect(version=(19, 0, 0), version_full=(19, 27, 0, 0, 0))
    fake = _dialect(version=(19, 28, 0))
    other_major = _dialect(version=(20, 0, 0), version_full=(20, 28, 0, 0, 0))
    future = _dialect(version=(23, 0, 0), version_full=(23, 28, 0, 0, 0))
    supported = _dialect(version=(19, 0, 0), version_full=(19, 28, 0, 0, 0))
    definition = OracleIncompleteTypeDefinition(base)

    assert base.supports_create_type_if_not_exists() is False
    assert fake.supports_create_type_if_not_exists() is False
    assert other_major.supports_create_type_if_not_exists() is False
    assert future.supports_create_type_if_not_exists() is False
    assert supported.supports_create_type_if_not_exists() is True
    with pytest.raises(UnsupportedFeatureError):
        CreateTypeExpression(base, "forward_t", definition, if_not_exists=True).to_sql()
    with pytest.raises(UnsupportedFeatureError):
        DropTypeBodyExpression(base, "forward_t", if_exists=True).to_sql()
    assert CreateTypeExpression(
        supported,
        "forward_t",
        OracleIncompleteTypeDefinition(supported),
        if_not_exists=True,
    ).to_sql()[0] == 'CREATE TYPE IF NOT EXISTS "FORWARD_T"'
    with pytest.raises(ValueError):
        CreateTypeExpression(
            supported,
            "forward_t",
            OracleIncompleteTypeDefinition(supported),
            if_not_exists=True,
            or_replace=True,
        )


def test_common_definition_does_not_accept_method_body() -> None:
    dialect = _dialect()
    with pytest.raises(TypeError):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER")],
            body="BEGIN NULL; END;",
        )
    with pytest.raises(ValueError):
        OracleTypeMethod("MEMBER FUNCTION id RETURN NUMBER IS BEGIN RETURN 1; END;")
    with pytest.raises(ValueError):
        OracleObjectTypeDefinition(
            dialect,
            attributes=[OracleTypeAttribute("id", "NUMBER", not_null=True)],
        )


def test_type_ddl_serialization_registration() -> None:
    dialect = _dialect(version_full=(19, 28, 0, 0, 0))
    expressions = (
        OracleObjectTypeDefinition(dialect, [OracleTypeAttribute("id", "NUMBER")]),
        OracleSqljTypeDefinition(
            dialect,
            [OracleTypeAttribute("id", "NUMBER")],
            external_name="Person",
            language="JAVA",
            using_clause="SQLData",
        ),
        OracleVarrayTypeDefinition(dialect, "NUMBER", 4),
        OracleNestedTableTypeDefinition(dialect, "NUMBER"),
        OracleIncompleteTypeDefinition(dialect),
        OracleAlterTypeAddAttributeAction(dialect, "id", "NUMBER"),
        OracleAlterTypeModifyAttributeAction(dialect, "id", "NUMBER"),
        OracleAlterTypeDropAttributeAction(dialect, "id"),
        OracleAlterTypeAddMethodAction(dialect, "MEMBER FUNCTION id RETURN NUMBER"),
        OracleAlterTypeDropMethodAction(dialect, name="id", kind="MEMBER FUNCTION", parameters=[]),
        OracleAlterTypeLimitAction(dialect, 4),
        OracleAlterTypeCompileAction(dialect),
        OracleAlterTypeFinalAction(dialect),
        OracleAlterTypeInstantiableAction(dialect),
        OracleCreateTypeBodyExpression(dialect, "person_t", "BEGIN NULL; END;"),
        OracleDropTypeExpression(dialect, "person_t", force=True),
        DropTypeBodyExpression(dialect, "person_t"),
    )
    for expression in expressions:
        restored = deserialize(serialize(expression), dialect)
        assert type(restored) is type(expression)
        assert serialize(restored) == serialize(expression)


def _bare_backend(backend_type: Any) -> Any:
    backend = backend_type.__new__(backend_type)
    backend._connection = object()
    backend._version = (19, 0, 0)
    backend._version_full = None
    backend._ru_version = None
    backend._dialect = OracleDialect(version=(19, 0, 0))
    backend._register_oracle_adapters = lambda: None
    backend.log = lambda *args: None
    return backend


def test_sync_introspect_refreshes_version_full_cache() -> None:
    backend = _bare_backend(OracleBackend)
    previous_dialect = backend._dialect

    def get_server_version() -> Tuple[int, int, int]:
        backend._version_full = (19, 28, 0, 0, 0)
        backend._ru_version = 28
        return (19, 0, 0)

    backend.get_server_version = get_server_version
    backend.introspect_and_adapt()
    assert backend._dialect is not previous_dialect
    assert backend._version_full == (19, 28, 0, 0, 0)
    assert backend._ru_version == 28
    assert backend.dialect.version_full == (19, 28, 0, 0, 0)
    assert backend.dialect.ru_version == 28


@pytest.mark.asyncio
async def test_async_introspect_refreshes_version_full_cache() -> None:
    backend = _bare_backend(AsyncOracleBackend)
    previous_dialect = backend._dialect

    async def get_server_version() -> Tuple[int, int, int]:
        backend._version_full = (19, 28, 0, 0, 0)
        backend._ru_version = 28
        return (19, 0, 0)

    backend.get_server_version = get_server_version
    await backend.introspect_and_adapt()
    assert backend._dialect is not previous_dialect
    assert backend._version_full == (19, 28, 0, 0, 0)
    assert backend._ru_version == 28
    assert backend.dialect.version_full == (19, 28, 0, 0, 0)
    assert backend.dialect.ru_version == 28


def test_core_drop_type_expression_is_supported() -> None:
    dialect = _dialect()
    sql, params = DropTypeExpression(dialect, "person_t").to_sql()
    assert sql == 'DROP TYPE "PERSON_T"'
    assert params == ()
