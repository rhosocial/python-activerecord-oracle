# src/rhosocial/activerecord/backend/impl/oracle/expression/ddl/type.py
"""Oracle user-defined type DDL expressions."""

from __future__ import annotations

from collections.abc import Sequence as ABCSequence
from dataclasses import dataclass
from enum import Enum
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING, cast

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.serialization import ExpressionRegistry
from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    DropTypeExpression,
    TypeAlterAction,
    TypeDefinition,
)

if TYPE_CHECKING:
    from ...dialect import OracleDialect


def _validate_name(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _validate_fragment(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    if any(token in value for token in (";", "--", "/*", "*/")):
        raise ValueError(f"{field_name} must not contain SQL statement or comment tokens")
    return value.strip()


def _validate_optional_name(value: Optional[str], field_name: str) -> Optional[str]:
    if value is not None:
        _validate_name(value, field_name)
    return value


def _normalize_flag(value: Optional[bool], alias: Optional[bool], field_name: str) -> Optional[bool]:
    if value is not None and alias is not None:
        raise ValueError(f"{field_name} and its NOT alias are mutually exclusive")
    if alias is not None:
        return not alias
    return value


def _normalize_invoker_rights(value: Any) -> Optional[bool]:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = re.sub(r"\s+", " ", value.strip().upper())
        if normalized == "INVOKER RIGHTS":
            return True
    raise ValueError("invoker_rights must be a bool or 'INVOKER RIGHTS'")


def _normalize_keyword(value: str, field_name: str) -> str:
    keyword = str(value).upper()
    if keyword not in ("AS", "IS"):
        raise ValueError(f"{field_name} must be 'AS' or 'IS'")
    return keyword


_ACCESSOR_KINDS = frozenset(("FUNCTION", "PACKAGE", "PROCEDURE", "TRIGGER", "TYPE"))
_IDENTIFIER_PART = r'(?:[A-Za-z][A-Za-z0-9_$#]*|"(?:[^"]|"")*")'
_IDENTIFIER_PART_RE = re.compile(_IDENTIFIER_PART)
_ACCESSOR_NAME_RE = re.compile(rf"^{_IDENTIFIER_PART}(?:\.{_IDENTIFIER_PART})*$")


def _validate_accessor(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("ACCESSIBLE BY items must be non-empty strings")
    text = value.strip()
    if any(token in text for token in (";", "--", "/*", "*/", "(", ")")):
        raise ValueError("ACCESSIBLE BY items must be accessor names")
    kind_match = re.match(
        rf"^(?:{'|'.join(sorted(_ACCESSOR_KINDS))})\s+(.+)$",
        text,
        re.IGNORECASE,
    )
    if kind_match is not None:
        name = kind_match.group(1).strip()
    else:
        name = text
    if _ACCESSOR_NAME_RE.fullmatch(name) is None:
        raise ValueError("ACCESSIBLE BY accessor name is invalid")
    return text


def _validate_accessible_by(value: Any) -> Optional[Tuple[str, ...]]:
    if value is None:
        return None
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple)):
        values = list(value)
    else:
        raise TypeError("accessible_by must be a string or a sequence of strings")
    if not values:
        raise ValueError("ACCESSIBLE BY requires at least one accessor")
    return tuple(_validate_accessor(item) for item in values)


_METHOD_KIND_ALIASES = {
    "MEMBER": "MEMBER FUNCTION",
    "STATIC": "STATIC FUNCTION",
    "CONSTRUCTOR": "CONSTRUCTOR FUNCTION",
    "MAP": "MAP MEMBER FUNCTION",
    "ORDER": "ORDER MEMBER FUNCTION",
    "MAP MEMBER": "MAP MEMBER FUNCTION",
    "ORDER MEMBER": "ORDER MEMBER FUNCTION",
    "MEMBER FUNCTION": "MEMBER FUNCTION",
    "MEMBER PROCEDURE": "MEMBER PROCEDURE",
    "STATIC FUNCTION": "STATIC FUNCTION",
    "STATIC PROCEDURE": "STATIC PROCEDURE",
    "CONSTRUCTOR FUNCTION": "CONSTRUCTOR FUNCTION",
    "MAP MEMBER FUNCTION": "MAP MEMBER FUNCTION",
    "ORDER MEMBER FUNCTION": "ORDER MEMBER FUNCTION",
}
_METHOD_FUNCTION_KINDS = frozenset(
    (
        "MEMBER FUNCTION",
        "STATIC FUNCTION",
        "CONSTRUCTOR FUNCTION",
        "MAP MEMBER FUNCTION",
        "ORDER MEMBER FUNCTION",
    )
)
_METHOD_PROCEDURE_KINDS = frozenset(("MEMBER PROCEDURE", "STATIC PROCEDURE"))
_SQLJ_USING_CLAUSES = {
    "SQLDATA": "SQLData",
    "CUSTOMDATUM": "CustomDatum",
    "ORADATA": "OraData",
}


def _normalize_sqlj_using(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("SQLJ USING clause is required")
    normalized = re.sub(r"\s+", " ", value.strip().upper())
    try:
        return _SQLJ_USING_CLAUSES[normalized]
    except KeyError as error:
        raise ValueError("USING must be SQLData, CustomDatum or OraData") from error


def _normalize_method_kind(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("method kind must be a non-empty string")
    normalized = re.sub(r"\s+", " ", value.strip().upper())
    try:
        return _METHOD_KIND_ALIASES[normalized]
    except KeyError as error:
        raise ValueError(f"Unsupported Oracle method kind: {value!r}") from error


def _validate_method_name(value: str) -> str:
    _validate_name(value, "method name")
    if _IDENTIFIER_PART_RE.fullmatch(value) is None:
        raise ValueError("method name must be an Oracle identifier")
    return value


def _has_top_level_comma(value: str) -> bool:
    depth = 0
    quote: Optional[str] = None
    for character in value:
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in ("'", '"'):
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth < 0:
                return True
        elif character == "," and depth == 0:
            return True
    return depth != 0 or quote is not None


def _validate_method_parameter(value: Any) -> Any:
    if isinstance(value, BaseExpression):
        return value
    if not isinstance(value, str) or not value.strip():
        raise ValueError("method parameters must be non-empty strings or expressions")
    text = _validate_fragment(value, "method parameter")
    if _has_top_level_comma(text):
        raise ValueError("method parameter contains an unsafe separator")
    return text


def _validate_method_parameters(value: Any) -> Any:
    if value is None or isinstance(value, BaseExpression):
        return value
    if isinstance(value, str):
        return _validate_method_parameter(value)
    if isinstance(value, ABCSequence):
        for parameter in value:
            _validate_method_parameter(parameter)
        return value
    raise TypeError("method parameters must be a sequence, string or expression")


def _coerce_attribute(value: Any, require_type: bool = True) -> "OracleTypeAttribute":
    if isinstance(value, OracleTypeAttribute):
        attribute = value
    elif isinstance(value, dict):
        if "name" not in value and len(value) == 1:
            name, data_type = next(iter(value.items()))
            attribute = OracleTypeAttribute(name, data_type)
        else:
            attribute = OracleTypeAttribute(**value)
    elif isinstance(value, (tuple, list)) and len(value) in (2, 3):
        attribute = OracleTypeAttribute(*value)
    elif isinstance(value, str):
        text = _validate_fragment(value, "attribute")
        pieces = text.split(None, 1)
        if len(pieces) != 2:
            if require_type:
                raise ValueError("attribute declarations must contain a name and a data type")
            attribute = OracleTypeAttribute(text, None)
        else:
            attribute = OracleTypeAttribute(pieces[0], pieces[1])
    else:
        raise TypeError(
            "attribute must be an OracleTypeAttribute, mapping, pair or declaration string, "
            f"got {type(value).__name__}"
        )
    if require_type and attribute.data_type is None:
        raise ValueError("attribute data_type is required")
    return attribute


def _coerce_attributes(value: Any, require_type: bool = True) -> List["OracleTypeAttribute"]:
    if value is None:
        return []
    if isinstance(value, (OracleTypeAttribute, dict)):
        return [_coerce_attribute(value, require_type)]
    if isinstance(value, str):
        return [_coerce_attribute(value, require_type)]
    if isinstance(value, tuple) and len(value) in (2, 3):
        first = value[0]
        if len(value) == 2 and isinstance(first, str) and " " in first:
            return [_coerce_attribute(item, require_type) for item in value]
        return [_coerce_attribute(value, require_type)]
    if isinstance(value, (list, tuple, ABCSequence)):
        return [_coerce_attribute(item, require_type) for item in value]
    raise TypeError(
        "attributes must be a sequence of attribute declarations, "
        f"got {type(value).__name__}"
    )


def _coerce_method(value: Any) -> "OracleTypeMethod":
    if isinstance(value, OracleTypeMethod):
        return value
    if isinstance(value, dict):
        return OracleTypeMethod(**value)
    if isinstance(value, str):
        return OracleTypeMethod(_validate_fragment(value, "method"))
    if isinstance(value, (tuple, list)) and len(value) in (1, 2):
        return OracleTypeMethod(*value)
    if isinstance(value, (tuple, list)) and len(value) in (4, 5):
        return OracleTypeMethod(
            kind=value[0],
            name=value[1],
            parameters=value[2],
            return_type=value[3],
            external_name=value[4] if len(value) == 5 else None,
        )
    raise TypeError(
        "method must be an OracleTypeMethod, mapping or declaration string, "
        f"got {type(value).__name__}"
    )


def _coerce_methods(value: Any) -> List["OracleTypeMethod"]:
    if value is None:
        return []
    if isinstance(value, (OracleTypeMethod, dict, str)):
        return [_coerce_method(value)]
    if isinstance(value, ABCSequence):
        return [_coerce_method(item) for item in value]
    raise TypeError(
        "methods must be a sequence of method declarations, "
        f"got {type(value).__name__}"
    )


def _validate_object_external_names(
    attributes: List["OracleTypeAttribute"],
    methods: List["OracleTypeMethod"],
) -> None:
    if any(attribute.external_name is not None for attribute in attributes):
        raise ValueError("EXTERNAL NAME is only valid for SQLJ object types")
    for method in methods:
        if method.external_name is not None:
            raise ValueError("EXTERNAL NAME is only valid for SQLJ object types")
        if method.declaration and re.search(
            r"\bEXTERNAL\s+(?:NAME|VARIABLE)\b",
            method.declaration,
            re.IGNORECASE,
        ):
            raise ValueError("EXTERNAL NAME is only valid for SQLJ object types")


def _coerce_dependent_handling(value: Any) -> Optional["OracleAlterTypeDependentHandling"]:
    if value is None or isinstance(value, OracleAlterTypeDependentHandling):
        return value
    if not isinstance(value, str) or not value.strip():
        raise TypeError("dependent_handling must be an OracleAlterTypeDependentHandling or string")
    normalized = re.sub(r"\s+", " ", value.strip().upper())
    for member in OracleAlterTypeDependentHandling:
        if member.value == normalized:
            return member
    aliases = {
        "CASCADE INCLUDING DATA": OracleAlterTypeDependentHandling.CASCADE_INCLUDING_TABLE_DATA,
        "CASCADE NOT INCLUDING DATA": (
            OracleAlterTypeDependentHandling.CASCADE_NOT_INCLUDING_TABLE_DATA
        ),
        "CONVERT TO SUBSTITUTABLE": (
            OracleAlterTypeDependentHandling.CASCADE_CONVERT_TO_SUBSTITUTABLE
        ),
    }
    if normalized in aliases:
        return aliases[normalized]
    raise ValueError(f"Unsupported ALTER TYPE dependent handling: {value!r}")


@dataclass(frozen=True)
class OracleTypeAttribute:
    """One attribute declaration in an Oracle object type."""

    name: str
    data_type: Any = None
    external_name: Optional[str] = None
    not_null: bool = False

    def __post_init__(self) -> None:
        _validate_name(self.name, "attribute name")
        if self.external_name is not None:
            _validate_fragment(self.external_name, "external_name")
        if not isinstance(self.not_null, bool):
            raise TypeError("attribute not_null must be a bool")


@dataclass(frozen=True)
class OracleTypeMethod:
    """One method declaration in an Oracle object type."""

    declaration: Optional[str] = None
    external_name: Optional[str] = None
    kind: Optional[str] = None
    name: Optional[str] = None
    parameters: Optional[Sequence[Any]] = None
    return_type: Any = None
    require_return_type: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.require_return_type, bool):
            raise TypeError("require_return_type must be a bool")
        if self.declaration is not None:
            if any(
                value is not None
                for value in (self.kind, self.name, self.parameters, self.return_type)
            ):
                raise ValueError("raw method declarations cannot include structured fields")
            _validate_fragment(self.declaration, "method declaration")
            if re.search(r"\b(?:IS|BEGIN|DECLARE|END)\b", self.declaration, re.IGNORECASE):
                raise ValueError("method implementations belong in a separate TYPE BODY")
        else:
            normalized_kind = _normalize_method_kind(self.kind)
            if normalized_kind is None:
                raise ValueError("structured methods require a method kind")
            object.__setattr__(self, "kind", normalized_kind)
            if self.require_return_type and normalized_kind in _METHOD_FUNCTION_KINDS:
                if self.return_type is None:
                    raise ValueError(f"{normalized_kind} requires return_type")
            if normalized_kind in _METHOD_PROCEDURE_KINDS and self.return_type is not None:
                raise ValueError(f"{normalized_kind} must not define return_type")
            if normalized_kind == "CONSTRUCTOR FUNCTION":
                return_text = str(self.return_type or "").strip()
                if self.require_return_type and re.sub(
                    r"\s+", " ", return_text.upper()
                ) != "SELF AS RESULT":
                    raise ValueError("CONSTRUCTOR FUNCTION requires RETURN SELF AS RESULT")
        if self.external_name is not None:
            _validate_fragment(self.external_name, "external_name")
        if self.name is not None:
            _validate_method_name(self.name)
        if self.declaration is None and not self.name:
            raise ValueError("method requires declaration or name")
        if self.declaration is None:
            object.__setattr__(self, "parameters", _validate_method_parameters(self.parameters))
        if self.return_type is not None and not isinstance(self.return_type, (str, BaseExpression)):
            raise TypeError("return_type must be a string or expression")
        if isinstance(self.return_type, str):
            _validate_fragment(self.return_type, "return_type")


class OracleTypeDefinitionBase(TypeDefinition):
    """Shared state for Oracle type definitions."""

    def _initialize_options(
        self,
        under: Optional[str],
        final: Optional[bool],
        instantiable: Optional[bool],
        persistable: Optional[bool],
        not_final: Optional[bool],
        not_instantiable: Optional[bool],
        not_persistable: Optional[bool],
        force: bool,
        authid: Optional[str],
        keyword: str,
        editionable: Optional[bool],
        sharing: Optional[str],
        oid: Optional[str],
        default_collation: Optional[str],
        accessible_by: Any,
        invoker_rights: Any,
    ) -> None:
        self.under = _validate_optional_name(under, "under")
        self.final = _normalize_flag(final, not_final, "final")
        self.not_final = not_final
        self.instantiable = _normalize_flag(
            instantiable,
            not_instantiable,
            "instantiable",
        )
        self.not_instantiable = not_instantiable
        self.persistable = _normalize_flag(
            persistable,
            not_persistable,
            "persistable",
        )
        self.not_persistable = not_persistable
        if self.under is not None and self.persistable is not None:
            raise ValueError("subtypes inherit PERSISTABLE and cannot specify it")
        if not isinstance(force, bool):
            raise TypeError("force must be a bool")
        self.force = force
        self.authid = authid.upper() if isinstance(authid, str) else authid
        if self.authid is not None and self.authid not in ("CURRENT_USER", "DEFINER"):
            raise ValueError("authid must be CURRENT_USER or DEFINER")
        self.keyword = _normalize_keyword(keyword, "keyword")
        if editionable is not None and not isinstance(editionable, bool):
            raise TypeError("editionable must be a bool or None")
        self.editionable = editionable
        self.sharing = _validate_fragment(sharing, "sharing") if sharing is not None else None
        self.oid = _validate_fragment(oid, "oid") if oid is not None else None
        self.default_collation = (
            _validate_fragment(default_collation, "default_collation")
            if default_collation is not None
            else None
        )
        self.accessible_by = _validate_accessible_by(accessible_by)
        self.invoker_rights = _normalize_invoker_rights(invoker_rights)
        if self.authid is not None and self.invoker_rights:
            raise ValueError("authid and invoker_rights are mutually exclusive")

    def get_params(self) -> Dict[str, Any]:
        params = cast(Dict[str, Any], super().get_params())
        for alias in ("not_final", "not_instantiable", "not_persistable"):
            params.pop(alias, None)
        params["final"] = self.final
        params["instantiable"] = self.instantiable
        params["persistable"] = self.persistable
        return params


class OracleObjectTypeDefinition(OracleTypeDefinitionBase):
    """Oracle object type definition."""

    definition_kind = "oracle.object"

    def __init__(
        self,
        dialect: "OracleDialect",
        attributes: Any = None,
        methods: Any = None,
        *,
        external_name: Optional[str] = None,
        under: Optional[str] = None,
        final: Optional[bool] = None,
        instantiable: Optional[bool] = None,
        persistable: Optional[bool] = None,
        not_final: Optional[bool] = None,
        not_instantiable: Optional[bool] = None,
        not_persistable: Optional[bool] = None,
        force: bool = False,
        authid: Optional[str] = None,
        keyword: str = "AS",
        editionable: Optional[bool] = None,
        sharing: Optional[str] = None,
        oid: Optional[str] = None,
        default_collation: Optional[str] = None,
        accessible_by: Any = None,
        invoker_rights: Any = None,
    ) -> None:
        super().__init__(dialect)
        if external_name is not None:
            raise ValueError("EXTERNAL NAME is only valid for SQLJ object types")
        self.external_name = None
        self.attributes = _coerce_attributes(attributes)
        if not self.attributes:
            raise ValueError("Oracle object type definitions require at least one attribute")
        if any(attribute.not_null for attribute in self.attributes):
            raise ValueError("Oracle object type attributes do not support NOT NULL")
        self.methods = _coerce_methods(methods)
        _validate_object_external_names(self.attributes, self.methods)
        self._initialize_options(
            under,
            final,
            instantiable,
            persistable,
            not_final,
            not_instantiable,
            not_persistable,
            force,
            authid,
            keyword,
            editionable,
            sharing,
            oid,
            default_collation,
            accessible_by,
            invoker_rights,
        )

    def get_params(self) -> Dict[str, Any]:
        params = cast(Dict[str, Any], super().get_params())
        params.pop("external_name", None)
        return params


class OracleSqljTypeDefinition(OracleTypeDefinitionBase):
    """Oracle SQLJ object type definition."""

    definition_kind = "oracle.sqlj"

    def __init__(
        self,
        dialect: "OracleDialect",
        attributes: Any = None,
        methods: Any = None,
        *,
        external_name: Optional[str] = None,
        language: Optional[str] = None,
        using_clause: Optional[str] = None,
        java_class: Optional[str] = None,
        using_type: Optional[str] = None,
        under: Optional[str] = None,
        final: Optional[bool] = None,
        instantiable: Optional[bool] = None,
        persistable: Optional[bool] = None,
        not_final: Optional[bool] = None,
        not_instantiable: Optional[bool] = None,
        not_persistable: Optional[bool] = None,
        force: bool = False,
        authid: Optional[str] = None,
        keyword: str = "AS",
        editionable: Optional[bool] = None,
        sharing: Optional[str] = None,
        oid: Optional[str] = None,
        default_collation: Optional[str] = None,
        accessible_by: Any = None,
        invoker_rights: Any = None,
    ) -> None:
        super().__init__(dialect)
        self.attributes = _coerce_attributes(attributes)
        if not self.attributes:
            raise ValueError("Oracle object type definitions require at least one attribute")
        if any(attribute.not_null for attribute in self.attributes):
            raise ValueError("Oracle object type attributes do not support NOT NULL")
        self.methods = _coerce_methods(methods)
        if java_class is not None:
            if external_name is not None and external_name != java_class:
                raise ValueError("external_name and java_class are mutually exclusive")
            external_name = java_class
        if using_type is not None:
            if not isinstance(using_type, str) or not using_type.strip():
                raise ValueError("using_type must be a non-empty string")
            if (
                using_clause is not None
                and (
                    not isinstance(using_clause, str)
                    or using_clause.strip().upper() not in ("SQLDATA", using_type.upper())
                )
            ):
                raise ValueError("using_clause and using_type are mutually exclusive")
            using_clause = using_type
        if external_name is None:
            raise ValueError("SQLJ object types require EXTERNAL NAME")
        self.external_name = _validate_fragment(external_name, "external_name")
        self.java_class = java_class
        if not isinstance(language, str) or language.strip().upper() != "JAVA":
            raise ValueError("SQLJ object types only support LANGUAGE JAVA")
        self.language = "JAVA"
        self.using_clause = _normalize_sqlj_using(using_clause)
        self.using_type = using_type
        self._initialize_options(
            under,
            final,
            instantiable,
            persistable,
            not_final,
            not_instantiable,
            not_persistable,
            force,
            authid,
            keyword,
            editionable,
            sharing,
            oid,
            default_collation,
            accessible_by,
            invoker_rights,
        )

    def get_params(self) -> Dict[str, Any]:
        params = cast(Dict[str, Any], super().get_params())
        params.pop("java_class", None)
        params.pop("using_type", None)
        params["external_name"] = self.external_name
        params["using_clause"] = self.using_clause
        return params


class OracleVarrayTypeDefinition(TypeDefinition):
    """Oracle named VARRAY type definition."""

    definition_kind = "oracle.varray"

    def __init__(
        self,
        dialect: "OracleDialect",
        element_type: Any = None,
        size_limit: Any = None,
        *,
        force: bool = False,
        not_null: bool = False,
        persistable: Optional[bool] = None,
        not_persistable: Optional[bool] = None,
        max_size: Any = None,
        max_length: Any = None,
        size: Any = None,
    ) -> None:
        super().__init__(dialect)
        if not isinstance(force, bool):
            raise TypeError("force must be a bool")
        if element_type is None:
            raise ValueError("VARRAY element_type is required")
        if isinstance(element_type, int) and not isinstance(element_type, bool):
            if size_limit is not None and not isinstance(size_limit, int):
                element_type, size_limit = size_limit, element_type
        if size_limit is None:
            size_limit = (
                max_size
                if max_size is not None
                else max_length
                if max_length is not None
                else size
            )
        elif max_size is not None or max_length is not None or size is not None:
            raise ValueError(
                "size_limit, max_size, max_length and size are mutually exclusive aliases"
            )
        if not isinstance(size_limit, int) or isinstance(size_limit, bool):
            raise TypeError("size_limit must be an int")
        if size_limit <= 0:
            raise ValueError("size_limit must be positive")
        if not isinstance(not_null, bool):
            raise TypeError("not_null must be a bool")
        self.element_type = element_type
        self.force = force
        self.size_limit = size_limit
        self.max_size = max_size
        self.max_length = max_length
        self.size = size
        self.not_null = not_null
        self.persistable = _normalize_flag(
            persistable,
            not_persistable,
            "persistable",
        )
        self.not_persistable = not_persistable

    def get_params(self) -> Dict[str, Any]:
        params = cast(Dict[str, Any], super().get_params())
        for key in ("max_size", "max_length", "size", "not_persistable"):
            params.pop(key, None)
        params["size_limit"] = self.size_limit
        params["persistable"] = self.persistable
        return params


class OracleNestedTableTypeDefinition(TypeDefinition):
    """Oracle named nested table type definition."""

    definition_kind = "oracle.nested_table"

    def __init__(
        self,
        dialect: "OracleDialect",
        element_type: Any = None,
        *,
        force: bool = False,
        not_null: bool = False,
        persistable: Optional[bool] = None,
        not_persistable: Optional[bool] = None,
    ) -> None:
        super().__init__(dialect)
        if not isinstance(force, bool):
            raise TypeError("force must be a bool")
        if element_type is None:
            raise ValueError("nested table element_type is required")
        if not isinstance(not_null, bool):
            raise TypeError("not_null must be a bool")
        self.element_type = element_type
        self.force = force
        self.not_null = not_null
        self.persistable = _normalize_flag(
            persistable,
            not_persistable,
            "persistable",
        )
        self.not_persistable = not_persistable

    def get_params(self) -> Dict[str, Any]:
        params = cast(Dict[str, Any], super().get_params())
        params.pop("not_persistable", None)
        params["persistable"] = self.persistable
        return params


class OracleIncompleteTypeDefinition(TypeDefinition):
    """Oracle forward/incomplete type definition."""

    definition_kind = "oracle.incomplete"

    def __init__(self, dialect: "OracleDialect") -> None:
        super().__init__(dialect)


class OracleAlterTypeDependentHandling(Enum):
    """Dependent handling clauses accepted after ALTER TYPE actions."""

    INVALIDATE = "INVALIDATE"
    CASCADE = "CASCADE"
    CASCADE_INCLUDING_TABLE_DATA = "CASCADE INCLUDING TABLE DATA"
    CASCADE_NOT_INCLUDING_TABLE_DATA = "CASCADE NOT INCLUDING TABLE DATA"
    CASCADE_CONVERT_TO_SUBSTITUTABLE = "CASCADE CONVERT TO SUBSTITUTABLE"


class OracleTypeAlterAction(TypeAlterAction):
    """Base for Oracle ALTER TYPE actions with dependent handling."""

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect)
        self.dependent_handling = _coerce_dependent_handling(dependent_handling)
        if not isinstance(force, bool):
            raise TypeError("force must be a bool")
        self.force = force

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params["dependent_handling"] = self.dependent_handling
        params["force"] = self.force
        return cast(Dict[str, Any], params)


class OracleAlterTypeAddAttributeAction(OracleTypeAlterAction):
    """Add one or more object attributes."""

    action_kind = "oracle.attribute.add"

    def __init__(
        self,
        dialect: "OracleDialect",
        attribute: Any = None,
        data_type: Any = None,
        *,
        attribute_name: Optional[str] = None,
        attributes: Any = None,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect, dependent_handling=dependent_handling, force=force)
        supplied = [
            item
            for item in (attribute, attribute_name, attributes)
            if item is not None
        ]
        if len(supplied) > 1:
            raise ValueError("attribute, attribute_name and attributes are mutually exclusive")
        value = attribute if attribute is not None else attribute_name
        if value is None:
            value = attributes
        if data_type is not None:
            if isinstance(value, OracleTypeAttribute):
                raise ValueError("data_type cannot be combined with OracleTypeAttribute")
            value = OracleTypeAttribute(str(value), data_type)
        self.attributes = _coerce_attributes(value)
        if not self.attributes:
            raise ValueError("ADD ATTRIBUTE requires at least one attribute")
        self.attribute = self.attributes[0] if len(self.attributes) == 1 else self.attributes
        self.attribute_name = attribute_name
        self.data_type = data_type
        self.attributes_argument = attributes

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.pop("attribute_name", None)
        params.pop("attributes", None)
        params.pop("data_type", None)
        params.pop("attributes_argument", None)
        params["attribute"] = self.attribute
        return params


class OracleAlterTypeModifyAttributeAction(OracleAlterTypeAddAttributeAction):
    """Modify one or more scalar object attributes."""

    action_kind = "oracle.attribute.modify"


class OracleAlterTypeDropAttributeAction(OracleTypeAlterAction):
    """Drop one or more object attributes."""

    action_kind = "oracle.attribute.drop"

    def __init__(
        self,
        dialect: "OracleDialect",
        attribute: Any = None,
        *,
        attribute_name: Optional[str] = None,
        attributes: Any = None,
        names: Any = None,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect, dependent_handling=dependent_handling, force=force)
        supplied = [
            item
            for item in (attribute, attribute_name, attributes, names)
            if item is not None
        ]
        if len(supplied) > 1:
            raise ValueError(
                "attribute, attribute_name, attributes and names are mutually exclusive"
            )
        if not supplied:
            raise ValueError("DROP ATTRIBUTE requires at least one attribute name")
        value = attribute if attribute is not None else attribute_name
        if value is None:
            value = attributes if attributes is not None else names
        if isinstance(value, str):
            values = [value]
        elif isinstance(value, (list, tuple, ABCSequence)):
            values = list(value)
        else:
            values = [value]
        self.attributes = [_coerce_attribute(item, require_type=False) for item in values]
        self.attribute = self.attributes[0] if len(self.attributes) == 1 else self.attributes
        self.attribute_name = attribute_name
        self.names = names

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.pop("attribute_name", None)
        params.pop("attributes", None)
        params.pop("names", None)
        params["attribute"] = self.attribute
        return params


class OracleAlterTypeAddMethodAction(OracleTypeAlterAction):
    """Add one or more method declarations."""

    action_kind = "oracle.method.add"

    def __init__(
        self,
        dialect: "OracleDialect",
        method: Any = None,
        *,
        declaration: Any = None,
        external_name: Optional[str] = None,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect, dependent_handling=dependent_handling, force=force)
        if method is not None and declaration is not None:
            raise ValueError("method and declaration are mutually exclusive")
        value = method if method is not None else declaration
        if isinstance(value, OracleTypeMethod) and external_name is not None:
            value = OracleTypeMethod(
                declaration=value.declaration,
                external_name=external_name,
                kind=value.kind,
                name=value.name,
                parameters=value.parameters,
                return_type=value.return_type,
            )
        elif external_name is not None:
            value = OracleTypeMethod(declaration=str(value), external_name=external_name)
        self.method = _coerce_method(value)
        self.declaration = declaration
        self.external_name = external_name

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.pop("declaration", None)
        params.pop("external_name", None)
        params["method"] = self.method
        return params


class OracleAlterTypeDropMethodAction(OracleTypeAlterAction):
    """Drop one or more method specifications."""

    action_kind = "oracle.method.drop"

    def __init__(
        self,
        dialect: "OracleDialect",
        method: Any = None,
        *,
        name: Optional[str] = None,
        method_name: Optional[str] = None,
        kind: Optional[str] = None,
        parameters: Any = None,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect, dependent_handling=dependent_handling, force=force)
        supplied = [item for item in (method, name, method_name) if item is not None]
        if len(supplied) > 1:
            raise ValueError("method, name and method_name are mutually exclusive")
        if method is None and (name is not None or method_name is not None):
            method = OracleTypeMethod(
                kind=kind or "MEMBER FUNCTION",
                name=name or method_name,
                parameters=parameters,
                require_return_type=False,
            )
        self.method = _coerce_method(method)
        self.name = name
        self.method_name = method_name or name
        self.kind = kind
        self.parameters = parameters

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        for alias in ("name", "method_name", "kind", "parameters"):
            params.pop(alias, None)
        params["method"] = self.method
        return cast(Dict[str, Any], params)


class OracleAlterTypeLimitAction(OracleTypeAlterAction):
    """Increase a VARRAY type's element limit."""

    action_kind = "oracle.limit"

    def __init__(
        self,
        dialect: "OracleDialect",
        limit: Any = None,
        *,
        size_limit: Any = None,
        new_limit: Any = None,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect, dependent_handling=dependent_handling, force=force)
        supplied = [item for item in (limit, size_limit, new_limit) if item is not None]
        if len(supplied) > 1:
            raise ValueError("limit, size_limit and new_limit are mutually exclusive")
        value = limit if limit is not None else size_limit
        if value is None:
            value = new_limit
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError("limit must be a positive integer")
        self.limit = value
        self.size_limit = size_limit
        self.new_limit = new_limit

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.pop("size_limit", None)
        params.pop("new_limit", None)
        return params


class OracleAlterTypeElementTypeAction(OracleTypeAlterAction):
    """Modify a collection element data type."""

    action_kind = "oracle.element_type"

    def __init__(
        self,
        dialect: "OracleDialect",
        element_type: Any = None,
        *,
        new_element_type: Any = None,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect, dependent_handling=dependent_handling, force=force)
        if element_type is not None and new_element_type is not None:
            raise ValueError("element_type and new_element_type are mutually exclusive")
        self.element_type = element_type if element_type is not None else new_element_type
        self.new_element_type = new_element_type
        if self.element_type is None:
            raise ValueError("MODIFY ELEMENT TYPE requires a data type")

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.pop("new_element_type", None)
        return params


class OracleAlterTypeCompileAction(OracleTypeAlterAction):
    """Compile an Oracle type specification or body."""

    action_kind = "oracle.compile"

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        debug: bool = False,
        target: Optional[str] = None,
        compile_target: Optional[str] = None,
        reuse_settings: bool = False,
        compiler_parameters: Any = None,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect, dependent_handling=dependent_handling, force=force)
        if target is not None and compile_target is not None and target != compile_target:
            raise ValueError("target and compile_target are mutually exclusive")
        if target is None:
            target = compile_target
        if target is not None:
            target = str(target).upper()
            if target not in ("SPECIFICATION", "BODY"):
                raise ValueError("target must be SPECIFICATION or BODY")
        if not isinstance(debug, bool) or not isinstance(reuse_settings, bool):
            raise TypeError("debug and reuse_settings must be bools")
        if compiler_parameters is None:
            compiler_parameters = []
        elif isinstance(compiler_parameters, str):
            compiler_parameters = [compiler_parameters]
        else:
            compiler_parameters = list(compiler_parameters)
        self.debug = debug
        self.target = target
        self.compile_target = compile_target
        self.reuse_settings = reuse_settings
        self.compiler_parameters = compiler_parameters

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.pop("compile_target", None)
        return params


class OracleAlterTypeFinalAction(OracleTypeAlterAction):
    """Change the FINAL property of an object type."""

    action_kind = "oracle.final"

    def __init__(
        self,
        dialect: "OracleDialect",
        final: Optional[bool] = None,
        *,
        is_final: Optional[bool] = None,
        not_final: Optional[bool] = None,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect, dependent_handling=dependent_handling, force=force)
        if final is not None and is_final is not None:
            raise ValueError("final and is_final are mutually exclusive")
        if final is None:
            final = is_final
        self.final = _normalize_flag(final, not_final, "final")
        if self.final is None:
            self.final = True
        self.is_final = is_final
        self.not_final = not_final

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.pop("is_final", None)
        params.pop("not_final", None)
        return params


class OracleAlterTypeInstantiableAction(OracleAlterTypeFinalAction):
    """Change the INSTANTIABLE property of an object type."""

    action_kind = "oracle.instantiable"

    def __init__(
        self,
        dialect: "OracleDialect",
        instantiable: Optional[bool] = None,
        *,
        is_instantiable: Optional[bool] = None,
        not_instantiable: Optional[bool] = None,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        OracleTypeAlterAction.__init__(
            self,
            dialect,
            dependent_handling=dependent_handling,
            force=force,
        )
        if instantiable is not None and is_instantiable is not None:
            raise ValueError("instantiable and is_instantiable are mutually exclusive")
        if instantiable is None:
            instantiable = is_instantiable
        self.instantiable = _normalize_flag(
            instantiable,
            not_instantiable,
            "instantiable",
        )
        self.is_instantiable = is_instantiable
        if self.instantiable is None:
            self.instantiable = True
        self.not_instantiable = not_instantiable
        self.final = None
        self.not_final = None

    def get_params(self) -> Dict[str, Any]:
        params = OracleTypeAlterAction.get_params(self)
        params.pop("final", None)
        params.pop("not_final", None)
        params.pop("is_instantiable", None)
        params.pop("not_instantiable", None)
        return params


class OracleAlterTypeResetAction(OracleTypeAlterAction):
    """Reset an evolved type to version one."""

    action_kind = "oracle.reset"

    def __init__(
        self,
        dialect: "OracleDialect",
        *,
        dependent_handling: Any = None,
        force: bool = False,
    ) -> None:
        super().__init__(dialect, dependent_handling=dependent_handling, force=force)


class OracleCreateTypeBodyExpression(BaseExpression):
    """Oracle CREATE TYPE BODY expression."""

    def __init__(
        self,
        dialect: "OracleDialect",
        type_name: str,
        body: str,
        *,
        schema_name: Optional[str] = None,
        schema: Optional[str] = None,
        if_not_exists: bool = False,
        or_replace: bool = False,
        keyword: str = "AS",
        editionable: Optional[bool] = None,
    ) -> None:
        super().__init__(dialect)
        _validate_name(type_name, "type_name")
        if schema_name is not None and schema is not None and schema_name != schema:
            raise ValueError("schema_name and schema are mutually exclusive")
        schema_name = schema_name if schema_name is not None else schema
        _validate_optional_name(schema_name, "schema_name")
        if not isinstance(body, str) or not body.strip():
            raise ValueError("body must be a non-empty string")
        if if_not_exists and or_replace:
            raise ValueError("CREATE TYPE BODY IF NOT EXISTS and OR REPLACE are mutually exclusive")
        self.type_name = type_name
        self.body = body
        self.schema_name = schema_name
        self.schema = schema
        self.if_not_exists = bool(if_not_exists)
        self.or_replace = bool(or_replace)
        self.keyword = _normalize_keyword(keyword, "keyword")
        if editionable is not None and not isinstance(editionable, bool):
            raise TypeError("editionable must be a bool or None")
        self.editionable = editionable

    def get_params(self) -> Dict[str, Any]:
        params = cast(Dict[str, Any], super().get_params())
        params.pop("schema", None)
        return params

    @property
    def format_method(self) -> str:
        return "format_create_type_body_statement"


class OracleDropTypeExpression(DropTypeExpression):
    """Oracle DROP TYPE expression with FORCE or VALIDATE."""

    def __init__(
        self,
        dialect: "OracleDialect",
        type_name: str,
        *,
        schema_name: Optional[str] = None,
        schema: Optional[str] = None,
        if_exists: bool = False,
        force: bool = False,
        validate: bool = False,
    ) -> None:
        if schema_name is not None and schema is not None and schema_name != schema:
            raise ValueError("schema_name and schema are mutually exclusive")
        schema_name = schema_name if schema_name is not None else schema
        super().__init__(
            dialect,
            type_name,
            schema_name=schema_name,
            if_exists=if_exists,
        )
        if force and validate:
            raise ValueError("DROP TYPE FORCE and VALIDATE are mutually exclusive")
        if not isinstance(force, bool) or not isinstance(validate, bool):
            raise TypeError("force and validate must be bools")
        self.schema = schema
        self.force = force
        self.validate = validate

    def get_params(self) -> Dict[str, Any]:
        params = cast(Dict[str, Any], super().get_params())
        params.pop("schema", None)
        return params


class DropTypeBodyExpression(BaseExpression):
    """Oracle DROP TYPE BODY expression."""

    def __init__(
        self,
        dialect: "OracleDialect",
        type_name: str,
        *,
        schema_name: Optional[str] = None,
        schema: Optional[str] = None,
        if_exists: bool = False,
    ) -> None:
        super().__init__(dialect)
        _validate_name(type_name, "type_name")
        if schema_name is not None and schema is not None and schema_name != schema:
            raise ValueError("schema_name and schema are mutually exclusive")
        schema_name = schema_name if schema_name is not None else schema
        _validate_optional_name(schema_name, "schema_name")
        self.type_name = type_name
        self.schema_name = schema_name
        self.schema = schema
        self.if_exists = bool(if_exists)

    def get_params(self) -> Dict[str, Any]:
        params = cast(Dict[str, Any], super().get_params())
        params.pop("schema", None)
        return params

    @property
    def format_method(self) -> str:
        return "format_drop_type_body_statement"


class OracleDropTypeBodyExpression(DropTypeBodyExpression):
    """Oracle-prefixed alias for DROP TYPE BODY."""


OracleTypeAddAttributeAction = OracleAlterTypeAddAttributeAction
OracleTypeModifyAttributeAction = OracleAlterTypeModifyAttributeAction
OracleTypeDropAttributeAction = OracleAlterTypeDropAttributeAction
OracleTypeAddMethodAction = OracleAlterTypeAddMethodAction
OracleTypeDropMethodAction = OracleAlterTypeDropMethodAction
OracleTypeLimitAction = OracleAlterTypeLimitAction
OracleTypeElementTypeAction = OracleAlterTypeElementTypeAction
OracleTypeCompileAction = OracleAlterTypeCompileAction
OracleTypeFinalAction = OracleAlterTypeFinalAction
OracleTypeInstantiableAction = OracleAlterTypeInstantiableAction
OracleTypeResetAction = OracleAlterTypeResetAction
OracleAddTypeAttributeAction = OracleAlterTypeAddAttributeAction
OracleModifyTypeAttributeAction = OracleAlterTypeModifyAttributeAction
OracleDropTypeAttributeAction = OracleAlterTypeDropAttributeAction
OracleAddTypeMethodAction = OracleAlterTypeAddMethodAction
OracleDropTypeMethodAction = OracleAlterTypeDropMethodAction
OracleModifyTypeLimitAction = OracleAlterTypeLimitAction
OracleTypeBodyCreateExpression = OracleCreateTypeBodyExpression
OracleTypeBodyDropExpression = DropTypeBodyExpression
OracleTypeDependentHandling = OracleAlterTypeDependentHandling
OracleAlterTypeAttributeAction = OracleAlterTypeAddAttributeAction
OracleAlterTypeMethodAction = OracleAlterTypeAddMethodAction
OracleAlterTypeModifyLimitAction = OracleAlterTypeLimitAction
OracleCompileTypeAction = OracleAlterTypeCompileAction
OracleSetTypeFinalAction = OracleAlterTypeFinalAction
OracleSetTypeInstantiableAction = OracleAlterTypeInstantiableAction
OracleDropTypeWithOptionsExpression = OracleDropTypeExpression


_EXPRESSION_TYPES = (
    OracleObjectTypeDefinition,
    OracleSqljTypeDefinition,
    OracleVarrayTypeDefinition,
    OracleNestedTableTypeDefinition,
    OracleIncompleteTypeDefinition,
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
    OracleCreateTypeBodyExpression,
    OracleDropTypeExpression,
    DropTypeBodyExpression,
    OracleDropTypeBodyExpression,
)
for _expression_type in _EXPRESSION_TYPES:
    ExpressionRegistry.register(_expression_type)


__all__ = [
    "OracleTypeAttribute",
    "OracleTypeMethod",
    "OracleObjectTypeDefinition",
    "OracleSqljTypeDefinition",
    "OracleVarrayTypeDefinition",
    "OracleNestedTableTypeDefinition",
    "OracleIncompleteTypeDefinition",
    "OracleAlterTypeDependentHandling",
    "OracleTypeAlterAction",
    "OracleAlterTypeAddAttributeAction",
    "OracleAlterTypeModifyAttributeAction",
    "OracleAlterTypeDropAttributeAction",
    "OracleAlterTypeAddMethodAction",
    "OracleAlterTypeDropMethodAction",
    "OracleAlterTypeLimitAction",
    "OracleAlterTypeElementTypeAction",
    "OracleAlterTypeCompileAction",
    "OracleAlterTypeFinalAction",
    "OracleAlterTypeInstantiableAction",
    "OracleAlterTypeResetAction",
    "OracleCreateTypeBodyExpression",
    "OracleDropTypeExpression",
    "DropTypeBodyExpression",
    "OracleDropTypeBodyExpression",
    "OracleTypeAddAttributeAction",
    "OracleTypeModifyAttributeAction",
    "OracleTypeDropAttributeAction",
    "OracleTypeAddMethodAction",
    "OracleTypeDropMethodAction",
    "OracleTypeLimitAction",
    "OracleTypeElementTypeAction",
    "OracleTypeCompileAction",
    "OracleTypeFinalAction",
    "OracleTypeInstantiableAction",
    "OracleTypeResetAction",
    "OracleAddTypeAttributeAction",
    "OracleModifyTypeAttributeAction",
    "OracleDropTypeAttributeAction",
    "OracleAddTypeMethodAction",
    "OracleDropTypeMethodAction",
    "OracleModifyTypeLimitAction",
    "OracleTypeBodyCreateExpression",
    "OracleTypeBodyDropExpression",
    "OracleTypeDependentHandling",
    "OracleAlterTypeAttributeAction",
    "OracleAlterTypeMethodAction",
    "OracleAlterTypeModifyLimitAction",
    "OracleCompileTypeAction",
    "OracleSetTypeFinalAction",
    "OracleSetTypeInstantiableAction",
    "OracleDropTypeWithOptionsExpression",
]
