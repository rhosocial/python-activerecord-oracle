# src/rhosocial/activerecord/backend/impl/oracle/expression/ddl/type.py
"""Oracle user-defined type DDL expressions."""

from __future__ import annotations

from collections.abc import Sequence as ABCSequence
from dataclasses import dataclass
from enum import Enum
import re
from typing import Any, List, Optional, Sequence, Tuple, TYPE_CHECKING, cast

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import Type
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


class _ResolvedFlagView:
    """Read-only merged view over Oracle's alias spellings of one tri-state flag.

    Oracle spells a single tri-state flag several ways: the positive parameter
    (``final=True``), an ``is_``-prefixed duplicate (``is_final=True``) and a
    negating parameter (``not_final=True``). They are mutually exclusive, so
    ``__init__`` stores each one *verbatim* under the name the caller actually
    used and leaves the others at ``None`` ("not supplied"). The merged value is
    computed here, once, and every reader uses it, so the generic
    introspection-based ``get_params()`` needs no override: it emits the
    spellings verbatim and the reconstruction takes the same branch as the
    original construction.
    """

    def _aliased_flag(self, *names: str) -> Optional[bool]:
        for name in names:
            value = getattr(self, name, None)
            if value is not None:
                return value
        return None

    def _merged_flag(
        self,
        field_name: str,
        positive: Sequence[str],
        negative: str,
    ) -> Optional[bool]:
        """Merge one flag's spellings, rejecting two at once.

        Deliberately reads the stored attributes directly rather than going
        through the ``resolved_*`` properties, so a subclass that overrides one
        of those to apply a default cannot recurse back into it.
        """
        return _normalize_flag(
            self._aliased_flag(*positive),
            self._aliased_flag(negative),
            field_name,
        )

    def _validate_resolved_flags(self) -> None:
        """Reject mutually exclusive flag spellings at construction time.

        The merged values are computed on demand by the properties below, so
        validation happens here instead of being deferred to render time.
        """
        self._merged_flag("final", ("final", "is_final"), "not_final")
        self._merged_flag("instantiable", ("instantiable", "is_instantiable"), "not_instantiable")
        self._merged_flag("persistable", ("persistable",), "not_persistable")

    @property
    def resolved_final(self) -> Optional[bool]:
        """``FINAL`` / ``NOT FINAL`` / unspecified, whichever spelling was used."""
        return self._merged_flag("final", ("final", "is_final"), "not_final")

    @property
    def resolved_instantiable(self) -> Optional[bool]:
        """``INSTANTIABLE`` / ``NOT INSTANTIABLE`` / unspecified."""
        return self._merged_flag(
            "instantiable",
            ("instantiable", "is_instantiable"),
            "not_instantiable",
        )

    @property
    def resolved_persistable(self) -> Optional[bool]:
        """``PERSISTABLE`` / ``NOT PERSISTABLE`` / unspecified."""
        return self._merged_flag("persistable", ("persistable",), "not_persistable")


class OracleTypeDefinitionBase(_ResolvedFlagView, TypeDefinition):
    """Shared state for Oracle type definitions.

    Every option is stored verbatim under the parameter name the caller used, so
    the merged value of the aliased flags is reached through
    :class:`_ResolvedFlagView` rather than by rewriting the serialized form.
    """

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
        editionable: bool,
        noneditionable: bool,
        sharing: Optional[str],
        oid: Optional[str],
        default_collation: Optional[str],
        accessible_by: Any,
        invoker_rights: Any,
    ) -> None:
        self.under = _validate_optional_name(under, "under")
        self.final = final
        self.not_final = not_final
        self.instantiable = instantiable
        self.not_instantiable = not_instantiable
        self.persistable = persistable
        self.not_persistable = not_persistable
        self._validate_resolved_flags()
        if self.under is not None and self.resolved_persistable is not None:
            raise ValueError("subtypes inherit PERSISTABLE and cannot specify it")
        if not isinstance(force, bool):
            raise TypeError("force must be a bool")
        self.force = force
        self.authid = authid.upper() if isinstance(authid, str) else authid
        if self.authid is not None and self.authid not in ("CURRENT_USER", "DEFINER"):
            raise ValueError("authid must be CURRENT_USER or DEFINER")
        self.keyword = _normalize_keyword(keyword, "keyword")
        if editionable and noneditionable:
            raise ValueError(
                "editionable and noneditionable are mutually exclusive options"
            )
        if not isinstance(editionable, bool) or not isinstance(noneditionable, bool):
            raise TypeError("editionable and noneditionable must be bools")
        self.editionable = editionable
        self.noneditionable = noneditionable
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


class OracleObjectTypeDefinition(OracleTypeDefinitionBase):
    """Oracle object type definition.

    ``external_name`` exists only so that the SQLJ subclass's signature is a
    superset of this one; it is rejected above, so it always round-trips as
    ``None`` and the generic ``get_params()`` carries it unchanged.
    """

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
        editionable: bool = False,
        noneditionable: bool = False,
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
            noneditionable,
            sharing,
            oid,
            default_collation,
            accessible_by,
            invoker_rights,
        )


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
        editionable: bool = False,
        noneditionable: bool = False,
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
        # `java_class` and `using_type` are aliases of `external_name` and
        # `using_clause`. They are stored verbatim so the generic get_params()
        # round trip reproduces the same branch; the renderer reads the merged
        # values below.
        self.using_clause = using_clause
        self.using_type = using_type
        self._normalized_using_clause = _normalize_sqlj_using(using_clause)
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
            noneditionable,
            sharing,
            oid,
            default_collation,
            accessible_by,
            invoker_rights,
        )

    @property
    def normalized_using_clause(self) -> str:
        """The USING clause, whichever spelling supplied it.

        ``using_clause`` and ``using_type`` are two names for one clause; only
        the spelling that was passed owns the corresponding attribute.
        """
        return self._normalized_using_clause


class OracleVarrayTypeDefinition(_ResolvedFlagView, TypeDefinition):
    """Oracle named VARRAY type definition.

    ``size_limit`` has three aliases (``max_size``, ``max_length``, ``size``)
    which are mutually exclusive with it. The one the caller used owns the
    corresponding attribute; the merged limit is read through
    :attr:`resolved_size_limit`.
    """

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
        # Resolve the limit, then record which spelling actually supplied it: the
        # four names are mutually exclusive aliases of one value, and only the
        # one the caller used owns the corresponding attribute (the others stay
        # None, "not supplied"). Storing the resolved value under every name
        # would make the reconstruction see two spellings at once.
        limit_alias = "size_limit"
        if size_limit is None:
            if max_size is not None:
                limit_alias, limit_value = "max_size", max_size
            elif max_length is not None:
                limit_alias, limit_value = "max_length", max_length
            elif size is not None:
                limit_alias, limit_value = "size", size
            else:
                limit_value = None
        elif max_size is not None or max_length is not None or size is not None:
            raise ValueError(
                "size_limit, max_size, max_length and size are mutually exclusive aliases"
            )
        else:
            limit_value = size_limit
        if not isinstance(limit_value, int) or isinstance(limit_value, bool):
            raise TypeError("size_limit must be an int")
        if limit_value <= 0:
            raise ValueError("size_limit must be positive")
        if not isinstance(not_null, bool):
            raise TypeError("not_null must be a bool")
        self.element_type = element_type
        self.force = force
        self.size_limit = limit_value if limit_alias == "size_limit" else None
        self.max_size = limit_value if limit_alias == "max_size" else None
        self.max_length = limit_value if limit_alias == "max_length" else None
        self.size = limit_value if limit_alias == "size" else None
        self.not_null = not_null
        self.persistable = persistable
        self.not_persistable = not_persistable
        self._validate_resolved_flags()

    @property
    def resolved_size_limit(self) -> int:
        """The VARRAY element limit, whichever of the four spellings supplied it."""
        for value in (self.size_limit, self.max_size, self.max_length, self.size):
            if value is not None:
                return cast(int, value)
        raise ValueError("VARRAY requires a size limit")


class OracleNestedTableTypeDefinition(_ResolvedFlagView, TypeDefinition):
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
        self.persistable = persistable
        self.not_persistable = not_persistable
        self._validate_resolved_flags()


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


class OracleAlterTypeAddAttributeAction(OracleTypeAlterAction):
    """Add one or more object attributes.

    ``attribute``, ``attribute_name`` and ``attributes`` are mutually exclusive
    spellings of the same list, optionally combined with ``data_type`` to build a
    single ``OracleTypeAttribute``. Each is stored verbatim under the name the
    caller used; the coerced list is read through :attr:`resolved_attributes`.
    """

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
        self._resolved_attributes = _coerce_attributes(value)
        if not self._resolved_attributes:
            raise ValueError("ADD ATTRIBUTE requires at least one attribute")
        self.attribute = attribute
        self.attribute_name = attribute_name
        self.attributes = attributes
        self.data_type = data_type

    @property
    def resolved_attributes(self) -> List["OracleTypeAttribute"]:
        """The declared attributes, whichever spelling the caller used.

        ``attribute``, ``attribute_name`` and ``attributes`` are three names for
        one list; only the spelling that was passed owns the corresponding
        attribute, so read them back through this instead.
        """
        return self._resolved_attributes


class OracleAlterTypeModifyAttributeAction(OracleAlterTypeAddAttributeAction):
    """Modify one or more scalar object attributes."""

    action_kind = "oracle.attribute.modify"


class OracleAlterTypeDropAttributeAction(OracleTypeAlterAction):
    """Drop one or more object attributes.

    ``attribute``, ``attribute_name``, ``attributes`` and ``names`` are mutually
    exclusive spellings of the same list. Each is stored verbatim under the name
    the caller used; the coerced list is read through :attr:`resolved_attributes`.
    """

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
        self._resolved_attributes = [
            _coerce_attribute(item, require_type=False) for item in values
        ]
        self.attribute = attribute
        self.attribute_name = attribute_name
        self.attributes = attributes
        self.names = names

    @property
    def resolved_attributes(self) -> List["OracleTypeAttribute"]:
        """The dropped attributes, whichever spelling the caller used.

        ``attribute``, ``attribute_name``, ``attributes`` and ``names`` are four
        names for one list; only the spelling that was passed owns the
        corresponding attribute, so read them back through this instead.
        """
        return self._resolved_attributes


class OracleAlterTypeAddMethodAction(OracleTypeAlterAction):
    """Add one or more method declarations.

    ``method`` and ``declaration`` are mutually exclusive spellings of the same
    value, and ``external_name`` is folded into the resulting
    ``OracleTypeMethod``. Each is stored verbatim under the name the caller used;
    the coerced method is read through :attr:`resolved_method`.
    """

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
        self._resolved_method = _coerce_method(value)
        self.method = method
        self.declaration = declaration
        self.external_name = external_name

    @property
    def resolved_method(self) -> "OracleTypeMethod":
        """The declared method, whichever spelling the caller used.

        ``method`` and ``declaration`` are two names for one value; only the
        spelling that was passed owns the corresponding attribute.
        """
        return self._resolved_method


class OracleAlterTypeDropMethodAction(OracleTypeAlterAction):
    """Drop one or more method specifications.

    ``method`` is one spelling of the value; ``name`` / ``method_name`` /
    ``kind`` / ``parameters`` are a second, decomposed one that ``__init__``
    assembles into an ``OracleTypeMethod``. Each parameter is stored verbatim
    under its own name, so the generic ``get_params()`` reproduces the same
    branch on reconstruction; the assembled method is read through
    :attr:`resolved_method`.
    """

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
        assembled = method
        if assembled is None and (name is not None or method_name is not None):
            assembled = OracleTypeMethod(
                kind=kind or "MEMBER FUNCTION",
                name=name or method_name,
                parameters=parameters,
                require_return_type=False,
            )
        self._resolved_method = _coerce_method(assembled)
        self.method = method
        self.name = name
        self.method_name = method_name
        self.kind = kind
        self.parameters = parameters

    @property
    def resolved_method(self) -> "OracleTypeMethod":
        """The method being dropped, whichever spelling supplied it."""
        return self._resolved_method


class OracleAlterTypeLimitAction(OracleTypeAlterAction):
    """Increase a VARRAY type's element limit.

    ``limit``, ``size_limit`` and ``new_limit`` are mutually exclusive
    spellings of one positive integer; the one the caller used owns the
    corresponding attribute and the merged value is read through
    :attr:`resolved_limit`.
    """

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
        self.limit = limit
        self.size_limit = size_limit
        self.new_limit = new_limit
        self._resolved_limit = value

    @property
    def resolved_limit(self) -> int:
        """The new element limit, whichever spelling the caller used."""
        return self._resolved_limit


class OracleAlterTypeElementTypeAction(OracleTypeAlterAction):
    """Modify a collection element data type.

    ``element_type`` and ``new_element_type`` are mutually exclusive spellings
    of the same value; only the spelling that was passed owns the corresponding
    attribute.
    """

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
        self.element_type = element_type
        self.new_element_type = new_element_type
        self._resolved_element_type = (
            element_type if element_type is not None else new_element_type
        )
        if self._resolved_element_type is None:
            raise ValueError("MODIFY ELEMENT TYPE requires a data type")

    @property
    def resolved_element_type(self) -> Any:
        """The new element type, whichever spelling the caller used."""
        return self._resolved_element_type


class OracleAlterTypeCompileAction(OracleTypeAlterAction):
    """Compile an Oracle type specification or body.

    ``target`` and ``compile_target`` are two spellings of the same compile
    target; the one the caller used owns the corresponding attribute.
    """

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
        resolved_target = target if target is not None else compile_target
        if resolved_target is not None:
            resolved_target = str(resolved_target).upper()
            if resolved_target not in ("SPECIFICATION", "BODY"):
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
        self._resolved_target = resolved_target
        self.reuse_settings = reuse_settings
        self.compiler_parameters = compiler_parameters

    @property
    def resolved_target(self) -> Optional[str]:
        """The compile target, whichever spelling the caller used."""
        return self._resolved_target


class OracleAlterTypeFinalAction(_ResolvedFlagView, OracleTypeAlterAction):
    """Change the FINAL property of an object type.

    ``final``, ``is_final`` and ``not_final`` are mutually exclusive spellings of
    one flag; only the spelling that was passed owns the corresponding attribute,
    and the clause is mandatory: exactly one spelling must be set, or the
    construction is refused. The merged value is what the formatter reads, so
    it is never ``None`` by the time anything renders it.
    """

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
        self.final = final
        self.is_final = is_final
        self.not_final = not_final
        merged = self._merged_flag("final", ("final", "is_final"), "not_final")
        if merged is None:
            raise ValueError(
                "ALTER TYPE ... FINAL requires exactly one of final=True or "
                "not_final=True"
            )
        self._resolved_final = merged

    @property
    def resolved_final(self) -> bool:
        """The new FINAL setting; exactly one spelling is mandatory."""
        return self._resolved_final


class OracleAlterTypeInstantiableAction(OracleAlterTypeFinalAction):
    """Change the INSTANTIABLE property of an object type.

    ``instantiable``, ``is_instantiable`` and ``not_instantiable`` are mutually
    exclusive spellings of one flag; only the spelling that was passed owns the
    corresponding attribute, and the clause is mandatory: exactly one spelling
    must be set, or the construction is refused.
    """

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
        self.instantiable = instantiable
        self.is_instantiable = is_instantiable
        self.not_instantiable = not_instantiable
        merged = self._merged_flag(
            "instantiable",
            ("instantiable", "is_instantiable"),
            "not_instantiable",
        )
        if merged is None:
            raise ValueError(
                "ALTER TYPE ... INSTANTIABLE requires exactly one of "
                "instantiable=True or not_instantiable=True"
            )
        self._resolved_instantiable = merged
        self.final = None
        self.not_final = None
        self.is_final = None
        self._resolved_final = None

    @property
    def resolved_instantiable(self) -> bool:
        """The new INSTANTIABLE setting; exactly one spelling is mandatory."""
        return self._resolved_instantiable


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
    """Oracle CREATE TYPE BODY expression.

    ``schema`` is a deprecated alias of ``schema_name``; both are stored
    verbatim and ``__init__`` resolves them, so the generic ``get_params()``
    round trip carries the same pair back.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        type: Type,
        body: str,
        *,
        if_not_exists: bool = False,
        or_replace: bool = False,
        keyword: str = "AS",
        editionable: bool = False,
        noneditionable: bool = False,
    ) -> None:
        super().__init__(dialect)
        if not isinstance(type, Type):
            raise TypeError(
                f"type must be a Type, got {type.__name__}"
            )
        if not isinstance(body, str) or not body.strip():
            raise ValueError("body must be a non-empty string")
        if if_not_exists and or_replace:
            raise ValueError("CREATE TYPE BODY IF NOT EXISTS and OR REPLACE are mutually exclusive")
        self.type = type
        self.body = body
        self.if_not_exists = bool(if_not_exists)
        self.or_replace = bool(or_replace)
        self.keyword = _normalize_keyword(keyword, "keyword")
        if editionable and noneditionable:
            raise ValueError(
                "editionable and noneditionable are mutually exclusive options"
            )
        if not isinstance(editionable, bool) or not isinstance(noneditionable, bool):
            raise TypeError("editionable and noneditionable must be bools")
        self.editionable = editionable
        self.noneditionable = noneditionable

    @property
    def format_method(self) -> str:
        return "format_create_type_body_statement"


class OracleDropTypeExpression(DropTypeExpression):
    """Oracle DROP TYPE expression with FORCE or VALIDATE.

    ``schema`` is a deprecated alias of ``schema_name``; both are stored
    verbatim and ``__init__`` resolves them.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        type: Type,
        *,
        if_exists: bool = False,
        force: bool = False,
        validate: bool = False,
    ) -> None:
        super().__init__(
            dialect,
            type,
            if_exists=if_exists,
        )
        if force and validate:
            raise ValueError("DROP TYPE FORCE and VALIDATE are mutually exclusive")
        if not isinstance(force, bool) or not isinstance(validate, bool):
            raise TypeError("force and validate must be bools")
        self.force = force
        self.validate = validate


class DropTypeBodyExpression(BaseExpression):
    """Oracle DROP TYPE BODY expression.

    ``schema`` is a deprecated alias of ``schema_name``; both are stored
    verbatim and ``__init__`` resolves them.
    """

    def __init__(
        self,
        dialect: "OracleDialect",
        type: Type,
        *,
        if_exists: bool = False,
    ) -> None:
        super().__init__(dialect)
        if not isinstance(type, Type):
            raise TypeError(
                f"type must be a Type, got {type.__name__}"
            )
        self.type = type
        self.if_exists = bool(if_exists)

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
