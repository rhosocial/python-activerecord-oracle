# src/rhosocial/activerecord/backend/impl/oracle/mixins/ddl_type.py
"""Oracle user-defined type DDL capability and SQL formatting."""

from __future__ import annotations

import re
from typing import Any, Iterable, List, Optional, Tuple, Type, cast

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.mixins.user_defined_type import UserDefinedTypeMixin
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    AlterTypeExpression,
    CreateTypeExpression,
    DropTypeExpression,
    TypeAlterAction,
    TypeDefinition,
)

from ..expression.ddl.type import (
    DropTypeBodyExpression,
    OracleAlterTypeAddAttributeAction,
    OracleAlterTypeAddMethodAction,
    OracleAlterTypeCompileAction,
    OracleAlterTypeDropAttributeAction,
    OracleAlterTypeDropMethodAction,
    OracleAlterTypeElementTypeAction,
    OracleAlterTypeFinalAction,
    OracleAlterTypeInstantiableAction,
    OracleAlterTypeLimitAction,
    OracleAlterTypeModifyAttributeAction,
    OracleAlterTypeResetAction,
    OracleCreateTypeBodyExpression,
    OracleIncompleteTypeDefinition,
    OracleNestedTableTypeDefinition,
    OracleObjectTypeDefinition,
    OracleSqljTypeDefinition,
    OracleTypeAlterAction,
    OracleTypeAttribute,
    OracleTypeMethod,
    OracleVarrayTypeDefinition,
    _normalize_invoker_rights,
    _normalize_method_kind,
    _normalize_sqlj_using,
    _validate_accessible_by,
    _validate_method_name,
    _validate_method_parameter,
)


class OracleTypeDDLMixin(UserDefinedTypeMixin):
    """Oracle TYPE DDL capability checks and formatters."""

    _TYPE_DEFINITION_TYPES = (
        OracleObjectTypeDefinition,
        OracleSqljTypeDefinition,
        OracleVarrayTypeDefinition,
        OracleNestedTableTypeDefinition,
        OracleIncompleteTypeDefinition,
    )
    _TYPE_ACTION_TYPES = (
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
    )
    _TYPE_MIN_VERSION = (9, 0, 0)
    _TYPE_PERSISTABLE_MIN_VERSION = (18, 0, 0)
    _TYPE_IF_EXISTS_MAJOR = 19
    _TYPE_IF_EXISTS_RU = 28

    def _has_type_support(self) -> bool:
        version = getattr(self, "_version", None)
        return version is not None and tuple(version) >= self._TYPE_MIN_VERSION

    def supports_type_persistable(self) -> bool:
        version = getattr(self, "_version", None)
        return version is not None and tuple(version) >= self._TYPE_PERSISTABLE_MIN_VERSION

    def _supports_type_if_exists(self) -> bool:
        version = getattr(self, "_version", None)
        if version is None:
            return False
        version = tuple(version)
        if not version or version[0] != self._TYPE_IF_EXISTS_MAJOR:
            return False
        full_version = getattr(self, "version_full", None)
        configured_ru = cast(Optional[int], getattr(self, "_ru_version", None))
        ru_version = configured_ru
        if full_version and len(full_version) >= 2:
            full_version = cast(Tuple[int, ...], tuple(full_version))
            if full_version[0] != self._TYPE_IF_EXISTS_MAJOR:
                return False
            if configured_ru is not None and configured_ru != full_version[1]:
                return False
            ru_version = full_version[1]
        return ru_version is not None and ru_version >= self._TYPE_IF_EXISTS_RU

    def supports_type_objects(self) -> bool:
        return self._has_type_support()

    def supports_create_type(self) -> bool:
        return self._has_type_support()

    def supports_alter_type(self) -> bool:
        return self._has_type_support()

    def supports_drop_type(self) -> bool:
        return self._has_type_support()

    def supported_type_definitions(self) -> Tuple[Type[TypeDefinition], ...]:
        return self._TYPE_DEFINITION_TYPES

    def supports_type_definition(
        self,
        definition_type: Type[TypeDefinition],
    ) -> bool:
        try:
            return issubclass(definition_type, self._TYPE_DEFINITION_TYPES)
        except TypeError:
            return False

    def supports_type_alter_action(
        self,
        action_type: Type[TypeAlterAction],
    ) -> bool:
        try:
            return issubclass(action_type, self._TYPE_ACTION_TYPES)
        except TypeError:
            return False

    def supports_create_type_or_replace(self) -> bool:
        return self._has_type_support()

    def supports_create_type_if_not_exists(self) -> bool:
        return self._supports_type_if_exists()

    def supports_alter_type_if_exists(self) -> bool:
        return self._supports_type_if_exists()

    def supports_drop_type_if_exists(self) -> bool:
        return self._supports_type_if_exists()

    def supports_multiple_type_alter_actions(self) -> bool:
        return self._has_type_support()

    def supports_create_type_body(self) -> bool:
        return self._has_type_support()

    def supports_drop_type_body(self) -> bool:
        return self._has_type_support()

    def supports_create_type_body_if_not_exists(self) -> bool:
        return self._supports_type_if_exists()

    def supports_drop_type_body_if_exists(self) -> bool:
        return self._supports_type_if_exists()

    def supports_drop_type_force(self) -> bool:
        return self._has_type_support()

    def supports_drop_type_validate(self) -> bool:
        return self._has_type_support()

    def supports_type_attribute_actions(self) -> bool:
        return self._has_type_support()

    def supports_type_method_actions(self) -> bool:
        return self._has_type_support()

    def supports_type_limit_actions(self) -> bool:
        return self._has_type_support()

    def supports_type_compile(self) -> bool:
        return self._has_type_support()

    def supports_type_final(self) -> bool:
        return self._has_type_support()

    def supports_type_instantiable(self) -> bool:
        return self._has_type_support()

    def supports_sqlj_type(self) -> bool:
        return self._has_type_support()

    def _format_type_name(self, type_name: str, schema_name: Optional[str]) -> str:
        name_sql = cast(str, self.format_identifier(type_name))
        if schema_name is not None:
            name_sql = f"{cast(str, self.format_identifier(schema_name))}.{name_sql}"
        return name_sql

    def _format_type_value(self, value: Any) -> str:
        if isinstance(value, BaseExpression):
            sql, params = value.to_sql()
            if params:
                raise ValueError("Oracle type declarations must render without bind parameters")
            return cast(str, sql)
        if not isinstance(value, str) or not value.strip():
            raise TypeError("Oracle data type must be a BaseExpression or non-empty string")
        if any(token in value for token in (";", "--", "/*", "*/")):
            raise ValueError("Oracle data type must not contain SQL statement or comment tokens")
        return value.strip()

    def _format_type_reference(self, value: Any) -> str:
        if isinstance(value, BaseExpression):
            return self._format_type_value(value)
        if not isinstance(value, str) or not value.strip():
            raise TypeError("Oracle type reference must be a non-empty string")
        text = value.strip()
        if any(token in text for token in (";", "--", "/*", "*/")):
            raise ValueError("Oracle type reference must not contain SQL tokens")
        if "." in text and '"' not in text:
            return ".".join(self.format_identifier(part) for part in text.split("."))
        return text

    def _format_external_name(self, value: str) -> str:
        escaped = value.replace("'", "''")
        return f"EXTERNAL NAME '{escaped}'"

    def _format_attribute(self, attribute: OracleTypeAttribute) -> str:
        if attribute.data_type is None:
            raise ValueError("attribute data_type is required")
        if attribute.not_null:
            raise ValueError("Oracle object type attributes do not support NOT NULL")
        parts = [self.format_identifier(attribute.name), self._format_type_value(attribute.data_type)]
        if attribute.external_name is not None:
            parts.append(self._format_external_name(attribute.external_name))
        return " ".join(parts)

    def _format_method_parameter(self, parameter: Any) -> str:
        if isinstance(parameter, BaseExpression):
            return self._format_type_value(parameter)
        return cast(str, _validate_method_parameter(parameter))

    def _format_method(self, method: OracleTypeMethod) -> str:
        if method.declaration is not None:
            declaration = method.declaration
        else:
            if method.require_return_type is False:
                raise ValueError("return_type is required when formatting a method specification")
            kind = _normalize_method_kind(method.kind)
            if kind is None or method.name is None:
                raise ValueError("structured method kind and name are required")
            _validate_method_name(method.name)
            parameters = method.parameters
            if parameters is None:
                parameter_values = []
            elif isinstance(parameters, (str, BaseExpression)):
                parameter_values = [parameters]
            else:
                parameter_values = list(parameters)
            rendered_parameters = [
                self._format_method_parameter(parameter) for parameter in parameter_values
            ]
            parameter_sql = f"({', '.join(rendered_parameters)})"
            if kind in {
                "MEMBER FUNCTION",
                "STATIC FUNCTION",
                "CONSTRUCTOR FUNCTION",
                "MAP MEMBER FUNCTION",
                "ORDER MEMBER FUNCTION",
            }:
                if method.return_type is None:
                    raise ValueError(f"{kind} requires return_type")
                return_sql = f"RETURN {self._format_type_value(method.return_type)}"
            else:
                if method.return_type is not None:
                    raise ValueError(f"{kind} must not define return_type")
                return_sql = ""
            declaration = f"{kind} {method.name}{parameter_sql}"
            if return_sql:
                declaration = f"{declaration} {return_sql}"
        if method.external_name is not None:
            declaration = f"{declaration} {self._format_external_name(method.external_name)}"
        return declaration

    def _format_drop_method(self, method: OracleTypeMethod) -> str:
        if method.name is not None:
            _validate_method_name(method.name)
            kind = _normalize_method_kind(method.kind) or "MEMBER FUNCTION"
            parameters = method.parameters
            if isinstance(parameters, (str, BaseExpression)):
                parameter_values = [parameters]
            elif parameters is not None:
                parameter_values = list(parameters)
            else:
                parameter_values = []
            rendered_parameters = [
                self._format_method_parameter(parameter) for parameter in parameter_values
            ]
            parameter_sql = f"({', '.join(rendered_parameters)})"
            return f"{kind} {method.name}{parameter_sql}"
        if method.declaration is None:
            raise ValueError("DROP method requires a method name or declaration")
        declaration = re.sub(r"\s+", " ", method.declaration.strip())
        declaration = re.sub(r"\bRETURN\b.*$", "", declaration, flags=re.IGNORECASE).strip()
        declaration = re.sub(r"\bDROP\s+", "", declaration, flags=re.IGNORECASE).strip()
        if not declaration:
            raise ValueError("DROP method requires a method name or declaration")
        if not re.match(
            r"(?:MAP|ORDER|MEMBER|STATIC|CONSTRUCTOR)\b",
            declaration,
            re.IGNORECASE,
        ):
            declaration = f"MEMBER FUNCTION {declaration}"
        return declaration

    def _format_attributes(self, attributes: Iterable[OracleTypeAttribute]) -> str:
        return ", ".join(self._format_attribute(attribute) for attribute in attributes)

    def _format_methods(self, methods: Iterable[OracleTypeMethod]) -> str:
        return ", ".join(self._format_method(method) for method in methods)

    def _format_flags(self, expr: Any) -> List[str]:
        flags: List[str] = []
        for name in ("final", "instantiable", "persistable"):
            value = getattr(expr, name, None)
            if value is None:
                continue
            flags.append(name.upper() if value else f"NOT {name.upper()}")
        return flags

    def _format_common_prefix(self, expr: Any) -> List[str]:
        parts: List[str] = []
        if getattr(expr, "force", False):
            parts.append("FORCE")
        authid = getattr(expr, "authid", None)
        invoker_rights = _normalize_invoker_rights(getattr(expr, "invoker_rights", None))
        if authid is not None and invoker_rights:
            raise ValueError("authid and invoker_rights are mutually exclusive")
        if invoker_rights:
            parts.append("AUTHID CURRENT_USER")
        elif authid:
            parts.append(f"AUTHID {authid}")
        sharing = getattr(expr, "sharing", None)
        if sharing:
            parts.append(sharing)
        default_collation = getattr(expr, "default_collation", None)
        if default_collation:
            parts.append(f"DEFAULT COLLATION {default_collation}")
        accessible_by = getattr(expr, "accessible_by", None)
        if accessible_by is not None:
            accessors = _validate_accessible_by(accessible_by) or ()
            parts.append(f"ACCESSIBLE BY ({', '.join(accessors)})")
        return parts

    def _format_object_definition(self, expr: OracleObjectTypeDefinition) -> str:
        parts = self._format_common_prefix(expr)
        oid = getattr(expr, "oid", None)
        if oid:
            escaped = oid.replace("'", "''")
            oid_sql = f"OID '{escaped}'"
            parts.insert(1 if parts and parts[0] == "FORCE" else 0, oid_sql)
        if expr.under is not None:
            parts.append(f"UNDER {self._format_type_reference(expr.under)}")
        else:
            parts.append(f"{expr.keyword} OBJECT")
        parts.append(f"({self._format_attributes(expr.attributes)})")
        if expr.methods:
            parts[-1] = f"({self._format_attributes(expr.attributes)}, {self._format_methods(expr.methods)})"
        parts.extend(self._format_flags(expr))
        return " ".join(parts)

    def _format_sqlj_definition(self, expr: OracleSqljTypeDefinition) -> str:
        parts = self._format_common_prefix(expr)
        if expr.oid:
            escaped = expr.oid.replace("'", "''")
            parts.append(f"OID '{escaped}'")
        if expr.under is not None:
            parts.append(f"UNDER {self._format_type_reference(expr.under)}")
        else:
            parts.append(f"{expr.keyword} OBJECT")
        external_name = getattr(expr, "external_name", None)
        if not isinstance(external_name, str):
            raise ValueError("SQLJ object types require EXTERNAL NAME")
        parts.append(self._format_external_name(external_name))
        language = getattr(expr, "language", None)
        if not isinstance(language, str) or re.sub(r"\s+", " ", language.strip().upper()) != "JAVA":
            raise ValueError("SQLJ object types only support LANGUAGE JAVA")
        parts.append("LANGUAGE JAVA")
        members = []
        if expr.attributes:
            members.append(self._format_attributes(expr.attributes))
        if expr.methods:
            members.append(self._format_methods(expr.methods))
        using_clause = _normalize_sqlj_using(getattr(expr, "using_clause", None))
        parts.append(f"USING {using_clause} ({', '.join(members)})")
        parts.extend(self._format_flags(expr))
        return " ".join(parts)

    def _format_varray_definition(self, expr: OracleVarrayTypeDefinition) -> str:
        prefix = "FORCE " if expr.force else ""
        element_sql = self._format_type_value(expr.element_type)
        if expr.not_null:
            element_sql = f"{element_sql} NOT NULL"
        if expr.persistable is None:
            return f"{prefix}AS VARRAY({expr.size_limit}) OF {element_sql}"
        persistable = "PERSISTABLE" if expr.persistable else "NOT PERSISTABLE"
        return f"{prefix}AS VARRAY({expr.size_limit}) OF ({element_sql}) {persistable}"

    def _format_nested_table_definition(self, expr: OracleNestedTableTypeDefinition) -> str:
        prefix = "FORCE " if expr.force else ""
        element_sql = self._format_type_value(expr.element_type)
        if expr.not_null:
            element_sql = f"{element_sql} NOT NULL"
        if expr.persistable is None:
            return f"{prefix}AS TABLE OF {element_sql}"
        persistable = "PERSISTABLE" if expr.persistable else "NOT PERSISTABLE"
        return f"{prefix}AS TABLE OF ({element_sql}) {persistable}"

    def _format_dependent_clause(self, expr: OracleTypeAlterAction) -> str:
        handling = expr.dependent_handling
        if handling is None:
            if not expr.force:
                return ""
            return "CASCADE FORCE"
        text = handling.value
        if expr.force:
            if not text.startswith("CASCADE"):
                raise ValueError("ALTER TYPE force requires a CASCADE dependent clause")
            return f"{text} FORCE"
        return text

    def format_type_definition(self, expr: TypeDefinition) -> Tuple[str, tuple]:
        if not self.supports_type_objects():
            raise UnsupportedFeatureError(self.name, "TYPE definition")
        if getattr(expr, "persistable", None) is not None and not self.supports_type_persistable():
            raise UnsupportedFeatureError(self.name, "TYPE PERSISTABLE clause")
        if not self.supports_type_definition(type(expr)):
            raise UnsupportedFeatureError(
                self.name,
                f"TYPE definition {getattr(expr, 'definition_kind', type(expr).__name__)}",
            )
        if isinstance(expr, OracleSqljTypeDefinition):
            return self._format_sqlj_definition(expr), ()
        if isinstance(expr, OracleObjectTypeDefinition):
            return self._format_object_definition(expr), ()
        if isinstance(expr, OracleVarrayTypeDefinition):
            return self._format_varray_definition(expr), ()
        if isinstance(expr, OracleNestedTableTypeDefinition):
            return self._format_nested_table_definition(expr), ()
        if isinstance(expr, OracleIncompleteTypeDefinition):
            return "", ()
        raise UnsupportedFeatureError(self.name, "TYPE definition")

    def format_create_type_statement(
        self,
        expr: CreateTypeExpression,
    ) -> Tuple[str, tuple]:
        if not self.supports_type_objects() or not self.supports_create_type():
            raise UnsupportedFeatureError(self.name, "CREATE TYPE")
        if expr.if_not_exists and not self.supports_create_type_if_not_exists():
            raise UnsupportedFeatureError(self.name, "CREATE TYPE IF NOT EXISTS")
        if expr.or_replace and not self.supports_create_type_or_replace():
            raise UnsupportedFeatureError(self.name, "CREATE OR REPLACE TYPE")
        if expr.if_not_exists and expr.or_replace:
            raise ValueError("CREATE TYPE IF NOT EXISTS and OR REPLACE are mutually exclusive")
        if expr.if_not_exists and getattr(expr.definition, "force", False):
            raise ValueError("CREATE TYPE IF NOT EXISTS cannot be combined with FORCE")
        if not self.supports_type_definition(type(expr.definition)):
            raise UnsupportedFeatureError(
                self.name,
                f"TYPE definition {expr.definition.definition_kind}",
            )
        definition_sql, definition_params = expr.definition.to_sql()
        if definition_params:
            raise ValueError("CREATE TYPE must render without bind parameters")
        parts = ["CREATE"]
        if expr.or_replace:
            parts.append("OR REPLACE")
        editionable = getattr(expr.definition, "editionable", None)
        if editionable is not None:
            parts.append("EDITIONABLE" if editionable else "NONEDITIONABLE")
        parts.append("TYPE")
        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")
        parts.append(self._format_type_name(expr.type_name, expr.schema_name))
        if definition_sql:
            parts.append(definition_sql)
        return " ".join(parts), tuple(definition_params)

    def _format_single_action(self, action: TypeAlterAction) -> Tuple[str, tuple]:
        if not self.supports_type_alter_action(type(action)):
            raise UnsupportedFeatureError(
                self.name,
                f"ALTER TYPE action {getattr(action, 'action_kind', type(action).__name__)}",
            )
        action_sql, params = action.to_sql()
        return action_sql, tuple(params)

    def _alter_action_family(self, action: TypeAlterAction) -> Optional[str]:
        if isinstance(action, OracleAlterTypeModifyAttributeAction):
            return "attribute.modify"
        if isinstance(action, OracleAlterTypeAddAttributeAction):
            return "attribute.add"
        if isinstance(action, OracleAlterTypeDropAttributeAction):
            return "attribute.drop"
        if isinstance(action, (OracleAlterTypeAddMethodAction, OracleAlterTypeDropMethodAction)):
            return "method"
        if isinstance(action, OracleAlterTypeLimitAction):
            return "collection.limit"
        if isinstance(action, OracleAlterTypeElementTypeAction):
            return "collection.element"
        if isinstance(action, OracleAlterTypeInstantiableAction):
            return "standalone.instantiable"
        if isinstance(action, OracleAlterTypeFinalAction):
            return "standalone.final"
        if isinstance(action, OracleAlterTypeCompileAction):
            return "standalone.compile"
        if isinstance(action, OracleAlterTypeResetAction):
            return "standalone.reset"
        return None

    def _format_alter_dependent_group(self, actions: List[TypeAlterAction]) -> str:
        suffixes = []
        for action in actions:
            suffix = self._format_dependent_clause(cast(OracleTypeAlterAction, action))
            if suffix:
                suffixes.append(suffix)
        if len(suffixes) > 1:
            raise UnsupportedFeatureError(
                self.name,
                "multiple ALTER TYPE dependent handling clauses",
            )
        return suffixes[0] if suffixes else ""

    def _format_attribute_action_group(
        self,
        actions: List[TypeAlterAction],
        keyword: str,
    ) -> Tuple[str, tuple]:
        attributes: List[OracleTypeAttribute] = []
        for action in actions:
            attributes.extend(cast(OracleAlterTypeAddAttributeAction, action).attributes)
        suffix = self._format_alter_dependent_group(actions)
        if keyword == "DROP":
            clause = self._format_attribute_names(attributes)
        else:
            clause = self._format_attributes(attributes)
        return f"{keyword} ATTRIBUTE ({clause}) {suffix}".rstrip(), ()

    def _format_method_action_group(self, actions: List[TypeAlterAction]) -> Tuple[str, tuple]:
        action_parts: List[str] = []
        for action in actions:
            action_sql, _ = self.format_type_alter_action(action)
            suffix = self._format_dependent_clause(cast(OracleTypeAlterAction, action))
            if suffix and action_sql.endswith(suffix):
                action_sql = action_sql[: -len(suffix)].rstrip()
            action_parts.append(action_sql)
        suffix = self._format_alter_dependent_group(actions)
        return f"{', '.join(action_parts)} {suffix}".rstrip(), ()

    def _validate_alter_action_sequence(self, actions: List[TypeAlterAction]) -> str:
        families = [self._alter_action_family(action) for action in actions]
        if any(family is None for family in families):
            raise UnsupportedFeatureError(self.name, "unsupported ALTER TYPE action")
        unique_families = set(families)
        if len(unique_families) != 1:
            raise UnsupportedFeatureError(self.name, "mixed ALTER TYPE action families")
        family = next(iter(unique_families))
        if family not in {
            "attribute.add",
            "attribute.modify",
            "attribute.drop",
            "method",
        }:
            raise UnsupportedFeatureError(self.name, f"multiple {family} actions")
        return cast(str, family)

    def format_type_alter_action(self, expr: TypeAlterAction) -> Tuple[str, tuple]:
        if not self.supports_type_alter_action(type(expr)):
            raise UnsupportedFeatureError(
                self.name,
                f"ALTER TYPE action {getattr(expr, 'action_kind', type(expr).__name__)}",
            )
        if isinstance(expr, OracleAlterTypeInstantiableAction):
            value = "INSTANTIABLE" if expr.instantiable else "NOT INSTANTIABLE"
            suffix = self._format_dependent_clause(expr)
            return f"{value} {suffix}".rstrip(), ()
        if isinstance(expr, OracleAlterTypeFinalAction):
            value = "FINAL" if expr.final else "NOT FINAL"
            suffix = self._format_dependent_clause(expr)
            return f"{value} {suffix}".rstrip(), ()
        if isinstance(expr, OracleAlterTypeCompileAction):
            if expr.dependent_handling is not None or expr.force:
                raise ValueError("COMPILE does not accept dependent handling")
            parts = ["COMPILE"]
            if expr.debug:
                parts.append("DEBUG")
            if expr.target:
                parts.append(expr.target)
            parts.extend(str(parameter) for parameter in expr.compiler_parameters)
            if expr.reuse_settings:
                parts.append("REUSE SETTINGS")
            return " ".join(parts), ()
        if isinstance(expr, OracleAlterTypeResetAction):
            if expr.dependent_handling is not None or expr.force:
                raise ValueError("RESET does not accept dependent handling")
            return "RESET", ()
        if isinstance(expr, OracleAlterTypeAddAttributeAction):
            if isinstance(expr, OracleAlterTypeModifyAttributeAction):
                keyword = "MODIFY"
            else:
                keyword = "ADD"
            return (
                f"{keyword} ATTRIBUTE ({self._format_attributes(expr.attributes)}) "
                f"{self._format_dependent_clause(expr)}".rstrip(),
                (),
            )
        if isinstance(expr, OracleAlterTypeDropAttributeAction):
            return (
                f"DROP ATTRIBUTE ({self._format_attribute_names(expr.attributes)}) "
                f"{self._format_dependent_clause(expr)}".rstrip(),
                (),
            )
        if isinstance(expr, OracleAlterTypeDropMethodAction):
            return (
                f"DROP {self._format_drop_method(expr.method)} "
                f"{self._format_dependent_clause(expr)}".rstrip(),
                (),
            )
        if isinstance(expr, OracleAlterTypeAddMethodAction):
            return (
                f"ADD {self._format_method(expr.method)} "
                f"{self._format_dependent_clause(expr)}".rstrip(),
                (),
            )
        if isinstance(expr, OracleAlterTypeLimitAction):
            return (
                f"MODIFY LIMIT {expr.limit} {self._format_dependent_clause(expr)}".rstrip(),
                (),
            )
        if isinstance(expr, OracleAlterTypeElementTypeAction):
            return (
                f"MODIFY ELEMENT TYPE {self._format_type_value(expr.element_type)} "
                f"{self._format_dependent_clause(expr)}".rstrip(),
                (),
            )
        raise UnsupportedFeatureError(
            self.name,
            f"ALTER TYPE action {getattr(expr, 'action_kind', type(expr).__name__)}",
        )

    def format_alter_type_statement(self, expr: AlterTypeExpression) -> Tuple[str, tuple]:
        if not self.supports_type_objects() or not self.supports_alter_type():
            raise UnsupportedFeatureError(self.name, "ALTER TYPE")
        if expr.if_exists and not self.supports_alter_type_if_exists():
            raise UnsupportedFeatureError(self.name, "ALTER TYPE IF EXISTS")
        actions = list(expr.actions)
        if len(actions) > 1 and not self.supports_multiple_type_alter_actions():
            raise UnsupportedFeatureError(self.name, "multiple ALTER TYPE actions")
        if len(actions) == 1:
            action_sql, action_params = self._format_single_action(actions[0])
        else:
            family = self._validate_alter_action_sequence(actions)
            if family.startswith("attribute."):
                action_sql, action_params = self._format_attribute_action_group(
                    actions,
                    family.rsplit(".", 1)[1].upper(),
                )
            else:
                action_sql, action_params = self._format_method_action_group(actions)
        parts = ["ALTER TYPE"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self._format_type_name(expr.type_name, expr.schema_name))
        parts.append(action_sql)
        return " ".join(parts), tuple(action_params)

    def format_drop_type_statement(self, expr: DropTypeExpression) -> Tuple[str, tuple]:
        if not self.supports_type_objects() or not self.supports_drop_type():
            raise UnsupportedFeatureError(self.name, "DROP TYPE")
        if expr.if_exists and not self.supports_drop_type_if_exists():
            raise UnsupportedFeatureError(self.name, "DROP TYPE IF EXISTS")
        force = bool(getattr(expr, "force", False))
        validate = bool(getattr(expr, "validate", False))
        if force and validate:
            raise ValueError("DROP TYPE FORCE and VALIDATE are mutually exclusive")
        if force and not self.supports_drop_type_force():
            raise UnsupportedFeatureError(self.name, "DROP TYPE FORCE")
        if validate and not self.supports_drop_type_validate():
            raise UnsupportedFeatureError(self.name, "DROP TYPE VALIDATE")
        parts = ["DROP TYPE"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self._format_type_name(expr.type_name, expr.schema_name))
        if force:
            parts.append("FORCE")
        elif validate:
            parts.append("VALIDATE")
        return " ".join(parts), ()

    def format_create_type_body_statement(
        self,
        expr: OracleCreateTypeBodyExpression,
    ) -> Tuple[str, tuple]:
        if not self.supports_type_objects() or not self.supports_create_type_body():
            raise UnsupportedFeatureError(self.name, "CREATE TYPE BODY")
        if expr.if_not_exists and not self.supports_create_type_body_if_not_exists():
            raise UnsupportedFeatureError(self.name, "CREATE TYPE BODY IF NOT EXISTS")
        if expr.or_replace and not self.supports_create_type_or_replace():
            raise UnsupportedFeatureError(self.name, "CREATE OR REPLACE TYPE BODY")
        if expr.if_not_exists and expr.or_replace:
            raise ValueError("CREATE TYPE BODY IF NOT EXISTS and OR REPLACE are mutually exclusive")
        parts = ["CREATE"]
        if expr.or_replace:
            parts.append("OR REPLACE")
        if expr.editionable is not None:
            parts.append("EDITIONABLE" if expr.editionable else "NONEDITIONABLE")
        parts.append("TYPE BODY")
        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")
        parts.append(self._format_type_name(expr.type_name, expr.schema_name))
        parts.append(f"{expr.keyword} {expr.body}")
        return " ".join(parts), ()

    def format_drop_type_body_statement(
        self,
        expr: DropTypeBodyExpression,
    ) -> Tuple[str, tuple]:
        if not self.supports_type_objects() or not self.supports_drop_type_body():
            raise UnsupportedFeatureError(self.name, "DROP TYPE BODY")
        if expr.if_exists and not self.supports_drop_type_body_if_exists():
            raise UnsupportedFeatureError(self.name, "DROP TYPE BODY IF EXISTS")
        parts = ["DROP TYPE BODY"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self._format_type_name(expr.type_name, expr.schema_name))
        return " ".join(parts), ()

    def _format_attribute_names(self, attributes: Iterable[OracleTypeAttribute]) -> str:
        return ", ".join(self.format_identifier(attribute.name) for attribute in attributes)


__all__ = ["OracleTypeDDLMixin"]
