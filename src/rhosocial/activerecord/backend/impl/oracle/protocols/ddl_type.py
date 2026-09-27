# src/rhosocial/activerecord/backend/impl/oracle/protocols/ddl_type.py
"""Oracle user-defined type DDL protocol."""

from __future__ import annotations

from typing import Any, Protocol, Tuple, Type, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import UserDefinedTypeSupport


@runtime_checkable
class OracleTypeDDLSupport(UserDefinedTypeSupport, Protocol):
    """Protocol for Oracle TYPE DDL support."""

    def supports_type_objects(self) -> bool:
        """Whether Oracle user-defined type objects are supported."""
        ...

    def supports_create_type(self) -> bool:
        """Whether CREATE TYPE is supported."""
        ...

    def supports_alter_type(self) -> bool:
        """Whether ALTER TYPE is supported."""
        ...

    def supports_drop_type(self) -> bool:
        """Whether DROP TYPE is supported."""
        ...

    def supports_type_persistable(self) -> bool:
        """Whether PERSISTABLE type clauses are supported."""
        ...

    def supports_type_definition(self, definition_type: Type[Any]) -> bool:
        """Whether a type-definition class is supported."""
        ...

    def supported_type_definitions(self) -> Tuple[Type[Any], ...]:
        """Return supported type-definition classes."""
        ...

    def supports_type_alter_action(self, action_type: Type[Any]) -> bool:
        """Whether an ALTER TYPE action class is supported."""
        ...

    def supports_create_type_if_not_exists(self) -> bool:
        """Whether CREATE TYPE IF NOT EXISTS is supported."""
        ...

    def supports_create_type_or_replace(self) -> bool:
        """Whether CREATE OR REPLACE TYPE is supported."""
        ...

    def supports_alter_type_if_exists(self) -> bool:
        """Whether ALTER TYPE IF EXISTS is supported."""
        ...

    def supports_drop_type_if_exists(self) -> bool:
        """Whether DROP TYPE IF EXISTS is supported."""
        ...

    def supports_multiple_type_alter_actions(self) -> bool:
        """Whether multiple ALTER TYPE actions are supported."""
        ...

    def supports_create_type_body(self) -> bool:
        """Whether CREATE TYPE BODY is supported."""
        ...

    def supports_drop_type_body(self) -> bool:
        """Whether DROP TYPE BODY is supported."""
        ...

    def supports_create_type_body_if_not_exists(self) -> bool:
        """Whether CREATE TYPE BODY IF NOT EXISTS is supported."""
        ...

    def supports_drop_type_body_if_exists(self) -> bool:
        """Whether DROP TYPE BODY IF EXISTS is supported."""
        ...

    def supports_drop_type_force(self) -> bool:
        """Whether DROP TYPE FORCE is supported."""
        ...

    def supports_drop_type_validate(self) -> bool:
        """Whether DROP TYPE VALIDATE is supported."""
        ...

    def supports_type_attribute_actions(self) -> bool:
        """Whether attribute ALTER TYPE actions are supported."""
        ...

    def supports_type_method_actions(self) -> bool:
        """Whether method ALTER TYPE actions are supported."""
        ...

    def supports_type_limit_actions(self) -> bool:
        """Whether collection ALTER TYPE actions are supported."""
        ...

    def supports_type_compile(self) -> bool:
        """Whether ALTER TYPE COMPILE is supported."""
        ...

    def supports_type_final(self) -> bool:
        """Whether ALTER TYPE FINAL is supported."""
        ...

    def supports_type_instantiable(self) -> bool:
        """Whether ALTER TYPE INSTANTIABLE is supported."""
        ...

    def supports_sqlj_type(self) -> bool:
        """Whether SQLJ type definitions are supported."""
        ...

    def format_type_definition(self, expr: Any) -> Tuple[str, tuple]:
        """Format a type definition."""
        ...

    def format_type_alter_action(self, expr: Any) -> Tuple[str, tuple]:
        """Format an ALTER TYPE action."""
        ...

    def format_create_type_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format CREATE TYPE."""
        ...

    def format_alter_type_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format ALTER TYPE."""
        ...

    def format_drop_type_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format DROP TYPE."""
        ...

    def format_create_type_body_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format CREATE TYPE BODY."""
        ...

    def format_drop_type_body_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format DROP TYPE BODY."""
        ...


OracleTypeSupport = OracleTypeDDLSupport
OracleUserDefinedTypeSupport = OracleTypeDDLSupport


__all__ = [
    "OracleTypeDDLSupport",
    "OracleTypeSupport",
    "OracleUserDefinedTypeSupport",
]
