# src/rhosocial/activerecord/backend/impl/oracle/mixins/routine.py
"""Oracle PL/SQL routine and package DDL formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.ddl.routine import (
        OracleCreateFunctionExpression,
        OracleCreatePackageBodyExpression,
        OracleCreatePackageExpression,
        OracleCreateProcedureExpression,
        OracleDropRoutineExpression,
    )


class OracleRoutineMixin:
    """Oracle stored routine and package capability checks and formatters.

    Stored procedures, functions and packages have existed since the
    earliest PL/SQL releases; the formatters here gate on ``(9, 0, 0)``
    per the backend implementation contract. Routine bodies are passed
    through verbatim as raw PL/SQL strings.
    """

    def supports_create_procedure(self) -> bool:
        return True

    def supports_create_function(self) -> bool:
        return True

    def supports_create_package(self) -> bool:
        return True

    def supports_create_package_body(self) -> bool:
        return True

    def _format_parameters(self, parameters) -> str:
        """Render formal parameters as a comma-separated declaration list."""
        rendered = []
        for param in parameters:
            parts = [self.format_identifier(param.name)]
            if param.mode is not None:
                parts.append(param.mode.value)
            parts.append(self.format_identifier(param.data_type))
            rendered.append(" ".join(parts))
        return ", ".join(rendered)

    def format_create_procedure_statement(
        self, expr: "OracleCreateProcedureExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE PROCEDURE",
                suggestion=(
                    f"Oracle {self.version} does not support stored "
                    "procedures; they require Oracle 9i or later."
                ),
            )
        parts = ["CREATE"]
        if expr.or_replace:
            parts.append("OR REPLACE")
        parts.append("PROCEDURE")
        parts.append(self.format_identifier(expr.procedure_name))
        if expr.parameters:
            parts.append(f"({self._format_parameters(expr.parameters)})")
        parts.append(expr.keyword)
        parts.append(expr.body)
        return " ".join(parts), ()

    def format_create_function_statement(
        self, expr: "OracleCreateFunctionExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE FUNCTION",
                suggestion=(
                    f"Oracle {self.version} does not support stored "
                    "functions; they require Oracle 9i or later."
                ),
            )
        parts = ["CREATE"]
        if expr.or_replace:
            parts.append("OR REPLACE")
        parts.append("FUNCTION")
        parts.append(self.format_identifier(expr.function_name))
        if expr.parameters:
            parts.append(f"({self._format_parameters(expr.parameters)})")
        parts.append(expr.return_keyword)
        parts.append(self.format_identifier(expr.return_type))
        parts.append(expr.keyword)
        parts.append(expr.body)
        return " ".join(parts), ()

    def format_create_package_statement(
        self, expr: "OracleCreatePackageExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE PACKAGE",
                suggestion=(
                    f"Oracle {self.version} does not support packages; "
                    "they require Oracle 9i or later."
                ),
            )
        parts = ["CREATE"]
        if expr.or_replace:
            parts.append("OR REPLACE")
        parts.append("PACKAGE")
        parts.append(self.format_identifier(expr.package_name))
        parts.append(expr.keyword)
        parts.append(expr.body)
        return " ".join(parts), ()

    def format_create_package_body_statement(
        self, expr: "OracleCreatePackageBodyExpression"
    ) -> Tuple[str, tuple]:
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE PACKAGE BODY",
                suggestion=(
                    f"Oracle {self.version} does not support packages; "
                    "they require Oracle 9i or later."
                ),
            )
        parts = ["CREATE"]
        if expr.or_replace:
            parts.append("OR REPLACE")
        parts.append("PACKAGE BODY")
        parts.append(self.format_identifier(expr.package_name))
        parts.append(expr.keyword)
        parts.append(expr.body)
        return " ".join(parts), ()

    def format_drop_routine_statement(
        self, expr: "OracleDropRoutineExpression"
    ) -> Tuple[str, tuple]:
        feature = f"DROP {expr.object_type.value}"
        if self.version < (9, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                feature,
                suggestion=(
                    f"Oracle {self.version} does not support {feature}; "
                    "it requires Oracle 9i or later."
                ),
            )
        parts = ["DROP"]
        parts.append(expr.object_type.value)
        parts.append(self.format_identifier(expr.object_name))
        return " ".join(parts), ()
