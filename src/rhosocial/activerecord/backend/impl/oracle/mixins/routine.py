# src/rhosocial/activerecord/backend/impl/oracle/mixins/routine.py
"""Oracle PL/SQL routine and package DDL formatter mixin."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

from ..expression.objects import OraclePackage

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

    def format_package_object(self, expr: "OraclePackage") -> Tuple[str, tuple]:
        """Render a PL/SQL package name.

        A package is a container of routines rather than a callable one, so the
        shared object tree gives it no kind; Oracle declares
        :class:`~...impl.oracle.expression.objects.OraclePackage` instead. The
        spelling is the same shape as every other name, so it asks the same two
        questions rather than repeating the spelling: whether the owner the
        package carries is one this dialect can express, and what the qualified
        name looks like.

        Args:
            expr: The :class:`OraclePackage` being named.

        Returns:
            A ``(sql, params)`` tuple; ``params`` is empty, because an
            identifier is never a bind parameter.

        Raises:
            TypeError: ``expr`` is not an :class:`OraclePackage`.
            UnsupportedFeatureError: ``expr`` carries a ``catalog_name``.
                Oracle has no namespace above the owner, so an object that names
                one is reported rather than having that slot silently dropped.
        """
        if not isinstance(expr, OraclePackage):
            raise TypeError(
                f"format_package_object expects an OraclePackage, "
                f"got {type(expr).__name__}"
            )
        self.validate_namespace(expr)
        return self.format_qualified_name(expr)

    def format_parameters(self, parameters: list) -> Tuple[str, tuple]:
        """Render formal parameters as a comma-separated declaration list."""
        rendered = []
        for param in parameters:
            parts = [self.format_identifier(param.name)]
            if param.mode is not None:
                parts.append(param.mode.value)
            # A data type is not an identifier: upper-case it without quoting.
            parts.append(param.data_type.upper())
            rendered.append(" ".join(parts))
        return ", ".join(rendered), ()

    def format_create_procedure_statement(
        self, expr: "OracleCreateProcedureExpression"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE [OR REPLACE] PROCEDURE`` for Oracle.

        Raises:
            TypeError: ``expr.procedure`` is not a ``Procedure``. A function
                would render its own name, so the statement would create a
                function and name a procedure.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        from rhosocial.activerecord.backend.expression.objects import Procedure

        if not isinstance(expr.procedure, Procedure):
            raise TypeError(
                f"OracleCreateProcedureExpression.procedure must be a Procedure, "
                f"got {type(expr.procedure).__name__}"
            )

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
        parts.append(expr.procedure.to_sql()[0])
        if expr.parameters:
            parts.append(f"({self.format_parameters(expr.parameters)[0]})")
        parts.append(expr.keyword)
        parts.append(expr.body)
        return " ".join(parts), ()

    def format_create_function_statement(
        self, expr: "OracleCreateFunctionExpression"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE [OR REPLACE] FUNCTION`` for Oracle.

        Raises:
            TypeError: ``expr.function`` is not a ``Function``. A procedure
                would render its own name, so the statement would create a
                procedure and name a function.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        from rhosocial.activerecord.backend.expression.objects import Function

        if not isinstance(expr.function, Function):
            raise TypeError(
                f"OracleCreateFunctionExpression.function must be a Function, "
                f"got {type(expr.function).__name__}"
            )

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
        parts.append(expr.function.to_sql()[0])
        if expr.parameters:
            parts.append(f"({self.format_parameters(expr.parameters)[0]})")
        parts.append(expr.return_keyword)
        # A data type is not an identifier: upper-case it without quoting.
        parts.append(expr.return_type.upper())
        parts.append(expr.keyword)
        parts.append(expr.body)
        return " ".join(parts), ()

    def format_create_package_statement(
        self, expr: "OracleCreatePackageExpression"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE [OR REPLACE] PACKAGE`` for Oracle.

        Raises:
            TypeError: ``expr.package`` is not an ``OraclePackage``. Any other
                catalogue object renders its own name, so the statement would
                declare something else and name a package.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        if not isinstance(expr.package, OraclePackage):
            raise TypeError(
                f"OracleCreatePackageExpression.package must be an OraclePackage, "
                f"got {type(expr.package).__name__}"
            )

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
        parts.append(expr.package.to_sql()[0])
        parts.append(expr.keyword)
        parts.append(expr.body)
        return " ".join(parts), ()

    def format_create_package_body_statement(
        self, expr: "OracleCreatePackageBodyExpression"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE [OR REPLACE] PACKAGE BODY`` for Oracle.

        Raises:
            TypeError: ``expr.package`` is not an ``OraclePackage``.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        if not isinstance(expr.package, OraclePackage):
            raise TypeError(
                f"OracleCreatePackageBodyExpression.package must be an OraclePackage, "
                f"got {type(expr.package).__name__}"
            )

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
        parts.append(expr.package.to_sql()[0])
        parts.append(expr.keyword)
        parts.append(expr.body)
        return " ".join(parts), ()

    def format_drop_routine_statement(
        self, expr: "OracleDropRoutineExpression"
    ) -> Tuple[str, tuple]:
        """Format ``DROP <ROUTINE KIND>`` for Oracle.

        Raises:
            TypeError: ``expr.routine`` is not a ``RoutineObject``. A table
                renders its own name, so the statement would drop a table and
                name a routine -- or, for a PACKAGE, name something that has no
                kind of its own here.
            UnsupportedFeatureError: The Oracle version is below 9i.
        """
        from rhosocial.activerecord.backend.expression.objects import RoutineObject

        if not isinstance(expr.routine, RoutineObject):
            raise TypeError(
                f"OracleDropRoutineExpression.routine must be a RoutineObject, "
                f"got {type(expr.routine).__name__}"
            )

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
        parts.append(expr.routine.to_sql()[0])
        return " ".join(parts), ()
