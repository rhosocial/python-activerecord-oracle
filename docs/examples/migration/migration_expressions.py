# docs/examples/migration/migration_expressions.py
"""Version-aware DDL expressions used by the class-style migration demo.

Each function receives the (adapted) dialect and returns a statement object.
No SQL string is assembled here: the Oracle dialect chooses the right spelling
for the connected server version when ``expr.to_sql()`` is called.
"""

from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.suggestion import IntegerAutoIncrementSuggestionRenderer
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    ColumnConstraint,
    ColumnDefinition,
    ColumnPrimaryKeyConstraint,
    CreateTableStatement,
    DropTableStatement,
)
from rhosocial.activerecord.backend.expression.types import IntegerType, VarCharType


def create_audit_table(dialect):
    """CREATE TABLE audit_log (id ... IDENTITY, actor VARCHAR2, action VARCHAR2)."""
    id_type, id_attributes = IntegerAutoIncrementSuggestionRenderer().parts(dialect, IntegerType(dialect))
    return CreateTableStatement(
        dialect,
        table=Table(dialect, "audit_log"),
        columns=[
            ColumnDefinition(
                dialect,
                "id",
                id_type,
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))
                ],
                attributes=id_attributes,
            ),
            ColumnDefinition(dialect, "actor", VarCharType(length=64, dialect=dialect)),
            ColumnDefinition(dialect, "action", VarCharType(length=128, dialect=dialect)),
        ],
    )


def drop_audit_table(dialect):
    """DROP TABLE IF EXISTS audit_log (gated by the server version)."""
    return DropTableStatement(dialect, table=Table(dialect, "audit_log"), if_exists=True)
