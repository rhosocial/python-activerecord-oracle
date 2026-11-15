# tests/rhosocial/activerecord_oracle_test/feature/backend/adapters/native/_scratch_render.py
"""Scratch: verify the planned definition classes render the old SQL."""

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.expression import (
    ColumnConstraint as Constraint,
    ColumnDefinition as Column,
    CreateTableStatement as CreateTable,
    DropTableStatement as DropTable,
)
from rhocial.activerecord.backend.expression.objects import Table