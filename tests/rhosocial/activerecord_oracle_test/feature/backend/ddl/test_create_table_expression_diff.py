# tests/rhosocial/activerecord_oracle_test/feature/backend/ddl/test_create_table_expression_diff.py
"""CreateTableExpression.diff() coverage for the Oracle dialect.

Verified dialect behaviour underpinning the capability hooks:

- In-place column type change: Oracle redefines a column via
  ``ALTER TABLE t MODIFY (col TYPE [constraints])``. The core
  ``ModifyColumn`` action already dispatches to
  ``OracleModifyColumnMixin.format_modify_column_action``
  → ``_supports_alter_column_type()`` overridden to True and
  ``alter_column_type_action()`` returns a core ``ModifyColumn``.
- Property changes (SET/DROP DEFAULT, SET/DROP NOT NULL): Oracle has no
  standalone ``ALTER COLUMN SET DEFAULT`` syntax; defaults and nullability
  are merged into the ``MODIFY`` clause. The generic diff property
  operations render ``ALTER COLUMN ...``, which Oracle rejects
  → ``_supports_alter_column_properties()`` overridden to False
  (property-only changes rebuild).
- Index changes: Oracle has no ``ALTER TABLE ADD/DROP INDEX``
  → ``_supports_alter_table_index_actions()`` overridden to False
  (index changes rebuild).
"""

import pytest

from rhosocial.activerecord.backend.dialect.protocols import CreateTableExpressionDiffSupport
from rhosocial.activerecord.backend.expression import DiffPlan
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AddColumn,
    AlterTableExpression,
    DropColumn,
    ModifyColumn,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    ColumnConstraint,
    ColumnConstraintType,
    ColumnDefinition,
    CreateTableExpression,
    IndexDefinition,
    TableConstraint,
    TableConstraintType,
)
from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    IntegerType,
    TextType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


def _col(name, dtype, *constraints):
    return ColumnDefinition(name=name, data_type=dtype, constraints=list(constraints))


def _pk():
    return ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)


def _not_null():
    return ColumnConstraint(constraint_type=ColumnConstraintType.NOT_NULL)


def _default(value):
    return ColumnConstraint(
        constraint_type=ColumnConstraintType.DEFAULT, default_value=value
    )


def _expr(dialect, columns, indexes=None, constraints=None, **kwargs):
    return CreateTableExpression(
        dialect=dialect,
        table=kwargs.pop("table", "items"),
        columns=columns,
        indexes=indexes,
        table_constraints=constraints,
        **kwargs,
    )


@pytest.fixture(scope="module")
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestProtocolConformance:

    def test_oracle_dialect_satisfies_protocol(self, dialect):
        assert isinstance(dialect, CreateTableExpressionDiffSupport)

    def test_capability_hooks(self, dialect):
        """The hooks match Oracle reality: native in-place type change via
        MODIFY, no property subclauses, no ALTER TABLE index actions."""
        assert dialect._supports_alter_column_type() is True
        assert dialect._supports_alter_column_properties() is False
        assert dialect._supports_alter_table_index_actions() is False


class TestValidation:

    def test_cross_dialect_raises(self, dialect):
        from rhosocial.activerecord.backend.impl.sqlite.dialect import SQLiteDialect

        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(SQLiteDialect(), [_col("id", IntegerType(), _pk())])
        with pytest.raises(ValueError, match="different dialects"):
            old.diff(new)

    def test_cross_table_raises(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk())], table="other")
        with pytest.raises(ValueError, match="different tables"):
            old.diff(new)


class TestNoChange:

    def test_identical_definitions_empty_plan(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("name", VarCharType(length=100))])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("name", VarCharType(length=100))])
        plan = old.diff(new)
        assert not plan.has_changes
        assert plan.rebuild is None
        assert plan.alters == []


class TestColumnChanges:

    def test_added_column_yields_add_action(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("bio", VarCharType(length=1000))])
        plan = old.diff(new)
        assert plan.rebuild is None and plan.has_changes
        (alter,) = plan.alters
        assert len(alter.actions) == 1
        action = alter.actions[0]
        assert isinstance(action, AddColumn)
        assert action.column.name == "bio"

    def test_removed_column_yields_drop_action(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("bio", VarCharType(length=1000))])
        new = _expr(dialect, [_col("id", IntegerType(), _pk())])
        plan = old.diff(new)
        (alter,) = plan.alters
        action = alter.actions[0]
        assert isinstance(action, DropColumn)
        assert action.column_name == "bio"

    def test_add_action_renders_oracle_add(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("bio", VarCharType(length=100))])
        plan = old.diff(new)
        sql, _params = plan.alters[0].to_sql()
        assert sql.upper().startswith("ALTER TABLE ITEMS ADD")
        assert "BIO" in sql.upper()
        assert "VARCHAR2(100)" in sql.upper()


class TestTypeChangeRebuild:
    """Type changes stay in place on Oracle via MODIFY COLUMN (no rebuild)."""

    def test_type_change_yields_modify_column_action(self, dialect):
        """Native Oracle path: a type change produces a core ``ModifyColumn``
        action rendered as ``ALTER TABLE t MODIFY (col TYPE)`` instead of a
        rebuild plan."""
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", IntegerType())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", BigIntType())])
        plan = old.diff(new)
        assert plan.rebuild is None
        assert plan.has_changes
        (alter,) = plan.alters
        assert len(alter.actions) == 1
        action = alter.actions[0]
        assert isinstance(action, ModifyColumn)
        assert action.column.name == "code"
        assert action.column.data_type == BigIntType()

    def test_modify_column_renders_oracle_modify(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", IntegerType())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", VarCharType(length=50))])
        plan = old.diff(new)
        sql, _params = plan.alters[0].to_sql()
        assert sql.upper() == "ALTER TABLE ITEMS MODIFY (CODE VARCHAR2(50))"

    def test_length_change_also_in_place(self, dialect):
        """Oracle can widen a VARCHAR2 in place, so a length change follows
        the same MODIFY path (no rebuild)."""
        old = _expr(dialect, [_col("name", VarCharType(length=50))])
        new = _expr(dialect, [_col("name", VarCharType(length=100))])
        plan = old.diff(new)
        assert plan.rebuild is None
        (alter,) = plan.alters
        assert isinstance(alter.actions[0], ModifyColumn)

    def test_type_change_preserves_other_column_actions(self, dialect):
        """A type change on one column and an add on another land in the
        same alter statement in diff order."""
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", IntegerType())])
        new = _expr(
            dialect,
            [_col("id", IntegerType(), _pk()), _col("code", BigIntType()), _col("extra", TextType())],
        )
        plan = old.diff(new)
        assert plan.rebuild is None
        (alter,) = plan.alters
        assert [type(a).__name__ for a in alter.actions] == ["ModifyColumn", "AddColumn"]


class TestColumnPropertyChanges:
    """Property changes have no standalone subclause on Oracle → rebuild."""

    def test_set_default(self, dialect):
        """No standalone ALTER COLUMN SET DEFAULT on Oracle → rebuild."""
        old = _expr(dialect, [_col("status", VarCharType(length=20))])
        new = _expr(dialect, [_col("status", VarCharType(length=20), _default("ok"))])
        plan = old.diff(new)
        assert plan.alters == []
        assert plan.rebuild is not None
        assert "not supported in-place" in plan.rebuild.reason

    def test_drop_default(self, dialect):
        old = _expr(dialect, [_col("status", VarCharType(length=20), _default("ok"))])
        new = _expr(dialect, [_col("status", VarCharType(length=20))])
        assert old.diff(new).rebuild is not None

    def test_set_not_null(self, dialect):
        old = _expr(dialect, [_col("name", VarCharType(length=100))])
        new = _expr(dialect, [_col("name", VarCharType(length=100), _not_null())])
        assert old.diff(new).rebuild is not None

    def test_drop_not_null(self, dialect):
        old = _expr(dialect, [_col("name", VarCharType(length=100), _not_null())])
        new = _expr(dialect, [_col("name", VarCharType(length=100))])
        assert old.diff(new).rebuild is not None


class TestIndexChanges:
    """No ALTER TABLE ADD/DROP INDEX on Oracle → index changes rebuild."""

    def test_added_index(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk())],
                    indexes=[IndexDefinition(name="idx_code", columns=["id"])])
        plan = old.diff(new)
        assert plan.alters == []
        assert plan.rebuild is not None
        assert "index change" in plan.rebuild.reason

    def test_removed_index(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())],
                    indexes=[IndexDefinition(name="idx_id", columns=["id"])])
        new = _expr(dialect, [_col("id", IntegerType(), _pk())])
        assert old.diff(new).rebuild is not None

    def test_rebuild_plan_carries_new_indexes(self, dialect):
        """The rebuild's CREATE TABLE carries the new index set."""
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk())],
                    indexes=[IndexDefinition(name="idx_id", columns=["id"])])
        rp = old.diff(new).rebuild
        assert [i.name for i in rp.create.indexes] == ["idx_id"]


class TestTableConstraintChanges:

    def test_pk_change_rebuilds(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType()), _col("code", VarCharType(length=50))],
                    constraints=[TableConstraint(
                        constraint_type=TableConstraintType.PRIMARY_KEY, columns=["id"])])
        new = _expr(dialect, [_col("id", IntegerType()), _col("code", VarCharType(length=50))],
                    constraints=[TableConstraint(
                        constraint_type=TableConstraintType.PRIMARY_KEY, columns=["code"])])
        plan = old.diff(new)
        assert plan.rebuild is not None
        assert "primary key" in plan.rebuild.reason

    def test_named_unique_constraint_add(self, dialect):
        """Named non-PK table constraints stay on the alter path."""
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("email", VarCharType(length=255))])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("email", VarCharType(length=255))],
                    constraints=[TableConstraint(
                        constraint_type=TableConstraintType.UNIQUE,
                        name="uq_email", columns=["email"])])
        plan = old.diff(new)
        assert plan.rebuild is None
        (alter,) = plan.alters
        assert type(alter.actions[0]).__name__ == "AddTableConstraint"


class TestDiffPlanInvariants:

    def test_alters_and_rebuild_mutually_exclusive(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("x", TextType())])
        plan = old.diff(new)
        assert plan.rebuild is None and plan.alters
        old2 = _expr(dialect, [_col("name", VarCharType(length=100))])
        new2 = _expr(dialect, [_col("name", VarCharType(length=100), _not_null())])
        plan2 = old2.diff(new2)
        assert plan2.rebuild is not None and plan2.alters == []

    def test_plan_rejects_both_fields(self, dialect):
        old = _expr(dialect, [_col("status", VarCharType(length=20))])
        new = _expr(dialect, [_col("status", VarCharType(length=20), _default("ok"))])
        rp = old.diff(new).rebuild
        assert rp is not None
        alter = AlterTableExpression(dialect, table="t", actions=[])
        with pytest.raises(ValueError, match="mutually exclusive"):
            DiffPlan(alters=[alter], rebuild=rp)
