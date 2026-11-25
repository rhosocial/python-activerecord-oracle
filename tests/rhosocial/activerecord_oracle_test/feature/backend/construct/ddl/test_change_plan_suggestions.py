# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_change_plan_suggestions.py
"""Offline contract tests for the four Oracle migration change-point suggestions.

No server is needed: the dialect's own formatters render each returned node, so
these tests check the tri-state value, that a supported answer is a real node
(not hand-built SQL), its exact SQL, and the documented reason behind every
``False`` answer.
"""

import dataclasses

import pytest
from rhosocial.activerecord.backend.expression.statements import (
    AlterTableStatement,
    CreateIndexStatement,
    DropIndexStatement,
    DropTableConstraintAction,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
    OracleCoalescePartitionExpression,
)
from rhosocial.activerecord.backend.impl.oracle.suggestion import (
    column_reorder_suggestion as _column_reorder_module,
)
from rhosocial.activerecord.backend.impl.oracle.suggestion import (
    partition_scheme_change_suggestion as _partition_scheme_module,
)
from rhosocial.activerecord.backend.impl.oracle.suggestion import (
    table_constraint_change_suggestion as _constraint_change_module,
)
from rhosocial.activerecord.backend.impl.oracle.suggestion import (
    table_index_change_suggestion as _index_change_module,
)
from rhosocial.activerecord.backend.suggestion import (
    ColumnReorderSuggestion,
    PartitionSchemeChangeSuggestion,
    TableConstraintChangeSuggestion,
    TableIndexChangeSuggestion,
)
from rhosocial.activerecord.backend.suggestion.protocols import (
    ColumnReorderSuggestionSupport,
    PartitionSchemeChangeSuggestionSupport,
    TableConstraintChangeSuggestionSupport,
    TableIndexChangeSuggestionSupport,
)


def _projection(**items):
    """Mimic core's ``_freeze`` ``("params", ((k, v), ...))`` projection."""
    return ("params", tuple(sorted(items.items())))


def _constraint(name=None, columns=("created_at",)):
    return _projection(
        branch=_projection(columns=columns),
        name=name,
        deferrable=False,
        not_deferrable=False,
        initially_deferred=False,
        initially_immediate=False,
        validation=None,
        enforced=False,
        not_enforced=False,
    )


def _index(name, columns=("created_at",), **options):
    items = dict(
        columns=columns,
        concurrent=None,
        if_exists=False,
        if_not_exists=False,
        include_columns=None,
        name=name,
        partial_condition=None,
        tablespace=None,
        type=None,
        unique=False,
    )
    items.update(options)
    return _projection(**items)


def _partition(method="HASH", count=None):
    key = _projection(
        alias=None,
        alias_need_quote=True,
        name="id",
        name_need_quote=True,
        schema_name=None,
        schema_need_quote=True,
        table=None,
        table_need_quote=True,
    )
    items = dict(method=method, keys=(key,))
    if count is not None:
        items["partitions_count"] = count
    return _projection(**items)


def _sql(operations):
    return " ".join(expr.to_sql()[0] for expr in operations)


def _reason(method, module):
    """Reasons are documented on the module and on the suggestion method."""
    return ((method.__doc__ or "") + (module.__doc__ or "")).lower()


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 1, 0))


PROTOCOLS = (
    ("suggest_column_reorder", ColumnReorderSuggestion, ColumnReorderSuggestionSupport),
    ("suggest_table_constraint_change", TableConstraintChangeSuggestion,
     TableConstraintChangeSuggestionSupport),
    ("suggest_table_index_change", TableIndexChangeSuggestion, TableIndexChangeSuggestionSupport),
    ("suggest_partition_scheme_change", PartitionSchemeChangeSuggestion,
     PartitionSchemeChangeSuggestionSupport),
)


def test_dialect_satisfies_all_four_protocols(dialect):
    for _method, _cls, protocol in PROTOCOLS:
        assert isinstance(dialect, protocol)


def test_answers_are_frozen_points_with_tri_state(dialect):
    points = (
        ColumnReorderSuggestion(table_name="events"),
        TableConstraintChangeSuggestion(table_name="events"),
        TableIndexChangeSuggestion(table_name="events"),
        PartitionSchemeChangeSuggestion(table_name="events"),
    )
    for method, cls, _protocol in PROTOCOLS:
        point = next(p for p in points if isinstance(p, cls))
        answer = getattr(dialect, method)(point)
        assert isinstance(answer, cls)
        assert dataclasses.is_dataclass(answer)
        assert answer.table_name == "events"
        assert answer.state in {"actions", "unaddressable", "absent"}
        with pytest.raises(dataclasses.FrozenInstanceError):
            answer.operations = False


# --- column_reorder -------------------------------------------------------


def test_column_reorder_is_always_false_with_rebuild_reason(dialect):
    point = ColumnReorderSuggestion(
        table_name="events",
        old_order=("id", "created_at"),
        new_order=("created_at", "id"),
        reordered_columns=("id", "created_at"),
    )
    answer = dialect.suggest_column_reorder(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"
    # The context is preserved verbatim while the answer is filled in.
    assert answer.old_order == point.old_order
    assert answer.new_order == point.new_order
    assert answer.reordered_columns == point.reordered_columns
    # The documented reason is shipped in source, not hand-waved.
    assert "rebuild" in _reason(dialect.suggest_column_reorder, _column_reorder_module)


# --- table_constraint_change ---------------------------------------------


def test_constraint_drop_returns_real_alter_table_statements(dialect):
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(_constraint("uq_created"), _constraint("uq_email")),
        new_constraints=(),
    )
    answer = dialect.suggest_table_constraint_change(point)
    assert answer.state == "actions"
    assert len(answer.operations) == 2
    assert all(isinstance(op, AlterTableStatement) for op in answer.operations)
    for op in answer.operations:
        assert len(op.actions) == 1
        assert isinstance(op.actions[0], DropTableConstraintAction)
    assert _sql(answer.operations) == (
        'ALTER TABLE "EVENTS" DROP CONSTRAINT "UQ_CREATED" '
        'ALTER TABLE "EVENTS" DROP CONSTRAINT "UQ_EMAIL"'
    )


def test_constraint_add_is_false_because_frozen_branch_kind_is_lost(dialect):
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(),
        new_constraints=(_constraint("uq_created"),),
    )
    answer = dialect.suggest_table_constraint_change(point)
    assert answer.operations is False
    assert "branch" in _reason(dialect.suggest_table_constraint_change, _constraint_change_module)


def test_constraint_mixed_add_and_drop_is_false(dialect):
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(_constraint("uq_old"),),
        new_constraints=(_constraint("uq_new"),),
    )
    assert dialect.suggest_table_constraint_change(point).operations is False


def test_constraint_unnamed_drop_and_no_name_are_false(dialect):
    unnamed = TableConstraintChangeSuggestion(
        table_name="events", old_constraints=(_constraint(None),), new_constraints=()
    )
    assert dialect.suggest_table_constraint_change(unnamed).operations is False
    no_table = TableConstraintChangeSuggestion(
        table_name="", old_constraints=(_constraint("uq_created"),), new_constraints=()
    )
    assert dialect.suggest_table_constraint_change(no_table).operations is False


def test_constraint_no_change_is_absent(dialect):
    same = _constraint("uq_created")
    point = TableConstraintChangeSuggestion(
        table_name="events", old_constraints=(same,), new_constraints=(same,)
    )
    assert dialect.suggest_table_constraint_change(point).operations is None


# --- table_index_change ---------------------------------------------------


def test_index_create_returns_real_create_index_statement(dialect):
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(),
        new_indexes=(_index("idx_created", columns=("created_at", "id"), unique=True),),
    )
    answer = dialect.suggest_table_index_change(point)
    assert answer.state == "actions"
    assert len(answer.operations) == 1
    assert isinstance(answer.operations[0], CreateIndexStatement)
    assert _sql(answer.operations) == (
        'CREATE UNIQUE INDEX "IDX_CREATED" ON "EVENTS" ("CREATED_AT", "ID")'
    )


def test_index_drop_returns_real_drop_index_statement(dialect):
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(_index("idx_created"),),
        new_indexes=(),
    )
    answer = dialect.suggest_table_index_change(point)
    assert len(answer.operations) == 1
    assert isinstance(answer.operations[0], DropIndexStatement)
    assert answer.operations[0].index.name == "idx_created"
    assert _sql(answer.operations) == 'DROP INDEX "IDX_CREATED"'


def test_index_replace_drops_before_creating(dialect):
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(_index("idx_created"),),
        new_indexes=(_index("idx_created", columns=("id",)),),
    )
    answer = dialect.suggest_table_index_change(point)
    assert isinstance(answer.operations[0], DropIndexStatement)
    assert isinstance(answer.operations[1], CreateIndexStatement)
    assert _sql(answer.operations) == (
        'DROP INDEX "IDX_CREATED" CREATE INDEX "IDX_CREATED" ON "EVENTS" ("ID")'
    )


def test_index_no_change_is_absent(dialect):
    same = _index("idx_created")
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(same,), new_indexes=(same,)
    )
    assert dialect.suggest_table_index_change(point).operations is None


@pytest.mark.parametrize(
    "options",
    (
        {"type": "BTREE"},
        {"include_columns": ["id"]},
        {"partial_condition": "c"},
        {"tablespace": "ts"},
        {"concurrent": True},
        {"columns": (("--params", ()),)},  # functional column cannot be rebuilt
    ),
)
def test_index_unspellable_options_are_false_with_reasons(dialect, options):
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(), new_indexes=(_index("idx_created", **options),)
    )
    assert dialect.suggest_table_index_change(point).operations is False
    reason = _reason(dialect.suggest_table_index_change, _index_change_module)
    assert "create index" in reason and "grammar" in reason


def test_index_if_not_exists_gate_follows_two_anchors():
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(),
        new_indexes=(_index("idx_created", if_not_exists=True),),
    )
    # 23ai: native IF NOT EXISTS is in the base release.
    assert _sql(OracleDialect(version=(23, 1, 0)).suggest_table_index_change(point).operations) == (
        'CREATE INDEX IF NOT EXISTS "IDX_CREATED" ON "EVENTS" ("CREATED_AT")'
    )
    # Below the two anchors the clause is ORA-00922, so the answer is False.
    for version in ((19, 27, 0), (21, 3, 0)):
        assert OracleDialect(version=version).suggest_table_index_change(point).operations is False
    # 19c is answered only at RU 28 or later (VERSION_FULL / ru_version).
    assert (
        OracleDialect(version=(19, 0, 0), version_full=(19, 27, 0, 0, 0))
        .suggest_table_index_change(point)
        .operations
        is False
    )
    assert _sql(
        OracleDialect(version=(19, 0, 0), version_full=(19, 28, 0, 0, 0))
        .suggest_table_index_change(point)
        .operations
    ) == 'CREATE INDEX IF NOT EXISTS "IDX_CREATED" ON "EVENTS" ("CREATED_AT")'


def test_index_drop_if_exists_gate_follows_two_anchors():
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(_index("idx_created", if_exists=True),),
        new_indexes=(),
    )
    assert _sql(OracleDialect(version=(23, 1, 0)).suggest_table_index_change(point).operations) == (
        'DROP INDEX IF EXISTS "IDX_CREATED"'
    )
    assert OracleDialect(version=(21, 3, 0)).suggest_table_index_change(point).operations is False
    assert (
        OracleDialect(version=(19, 0, 0), version_full=(19, 27, 0, 0, 0))
        .suggest_table_index_change(point)
        .operations
        is False
    )
    assert _sql(
        OracleDialect(version=(19, 0, 0), version_full=(19, 28, 0, 0, 0))
        .suggest_table_index_change(point)
        .operations
    ) == 'DROP INDEX IF EXISTS "IDX_CREATED"'


def test_index_empty_table_and_bad_projection_are_false(dialect):
    no_table = TableIndexChangeSuggestion(
        table_name="", old_indexes=(), new_indexes=(_index("idx_created"),)
    )
    assert dialect.suggest_table_index_change(no_table).operations is False
    malformed = TableIndexChangeSuggestion(
        table_name="events", old_indexes=("not-a-projection",), new_indexes=()
    )
    assert dialect.suggest_table_index_change(malformed).operations is False


# --- partition_scheme_change ---------------------------------------------


def test_partition_scheme_no_change_is_absent(dialect):
    same = _partition("HASH", count=4)
    point = PartitionSchemeChangeSuggestion(
        table_name="events", old_partition=same, new_partition=same
    )
    assert dialect.suggest_partition_scheme_change(point).operations is None
    neither = PartitionSchemeChangeSuggestion(
        table_name="events", old_partition=None, new_partition=None
    )
    assert dialect.suggest_partition_scheme_change(neither).operations is None


def test_partition_hash_shrink_by_one_reuses_coalesce(dialect):
    point = PartitionSchemeChangeSuggestion(
        table_name="events",
        old_partition=_partition("HASH", count=4),
        new_partition=_partition("HASH", count=3),
    )
    answer = dialect.suggest_partition_scheme_change(point)
    assert answer.state == "actions"
    assert len(answer.operations) == 1
    assert isinstance(answer.operations[0], OracleCoalescePartitionExpression)
    assert _sql(answer.operations) == 'ALTER TABLE "EVENTS" COALESCE PARTITION'


@pytest.mark.parametrize(
    "old,new",
    (
        (_partition("HASH", count=4), _partition("HASH", count=5)),  # grow
        (_partition("HASH", count=4), _partition("HASH", count=2)),  # shrink by two
        (_partition("HASH", count=4), _partition("RANGE", count=4)),  # method change
        (_partition("RANGE"), _partition("RANGE", count=3)),  # non-HASH change
        (None, _partition("HASH", count=4)),  # add partitioning
        (_partition("HASH", count=4), None),  # drop partitioning
    ),
)
def test_partition_scheme_unsupported_changes_are_false(dialect, old, new):
    point = PartitionSchemeChangeSuggestion(
        table_name="events", old_partition=old, new_partition=new
    )
    assert dialect.suggest_partition_scheme_change(point).operations is False
    reason = _reason(dialect.suggest_partition_scheme_change, _partition_scheme_module)
    assert "coalesce" in reason and "count-less" in reason and "hash-only" in reason
