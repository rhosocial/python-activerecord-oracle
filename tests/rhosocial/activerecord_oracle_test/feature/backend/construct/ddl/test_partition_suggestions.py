# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_partition_suggestions.py
"""Offline contract tests for the 8 Oracle partition suggestion methods.

No server is needed: the dialect's own formatters render each suggestion's
expression chain, so these tests verify both the tri-state shape and that the
returned nodes are real Oracle statements/clauses rather than hand-built SQL.
The final test blocks the retired maintenance modules in ``sys.modules`` to
prove the suggestion mixin is self-contained.
"""

import dataclasses
import importlib
import sys
from datetime import date

import pytest
from rhosocial.activerecord.backend.dialect.protocols import (
    PartitionAddSuggestionSupport,
    PartitionBuildSuggestionSupport,
    PartitionDropSuggestionSupport,
    PartitionMergeSuggestionSupport,
    PartitionRetentionSuggestionSupport,
    PartitionSplitSuggestionSupport,
    PartitionTableAlterSuggestionSupport,
    PartitionTableDropSuggestionSupport,
)
from rhosocial.activerecord.backend.expression import Column, DropTableStatement
from rhosocial.activerecord.backend.expression.statements import (
    PartitionDefinitionExpression,
)
from rhosocial.activerecord.backend.expression.operators import RawSQL
from rhosocial.activerecord.backend.suggestion import (
    PartitionAddSuggestion,
    PartitionBuildSuggestion,
    PartitionDropSuggestion,
    PartitionMergeSuggestion,
    PartitionRetentionSuggestion,
    PartitionSplitSuggestion,
    PartitionTableAlterSuggestion,
    PartitionTableDropSuggestion,
)

from rhosocial.activerecord.backend.expression.statements import (
    PartitionByHashClause,
    PartitionByListClause,
    PartitionByRangeClause,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OracleIntervalPartitionClause,
    OraclePartitionByHash,
    OraclePartitionByList,
    OraclePartitionByRange,
    OraclePartitionMaxValue,
    OraclePartitionValue,
    OracleReferencePartitionClause,
)
from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
    OracleAddPartitionStatement,
    OracleDropPartitionStatement,
    OracleMergePartitionsStatement,
    OracleSplitPartitionStatement,
)
from rhosocial.activerecord.backend.impl.oracle.partition import (
    OraclePartitionPlan,
    OracleRangePolicy,
)


def _strategy(dialect, word):
    """Build a strategy clause for a suggestion call.

    Core has dedicated clause nodes for RANGE / LIST / HASH, which is what
    the suggestion protocol is annotated with. INTERVAL and REFERENCE are
    Oracle-only words with no core clause, so their plain word is passed
    (the suggestion runtime still accepts a word alongside a clause).
    """
    keys = [Column(dialect, "ID")]
    return {
        "RANGE": PartitionByRangeClause(dialect, keys),
        "LIST": PartitionByListClause(dialect, keys),
        "HASH": PartitionByHashClause(dialect, keys),
    }.get(word, word)


_ALL_STRATEGIES = ("RANGE", "LIST", "HASH", "INTERVAL", "REFERENCE")

PROTOCOLS = (
    ("build", PartitionBuildSuggestionSupport, PartitionBuildSuggestion),
    ("add", PartitionAddSuggestionSupport, PartitionAddSuggestion),
    ("drop", PartitionDropSuggestionSupport, PartitionDropSuggestion),
    ("retention", PartitionRetentionSuggestionSupport, PartitionRetentionSuggestion),
    ("table_drop", PartitionTableDropSuggestionSupport, PartitionTableDropSuggestion),
    ("table_alter", PartitionTableAlterSuggestionSupport, PartitionTableAlterSuggestion),
    ("split", PartitionSplitSuggestionSupport, PartitionSplitSuggestion),
    ("merge", PartitionMergeSuggestionSupport, PartitionMergeSuggestion),
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 1, 0))


def _range(dialect, name, bound):
    return PartitionDefinitionExpression(
        dialect, name=name, less_than=[OraclePartitionValue(dialect, bound)]
    )


def _list(dialect, name, value):
    return PartitionDefinitionExpression(
        dialect, name=name, in_values=[OraclePartitionValue(dialect, value)]
    )


def _maxvalue(dialect, name):
    return PartitionDefinitionExpression(
        dialect, name=name, less_than=[OraclePartitionMaxValue(dialect)]
    )


def _sql(operations):
    return " ".join(expr.to_sql()[0] for expr in operations)


def test_dialect_satisfies_all_eight_protocols(dialect):
    for _name, protocol, _dataclass in PROTOCOLS:
        assert isinstance(dialect, protocol)


def test_suggestion_values_are_frozen_dataclasses(dialect):
    for name, _protocol, cls in PROTOCOLS:
        suggestion = getattr(dialect, f"suggest_partition_{name}")(
            _strategy(dialect, "RANGE"), None
        )
        assert isinstance(suggestion, cls)
        assert dataclasses.is_dataclass(suggestion)
        with pytest.raises(dataclasses.FrozenInstanceError):
            suggestion.operations = False


def test_build_returns_real_partition_by_clauses(dialect):
    definitions = [_range(dialect, "P1", 100), _range(dialect, "P2", 200)]
    cases = (
        (_strategy(dialect, "RANGE"), {"definitions": definitions}, OraclePartitionByRange),
        (
            _strategy(dialect, "LIST"),
            {"definitions": [_list(dialect, "P1", "a"), _list(dialect, "P2", "b")]},
            OraclePartitionByList,
        ),
        (_strategy(dialect, "HASH"), {"partitions_count": 4}, OraclePartitionByHash),
    )
    for strategy, extra, clause_class in cases:
        plan = {"keys": [Column(dialect, "ID")], **extra}
        suggestion = dialect.suggest_partition_build(strategy, plan)
        assert len(suggestion.operations) == 1
        assert isinstance(suggestion.operations[0], clause_class)
        assert "PARTITION BY" in _sql(suggestion.operations)

    interval = dialect.suggest_partition_build(
        _strategy(dialect, "INTERVAL"),
        {
            "keys": [Column(dialect, "CREATED_AT")],
            "interval": RawSQL(dialect, "NUMTOYMINTERVAL(1, 'MONTH')"),
            "definitions": [_range(dialect, "P1", 100)],
        },
    )
    assert isinstance(interval.operations[0], OracleIntervalPartitionClause)
    assert "INTERVAL (" in _sql(interval.operations)

    reference = dialect.suggest_partition_build(
        _strategy(dialect, "REFERENCE"), {"fk_constraint": "FK_PARENT"}
    )
    assert isinstance(reference.operations[0], OracleReferencePartitionClause)
    assert "PARTITION BY REFERENCE" in _sql(reference.operations)


def test_build_declines_unknown_or_incomplete_plan(dialect):
    assert dialect.suggest_partition_build("SPATIAL", {}).operations is False
    assert dialect.suggest_partition_build(_strategy(dialect, "RANGE"), {}).operations is False
    assert (
        dialect.suggest_partition_build(
            _strategy(dialect, "INTERVAL"), {"keys": [Column(dialect, "C")]}
        ).operations
        is False
    )
    assert dialect.suggest_partition_build(_strategy(dialect, "REFERENCE"), {}).operations is False


def test_table_drop_is_drop_table_statement(dialect):
    suggestion = dialect.suggest_partition_table_drop(_strategy(dialect, "RANGE"), {"table": "T"})
    assert len(suggestion.operations) == 1
    assert isinstance(suggestion.operations[0], DropTableStatement)
    assert _sql(suggestion.operations).upper().startswith("DROP TABLE")
    assert dialect.suggest_partition_table_drop(_strategy(dialect, "RANGE"), {}).operations is False


def test_table_alter_reports_no_mechanism(dialect):
    for strategy in _ALL_STRATEGIES:
        assert dialect.suggest_partition_table_alter(strategy, {"table": "T"}).operations is False


def test_add_returns_one_add_partition_per_definition(dialect):
    suggestion = dialect.suggest_partition_add(
        _strategy(dialect, "RANGE"),
        {"table": "T", "definitions": [_range(dialect, "P1", 100), _range(dialect, "P2", 200)]},
    )
    assert len(suggestion.operations) == 2
    assert all(isinstance(op, OracleAddPartitionStatement) for op in suggestion.operations)
    assert "ADD PARTITION" in _sql(suggestion.operations)
    assert (
        dialect.suggest_partition_add(_strategy(dialect, "RANGE"), {"table": "T"}).operations
        is False
    )
    assert (
        dialect.suggest_partition_add(
            _strategy(dialect, "REFERENCE"),
            {"table": "T", "definitions": [_range(dialect, "P1", 100)]},
        ).operations
        is False
    )


def test_drop_by_name_and_tri_state(dialect):
    suggestion = dialect.suggest_partition_drop(
        _strategy(dialect, "RANGE"), {"table": "T", "names": ["P0", "P1"]}
    )
    assert all(isinstance(op, OracleDropPartitionStatement) for op in suggestion.operations)
    assert "DROP PARTITION" in _sql(suggestion.operations)
    assert (
        dialect.suggest_partition_drop(_strategy(dialect, "RANGE"), {"table": "T"}).operations
        is None
    )
    assert (
        dialect.suggest_partition_drop(
            _strategy(dialect, "REFERENCE"), {"table": "T", "names": ["P0"]}
        ).operations
        is False
    )


def test_split_returns_a_real_split_statement(dialect):
    suggestion = dialect.suggest_partition_split(
        _strategy(dialect, "RANGE"),
        {
            "table": "T",
            "partition": "P0",
            "at_values": [OraclePartitionValue(dialect, 150)],
            "into": [_range(dialect, "P0", 150), _range(dialect, "P1", 200)],
        },
    )
    assert len(suggestion.operations) == 1
    assert isinstance(suggestion.operations[0], OracleSplitPartitionStatement)
    assert "SPLIT PARTITION" in _sql(suggestion.operations)
    assert (
        dialect.suggest_partition_split(_strategy(dialect, "RANGE"), {"table": "T"}).operations
        is False
    )


def test_merge_returns_a_real_merge_statement(dialect):
    suggestion = dialect.suggest_partition_merge(
        _strategy(dialect, "RANGE"),
        {
            "table": "T",
            "names": ["P0", "P1"],
            "into": [_range(dialect, "P01", 200)],
        },
    )
    assert len(suggestion.operations) == 1
    assert isinstance(suggestion.operations[0], OracleMergePartitionsStatement)
    assert "MERGE PARTITIONS" in _sql(suggestion.operations)
    assert (
        dialect.suggest_partition_merge(_strategy(dialect, "RANGE"), {"table": "T"}).operations
        is False
    )


def test_retention_drops_old_partitions_but_never_maxvalue(dialect):
    definitions = [
        _range(dialect, "P0", date(2026, 1, 1)),
        _range(dialect, "P1", date(2026, 2, 1)),
        _maxvalue(dialect, "PMAX"),
    ]
    suggestion = dialect.suggest_partition_retention(
        _strategy(dialect, "RANGE"),
        {"table": "T", "definitions": definitions, "cutoff": date(2026, 2, 1)},
    )
    assert all(isinstance(op, OracleDropPartitionStatement) for op in suggestion.operations)
    assert [op.partition_name for op in suggestion.operations] == ["P0", "P1"]
    assert "PMAX" not in _sql(suggestion.operations)

    nothing = dialect.suggest_partition_retention(
        _strategy(dialect, "RANGE"),
        {"table": "T", "definitions": definitions, "cutoff": date(2025, 1, 1)},
    )
    assert nothing.operations is None
    assert (
        dialect.suggest_partition_retention(_strategy(dialect, "LIST"), {"table": "T"}).operations
        is False
    )


def test_retention_compares_high_value_of_a_real_plan(dialect):
    plan = OraclePartitionPlan(
        parent="EVENTS",
        method="RANGE",
        columns="CREATED_AT",
        policy=OracleRangePolicy(
            bounds=(
                (date(2026, 1, 1), date(2026, 2, 1)),
                (date(2026, 2, 1), date(2026, 3, 1)),
            ),
            maxvalue=True,
        ),
    )
    suggestion = dialect.suggest_partition_retention(
        _strategy(dialect, "RANGE"),
        {"table": "EVENTS", "plan": plan, "cutoff": date(2026, 2, 1)},
    )
    names = [op.partition_name for op in suggestion.operations]
    assert "EVENTS_2026_01_01" in names
    assert "EVENTS_maxvalue" not in names
    assert all("maxvalue" not in name.lower() for name in names)


def test_suggestions_work_with_maintenance_modules_blocked(dialect, monkeypatch):
    """The mixin must not depend on the retired maintenance/report modules."""
    module_names = (
        "rhosocial.activerecord.backend.impl.oracle.partition.maintenance",
        "rhosocial.activerecord.backend.impl.oracle.partition.report",
    )
    for name in module_names:
        monkeypatch.setitem(sys.modules, name, None)

    package = importlib.import_module("rhosocial.activerecord.backend.impl.oracle.suggestion")
    importlib.reload(package)
    for name in (
        "_common",
        "partition_add_suggestion",
        "partition_build_suggestion",
        "partition_drop_suggestion",
        "partition_merge_suggestion",
        "partition_retention_suggestion",
        "partition_split_suggestion",
        "partition_table_alter_suggestion",
        "partition_table_drop_suggestion",
    ):
        module = importlib.import_module(f"{package.__name__}.{name}")
        importlib.reload(module)
    assert dialect.suggest_partition_build(
        _strategy(dialect, "RANGE"),
        {"keys": [Column(dialect, "ID")], "definitions": [_range(dialect, "P1", 1)]},
    ).operations
    assert dialect.suggest_partition_add(
        _strategy(dialect, "RANGE"), {"table": "T", "definitions": [_range(dialect, "P1", 1)]}
    ).operations
    assert dialect.suggest_partition_drop(
        _strategy(dialect, "RANGE"), {"table": "T", "names": ["P1"]}
    ).operations
    assert (
        dialect.suggest_partition_retention(
            _strategy(dialect, "RANGE"),
            {"table": "T", "definitions": [_range(dialect, "P1", 1)], "cutoff": date(2026, 1, 1)},
        ).operations
        is not None
    )
