# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_clause_pair_guard.py
"""Guard: every clause pair the Oracle dialect consumes distinguishes four states.

The expressiveness round gave each two-spelling clause one parameter per
spelling.  For every pair this backend consumes, the four API states must be
pairwise distinguishable:

* neither parameter set -- no spelling is emitted (or, for a mandatory clause,
  construction is refused by ``ValueError``);
* the positive parameter set -- the positive spelling is emitted;
* the negative parameter set -- the negative spelling is emitted;
* both set -- construction is refused by ``ValueError``.

A formatter that reads only one side of a pair makes the two states render the
same SQL; a node that still defaults one side makes "unspecified" collide with a
spelling.  Either defect turns this file red by name, with the rendered SQL as
evidence.

The guard was run against the unmodified backend first (its RED output is
recorded in the round's plan directory); it is green after the change.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.objects import (
    MaterializedView,
    Sequence,
    Table,
    Type,
    View,
)
from rhosocial.activerecord.backend.expression.query_sources import (
    SetOperationExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    AlterSequenceExpression,
    CreateMaterializedViewExpression,
    CreateSequenceExpression,
    CreateTableAsExpression,
    CreateTypeExpression,
    DropMaterializedViewExpression,
    DropTableExpression,
    DropViewExpression,
    ForeignKeyConstraint,
    IdentityClause,
    QueryExpression,
    RefreshMaterializedViewExpression,
    TableConstraint,
    TableConstraintType,
    TruncateExpression,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.alter_table import (
    OracleReadOnlyAction,
    OracleRowMovementAction,
)
from rhosocial.activerecord.backend.impl.oracle.expression.ddl.type import (
    OracleAlterTypeFinalAction,
    OracleAlterTypeInstantiableAction,
    OracleCreateTypeBodyExpression,
    OracleObjectTypeDefinition,
    OracleVarrayTypeDefinition,
)
from rhosocial.activerecord.backend.impl.oracle.expression.materialized_view import (
    OracleCreateMaterializedViewExpression,
)
from rhosocial.activerecord.backend.impl.oracle.expression.sequence import (
    OracleCreateSequenceExpression,
)


def _dialect() -> OracleDialect:
    return OracleDialect(version=(19, 0, 0))


def _query(dialect: OracleDialect) -> QueryExpression:
    return QueryExpression(dialect, select=[Literal(dialect, 1)])


@dataclass(frozen=True)
class PairSite:
    """One clause pair consumed by this dialect, in its four API states."""

    name: str
    build: Callable[[OracleDialect, Dict[str, Any]], Any]
    states: Dict[str, Dict[str, Any]]
    mandatory: bool = False
    a_render: Optional[str] = None
    b_render: Optional[str] = None
    a_refuse: Optional[str] = None
    b_refuse: Optional[str] = None
    none_forbidden: Tuple[str, ...] = field(default_factory=tuple)


def _outcome(site: PairSite, dialect: OracleDialect, state: str) -> Tuple:
    try:
        expr = site.build(dialect, dict(site.states[state]))
    except Exception as exc:  # noqa: BLE001 - the exception is the evidence
        return ("construct", type(exc).__name__, str(exc))
    try:
        sql, _params = expr.to_sql()
    except Exception as exc:  # noqa: BLE001
        return ("render-error", type(exc).__name__, str(exc))
    return ("render", sql)


def _assert_side(site_name: str, side: str, outcome: Tuple, render: Optional[str], refuse: Optional[str]) -> None:
    if refuse is not None:
        assert outcome[0] == "render-error", (
            f"{site_name}: {side} must be refused by name, got {outcome!r}"
        )
        assert outcome[1] == "UnsupportedFeatureError", (
            f"{site_name}: {side} must refuse with UnsupportedFeatureError, "
            f"got {outcome!r}"
        )
        assert refuse in outcome[2], (
            f"{site_name}: {side} refusal must name {refuse!r}, got {outcome[2]!r}"
        )
        return
    assert render is not None, f"{site_name}: {side} has no expectation"
    assert outcome[0] == "render", (
        f"{site_name}: {side} must render, got {outcome!r}"
    )
    assert re.search(render, outcome[1]), (
        f"{site_name}: {side} must render /{render}/, got {outcome[1]!r}"
    )


SITES = (
    # ------------------------------------------------ core sequence via oracle
    PairSite(
        "CreateSequenceExpression cycle/no_cycle",
        lambda d, kw: CreateSequenceExpression(d, Sequence(d, "s"), **kw),
        {"none": {}, "a": {"cycle": True}, "b": {"no_cycle": True}, "both": {"cycle": True, "no_cycle": True}},
        a_render=r"(?<!NO)CYCLE",
        b_render=r"NOCYCLE",
        none_forbidden=(r"(?<!NO)CYCLE", r"NOCYCLE"),
    ),
    PairSite(
        "CreateSequenceExpression order/no_order",
        lambda d, kw: CreateSequenceExpression(d, Sequence(d, "s"), **kw),
        {"none": {}, "a": {"order": True}, "b": {"no_order": True}, "both": {"order": True, "no_order": True}},
        a_render=r"(?<!NO)ORDER",
        b_render=r"NOORDER",
        none_forbidden=(r"(?<!NO)ORDER", r"NOORDER"),
    ),
    PairSite(
        "CreateSequenceExpression cache/no_cache",
        lambda d, kw: CreateSequenceExpression(d, Sequence(d, "s"), **kw),
        {"none": {}, "a": {"cache": 10}, "b": {"no_cache": True}, "both": {"cache": 10, "no_cache": True}},
        a_render=r"(?<!NO)CACHE 10",
        b_render=r"NOCACHE",
        none_forbidden=(r"(?<!NO)CACHE", r"NOCACHE"),
    ),
    PairSite(
        "AlterSequenceExpression cycle/no_cycle",
        lambda d, kw: AlterSequenceExpression(d, Sequence(d, "s"), **kw),
        {"none": {}, "a": {"cycle": True}, "b": {"no_cycle": True}, "both": {"cycle": True, "no_cycle": True}},
        a_render=r"(?<!NO)CYCLE",
        b_render=r"NOCYCLE",
        none_forbidden=(r"(?<!NO)CYCLE", r"NOCYCLE"),
    ),
    PairSite(
        "AlterSequenceExpression order/no_order",
        lambda d, kw: AlterSequenceExpression(d, Sequence(d, "s"), **kw),
        {"none": {}, "a": {"order": True}, "b": {"no_order": True}, "both": {"order": True, "no_order": True}},
        a_render=r"(?<!NO)ORDER",
        b_render=r"NOORDER",
        none_forbidden=(r"(?<!NO)ORDER", r"NOORDER"),
    ),
    PairSite(
        "AlterSequenceExpression cache/no_cache",
        lambda d, kw: AlterSequenceExpression(d, Sequence(d, "s"), **kw),
        {"none": {}, "a": {"cache": 10}, "b": {"no_cache": True}, "both": {"cache": 10, "no_cache": True}},
        a_render=r"(?<!NO)CACHE 10",
        b_render=r"NOCACHE",
        none_forbidden=(r"(?<!NO)CACHE", r"NOCACHE"),
    ),
    # --------------------------------------------------- core identity via oracle
    PairSite(
        "IdentityClause cycle/no_cycle",
        lambda d, kw: IdentityClause(d, **kw),
        {"none": {}, "a": {"cycle": True}, "b": {"no_cycle": True}, "both": {"cycle": True, "no_cycle": True}},
        a_render=r"(?<!NO)CYCLE\)",
        b_render=r"NOCYCLE\)",
        none_forbidden=(r"(?<!NO)CYCLE", r"NOCYCLE"),
    ),
    PairSite(
        "IdentityClause order/no_order",
        lambda d, kw: IdentityClause(d, **kw),
        {"none": {}, "a": {"order": True}, "b": {"no_order": True}, "both": {"order": True, "no_order": True}},
        a_render=r"(?<!NO)ORDER\)",
        b_render=r"NOORDER\)",
        none_forbidden=(r"(?<!NO)ORDER", r"NOORDER"),
    ),
    PairSite(
        "IdentityClause cache/no_cache",
        lambda d, kw: IdentityClause(d, **kw),
        {"none": {}, "a": {"cache": 10}, "b": {"no_cache": True}, "both": {"cache": 10, "no_cache": True}},
        a_render=r"(?<!NO)CACHE 10\)",
        b_render=r"NOCACHE\)",
        none_forbidden=(r"(?<!NO)CACHE", r"NOCACHE"),
    ),
    # --------------------------------------------------------- core truncate
    PairSite(
        "TruncateExpression cascade/restrict",
        lambda d, kw: TruncateExpression(d, Table(d, "t"), **kw),
        {"none": {}, "a": {"cascade": True}, "b": {"restrict": True}, "both": {"cascade": True, "restrict": True}},
        a_refuse="TRUNCATE CASCADE",
        b_refuse="TRUNCATE RESTRICT",
        none_forbidden=(r"CASCADE", r"RESTRICT"),
    ),
    PairSite(
        "TruncateExpression restart_identity/continue_identity",
        lambda d, kw: TruncateExpression(d, Table(d, "t"), **kw),
        {
            "none": {},
            "a": {"restart_identity": True},
            "b": {"continue_identity": True},
            "both": {"restart_identity": True, "continue_identity": True},
        },
        a_refuse="TRUNCATE RESTART IDENTITY",
        b_refuse="TRUNCATE CONTINUE IDENTITY",
        none_forbidden=(r"RESTART IDENTITY", r"CONTINUE IDENTITY"),
    ),
    # ------------------------------------------------------------ core drops
    PairSite(
        "DropViewExpression cascade/restrict",
        lambda d, kw: DropViewExpression(d, View(d, "v"), **kw),
        {"none": {}, "a": {"cascade": True}, "b": {"restrict": True}, "both": {"cascade": True, "restrict": True}},
        a_refuse="DROP VIEW CASCADE",
        b_refuse="DROP VIEW RESTRICT",
        none_forbidden=(r"CASCADE", r"RESTRICT"),
    ),
    PairSite(
        "DropMaterializedViewExpression cascade/restrict",
        lambda d, kw: DropMaterializedViewExpression(d, MaterializedView(d, "mv"), **kw),
        {"none": {}, "a": {"cascade": True}, "b": {"restrict": True}, "both": {"cascade": True, "restrict": True}},
        a_refuse="DROP MATERIALIZED VIEW CASCADE",
        b_refuse="DROP MATERIALIZED VIEW RESTRICT",
        none_forbidden=(r"CASCADE", r"RESTRICT"),
    ),
    PairSite(
        "DropTableExpression cascade/restrict",
        lambda d, kw: DropTableExpression(d, Table(d, "t"), **kw),
        {"none": {}, "a": {"cascade": True}, "b": {"restrict": True}, "both": {"cascade": True, "restrict": True}},
        a_render=r"CASCADE CONSTRAINTS",
        b_refuse="DROP TABLE ... RESTRICT",
        none_forbidden=(r"CASCADE", r"RESTRICT"),
    ),
    # --------------------------------------------------- materialized views
    PairSite(
        "CreateMaterializedViewExpression with_data/no_data",
        lambda d, kw: CreateMaterializedViewExpression(d, MaterializedView(d, "mv"), _query(d), **kw),
        {"none": {}, "a": {"with_data": True}, "b": {"no_data": True}, "both": {"with_data": True, "no_data": True}},
        a_render=r"BUILD IMMEDIATE",
        b_render=r"BUILD DEFERRED",
        none_forbidden=(r"BUILD IMMEDIATE", r"BUILD DEFERRED"),
    ),
    PairSite(
        "RefreshMaterializedViewExpression with_data/no_data",
        lambda d, kw: RefreshMaterializedViewExpression(d, MaterializedView(d, "mv"), **kw),
        {"none": {}, "a": {"with_data": True}, "b": {"no_data": True}, "both": {"with_data": True, "no_data": True}},
        a_refuse="REFRESH MATERIALIZED VIEW WITH DATA",
        b_refuse="REFRESH MATERIALIZED VIEW WITH NO DATA",
        none_forbidden=(r"WITH DATA", r"WITH NO DATA"),
    ),
    # ------------------------------------------------------- core constraints
    PairSite(
        "ForeignKeyConstraint deferrable/not_deferrable",
        lambda d, kw: ForeignKeyConstraint(
            d, columns=["a"], foreign_key_table=Table(d, "t2"), foreign_key_columns=["b"], **kw
        ),
        {
            "none": {},
            "a": {"deferrable": True},
            "b": {"not_deferrable": True},
            "both": {"deferrable": True, "not_deferrable": True},
        },
        a_render=r"(?<!NOT )DEFERRABLE",
        b_render=r"NOT DEFERRABLE",
        none_forbidden=(r"DEFERRABLE",),
    ),
    PairSite(
        "ForeignKeyConstraint initially_deferred/initially_immediate",
        lambda d, kw: ForeignKeyConstraint(
            d, columns=["a"], foreign_key_table=Table(d, "t2"), foreign_key_columns=["b"], **kw
        ),
        {
            "none": {},
            "a": {"initially_deferred": True},
            "b": {"initially_immediate": True},
            "both": {"initially_deferred": True, "initially_immediate": True},
        },
        a_render=r"INITIALLY DEFERRED",
        b_render=r"INITIALLY IMMEDIATE",
        none_forbidden=(r"INITIALLY",),
    ),
    PairSite(
        "ForeignKeyConstraint enforced/not_enforced",
        lambda d, kw: ForeignKeyConstraint(
            d, columns=["a"], foreign_key_table=Table(d, "t2"), foreign_key_columns=["b"], **kw
        ),
        {"none": {}, "a": {"enforced": True}, "b": {"not_enforced": True}, "both": {"enforced": True, "not_enforced": True}},
        a_refuse="constraint ENFORCED",
        b_refuse="constraint NOT ENFORCED",
        none_forbidden=(r"ENFORCED",),
    ),
    PairSite(
        "TableConstraint(check) enforced/not_enforced",
        lambda d, kw: TableConstraint(d, TableConstraintType.CHECK, check_condition=Literal(d, 1), **kw),
        {"none": {}, "a": {"enforced": True}, "b": {"not_enforced": True}, "both": {"enforced": True, "not_enforced": True}},
        a_refuse="constraint ENFORCED",
        b_refuse="constraint NOT ENFORCED",
        none_forbidden=(r"ENFORCED",),
    ),
    # ------------------------------------------------------- core set operation
    PairSite(
        "SetOperationExpression all_/distinct",
        lambda d, kw: SetOperationExpression(d, left=_query(d), right=_query(d), operation="UNION", **kw),
        {"none": {}, "a": {"all_": True}, "b": {"distinct": True}, "both": {"all_": True, "distinct": True}},
        a_render=r"UNION ALL",
        b_render=r"UNION DISTINCT",
        none_forbidden=(r"UNION ALL", r"UNION DISTINCT"),
    ),
    # --------------------------------------------- oracle-specific sequences
    PairSite(
        "OracleCreateSequenceExpression cycle/no_cycle",
        lambda d, kw: OracleCreateSequenceExpression(d, Sequence(d, "s"), **kw),
        {"none": {}, "a": {"cycle": True}, "b": {"no_cycle": True}, "both": {"cycle": True, "no_cycle": True}},
        a_render=r"(?<!NO)CYCLE",
        b_render=r"NOCYCLE",
        none_forbidden=(r"(?<!NO)CYCLE", r"NOCYCLE"),
    ),
    PairSite(
        "OracleCreateSequenceExpression order/no_order",
        lambda d, kw: OracleCreateSequenceExpression(d, Sequence(d, "s"), **kw),
        {"none": {}, "a": {"order": True}, "b": {"no_order": True}, "both": {"order": True, "no_order": True}},
        a_render=r"(?<!NO)ORDER",
        b_render=r"NOORDER",
        none_forbidden=(r"(?<!NO)ORDER", r"NOORDER"),
    ),
    PairSite(
        "OracleCreateSequenceExpression cache/no_cache",
        lambda d, kw: OracleCreateSequenceExpression(d, Sequence(d, "s"), **kw),
        {"none": {}, "a": {"cache": 10}, "b": {"no_cache": True}, "both": {"cache": 10, "no_cache": True}},
        a_render=r"(?<!NO)CACHE 10",
        b_render=r"NOCACHE",
        none_forbidden=(r"(?<!NO)CACHE", r"NOCACHE"),
    ),
    # ------------------------------------------------------ oracle type flags
    PairSite(
        "OracleAlterTypeFinalAction final/not_final",
        lambda d, kw: OracleAlterTypeFinalAction(d, **kw),
        {"none": {}, "a": {"final": True}, "b": {"not_final": True}, "both": {"final": True, "not_final": True}},
        mandatory=True,
        a_render=r"(?<!NOT )FINAL",
        b_render=r"NOT FINAL",
        none_forbidden=(r"FINAL",),
    ),
    PairSite(
        "OracleAlterTypeInstantiableAction instantiable/not_instantiable",
        lambda d, kw: OracleAlterTypeInstantiableAction(d, **kw),
        {
            "none": {},
            "a": {"instantiable": True},
            "b": {"not_instantiable": True},
            "both": {"instantiable": True, "not_instantiable": True},
        },
        mandatory=True,
        a_render=r"(?<!NOT )INSTANTIABLE",
        b_render=r"NOT INSTANTIABLE",
        none_forbidden=(r"INSTANTIABLE",),
    ),
    PairSite(
        "OracleObjectTypeDefinition editionable/noneditionable",
        lambda d, kw: CreateTypeExpression(
            d, Type(d, "t"), OracleObjectTypeDefinition(d, attributes=["a NUMBER"], **kw)
        ),
        {
            "none": {},
            "a": {"editionable": True},
            "b": {"noneditionable": True},
            "both": {"editionable": True, "noneditionable": True},
        },
        a_render=r"(?<!NON)EDITIONABLE",
        b_render=r"NONEDITIONABLE",
        none_forbidden=(r"EDITIONABLE",),
    ),
    PairSite(
        "OracleCreateTypeBodyExpression editionable/noneditionable",
        lambda d, kw: OracleCreateTypeBodyExpression(d, Type(d, "t"), "END;", **kw),
        {
            "none": {},
            "a": {"editionable": True},
            "b": {"noneditionable": True},
            "both": {"editionable": True, "noneditionable": True},
        },
        a_render=r"(?<!NON)EDITIONABLE",
        b_render=r"NONEDITIONABLE",
        none_forbidden=(r"EDITIONABLE",),
    ),
    PairSite(
        "OracleVarrayTypeDefinition persistable/not_persistable (already target shape)",
        lambda d, kw: OracleVarrayTypeDefinition(d, element_type="NUMBER", size_limit=10, **kw),
        {"none": {}, "a": {"persistable": True}, "b": {"persistable": False}, "both": {"persistable": True, "not_persistable": True}},
        a_render=r"(?<!NOT )PERSISTABLE",
        b_render=r"NOT PERSISTABLE",
        none_forbidden=(r"PERSISTABLE",),
    ),
    # ------------------------------------------------ oracle materialized view
    PairSite(
        "OracleCreateMaterializedViewExpression enable/disable query rewrite",
        lambda d, kw: OracleCreateMaterializedViewExpression(d, MaterializedView(d, "mv"), _query(d), **kw),
        {
            "none": {},
            "a": {"enable_query_rewrite": True},
            "b": {"disable_query_rewrite": True},
            "both": {"enable_query_rewrite": True, "disable_query_rewrite": True},
        },
        a_render=r"ENABLE QUERY REWRITE",
        b_render=r"DISABLE QUERY REWRITE",
        none_forbidden=(r"QUERY REWRITE",),
    ),
    # ------------------------------------------------- oracle ALTER TABLE modes
    PairSite(
        "OracleReadOnlyAction read_only/read_write",
        lambda d, kw: OracleReadOnlyAction(d, **kw),
        {"none": {}, "a": {"read_only": True}, "b": {"read_write": True}, "both": {"read_only": True, "read_write": True}},
        mandatory=True,
        a_render=r"READ ONLY",
        b_render=r"READ WRITE",
        none_forbidden=(r"READ ONLY", r"READ WRITE"),
    ),
    PairSite(
        "OracleRowMovementAction enable/disable",
        lambda d, kw: OracleRowMovementAction(d, **kw),
        {"none": {}, "a": {"enable": True}, "b": {"disable": True}, "both": {"enable": True, "disable": True}},
        mandatory=True,
        a_render=r"ENABLE ROW MOVEMENT",
        b_render=r"DISABLE ROW MOVEMENT",
        none_forbidden=(r"ROW MOVEMENT",),
    ),
)


@pytest.mark.parametrize("site", SITES, ids=[site.name for site in SITES])
def test_pair_states_are_pairwise_distinguishable(site: PairSite) -> None:
    dialect = _dialect()
    outcomes = {state: _outcome(site, dialect, state) for state in ("none", "a", "b", "both")}

    # 1. Both sides set is API misuse: ValueError at construction.
    both = outcomes["both"]
    assert both[0] == "construct", f"{site.name}: both-set must fail at construction, got {both!r}"
    assert both[1] == "ValueError", f"{site.name}: both-set must raise ValueError, got {both!r}"
    assert "mutually exclusive" in both[2], f"{site.name}: both-set message must say 'mutually exclusive', got {both[2]!r}"

    # 2. Neither set: no spelling (optional) or a named refusal (mandatory).
    none = outcomes["none"]
    if site.mandatory:
        assert none[0] == "construct", f"{site.name}: mandatory clause must refuse neither-set, got {none!r}"
        assert none[1] == "ValueError", f"{site.name}: neither-set must raise ValueError, got {none!r}"
        assert "exactly one" in none[2], f"{site.name}: neither-set message must say 'exactly one', got {none[2]!r}"
    else:
        assert none[0] == "render", f"{site.name}: neither-set must render, got {none!r}"
        for pattern in site.none_forbidden:
            assert not re.search(pattern, none[1]), (
                f"{site.name}: neither-set must not emit /{pattern}/, got {none[1]!r}"
            )

    # 3. Each side set: its own spelling, or a named refusal where the dialect
    #    has no spelling for that side.
    _assert_side(site.name, "a", outcomes["a"], site.a_render, site.a_refuse)
    _assert_side(site.name, "b", outcomes["b"], site.b_render, site.b_refuse)

    # 4. Four states pairwise distinguishable.
    assert len(set(outcomes.values())) == 4, (
        f"{site.name}: the four states are not pairwise distinguishable: {outcomes!r}"
    )
