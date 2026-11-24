# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_if_exists_family.py
"""Statement-level ``IF [NOT] EXISTS`` on Oracle: floors, rendering, refusals.

Oracle introduced the clause with 19c **Release Update 19.28** and ships it in
the **23ai base release**; the 21c line has no spelling. The probes therefore
use two anchors (19c RU >= 28, or base major >= 23), not a literal
``>= (19, 28, 0)`` tuple -- that comparison would wrongly answer True on 21.3,
where the clause is absent.

These tests pin the shared probe answers, the rendered SQL at both anchors,
the refusal below them, and the CREATE TABLE PL/SQL fallback that stays valid
below the native capability.

Primary evidence:
* 19c changes page (new in Release 19.28):
  https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Changes-in-This-Release-for-Oracle-Database-SQL-Language-Reference.html
* 21c DROP TABLE grammar without the clause:
  https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/img_text/drop_table.html
* 23ai CREATE TABLE / DROP TABLE grammar with the clause:
  https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/img_text/create_table.html
"""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    Column,
    CreateTableStatement,
    CreateViewStatement,
    DropTableStatement,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import (
    Index,
    Sequence,
    Table,
    Trigger,
    View,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    CreateIndexStatement,
    CreateSequenceStatement,
    DropIndexStatement,
    DropSequenceStatement,
    DropTriggerStatement,
    DropViewStatement,
)
from rhosocial.activerecord.backend.expression.types import IntegerType
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


def _d(base=(19, 0, 0), full=None, ru=None):
    return OracleDialect(base, version_full=full, ru_version=ru)


#: Every statement-level gate that shares the two-anchor rule. The TYPE family
#: has its own five probes but they all resolve through the same helper.
FAMILY_PROBES = (
    "supports_if_not_exists_table",
    "supports_if_exists_table",
    "supports_if_not_exists_view",
    "supports_if_exists_view",
    "supports_sequence_if_not_exists",
    "supports_sequence_if_exists",
    "supports_index_if_not_exists",
    "supports_index_if_exists",
    "supports_drop_trigger_if_exists",
    "supports_create_type_if_not_exists",
    "supports_alter_type_if_exists",
    "supports_drop_type_if_exists",
    "supports_create_type_body_if_not_exists",
    "supports_drop_type_body_if_exists",
)


class TestFamilyProbeFloors:
    """19.28 is a 19c release update, 23 is a base release, 21c is neither."""

    @pytest.mark.parametrize("method", FAMILY_PROBES)
    def test_19c_ru_27_is_below_the_floor(self, method):
        assert getattr(_d((19, 0, 0), (19, 27, 0, 0, 0)), method)() is False

    @pytest.mark.parametrize("method", FAMILY_PROBES)
    def test_19c_ru_28_is_at_the_floor(self, method):
        assert getattr(_d((19, 0, 0), (19, 28, 0, 0, 0)), method)() is True

    @pytest.mark.parametrize("method", FAMILY_PROBES)
    def test_21c_is_not_covered_by_the_19_28_anchor(self, method):
        """``21 > 19``, but 21.3 has no clause; a tuple floor would lie."""
        assert getattr(_d((21, 3, 0), (21, 3, 0, 0, 0), 3), method)() is False

    @pytest.mark.parametrize("method", FAMILY_PROBES)
    def test_23ai_base_release_is_at_the_floor(self, method):
        assert getattr(_d((23, 0, 0)), method)() is True

    @pytest.mark.parametrize("method", FAMILY_PROBES)
    def test_every_older_line_is_below_the_floor(self, method):
        for base in ((11, 2, 0), (12, 2, 0), (18, 0, 0), (20, 0, 0)):
            assert getattr(_d(base), method)() is False

    @pytest.mark.parametrize("method", FAMILY_PROBES)
    def test_a_fabricated_19_28_base_tuple_does_not_answer(self, method):
        """No 19c server reports ``(19, 28, 0)``; the RU lives in VERSION_FULL."""
        assert getattr(_d((19, 28, 0)), method)() is False

    @pytest.mark.parametrize("method", FAMILY_PROBES)
    def test_a_conflicting_ru_and_version_full_is_not_trusted(self, method):
        assert (
            getattr(_d((19, 0, 0), (19, 28, 0, 0, 0), 27), method)() is False
        )

    @pytest.mark.parametrize("method", FAMILY_PROBES)
    def test_an_unversioned_dialect_answers_false(self, method):
        assert getattr(OracleDialect(), method)() is False


# ---------------------------------------------------------------------------
# Rendering at the two anchors
# ---------------------------------------------------------------------------


def _query(d):
    return QueryStatement(d, select=[Column(d, "id")], from_=Table(d, "t"))


def _create_table(d, **kwargs):
    return CreateTableStatement(
        d,
        Table(d, "t"),
        [ColumnDefinition(d, "id", IntegerType(d))],
        **kwargs,
    )


def _create_view(d, **kwargs):
    return CreateViewStatement(d, View(d, "v"), _query(d), **kwargs)


def _create_index(d, **kwargs):
    return CreateIndexStatement(d, Index(d, "ix"), Table(d, "t"), ["id"], **kwargs)


def _create_sequence(d, **kwargs):
    return CreateSequenceStatement(d, Sequence(d, "seq"), **kwargs)


RENDERING_BUILDERS = (
    ("DROP TABLE IF EXISTS", lambda d: DropTableStatement(d, Table(d, "t"), if_exists=True)),
    ("CREATE TABLE IF NOT EXISTS", lambda d: _create_table(d, if_not_exists=True)),
    ("DROP VIEW IF EXISTS", lambda d: DropViewStatement(d, View(d, "v"), if_exists=True)),
    ("CREATE VIEW IF NOT EXISTS", lambda d: _create_view(d, if_not_exists=True)),
    ("DROP SEQUENCE IF EXISTS", lambda d: DropSequenceStatement(d, Sequence(d, "seq"), if_exists=True)),
    ("CREATE SEQUENCE IF NOT EXISTS", lambda d: _create_sequence(d, if_not_exists=True)),
    ("DROP INDEX IF EXISTS", lambda d: DropIndexStatement(d, Index(d, "ix"), if_exists=True)),
    ("CREATE INDEX IF NOT EXISTS", lambda d: _create_index(d, if_not_exists=True)),
    ("DROP TRIGGER IF EXISTS", lambda d: DropTriggerStatement(d, Trigger(d, "trg"), if_exists=True)),
)


@pytest.mark.parametrize(
    "fragment,builder", RENDERING_BUILDERS, ids=[item[0] for item in RENDERING_BUILDERS]
)
def test_the_clause_renders_from_19_28_ru(fragment, builder):
    d = _d((19, 0, 0), (19, 28, 0, 0, 0))
    sql, params = builder(d).to_sql()
    assert fragment in sql
    assert params == ()


@pytest.mark.parametrize(
    "fragment,builder", RENDERING_BUILDERS, ids=[item[0] for item in RENDERING_BUILDERS]
)
def test_the_clause_renders_from_23ai(fragment, builder):
    d = _d((23, 0, 0))
    sql, params = builder(d).to_sql()
    assert fragment in sql
    assert params == ()


def test_owner_qualified_names_keep_the_clause_in_place():
    d = _d((23, 0, 0))
    assert DropTableStatement(d, Table(d, "t", schema_name="app"), if_exists=True).to_sql() == (
        'DROP TABLE IF EXISTS "APP"."T"',
        (),
    )
    assert DropViewStatement(d, View(d, "v", schema_name="app"), if_exists=True).to_sql() == (
        'DROP VIEW IF EXISTS "APP"."V"',
        (),
    )


# ---------------------------------------------------------------------------
# Refusals below the floor
# ---------------------------------------------------------------------------


REFUSALS = (
    ("DROP TABLE IF EXISTS", lambda d: DropTableStatement(d, Table(d, "t"), if_exists=True)),
    ("CREATE TABLE IF NOT EXISTS", lambda d: _create_table(d, temporary=True, if_not_exists=True)),
    ("DROP VIEW IF EXISTS", lambda d: DropViewStatement(d, View(d, "v"), if_exists=True)),
    ("CREATE VIEW IF NOT EXISTS", lambda d: _create_view(d, if_not_exists=True)),
    ("DROP SEQUENCE IF EXISTS", lambda d: DropSequenceStatement(d, Sequence(d, "seq"), if_exists=True)),
    ("CREATE SEQUENCE IF NOT EXISTS", lambda d: _create_sequence(d, if_not_exists=True)),
    ("DROP INDEX IF EXISTS", lambda d: DropIndexStatement(d, Index(d, "ix"), if_exists=True)),
    ("CREATE INDEX IF NOT EXISTS", lambda d: _create_index(d, if_not_exists=True)),
    ("DROP TRIGGER IF EXISTS", lambda d: DropTriggerStatement(d, Trigger(d, "trg"), if_exists=True)),
)


@pytest.mark.parametrize(
    "feature,builder", REFUSALS, ids=[item[0] for item in REFUSALS]
)
def test_the_request_is_refused_by_name_below_the_floor(feature, builder):
    d = _d((19, 0, 0), (19, 27, 0, 0, 0))
    with pytest.raises(UnsupportedFeatureError, match=feature):
        builder(d).to_sql()


@pytest.mark.parametrize(
    "feature,builder",
    [
        ("DROP TABLE IF EXISTS", lambda d: DropTableStatement(d, Table(d, "t"), if_exists=True)),
        ("CREATE VIEW IF NOT EXISTS", lambda d: _create_view(d, if_not_exists=True)),
        ("DROP SEQUENCE IF EXISTS", lambda d: DropSequenceStatement(d, Sequence(d, "seq"), if_exists=True)),
        ("CREATE INDEX IF NOT EXISTS", lambda d: _create_index(d, if_not_exists=True)),
        ("DROP TRIGGER IF EXISTS", lambda d: DropTriggerStatement(d, Trigger(d, "trg"), if_exists=True)),
    ],
    ids=["table", "view", "sequence", "index", "trigger"],
)
def test_21c_also_refuses(feature, builder):
    d = _d((21, 3, 0), (21, 3, 0, 0, 0), 3)
    with pytest.raises(UnsupportedFeatureError, match=feature):
        builder(d).to_sql()


def test_create_table_keeps_the_documented_fallback_below_the_floor():
    """Below the native capability the PL/SQL existence check still works.

    The probe reports the *native clause*; the fallback is a different,
    documented mechanism and is not silently lost. Only shapes the fallback
    cannot wrap are refused (covered above via ``temporary=True``).
    """
    d = _d((19, 0, 0), (19, 27, 0, 0, 0))
    sql, params = _create_table(d, if_not_exists=True).to_sql()
    assert sql.startswith("DECLARE v_cnt NUMBER")
    assert "CREATE TABLE" in sql
    assert "IF NOT EXISTS" not in sql
    assert params == ()


def test_create_view_replace_and_if_not_exists_are_mutually_exclusive():
    """ORA-11541: REPLACE and IF NOT EXISTS cannot coexist in one statement."""
    d = _d((23, 0, 0))
    expr = _create_view(d, replace=True, if_not_exists=True)
    with pytest.raises(UnsupportedFeatureError, match="CREATE VIEW OR REPLACE IF NOT EXISTS"):
        expr.to_sql()


# ---------------------------------------------------------------------------
# ALTER TABLE ... DROP ...: no column/constraint-level clause on any release
# ---------------------------------------------------------------------------


def test_column_and_constraint_if_exists_stay_false_even_on_23ai():
    """23ai's drop_column_clause / drop_constraint_clause carry no IF EXISTS."""
    d = _d((23, 0, 0))
    assert d.supports_add_column_if_not_exists() is False
    assert d.supports_drop_column_if_exists() is False
    assert d.supports_drop_constraint_if_exists() is False
