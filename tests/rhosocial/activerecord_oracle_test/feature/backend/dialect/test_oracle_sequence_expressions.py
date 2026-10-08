# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_sequence_expressions.py
"""Tests for Oracle sequence expressions.

Covers the Oracle-specific ``seq.NEXTVAL`` / ``seq.CURRVAL`` value
expressions and the ``CREATE/DROP SEQUENCE`` DDL formatters, including
capability switches, identifier quoting, the ``IF NOT EXISTS`` / ``IF
EXISTS`` version gate (23ai) and the ``(9, 0, 0)`` version boundary.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    CreateSequenceExpression,
    DropSequenceExpression,
    InsertExpression,
    QueryExpression,
    ValuesSource,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.objects import Sequence, Table
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleCreateSequenceExpression,
    OracleDropSequenceExpression,
    OracleSequenceValueExpression,
    OracleSequenceValueMode,
)


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


class TestOracleSequenceCapabilities:
    def test_supports_create_and_drop(self, dialect):
        assert dialect.supports_create_sequence() is True
        assert dialect.supports_drop_sequence() is True

    def test_master_switch_is_version_gated(self):
        """8i has no sequence object; 9i is where sequences begin."""
        assert OracleDialect(version=(8, 1, 0)).supports_sequence() is False
        assert OracleDialect(version=(9, 0, 0)).supports_sequence() is True

    def test_statement_probes_follow_the_master_switch(self):
        """CREATE / DROP / ALTER inherit the master answer, not a flat True."""
        old = OracleDialect(version=(8, 1, 0))
        new = OracleDialect(version=(9, 0, 0))
        for probe in (
            "supports_create_sequence",
            "supports_drop_sequence",
            "supports_alter_sequence",
        ):
            assert getattr(old, probe)() is False
            assert getattr(new, probe)() is True

    def test_option_probes(self):
        """Every option Oracle accepts since 9i answers True; OWNED BY does not."""
        d = OracleDialect(version=(19, 0, 0))
        for probe in (
            "supports_sequence_start",
            "supports_sequence_increment",
            "supports_sequence_minvalue",
            "supports_sequence_maxvalue",
            "supports_sequence_cycle",
            "supports_sequence_cache",
            "supports_sequence_order",
        ):
            assert getattr(d, probe)() is True
        assert d.supports_sequence_owned_by() is False

    def test_graceful_ddl_is_23ai_gated(self):
        """IF [NOT] EXISTS is 23ai; 18c/21c answer False."""
        old = OracleDialect(version=(21, 0, 0))
        new = OracleDialect(version=(23, 0, 0))
        assert old.supports_sequence_if_not_exists() is False
        assert new.supports_sequence_if_not_exists() is True
        assert old.supports_sequence_if_exists() is False
        assert new.supports_sequence_if_exists() is True


class TestOracleSequenceValueExpression:
    def test_nextval(self, dialect):
        expr = OracleSequenceValueExpression(dialect, "user_seq")
        sql, params = expr.to_sql()
        assert sql == '"USER_SEQ".NEXTVAL'
        assert params == ()

    def test_currval(self, dialect):
        expr = OracleSequenceValueExpression(
            dialect, "user_seq", OracleSequenceValueMode.CURRVAL
        )
        sql, params = expr.to_sql()
        assert sql == '"USER_SEQ".CURRVAL'
        assert params == ()

    def test_identifier_upper_cased(self, dialect):
        expr = OracleSequenceValueExpression(dialect, "My_Seq")
        sql, params = expr.to_sql()
        assert sql == '"MY_SEQ".NEXTVAL'
        assert params == ()

    def test_invalid_mode_rejected(self, dialect):
        with pytest.raises(TypeError, match="mode must be an OracleSequenceValueMode"):
            OracleSequenceValueExpression(dialect, "seq", mode="NEXTVAL")

    def test_empty_sequence_rejected(self, dialect):
        with pytest.raises(ValueError, match="sequence must be a non-empty string"):
            OracleSequenceValueExpression(dialect, "  ")

    def test_nextval_in_select(self, dialect):
        value = OracleSequenceValueExpression(dialect, "user_seq")
        query = QueryExpression(dialect, select=[value], from_=Table(dialect, "dual"))
        sql, params = query.to_sql()
        assert sql == 'SELECT "USER_SEQ".NEXTVAL FROM "DUAL"'
        assert params == ()

    def test_nextval_in_insert(self, dialect):
        value = OracleSequenceValueExpression(dialect, "user_seq")
        source = ValuesSource(dialect, values_list=[[value, Literal(dialect, "x")]])
        expr = InsertExpression(dialect, into=Table(dialect, "t"), source=source, columns=["id", "name"])
        sql, params = expr.to_sql()
        assert sql == 'INSERT INTO "T" ("ID", "NAME") VALUES ("USER_SEQ".NEXTVAL, ?)'
        assert params == ("x",)


class TestOracleCreateSequenceExpression:
    def test_full_options(self, dialect):
        expr = OracleCreateSequenceExpression(
            dialect,
            sequence=Sequence(dialect, "seq"),
            start=1,
            increment=1,
            minvalue=1,
            maxvalue=999999,
            cycle=True,
            cache=20,
        )
        sql, params = expr.to_sql()
        assert sql == (
            'CREATE SEQUENCE "SEQ" START WITH 1 INCREMENT BY 1 '
            "MINVALUE 1 MAXVALUE 999999 CYCLE CACHE 20"
        )
        assert params == ()

    def test_minimal_omits_optional_clauses(self, dialect):
        expr = OracleCreateSequenceExpression(dialect, sequence=Sequence(dialect, "seq"))
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE "SEQ"'
        assert params == ()

    def test_nocycle_nocache_noorder(self, dialect):
        expr = OracleCreateSequenceExpression(
            dialect,
            sequence=Sequence(dialect, "seq"),
            no_cycle=True,
            no_cache=True,
            no_order=True,
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE "SEQ" NOCYCLE NOCACHE NOORDER'
        assert params == ()

    def test_order_and_cache(self, dialect):
        expr = OracleCreateSequenceExpression(
            dialect, sequence=Sequence(dialect, "seq"), order=True, cache=100
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE "SEQ" CACHE 100 ORDER'
        assert params == ()

    def test_core_create_sequence_expression(self, dialect):
        expr = CreateSequenceExpression(dialect, sequence=Sequence(dialect, "seq"))
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE "SEQ"'
        assert params == ()

    def test_owned_by_unsupported(self, dialect):
        expr = CreateSequenceExpression(dialect, sequence=Sequence(dialect, "seq"), owned_by="t.id")
        with pytest.raises(UnsupportedFeatureError, match="OWNED BY"):
            expr.to_sql()

    def test_if_not_exists_pre_23ai_raises(self, dialect):
        expr = OracleCreateSequenceExpression(dialect, sequence=Sequence(dialect, "seq"), if_not_exists=True)
        # The refusal is now the capability gate's, not the formatter's own
        # version-interpolated one, so the pin names the gate's feature.
        with pytest.raises(
            UnsupportedFeatureError,
            match="does not support CREATE SEQUENCE IF NOT EXISTS",
        ):
            expr.to_sql()

    def test_if_not_exists_23ai(self):
        d23 = OracleDialect(version=(23, 0, 0))
        expr = OracleCreateSequenceExpression(d23, sequence=Sequence(d23, "seq"), if_not_exists=True)
        sql, params = expr.to_sql()
        assert sql == 'CREATE SEQUENCE IF NOT EXISTS "SEQ"'
        assert params == ()

    def test_negative_cache_rejected(self, dialect):
        with pytest.raises(ValueError, match="cache must be a positive integer"):
            OracleCreateSequenceExpression(dialect, sequence=Sequence(dialect, "seq"), cache=-1)
        with pytest.raises(ValueError, match="cache must be a positive integer"):
            OracleCreateSequenceExpression(dialect, sequence=Sequence(dialect, "seq"), cache=0)

    def test_pair_options_are_mutually_exclusive(self, dialect):
        for kwargs in (
            {"cycle": True, "no_cycle": True},
            {"order": True, "no_order": True},
            {"cache": 10, "no_cache": True},
        ):
            with pytest.raises(ValueError, match="mutually exclusive"):
                OracleCreateSequenceExpression(
                    dialect, sequence=Sequence(dialect, "seq"), **kwargs
                )


class TestOracleSequenceOptionProbesAreLoadBearing:
    """Every sequence option probe must change behaviour when it is flipped.

    A probe the formatter never consults is decorative: flipping its answer
    leaves the rendered SQL untouched, so a wrong declaration is invisible.
    Each case below renders an option with the probe's declared answer, then
    flips that one probe in a subclass and re-renders. A load-bearing probe
    changes the outcome -- a render becomes a refusal or vice versa; a
    decorative probe behaves identically both times and is reported by name.
    Seven probes answer ``True`` on Oracle, so their flip must refuse the
    option; ``supports_sequence_owned_by`` answers ``False``, so its flip must
    turn the refusal into a render.
    """

    def test_every_sequence_option_probe_is_load_bearing(self):
        cases = [
            (
                "supports_sequence_start",
                OracleCreateSequenceExpression,
                {"start": 1},
                True,
                'CREATE SEQUENCE "SEQ" START WITH 1',
            ),
            (
                "supports_sequence_increment",
                OracleCreateSequenceExpression,
                {"increment": 10},
                True,
                'CREATE SEQUENCE "SEQ" INCREMENT BY 10',
            ),
            (
                "supports_sequence_minvalue",
                OracleCreateSequenceExpression,
                {"minvalue": 1},
                True,
                'CREATE SEQUENCE "SEQ" MINVALUE 1',
            ),
            (
                "supports_sequence_maxvalue",
                OracleCreateSequenceExpression,
                {"maxvalue": 999999},
                True,
                'CREATE SEQUENCE "SEQ" MAXVALUE 999999',
            ),
            (
                "supports_sequence_cycle",
                OracleCreateSequenceExpression,
                {"cycle": True},
                True,
                'CREATE SEQUENCE "SEQ" CYCLE',
            ),
            (
                "supports_sequence_cycle",
                OracleCreateSequenceExpression,
                {"no_cycle": True},
                True,
                'CREATE SEQUENCE "SEQ" NOCYCLE',
            ),
            (
                "supports_sequence_cache",
                OracleCreateSequenceExpression,
                {"cache": 20},
                True,
                'CREATE SEQUENCE "SEQ" CACHE 20',
            ),
            (
                "supports_sequence_cache",
                OracleCreateSequenceExpression,
                {"no_cache": True},
                True,
                'CREATE SEQUENCE "SEQ" NOCACHE',
            ),
            (
                "supports_sequence_order",
                OracleCreateSequenceExpression,
                {"order": True},
                True,
                'CREATE SEQUENCE "SEQ" ORDER',
            ),
            (
                "supports_sequence_order",
                OracleCreateSequenceExpression,
                {"no_order": True},
                True,
                'CREATE SEQUENCE "SEQ" NOORDER',
            ),
            (
                "supports_sequence_owned_by",
                # OracleCreateSequenceExpression has no owned_by field; the
                # core expression is what the formatter's getattr path serves.
                CreateSequenceExpression,
                {"owned_by": "t.id"},
                False,
                None,  # the declared answer refuses; flipping it must render
            ),
        ]

        def render_outcome(expression_cls, dialect, options):
            expr = expression_cls(
                dialect, sequence=Sequence(dialect, "seq"), **options
            )
            try:
                return "rendered", expr.to_sql()[0]
            except UnsupportedFeatureError:
                return "refused", None

        decorative = []
        for probe, expression_cls, options, declared_answer, expected in cases:
            declared = OracleDialect(version=(19, 0, 0))
            assert getattr(declared, probe)() is declared_answer, (
                f"{probe} is declared {declared_answer} on Oracle"
            )
            declared_outcome = render_outcome(expression_cls, declared, options)
            if expected is None:
                assert declared_outcome == ("refused", None), (
                    f"{probe}: declared answer {declared_answer} rendered "
                    f"{declared_outcome}"
                )
            else:
                assert declared_outcome == ("rendered", expected), (
                    f"{probe}: declared answer {declared_answer} rendered "
                    f"{declared_outcome}"
                )

            flipped_answer = not declared_answer
            flipped_cls = type(
                f"OracleDialectWith{probe}",
                (OracleDialect,),
                {probe: lambda self, _answer=flipped_answer: _answer},
            )
            flipped = flipped_cls(version=(19, 0, 0))
            if render_outcome(expression_cls, flipped, options) == declared_outcome:
                decorative.append(probe)

        assert not decorative, (
            "decorative sequence option probes: flipping these changes nothing, "
            f"so the CREATE formatter never consults them: {decorative}"
        )


class TestOracleDropSequenceExpression:
    def test_basic_drop(self, dialect):
        expr = OracleDropSequenceExpression(dialect, sequence=Sequence(dialect, "seq"))
        sql, params = expr.to_sql()
        assert sql == 'DROP SEQUENCE "SEQ"'
        assert params == ()

    def test_core_drop_sequence_expression(self, dialect):
        expr = DropSequenceExpression(dialect, sequence=Sequence(dialect, "seq"))
        sql, params = expr.to_sql()
        assert sql == 'DROP SEQUENCE "SEQ"'
        assert params == ()

    def test_if_exists_pre_23ai_raises(self, dialect):
        expr = OracleDropSequenceExpression(dialect, sequence=Sequence(dialect, "seq"), if_exists=True)
        # As with IF NOT EXISTS above: the gate's message, not the formatter's.
        with pytest.raises(
            UnsupportedFeatureError,
            match="does not support DROP SEQUENCE IF EXISTS",
        ):
            expr.to_sql()

    def test_if_exists_23ai(self):
        d23 = OracleDialect(version=(23, 0, 0))
        expr = OracleDropSequenceExpression(d23, sequence=Sequence(d23, "seq"), if_exists=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP SEQUENCE IF EXISTS "SEQ"'
        assert params == ()


class TestOracleSequenceVersionBoundary:
    def test_nextval_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleSequenceValueExpression(d8, "seq")
        with pytest.raises(UnsupportedFeatureError, match="NEXTVAL"):
            expr.to_sql()

    def test_currval_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleSequenceValueExpression(d8, "seq", OracleSequenceValueMode.CURRVAL)
        with pytest.raises(UnsupportedFeatureError, match="CURRVAL"):
            expr.to_sql()

    def test_create_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleCreateSequenceExpression(d8, sequence=Sequence(d8, "seq"))
        with pytest.raises(UnsupportedFeatureError, match="CREATE SEQUENCE"):
            expr.to_sql()

    def test_drop_below_9i_raises(self):
        d8 = OracleDialect(version=(8, 1, 0))
        expr = OracleDropSequenceExpression(d8, sequence=Sequence(d8, "seq"))
        with pytest.raises(UnsupportedFeatureError, match="DROP SEQUENCE"):
            expr.to_sql()

    def test_at_9i_works(self):
        d9 = OracleDialect(version=(9, 0, 0))
        assert OracleSequenceValueExpression(d9, "seq").to_sql()[0] == '"SEQ".NEXTVAL'
        assert OracleCreateSequenceExpression(d9, Sequence(d9, "seq")).to_sql()[0] == 'CREATE SEQUENCE "SEQ"'
