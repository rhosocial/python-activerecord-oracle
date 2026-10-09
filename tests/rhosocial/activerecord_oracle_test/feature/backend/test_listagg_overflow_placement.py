# tests/rhosocial/activerecord_oracle_test/feature/backend/test_listagg_overflow_placement.py
"""ON OVERFLOW belongs to the argument list, not after the sort clause.

Oracle's grammar is

    LISTAGG ( expr [, delim] [ ON OVERFLOW clause ] ) WITHIN GROUP ( ORDER BY ... )

so the clause that says what to do when the result is too long goes inside the
parentheses. Emitting it after WITHIN GROUP parses in no version:

    LISTAGG(x, ';') WITHIN GROUP (ORDER BY x) ON OVERFLOW TRUNCATE
                                                             ^^^^^^^
    ORA-00923: FROM keyword not found where expected

Which made the failure arrive exactly when it mattered: an aggregate that fits
in the limit works and looks right, and the first one that overflows is the one
that raises. The clause had been in the wrong place since it was written.
"""

import pytest

from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.functions import listagg


@pytest.fixture
def dialect():
    return OracleDialect()


def _listagg(dialect, within_group=None, on_overflow=None):
    expr = listagg(dialect, Column(dialect, "x", table="la"))
    expr._oracle_within_group = within_group
    expr._oracle_on_overflow = on_overflow
    return expr


class TestOverflowClausePlacement:
    def test_overflow_is_inside_the_argument_list(self, dialect):
        sql, params = _listagg(dialect, "x", "TRUNCATE").to_sql()
        assert sql == 'LISTAGG("LA"."X", ? ON OVERFLOW TRUNCATE) WITHIN GROUP (ORDER BY x)'
        assert params == (",",)

    def test_the_sort_clause_comes_last(self, dialect):
        """The order is the whole point: anything after WITHIN GROUP is a
        syntax error, so the clause cannot be appended at the end."""
        sql, _ = _listagg(dialect, "x", "TRUNCATE").to_sql()
        within = sql.index("WITHIN GROUP")
        overflow = sql.index("ON OVERFLOW")
        assert overflow < within

    def test_overflow_without_a_sort_clause(self, dialect):
        sql, _ = _listagg(dialect, None, "TRUNCATE").to_sql()
        assert sql == 'LISTAGG("LA"."X", ? ON OVERFLOW TRUNCATE)'

    def test_sort_clause_without_overflow_is_unchanged(self, dialect):
        sql, _ = _listagg(dialect, "x", None).to_sql()
        assert sql == 'LISTAGG("LA"."X", ?) WITHIN GROUP (ORDER BY x)'

    def test_neither_clause(self, dialect):
        sql, _ = _listagg(dialect, None, None).to_sql()
        assert sql == 'LISTAGG("LA"."X", ?)'

    def test_distinct_still_reaches_the_arguments(self, dialect):
        expr = listagg(dialect, Column(dialect, "x", table="la"))
        expr.is_distinct = True
        expr._oracle_on_overflow = "TRUNCATE"
        sql, _ = expr.to_sql()
        assert sql.startswith("LISTAGG(DISTINCT ")


class TestAgainstTheServer:
    """Rendering is not validation: run all four shapes."""

    @pytest.fixture
    def live(self):
        import pathlib

        import oracledb
        import yaml

        cfg = pathlib.Path(__file__).resolve()
        scenarios = {}
        for parent in cfg.parents:
            candidate = parent / "config" / "oracle_scenarios.yaml"
            if candidate.exists():
                scenarios = yaml.safe_load(
                    candidate.read_text()).get("scenarios", {})
                break
        conn = None
        for name, s in scenarios.items():
            try:
                conn = oracledb.connect(
                    user=s["username"], password=s["password"],
                    dsn=f"{s['host']}:{s['port']}/{s['service_name']}",
                )
                break
            except Exception:
                continue
        if conn is None:  # pragma: no cover
            pytest.skip("no reachable Oracle scenario")
        cur = conn.cursor()
        try:
            cur.execute("DROP TABLE la")
        except Exception:
            pass
        cur.execute("CREATE TABLE la (x VARCHAR2(10))")
        cur.execute("INSERT INTO la VALUES (:1)", ["a"])
        cur.execute("INSERT INTO la VALUES (:1)", ["b"])
        conn.commit()
        yield cur
        try:
            cur.execute("DROP TABLE la")
        except Exception:
            pass
        conn.close()

    @pytest.mark.parametrize(
        "within_group,on_overflow",
        [("x", None), ("x", "TRUNCATE"), (None, "TRUNCATE"), (None, None)],
        ids=["sort_only", "sort_and_overflow", "overflow_only", "neither"],
    )
    def test_every_combination_executes(self, dialect, live, within_group,
                                        on_overflow):
        sql, params = _listagg(dialect, within_group, on_overflow).to_sql()
        # oracledb binds by name; the renderer emits positional placeholders.
        got = live.execute(
            f"SELECT {sql.replace('?', ':1')} FROM la", params).fetchone()[0]
        assert got == "a,b"