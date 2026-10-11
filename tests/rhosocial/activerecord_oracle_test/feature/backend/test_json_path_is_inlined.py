# tests/rhosocial/activerecord_oracle_test/feature/backend/test_json_path_is_inlined.py
"""A JSON path reaches Oracle as SQL text, not as a bind parameter.

Oracle rejects a bound path outright -- ORA-40454, "path expression not a
literal" -- so the factory path and the expression path have to agree. They did
not: ``mixins/json.py`` inlined the path and worked, while
``functions/json.py`` wrapped it in a Literal and produced a statement the
server refuses. Both are the same feature, and only one of them ran.

These tests exist because the difference is invisible in the rendered SQL
unless you look at the parameters: ``JSON_VALUE("DOC", ?)`` reads like a
working statement. It binds ``'$.a'`` and fails at execution.
"""

import pytest

from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.functions.json import (
    json_exists,
    json_query,
    json_value,
)


@pytest.fixture
def dialect():
    return OracleDialect()


@pytest.fixture
def doc(dialect):
    return Column(dialect, "DOC", table="T_JSON")


class TestJsonPathIsNotBound:
    """The path is part of the statement. A placeholder cannot stand for it."""

    @pytest.mark.parametrize(
        "factory,expected",
        [
            (json_value, "JSON_VALUE"),
            (json_query, "JSON_QUERY"),
            (json_exists, "JSON_EXISTS"),
        ],
    )
    def test_path_appears_in_the_sql(self, dialect, doc, factory, expected):
        sql, params = factory(dialect, doc, "$.a").to_sql()
        assert f"{expected}(\"T_JSON\".\"DOC\", '$.a')" == sql
        # Nothing is bound at all: the document is a column reference and the
        # path is inline, so there is no parameter left to send.
        assert params == ()

    @pytest.mark.parametrize("factory", [json_value, json_query, json_exists])
    def test_no_placeholder_reaches_the_statement(self, dialect, doc, factory):
        """A ``?`` here would mean the path went back to being a parameter."""
        sql, params = factory(dialect, doc, "$.nested.deeper").to_sql()
        assert "?" not in sql
        assert "$.nested.deeper" in sql
        assert params == ()

    def test_a_path_with_a_quote_is_escaped_not_truncated(self, dialect):
        """Inline means escaped, not interpolated.

        The path is now SQL text, so it has to be quoted the way every other
        inline literal is. An unescaped quote would end the literal early and
        the rest of the path would be parsed as SQL."""
        sql, _ = json_value(dialect, Column(dialect, "D"), "$.a'b").to_sql()
        assert "'$.a''b'" in sql or "'$.a\\'b'" in sql


class TestJsonReturningNamesOracleWords:
    """The RETURNING whitelist resolves its words to Oracle's own classes.

    ``BINARY_FLOAT``/``BINARY_DOUBLE`` used to instantiate core
    ``FloatType``/``DoubleType``, whose rendering is ``FLOAT(126)`` — and
    ``RETURNING FLOAT(126)`` is ``ORA-40449: invalid data type for return
    value`` on 21c (measured on 21.3 XE), while ``RETURNING BINARY_FLOAT`` /
    ``BINARY_DOUBLE`` is accepted on 21c and 26ai (23.26.1).  The words are
    also distinct storage that must not collapse into the NUMBER family
    (finding F.4-4 of the plan appendix).
    """

    @pytest.mark.parametrize("word", ["BINARY_FLOAT", "BINARY_DOUBLE"])
    def test_the_ieee_words_render_themselves(self, dialect, doc, word):
        sql, _ = json_value(dialect, doc, "$.a", returning=word).to_sql()
        assert f"RETURNING {word}" in sql

    def test_float_still_names_a_number_based_float(self, dialect, doc):
        sql, _ = json_value(dialect, doc, "$.a", returning="FLOAT").to_sql()
        assert "RETURNING FLOAT(126)" in sql


@pytest.mark.usefixtures("dialect")
class TestAgainstTheServer:
    """Rendering is not validation: run it.

    The scenario list is the project's own, so a version that is not running
    skips rather than reporting a pass it did not earn.
    """

    @pytest.fixture
    def live(self, request):
        try:
            import oracledb
        except ImportError:  # pragma: no cover
            pytest.skip("oracledb not installed")
        # Take the first scenario that actually answers. The list is ordered
        # oldest-first and the oldest container is often down, so stopping at
        # the first entry would skip the execution tests everywhere while
        # looking like they had run.
        conn = None
        for name, cfg in _scenarios().items():
            try:
                conn = oracledb.connect(
                    user=cfg["username"], password=cfg["password"],
                    dsn=f"{cfg['host']}:{cfg['port']}/{cfg['service_name']}",
                )
                break
            except Exception:
                continue
        if conn is None:  # pragma: no cover
            pytest.skip("no reachable Oracle scenario")
        cur = conn.cursor()
        yield cur
        conn.close()

    @pytest.fixture(autouse=True)
    def table(self, live):
        try:
            live.execute("DROP TABLE T_JSON")
        except Exception:
            pass
        live.execute("CREATE TABLE T_JSON (DOC CLOB)")
        live.execute("INSERT INTO T_JSON VALUES (:1)", ['{"a":1}'])
        live.connection.commit()

    def test_json_value_reads_the_scalar(self, dialect, live):
        """JSON_VALUE returns VARCHAR2 unless a RETURNING clause says
        otherwise, so the value arrives as text -- that is Oracle's default,
        not a rendering problem."""
        sql, params = json_value(dialect, Column(dialect, "DOC", table="T_JSON"),
                                 "$.a").to_sql()
        assert live.execute(f"SELECT {sql} FROM T_JSON", params).fetchone()[0] == "1"

    def test_json_query_reads_the_document(self, dialect, live):
        sql, params = json_query(dialect, Column(dialect, "DOC", table="T_JSON"),
                                 "$.a").to_sql()
        assert live.execute(f"SELECT {sql} FROM T_JSON", params).fetchone()[0] == "1"

    def test_json_exists_is_true_for_a_present_path(self, dialect, live):
        """JSON_EXISTS is a predicate and Oracle will not put it anywhere else.

        Rendering it into the select list raises ORA-40458, so it is asked here
        inside a CASE the same way a caller has to."""
        sql, params = json_exists(dialect, Column(dialect, "DOC", table="T_JSON"),
                                  "$.a").to_sql()
        wrapped = f"CASE WHEN {sql} THEN 1 ELSE 0 END"
        assert live.execute(f"SELECT {wrapped} FROM T_JSON", params).fetchone()[0] == 1

    def test_a_bound_path_is_what_used_to_break(self, live):
        """The regression, stated as a fact about the server rather than us.

        If this ever starts working, the inlining above is no longer necessary --
        and if it stops working, the reason is a driver restriction rather than
        Oracle's, which would be worth knowing before removing it."""
        import oracledb

        with pytest.raises(oracledb.DatabaseError) as err:
            live.execute("SELECT JSON_VALUE(:1, :2) FROM DUAL", ['{"a":1}', "$.a"])
        assert "ORA-40454" in str(err.value)


def _scenarios():
    """The project's own scenario file, so ports and credentials stay in one place."""
    import pathlib

    import yaml

    cfg = pathlib.Path(__file__).resolve()
    for parent in cfg.parents:
        candidate = parent / "config" / "oracle_scenarios.yaml"
        if candidate.exists():
            return yaml.safe_load(candidate.read_text()).get("scenarios", {})
    return {}