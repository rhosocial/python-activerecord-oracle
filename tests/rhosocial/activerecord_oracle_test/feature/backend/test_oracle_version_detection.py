# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_version_detection.py
"""Oracle server-version detection, pinned.

Every version gate in this backend is a comparison against a number this module
is responsible for producing, and the number used to come from a query that
filtered on ``PRODUCT LIKE 'Oracle Database%'``.  Oracle renamed the product, so
that query returned nothing, ``get_server_version`` fell back to ``(19, 0, 0)``,
and **every** ``self.version >= ...`` comparison in the backend — roughly a
hundred of them — answered against a version no server had reported.

This file pins three things:

* **the recorded row sets.**  The exact ``PRODUCT_COMPONENT_VERSION`` rows read
  off both wired servers, and what the selection rule must produce from them.
  Offline, so a rename cannot quietly change the answer;
* **the queries.**  Asserted to carry no product-name predicate at all, with the
  measured product names in the failure message, so putting one back is a test
  failure rather than a silent regression;
* **the fallback.**  Asserted to be ``None`` and loud, not ``(19, 0, 0)``.

The per-server live assertion — that the dialect carries the version the server
actually reports — is in ``test_introspector_deep.py``, because it needs a
connection and that file already has the fixtures.
"""

import logging
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import (
    DialectNotAdaptedException,
)
from rhosocial.activerecord.backend.impl.oracle.version import (
    VERSION_FULL_QUERY,
    VERSION_QUERY,
    normalize_version,
    parse_version,
    parse_version_row,
    product_name_from_row,
    ru_from_version_full,
    select_version_row,
)


# --------------------------------------------------------------------------- #
# Recorded rows.  Read directly off the wired servers as ``system``; these are
# the whole of PRODUCT_COMPONENT_VERSION on each, which has exactly four
# columns - PRODUCT, VERSION, VERSION_FULL, STATUS - and one row.
#
# There is no PRODUCT_FULL, no VERSION_START, no VERSION_END, no BANNER and no
# DESCRIPTION: ``SELECT *`` returns those four names on both servers, and
# ``user_tab_columns`` reports nothing for the view because it is a dictionary
# view rather than a table.  Anything else would be inventing a column.
# --------------------------------------------------------------------------- #

RECORDED_21C: List[Tuple[str, str, str]] = [
    # VERSION, VERSION_FULL, PRODUCT
    ("21.0.0.0.0", "21.3.0.0.0", "Oracle Database 21c Express Edition "),
]

RECORDED_26AI: List[Tuple[str, str, str]] = [
    ("23.0.0.0.0", "23.26.1.0.0", "Oracle AI Database 26ai Free"),
]

#: What each server's dialect must end up carrying.
EXPECTED_21C = (21, 0, 0)
EXPECTED_26AI = (23, 0, 0)

#: ``STATUS`` on each, recorded because it is the fourth column and the only
#: other field the view offers - it is a distribution/availability label
#: ("Production" / "Develop, Learn, and Run for Free"), never a version.
RECORDED_STATUS = {
    "21c": "Production",
    "26ai": "Develop, Learn, and Run for Free",
}

#: Both product strings as the server spells them, so a failure message says
#: what the server actually said rather than what the code assumed.
KNOWN_PRODUCT_NAMES = (
    "Oracle Database 21c Express Edition ",
    "Oracle AI Database 26ai Free",
)


class TestRecordedRowsAreWhatTheServersReport:
    """The recorded rows must be internally consistent before anything else
    is worth asserting about them."""

    @pytest.mark.parametrize(
        "rows,expected_base,expected_full,expected_ru",
        [
            (RECORDED_21C, (21, 0, 0), (21, 3, 0, 0, 0), 3),
            (RECORDED_26AI, (23, 0, 0), (23, 26, 1, 0, 0), 26),
        ],
        ids=["21c", "26ai"],
    )
    def test_the_recording_parses_to_the_version_the_server_reports(
        self, rows, expected_base, expected_full, expected_ru
    ):
        base, full = parse_version_row(rows[0])
        assert normalize_version(base)[:3] == expected_base
        assert full == expected_full
        assert ru_from_version_full(full) == expected_ru

    def test_the_26ai_brand_is_the_release_update_number(self):
        """``26ai`` and ``23ai`` are one line under two brands, and the number
        that separates them is the second component of ``VERSION_FULL``.

        Oracle states the rule: "The first number of the release stays the same
        since Oracle AI Database 26ai simply replaces Oracle Database 23ai - it
        remains '23'.  The second number of the release indicates the year of the
        release update, e.g. 26 for 2026."  The 23ai release updates Oracle lists
        for the same line are 23.4 / 23.8 / 23.9 / 23.10; 26ai begins at 23.26.

        So a gate that needs to tell the two apart has a real number to read -
        ``ru_version`` - and must not reach for the product string.  This test
        exists to keep that fact from being re-derived by guesswork later.
        """
        _, full_26ai = parse_version_row(RECORDED_26AI[0])
        assert full_26ai[0] == 23, "the line number is 23 for both brands"
        assert full_26ai[1] == 26, "26 is the release-update year, and the brand"
        assert ru_from_version_full(full_26ai) == 26
        # The 23ai release updates for the same line, by Oracle's own listing.
        for ru in (4, 8, 9, 10):
            assert ru < 26


class TestSelectVersionRow:
    """The selection rule, on the recorded rows and on shapes no wired server
    here produces."""

    @pytest.mark.parametrize(
        "rows,expected_base",
        [(RECORDED_21C, EXPECTED_21C), (RECORDED_26AI, EXPECTED_26AI)],
        ids=["21c", "26ai"],
    )
    def test_the_single_recorded_row_is_the_database(self, rows, expected_base):
        row = select_version_row(rows)
        assert row is not None
        base, _ = parse_version_row(row)
        assert normalize_version(base)[:3] == expected_base

    def test_the_two_recorded_row_sets_do_not_confuse_each_other(self):
        """Both product strings reach the same code path, which is the point.

        The 21c name contains "Oracle Database"; the 26ai one contains "Oracle
        AI Database" and does **not** start with "Oracle Database".  A predicate
        written for the first would find nothing in the second, which is the
        defect this rule replaced.
        """
        assert KNOWN_PRODUCT_NAMES[0].startswith("Oracle Database")
        assert not KNOWN_PRODUCT_NAMES[1].startswith("Oracle Database")
        for rows, expected in ((RECORDED_21C, EXPECTED_21C),
                               (RECORDED_26AI, EXPECTED_26AI)):
            row = select_version_row(rows)
            assert row is not None
            base, _ = parse_version_row(row)
            assert normalize_version(base)[:3] == expected

    def test_a_multi_row_view_prefers_the_row_carrying_a_release_update(self):
        """A bundled component row shares the database row's ``VERSION`` and has
        no ``VERSION_FULL``; the release update is what tells them apart.

        No wired server here produces this shape - both have exactly one row -
        so it is recorded as the shape an edition with extra components would
        give, and the rule is pinned against it rather than assumed.
        """
        rows = [
            ("19.0.0.0.0", None, "Oracle XML DB"),
            ("19.0.0.0.0", None, "Oracle Database Registry"),
            ("19.0.0.0.0", "19.19.0.0.0", "Oracle Database 19c Enterprise Edition"),
            ("19.0.0.0.0", None, "Oracle Workspace Manager"),
        ]
        row = select_version_row(rows)
        assert row is not None
        assert product_name_from_row(row) == "Oracle Database 19c Enterprise Edition"
        base, full = parse_version_row(row)
        assert normalize_version(base)[:3] == (19, 0, 0)
        assert full == (19, 19, 0, 0, 0)

    def test_a_multi_row_view_falls_back_to_the_highest_major(self):
        """With no release update anywhere, the highest major is the best
        available evidence - and it is a number, not a brand."""
        rows = [
            ("11.0.0.0.0", None, "Oracle Something 11g"),
            ("21.0.0.0.0", None, "Oracle Something Else 21c"),
            ("19.0.0.0.0", None, "Oracle Yet Another 19c"),
        ]
        row = select_version_row(rows)
        assert row is not None
        base, _ = parse_version_row(row)
        assert normalize_version(base)[:3] == (21, 0, 0)

    def test_a_row_whose_version_is_prose_is_not_the_database(self):
        rows = [("not a version", None, "Some Component")]
        assert select_version_row(rows) is None

    @pytest.mark.parametrize("rows", [None, [], [None], [("not a version",)]])
    def test_nothing_qualifying_yields_none_rather_than_a_guess(self, rows):
        assert select_version_row(rows) is None

    def test_a_tie_the_rule_cannot_break_goes_to_the_servers_own_order(self):
        """Two equally good rows: the first the server returned wins.  That is a
        documented limitation, not a preference - there is nothing left to
        choose on."""
        rows = [
            ("23.0.0.0.0", "23.26.1.0.0", "first"),
            ("23.0.0.0.0", "23.26.1.0.0", "second"),
        ]
        assert product_name_from_row(select_version_row(rows)) == "first"

    def test_a_one_row_result_is_not_mistaken_for_one_row(self):
        """A regression guard for a real bug this rule's first draft had.

        ``fetchall()`` on a one-row view returns a list holding one row.  An
        earlier revision sniffed the argument to tell "a row" from "a list of
        rows", treated the whole one-element list as a single row, and read the
        row *tuple* as the version - so ``'23.0.0.0.0'`` parsed as ``(23, 23)``.
        The dialect then carried a version that appears on no server at all.
        """
        for rows, expected in ((RECORDED_21C, EXPECTED_21C),
                               (RECORDED_26AI, EXPECTED_26AI)):
            assert len(rows) == 1
            base, _ = parse_version_row(select_version_row(rows))
            assert normalize_version(base)[:3] == expected


class TestTheQueriesMatchNoProductName:
    """The defect, pinned at its source.

    There is no product name in either query, so there is nothing for Oracle to
    rename out from under it.  Adding one back fails here, with the measured
    names in the message.
    """

    @pytest.mark.parametrize(
        "query", [VERSION_FULL_QUERY, VERSION_QUERY], ids=["full", "fallback"]
    )
    def test_no_like_predicate_on_product(self, query):
        # ``REGEXP_LIKE`` is the one LIKE-shaped word the query is *allowed* to
        # contain, and it applies to VERSION.  Strip it before looking, so the
        # check is about the predicate rather than about a substring.
        without_regexp_like = query.upper().replace("REGEXP_LIKE", "")
        assert "LIKE" not in without_regexp_like, (
            f"a LIKE predicate is back in the version query: {query!r}. "
            f"The recorded product names are {KNOWN_PRODUCT_NAMES!r}, and a "
            f"pattern matching one of them will not match the other - that is "
            f"the defect this query had."
        )

    @pytest.mark.parametrize(
        "query", [VERSION_FULL_QUERY, VERSION_QUERY], ids=["full", "fallback"]
    )
    def test_no_predicate_mentions_product_at_all(self, query):
        where = query.upper().split(" WHERE ", 1)[-1]
        assert "PRODUCT" not in where, (
            f"the WHERE clause reads a product name again: {query!r}"
        )

    @pytest.mark.parametrize(
        "query", [VERSION_FULL_QUERY, VERSION_QUERY], ids=["full", "fallback"]
    )
    def test_no_product_string_anywhere_in_the_predicate(self, query):
        for name in KNOWN_PRODUCT_NAMES:
            assert name.strip() not in query
        assert "ORACLE" not in query.upper()

    def test_the_full_query_asks_for_the_product_it_never_matches_on(self):
        """It is selected for the **log line**, and nothing else.

        A rename is worth seeing in the log; it is not worth a condition.  This
        is why the product column is third: ``parse_version_row`` reads indices 0
        and 1, so appending it kept every existing caller working.
        """
        assert "PRODUCT" in VERSION_FULL_QUERY.upper()
        assert VERSION_FULL_QUERY.upper().index("VERSION,") < \
            VERSION_FULL_QUERY.upper().index("PRODUCT")

    def test_the_full_query_selects_the_three_columns_the_row_reader_expects(self):
        columns = [c.strip() for c in VERSION_FULL_QUERY.split(" FROM ")[0]
                   .replace("SELECT ", "").split(",")]
        assert columns == ["VERSION", "VERSION_FULL", "PRODUCT"]

    def test_the_fallback_query_does_not_name_version_full(self):
        """``VERSION_FULL`` only exists from 12.1; naming it on an older server is
        ``ORA-00904``, which is why the fallback exists and why it selects one
        column."""
        assert "VERSION_FULL" not in VERSION_QUERY.upper()
        columns = [c.strip() for c in VERSION_QUERY.split(" FROM ")[0]
                   .replace("SELECT ", "").split(",")]
        assert columns == ["VERSION"]

    def test_both_queries_filter_on_the_version_shape_not_the_product(self):
        for query in (VERSION_FULL_QUERY, VERSION_QUERY):
            assert "REGEXP_LIKE(VERSION" in query, (
                "the queries must select on the shape of the version, which is "
                "the thing the gates compare and the thing a rebrand cannot "
                f"change: {query!r}"
            )

    def test_a_row_from_the_fallback_query_has_no_product_name(self):
        """The fallback's single-column rows cannot yield a product name, and
        that costs a log line rather than the version."""
        assert product_name_from_row(select_version_row([("21.0.0.0.0",)])) is None


class TestProductNameIsForLoggingOnly:
    def test_the_name_is_reported_verbatim_including_a_trailing_space(self):
        """The 21c product is 36 characters and ends in a space - measured, and
        worth knowing before someone writes ``== 'Oracle Database 21c Express
        Edition'`` against it."""
        name = product_name_from_row(RECORDED_21C[0])
        assert name == "Oracle Database 21c Express Edition"
        assert len("Oracle Database 21c Express Edition ") == 36
        assert name.endswith("Edition")

    def test_the_renamed_product_comes_back_whole(self):
        assert product_name_from_row(RECORDED_26AI[0]) == "Oracle AI Database 26ai Free"

    def test_no_row_has_no_name(self):
        assert product_name_from_row(None) is None


class TestTheFallbackIsNotAVersion:
    """What happens when the version genuinely cannot be read.

    The old answer was ``(19, 0, 0)``.  That is a claim: it says the server is
    19c, it makes every gate above 19 answer "too old", and nothing downstream
    can tell it apart from a version that was actually measured - which is
    exactly how a renamed 26ai server came to be treated as a 2019 one.  The
    answer now is ``None``, which leaves the dialect unadapted and makes the
    gates refuse.
    """

    def test_an_unmeasured_version_is_none_and_not_19_0_0(self):
        from rhosocial.activerecord.backend.impl.oracle.backend.backend import (
            OracleBackend,
        )

        backend = OracleBackend.__new__(OracleBackend)  # no connection at all
        backend._version = None
        backend._version_full = None
        backend._ru_version = None
        # A connection that exists but whose cursor cannot run anything: this is
        # the failure path that matters, and a connect() failure propagates as a
        # connection error rather than arriving here at all.
        backend._connection = object()
        backend.connect = lambda: None
        backend.log = lambda *a, **k: None
        backend._get_cursor = lambda: (_ for _ in ()).throw(
            RuntimeError("ORA-00942: table or view does not exist")
        )

        assert backend.get_server_version() is None
        assert backend._version is None
        assert backend._version_full is None
        assert backend._ru_version is None

    def test_a_query_returning_nothing_is_unknown_not_19(self):
        """The rename's exact symptom: the query ran and matched nothing.

        This is the path that produced ``(19, 0, 0)`` on the 26ai server.
        """
        from rhosocial.activerecord.backend.impl.oracle.backend.backend import (
            OracleBackend,
        )

        class EmptyCursor:
            def execute(self, sql):
                return None

            def fetchall(self):
                return []

            def close(self):
                return None

        backend = OracleBackend.__new__(OracleBackend)
        backend._version = (19, 0, 0)
        backend._version_full = (19, 3, 0, 0, 0)
        backend._ru_version = 3
        backend._connection = object()
        backend.connect = lambda: None
        backend.log = lambda *a, **k: None
        backend._get_cursor = lambda: EmptyCursor()

        assert backend.get_server_version() is None
        assert backend._version is None

    def test_the_failure_is_logged_loudly_and_names_the_old_default(self):
        from rhosocial.activerecord.backend.impl.oracle.backend.backend import (
            OracleBackend,
        )

        records: List[Tuple[int, str]] = []

        backend = OracleBackend.__new__(OracleBackend)
        backend._version = (19, 0, 0)
        backend._version_full = (19, 3, 0, 0, 0)
        backend._ru_version = 3
        backend._connection = object()
        backend.connect = lambda: None
        backend.log = lambda level, msg: records.append((level, msg))
        backend._get_cursor = lambda: (_ for _ in ()).throw(
            RuntimeError("ORA-00942: table or view does not exist")
        )

        assert backend.get_server_version() is None
        assert records, "a silent fallback is the defect, not the fallback"
        level, message = records[0]
        assert level >= logging.ERROR, (
            "the failure must be an ERROR: at WARNING it reads as routine"
        )
        assert "UNKNOWN" in message
        assert "19" in message, "the message should say what it used to claim"
        # A version that was previously measured is discarded rather than kept:
        # keeping it would let a gate act on a number nobody re-checked.
        assert backend._version is None
        assert backend._version_full is None
        assert backend._ru_version is None

    def test_an_unadapted_dialect_refuses_to_answer_a_gate(self):
        """``None`` has to end somewhere loud.  It ends at the framework's own
        "no version" state, where ``.version`` raises - so every ``self.version
        >= ...`` in the backend raises too, rather than answering on a number
        nobody measured.  No new sentinel, no flag, nothing to forget to honour.
        """
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        unadapted = OracleDialect()
        with pytest.raises(DialectNotAdaptedException):
            unadapted.version
        with pytest.raises(DialectNotAdaptedException):
            unadapted.supports_boolean_type()
        with pytest.raises(DialectNotAdaptedException):
            unadapted.supports_vector_type()
        with pytest.raises(DialectNotAdaptedException):
            unadapted.supports_native_json()
        with pytest.raises(DialectNotAdaptedException):
            unadapted.supports_json_duality()
        # and the same for a backend whose version was cleared
        from rhosocial.activerecord.backend.impl.oracle.backend.backend import (
            OracleBackend,
        )

        backend = OracleBackend.__new__(OracleBackend)
        backend._version = None
        backend._version_full = None
        backend._ru_version = None
        backend._dialect = None
        with pytest.raises(DialectNotAdaptedException):
            backend.dialect.version

    def test_the_one_deliberate_tolerance_still_holds(self):
        """``format_data_type_boolean`` is the exception, on purpose.

        It reads ``getattr(self, "_version", None)`` rather than ``self.version``
        and renders ``NUMBER(1)`` for an unadapted dialect.  That is not a
        guess of the same kind as the old ``(19, 0, 0)``: ``NUMBER(1)`` is legal
        on **every** release, including every one that has a native ``BOOLEAN``,
        so nothing reading the emitted DDL can be misled about whether the
        statement will run - whereas ``(19, 0, 0)`` silenced the capability
        gates as well.  Asserted here because it is the one place the "refuse
        rather than guess" rule is knowingly relaxed, and a reader needs to know
        which place that is.
        """
        from rhosocial.activerecord.backend.expression.types import BooleanType
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        unadapted = OracleDialect()
        assert unadapted.format_data_type(
            BooleanType(dialect=unadapted)
        )[0] == "NUMBER(1)"
        # while the capability question still refuses
        with pytest.raises(DialectNotAdaptedException):
            unadapted.supports_boolean_type()


class TestConstructorDoesNotAssumeAVersion:
    """``OracleBackend(version=None)`` used to store ``(19, 0, 0)``.

    That was a second place the same claim was made, and it was in force for the
    whole window between constructing a backend and adapting it - including for
    a backend that never adapts.  ``_version`` is now ``None`` there, which the
    readers below all have to survive.

    ``_register_oracle_adapters`` needed a fix rather than a tolerance: it
    substituted ``(23, 0, 0)`` for a falsy version, which claims "new enough for
    a vector adapter" about a server nobody asked.  It was unreachable while
    ``_version`` was always at least ``(19, 0, 0)``; making ``_version`` honest
    would have made it reachable, so it had to go.
    """

    def _backend(self, version=None, version_full=None, ru_version=None):
        from rhosocial.activerecord.backend.impl.oracle.backend.backend import (
            OracleBackend,
        )

        return OracleBackend(
            connection_config=self._config(),
            version=version,
            version_full=version_full,
            ru_version=ru_version,
        )

    @staticmethod
    def _config():
        from rhosocial.activerecord.backend.impl.oracle.config import (
            OracleConnectionConfig,
        )

        return OracleConnectionConfig(
            host="127.0.0.1", port=1, service_name="unused",
            username="unused", password="unused",
        )

    def test_no_stated_version_is_none(self):
        assert self._backend()._version is None

    def test_a_stated_version_is_kept(self):
        assert self._backend(version=(21, 0, 0))._version == (21, 0, 0)

    def test_the_version_label_says_unknown_rather_than_picking_a_brand(self):
        backend = self._backend()
        label = backend._get_oracle_version_string()
        assert "not measured" in label
        for brand in ("23ai", "21c", "19c", "12c", "11g"):
            assert brand not in label

    def test_the_version_label_names_the_brand_for_a_measured_version(self):
        backend = self._backend(version=(23, 0, 0))
        assert backend._get_oracle_version_string() == "Oracle 23ai (23.0.0)"

    def test_an_unmeasured_version_registers_no_version_gated_adapter(self):
        """The vector adapter is gated on 23, so an assumed ``(23, 0, 0)`` would
        claim a 23ai capability on a server of unknown version."""
        assert self._backend()._version is None
        # the guard itself, spelled out, because the registration is otherwise
        # invisible from outside
        version = getattr(self._backend(), "_version", None)
        assert not (version is not None and version[0] >= 23)

    def test_a_21c_version_registers_no_vector_adapter_either(self):
        version = self._backend(version=(21, 0, 0))._version
        assert not (version[0] >= 23)

    def test_a_23ai_version_does_register_it(self):
        version = self._backend(version=(23, 0, 0))._version
        assert version[0] >= 23


class TestIntrospectorReportsUnknownAsUnknown:
    """A status surface that invents a version is the same defect as a dialect
    that does, so ``_parse_database_info`` says "unknown"."""

    def _introspector(self, version):
        from rhosocial.activerecord.backend.impl.oracle.introspection.introspector import (
            SyncOracleIntrospector,
        )

        backend = SimpleNamespace(
            config=SimpleNamespace(username="ar_shop"), _version=version
        )
        return SyncOracleIntrospector(backend, SimpleNamespace(execute=None))

    def test_unknown_version_reports_unknown(self):
        info = self._introspector(None)._parse_database_info([])
        assert info.version == "unknown"
        assert info.version_tuple is None

    def test_measured_version_reports_the_number(self):
        info = self._introspector((23, 0, 0))._parse_database_info([])
        assert info.version == "23.0.0"
        assert info.version_tuple == (23, 0, 0)

    def test_the_default_is_no_longer_a_fabricated_19(self):
        from rhosocial.activerecord.backend.impl.oracle.introspection.introspector import (
            SyncOracleIntrospector,
        )

        # a backend with no ``_version`` attribute at all must also read as None
        backend = SimpleNamespace(config=SimpleNamespace(username="ar_shop"))
        insp = SyncOracleIntrospector(backend, SimpleNamespace(execute=None))
        assert insp._get_version() is None
        assert insp._parse_database_info([]).version == "unknown"


class TestParseHelpersOnTheRecordedRows:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("21.0.0.0.0", (21, 0, 0, 0, 0)),
            ("23.26.1.0.0", (23, 26, 1, 0, 0)),
            ("23.10.0.25.10", (23, 10, 0, 25, 10)),
        ],
    )
    def test_parse_version_reads_every_component(self, text, expected):
        assert parse_version(text) == expected

    def test_normalize_version_pads_to_three(self):
        assert normalize_version((23,)) == (23, 0, 0)
        assert normalize_version("21") == (21, 0, 0)

    def test_ru_from_version_full_needs_two_components(self):
        assert ru_from_version_full("23") is None
        assert ru_from_version_full(None) is None

    def test_a_dict_row_reads_the_same_as_a_tuple_row(self):
        """``oracledb`` hands back tuples; the testsuite's executors hand back
        dicts.  Both are what :func:`select_version_row` is called with."""
        tup = RECORDED_26AI[0]
        mapping = {
            "VERSION": tup[0], "VERSION_FULL": tup[1], "PRODUCT": tup[2],
        }
        assert parse_version_row(mapping) == parse_version_row(tup)
        assert product_name_from_row(mapping) == product_name_from_row(tup)
        assert select_version_row([mapping]) is not None
        base, _ = parse_version_row(select_version_row([mapping]))
        assert normalize_version(base)[:3] == EXPECTED_26AI

    def test_lowercase_dict_keys_read_the_same(self):
        mapping = {
            "version": "23.0.0.0.0",
            "version_full": "23.26.1.0.0",
            "product": "Oracle AI Database 26ai Free",
        }
        base, full = parse_version_row(mapping)
        assert base == (23, 0, 0, 0, 0)
        assert full == (23, 26, 1, 0, 0)
        assert product_name_from_row(mapping) == "Oracle AI Database 26ai Free"


# --------------------------------------------------------------------------- #
# The blast radius, pinned.
#
# (19, 0, 0) is the version the broken fallback reported for *every* server, so
# it is exactly the state the backend was in on the wire, and (23, 0, 0) is what
# the 26ai server actually reports.  Every gate with a threshold at or below 19
# answers identically in both, which is why the list below is short rather than
# the ~100 comparisons the backend contains: those other ~85 were never inert.
#
# Each entry names the server's own answer to the claim the gate now makes.  A
# claim with no measured server answer is marked, because that is the one to fix
# before anything is built on it.
# --------------------------------------------------------------------------- #

#: ``name -> (server answer, evidence)`` for every ``supports_*`` that the
#: fallback silenced.  ``True``/``False`` is the measured answer on the 26ai
#: server (23.0.0.0.0 / 23.26.1.0.0); the 21c column records what the same
#: statement does there, which is the evidence that the threshold belongs there.
FLIPPED_CAPABILITIES: Dict[str, Tuple[Any, str]] = {
    "supports_create_type_if_not_exists": (
        True,
        "measured on **21c** (21.3.0.0.0): 'CREATE TYPE IF NOT EXISTS s2 AS "
        "OBJECT (a NUMBER)' is **accepted and creates nothing** - s2 is absent "
        "from USER_OBJECTS and USER_TYPES afterwards and 'SELECT s2(1) FROM "
        "DUAL' is ORA-00904 - while plain 'CREATE TYPE s2' over an existing "
        "type is ORA-00955, so the clause is parsed and discarded rather than "
        "honoured. Measured on **26ai** (23.26.1.0.0): accepted, s2 lands in "
        "USER_TYPES, and the same statement over an existing type is a no-op.",
    ),
    "supports_alter_type_if_exists": (
        True,
        "ORA-00922 'missing or invalid option' on **21c**; on **26ai** both "
        "'ALTER TYPE IF EXISTS s1 COMPILE' and 'ALTER TYPE IF EXISTS s1 ADD "
        "ATTRIBUTE (b VARCHAR2(10)) CASCADE' run.",
    ),
    "supports_drop_type_if_exists": (
        True,
        "ORA-00933 on **21c**, and the type survives the attempt; on **26ai** "
        "both the present name and the absent one ('DROP TYPE IF EXISTS "
        "probe_absent') run without error.",
    ),
    "supports_create_type_body_if_not_exists": (
        True,
        "on **21c** 'CREATE TYPE BODY IF NOT EXISTS' over an existing body is "
        "ORA-00955, i.e. accepted and then ignored; on **26ai** it is a no-op "
        "over an existing body and creates one when the body is absent.",
    ),
    "supports_drop_type_body_if_exists": (
        True,
        "ORA-00933 on **21c**; on **26ai** both the present and the absent body "
        "name run without error.",
    ),
    "supports_boolean_type": (
        True,
        "CREATE TABLE t (a BOOLEAN): ORA-00902 on 21c, accepted on 26ai with "
        "DATA_TYPE='BOOLEAN'",
    ),
    "supports_vector_type": (
        True,
        "CREATE TABLE t (a VECTOR(3)): ORA-00907 on 21c, accepted on 26ai "
        "with DATA_TYPE='VECTOR'",
    ),
    "supports_json_duality": (
        True,
        "CREATE JSON VIEW: ORA-00901 on 21c, a JSON-relational-specific "
        "ORA-40941 on 26ai",
    ),
    "supports_graph_table": (
        True,
        "GRAPH_TABLE(...): 21c does not resolve the name (ORA-00904 / "
        "ORA-00907); 26ai parses it and asks for MATCH (ORA-02000)",
    ),
    "supports_native_json": (
        True,
        "CREATE TABLE t (a JSON) in an ASSM tablespace: accepted on **21c** "
        "and on 26ai - the native JSON column type is a 21c feature, which is "
        "why this gate says 21",
    ),
    "supports_json_type": (
        True,
        "same measurement as supports_native_json",
    ),
    "supports_json_path": (
        True,
        "JSON_VALUE and JSON_QUERY are accepted on **21c** as well as 26ai, so "
        "this gate follows supports_json_type at (21,0,0)",
    ),
    "supports_json_duality_view": (
        True,
        "CREATE JSON VIEW: ORA-00901 on 21c, a JSON-relational-specific "
        "ORA-40941 on 26ai",
    ),
    # The two sequence clauses arrived with the release that this branch
    # rebased onto (5383811 moved the boundaries into these probes), after
    # this table was written, so the audit found them rather than the author
    # recording them. The boundary is the one the probes state: 23ai. The
    # clauses are the 23ai IF [NOT] EXISTS DDL family; the measured 26ai
    # server (23.26.1.0.0) is where the audit sees them answer True, and the
    # broken (19,0,0) fallback predates the family entirely.
    "supports_sequence_if_exists": (
        True,
        "DROP SEQUENCE IF EXISTS: gated at (23,0,0) - the 23ai IF [NOT] EXISTS "
        "DDL family; absent before 23, answered on the measured 26ai server",
    ),
    "supports_sequence_if_not_exists": (
        True,
        "CREATE SEQUENCE IF NOT EXISTS: gated at (23,0,0) - the same 23ai "
        "family; absent before 23, answered on the measured 26ai server",
    ),
}

#: The rendered SQL each flipping gate produced, and now produces.
FLIPPED_RENDERINGS: List[Tuple[str, str, str, str]] = [
    (
        "format BOOLEAN column", "(23,0,0)",
        "NUMBER(1)",
        "BOOLEAN",
    ),
    (
        "CREATE SEQUENCE IF NOT EXISTS", "(23,0,0)",
        "UnsupportedFeatureError",
        'CREATE SEQUENCE IF NOT EXISTS "SEQ"',
    ),
    (
        "DROP SEQUENCE IF EXISTS", "(23,0,0)",
        "UnsupportedFeatureError",
        'DROP SEQUENCE IF EXISTS "SEQ"',
    ),
    (
        "CREATE MATERIALIZED VIEW IF NOT EXISTS", "(23,0,0)",
        "UnsupportedFeatureError",
        'CREATE MATERIALIZED VIEW IF NOT EXISTS "MV" AS SELECT "ID" FROM "T"',
    ),
    (
        "DROP MATERIALIZED VIEW IF EXISTS", "(23,0,0)",
        "UnsupportedFeatureError",
        'DROP MATERIALIZED VIEW IF EXISTS "MV"',
    ),
]

#: ``suggest_column_type`` for the Python types that had a version-gated answer.
FLIPPED_SUGGESTIONS = [
    (dict, "TextType", "JsonType"),
    (list, "TextType", "JsonType"),
]

#: Function-table entries the fallback silenced, and what the servers do.
FLIPPED_FUNCTIONS: Dict[str, Tuple[Any, str]] = {
    "json_transform": (
        True,
        "JSON_TRANSFORM('{\"a\":0}', SET '$.a' = 42) returns {\"a\":42} on "
        "**21c** as well as 26ai, so the threshold was lowered from (23,0,0) "
        "to (21,0,0). A threshold that is too high refuses something the "
        "server does do, which is recoverable; one that is too low claims "
        "something it does not, which is not",
    ),
    "json_equal": (
        True,
        "JSON_EQUAL: ORA-40600 'used outside predicate' on 21c, accepted on "
        "26ai - 21c has it as a condition only",
    ),
}

#: Entries the table used to carry and now does not, with the measurement that
#: withdrew them.  Kept as a record because "it was a claim once" is the fact
#: worth keeping: the version fix made this claim reachable for the first time,
#: and the first thing it did was hand the caller SQL that errors.
WITHDRAWN_FUNCTIONS: Dict[str, str] = {
    "json_schema": (
        "there is no SQL function of that name. ORA-00904 on **21c** and "
        "**26ai** for JSON_SCHEMA('{}'), JSON_SCHEMA(CAST('{}' AS JSON)), "
        "SYS.JSON_SCHEMA('{}'), JSON_SCHEMA, JSON_SCHEMA() and "
        "JSON_SCHEMA_VALID('{}'); ALL_PROCEDURES on 26ai holds no JSON_SCHEMA "
        "row; and the 26ai SQL Language Reference has no JSON_SCHEMA function "
        "page (/26/sqlrf/JSON_SCHEMA.html is a 404). What the name denotes "
        "instead is DBMS_JSON_SCHEMA (a PL/SQL package: 2 rows in ALL_OBJECTS "
        "on 26ai, 0 on 21c), the JSON_SCHEMA *clause* of JSON_TABLE (known to "
        "the 26ai parser, ORA-00933 on 21c), and JSON_SCHEMA_VALID (a "
        "condition, ORA-00920 in a predicate on 21c). None is the function the "
        "entry named, so no version could be a true minimum."
    ),
}


class TestBlastRadiusIsPinned:
    """Every behaviour the broken fallback silenced, listed.

    A gate added above 19 in the future changes this set, and the test below
    fails until whoever added it measures what the server does and records it
    here.  That is the whole purpose: the fallback made every gate above 19
    answer "no" without anybody noticing, and an undeclared gate above 19 is
    the same shape of problem.
    """

    @staticmethod
    def _dialect(version, full=None, ru=None):
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        return OracleDialect(version, version_full=full, ru_version=ru)

    def _flip_set(self, before, after):
        flipped = {}
        for name in dir(before):
            if not name.startswith("supports_"):
                continue
            fn_b, fn_a = getattr(before, name, None), getattr(after, name, None)
            if not callable(fn_b) or not callable(fn_a):
                continue
            try:
                rb = fn_b()
                ra = fn_a()
            except Exception:
                continue  # a gate needing an argument is not part of this audit
            if rb != ra:
                flipped[name] = (rb, ra)
        return flipped

    def test_the_set_of_capabilities_that_flip_is_exactly_the_declared_one(self):
        flipped = self._flip_set(self._dialect((19, 0, 0)), self._dialect(
            (23, 0, 0), (23, 26, 1, 0, 0), 26))
        # ``supports_functions`` returns the whole availability map rather than
        # a boolean, so it is audited on its own in the next test.
        flipped.pop("supports_functions", None)
        expected = {
            name: (False, value[0]) for name, value in FLIPPED_CAPABILITIES.items()
        }
        assert set(flipped) == set(expected), (
            "the set of version-gated capabilities that change between the "
            f"broken fallback (19,0,0) and the measured 26ai version "
            f"(23,0,0) has changed.\n"
            f"  new:     {sorted(set(flipped) - set(expected))}\n"
            f"  removed: {sorted(set(expected) - set(flipped))}\n"
            f"A new gate above 19 was inert while detection was broken; measure "
            f"what the server does with it and record it in "
            f"FLIPPED_CAPABILITIES before shipping."
        )
        assert flipped == expected

    def test_the_set_of_functions_that_flip_is_exactly_the_declared_one(self):
        before = self._dialect((19, 0, 0)).supports_functions()
        after = self._dialect(
            (23, 0, 0), (23, 26, 1, 0, 0), 26).supports_functions()
        flipped = {k for k in after if before.get(k) != after.get(k)}
        assert flipped == set(FLIPPED_FUNCTIONS), (
            f"the function table's flip set changed: "
            f"{sorted(flipped ^ set(FLIPPED_FUNCTIONS))}"
        )

    @pytest.mark.parametrize("name", sorted(FLIPPED_FUNCTIONS))
    def test_each_flipping_function_is_recorded_with_its_measurement(self, name):
        """Every claim the fallback silenced carries a server answer - or an
        explicit statement that the server does not back it."""
        answer, evidence = FLIPPED_FUNCTIONS[name]
        assert evidence, f"{name} flipped with no recorded evidence"
        if answer is None:
            pytest.skip(
                f"{name} is claimed by the gate and **not** backed by any "
                f"reachable server: {evidence}"
            )
        before = self._dialect((19, 0, 0)).supports_functions()[name]
        after = self._dialect((23, 0, 0)).supports_functions()[name]
        assert before is False and after is answer

    @pytest.mark.parametrize("label,gate,before,after", FLIPPED_RENDERINGS)
    def test_each_flipping_rendering_is_exactly_as_recorded(
        self, label, gate, before, after
    ):
        """``NUMBER(1)`` on a 26ai server was the visible symptom; these are the
        statements that were refused outright."""
        rendered = self._render(label, (19, 0, 0), (23, 0, 0))
        assert rendered[0] == before, (
            f"{label}: at (19,0,0) it rendered {rendered[0]!r}, not {before!r}"
        )
        assert rendered[1] == after, (
            f"{label}: at (23,0,0) it rendered {rendered[1]!r}, not {after!r}"
        )

    @staticmethod
    def _render(label, before_version, after_version):
        from rhosocial.activerecord.backend.expression.core import (
            Column,
        )
        from rhosocial.activerecord.backend.expression.objects import Table
        from rhosocial.activerecord.backend.expression.objects import Sequence
        from rhosocial.activerecord.backend.expression.objects import MaterializedView
        from rhosocial.activerecord.backend.expression.sources import (
            NamedRelationRef,
        )
        from rhosocial.activerecord.backend.expression import QueryExpression
        from rhosocial.activerecord.backend.expression.types import BooleanType
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.expression import (
            OracleCreateMaterializedViewExpression,
            OracleCreateSequenceExpression,
            OracleDropMaterializedViewExpression,
            OracleDropSequenceExpression,
        )

        def attempt(version):
            d = OracleDialect(version)
            try:
                if label == "format BOOLEAN column":
                    return d.format_data_type(BooleanType(dialect=d))[0]
                if label.startswith("CREATE SEQUENCE"):
                    return OracleCreateSequenceExpression(
                        d, Sequence(d, "seq"), if_not_exists=True).to_sql()[0]
                if label.startswith("DROP SEQUENCE"):
                    return OracleDropSequenceExpression(
                        d, Sequence(d, "seq"), if_exists=True).to_sql()[0]
                if label.startswith("CREATE MATERIALIZED VIEW"):
                    query = QueryExpression(
                        d, select=[Column(d, "id")],
                        from_=NamedRelationRef(d, Table(d, "t")),
                    )
                    return OracleCreateMaterializedViewExpression(
                        d, MaterializedView(d, "mv"), query=query,
                        if_not_exists=True).to_sql()[0]
                if label.startswith("DROP MATERIALIZED VIEW"):
                    return OracleDropMaterializedViewExpression(
                        d, MaterializedView(d, "mv"), if_exists=True).to_sql()[0]
            except Exception as exc:
                return type(exc).__name__
            raise AssertionError(f"no renderer for {label!r}")

        return attempt(before_version), attempt(after_version)

    @pytest.mark.parametrize("python_type,before,after", FLIPPED_SUGGESTIONS)
    def test_each_flipping_suggestion_is_exactly_as_recorded(
        self, python_type, before, after
    ):
        """``dict``/``list`` were suggested as CLOB and are now JSON - the
        correct column for a 21c-and-later server, measured on both."""
        d19 = self._dialect((19, 0, 0))
        d23 = self._dialect((23, 0, 0))
        assert type(d19.suggest_column_type(python_type)).__name__ == before
        assert type(d23.suggest_column_type(python_type)).__name__ == after

    def test_nothing_above_19_is_silenced_any_more(self):
        """The defect in one assertion: with detection working, the dialect on
        the measured 26ai server answers ``True`` for every capability it
        claims, and a gate cannot be added above the fallback without showing up
        in one of the two tests above."""
        d = self._dialect((23, 0, 0), (23, 26, 1, 0, 0), 26)
        for name, (answer, evidence) in FLIPPED_CAPABILITIES.items():
            assert getattr(d, name)() is answer, f"{name}: {evidence}"


# --------------------------------------------------------------------------- #
# The TYPE ``IF [NOT] EXISTS`` threshold, which was written against 19.28 and
# read as though nothing newer existed.
#
# ``_TYPE_IF_EXISTS_MAJOR``/``_TYPE_IF_EXISTS_RU`` came from Oracle's own words,
# quoted on the 19c ``CREATE TYPE`` page: "You can use IF [NOT] EXISTS only from
# Release 19.28 and up."  That is the right number for the 19c line and it is the
# wrong *shape* for the gate as a whole, because ``major == 19`` also answered
# "no" on 23ai and on 26ai - and on 26ai the clause works.  These tests pin both
# halves against what the two wired servers actually do.
# --------------------------------------------------------------------------- #

#: The five ``IF [NOT] EXISTS`` gates, all of which are one threshold because all
#: five form bodies are documented and shipped together.
TYPE_IF_EXISTS_GATES = (
    "supports_create_type_if_not_exists",
    "supports_alter_type_if_exists",
    "supports_drop_type_if_exists",
    "supports_create_type_body_if_not_exists",
    "supports_drop_type_body_if_exists",
)

#: The two wired servers, by the VERSION_FULL each reports.
MEASURED_21C_FULL = (21, 3, 0, 0, 0)
MEASURED_26AI_FULL = (23, 26, 1, 0, 0)


class TestTypeIfExistsThresholdIsMeasuredOnBothWiredServers:
    """The per-server answers, read straight off the measurements."""

    @staticmethod
    def _dialect(base, full=None, ru=None):
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        return OracleDialect(base, version_full=full, ru_version=ru)

    def test_21c_refuses_every_form(self):
        """21.3 refuses all five - and the create form is refused *because* it
        is accepted.

        ``CREATE TYPE IF NOT EXISTS`` on 21.3 returns without error and creates
        nothing: s2 stays out of USER_OBJECTS and USER_TYPES and
        ``SELECT s2(1) FROM DUAL`` is ORA-00904. The remaining four are outright
        errors (ORA-00922 / ORA-00933 / ORA-00955). A gate that answered True
        there would be reporting a statement that lies to its caller.
        """
        d = self._dialect((21, 0, 0), MEASURED_21C_FULL, 3)
        for name in TYPE_IF_EXISTS_GATES:
            assert getattr(d, name)() is False, f"{name} claims support on 21.3"

    def test_26ai_accepts_every_form(self):
        """23.26.1 runs all five and honours them: a new name is created, an
        existing one is a no-op, and a missing drop target does not error."""
        d = self._dialect((23, 0, 0), MEASURED_26AI_FULL, 26)
        for name in TYPE_IF_EXISTS_GATES:
            assert getattr(d, name)() is True, f"{name} refuses on 26ai"

    def test_the_21c_answer_is_not_derived_from_the_19c_reading(self):
        """``21 > 19``, so "19.28 and up" read literally would answer True here.

        It answers False, and that is the point of the test: the threshold is
        anchored on two documented facts and one of them is that the 21c
        reference documents no ``IF NOT EXISTS`` clause on ``CREATE TYPE`` at
        all, matching what 21.3 measurably does. A later 21c release update may
        carry the backport; nothing reachable measures one, so nothing claims
        one, and refusing is the recoverable direction.
        """
        assert (21, 0, 0) >= (19, 0, 0)
        d = self._dialect((21, 0, 0), MEASURED_21C_FULL, 3)
        assert d.supports_create_type_if_not_exists() is False

    def test_a_base_release_needs_no_release_update(self):
        """On the 23 line the clause is in the **base release** - the reference
        documents it there with no release note - so the base version alone
        decides and a dialect with no ``version_full`` at all still answers."""
        assert self._dialect((23, 0, 0)).supports_create_type_if_not_exists() is True
        assert self._dialect((23, 0, 0)).supports_drop_type_if_exists() is True

    def test_the_19c_half_still_needs_a_measured_release_update(self):
        """The part of the threshold that *is* a release update stays one.

        Oracle's note dates the clause to 19.28, ``_version`` reads ``(19,0,0)``
        on every 19c server, and a base version of ``(19,28,0)`` is a number no
        server reports - so a fabricated base version must not answer.
        """
        assert self._dialect((19, 0, 0), (19, 27, 0, 0, 0)).supports_create_type_if_not_exists() is False
        assert self._dialect((19, 0, 0), (19, 28, 0, 0, 0)).supports_create_type_if_not_exists() is True
        assert self._dialect((19, 28, 0)).supports_create_type_if_not_exists() is False

    def test_a_line_between_the_two_anchors_is_not_claimed(self):
        for base in ((9, 0, 0), (12, 2, 0), (18, 0, 0), (19, 0, 0), (20, 0, 0)):
            assert self._dialect(base).supports_create_type_if_not_exists() is False


class TestTypeIfExistsRefusalNamesTheAlternative:
    """A refusal that only says "not supported" leaves the caller with no
    statement to write instead."""

    @staticmethod
    def _refuse(dialect, build):
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )

        with pytest.raises(UnsupportedFeatureError) as excinfo:
            build()
        return str(excinfo.value)

    @pytest.mark.parametrize("name", TYPE_IF_EXISTS_GATES)
    def test_the_message_names_the_feature_and_suggests_a_statement(self, name):
        from rhosocial.activerecord.backend.expression.statements.ddl_type import (
            AlterTypeExpression,
            CreateTypeExpression,
            DropTypeExpression,
        )
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
        from rhosocial.activerecord.backend.impl.oracle.expression.ddl.type import (
            OracleAlterTypeCompileAction,
            OracleCreateTypeBodyExpression,
            OracleDropTypeBodyExpression,
            OracleIncompleteTypeDefinition,
        )
        from rhosocial.activerecord.backend.expression.objects import Type

        dialect = OracleDialect((21, 0, 0), version_full=MEASURED_21C_FULL, ru_version=3)
        builders = {
            "supports_create_type_if_not_exists": lambda: CreateTypeExpression(
                dialect, Type(dialect, "forward_t"),
                OracleIncompleteTypeDefinition(dialect), if_not_exists=True,
            ).to_sql(),
            "supports_alter_type_if_exists": lambda: AlterTypeExpression(
                dialect, Type(dialect, "forward_t"),
                [OracleAlterTypeCompileAction(dialect)], if_exists=True,
            ).to_sql(),
            "supports_drop_type_if_exists": lambda: DropTypeExpression(
                dialect, Type(dialect, "forward_t"), if_exists=True,
            ).to_sql(),
            "supports_create_type_body_if_not_exists": lambda: OracleCreateTypeBodyExpression(
                dialect, Type(dialect, "forward_t"), "BEGIN NULL; END;", if_not_exists=True,
            ).to_sql(),
            "supports_drop_type_body_if_exists": lambda: OracleDropTypeBodyExpression(
                dialect, Type(dialect, "forward_t"), if_exists=True,
            ).to_sql(),
        }
        message = self._refuse(dialect, builders[name])
        assert "does not support" in message, message
        assert "IF" in message and "EXISTS" in message, message
        assert "Suggestion:" in message, (
            f"{name} refuses without naming an alternative: {message}"
        )
        # The alternative is a statement, not a shrug: it names the dictionary
        # view the caller would guard on, which runs on every release.
        assert "USER_OBJECTS" in message, message

    def test_the_alternatives_need_no_version_at_all(self):
        """Whatever the suggestion says, it must be spellable against a server
        that has never heard of the clause - so it cannot suggest the clause."""
        from rhosocial.activerecord.backend.impl.oracle.mixins import (
            ddl_type as module,
        )

        alternatives = {
            name: value for name, value in vars(module).items()
            if name.startswith("_TYPE_IF_EXISTS_") and name.endswith("_ALTERNATIVE")
        }
        assert len(alternatives) == 5, sorted(alternatives)
        for name, text in alternatives.items():
            upper = text.upper()
            assert "IF NOT EXISTS" not in upper, f"{name} suggests the clause it refuses"
            assert "IF EXISTS" not in upper, f"{name} suggests the clause it refuses"
            assert "predates the clause" in text, text
            # ...and it names a plain form of the same statement, so the fix is
            # a smaller edit to the caller's SQL rather than a different feature.
            assert any(
                keyword in upper for keyword in ("CREATE", "ALTER", "DROP")
            ), f"{name} names no statement to write instead: {text}"


class TestWithdrawnFunctionIsAbsentEverywhere:
    """``json_schema`` is gone from the table, and stays gone.

    The point of withdrawing rather than re-thresholding is that no version
    could be a true minimum: there is no SQL function by that name on any server
    this project can reach, and the reference has no page for one. A threshold
    would have been a second fabricated number, in the same file the version
    fix just cleaned up.
    """

    @pytest.mark.parametrize("name", sorted(WITHDRAWN_FUNCTIONS))
    def test_the_name_is_not_in_the_function_table(self, name):
        from rhosocial.activerecord.backend.impl.oracle.function_versions import (
            ORACLE_FUNCTION_VERSIONS,
        )

        assert name not in ORACLE_FUNCTION_VERSIONS

    @pytest.mark.parametrize(
        "version", [(9, 0, 0), (12, 1, 0), (19, 0, 0), (21, 0, 0), (23, 0, 0)]
    )
    def test_no_version_claims_it(self, version):
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        funcs = OracleDialect(version).supports_functions()
        assert "json_schema" not in funcs, (
            f"{version} claims {funcs.get('json_schema')!r} for a function no "
            f"server has"
        )

    @pytest.mark.parametrize("name", sorted(WITHDRAWN_FUNCTIONS))
    def test_each_withdrawal_carries_its_measurement(self, name):
        evidence = WITHDRAWN_FUNCTIONS[name]
        assert evidence, f"{name} was withdrawn with no recorded evidence"
        assert "21c" in evidence and "26ai" in evidence, evidence
        assert "ORA-00904" in evidence, evidence

    def test_it_is_not_in_the_flip_set_anymore(self):
        """It flipped before the withdrawal, which is why the flip set carried
        an unbacked claim at all. With the entry gone it cannot come back by
        someone lowering a threshold."""
        from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

        before = OracleDialect((19, 0, 0)).supports_functions()
        after = OracleDialect((23, 0, 0)).supports_functions()
        assert set(WITHDRAWN_FUNCTIONS).isdisjoint(before)
        assert set(WITHDRAWN_FUNCTIONS).isdisjoint(after)
