# tests/rhosocial/activerecord_oracle_test/feature/backend/test_introspection_parser_mapping.py
"""What introspection reports *is* what the catalogue said, not an assumption.

Five answers used to be conventions rather than facts: every listed object was
a base table, a column's "character length" was whatever byte count was at hand,
no index was ever a primary key, a package had no owner, and a view had no
columns. Each is a claim a caller acts on -- a differ that thinks a view is a
table, a length compared against the wrong unit -- so each is pinned here.

Everything is offline: the statements are rendered and inspected as SQL, and
the ``parse_*`` helpers are driven with synthetic data-dictionary rows. No
connection is opened, because every defect is in the mapping between the two,
not in the server.
"""

import inspect
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.introspection.introspector import (
    AsyncOracleIntrospector,
    SyncOracleIntrospector,
)
from rhosocial.activerecord.backend.introspection.types import TableType


# --------------------------------------------------------------------- #
# Offline collaborators
# --------------------------------------------------------------------- #


class StubBackend:
    """The backend surface the introspector mixin actually reads."""

    def __init__(
        self, username: str = "ar_crm", version: Optional[Tuple[int, ...]] = (23, 0, 0)
    ) -> None:
        self.config = SimpleNamespace(username=username)
        self._version = version
        self.dialect = OracleDialect(version=version)

    def get_cached_server_version(self) -> Optional[Tuple[int, ...]]:
        """The I/O-free version the introspector's status surface reads."""
        return self._version


class QuestionRecorder:
    """Replays a fixed row set per dictionary view and remembers every question.

    ``answers`` maps an upper-case marker (``ALL_MVIEWS``, ``ALL_TAB_COLUMNS``,
    …) to the rows that query returns. The first marker found in the SQL wins,
    which is enough for queries that each read one family.
    """

    def __init__(self, answers: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> None:
        self.answers = answers or {}
        self.questions: List[Tuple[str, Tuple]] = []

    def _answer(self, sql: str) -> List[Dict[str, Any]]:
        upper = sql.upper()
        for marker, rows in self.answers.items():
            if marker in upper:
                return rows
        return []

    def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        self.questions.append((sql, params))
        return self._answer(sql)


class AsyncQuestionRecorder(QuestionRecorder):
    """The async twin of :class:`QuestionRecorder`."""

    async def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        self.questions.append((sql, params))
        return self._answer(sql)


def make_sync(
    answers: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    version: Optional[Tuple[int, ...]] = (23, 0, 0),
) -> Tuple[SyncOracleIntrospector, QuestionRecorder]:
    recorder = QuestionRecorder(answers)
    return SyncOracleIntrospector(StubBackend(version=version), recorder), recorder


def make_async(
    answers: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Tuple[AsyncOracleIntrospector, AsyncQuestionRecorder]:
    recorder = AsyncQuestionRecorder(answers)
    return AsyncOracleIntrospector(StubBackend(), recorder), recorder


def squash(sql: str) -> str:
    """Lower-case, whitespace-free, quote-free SQL for shape assertions."""
    return "".join(sql.lower().split()).replace('"', "")


# --------------------------------------------------------------------- #
# 1. The object kind travels from the statement to the parsed value
# --------------------------------------------------------------------- #


TABLE_KIND_ROWS = [
    {"TABLE_NAME": "CUSTOMERS", "TABLE_TYPE": "TABLE"},
    {"TABLE_NAME": "V_ACTIVE", "TABLE_TYPE": "VIEW"},
    {"TABLE_NAME": "MV_TOTALS", "TABLE_TYPE": "MATERIALIZED VIEW"},
]


class TestObjectKindIsReported:
    def test_the_statement_reads_each_family_and_names_the_kind(self):
        """One side per kind, each projecting its own ``TABLE_TYPE``."""
        insp, _ = make_sync()
        sql, _ = insp.table_list_statement(
            schema="ar_crm", include_system=False, include_views=True
        ).to_sql()
        squashed = squash(sql)
        assert "all_tables" in squashed
        assert "all_views" in squashed
        assert "all_mviews" in squashed
        assert "'table'" in squashed
        assert "'view'" in squashed
        assert "'materializedview'" in squashed
        assert "unionall" in squashed
        # The schema is request data and never part of the text.
        assert "ar_crm" not in sql.lower()

    def test_the_table_side_subtracts_materialized_views(self):
        """``ALL_TABLES`` lists an MV as a table, so it must be taken back out.

        Otherwise the union reports one materialized view twice: once as a
        table and once as the materialized view it actually is.
        """
        insp, _ = make_sync()
        sql, params = insp.table_list_statement(
            schema="ar_crm", include_system=False, include_views=False
        ).to_sql()
        squashed = squash(sql)
        assert "all_tables" in squashed
        assert "all_mviews" in squashed
        assert "all_views" not in squashed
        assert "not(t.table_namein(select" in squashed
        assert params[0] == "AR_CRM"

    @pytest.mark.parametrize(
        "kind,expected",
        [
            ("TABLE", TableType.BASE_TABLE),
            ("VIEW", TableType.VIEW),
            ("MATERIALIZED VIEW", TableType.MATERIALIZED_VIEW),
            ("materialized view", TableType.MATERIALIZED_VIEW),
        ],
    )
    def test_the_parser_maps_the_kind_it_was_given(self, kind, expected):
        insp, _ = make_sync()
        (table,) = insp.parse_tables(
            [{"TABLE_NAME": "SOMETHING", "TABLE_TYPE": kind}], "AR_CRM"
        )
        assert table.table_type is expected

    def test_every_kind_in_one_answer_stays_distinct(self):
        insp, _ = make_sync()
        tables = insp.parse_tables(TABLE_KIND_ROWS, "AR_CRM")
        assert [t.name for t in tables] == ["CUSTOMERS", "V_ACTIVE", "MV_TOTALS"]
        assert [t.table_type for t in tables] == [
            TableType.BASE_TABLE,
            TableType.VIEW,
            TableType.MATERIALIZED_VIEW,
        ]

    def test_a_row_without_a_kind_is_still_a_table(self):
        """Compatibility for a recorded row set that never projected the kind.

        The statement always projects it now, so this only keeps a caller
        replaying older rows working -- and it still says "table" rather than
        inventing a kind nothing stated.
        """
        insp, _ = make_sync()
        (table,) = insp.parse_tables([{"TABLE_NAME": "T"}], "AR_CRM")
        assert table.table_type is TableType.BASE_TABLE


# --------------------------------------------------------------------- #
# 2. Each column dimension comes from the column that means it
# --------------------------------------------------------------------- #


NUMBER_ROW = {
    "COLUMN_NAME": "AMOUNT", "DATA_TYPE": "NUMBER", "DATA_PRECISION": 12,
    "DATA_SCALE": 2, "DATA_LENGTH": 22, "CHAR_LENGTH": None,
    "NULLABLE": "Y", "COLUMN_ID": 1, "IDENTITY_COLUMN": "NO",
}

VARCHAR_ROW = {
    "COLUMN_NAME": "NAME", "DATA_TYPE": "VARCHAR2", "DATA_PRECISION": None,
    "DATA_SCALE": None, "DATA_LENGTH": 400, "CHAR_LENGTH": 100,
    "NULLABLE": "N", "COLUMN_ID": 2, "IDENTITY_COLUMN": "NO",
}

RAW_ROW = {
    "COLUMN_NAME": "TOKEN", "DATA_TYPE": "RAW", "DATA_PRECISION": None,
    "DATA_SCALE": None, "DATA_LENGTH": 16, "CHAR_LENGTH": 0,
    "NULLABLE": "Y", "COLUMN_ID": 3, "IDENTITY_COLUMN": "NO",
}

TIMESTAMP_ROW = {
    "COLUMN_NAME": "CREATED_AT", "DATA_TYPE": "TIMESTAMP(6)",
    "DATA_PRECISION": None, "DATA_SCALE": 6, "DATA_LENGTH": 11,
    "CHAR_LENGTH": None, "NULLABLE": "Y", "COLUMN_ID": 4,
    "IDENTITY_COLUMN": "NO",
}


class TestColumnDimensionsAreNotInterchangeable:
    def test_a_decimal_column_reports_precision_and_scale_not_a_char_length(self):
        """``NUMBER(12,2)``'s ``DATA_LENGTH`` is 22 *bytes of storage*.

        Reporting it as ``character_maximum_length`` answered a question about
        characters with a number about bytes, and left the two fields that
        actually describe the width -- precision and scale -- unread.
        """
        insp, _ = make_sync()
        (column,) = insp.parse_columns([NUMBER_ROW], "T", "AR_CRM")
        assert column.character_maximum_length is None
        assert (column.numeric_precision, column.numeric_scale) == (12, 2)
        assert column.data_type_full == "NUMBER(12,2)"

    def test_a_character_column_reports_characters_not_bytes(self):
        """``VARCHAR2(100 CHAR)`` is 100 characters and 400 bytes."""
        insp, _ = make_sync()
        (column,) = insp.parse_columns([VARCHAR_ROW], "T", "AR_CRM")
        assert column.character_maximum_length == 100
        assert column.numeric_precision is None
        assert column.numeric_scale is None
        assert column.data_type_full == "VARCHAR2(100)"

    def test_a_character_column_falls_back_to_data_length_only_when_char_length_absent(self):
        """``CHAR_LENGTH`` arrived in 9i; on an older server the byte count is
        the same number in a single-byte character set, so it is the fallback
        for character types -- and for nowhere else."""
        insp, _ = make_sync()
        row = dict(VARCHAR_ROW, CHAR_LENGTH=None)
        (column,) = insp.parse_columns([row], "T", "AR_CRM")
        assert column.character_maximum_length == 400

    def test_a_binary_column_reports_no_character_length(self):
        insp, _ = make_sync()
        (column,) = insp.parse_columns([RAW_ROW], "T", "AR_CRM")
        assert column.character_maximum_length is None
        assert column.data_type_full == "RAW(16)"

    def test_a_timestamp_scale_is_a_fractional_seconds_precision(self):
        """``DATA_SCALE`` on a ``TIMESTAMP(6)`` is not a decimal scale."""
        insp, _ = make_sync()
        (column,) = insp.parse_columns([TIMESTAMP_ROW], "T", "AR_CRM")
        assert column.datetime_precision == 6
        assert column.numeric_scale is None
        assert column.numeric_precision is None

    def test_the_statement_projects_every_dimension_the_parser_reads(self):
        insp, _ = make_sync()
        sql, _ = insp.column_info_statement(table_name="T", schema="AR_CRM").to_sql()
        squashed = squash(sql)
        for projected in ("char_length", "data_length", "data_precision", "data_scale"):
            assert projected in squashed


# --------------------------------------------------------------------- #
# 3. Index facts come from the constraint that owns the index
# --------------------------------------------------------------------- #


PRIMARY_INDEX_ROWS = [
    {
        "INDEX_NAME": "PK_CUSTOMERS", "INDEX_TYPE": "NORMAL",
        "UNIQUENESS": "UNIQUE", "CONSTRAINT_TYPE": "P",
        "COLUMN_NAME": "ID", "COLUMN_POSITION": 1, "DESCEND": "ASC",
    },
]

NON_UNIQUE_INDEX_ROWS = [
    {
        "INDEX_NAME": "IX_NAME", "INDEX_TYPE": "NORMAL",
        "UNIQUENESS": "NONUNIQUE", "CONSTRAINT_TYPE": None,
        "COLUMN_NAME": "NAME", "COLUMN_POSITION": 1, "DESCEND": "ASC",
    },
]

UNIQUE_CONSTRAINT_INDEX_ROWS = [
    {
        "INDEX_NAME": "UK_EMAIL", "INDEX_TYPE": "NORMAL",
        "UNIQUENESS": "UNIQUE", "CONSTRAINT_TYPE": None,
        "COLUMN_NAME": "EMAIL", "COLUMN_POSITION": 1, "DESCEND": "ASC",
    },
]


class TestIndexFactsComeFromTheConstraint:
    def test_a_primary_key_index_is_reported_as_one(self):
        insp, _ = make_sync()
        (index,) = insp.parse_indexes(PRIMARY_INDEX_ROWS, "CUSTOMERS", "AR_CRM")
        assert index.is_primary is True
        assert index.is_unique is True

    def test_an_ordinary_index_is_neither(self):
        insp, _ = make_sync()
        (index,) = insp.parse_indexes(NON_UNIQUE_INDEX_ROWS, "CUSTOMERS", "AR_CRM")
        assert index.is_primary is False
        assert index.is_unique is False

    def test_a_unique_constraints_index_is_unique_but_not_the_primary_key(self):
        insp, _ = make_sync()
        (index,) = insp.parse_indexes(
            UNIQUE_CONSTRAINT_INDEX_ROWS, "CUSTOMERS", "AR_CRM"
        )
        assert index.is_unique is True
        assert index.is_primary is False

    def test_the_marker_on_a_later_row_still_upgrades_the_index(self):
        """Rows arrive one per column; the constraint fact rides on each."""
        insp, _ = make_sync()
        rows = [
            dict(PRIMARY_INDEX_ROWS[0], CONSTRAINT_TYPE=None, COLUMN_POSITION=1),
            dict(PRIMARY_INDEX_ROWS[0], COLUMN_NAME="TENANT", COLUMN_POSITION=2),
        ]
        (index,) = insp.parse_indexes(rows, "CUSTOMERS", "AR_CRM")
        assert index.is_primary is True
        assert index.is_unique is True
        assert [c.name for c in index.columns] == ["ID", "TENANT"]

    def test_the_statement_joins_the_primary_key_constraint(self):
        insp, _ = make_sync()
        sql, params = insp.index_info_statement(
            table_name="customers", schema="ar_crm"
        ).to_sql()
        squashed = squash(sql)
        assert "all_indexes" in squashed
        assert "all_constraints" in squashed
        assert "leftjoin" in squashed
        assert "'p'" in squashed
        assert "constraint_type" in squashed
        # The table and owner are bound, never inlined.
        assert params == ("AR_CRM", "CUSTOMERS")


# --------------------------------------------------------------------- #
# 4. An owned object reports its owner
# --------------------------------------------------------------------- #


PACKAGE_ROW = {
    "OWNER": "AR_OBJ",
    "OBJECT_NAME": "PKG_GATE",
    "OBJECT_TYPE": "PACKAGE",
    "STATUS": "VALID",
    "CREATED": "2026-10-23 10:00:00",
    "LAST_DDL_TIME": "2026-10-23 10:05:00",
}


class TestOwnersAreProjectedAndParsed:
    def test_the_package_statement_asks_for_the_owner(self):
        insp, _ = make_sync()
        sql, params = insp.package_list_statement(owner="ar_obj").to_sql()
        squashed = squash(sql)
        assert "owner" in squashed
        assert "all_objects" in squashed
        assert "ar_obj" not in sql.lower()
        assert params == ("AR_OBJ",)

    def test_the_package_parser_reports_the_owner(self):
        insp, _ = make_sync()
        (package,) = insp.parse_packages([PACKAGE_ROW])
        assert package.owner == "AR_OBJ"

    def test_the_other_owned_families_project_and_parse_their_owner(self):
        """Sequences and synonyms already did; pinned so they cannot regress."""
        insp, _ = make_sync()
        sequence_sql, _ = insp.sequence_list_statement(owner="ar_obj").to_sql()
        synonym_sql, _ = insp.synonym_list_statement(owner="ar_obj").to_sql()
        assert "sequence_owner" in squash(sequence_sql)
        assert "owner" in squash(synonym_sql)

        (sequence,) = insp.parse_sequences(
            [{"SEQUENCE_NAME": "SEQ_GATE", "SEQUENCE_OWNER": "AR_OBJ"}]
        )
        (synonym,) = insp.parse_synonyms(
            [{"SYNONYM_NAME": "SYN_GATE", "OWNER": "AR_OBJ"}]
        )
        assert sequence.owner == "AR_OBJ"
        assert synonym.owner == "AR_OBJ"


# --------------------------------------------------------------------- #
# 5. A view's columns are part of what a view is
# --------------------------------------------------------------------- #


VIEW_ROW = {
    "VIEW_NAME": "V_ACTIVE",
    "VIEW_DEFINITION": "SELECT ID, NAME FROM CUSTOMERS",
    "READ_ONLY": "N",
}

VIEW_COLUMN_ROWS = [
    {
        "COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER", "DATA_PRECISION": 10,
        "DATA_SCALE": 0, "DATA_LENGTH": 22, "CHAR_LENGTH": None,
        "NULLABLE": "Y", "COLUMN_ID": 1, "IDENTITY_COLUMN": "NO",
    },
    {
        "COLUMN_NAME": "NAME", "DATA_TYPE": "VARCHAR2", "DATA_PRECISION": None,
        "DATA_SCALE": None, "DATA_LENGTH": 100, "CHAR_LENGTH": 100,
        "NULLABLE": "Y", "COLUMN_ID": 2, "IDENTITY_COLUMN": "NO",
    },
]


class TestViewColumnsAreReported:
    def test_get_view_info_fills_the_columns_from_the_dictionary(self):
        insp, recorder = make_sync(
            {"ALL_VIEWS": [VIEW_ROW], "ALL_TAB_COLUMNS": VIEW_COLUMN_ROWS}
        )
        view = insp.get_view_info("v_active", schema="AR_CRM")
        assert view is not None
        assert view.name == "V_ACTIVE"
        assert [c.name for c in view.columns] == ["ID", "NAME"]
        # A view's columns are asked of ALL_TAB_COLUMNS, the same view a
        # table's columns come from -- the question is stated as a statement.
        assert any("ALL_TAB_COLUMNS" in sql.upper() for sql, _ in recorder.questions)

    def test_the_completed_view_is_what_is_cached(self):
        insp, recorder = make_sync(
            {"ALL_VIEWS": [VIEW_ROW], "ALL_TAB_COLUMNS": VIEW_COLUMN_ROWS}
        )
        first = insp.get_view_info("v_active", schema="AR_CRM")
        asked = len(recorder.questions)
        second = insp.get_view_info("v_active", schema="AR_CRM")
        assert len(recorder.questions) == asked
        assert second is first
        assert [c.name for c in second.columns] == ["ID", "NAME"]

    def test_an_absent_view_is_none_and_asks_for_no_columns(self):
        insp, recorder = make_sync({"ALL_VIEWS": []})
        assert insp.get_view_info("missing", schema="AR_CRM") is None
        assert not any(
            "ALL_TAB_COLUMNS" in sql.upper() for sql, _ in recorder.questions
        )

    async def test_the_async_introspector_fills_them_too(self):
        insp, recorder = make_async(
            {"ALL_VIEWS": [VIEW_ROW], "ALL_TAB_COLUMNS": VIEW_COLUMN_ROWS}
        )
        view = await insp.get_view_info("v_active", schema="AR_CRM")
        assert view is not None
        assert [c.name for c in view.columns] == ["ID", "NAME"]
        assert any("ALL_TAB_COLUMNS" in sql.upper() for sql, _ in recorder.questions)

    def test_the_two_introspectors_sign_the_same_surface(self):
        """Sync and async expose ``get_view_info`` with one signature."""
        sync_sig = inspect.signature(SyncOracleIntrospector.get_view_info)
        async_sig = inspect.signature(AsyncOracleIntrospector.get_view_info)
        assert sync_sig == async_sig
        assert inspect.iscoroutinefunction(AsyncOracleIntrospector.get_view_info)
        assert not inspect.iscoroutinefunction(SyncOracleIntrospector.get_view_info)


# --------------------------------------------------------------------- #
# 6. The status surface answers from what the backend knows
# --------------------------------------------------------------------- #


class TestDatabaseInfoReadsTheContract:
    def test_an_unadapted_dialect_still_reports_the_cached_version(self):
        """The strict ``dialect.version`` raises before adaptation; a status
        surface must not. It reads ``self.server_version``, which falls through
        to what the backend last learned from its server."""
        backend = StubBackend(version=None)
        backend._version = (21, 3, 0)
        insp = SyncOracleIntrospector(backend, QuestionRecorder())
        info = insp.parse_database_info([])
        assert info.version == "21.3.0"
        assert info.version_tuple == (21, 3, 0)

    def test_an_unknown_version_uses_the_unknown_convention(self):
        backend = StubBackend(version=None)
        insp = SyncOracleIntrospector(backend, QuestionRecorder())
        info = insp.parse_database_info([])
        assert info.version == "unknown"
        assert info.version_tuple == (0, 0, 0)
