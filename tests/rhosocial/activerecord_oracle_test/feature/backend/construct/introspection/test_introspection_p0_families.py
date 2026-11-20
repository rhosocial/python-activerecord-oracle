# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/introspection/test_introspection_p0_families.py
"""The P0 introspection families: partitions, routines, types and constraints.

Offline throughout: the statements are rendered and inspected as SQL, the
``parse_*`` helpers are driven with synthetic data-dictionary rows, and the
cache/sync/async contracts are exercised through a recording executor. No
connection is opened, because every question here is about the mapping
between the dictionary's rows and this backend's value objects.

The identity audit is here too: Oracle's ``ALL_TAB_COLUMNS.IDENTITY_COLUMN``
is the column flag the model layer turns into
``__pk_auto_generated__``, so the parser must report it rather than requiring
a second query to guess.
"""

import inspect
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.introspection.introspector import (
    AsyncOracleIntrospector,
    OracleIntrospector,
)
from rhosocial.activerecord.backend.impl.oracle.introspection.types import (
    OracleConstraintInfo,
    OraclePartitionInfo,
    OraclePartitionStrategy,
    OracleRoutineInfo,
    OracleRoutineParameter,
    OracleTypeInfo,
)
from rhosocial.activerecord.backend.introspection.types import IntrospectionScope


# --------------------------------------------------------------------- #
# Offline collaborators
# --------------------------------------------------------------------- #


class StubBackend:
    """The backend surface the introspector mixin actually reads."""

    def __init__(self, username: str = "ar_crm", version=(23, 0, 0)) -> None:
        self.config = SimpleNamespace(username=username)
        self._version = version
        self.dialect = OracleDialect(version=version)


class RecordingExecutor:
    """A sync executor that records questions and replays fixed rows."""

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self.rows = rows if rows is not None else []
        self.questions: List[Tuple[str, Tuple]] = []

    def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        self.questions.append((sql, params))
        return self.rows


class AsyncRecordingExecutor:
    """The async twin of :class:`RecordingExecutor`."""

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self.rows = rows if rows is not None else []
        self.questions: List[Tuple[str, Tuple]] = []

    async def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        self.questions.append((sql, params))
        return self.rows


def make_sync(
    rows: Optional[List[Dict[str, Any]]] = None,
    username: str = "ar_crm",
    version=(23, 0, 0),
) -> Tuple[OracleIntrospector, RecordingExecutor]:
    executor = RecordingExecutor(rows)
    return OracleIntrospector(StubBackend(username, version), executor), executor


def make_async(
    rows: Optional[List[Dict[str, Any]]] = None,
    username: str = "ar_crm",
) -> Tuple[AsyncOracleIntrospector, AsyncRecordingExecutor]:
    executor = AsyncRecordingExecutor(rows)
    return AsyncOracleIntrospector(StubBackend(username), executor), executor


def squash(sql: str) -> str:
    """Lower-case, whitespace-free, quote-free SQL for shape assertions."""
    return "".join(sql.lower().split()).replace('"', "")


# --------------------------------------------------------------------- #
# Dictionary row fixtures
# --------------------------------------------------------------------- #

RANGE_ROW = {
    "OWNER": "AR_CRM",
    "TABLE_NAME": "ORDERS",
    "PARTITION_NAME": "P2026_01",
    "PARTITION_POSITION": 1,
    "HIGH_VALUE": "TO_DATE(' 2026-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')",
    "HIGH_VALUE_LENGTH": 62,
    "TABLESPACE_NAME": "USERS",
    "SUBPARTITION_COUNT": 0,
    "NUM_ROWS": 1200,
    "SIZE_BYTES": Decimal("65536"),
    "PARTITIONING_TYPE": "RANGE",
    "SUBPARTITIONING_TYPE": None,
    "PARTITION_COUNT": 12,
    "DEF_SUBPARTITION_COUNT": 0,
    "COMPRESSION": "DISABLED",
    "SEGMENT_CREATED": "YES",
    "READ_ONLY": "NO",
    "INTERVAL": None,
}

COMPOSITE_ROW = dict(
    RANGE_ROW,
    PARTITION_NAME="P2026_Q1",
    PARTITION_POSITION=1,
    HIGH_VALUE="MAXVALUE",
    SUBPARTITION_COUNT=4,
    PARTITIONING_TYPE="RANGE",
    SUBPARTITIONING_TYPE="HASH",
)

INTERVAL_ROW = dict(
    RANGE_ROW,
    PARTITION_NAME="SYS_P101",
    PARTITION_POSITION=99,
    HIGH_VALUE="MAXVALUE",
    INTERVAL="NUMTOYMINTERVAL(1,'MONTH')",
)

HASH_ROW = dict(
    RANGE_ROW,
    PARTITION_NAME="P0",
    HIGH_VALUE=None,
    PARTITIONING_TYPE="HASH",
    SUBPARTITIONING_TYPE=None,
)

LIST_ROW = dict(
    RANGE_ROW,
    PARTITION_NAME="P_NORTH",
    HIGH_VALUE="'NORTH', 'EAST'",
    PARTITIONING_TYPE="LIST",
    SUBPARTITIONING_TYPE=None,
)

PROCEDURE_ROWS = [
    {
        "OWNER": "AR_CRM",
        "OBJECT_NAME": "ARCHIVE_ORDERS",
        "OBJECT_TYPE": "PROCEDURE",
        "OBJECT_ID": 101,
        "SUBPROGRAM_ID": 1,
        "OVERLOAD": None,
        "AGGREGATE": "NO",
        "PIPELINED": "NO",
        "PARALLEL": "NO",
        "INTERFACE": "NO",
        "DETERMINISTIC": "NO",
        "AUTHID": "DEFINER",
        "RESULT_CACHE": "NO",
        "IMPL_TYPE_NAME": None,
        "POLYMORPHIC": None,
        "STATUS": "VALID",
        "CREATED": "2026-01-01 00:00:00",
        "LAST_DDL_TIME": "2026-01-02 00:00:00",
        "POSITION": 1,
        "ARGUMENT_NAME": "P_CUTOFF",
        "DATA_TYPE": "DATE",
        "IN_OUT": "IN",
        "DEFAULTED": "N",
        "DEFAULT_VALUE": None,
        "TYPE_OWNER": None,
        "TYPE_NAME": None,
        "DATA_PRECISION": None,
        "DATA_SCALE": None,
        "CHAR_LENGTH": None,
    },
    {
        "OWNER": "AR_CRM",
        "OBJECT_NAME": "ARCHIVE_ORDERS",
        "OBJECT_TYPE": "PROCEDURE",
        "OBJECT_ID": 101,
        "SUBPROGRAM_ID": 1,
        "STATUS": "VALID",
        "AUTHID": "DEFINER",
        "CREATED": "2026-01-01 00:00:00",
        "LAST_DDL_TIME": "2026-01-02 00:00:00",
        "POSITION": 2,
        "ARGUMENT_NAME": "P_BATCH",
        "DATA_TYPE": "NUMBER",
        "IN_OUT": "IN",
        "DEFAULTED": "Y",
        "DEFAULT_VALUE": None,
        "TYPE_OWNER": None,
        "TYPE_NAME": None,
        "DATA_PRECISION": 6,
        "DATA_SCALE": 0,
        "CHAR_LENGTH": None,
    },
]

FUNCTION_ROWS = [
    {
        "OWNER": "AR_CRM",
        "OBJECT_NAME": "CALCULATE_TAX",
        "OBJECT_TYPE": "FUNCTION",
        "OBJECT_ID": 202,
        "SUBPROGRAM_ID": 1,
        "OVERLOAD": None,
        "AGGREGATE": "NO",
        "PIPELINED": "YES",
        "PARALLEL": "YES",
        "INTERFACE": "NO",
        "DETERMINISTIC": "YES",
        "AUTHID": "CURRENT_USER",
        "RESULT_CACHE": "YES",
        "IMPL_TYPE_NAME": None,
        "POLYMORPHIC": None,
        "STATUS": "VALID",
        "CREATED": "2026-01-01 00:00:00",
        "LAST_DDL_TIME": "2026-01-02 00:00:00",
        "POSITION": 0,
        "ARGUMENT_NAME": None,
        "DATA_TYPE": "NUMBER",
        "IN_OUT": "OUT",
        "DEFAULTED": "N",
        "DEFAULT_VALUE": None,
        "TYPE_OWNER": None,
        "TYPE_NAME": None,
        "DATA_PRECISION": 12,
        "DATA_SCALE": 2,
        "CHAR_LENGTH": None,
    },
    {
        "OWNER": "AR_CRM",
        "OBJECT_NAME": "CALCULATE_TAX",
        "OBJECT_TYPE": "FUNCTION",
        "OBJECT_ID": 202,
        "SUBPROGRAM_ID": 1,
        "STATUS": "VALID",
        "AUTHID": "CURRENT_USER",
        "CREATED": "2026-01-01 00:00:00",
        "LAST_DDL_TIME": "2026-01-02 00:00:00",
        "POSITION": 1,
        "ARGUMENT_NAME": "P_AMOUNT",
        "DATA_TYPE": "NUMBER",
        "IN_OUT": "IN",
        "DEFAULTED": "N",
        "DEFAULT_VALUE": None,
        "TYPE_OWNER": None,
        "TYPE_NAME": None,
        "DATA_PRECISION": 12,
        "DATA_SCALE": 2,
        "CHAR_LENGTH": None,
    },
]

NO_ARGUMENT_ROWS = [
    {
        "OWNER": "AR_CRM",
        "OBJECT_NAME": "FLUSH_CACHE",
        "OBJECT_TYPE": "PROCEDURE",
        "OBJECT_ID": 303,
        "SUBPROGRAM_ID": 1,
        "STATUS": "VALID",
        "AUTHID": "DEFINER",
        "DETERMINISTIC": "NO",
        "PIPELINED": "NO",
        "AGGREGATE": "NO",
        "PARALLEL": "NO",
        "RESULT_CACHE": "NO",
        "POSITION": None,
        "ARGUMENT_NAME": None,
        "DATA_TYPE": None,
        "IN_OUT": None,
        "DEFAULTED": None,
        "DEFAULT_VALUE": None,
        "TYPE_OWNER": None,
        "TYPE_NAME": None,
        "DATA_PRECISION": None,
        "DATA_SCALE": None,
        "CHAR_LENGTH": None,
        "CREATED": "2026-01-01 00:00:00",
        "LAST_DDL_TIME": "2026-01-02 00:00:00",
    },
]

OBJECT_TYPE_ROW = {
    "OWNER": "AR_CRM",
    "TYPE_NAME": "ADDRESS_T",
    "TYPECODE": "OBJECT",
    "ATTRIBUTES": 2,
    "METHODS": 0,
    "PREDEFINED": "NO",
    "INCOMPLETE": "NO",
    "FINAL": "YES",
    "INSTANTIABLE": "YES",
    "SUPERTYPE_OWNER": None,
    "SUPERTYPE_NAME": None,
    "COLL_TYPE": None,
    "ELEM_TYPE_NAME": None,
    "STATUS": "VALID",
    "CREATED": "2026-01-01 00:00:00",
    "LAST_DDL_TIME": "2026-01-02 00:00:00",
    "ATTR_NAME": "STREET",
    "ATTR_TYPE_MOD": None,
    "ATTR_TYPE_OWNER": None,
    "ATTR_TYPE_NAME": "VARCHAR2",
    "ATTR_LENGTH": 100,
    "ATTR_PRECISION": None,
    "ATTR_SCALE": None,
    "ATTR_CHARSET": "CHAR_CS",
    "ATTR_NO": 1,
    "ATTR_INHERITED": "NO",
    "ATTR_CHAR_USED": "C",
}

OBJECT_TYPE_ROWS = [
    OBJECT_TYPE_ROW,
    dict(
        OBJECT_TYPE_ROW,
        ATTR_NAME="ZIP",
        ATTR_TYPE_NAME="NUMBER",
        ATTR_LENGTH=None,
        ATTR_PRECISION=10,
        ATTR_SCALE=0,
        ATTR_CHARSET=None,
        ATTR_NO=2,
        ATTR_CHAR_USED=None,
    ),
]

COLLECTION_TYPE_ROW = {
    "OWNER": "AR_CRM",
    "TYPE_NAME": "PHONE_LIST_T",
    "TYPECODE": "COLLECTION",
    "ATTRIBUTES": None,
    "METHODS": None,
    "PREDEFINED": "NO",
    "INCOMPLETE": None,
    "FINAL": None,
    "INSTANTIABLE": None,
    "SUPERTYPE_OWNER": None,
    "SUPERTYPE_NAME": None,
    "COLL_TYPE": "TABLE",
    "ELEM_TYPE_MOD": None,
    "ELEM_TYPE_OWNER": None,
    "ELEM_TYPE_NAME": "VARCHAR2",
    "ELEM_LENGTH": 20,
    "ELEM_PRECISION": None,
    "ELEM_SCALE": None,
    "ELEM_CHARSET": "CHAR_CS",
    "UPPER_BOUND": None,
    "STATUS": "VALID",
    "CREATED": "2026-01-01 00:00:00",
    "LAST_DDL_TIME": "2026-01-02 00:00:00",
    "ATTR_NAME": None,
}

COLLECTION_TYPE_ROWS = [
    COLLECTION_TYPE_ROW,
    dict(
        COLLECTION_TYPE_ROW,
        TYPE_NAME="VECTOR_T",
        COLL_TYPE="VARYING ARRAY",
        ELEM_TYPE_NAME="NUMBER",
        ELEM_LENGTH=None,
        ELEM_PRECISION=8,
        ELEM_SCALE=2,
        UPPER_BOUND=10,
    ),
]

CONSTRAINT_ROW = {
    "OWNER": "AR_CRM",
    "TABLE_NAME": "ORDERS",
    "CONSTRAINT_NAME": "PK_ORDERS",
    "CONSTRAINT_TYPE": "P",
    "STATUS": "ENABLED",
    "VALIDATED": "VALIDATED",
    "GENERATED": "USER NAME",
    "DEFERRABLE": "NOT DEFERRABLE",
    "DEFERRED": "IMMEDIATE",
    "RELY": None,
    "BAD": None,
    "SEARCH_CONDITION": None,
    "INDEX_NAME": "PK_ORDERS",
    "REFERENCED_OWNER": None,
    "REFERENCED_CONSTRAINT_NAME": None,
    "DELETE_RULE": None,
    "LAST_CHANGE": "2026-01-03 00:00:00",
    "INVALID": None,
    "VIEW_RELATED": None,
    "COLUMN_NAME": "ID",
    "COLUMN_POSITION": 1,
}

CONSTRAINT_ROWS = [
    CONSTRAINT_ROW,
    dict(
        CONSTRAINT_ROW,
        COLUMN_NAME="REGION",
        COLUMN_POSITION=2,
    ),
    {
        "OWNER": "AR_CRM",
        "TABLE_NAME": "ORDERS",
        "CONSTRAINT_NAME": "CK_AMOUNT",
        "CONSTRAINT_TYPE": "C",
        "STATUS": "ENABLED",
        "VALIDATED": "NOT VALIDATED",
        "GENERATED": "GENERATED NAME",
        "DEFERRABLE": "NOT DEFERRABLE",
        "DEFERRED": "IMMEDIATE",
        "RELY": "RELY",
        "BAD": None,
        "SEARCH_CONDITION": '"AMOUNT" > 0',
        "INDEX_NAME": None,
        "REFERENCED_OWNER": None,
        "REFERENCED_CONSTRAINT_NAME": None,
        "DELETE_RULE": None,
        "COLUMN_NAME": None,
    },
    {
        "OWNER": "AR_CRM",
        "TABLE_NAME": "ORDERS",
        "CONSTRAINT_NAME": "FK_ORDERS_CUSTOMER",
        "CONSTRAINT_TYPE": "R",
        "STATUS": "ENABLED",
        "VALIDATED": "VALIDATED",
        "GENERATED": "USER NAME",
        "DEFERRABLE": "NOT DEFERRABLE",
        "DEFERRED": "IMMEDIATE",
        "RELY": None,
        "BAD": None,
        "SEARCH_CONDITION": None,
        "INDEX_NAME": None,
        "REFERENCED_OWNER": "AR_CRM",
        "REFERENCED_CONSTRAINT_NAME": "PK_CUSTOMERS",
        "DELETE_RULE": "CASCADE",
        "COLUMN_NAME": "CUSTOMER_ID",
        "COLUMN_POSITION": 1,
    },
]


# --------------------------------------------------------------------- #
# SQL builders
# --------------------------------------------------------------------- #


class TestPartitionStatement:
    """The partition read joins the partition to the strategy that made it."""

    def test_it_reads_the_partition_view_and_the_strategy_view(self):
        insp, _ = make_sync()
        sql, params = insp.partition_list_statement(
            table_name="orders", schema="ar_crm"
        ).to_sql()
        squashed = squash(sql)
        assert "all_tab_partitions" in squashed
        assert "all_part_tables" in squashed
        # The strategy and the interval expression live on the table row.
        assert "partitioning_type" in squashed
        assert "subpartitioning_type" in squashed
        assert "interval" in squashed
        assert "orders" not in sql.lower()
        assert "ar_crm" not in sql.lower()
        assert params == ("AR_CRM", "ORDERS")

    def test_the_size_is_blocks_times_the_tablespace_block_size(self):
        insp, _ = make_sync()
        sql, _ = insp.partition_list_statement(
            table_name="orders", schema="ar_crm"
        ).to_sql()
        squashed = squash(sql)
        assert "user_tablespaces" in squashed
        assert "p.blocks*ts.block_size" in squashed
        # No assumed 8 KiB multiplication survives in the statement.
        assert "8192" not in squashed

    @pytest.mark.parametrize("version,projects_interval", [
        ((11, 0, 0), False),
        ((11, 1, 0), True),
        ((23, 0, 0), True),
    ])
    def test_the_interval_projection_is_gated_on_11_1(self, version, projects_interval):
        insp, _ = make_sync(version=version)
        sql, _ = insp.partition_list_statement(
            table_name="orders", schema="ar_crm"
        ).to_sql()
        assert ('"pt"."interval"' in sql.lower()) is projects_interval

    def test_a_name_carrying_sql_is_bound_as_data(self):
        insp, _ = make_sync()
        sql, params = insp.partition_list_statement(
            table_name="orders' OR '1'='1", schema="ar_crm"
        ).to_sql()
        assert "1'='1" not in sql
        assert params == ("AR_CRM", "ORDERS' OR '1'='1")


class TestRoutineStatements:
    """Procedures and functions ask the same three-relation question."""

    @pytest.mark.parametrize("kind", ["PROCEDURE", "FUNCTION"])
    def test_it_reads_properties_object_state_and_arguments(self, kind):
        insp, _ = make_sync()
        sql, params = insp.routine_list_statement(
            owner="ar_crm", routine_kind=kind
        ).to_sql()
        squashed = squash(sql)
        assert "all_procedures" in squashed
        assert "all_objects" in squashed
        assert "all_arguments" in squashed
        assert f"object_type='{kind.lower()}'" in squashed
        # A packaged subprogram is its package's answer, not this family's.
        assert "procedure_nameisnull" in squashed
        assert "package_nameisnull" in squashed
        assert "ar_crm" not in sql.lower()
        assert params == ("AR_CRM",)

    def test_the_flags_the_family_reports_are_projections(self):
        insp, _ = make_sync()
        sql, _ = insp.routine_list_statement(
            owner="ar_crm", routine_kind="FUNCTION"
        ).to_sql()
        squashed = squash(sql)
        for column in (
            "authid", "deterministic", "pipelined", "aggregate", "parallel",
            "result_cache", "interface", "impltypename", "polymorphic",
            "status", "created", "last_ddl_time", "position",
            "argument_name", "data_type", "in_out", "defaulted",
        ):
            assert column in squashed

    def test_the_position_zero_row_is_the_return_value(self):
        insp, _ = make_sync()
        sql, _ = insp.routine_list_statement(
            owner="ar_crm", routine_kind="FUNCTION"
        ).to_sql()
        assert '"a"."position"' in sql.lower()


class TestTypeStatement:
    """Object types and collections are one compound answer."""

    def test_it_unions_the_two_views_and_reads_the_attributes(self):
        insp, _ = make_sync()
        sql, params = insp.type_list_statement(owner="ar_crm").to_sql()
        squashed = squash(sql)
        assert "all_types" in squashed
        assert "all_coll_types" in squashed
        assert "all_type_attrs" in squashed
        assert "all_objects" in squashed
        assert "unionall" in squashed
        assert "object_type='type'" in squashed
        assert "ar_crm" not in sql.lower()
        # Owner narrows the object side, the subtraction and the collection side.
        assert params == ("AR_CRM", "AR_CRM", "AR_CRM")

    def test_the_object_side_subtracts_the_collection_names(self):
        insp, _ = make_sync()
        sql, _ = insp.type_list_statement(owner="ar_crm").to_sql()
        squashed = squash(sql)
        assert "not(t.type_namein(select" in squashed

    def test_the_outer_projection_addresses_the_union_by_name(self):
        insp, _ = make_sync()
        sql, _ = insp.type_list_statement(owner="ar_crm").to_sql()
        assert '"type_list"' in sql.lower()
        assert '"type_name"' in sql.lower()
        assert '"attr_no"' in sql.lower()


class TestConstraintStatement:
    """Constraints are the declaration, beside the index and foreign-key views."""

    def test_it_joins_the_declaration_to_its_columns(self):
        insp, _ = make_sync()
        sql, params = insp.constraint_list_statement(
            table_name="orders", schema="ar_crm"
        ).to_sql()
        squashed = squash(sql)
        assert "all_constraints" in squashed
        assert "all_cons_columns" in squashed
        assert "leftjoinall_cons_columns" in squashed
        for column in (
            "constraint_type", "status", "validated", "generated",
            "deferrable", "deferred", "rely", "bad", "search_condition",
            "index_name", "r_owner", "r_constraint_name", "delete_rule",
            "last_change", "invalid", "view_related", "column_name",
            "position",
        ):
            assert column in squashed
        assert "orders" not in sql.lower()
        assert params == ("AR_CRM", "ORDERS")


# --------------------------------------------------------------------- #
# Parsers
# --------------------------------------------------------------------- #


class TestPartitionParser:
    def test_range_keeps_the_bound_tablespace_and_subpartition_count(self):
        insp, _ = make_sync()
        (partition,) = insp.parse_partitions([RANGE_ROW], "ORDERS", "AR_CRM")
        assert isinstance(partition, OraclePartitionInfo)
        assert partition.strategy is OraclePartitionStrategy.RANGE
        assert partition.partition_name == "P2026_01"
        assert partition.partition_position == 1
        assert partition.tablespace_name == "USERS"
        assert partition.subpartition_count == 0
        assert partition.row_count == 1200
        assert partition.size_bytes == 65536
        assert partition.is_max_value is False
        assert "2026-02-01" in partition.high_value

    def test_a_maxvalue_bound_is_named_as_such(self):
        insp, _ = make_sync()
        (partition,) = insp.parse_partitions([INTERVAL_ROW], "ORDERS", "AR_CRM")
        assert partition.is_max_value is True
        assert partition.strategy is OraclePartitionStrategy.INTERVAL
        assert partition.interval == "NUMTOYMINTERVAL(1,'MONTH')"

    def test_composite_is_a_subpartitioning_type_not_a_word(self):
        insp, _ = make_sync()
        (partition,) = insp.parse_partitions([COMPOSITE_ROW], "ORDERS", "AR_CRM")
        assert partition.strategy is OraclePartitionStrategy.COMPOSITE
        assert partition.subpartition_strategy == "HASH"
        assert partition.subpartition_count == 4
        assert partition.is_max_value is True

    def test_the_word_none_is_not_a_subpartitioning_type(self):
        """Oracle spells "not subpartitioned" as ``NONE``, not as NULL."""
        insp, _ = make_sync()
        row = dict(RANGE_ROW, SUBPARTITIONING_TYPE="NONE")
        (partition,) = insp.parse_partitions([row], "ORDERS", "AR_CRM")
        assert partition.strategy is OraclePartitionStrategy.RANGE
        assert partition.subpartition_strategy is None

    @pytest.mark.parametrize("row,expected", [
        (HASH_ROW, OraclePartitionStrategy.HASH),
        (LIST_ROW, OraclePartitionStrategy.LIST),
    ])
    def test_the_two_remaining_top_level_strategies(self, row, expected):
        insp, _ = make_sync()
        (partition,) = insp.parse_partitions([row], "ORDERS", "AR_CRM")
        assert partition.strategy is expected

    def test_an_unrecognised_strategy_is_unknown_not_guessed(self):
        insp, _ = make_sync()
        row = dict(RANGE_ROW, PARTITIONING_TYPE="SYSTEM")
        (partition,) = insp.parse_partitions([row], "ORDERS", "AR_CRM")
        assert partition.strategy is OraclePartitionStrategy.UNKNOWN

    def test_a_row_without_a_partition_name_is_not_a_partition(self):
        insp, _ = make_sync()
        assert insp.parse_partitions([{"PARTITION_NAME": None}], "ORDERS") == []

    def test_the_owner_and_table_fall_back_to_the_question(self):
        insp, _ = make_sync()
        row = dict(RANGE_ROW, OWNER=None, TABLE_NAME=None)
        (partition,) = insp.parse_partitions([row], "ORDERS", "AR_CRM")
        assert (partition.owner, partition.table_name) == ("AR_CRM", "ORDERS")


class TestRoutineParser:
    def test_a_procedure_carries_its_parameters(self):
        insp, _ = make_sync()
        (procedure,) = insp.parse_routines(PROCEDURE_ROWS)
        assert isinstance(procedure, OracleRoutineInfo)
        assert procedure.kind == "PROCEDURE"
        assert procedure.authid == "DEFINER"
        assert procedure.return_type is None
        assert [p.name for p in procedure.parameters] == ["P_CUTOFF", "P_BATCH"]
        assert procedure.parameters[0].data_type == "DATE"
        assert procedure.parameters[1].data_type == "NUMBER(6)"
        # DEFAULTED is Y/N in ALL_ARGUMENTS, not YES/NO.
        assert procedure.parameters[1].defaulted is True

    def test_a_function_reports_its_return_type_and_flags(self):
        insp, _ = make_sync()
        (function,) = insp.parse_routines(FUNCTION_ROWS)
        assert function.kind == "FUNCTION"
        assert function.return_type == "NUMBER(12,2)"
        assert function.pipelined is True
        assert function.deterministic is True
        assert function.parallel is True
        assert function.result_cache is True
        assert function.authid == "CURRENT_USER"
        assert [p.name for p in function.parameters] == ["P_AMOUNT"]
        assert function.parameters[0].position == 1

    def test_a_routine_with_no_arguments_still_exists(self):
        """ALL_ARGUMENTS omits an argument-less routine from 12.1.0.2 on."""
        insp, _ = make_sync()
        (procedure,) = insp.parse_routines(NO_ARGUMENT_ROWS)
        assert procedure.name == "FLUSH_CACHE"
        assert procedure.parameters == []
        assert procedure.return_type is None

    def test_comment_is_none_because_oracle_keeps_no_routine_comment(self):
        insp, _ = make_sync()
        (procedure,) = insp.parse_routines(PROCEDURE_ROWS)
        assert procedure.comment is None
        assert "COMMENT ON" in (OracleRoutineInfo.__doc__ or "")

    def test_two_routines_are_not_one_by_a_shared_name(self):
        insp, _ = make_sync()
        rows = PROCEDURE_ROWS + [
            dict(
                row,
                OBJECT_ID=404,
                SUBPROGRAM_ID=2,
                OBJECT_NAME="ARCHIVE_ORDERS",
            )
            for row in PROCEDURE_ROWS
        ]
        routines = insp.parse_routines(rows)
        assert len(routines) == 2
        assert {r.object_id for r in routines} == {101, 404}

    def test_parameters_carry_user_defined_type_metadata(self):
        insp, _ = make_sync()
        row = dict(
            PROCEDURE_ROWS[0],
            DATA_TYPE="OBJECT",
            TYPE_OWNER="AR_CRM",
            TYPE_NAME="ADDRESS_T",
        )
        (procedure,) = insp.parse_routines([row])
        parameter = procedure.parameters[0]
        assert (parameter.type_owner, parameter.type_name) == ("AR_CRM", "ADDRESS_T")
        assert isinstance(parameter, OracleRoutineParameter)

    def test_a_nested_composite_component_is_not_a_second_parameter(self):
        """Before 18c a composite argument arrived as one row per component."""
        insp, _ = make_sync()
        nested = dict(PROCEDURE_ROWS[0], DATA_LEVEL=1, ARGUMENT_NAME="STREET")
        (procedure,) = insp.parse_routines(PROCEDURE_ROWS + [nested])
        assert [p.name for p in procedure.parameters] == ["P_CUTOFF", "P_BATCH"]


class TestTypeParser:
    def test_an_object_type_carries_its_attributes_in_order(self):
        insp, _ = make_sync()
        (type_info,) = insp.parse_types(OBJECT_TYPE_ROWS)
        assert isinstance(type_info, OracleTypeInfo)
        assert type_info.kind == "OBJECT"
        assert (type_info.type_code, type_info.attributes_count) == ("OBJECT", 2)
        assert type_info.final is True and type_info.instantiable is True
        assert type_info.status == "VALID"
        assert [a.name for a in type_info.attributes] == ["STREET", "ZIP"]
        assert type_info.attributes[0].data_type == "VARCHAR2(100)"
        assert type_info.attributes[0].inherited is False
        assert type_info.attributes[1].data_type == "NUMBER(10)"

    def test_a_nested_table_is_a_collection_and_not_a_second_object(self):
        insp, _ = make_sync()
        types = insp.parse_types(COLLECTION_TYPE_ROWS)
        nested = next(t for t in types if t.name == "PHONE_LIST_T")
        varray = next(t for t in types if t.name == "VECTOR_T")
        assert nested.kind == "NESTED TABLE"
        assert nested.collection_type == "TABLE"
        assert nested.element_type == "VARCHAR2"
        assert nested.attributes == []
        assert varray.kind == "VARRAY"
        assert varray.upper_bound == 10
        assert varray.element_type == "NUMBER"
        assert varray.element_type_precision == 8

    def test_final_and_instantiable_stay_none_for_a_collection(self):
        """The question does not apply, and ``False`` would be an answer."""
        insp, _ = make_sync()
        (type_info,) = insp.parse_types([COLLECTION_TYPE_ROWS[0]])
        assert type_info.final is None
        assert type_info.instantiable is None

    def test_an_object_type_and_a_collection_of_one_name_stay_distinct(self):
        insp, _ = make_sync()
        types = insp.parse_types(OBJECT_TYPE_ROWS + COLLECTION_TYPE_ROWS)
        assert {t.name for t in types} == {
            "ADDRESS_T", "PHONE_LIST_T", "VECTOR_T"
        }


class TestConstraintParser:
    def test_a_composite_key_keeps_every_column_in_position_order(self):
        insp, _ = make_sync()
        constraints = insp.parse_constraints(CONSTRAINT_ROWS, "ORDERS", "AR_CRM")
        by_name = {c.name: c for c in constraints}
        pk = by_name["PK_ORDERS"]
        assert isinstance(pk, OracleConstraintInfo)
        assert pk.constraint_type == "P"
        assert pk.columns == ["ID", "REGION"]
        assert pk.index_name == "PK_ORDERS"
        assert pk.search_condition is None
        assert pk.rely is False

    def test_a_check_constraint_keeps_its_expression_and_no_columns(self):
        insp, _ = make_sync()
        constraints = insp.parse_constraints(CONSTRAINT_ROWS, "ORDERS", "AR_CRM")
        check = next(c for c in constraints if c.name == "CK_AMOUNT")
        assert check.constraint_type == "C"
        assert check.columns == []
        assert check.search_condition == '"AMOUNT" > 0'
        # RELY is spelled RELY, not YES.
        assert check.rely is True
        assert check.validated == "NOT VALIDATED"

    def test_a_foreign_key_reports_the_referenced_constraint(self):
        insp, _ = make_sync()
        constraints = insp.parse_constraints(CONSTRAINT_ROWS, "ORDERS", "AR_CRM")
        fk = next(c for c in constraints if c.name == "FK_ORDERS_CUSTOMER")
        assert fk.constraint_type == "R"
        assert fk.columns == ["CUSTOMER_ID"]
        assert (fk.referenced_owner, fk.referenced_constraint_name) == (
            "AR_CRM", "PK_CUSTOMERS"
        )
        assert fk.delete_rule == "CASCADE"


# --------------------------------------------------------------------- #
# The introspector methods, cache and sync/async parity
# --------------------------------------------------------------------- #


class TestPartitionAndConstraintReads:
    def test_list_partitions_asks_once_then_reads_the_cache(self):
        insp, executor = make_sync([RANGE_ROW])
        cold = insp.list_partitions("ORDERS", "AR_CRM")
        assert len(executor.questions) == 1
        warm = insp.list_partitions("orders", "ar_crm")
        assert len(executor.questions) == 1
        # The question upper-cases the owner and the table name, so case folds
        # into one answer only for the owner (the cache key keeps the table as
        # the caller spelled it).
        assert warm is cold
        assert executor.questions[0][1] == ("AR_CRM", "ORDERS")

    def test_list_constraints_has_its_own_cache_entry(self):
        insp, executor = make_sync(CONSTRAINT_ROWS)
        insp.list_partitions("ORDERS", "AR_CRM")
        insp.list_constraints("ORDERS", "AR_CRM")
        insp.list_partitions("ORDERS", "AR_CRM")
        insp.list_constraints("ORDERS", "AR_CRM")
        assert len(executor.questions) == 2
        keys = {
            insp.cache.key(
                IntrospectionScope.TABLE, "ORDERS", schema="AR_CRM",
                extra="partitions",
            ),
            insp.cache.key(
                IntrospectionScope.TABLE, "ORDERS", schema="AR_CRM",
                extra="oracle:constraints",
            ),
        }
        assert all(insp.cache.get(key) is not None for key in keys)

    def test_the_default_schema_is_the_connected_user(self):
        insp, _ = make_sync([RANGE_ROW], username="ar_crm")
        sql, params = insp.partition_list_statement(
            table_name="ORDERS", schema=insp.default_schema
        ).to_sql()
        assert params[0] == "AR_CRM"


class TestRoutineReads:
    def test_list_procedures_and_functions_use_distinct_cache_entries(self):
        insp, executor = make_sync(PROCEDURE_ROWS)
        insp.list_procedures("AR_CRM")
        insp.list_functions("AR_CRM")
        insp.list_procedures("AR_CRM")
        assert len(executor.questions) == 2
        assert executor.questions[0][1] == ("AR_CRM",)
        assert executor.questions[1][1] == ("AR_CRM",)
        # The two questions differ in the inline OBJECT_TYPE literal, not in
        # the bind values.
        assert "object_type='procedure'" in squash(executor.questions[0][0])
        assert "object_type='function'" in squash(executor.questions[1][0])

    def test_get_returns_the_routine_or_none(self):
        insp, executor = make_sync(PROCEDURE_ROWS)
        found = insp.get_procedure_info("archive_orders", "ar_crm")
        assert found is not None and found.name == "ARCHIVE_ORDERS"
        assert insp.get_procedure_info("no_such_routine", "ar_crm") is None
        # The listing was read once and reused by the miss.
        assert len(executor.questions) == 1

    def test_owner_case_folds_into_one_question(self):
        insp, executor = make_sync(PROCEDURE_ROWS)
        insp.list_procedures("ar_crm")
        insp.list_procedures("AR_CRM")
        assert len(executor.questions) == 1

    async def test_async_listing_and_lookup(self):
        insp, executor = make_async(FUNCTION_ROWS)
        functions = await insp.list_functions("ar_crm")
        assert [f.name for f in functions] == ["CALCULATE_TAX"]
        found = await insp.get_function_info("calculate_tax", "AR_CRM")
        assert found is not None and found.return_type == "NUMBER(12,2)"
        assert len(executor.questions) == 1

    async def test_async_get_returns_none_when_absent(self):
        insp, executor = make_async([])
        assert await insp.get_procedure_info("GHOST", "AR_CRM") is None
        assert len(executor.questions) == 1


class TestTypeReads:
    def test_list_types_caches_and_get_finds_by_name(self):
        insp, executor = make_sync(OBJECT_TYPE_ROWS)
        types = insp.list_types("ar_crm")
        assert [t.name for t in types] == ["ADDRESS_T"]
        again = insp.list_types("AR_CRM")
        assert again is types
        assert len(executor.questions) == 1
        found = insp.get_type_info("address_t", "AR_CRM")
        assert found is not None and found.kind == "OBJECT"

    def test_get_type_info_answers_none_for_an_absent_type(self):
        insp, _ = make_sync(OBJECT_TYPE_ROWS)
        assert insp.get_type_info("NO_SUCH_TYPE", "AR_CRM") is None

    async def test_async_list_and_get(self):
        insp, executor = make_async(COLLECTION_TYPE_ROWS)
        types = await insp.list_types("ar_crm")
        assert {t.name for t in types} == {"PHONE_LIST_T", "VECTOR_T"}
        found = await insp.get_type_info("phone_list_t", "AR_CRM")
        assert found is not None and found.kind == "NESTED TABLE"
        assert len(executor.questions) == 1


# --------------------------------------------------------------------- #
# Identity audit
# --------------------------------------------------------------------- #


IDENTITY_YES = {
    "COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER", "DATA_PRECISION": 10,
    "DATA_SCALE": 0, "DATA_LENGTH": 22, "NULLABLE": "N", "COLUMN_ID": 1,
    "IDENTITY_COLUMN": "YES",
}

IDENTITY_NO = dict(
    IDENTITY_YES, COLUMN_NAME="NAME", DATA_TYPE="VARCHAR2", DATA_LENGTH=100,
    CHAR_LENGTH=100, COLUMN_ID=2, IDENTITY_COLUMN="NO",
)


class TestIdentityAudit:
    def test_an_identity_column_is_reported_as_auto_incremented(self):
        insp, _ = make_sync()
        columns = insp.parse_columns([IDENTITY_YES, IDENTITY_NO], "T", "AR_CRM")
        assert columns[0].is_auto_increment is True
        # GENERATED BY DEFAULT and GENERATED ALWAYS both answer YES here; the
        # generation style is not what this slot asks.
        assert columns[1].is_auto_increment is False

    def test_the_column_statement_projects_the_identity_flag(self):
        insp, _ = make_sync()
        sql, _ = insp.column_info_statement(table_name="T", schema="AR_CRM").to_sql()
        assert '"IDENTITY_COLUMN"' in sql

    def test_a_lower_case_yes_is_still_a_yes(self):
        """The flag is read case-insensitively rather than by word equality."""
        insp, _ = make_sync()
        (column,) = insp.parse_columns(
            [dict(IDENTITY_YES, IDENTITY_COLUMN="yes")], "T", "AR_CRM"
        )
        assert column.is_auto_increment is True


# --------------------------------------------------------------------- #
# Sync / async parity and capability switches
# --------------------------------------------------------------------- #

PARITY_METHODS = [
    "list_partitions",
    "list_constraints",
    "list_procedures",
    "get_procedure_info",
    "list_functions",
    "get_function_info",
    "list_types",
    "get_type_info",
]


class TestParity:
    @pytest.mark.parametrize("name", PARITY_METHODS)
    def test_sync_and_async_signatures_are_identical(self, name):
        assert inspect.signature(getattr(OracleIntrospector, name)) == inspect.signature(
            getattr(AsyncOracleIntrospector, name)
        )

    @pytest.mark.parametrize("name", PARITY_METHODS)
    def test_the_async_half_is_actually_async(self, name):
        assert inspect.iscoroutinefunction(getattr(AsyncOracleIntrospector, name))
        assert not inspect.iscoroutinefunction(getattr(OracleIntrospector, name))


class TestCapabilitySwitches:
    @pytest.mark.parametrize("switch", [
        "supports_partition_metadata_introspection",
        "supports_procedure_introspection",
        "supports_function_introspection",
        "supports_type_introspection",
        "supports_constraint_introspection",
    ])
    def test_the_switch_is_declared_and_answers_true(self, switch):
        dialect = OracleDialect(version=(23, 0, 0))
        predicate = getattr(dialect, switch, None)
        assert callable(predicate), f"{switch} is not a dialect method"
        assert predicate() is True
