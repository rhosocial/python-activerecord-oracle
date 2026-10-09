# tests/rhosocial/activerecord_oracle_test/feature/backend/test_introspector_deep.py
"""Deep coverage for introspection/introspector.py.

Offline half: the SQL builders and pure-Python ``_parse_*`` helpers are
driven with synthetic data-dictionary rows through stub collaborators.
Live half (pinned to the ``oracle_schema_dml`` xdist group): AR_CRM/AR_SHOP
are provisioned like test_schema_qualified_dml and probed through the
backend connection for table existence, column inventory, and cross-schema
reads; the status introspector surface is exercised on both sync/async
backends.
"""
from types import SimpleNamespace
from typing import Any, ClassVar, Dict, List, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.introspection.introspector import (
    AsyncOracleIntrospector, SyncOracleIntrospector,
)
from rhosocial.activerecord.backend.impl.oracle.introspection.status_introspector import (
    AsyncOracleStatusIntrospector, SyncOracleStatusIntrospector,
)
from rhosocial.activerecord.backend.impl.oracle.version import (
    normalize_version, parse_version_row, product_name_from_row,
    select_version_row,
)
from rhosocial.activerecord.backend.impl.oracle.expression.types import OracleRawType
from rhosocial.activerecord.backend.expression.types import (
    BlobType, BooleanType, IntegerType, VarCharType,
)
from rhosocial.activerecord.backend.expression.introspection import (
    ForeignKeyExpression, IndexInfoExpression, TableListExpression,
    TriggerListExpression, ViewListExpression,
)
from rhosocial.activerecord.backend.introspection.types import ColumnNullable
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.base.field_proxy import FieldProxy
from providers.scenarios import get_scenario_raw

pytestmark = pytest.mark.xdist_group("oracle_schema_dml")

SCHEMA_USERS = ("AR_CRM", "AR_SHOP")
SCHEMA_USER_PASSWORD = "Rh0social#2026"


class DeepCustomer(ActiveRecord):
    __table_name__ = "customers"
    __schema_name__ = "ar_crm"
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    name: str


class StubBackend:
    def __init__(self, username: str = "ar_crm",
                 version: Tuple[int, ...] = (23, 0, 0)) -> None:
        self.config = SimpleNamespace(username=username)
        self._version = version
        self.dialect = OracleDialect(version=version)


class StubExecutor:
    def __init__(self) -> None:
        self.executed: List[str] = []

    def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        self.executed.append(sql)
        return []


def make_sync_introspector(username: str = "ar_crm",
                           version: Tuple[int, ...] = (23, 0, 0)):
    backend = StubBackend(username, version)
    executor = StubExecutor()
    return SyncOracleIntrospector(backend, executor), backend, executor


class TestSchemaResolution:
    def test_default_schema_from_config_username(self):
        insp, _, _ = make_sync_introspector("ar_shop")
        assert insp._get_default_schema() == "AR_SHOP"

    def test_version_from_backend(self):
        insp, _, _ = make_sync_introspector(version=(19, 8, 0))
        assert insp._get_version() == (19, 8, 0)

    def test_status_property_is_lazy_and_cached(self):
        insp, _, _ = make_sync_introspector()
        first = insp.status
        assert isinstance(first, SyncOracleStatusIntrospector)
        assert insp.status is first


class TestSqlBuilders:
    def test_columns_sql_binds_uppercase_owner_and_table(self):
        """Table and owner are bind values, never interpolated literals."""
        insp, _, _ = make_sync_introspector("ar_crm")
        sql, params = insp._build_columns_sql("customers", "ar_crm")
        assert "ALL_TAB_COLUMNS" in sql.upper()
        assert "CUSTOMERS" not in sql.upper()
        assert "AR_CRM" not in sql.upper()
        assert params == ("CUSTOMERS", "AR_CRM")

    def test_primary_key_sql_binds_uppercase_owner_and_table(self):
        insp, _, _ = make_sync_introspector()
        sql, params = insp._build_primary_key_sql("orders", "ar_shop")
        assert "ALL_CONSTRAINTS" in sql.upper()
        assert "'P'" in sql
        assert "ORDERS" not in sql.upper()
        assert "AR_SHOP" not in sql.upper()
        assert params == ("ORDERS", "AR_SHOP")

    def test_quoting_attempt_in_table_name_is_not_executed(self):
        """A name carrying SQL is bound as data and cannot alter the query."""
        insp, _, _ = make_sync_introspector()
        _, params = insp._build_columns_sql("t' OR '1'='1", "scott")
        assert params == ("T' OR '1'='1", "SCOTT")

    def test_dialect_table_list_query_scopes_owner(self):
        dialect = OracleDialect(version=(23, 0, 0))
        sql, params = dialect.format_table_list_query(
            TableListExpression(dialect, schema="ar_shop",
                                include_views=False)
        )
        assert "ALL_TABLES" in sql.upper() and "ALL_VIEWS" not in sql.upper()
        assert "AR_SHOP" in [str(p) for p in params]

        sql_all, _ = dialect.format_table_list_query(
            TableListExpression(dialect)
        )
        assert "UNION ALL" in sql_all and "ALL_VIEWS" in sql_all.upper()

    def test_dialect_index_and_fk_queries_join_constraints(self):
        dialect = OracleDialect(version=(23, 0, 0))
        idx_sql, idx_params = dialect.format_index_info_query(
            IndexInfoExpression(dialect, "orders").schema("ar_shop")
        )
        assert "ALL_INDEXES" in idx_sql.upper()
        assert "ALL_IND_COLUMNS" in idx_sql.upper()
        assert [str(p) for p in idx_params] == ["AR_SHOP", "ORDERS"]

        fk_sql, fk_params = dialect.format_foreign_key_query(
            ForeignKeyExpression(dialect, "orders").schema("ar_shop")
        )
        assert "'R'" in fk_sql
        assert [str(p) for p in fk_params] == ["AR_SHOP", "ORDERS"]

    def test_dialect_view_and_trigger_queries_target_dictionary(self):
        dialect = OracleDialect(version=(23, 0, 0))
        view_sql, view_params = dialect.format_view_list_query(
            ViewListExpression(dialect, schema="ar_crm")
        )
        assert "ALL_VIEWS" in view_sql.upper()
        assert [str(p) for p in view_params] == ["AR_CRM"]

        trig_sql, trig_params = dialect.format_trigger_list_query(
            TriggerListExpression(dialect, schema="ar_crm", table_name="customers")
        )
        assert "ALL_TRIGGERS" in trig_sql.upper()
        assert [str(p) for p in trig_params] == ["AR_CRM", "CUSTOMERS"]

    def test_database_info_sql_reads_nls_parameters(self):
        insp, _, _ = make_sync_introspector()
        sql, params = insp._build_database_info_sql()
        assert "NLS_CHARACTERSET" in sql.upper()
        assert "DUAL" in sql.upper()
        assert params == ()


COLUMN_ROWS = [
    {
        "COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER", "DATA_PRECISION": 10,
        "DATA_SCALE": 0, "NULLABLE": "N", "COLUMN_ID": 1,
        "IDENTITY_COLUMN": "YES", "CHAR_LENGTH": None, "DATA_LENGTH": 22,
    },
    {
        "COLUMN_NAME": "NAME", "DATA_TYPE": "VARCHAR2", "DATA_PRECISION": None,
        "DATA_SCALE": None, "NULLABLE": "Y", "COLUMN_ID": 2,
        "IDENTITY_COLUMN": "NO", "CHAR_LENGTH": 100, "DATA_LENGTH": 100,
    },
]


def _raw_row(name: str, data_length: int, column_id: int) -> Dict[str, Any]:
    return {
        "COLUMN_NAME": name, "DATA_TYPE": "RAW", "DATA_PRECISION": None,
        "DATA_SCALE": None, "NULLABLE": "Y", "COLUMN_ID": column_id,
        "IDENTITY_COLUMN": "NO", "CHAR_LENGTH": 0, "DATA_LENGTH": data_length,
    }


class TestParseHelpers:
    def test_parse_tables_maps_metadata(self):
        insp, _, _ = make_sync_introspector()
        tables = insp._parse_tables(
            [{"TABLE_NAME": "CUSTOMERS", "COMMENTS": "people", "NUM_ROWS": 5,
              "DATA_LENGTH": 8192, "LAST_ANALYZED": None}],
            "AR_CRM",
        )
        table = tables[0]
        assert (table.name, table.schema) == ("CUSTOMERS", "AR_CRM")
        assert table.comment == "people"
        assert table.row_count == 5
        assert table.size_bytes == 8192

    def test_parse_columns_marks_primary_key_and_identity(self):
        insp, _, _ = make_sync_introspector()
        columns = insp._parse_columns(COLUMN_ROWS, "CUSTOMERS", "AR_CRM", ["ID"])
        id_col, name_col = columns
        assert id_col.is_primary_key and not name_col.is_primary_key
        assert id_col.is_auto_increment
        assert id_col.nullable.value if hasattr(id_col.nullable, "value") else True
        assert id_col.data_type_full == "NUMBER(10)"
        assert name_col.data_type_full == "VARCHAR2(100)"
        # The **core** VarCharType, not an Oracle-namespaced subclass: a
        # subclass renders the same SQL but ``DataType.__eq__`` is class
        # identity, so it would report this column as changed against the very
        # ``VarCharType(length=100)`` declaration that produced it.
        assert type(name_col.parsed_data_type) is VarCharType
        assert name_col.parsed_data_type.length == 100
        # ... and the same for the integer width on the line above.
        assert type(id_col.parsed_data_type) is IntegerType


class TestRawWidthSurvivesIntrospection:
    """``RAW``'s width used to be dropped here, which is a false identity claim
    produced by introspection rather than by a declaration: ``_parse_columns``
    spelled out the size for ``NUMBER``/``FLOAT`` and the character types and
    not for ``RAW``, so every ``RAW`` column reached ``parse_type`` as a bare
    ``"RAW"`` and came back as ``OracleRawType(length=2000)`` — Oracle's
    ``MAX_STRING_SIZE = STANDARD`` ceiling, which a ``RAW(16)`` column does not
    have.

    ``DATA_LENGTH`` holds the declared width exactly (measured on 21c and 23c:
    16 for a ``RAW(16)``, 64 for a ``RAW(64)``, with ``DATA_PRECISION`` and
    ``DATA_SCALE`` both ``NULL``), so it is read here and the parser never has
    to guess. ``RAW(n)`` is not optional in Oracle — "You must specify size for
    a ``RAW`` value", and ``CREATE TABLE t (a RAW)`` is ``ORA-00906`` — so the
    un-sized string this fix removes is not one Oracle can produce.
    https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html
    """

    def test_the_declared_width_is_spelled_out(self):
        insp, _, _ = make_sync_introspector()
        columns = insp._parse_columns(
            [_raw_row("A", 16, 1), _raw_row("B", 64, 2)], "T", "AR_CRM"
        )
        assert [c.data_type_full for c in columns] == ["RAW(16)", "RAW(64)"]

    def test_the_two_widths_parse_to_two_different_types(self):
        insp, backend, _ = make_sync_introspector()
        columns = insp._parse_columns(
            [_raw_row("A", 16, 1), _raw_row("B", 64, 2)], "T", "AR_CRM"
        )
        a, b = (c.parsed_data_type for c in columns)
        assert a.length == 16
        assert b.length == 64
        assert a != b

    def test_a_declared_column_equals_the_introspected_one(self):
        """``==`` is the whole comparison (D4), so this is the assertion that
        makes the round trip worth anything: what the caller declared and what
        the catalog reports are the same column, not merely two columns that
        render alike."""
        insp, backend, _ = make_sync_introspector()
        dialect = backend.dialect
        columns = insp._parse_columns(
            [_raw_row("A", 16, 1), _raw_row("B", 64, 2)], "T", "AR_CRM"
        )
        assert columns[0].parsed_data_type == OracleRawType(dialect, length=16)
        assert columns[1].parsed_data_type == OracleRawType(dialect, length=64)
        assert columns[0].parsed_data_type != columns[1].parsed_data_type

    def test_introspection_never_produces_an_unsized_raw(self):
        """The parser refuses an un-sized ``RAW`` rather than inventing a width,
        so if the introspector regressed the column would fail to parse instead
        of quietly reporting the wrong byte count."""
        insp, backend, _ = make_sync_introspector()
        for raw in ("RAW", "raw"):
            with pytest.raises(ValueError):
                backend.dialect.parse_type(raw)

    def test_parse_foreign_keys_groups_columns(self):
        insp, _, _ = make_sync_introspector()
        rows = [
            {"CONSTRAINT_NAME": "FK1", "DELETE_RULE": "CASCADE",
             "REFERENCED_TABLE_NAME": "CUSTOMERS", "COLUMN_NAME": "CID",
             "REFERENCED_COLUMN_NAME": "ID"},
            {"CONSTRAINT_NAME": "FK1", "DELETE_RULE": "CASCADE",
             "REFERENCED_TABLE_NAME": "CUSTOMERS", "COLUMN_NAME": "TENANT",
             "REFERENCED_COLUMN_NAME": "TENANT"},
        ]
        fks = insp._parse_foreign_keys(rows, "ORDERS", "AR_SHOP")
        assert len(fks) == 1
        fk = fks[0]
        assert fk.columns == ["CID", "TENANT"]
        assert fk.referenced_columns == ["ID", "TENANT"]
        assert fk.referenced_table == "CUSTOMERS"

    def test_parse_views_updatable_flag(self):
        insp, _, _ = make_sync_introspector()
        views = insp._parse_views(
            [{"VIEW_NAME": "V1", "TEXT_VC": "SELECT 1 FROM DUAL",
              "READ_ONLY": "N"}],
            "AR_CRM",
        )
        assert views[0].name == "V1"
        assert views[0].is_updatable is True

    def test_parse_triggers_splits_events(self):
        insp, _, _ = make_sync_introspector()
        triggers = insp._parse_triggers(
            [{"TRIGGER_NAME": "T1", "TRIGGER_TYPE": "BEFORE STATEMENT",
              "TRIGGERING_EVENT": "INSERT OR UPDATE", "TABLE_NAME": "C",
              "TRIGGER_BODY": "BEGIN NULL; END;"}],
            "AR_CRM",
        )
        trigger = triggers[0]
        assert trigger.events == ["INSERT", "UPDATE"]
        assert trigger.timing == "BEFORE STATEMENT"

    def test_parse_database_info_uses_backend_version(self):
        insp, backend, _ = make_sync_introspector(version=(23, 5, 0))
        info = insp._parse_database_info([{"CHARSET": "AL32UTF8",
                                           "LANGUAGE": "AMERICAN"}])
        assert info.version_tuple == (23, 5, 0)
        assert info.vendor == "Oracle"
        assert info.encoding == "AL32UTF8"

    def test_async_status_property_type(self):
        backend = StubBackend()
        insp = AsyncOracleIntrospector(backend, SimpleNamespace(execute=None))
        assert isinstance(insp.status, AsyncOracleStatusIntrospector)


@pytest.fixture(scope="module")
def provisioned():
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)

    def drop_user_block(user: str) -> str:
        return (f"BEGIN EXECUTE IMMEDIATE 'DROP USER {user} CASCADE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;")

    backend_class, config = get_scenario_raw("oracle_23c")
    DeepCustomer.configure(config, backend_class)
    backend = DeepCustomer.__backend__
    if not backend._connection:
        backend.connect()
    for user in SCHEMA_USERS:
        backend.execute(drop_user_block(user), options=ddl_options)
    for user in SCHEMA_USERS:
        try:
            backend.execute(
                f'CREATE USER {user} IDENTIFIED BY "{SCHEMA_USER_PASSWORD}"',
                options=ddl_options,
            )
        except DatabaseError as exc:
            if "ORA-01031" in str(exc):
                pytest.skip(
                    "connected user lacks privileges to create schema users"
                )
            raise
        backend.execute(f"GRANT UNLIMITED TABLESPACE TO {user}",
                        options=ddl_options)
    for statement in (
        "CREATE TABLE AR_CRM.CUSTOMERS (id NUMBER GENERATED BY DEFAULT "
        "AS IDENTITY PRIMARY KEY, name VARCHAR2(100) NOT NULL)",
        "CREATE TABLE AR_SHOP.ORDERS (id NUMBER GENERATED BY DEFAULT "
        "AS IDENTITY PRIMARY KEY, customer_id NUMBER NOT NULL, "
        "amount NUMBER NOT NULL)",
        # Two RAW widths, so the live round trip has something to tell apart.
        # A bare RAW is not possible here: Oracle requires the size
        # (ORA-00906 without it).
        "CREATE TABLE AR_SHOP.BLOB_HOLDERS (id NUMBER GENERATED BY DEFAULT "
        "AS IDENTITY PRIMARY KEY, small RAW(16), large RAW(64), "
        "unbounded BLOB)",
    ):
        backend.execute(statement, options=ddl_options)
    yield backend
    for user in SCHEMA_USERS:
        try:
            backend.execute(drop_user_block(user), options=ddl_options)
        except Exception:
            pass


def fetch_all(backend, sql: str) -> List[Dict[str, Any]]:
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    result = backend.execute(
        sql, options=ExecutionOptions(stmt_type=StatementType.SELECT)
    )
    return [dict(row) for row in (result.data or [])]


class TestLiveSchemaProbing:
    def test_tables_exist_per_owner(self, provisioned):
        insp = provisioned.introspector
        crm_columns = insp.list_columns("CUSTOMERS", schema="AR_CRM")
        shop_columns = insp.list_columns("ORDERS", schema="AR_SHOP")
        assert [c.name for c in crm_columns] == ["ID", "NAME"]
        assert [c.name for c in shop_columns] == ["ID", "CUSTOMER_ID", "AMOUNT"]

    def test_column_inventory_matches_ddl(self, provisioned):
        insp = provisioned.introspector
        columns = insp.list_columns("CUSTOMERS", schema="AR_CRM")
        assert [(c.name, c.data_type, c.nullable.value) for c in columns] == [
            ("ID", "number", ColumnNullable.NOT_NULL.value),
            ("NAME", "varchar2", ColumnNullable.NOT_NULL.value),
        ]

    def test_list_columns_marks_identity_primary_key(self, provisioned):
        insp = provisioned.introspector
        columns = insp.list_columns("CUSTOMERS", schema="AR_CRM")
        id_col, name_col = columns
        assert id_col.is_primary_key and id_col.is_auto_increment
        assert not name_col.is_primary_key and not name_col.is_auto_increment
        assert id_col.data_type_full == "NUMBER"
        assert name_col.data_type_full == "VARCHAR2(100)"

    def test_list_tables_scoped_to_owner(self, provisioned):
        insp = provisioned.introspector
        crm_names = {t.name.upper() for t in insp.list_tables("AR_CRM")}
        assert "CUSTOMERS" in crm_names
        assert "ORDERS" not in crm_names

    def test_get_table_info_assembles_columns_and_indexes(self, provisioned):
        insp = provisioned.introspector
        table = insp.get_table_info("CUSTOMERS", schema="AR_CRM")
        assert table is not None
        assert table.name == "CUSTOMERS"
        assert [c.name for c in table.columns] == ["ID", "NAME"]
        assert table.columns[0].is_primary_key
        assert any(i.columns for i in table.indexes)
        assert table.foreign_keys == []

    def test_list_indexes_and_foreign_keys(self, provisioned):
        insp = provisioned.introspector
        indexes = insp.list_indexes("ORDERS", schema="AR_SHOP")
        assert indexes and all(i.columns for i in indexes)
        assert any(i.is_unique for i in indexes)
        fks = insp.list_foreign_keys("ORDERS", schema="AR_SHOP")
        assert fks == []

    def test_cross_schema_read_via_admin_connection(self, provisioned):
        customer = DeepCustomer(name="deep_probe")
        customer.save()
        rows = fetch_all(
            provisioned,
            f"SELECT customer_id FROM AR_SHOP.ORDERS WHERE customer_id = "
            f"{customer.id}",
        )
        assert rows == []
        names = fetch_all(
            provisioned,
            f"SELECT name FROM AR_CRM.CUSTOMERS WHERE id = {customer.id}",
        )
        assert names[0]["name"] == "deep_probe"


class TestLiveRawRoundTrip:
    """The end-to-end assertion, on a real server: create two ``RAW`` columns of
    different widths, introspect them, and require that a declared
    ``OracleRawType(length=16)`` is *equal* to what ``RAW(16)`` comes back as.

    Checking only the rendered SQL would pass with the old introspector, because
    the old one rendered ``RAW(2000)`` — confidently, and wrong. ``==`` is what
    the schema differ uses (D4), so equality is the property that matters, and
    the two widths must not collapse into one type.
    """

    def test_the_catalog_reports_the_declared_widths(self, provisioned):
        rows = fetch_all(
            provisioned,
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION "
            "FROM ALL_TAB_COLUMNS WHERE OWNER = 'AR_SHOP' "
            "AND TABLE_NAME = 'BLOB_HOLDERS' ORDER BY COLUMN_ID",
        )
        widths = {str(r["COLUMN_NAME"]).upper() if "COLUMN_NAME" in r
                  else str(r["column_name"]).upper():
                  (r.get("DATA_TYPE", r.get("data_type")),
                   r.get("DATA_LENGTH", r.get("data_length")),
                   r.get("DATA_PRECISION", r.get("data_precision")))
                  for r in rows}
        assert widths["SMALL"] == ("RAW", 16, None)
        assert widths["LARGE"] == ("RAW", 64, None)

    def test_two_widths_introspect_as_two_different_types(self, provisioned):
        columns = provisioned.introspector.list_columns(
            "BLOB_HOLDERS", schema="AR_SHOP"
        )
        by_name = {c.name: c for c in columns}
        assert by_name["SMALL"].parsed_data_type.length == 16
        assert by_name["LARGE"].parsed_data_type.length == 64
        assert (by_name["SMALL"].parsed_data_type
                != by_name["LARGE"].parsed_data_type)

    def test_a_declared_raw_equals_the_introspected_one(self, provisioned):
        dialect = provisioned.dialect
        columns = provisioned.introspector.list_columns(
            "BLOB_HOLDERS", schema="AR_SHOP"
        )
        by_name = {c.name: c for c in columns}
        assert by_name["SMALL"].parsed_data_type == OracleRawType(
            dialect, length=16)
        assert by_name["LARGE"].parsed_data_type == OracleRawType(
            dialect, length=64)
        assert by_name["SMALL"].parsed_data_type != OracleRawType(
            dialect, length=2000)

    def test_the_introspected_column_renders_back_unchanged(self, provisioned):
        dialect = provisioned.dialect
        columns = provisioned.introspector.list_columns(
            "BLOB_HOLDERS", schema="AR_SHOP"
        )
        rendered = {c.name: dialect.format_data_type(c.parsed_data_type)[0]
                    for c in columns if c.parsed_data_type is not None}
        assert rendered["SMALL"] == "RAW(16)"
        assert rendered["LARGE"] == "RAW(64)"
        assert rendered["UNBOUNDED"] == "BLOB"

    def test_a_uuid_column_introspects_as_sixteen_bytes(self, provisioned):
        """The width this backend declares for a UUID — ``RAW(16)``, which is
        also what ``UUID()`` returns — used to introspect as 2000 bytes."""
        dialect = provisioned.dialect
        columns = provisioned.introspector.list_columns(
            "BLOB_HOLDERS", schema="AR_SHOP"
        )
        small = next(c for c in columns if c.name == "SMALL")
        assert small.parsed_data_type == dialect.suggested_data_types()["uuid"](
            dialect)

    def test_a_declared_column_equals_the_introspected_one_for_the_core_types(self, provisioned):
        """The same property for the concepts whose Oracle-namespaced classes
        were deleted: ``VARCHAR2(n)``, ``CHAR(n)`` and ``BLOB`` each used to
        introspect as a *subclass* of the core concept, which renders identical
        SQL and compares unequal. ``==`` is what the schema differ uses (D4), so
        it is the property that matters.

        (``NUMBER(10)`` is covered in
        ``test_oracle_type_protocol.py::test_a_declared_integer_equals_the_introspected_one``.
        The ``id`` column here is ``NUMBER GENERATED BY DEFAULT AS IDENTITY``,
        which the catalog reports as a bare ``NUMBER`` with no precision, so it
        legitimately parses as ``DecimalType()`` — see the note there about the
        ``NUMBER`` direction mapping.)"""
        dialect = provisioned.dialect
        columns = provisioned.introspector.list_columns(
            "BLOB_HOLDERS", schema="AR_SHOP"
        )
        by_name = {c.name: c for c in columns}
        assert by_name["UNBOUNDED"].parsed_data_type == BlobType(dialect)
        assert dialect.format_data_type(
            by_name["UNBOUNDED"].parsed_data_type)[0] == "BLOB"


@pytest.fixture(scope="module")
def boolean_provisioned():
    """A table with a native ``BOOLEAN`` column, on whichever server is wired.

    ``CREATE TABLE t (a BOOLEAN)`` is ``ORA-00902`` below 23ai, so this skips
    rather than errors there — the measurement itself is the point, and a skip
    says it plainly. The ``provisioned`` fixture is pinned to the 23c scenario;
    this one uses the same connection so it is not a second provisioning.
    """
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.errors import DatabaseError
    from rhosocial.activerecord.backend.schema import StatementType

    ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
    backend_class, config = get_scenario_raw("oracle_23c")
    DeepCustomer.configure(config, backend_class)
    backend = DeepCustomer.__backend__
    if not backend._connection:
        backend.connect()
    try:
        backend.execute("DROP TABLE AR_SHOP.BOOL_FLAGS PURGE", options=ddl_options)
    except Exception:
        pass
    try:
        backend.execute(
            "CREATE TABLE AR_SHOP.BOOL_FLAGS (id NUMBER PRIMARY KEY, "
            "flag BOOLEAN)", options=ddl_options)
    except DatabaseError as exc:
        if "ORA-00902" in str(exc):
            pytest.skip(
                "this server has no native BOOLEAN column type (ORA-00902); "
                "the type arrived in 23ai"
            )
        raise
    yield backend
    try:
        backend.execute("DROP TABLE AR_SHOP.BOOL_FLAGS PURGE", options=ddl_options)
    except Exception:
        pass


def _server_version(backend) -> Tuple[int, ...]:
    """The base version ``PRODUCT_COMPONENT_VERSION`` reports, read directly.

    An independent reading of the same view the backend reads, so a test can
    compare the two.  ``normalize_version(..., minimum_length=3)[:3]`` is what
    the backend does to it, so the comparison below is like for like rather
    than padding on one side only.
    """
    rows = fetch_all(backend, "SELECT VERSION, VERSION_FULL, PRODUCT "
                              "FROM PRODUCT_COMPONENT_VERSION")
    tuples = [tuple(row.values()) for row in rows]
    base, _ = parse_version_row(select_version_row(tuples))
    return normalize_version(base)[:3]


def wired_scenarios() -> List[str]:
    """The scenario names actually wired into this run.

    Used as the parametrisation source instead of a fixed tuple of names.
    A fixed tuple is not a list of the servers under test: CI wires **one**
    scenario per job (each job writes its own ``oracle_scenarios.yaml`` with a
    single entry), so ``get_scenario_raw("oracle_23c")`` in the 21c job
    silently answers with the 21c server -- see :func:`scenario_or_skip`.
    Parametrising over what is wired makes the ids tell the truth about which
    server each case ran against.
    """
    from providers.scenarios import get_enabled_scenarios

    return sorted(get_enabled_scenarios())


def scenario_or_skip(name: str):
    """``get_scenario_raw(name)``, but **only if that scenario is wired**.

    ``get_scenario_raw`` substitutes *the first wired scenario* for a name it
    does not recognise.  That fallback is reasonable for a provider that wants
    "some reachable server" and wrong for a test that says what it is talking
    to: asking for ``oracle_23c`` in a job that wires only ``oracle_21c`` used
    to hand back 21c, and a test then asserted things about the 23-line server
    while reading an 21c one.

    The cost of that was not a wrong number in a failure message, it was a
    **lie about the fixture**: measured in CI run 37245026297, both the 18c and
    the 21c job failed ``assert backend.dialect.version[0] == 23`` with
    ``18`` and ``21`` respectively -- the backend had detected each job's own
    server perfectly and the assertion was about a server that was not there.

    So a test that names its scenario gets that scenario or skips saying which
    are wired. It never silently gets a different one.
    """
    from providers.scenarios import get_scenario_raw

    wired = wired_scenarios()
    if not wired:
        pytest.skip("no Oracle scenario is wired in this run")
    if name not in wired:
        pytest.skip(
            f"{name} is not wired in this run (wired: {wired}); this test is "
            f"about {name} specifically and will not assert about another "
            f"server in its place"
        )
    return get_scenario_raw(name)


class TestLiveNativeBooleanColumn:
    """The 23ai ``BOOLEAN`` column type, end to end.

    Measured on both wired servers: ``CREATE TABLE t (a BOOLEAN)`` is
    ``ORA-00902: invalid datatype`` on 21c and reports ``DATA_TYPE = 'BOOLEAN'``
    on 23ai/26ai. Oracle's own release documentation puts the feature in 23ai
    (https://docs.oracle.com/en/learn/db23ai-sql-features/index.html), and the
    catalog row for it carries no precision and no length beyond ``DATA_LENGTH``
    = 1, so there is nothing for the introspector to invent — which is the whole
    of what this class checks: the column reaches the **boolean concept**, not
    ``CustomType`` (where every unmodelled name lands, and which would render
    the bare word back by accident rather than by understanding).
    """

    def test_the_catalog_reports_the_native_type(self, boolean_provisioned):
        rows = fetch_all(
            boolean_provisioned,
            "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS "
            "WHERE OWNER = 'AR_SHOP' AND TABLE_NAME = 'BOOL_FLAGS' "
            "AND COLUMN_NAME = 'FLAG'",
        )
        data_type = rows[0].get("DATA_TYPE", rows[0].get("data_type"))
        assert str(data_type).upper() == "BOOLEAN"

    def test_a_native_boolean_column_parses_to_the_boolean_concept(self, boolean_provisioned):
        columns = boolean_provisioned.introspector.list_columns(
            "BOOL_FLAGS", schema="AR_SHOP"
        )
        flag = next(c for c in columns if c.name == "FLAG")
        assert flag.data_type_full == "BOOLEAN"
        assert type(flag.parsed_data_type) is BooleanType

    def test_it_renders_back_as_the_native_type(self, boolean_provisioned):
        """The gate produces the right column for this server.

        The version is read from the **server** rather than taken from the
        dialect, so this test is a real comparison and not a tautology; the
        class below is what pins that the two agree.
        """
        version = _server_version(boolean_provisioned)
        dialect = OracleDialect(version=version)
        columns = boolean_provisioned.introspector.list_columns(
            "BOOL_FLAGS", schema="AR_SHOP"
        )
        flag = next(c for c in columns if c.name == "FLAG")
        rendered = dialect.format_data_type(flag.parsed_data_type)[0]
        assert rendered == "BOOLEAN"
        assert dialect.format_data_type(BooleanType(dialect))[0] == rendered
        # One release lower and the concept is the number again — the two
        # renderings are different columns, not two spellings of one.
        older = OracleDialect(version=(version[0] - 1, 0, 0))
        assert older.format_data_type(BooleanType(older))[0] == "NUMBER(1)"


class TestDialectCarriesTheServersOwnVersion:
    """The detection defect this class replaces, now fixed and pinned per server.

    ``version.py`` used to select ``... FROM PRODUCT_COMPONENT_VERSION WHERE
    PRODUCT LIKE 'Oracle Database%'``.  That pattern matched the 21c product
    name (``Oracle Database 21c Express Edition ``) but not the 23ai one, which
    is ``Oracle AI Database 26ai Free``: **the server was renamed and the query
    still looked for the old name**.  Both queries then returned no rows,
    ``get_server_version`` logged "Could not determine Oracle version … defaulting
    to 19.0.0", the dialect carried ``(19, 0, 0)``, and every one of the ~100
    ``self.version >= …`` comparisons in this backend answered against it —
    including the ``BOOLEAN`` gate, which therefore wrote ``NUMBER(1)`` on a
    server with a native ``BOOLEAN`` column type.

    Detection now selects on the **shape of the version** rather than on any
    product name (see ``version.py`` for why, and for the measurements), and the
    two queries match nothing that a rebrand can change.  What is asserted here
    is the property that actually matters and that a rename could not preserve
    by accident: **the dialect's version equals the version the server reports**,
    on every wired scenario, whatever the product is called.

    Measured on both wired servers, and the whole of what each view holds (it has
    four columns and one row). **This table is a record of one image build, not
    a contract**: ``PRODUCT_COMPONENT_VERSION`` moved for 21c between two builds
    of the same scenario, which is why nothing below asserts these numbers. What
    the table is kept for is the rename -- one product string begins
    ``Oracle Database`` and the other does not, and neither can decide a version.

    ==================================  =============================  ==============
    server                              ``PRODUCT``                    ``VERSION``
    ==================================  =============================  ==============
    21c (``21.3.0.0.0``)                ``Oracle Database 21c Express  ``21.0.0.0.0``
                                        ``Edition `` (36 chars)
    26ai Free (``23.26.1.0.0``)         ``Oracle AI Database 26ai Free``  ``23.0.0.0.0``
    ==================================  =============================  ==============
    """

    #: The wired scenarios this runs against: **whatever is wired**, via
    #: :func:`wired_scenarios`. Deliberately not a fixed tuple of names, and
    #: deliberately no table of the version each server is expected to report.
    #:
    #: Both were wrong, for the same underlying reason. CI wires **one**
    #: scenario per job, so a hard-coded name is very often not wired at all and
    #: ``get_scenario_raw`` answers with a *different* server -- which is how
    #: the 18c and 21c jobs came to fail an assertion about the 23-line server
    #: (see :func:`scenario_or_skip`).
    #:
    #: The version table was the wrong test for the reason already given in the
    #: class docstring: it asserted something about **Oracle's** component
    #: numbering rather than about this library. ``PRODUCT_COMPONENT_VERSION``
    #: is an image-build detail -- it moved for 21c between two builds of the
    #: same scenario. The library's contract is that the dialect carries what
    #: the server reports; what number Oracle stamps on a given build is Oracle's
    #: to change.
    #:
    #: Nothing was lost. A fabricated detection is still caught, by the first
    #: assertion below, because a fabricated version does not equal the reported
    #: one -- that is exactly how the old ``(19, 0, 0)`` fallback was caught. And
    #: the rename hazard, which a table of expected numbers could not have
    #: caught either, has its own test in
    #: :meth:`test_no_product_name_decides_the_version`.
    #:
    #: Evaluated at collection time, which is when the scenario config has been
    #: read: ``providers.scenarios`` registers on import, which this module
    #: already does at the top.
    SCENARIOS = wired_scenarios()

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_the_dialect_carries_what_the_server_reports(self, scenario):
        backend_class, config = scenario_or_skip(scenario)
        backend = backend_class(connection_config=config)
        try:
            backend.connect()
        except Exception as exc:  # a scenario whose container is down
            pytest.skip(f"{scenario} is not reachable: {exc}")
        try:
            reported = _server_version(backend)
            detected = backend.get_server_version()
            assert detected == reported, (
                f"{scenario}: server reports {reported}, the backend detected "
                f"{detected}. Detection must match the server, and the old "
                f"(19, 0, 0) fallback made these disagree on any renamed "
                f"product."
            )
        finally:
            try:
                backend.disconnect()
            except Exception:
                pass

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_no_product_name_decides_the_version(self, scenario):
        """Whatever the server calls itself, the same version must come out.

        The two wired product names differ in the middle of the string
        (``Oracle AI Database …`` does not start with ``Oracle Database``), and a
        predicate written for one would find nothing in the other.  Reading the
        server's own ``PRODUCT`` back and asserting it is *not* what the answer
        depends on keeps the two apart.
        """
        from rhosocial.activerecord.backend.impl.oracle.version import (
            VERSION_FULL_QUERY, select_version_row,
        )

        backend_class, config = scenario_or_skip(scenario)
        backend = backend_class(connection_config=config)
        try:
            backend.connect()
        except Exception as exc:
            pytest.skip(f"{scenario} is not reachable: {exc}")
        try:
            rows = fetch_all(backend, "SELECT PRODUCT, VERSION, VERSION_FULL "
                                      "FROM PRODUCT_COMPONENT_VERSION")
            tuples = [tuple(r.values()) for r in rows]
            products = {str(t[0]).strip() for t in tuples}
            versions = {t[1] for t in tuples}

            # The query finds a row here - that is what the old LIKE predicate
            # could not do on a renamed server.
            raw = fetch_all(backend, VERSION_FULL_QUERY)
            assert raw, (
                f"{scenario}: {VERSION_FULL_QUERY!r} matched nothing, so "
                f"detection would fall back to 'unknown'. Products on this "
                f"server: {products!r}"
            )
            assert select_version_row([tuple(r.values()) for r in raw]) is not None

            # And it is a *number* that decides, so a product string with no
            # bearing on the answer is ignored entirely.
            assert versions, f"{scenario} reported no VERSION at all"
            detected = backend.get_server_version()
            assert detected is not None
            assert detected[0] == int(
                sorted(versions, key=lambda v: -int(str(v).split(".")[0]))[0]
                .split(".")[0]
            )
        finally:
            try:
                backend.disconnect()
            except Exception:
                pass

    def test_the_wired_23ai_server_is_no_longer_mis_detected(self, provisioned):
        """The exact symptom this round's predecessor recorded as a fact, now
        recorded as its absence.

        It used to skip once the rename was handled; a skip is not an assertion,
        so it is now a real one: on a server at or past 23, the dialect must not
        be sitting on the fallback, and the ``BOOLEAN`` gate must therefore be
        live rather than inert.
        """
        reported = _server_version(provisioned)
        if reported[0] < 23:
            pytest.skip(f"server reports {reported}, before the renaming")
        assert provisioned.dialect.version == reported, (
            f"server reports {reported} but the dialect believes "
            f"{provisioned.dialect.version}"
        )
        assert provisioned.dialect.version != (19, 0, 0), (
            "the dialect is on the old (19, 0, 0) fallback; detection is "
            "broken again on this server"
        )
        # and the consequence: the gate is live, so the concept renders as the
        # native type rather than the number standing in for it.
        dialect = provisioned.dialect
        assert dialect.supports_boolean_type() is True
        assert dialect.format_data_type(BooleanType(dialect=dialect))[0] == (
            "BOOLEAN"
        )

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_the_dialect_carries_the_version_and_ru_the_server_reports(self, scenario):
        """**Detection.** Whatever the server reports, the dialect carries it.

        Three numbers, three sources, and every one of them read back off
        ``PRODUCT_COMPONENT_VERSION`` on the server rather than written down
        here: ``version`` from ``VERSION``, ``version_full`` from
        ``VERSION_FULL``, and ``ru_version`` from the second component of
        ``VERSION_FULL`` (``ru_from_version_full``). The dialect is refreshed
        through ``introspect_and_adapt`` so the whole chain runs.

        There is no ``23`` and no ``26`` in this test, and that is the point:
        it holds on 18c, 21c and the 23 line alike, so it cannot become a
        tripwire for Oracle's release numbering. ``test_oracle_version_detection.py``
        pins what those numbers mean, offline, against recorded rows.
        """
        backend_class, config = scenario_or_skip(scenario)
        backend = backend_class(connection_config=config)
        try:
            backend.connect()
        except Exception as exc:
            pytest.skip(f"{scenario} is not reachable: {exc}")
        try:
            rows = fetch_all(backend, "SELECT VERSION, VERSION_FULL, PRODUCT "
                                      "FROM PRODUCT_COMPONENT_VERSION")
            tuples = [tuple(r.values()) for r in rows]
            base, full = parse_version_row(select_version_row(tuples))
            backend.introspect_and_adapt()
            dialect = backend.dialect

            reported = tuple(base) + (0,) * (3 - len(base))
            assert dialect.version == reported[:3], (
                f"{scenario}: server reports VERSION {base}, the dialect "
                f"believes {dialect.version}"
            )
            assert full is not None, (
                f"{scenario}: no VERSION_FULL on this server, so there is no "
                f"release update to carry"
            )
            assert dialect.version_full == full, (
                f"{scenario}: server reports VERSION_FULL {full}, the dialect "
                f"carries {dialect.version_full}"
            )
            assert dialect.ru_version == full[1], (
                f"{scenario}: ru_version is the release-update component, so it "
                f"must be {full[1]} for {full}, not {dialect.ru_version}"
            )
            # The brand is read for logging and for nothing else: the product
            # name is free text and every number above came from a numeric
            # column. Naming it here is what makes that separation visible.
            product = product_name_from_row(select_version_row(tuples))
            assert product, f"{scenario}: this query selects PRODUCT"
        finally:
            try:
                backend.disconnect()
            except Exception:
                pass

    def test_the_26ai_scenario_is_a_23_line_server(self):
        """**The fixture**, stated as what it is for and asserted as such.

        Split out from the detection test above because it is a different kind
        of claim and it fails for a different reason. The two are not
        interchangeable, and conflating them is what made the earlier version
        of this test fail for the wrong reason in CI:

        * CI wires **one** scenario per job. The 18c and 21c jobs do not wire
          ``oracle_23c``, and ``get_scenario_raw`` answers an unknown name
          with the first wired scenario. So this test, which asked for
          ``oracle_23c``, was silently handed 18c and 21c and then asserted
          ``version[0] == 23`` about them -- failing with ``assert 18 == 23``
          and ``assert 21 == 23`` (measured, CI run 37245026297). The backend
          had detected each job's own server correctly; the assertion was about
          a server that was not there.
        * Meanwhile the 23c job, where the scenario *is* wired, failed
          something else entirely (a connection limit in an unrelated CLI
          test), so the premise of this scenario was never contradicted by CI
          at all.

        So: this test now skips unless ``oracle_23c`` is genuinely wired
        (:func:`scenario_or_skip`), and where it runs it says out loud which
        two facts about the fixture it depends on -- the line is 23, and the
        brand says 26 -- rather than leaving them implicit in a bare integer
        comparison. If Oracle ever ships a 26ai on a different line number,
        this fails with "this scenario is not the 23-line server" instead of a
        puzzling ``assert 26 == 23``, and that failure is worth having: it would
        be a real change in what this scenario demonstrates.

        Oracle's own statement of the rule: "The first number of the release
        stays the same since Oracle AI Database 26ai simply replaces Oracle
        Database 23ai - it remains '23'. The second number of the release
        indicates the year of the release update, e.g. 26 for 2026."
        """
        backend_class, config = scenario_or_skip("oracle_23c")
        backend = backend_class(connection_config=config)
        try:
            backend.connect()
        except Exception as exc:
            pytest.skip(f"oracle_23c is not reachable: {exc}")
        try:
            rows = fetch_all(backend, "SELECT VERSION, VERSION_FULL, PRODUCT "
                                      "FROM PRODUCT_COMPONENT_VERSION")
            tuples = [tuple(r.values()) for r in rows]
            base, full = parse_version_row(select_version_row(tuples))
            name = product_name_from_row(select_version_row(tuples))

            assert "26ai" in (name or ""), (
                "this scenario is meant to be the renamed server, but it "
                f"reports PRODUCT {name!r}; the 23ai/26ai distinction this "
                "test documents cannot be demonstrated here"
            )
            assert base and base[0] == 23, (
                f"this scenario is not the 23-line server: it reports VERSION "
                f"{base} (full {full}, product {name!r}). Either the scenario "
                f"now points at a different server, or Oracle moved the line "
                f"number off 23 -- and the rebrand this scenario exists to "
                f"demonstrate is premised on it staying 23."
            )
            backend.introspect_and_adapt()
            assert full is not None, (
                f"oracle_23c reports no VERSION_FULL, so the release-update "
                f"number that tells 23ai from 26ai cannot be demonstrated"
            )
            # Both true at once, and neither read from the other: the product
            # name says 26 while the version says 23.
            assert backend.dialect.version[0] == 23, (
                f"detection disagrees with the server: dialect says "
                f"{backend.dialect.version}, server reports {base}"
            )
            assert backend.dialect.ru_version == full[1], (
                f"ru_version must be the release-update component {full[1]} of "
                f"{full}, not {backend.dialect.ru_version}"
            )
        finally:
            try:
                backend.disconnect()
            except Exception:
                pass


class TestLiveStatusSurface:
    def test_tablespaces_listing_non_empty(self, provisioned):
        tablespaces = provisioned.introspector.status.list_tablespaces()
        assert len(tablespaces) >= 1

    def test_users_contain_system(self, provisioned):
        users = provisioned.introspector.status.list_users()
        assert "SYSTEM" in {u.name.upper() for u in users}


@pytest.mark.asyncio
async def test_async_status_tablespaces():
    from rhosocial.activerecord.backend.impl.oracle.backend.async_backend import AsyncOracleBackend

    _, config = get_scenario_raw("oracle_23c")
    backend = AsyncOracleBackend(connection_config=config)
    await backend.connect()
    try:
        tablespaces = await backend.introspector.status.list_tablespaces()
        assert isinstance(tablespaces, list)
    finally:
        try:
            await backend.disconnect()
        except Exception:
            pass
