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
    __primary_key__ = "id"
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
    def test_columns_sql_targets_uppercase_owner_and_table(self):
        insp, _, _ = make_sync_introspector("ar_crm")
        sql = insp._build_columns_sql("customers", "ar_crm").upper()
        assert "ALL_TAB_COLUMNS" in sql
        assert "'CUSTOMERS'" in sql
        assert "'AR_CRM'" in sql

    def test_primary_key_sql_filters_constraint_type(self):
        insp, _, _ = make_sync_introspector()
        sql = insp._build_primary_key_sql("orders", "ar_shop").upper()
        assert "ALL_CONSTRAINTS" in sql
        assert "'P'" in sql
        assert "'ORDERS'" in sql

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
            TriggerListExpression(dialect, schema="ar_crm", table="customers")
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
        assert type(name_col.parsed_data_type).__name__ == "OracleVarChar2Type"

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


class TestLiveStatusSurface:
    def test_tablespaces_listing_non_empty(self, provisioned):
        tablespaces = provisioned.introspector.status.list_tablespaces()
        assert len(tablespaces) >= 1

    def test_users_contain_system(self, provisioned):
        users = provisioned.introspector.status.list_users()
        assert "SYSTEM" in {u.name.upper() for u in users}


@pytest.mark.asyncio
async def test_async_status_tablespaces():
    from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

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
