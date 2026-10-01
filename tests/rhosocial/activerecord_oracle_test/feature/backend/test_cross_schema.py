# tests/rhosocial/activerecord_oracle_test/feature/backend/test_cross_schema.py
"""Oracle cross-schema behaviour, asserted against this dialect only.

Kept in this repository rather than the shared testsuite because schema
qualification is dialect-specific, and on Oracle the specifics are sharper
than elsewhere. A schema *is* the user: it is created with ``CREATE USER``
and dropped with ``DROP USER ... CASCADE``, so the tests here provision real
users rather than ``CREATE SCHEMA`` the way SQL Server does. The dialect also
folds every identifier to upper case in ``format_identifier``, so a model
declaring ``__schema_name__ = \"ar_xcrm\"`` addresses ``\"AR_XCRM\"`` -- the
round trip is only correct because Oracle is case-insensitive here, and a
test that asserted the declared spelling would be asserting the wrong thing.

``test_schema_qualified_dml.py`` already covers plain insert/update/delete
scoping. This file covers what that one cannot: a table name colliding
across two namespaces, soft-delete ``restore()``, a join that crosses a user
boundary, and the rendered form of the qualification.
"""
import re

from typing import ClassVar, Optional

import pytest

from rhosocial.activerecord.base.field_proxy import FieldProxy
from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.field.soft_delete import (
    DefaultAsyncSoftDeleteMixin,
    DefaultSoftDeleteMixin,
)
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord

from providers.scenarios import get_enabled_scenarios, get_scenario_raw

# The users below are shared and dropped per module, so the whole file has to
# land on a single xdist worker; --dist=loadgroup honours this mark.
pytestmark = pytest.mark.xdist_group("oracle_cross_schema")

# Names are deliberately not AR_CRM / AR_SHOP. Three other modules create and
# drop those users under the oracle_schema_dml group, and a different group
# lands on a different worker, so sharing the names had one module dropping a
# user out from under another mid-test (ORA-01920, then ORA-00942).
SCHEMA_CRM = "ar_xcrm"
SCHEMA_SHOP = "ar_xshop"
SCHEMA_PASSWORD = "Rh0social#2026"
SOFT_TABLE = "ar_soft_orders"
CUSTOMER_TABLE = "ar_customers"
SHOP_ORDER_TABLE = "ar_orders"


def _ddl_options() -> ExecutionOptions:
    return ExecutionOptions(stmt_type=StatementType.DDL)


def _drop_user_block(user: str) -> str:
    return (
        f"BEGIN EXECUTE IMMEDIATE 'DROP USER {user} CASCADE'; "
        "EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


def _create_user_with_retry(backend, user: str, attempts: int = 3) -> None:
    """CREATE USER right after DROP USER CASCADE can race (ORA-01920/01918)."""
    for attempt in range(attempts):
        try:
            backend.execute(
                f'CREATE USER {user} IDENTIFIED BY "{SCHEMA_PASSWORD}"',
                options=_ddl_options(),
            )
            return
        except DatabaseError as exc:
            if "ORA-01920" not in str(exc) or attempt == attempts - 1:
                raise
            backend.execute(_drop_user_block(user), options=_ddl_options())
            import time

            time.sleep(1.5)


def _drop_table_block(qualified: str) -> str:
    """Drop a table if it is there.

    A bare DROP TABLE raises ORA-00942 when the table is absent, which would
    abort provisioning before any CREATE TABLE ran. The users are dropped
    first, so their tables are usually already gone.
    """
    return (
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {qualified}'; "
        "EXCEPTION WHEN OTHERS THEN NULL; END;"
    )


def _provision(backend) -> None:
    for user in (SCHEMA_CRM, SCHEMA_SHOP):
        backend.execute(_drop_user_block(user), options=_ddl_options())
    for user in (SCHEMA_CRM, SCHEMA_SHOP):
        _create_user_with_retry(backend, user)
        backend.execute(
            f"GRANT UNLIMITED TABLESPACE TO {user}", options=_ddl_options()
        )

    # The soft-order table is created twice under one name -- once in the
    # default schema and once under AR_XCRM -- because that collision is the
    # whole point: it is what a namespace-scoped bug corrupts.
    soft_columns = (
        "id NUMBER NOT NULL PRIMARY KEY, "
        "label VARCHAR2(100) NOT NULL, "
        "deleted_at TIMESTAMP NULL"
    )
    statements = [
        _drop_table_block(f"{SCHEMA_CRM}.{SOFT_TABLE.upper()}"),
        _drop_table_block(SOFT_TABLE.upper()),
    ]
    for owner in (None, SCHEMA_CRM):
        qualified = f"{owner.upper()}.{SOFT_TABLE.upper()}" if owner else SOFT_TABLE.upper()
        statements.append(f"CREATE TABLE {qualified} ({soft_columns})")
    statements.append(_drop_table_block(f"{SCHEMA_CRM}.{CUSTOMER_TABLE.upper()}"))
    statements.append(
        f"CREATE TABLE {SCHEMA_CRM}.{CUSTOMER_TABLE.upper()} ("
        "id NUMBER NOT NULL PRIMARY KEY, name VARCHAR2(100) NOT NULL)"
    )
    # A differently named table on purpose: SQL Server rejects a join whose
    # two ranges share an exposed name, and the two modules are meant to
    # read the same way. Same-name coexistence is asserted on its own.
    statements.append(_drop_table_block(f"{SCHEMA_SHOP}.{SHOP_ORDER_TABLE.upper()}"))
    statements.append(
        f"CREATE TABLE {SCHEMA_SHOP}.{SHOP_ORDER_TABLE.upper()} ("
        "id NUMBER NOT NULL PRIMARY KEY, "
        "customer_id NUMBER NOT NULL, "
        "label VARCHAR2(100) NOT NULL)"
    )
    for sql in statements:
        backend.execute(sql, options=_ddl_options())


def _cleanup(backend) -> None:
    for user in (SCHEMA_CRM, SCHEMA_SHOP):
        try:
            backend.execute(_drop_user_block(user), options=_ddl_options())
        except Exception:
            pass


def _bind(model, config, backend_class, backend) -> None:
    model.__connection_config__ = config
    model.__backend_class__ = backend_class
    model.__backend__ = backend


class PlainSoftOrder(DefaultSoftDeleteMixin, ActiveRecord):
    """Soft-delete model in the default schema (the connected user)."""

    __table_name__ = SOFT_TABLE
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    label: str


class CrmSoftOrder(DefaultSoftDeleteMixin, ActiveRecord):
    """Identical table and identical model, owned by ``AR_XCRM``."""

    __table_name__ = SOFT_TABLE
    __schema_name__ = SCHEMA_CRM
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    label: str


class CrmCustomer(ActiveRecord):
    __table_name__ = CUSTOMER_TABLE
    __schema_name__ = SCHEMA_CRM
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    name: str


class ShopOrder(ActiveRecord):
    """A third owner, so a join can cross a user boundary.

    Deliberately a different table name from the soft-order table, so the
    join is not also asserting that the dialect tolerates two ranges with the
    same exposed name.
    """

    __table_name__ = SHOP_ORDER_TABLE
    __schema_name__ = SCHEMA_SHOP
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    customer_id: int
    label: str


class AsyncCrmSoftOrder(DefaultAsyncSoftDeleteMixin, AsyncActiveRecord):
    __table_name__ = SOFT_TABLE
    __schema_name__ = SCHEMA_CRM
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    label: str


class AsyncPlainSoftOrder(DefaultAsyncSoftDeleteMixin, AsyncActiveRecord):
    __table_name__ = SOFT_TABLE
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    label: str


@pytest.fixture(scope="module")
def cross_schema():
    """Provision both users on whichever scenario this job registered."""
    scenarios = get_enabled_scenarios()
    if not scenarios:
        pytest.skip("no Oracle scenario registered for this job")
    # get_scenario_raw, not get_scenario: under a database pool the latter
    # swaps in the worker's pooled user, which cannot create users, so every
    # test in this module would skip on ORA-01031. The admin credentials are
    # what CREATE USER needs, and the xdist_group mark keeps the shared users
    # to one worker.
    backend_class, config = get_scenario_raw(next(iter(scenarios)))
    PlainSoftOrder.configure(config, backend_class)
    backend = PlainSoftOrder.__backend__
    if not backend._connection:
        backend.connect()
    try:
        _provision(backend)
    except DatabaseError as exc:
        if "ORA-01031" in str(exc):
            pytest.skip("connected user lacks privileges to create schema users")
        raise
    for model in (
        PlainSoftOrder,
        CrmSoftOrder,
        CrmCustomer,
        ShopOrder,
        AsyncCrmSoftOrder,
        AsyncPlainSoftOrder,
    ):
        _bind(model, config, backend_class, backend)
    yield backend
    _cleanup(backend)


def _norm(sql: str) -> str:
    """Fold whitespace and case away, keep the double quotes.

    Oracle folds identifiers itself, so the quotes are the assertion: they
    are what lets a mixed-case name survive, and dropping them (as the old
    shared suite did) would hide a change in how the dialect quotes.
    """
    return re.sub(r"\s+", " ", sql).lower()


def test_declared_schema_is_folded_to_upper_case(cross_schema):
    """``__schema_name__ = "ar_xcrm"`` renders as ``"AR_XCRM"``.

    The model declares lower case and the dialect upper-cases it. That is
    correct here only because the user was created upper-case too, so the
    assertion is on the rendered form rather than on the declared spelling.

    Matched against the raw SQL rather than the normalised copy: _norm folds
    case, which is the very thing under test.
    """
    sql, _ = CrmSoftOrder.query().select(CrmSoftOrder.c.label).to_sql()

    assert '"AR_XCRM"."AR_SOFT_ORDERS"' in sql, (
        f"Expected the owner folded to upper case, got: {sql}"
    )


def test_qualified_column_round_trips(cross_schema):
    """Oracle keeps the owner on the column too, and that is valid here.

    A schema-bound model's column renders three-part --
    ``"AR_XCRM"."AR_SOFT_ORDERS"."LABEL"`` -- because the generic renderer
    qualifies whatever carries a schema. SQL Server cannot accept that form
    and its dialect drops the owner; Oracle accepts it, so this asserts the
    round trip rather than the shape. It is the same query the SQL Server
    module asserts the opposite of, which is the point of keeping the two
    suites separate.
    """
    CrmSoftOrder(id=61, label="round-trip").save()

    row = CrmSoftOrder.query().where(CrmSoftOrder.c.id == 61).one()
    assert row.label == "round-trip"

    sql, _ = CrmSoftOrder.query().select(CrmSoftOrder.c.label).to_sql()
    assert '"ar_xcrm"."ar_soft_orders"' in _norm(sql), f"Got: {sql}"


def test_same_named_tables_coexist_and_pk_is_namespace_scoped(cross_schema):
    """One table name, two owners, identical primary keys."""
    CrmSoftOrder(id=1, label="crm-row").save()
    PlainSoftOrder(id=1, label="default-row").save()

    assert CrmSoftOrder.query().where(CrmSoftOrder.c.id == 1).one().label == "crm-row"
    assert (
        PlainSoftOrder.query().where(PlainSoftOrder.c.id == 1).one().label
        == "default-row"
    )

    CrmSoftOrder.query().where(CrmSoftOrder.c.id == 1).delete_all()
    assert CrmSoftOrder.query().count() == 0
    assert PlainSoftOrder.query().count() == 1, (
        "deleting under AR_XCRM must not remove the identically keyed row "
        "owned by the connected user"
    )


def test_restore_writes_only_into_its_own_namespace(cross_schema):
    """``restore()`` has to carry the schema down to the UPDATE.

    An unqualified UPDATE resolves to the connected user's schema, so it
    would clear ``deleted_at`` on the default-schema row of the same primary
    key and leave the ``AR_XCRM`` row soft-deleted -- a silent cross-namespace
    write that no read-only assertion would catch.
    """
    scoped = CrmSoftOrder(id=7, label="scoped")
    plain = PlainSoftOrder(id=7, label="plain")
    scoped.save()
    plain.save()

    scoped.delete()
    assert CrmSoftOrder.query_only_deleted().count() == 1
    assert PlainSoftOrder.query_only_deleted().count() == 0

    assert scoped.restore() == 1
    assert CrmSoftOrder.query().where(CrmSoftOrder.c.id == 7).count() == 1, (
        "restore() must clear deleted_at under AR_XCRM"
    )
    assert PlainSoftOrder.query_only_deleted().count() == 0, (
        "the default-schema row was never soft-deleted and must stay untouched"
    )


def test_bulk_update_stays_inside_its_namespace(cross_schema):
    """A predicate on a schema-bound model must not widen to the sibling."""
    CrmSoftOrder(id=21, label="a").save()
    CrmSoftOrder(id=22, label="b").save()
    PlainSoftOrder(id=21, label="a").save()

    CrmSoftOrder.query().where(CrmSoftOrder.c.label == "a").update_all({"label": "z"})

    assert CrmSoftOrder.query().where(CrmSoftOrder.c.label == "z").count() == 1
    assert PlainSoftOrder.query().where(PlainSoftOrder.c.label == "a").count() == 1, (
        "bulk update leaked into the default schema"
    )


def test_join_across_two_owners(cross_schema):
    """``AR_XCRM`` joined to ``AR_XSHOP``, both sides fully qualified."""
    CrmCustomer(id=31, name="alice").save()
    ShopOrder(id=31, customer_id=31, label="from-shop").save()

    joined = CrmCustomer.query().join(
        ShopOrder, on=CrmCustomer.c.id == ShopOrder.c.customer_id
    )
    assert joined.count() == 1, "Expected the join to match across both owners"

    joined_sql, _ = joined.select(CrmCustomer.c.name).to_sql()
    normed = _norm(joined_sql)
    assert '"ar_xcrm"."ar_customers"' in normed, f"Got: {joined_sql}"
    assert '"ar_xshop"."ar_orders"' in normed, (
        f"Expected the joined range to keep its own owner, got: {joined_sql}"
    )


@pytest.mark.asyncio
async def test_async_restore_writes_only_into_its_own_namespace(cross_schema):
    """Async mirror of the restore contract."""
    from rhosocial.activerecord.backend.impl.oracle.backend.async_backend import (
        AsyncOracleBackend,
    )

    scenarios = get_enabled_scenarios()
    # Same credentials the module fixture provisioned under, so the async
    # connection reaches the database that actually holds AR_XCRM.
    backend_class, config = get_scenario_raw(next(iter(scenarios)))
    backend = AsyncOracleBackend(connection_config=config)
    await backend.connect()
    try:
        for model in (AsyncCrmSoftOrder, AsyncPlainSoftOrder):
            _bind(model, config, AsyncOracleBackend, backend)

        scoped = AsyncCrmSoftOrder(id=41, label="scoped")
        plain = AsyncPlainSoftOrder(id=41, label="plain")
        await scoped.save()
        await plain.save()

        await scoped.delete()
        assert await AsyncCrmSoftOrder.query_only_deleted().count() == 1
        assert await AsyncPlainSoftOrder.query_only_deleted().count() == 0

        assert await scoped.restore() == 1
        assert await AsyncCrmSoftOrder.query().where(
            AsyncCrmSoftOrder.c.id == 41
        ).count() == 1, "async restore() must clear deleted_at under AR_XCRM"
        assert await AsyncPlainSoftOrder.query_only_deleted().count() == 0
    finally:
        try:
            await backend.disconnect()
        except Exception:
            pass
