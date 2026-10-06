# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_namespace_wiring.py
"""Wiring tests for Oracle's schema-object naming path.

Every named object Oracle touches renders itself through the core's
``format_<kind>_object``, and these tests pin the two things that path is
responsible for:

* the owner slot is rendered -- ``"SCHEMA"."NAME"`` -- for every object kind;
* a slot the engine cannot express is reported rather than dropped.

The second point is the reason Oracle answers ``supports_catalog()`` false: the
owner is Oracle's only namespace, so an object that carries a ``catalog_name``
names something this dialect cannot address, and silently ignoring the slot
would resolve the name against the wrong object.

Two mixins answer two questions here, and the split is the point.
:class:`~rhosocial.activerecord.backend.impl.oracle.mixins.namespace.OracleNamespaceMixin`
is the naming side: it implements :class:`NamespaceSupport`, declares
``supports_schema_qualification()``, and spells the name through
``format_qualified_name``. :class:`~rhosocial.activerecord.backend.impl.oracle.mixins.schema.OracleSchemaMixin`
is the DDL side: does Oracle have schemas it can create and drop. It does not --
they follow CREATE USER -- so it answers False there and has nothing to say about
naming.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.protocols import NamespaceSupport
from rhosocial.activerecord.backend.expression.objects import (
    Domain,
    ForeignTable,
    Function,
    Index,
    MaterializedView,
    Procedure,
    Sequence,
    Synonym,
    Table,
    Trigger,
    Type,
    View,
)
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression.objects import OracleTable

SCHEMA = "ar_crm"

# Every kind Oracle names in a statement. A kind missing from this list would
# be a kind whose owner never reaches the SQL.
ALL_KINDS = [
    Table,
    ForeignTable,
    View,
    MaterializedView,
    Index,
    Sequence,
    Type,
    Domain,
    Function,
    Procedure,
    Trigger,
    Synonym,
]


@pytest.fixture
def dialect():
    return OracleDialect(version=(19, 0, 0))


def render(dialect, kind, *args, **slots):
    """Render *kind* the way a statement does: through its own to_sql()."""
    return kind(dialect, *args, **slots).to_sql()


class TestNamespaceProtocols:
    def test_namespace_support(self, dialect):
        assert isinstance(dialect, NamespaceSupport)

    def test_naming_and_ddl_switches_are_on_different_mixins(self, dialect):
        """The two questions have two owners, and neither mixin answers both.

        Asserted rather than described because the reason they were split is
        that one dialect must be able to disagree with itself: Oracle has owners
        (naming) but no schema DDL, and MySQL has the reverse. A single mixin
        holding both switches could not say so.
        """
        from rhosocial.activerecord.backend.impl.oracle.mixins import (
            OracleNamespaceMixin,
            OracleSchemaMixin,
        )

        assert "format_qualified_name" in vars(OracleNamespaceMixin)
        assert "supports_schema_qualification" in vars(OracleNamespaceMixin)
        assert "supports_schema" in vars(OracleSchemaMixin)
        assert "format_qualified_name" not in vars(OracleSchemaMixin)
        assert "supports_schema_qualification" not in vars(OracleSchemaMixin)

    def test_owner_may_qualify_a_name(self, dialect):
        assert dialect.supports_schema_qualification() is True

    def test_no_namespace_above_the_owner(self, dialect):
        """Oracle has no namespace above the owner, so it must not claim one."""
        assert dialect.supports_catalog() is False
        assert dialect.supports_catalog_qualification() is False

    def test_ddl_switch_is_a_separate_question(self, dialect):
        """Oracle namespaces per user schema but cannot CREATE SCHEMA."""
        assert dialect.supports_schema() is True
        assert dialect.supports_create_schema() is False
        assert dialect.supports_drop_schema() is False


class TestNamespaceRendering:
    @pytest.mark.parametrize("kind", ALL_KINDS)
    def test_owner_is_rendered(self, dialect, kind):
        sql, params = render(dialect, kind, "t", schema_name=SCHEMA)
        assert sql == '"AR_CRM"."T"'
        assert params == ()

    @pytest.mark.parametrize("kind", ALL_KINDS)
    def test_unqualified_name(self, dialect, kind):
        sql, _ = render(dialect, kind, "t")
        assert sql == '"T"'

    @pytest.mark.parametrize("kind", ALL_KINDS)
    def test_catalog_is_reported_not_dropped(self, dialect, kind):
        with pytest.raises(UnsupportedFeatureError, match="catalog"):
            render(dialect, kind, "t", catalog_name="other", schema_name=SCHEMA)

    def test_case_folding_is_preserved(self, dialect):
        """Oracle folds unquoted identifiers to upper case; quoting must too."""
        sql, _ = render(dialect, Table, "my_table", schema_name="scott")
        assert sql == '"SCOTT"."MY_TABLE"'

    def test_embedded_quote_is_doubled(self, dialect):
        sql, _ = render(dialect, Table, 'we"ird')
        assert sql == '"WE""IRD"'

    def test_unquoted_request_is_honoured(self, dialect):
        sql, _ = render(dialect, Table, "t", name_need_quote=False)
        assert sql == "t"

    def test_empty_slot_rejected(self, dialect):
        with pytest.raises(ValueError):
            render(dialect, Table, "t", schema_name="")


class TestRemoteNameSuffix:
    def test_dblink_rendered_after_the_name(self, dialect):
        sql, _ = OracleTable(
            dialect, "orders", schema_name=SCHEMA, dblink="world"
        ).to_sql()
        assert sql == '"AR_CRM"."ORDERS"@"WORLD"'

    def test_plain_table_has_no_suffix(self, dialect):
        sql, _ = render(dialect, Table, "orders")
        assert sql == '"ORDERS"'

    def test_dblink_may_opt_out_of_quoting(self, dialect):
        sql, _ = OracleTable(
            dialect, "orders", dblink="world", dblink_need_quote=False
        ).to_sql()
        assert sql == '"ORDERS"@world'


class TestBulkInsertSyncAsyncParity:
    """The two backends build bulk-INSERT text by hand and must agree.

    ``bulk_insert`` cannot go through an expression tree -- it binds one
    statement per row -- so the qualified name comes from the dialect's shared
    schema-object renderer instead of being concatenated inside each backend.
    That is what makes the two sides agree: they call the *same* function, so
    they cannot drift apart the way a copy of the logic per backend would.

    The invariant asserted here is structural (one implementation, shared) and
    is backed by the rendering assertions above (which fix what that one
    implementation produces).
    """

    def test_both_backends_render_the_name_in_one_place(self):
        from rhosocial.activerecord.backend.impl.oracle.backend.async_backend import (
            AsyncOracleBackend,
        )
        from rhosocial.activerecord.backend.impl.oracle.backend.backend import (
            OracleBackend,
        )
        from rhosocial.activerecord.backend.impl.oracle.mixins.backend_mixin import (
            OracleBackendMixin,
        )

        assert OracleBackend._qualified_name is OracleBackendMixin._qualified_name
        assert AsyncOracleBackend._qualified_name is OracleBackendMixin._qualified_name
        assert OracleBackend._identifier_list is OracleBackendMixin._identifier_list
        assert AsyncOracleBackend._identifier_list is OracleBackendMixin._identifier_list

    def test_bulk_insert_statement_is_not_hand_built(self):
        """The INSERT text may not concatenate the name itself.

        ``_current_returning_table`` still spells out ``SCHEMA.TABLE``, but it
        is a *dictionary lookup key* -- it is split back apart by
        ``_returning_lookup_key`` and never reaches the database -- and both
        backends build it identically, so it is deliberately out of scope here.
        """
        import inspect

        from rhosocial.activerecord.backend.impl.oracle.backend.async_backend import (
            AsyncOracleBackend,
        )
        from rhosocial.activerecord.backend.impl.oracle.backend.backend import (
            OracleBackend,
        )

        for backend_class in (OracleBackend, AsyncOracleBackend):
            source = inspect.getsource(backend_class.bulk_insert)
            assert "_qualified_name(" in source
            assert "_identifier_list(" in source
            assert "{options.schema_name}.{options.table}" not in source
