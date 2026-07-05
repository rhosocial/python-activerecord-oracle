# tests/providers/composite_pk.py
import os
from typing import Type, List

from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
from rhosocial.activerecord.model import ActiveRecord

from rhosocial.activerecord.testsuite.feature.composite_pk.interfaces import ICompositePKProvider
from rhosocial.activerecord.testsuite.feature.composite_pk.fixtures.models import (
    OrderItem as OrderItemBase,
    StoreInventory as StoreInventoryBase,
    Order as OrderBase,
    MappedOrderItem as MappedOrderItemBase,
)
from rhosocial.activerecord.testsuite.feature.composite_pk.fixtures.models import (
    AsyncOrderItem as AsyncOrderItemBase,
    AsyncStoreInventory as AsyncStoreInventoryBase,
    AsyncOrder as AsyncOrderBase,
    AsyncMappedOrderItem as AsyncMappedOrderItemBase,
)

from .scenarios import get_enabled_scenarios, get_scenario


class CompositePKProvider(ICompositePKProvider):
    """
    Oracle backend implementation of the `ICompositePKProvider` interface.

    This provider wires the generic composite-PK test suite fixtures to a real
    Oracle database. For each test scenario, it configures the ActiveRecord
    model with the Oracle backend, drops any pre-existing tables using a PL/SQL
    anonymous block (Oracle lacks `DROP TABLE IF EXISTS ... CASCADE`), and then
    loads the Oracle-specific schema DDL from the per-feature schema directory.
    """

    def __init__(self):
        self._active_backends: List = []
        self._active_async_backends: List = []

    def get_test_scenarios(self) -> List[str]:
        """Returns the list of enabled scenario names for the Oracle backend."""
        return list(get_enabled_scenarios().keys())

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        backend_class, config = get_scenario(scenario_name)
        model_class.configure(config, backend_class)
        backend_instance = model_class.__backend__
        if backend_instance not in self._active_backends:
            self._active_backends.append(backend_instance)
        self._drop_table_sync(backend_instance, table_name)
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        backend_instance.execute(schema_sql, options=ddl_options)
        return model_class

    async def _setup_async_model(
        self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str
    ) -> Type[ActiveRecord]:
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend
        _, config = get_scenario(scenario_name)
        await model_class.configure(config, AsyncOracleBackend)
        backend_instance = model_class.__backend__
        if backend_instance not in self._active_async_backends:
            self._active_async_backends.append(backend_instance)
        await self._drop_table_async(backend_instance, table_name)
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        await backend_instance.execute(schema_sql, options=ddl_options)
        return model_class

    def _drop_table_sync(self, backend_instance, table_name: str) -> None:
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            # Oracle has no `DROP TABLE IF EXISTS ... CASCADE` syntax. Use a
            # PL/SQL anonymous block that swallows the ORA-00942 "table does not
            # exist" error (and any other error) via EXCEPTION WHEN OTHERS.
            backend_instance.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=ddl_options,
            )
        except Exception:
            pass

    async def _drop_table_async(self, backend_instance, table_name: str) -> None:
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            await backend_instance.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=ddl_options,
            )
        except Exception:
            pass

    def _load_oracle_schema(self, filename: str) -> str:
        """
        Load an Oracle-specific schema DDL file for the composite_pk feature.

        Schema files are expected to live at:
            tests/rhosocial/activerecord_oracle_test/feature/composite_pk/schema/
        """
        schema_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "rhosocial",
            "activerecord_oracle_test",
            "feature",
            "composite_pk",
            "schema",
        )
        schema_path = os.path.join(schema_dir, filename)
        with open(schema_path, "r", encoding="utf-8") as f:
            return f.read()

    def setup_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(OrderItemBase, scenario_name, "order_items")

    def setup_store_inventory_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(StoreInventoryBase, scenario_name, "store_inventory")

    def setup_order_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(OrderBase, scenario_name, "orders")

    def setup_mapped_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(MappedOrderItemBase, scenario_name, "order_items")

    async def setup_async_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncOrderItemBase, scenario_name, "order_items")

    async def setup_async_store_inventory_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncStoreInventoryBase, scenario_name, "store_inventory")

    async def setup_async_order_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncOrderBase, scenario_name, "orders")

    async def setup_async_mapped_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncMappedOrderItemBase, scenario_name, "order_items")

    def cleanup_after_test(self, scenario_name: str):
        tables = ["order_items", "store_inventory", "orders"]
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        for backend in self._active_backends:
            try:
                for t in tables:
                    try:
                        backend.execute(
                            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t} CASCADE CONSTRAINTS PURGE'; "
                            f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                            options=ddl_options,
                        )
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                backend.disconnect()
            except Exception:
                pass
        self._active_backends.clear()

    async def cleanup_after_test_async(self, scenario_name: str):
        tables = ["order_items", "store_inventory", "orders"]
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        for backend in self._active_async_backends:
            try:
                for t in tables:
                    try:
                        await backend.execute(
                            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t} CASCADE CONSTRAINTS PURGE'; "
                            f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                            options=ddl_options,
                        )
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                await backend.disconnect()
            except Exception:
                pass
        self._active_async_backends.clear()
