# tests/providers/query.py
"""
Concrete implementations of `IQuerySyncProvider` and `IQueryAsyncProvider`
for the Oracle backend.
"""

import os
import sys
import logging
from typing import Type, List, Tuple

from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord  # noqa: E402

logger = logging.getLogger(__name__)

from rhosocial.activerecord.testsuite.utils import select_fixture  # noqa: E402

from rhosocial.activerecord.testsuite.feature.query.fixtures.models import (  # noqa: E402
    User as UserBase,
    JsonUser as JsonUserBase,
    Order as OrderBase,
    OrderItem as OrderItemBase,
    Post as PostBase,
    Comment as CommentBase,
    MappedUser as MappedUserBase,
    MappedPost as MappedPostBase,
    MappedComment as MappedCommentBase,
    AsyncMappedUser as AsyncMappedUserBase,
    AsyncMappedPost as AsyncMappedPostBase,
    AsyncMappedComment as AsyncMappedCommentBase,
)
from rhosocial.activerecord.testsuite.feature.query.fixtures.cte_models import Node  # noqa: E402
from rhosocial.activerecord.testsuite.feature.query.fixtures.extended_models import (  # noqa: E402
    ExtendedOrder,
    ExtendedOrderItem,
)
from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import (  # noqa: E402
    OrderItem as CompositeOrderItemBase,
    AsyncOrderItem as AsyncCompositeOrderItemBase,
)

User310 = JsonUser310 = Order310 = OrderItem310 = Post310 = Comment310 = None
MappedUser310 = MappedPost310 = MappedComment310 = None

if sys.version_info >= (3, 10):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py310 import (
            User as User310,
            JsonUser as JsonUser310,
            Order as Order310,
            OrderItem as OrderItem310,
            Post as Post310,
            Comment as Comment310,
            MappedUser as MappedUser310,
            MappedPost as MappedPost310,
            MappedComment as MappedComment310,
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.10+ fixtures: {e}")

User311 = JsonUser311 = Order311 = OrderItem311 = Post311 = Comment311 = None
MappedUser311 = MappedPost311 = MappedComment311 = None

if sys.version_info >= (3, 11):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py311 import (
            User as User311,
            JsonUser as JsonUser311,
            Order as Order311,
            OrderItem as OrderItem311,
            Post as Post311,
            Comment as Comment311,
            MappedUser as MappedUser311,
            MappedPost as MappedPost311,
            MappedComment as MappedComment311,
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.11+ fixtures: {e}")

User312 = JsonUser312 = Order312 = OrderItem312 = Post312 = Comment312 = None
MappedUser312 = MappedPost312 = MappedComment312 = None

if sys.version_info >= (3, 12):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py312 import (
            User as User312,
            JsonUser as JsonUser312,
            Order as Order312,
            OrderItem as OrderItem312,
            Post as Post312,
            Comment as Comment312,
            MappedUser as MappedUser312,
            MappedPost as MappedPost312,
            MappedComment as MappedComment312,
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.12+ fixtures: {e}")


def _select_model_class(base_cls, py312_cls, py311_cls, py310_cls, model_name: str) -> Type:
    """Select the most appropriate model class for the current Python version."""
    candidates = [c for c in [py312_cls, py311_cls, py310_cls, base_cls] if c is not None]
    selected = select_fixture(*candidates)
    logger.info(f"Selected {model_name}: {selected.__name__} from {selected.__module__}")
    return selected


User = _select_model_class(UserBase, User312, User311, User310, "User")
JsonUser = _select_model_class(JsonUserBase, JsonUser312, JsonUser311, JsonUser310, "JsonUser")
Order = _select_model_class(OrderBase, Order312, Order311, Order310, "Order")
OrderItem = _select_model_class(OrderItemBase, OrderItem312, OrderItem311, OrderItem310, "OrderItem")
Post = _select_model_class(PostBase, Post312, Post311, Post310, "Post")
Comment = _select_model_class(CommentBase, Comment312, Comment311, Comment310, "Comment")
MappedUser = _select_model_class(MappedUserBase, MappedUser312, MappedUser311, MappedUser310, "MappedUser")
MappedPost = _select_model_class(MappedPostBase, MappedPost312, MappedPost311, MappedPost310, "MappedPost")
MappedComment = _select_model_class(
    MappedCommentBase, MappedComment312, MappedComment311, MappedComment310, "MappedComment"
)

from rhosocial.activerecord.testsuite.feature.query.interfaces import (  # noqa: E402
    IQuerySyncProvider,
    IQueryAsyncProvider,
)
from rhosocial.activerecord.testsuite.core.protocols import WorkerTestProtocol  # noqa: E402

from .scenarios import get_enabled_scenarios, get_scenario  # noqa: E402


class QueryProviderBase:
    def __init__(self):
        self._scenario_db_files = {}

    @staticmethod
    def _ddl_options():
        from rhosocial.activerecord.backend.options import ExecutionOptions
        from rhosocial.activerecord.backend.schema import StatementType
        return ExecutionOptions(stmt_type=StatementType.DDL)

    def get_test_scenarios(self) -> List[str]:
        return list(get_enabled_scenarios().keys())

    def _load_oracle_schema(self, filename: str) -> str:
        schema_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "rhosocial",
            "activerecord_oracle_test",
            "feature",
            "query",
            "schema",
        )
        schema_path = os.path.join(schema_dir, filename)
        with open(schema_path, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def _table_cleanup_order() -> List[str]:
        return [
            "extended_order_items",
            "order_items",
            "comments",
            "posts",
            "extended_orders",
            "orders",
            "nodes",
            "json_users",
            "searchable_items",
            "users",
        ]


class QuerySyncProvider(QueryProviderBase, IQuerySyncProvider, WorkerTestProtocol):
    def __init__(self):
        super().__init__()
        self._active_backends: List = []

    # -- DDL helpers ----------------------------------------------------

    def _drop_table_sync(self, backend_instance, table_name: str) -> None:
        try:
            backend_instance.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=self._ddl_options(),
            )
        except Exception:
            pass

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        backend_class, config = get_scenario(scenario_name)
        model_class.configure(config, backend_class)
        backend_instance = model_class.__backend__
        if backend_instance not in self._active_backends:
            self._active_backends.append(backend_instance)
        self._drop_table_sync(backend_instance, table_name)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        backend_instance.execute(schema_sql, options=self._ddl_options())
        return model_class

    def _reset_schema(self, backend_instance, table_name: str) -> None:
        self._drop_table_sync(backend_instance, table_name)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        backend_instance.execute(schema_sql, options=self._ddl_options())

    def _setup_multiple_models(
        self, model_classes: List[Tuple[Type[ActiveRecord], str]], scenario_name: str
    ) -> Tuple[Type[ActiveRecord], ...]:
        result = []
        shared_backend = None
        for i, (model_class, table_name) in enumerate(model_classes):
            if i == 0:
                configured_model = self._setup_model(model_class, scenario_name, table_name)
                shared_backend = configured_model.__backend__
            else:
                backend_class, config = get_scenario(scenario_name)
                model_class.__connection_config__ = config
                model_class.__backend_class__ = backend_class
                model_class.__backend__ = shared_backend
                if shared_backend not in self._active_backends:
                    self._active_backends.append(shared_backend)
                self._reset_schema(shared_backend, table_name)
                configured_model = model_class
            result.append(configured_model)
        return tuple(result)

    # -- IQuerySyncProvider interface ----------------------------------

    def setup_order_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models(
            [(User, "users"), (Order, "orders"), (OrderItem, "order_items")], scenario_name
        )

    def setup_blog_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models([(User, "users"), (Post, "posts"), (Comment, "comments")], scenario_name)

    def setup_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        return (self._setup_model(JsonUser, scenario_name, "json_users"),)

    def setup_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        return (self._setup_model(Node, scenario_name, "nodes"),)

    def setup_extended_order_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models(
            [(User, "users"), (ExtendedOrder, "extended_orders"), (ExtendedOrderItem, "extended_order_items")],
            scenario_name,
        )

    def setup_combined_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models(
            [(User, "users"), (Order, "orders"), (OrderItem, "order_items"), (Post, "posts"), (Comment, "comments")],
            scenario_name,
        )

    def setup_annotated_query_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.annotated_adapter_models import SearchableItem

        return (self._setup_model(SearchableItem, scenario_name, "searchable_items"),)

    def setup_mapped_models(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models(
            [(MappedUser, "users"), (MappedPost, "posts"), (MappedComment, "comments")], scenario_name
        )

    def setup_profile_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord]]:
        Profile = User.get_relation('profile').get_related_model(User)
        return self._setup_multiple_models(
            [(User, "users"), (Profile, "profiles")], scenario_name
        )

    def setup_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        backend_class, config = get_scenario(scenario_name)
        CompositeOrderItemBase.configure(config, backend_class)
        backend_instance = CompositeOrderItemBase.__backend__
        if backend_instance not in self._active_backends:
            self._active_backends.append(backend_instance)
        self._drop_table_sync(backend_instance, "order_items")
        schema_sql = self._load_oracle_schema("order_items.sql")
        backend_instance.execute(schema_sql, options=self._ddl_options())
        return CompositeOrderItemBase

    # -- Cleanup ---------------------------------------------------------

    def cleanup_after_test(self, scenario_name: str):
        for backend_instance in self._active_backends:
            try:
                for table_name in self._table_cleanup_order():
                    try:
                        self._drop_table_sync(backend_instance, table_name)
                    except Exception:
                        pass
            finally:
                try:
                    backend_instance.disconnect()
                except Exception:
                    pass
        self._active_backends.clear()

    def get_worker_connection_params(self, scenario_name: str, fixture_type: str = "order") -> dict:
        from .scenarios import SCENARIO_MAP

        is_async = fixture_type and fixture_type.startswith("async_")
        backend_class_name = "AsyncOracleBackend" if is_async else "OracleBackend"
        base_fixture_type = fixture_type.replace("async_", "") if fixture_type else "order"
        schema_sql = self._get_schema_sql_for_fixture_type(base_fixture_type)
        if scenario_name not in SCENARIO_MAP:
            if SCENARIO_MAP:
                scenario_name = next(iter(SCENARIO_MAP))
            else:
                raise ValueError("No scenarios registered")
        config_dict = SCENARIO_MAP[scenario_name]
        return {
            "backend_module": "rhosocial.activerecord.backend.impl.oracle",
            "backend_class_name": backend_class_name,
            "config_class_module": "rhosocial.activerecord.backend.impl.oracle.config",
            "config_class_name": "OracleConnectionConfig",
            "config_kwargs": config_dict,
            "schema_sql": schema_sql,
        }

    def get_worker_schema_sql(self, scenario_name: str, table_name: str) -> str:
        return self._load_oracle_schema(f"{table_name}.sql")

    def _get_schema_sql_for_fixture_type(self, fixture_type: str) -> dict:
        table_map = {
            "order": ["users", "orders", "order_items"],
            "blog": ["users", "posts", "comments"],
            "user": ["users"],
            "combined": ["users", "orders", "order_items", "posts", "comments"],
        }
        return {table: self._load_oracle_schema(f"{table}.sql") for table in table_map.get(fixture_type, ["users"])}


class QueryAsyncProvider(QueryProviderBase, IQueryAsyncProvider):
    def __init__(self):
        super().__init__()
        self._active_async_backends: List = []

    # -- DDL helpers ----------------------------------------------------

    async def _drop_table_async(self, backend_instance, table_name: str) -> None:
        try:
            await backend_instance.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=self._ddl_options(),
            )
        except Exception:
            pass

    async def _setup_model_async(
        self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str, shared_backend=None
    ) -> Type[ActiveRecord]:
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

        _, config = get_scenario(scenario_name)
        if shared_backend is None:
            await model_class.configure(config, AsyncOracleBackend)
        else:
            model_class.__connection_config__ = config
            model_class.__backend_class__ = AsyncOracleBackend
            model_class.__backend__ = shared_backend
        backend_instance = model_class.__backend__
        if backend_instance not in self._active_async_backends:
            self._active_async_backends.append(backend_instance)
        await self._drop_table_async(backend_instance, table_name)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        await backend_instance.execute(schema_sql, options=self._ddl_options())
        return model_class

    async def _setup_multiple_models_async(
        self, model_classes: List[Tuple[Type[ActiveRecord], str]], scenario_name: str
    ) -> Tuple[Type[ActiveRecord], ...]:
        result = []
        shared_backend = None
        for i, (model_class, table_name) in enumerate(model_classes):
            if i == 0:
                configured_model = await self._setup_model_async(model_class, scenario_name, table_name)
                shared_backend = configured_model.__backend__
            else:
                _, config = get_scenario(scenario_name)
                model_class.__connection_config__ = config
                from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend
                model_class.__backend_class__ = AsyncOracleBackend
                model_class.__backend__ = shared_backend
                configured_model = await self._setup_model_async(model_class, scenario_name, table_name, shared_backend)
            result.append(configured_model)
        return tuple(result)

    # -- IQueryAsyncProvider interface ----------------------------------

    async def setup_order_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import (
            AsyncUser,
            AsyncOrder,
            AsyncOrderItem,
        )
        return await self._setup_multiple_models_async(
            [(AsyncUser, "users"), (AsyncOrder, "orders"), (AsyncOrderItem, "order_items")], scenario_name
        )

    async def setup_blog_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_blog_models import AsyncPost, AsyncComment
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser
        return await self._setup_multiple_models_async(
            [(AsyncUser, "users"), (AsyncPost, "posts"), (AsyncComment, "comments")], scenario_name
        )

    async def setup_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_json_models import AsyncJsonUser
        return (await self._setup_model_async(AsyncJsonUser, scenario_name, "json_users"),)

    async def setup_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_cte_models import AsyncNode
        return (await self._setup_model_async(AsyncNode, scenario_name, "nodes"),)

    async def setup_extended_order_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_extended_models import (
            AsyncUser,
            AsyncExtendedOrder,
            AsyncExtendedOrderItem,
        )
        return await self._setup_multiple_models_async(
            [(AsyncUser, "users"), (AsyncExtendedOrder, "extended_orders"),
             (AsyncExtendedOrderItem, "extended_order_items")],
            scenario_name,
        )

    async def setup_combined_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import (
            AsyncUser,
            AsyncOrder,
            AsyncOrderItem,
        )
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_blog_models import AsyncPost, AsyncComment
        return await self._setup_multiple_models_async(
            [(AsyncUser, "users"), (AsyncOrder, "orders"), (AsyncOrderItem, "order_items"),
             (AsyncPost, "posts"), (AsyncComment, "comments")],
            scenario_name,
        )

    async def setup_annotated_query_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_annotated_adapter_models import (
            AsyncSearchableItem,
        )
        return (await self._setup_model_async(AsyncSearchableItem, scenario_name, "searchable_items"),)

    async def setup_mapped_models(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return await self._setup_multiple_models_async(
            [(AsyncMappedUserBase, "users"), (AsyncMappedPostBase, "posts"), (AsyncMappedCommentBase, "comments")], scenario_name
        )

    async def setup_profile_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[AsyncActiveRecord], Type[AsyncActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser, AsyncProfile
        return await self._setup_multiple_models_async(
            [(AsyncUser, "users"), (AsyncProfile, "profiles")], scenario_name
        )

    async def setup_order_item_model(self, scenario_name: str) -> Type[AsyncActiveRecord]:
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend
        _, config = get_scenario(scenario_name)
        await AsyncCompositeOrderItemBase.configure(config, AsyncOracleBackend)
        backend_instance = AsyncCompositeOrderItemBase.__backend__
        if backend_instance not in self._active_async_backends:
            self._active_async_backends.append(backend_instance)
        await self._drop_table_async(backend_instance, "order_items")
        schema_sql = self._load_oracle_schema("order_items.sql")
        await backend_instance.execute(schema_sql, options=self._ddl_options())
        return AsyncCompositeOrderItemBase

    # -- Cleanup ---------------------------------------------------------

    async def cleanup_after_test(self, scenario_name: str) -> None:
        for backend_instance in self._active_async_backends:
            try:
                for table_name in self._table_cleanup_order():
                    try:
                        await self._drop_table_async(backend_instance, table_name)
                    except Exception:
                        pass
            finally:
                try:
                    await backend_instance.disconnect()
                except Exception:
                    pass
        self._active_async_backends.clear()
