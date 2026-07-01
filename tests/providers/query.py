"""Oracle backend Query provider implementation."""
import os
import sys
import logging
from typing import Type, List, Tuple

from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
from rhosocial.activerecord.testsuite.utils import select_fixture
from rhosocial.activerecord.testsuite.feature.query.interfaces import IQueryProvider
from rhosocial.activerecord.testsuite.core.protocols import WorkerTestProtocol
from rhosocial.activerecord.testsuite.feature.query.fixtures.models import (
    User as UserBase, JsonUser as JsonUserBase,
    Order as OrderBase, OrderItem as OrderItemBase,
    Post as PostBase, Comment as CommentBase,
    MappedUser as MappedUserBase, MappedPost as MappedPostBase, MappedComment as MappedCommentBase,
)
from rhosocial.activerecord.testsuite.feature.query.fixtures.cte_models import Node
from rhosocial.activerecord.testsuite.feature.query.fixtures.extended_models import ExtendedOrder, ExtendedOrderItem

from .scenarios import get_enabled_scenarios, get_scenario

logger = logging.getLogger(__name__)

User310 = JsonUser310 = Order310 = OrderItem310 = Post310 = Comment310 = None
MappedUser310 = MappedPost310 = MappedComment310 = None
if sys.version_info >= (3, 10):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py310 import (
            User as User310, JsonUser as JsonUser310,
            Order as Order310, OrderItem as OrderItem310,
            Post as Post310, Comment as Comment310,
            MappedUser as MappedUser310, MappedPost as MappedPost310, MappedComment as MappedComment310,
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.10+ fixtures: {e}")

User311 = JsonUser311 = Order311 = OrderItem311 = Post311 = Comment311 = None
MappedUser311 = MappedPost311 = MappedComment311 = None
if sys.version_info >= (3, 11):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py311 import (
            User as User311, JsonUser as JsonUser311,
            Order as Order311, OrderItem as OrderItem311,
            Post as Post311, Comment as Comment311,
            MappedUser as MappedUser311, MappedPost as MappedPost311, MappedComment as MappedComment311,
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.11+ fixtures: {e}")

User312 = JsonUser312 = Order312 = OrderItem312 = Post312 = Comment312 = None
MappedUser312 = MappedPost312 = MappedComment312 = None
if sys.version_info >= (3, 12):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py312 import (
            User as User312, JsonUser as JsonUser312,
            Order as Order312, OrderItem as OrderItem312,
            Post as Post312, Comment as Comment312,
            MappedUser as MappedUser312, MappedPost as MappedPost312, MappedComment as MappedComment312,
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.12+ fixtures: {e}")


def _select_model_class(base_cls, py312_cls, py311_cls, py310_cls, model_name: str) -> Type:
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
MappedComment = _select_model_class(MappedCommentBase, MappedComment312, MappedComment311, MappedComment310, "MappedComment")


class QueryProvider(IQueryProvider, WorkerTestProtocol):
    """Oracle backend implementation for query tests."""

    def __init__(self):
        self._active_backends = []
        self._active_async_backends = []

    def get_test_scenarios(self) -> List[str]:
        return list(get_enabled_scenarios().keys())

    def _track_backend(self, backend_instance, active_backends: list) -> None:
        if backend_instance not in active_backends:
            active_backends.append(backend_instance)

    def _drop_table_sync(self, backend_instance, table_name: str) -> None:
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        backend_instance.execute(
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
            options=ddl_options,
        )

    async def _drop_table_async(self, backend_instance, table_name: str) -> None:
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        await backend_instance.execute(
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
            options=ddl_options,
        )

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        backend_class, config = get_scenario(scenario_name)
        model_class.configure(config, backend_class)
        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_backends)

        self._drop_table_sync(backend_instance, table_name)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        backend_instance.execute(schema_sql, options=ExecutionOptions(stmt_type=StatementType.DDL))
        return model_class

    async def _setup_model_async(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

        _, config = get_scenario(scenario_name)
        await model_class.configure(config, AsyncOracleBackend)
        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_async_backends)

        await self._drop_table_async(backend_instance, table_name)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        await backend_instance.execute(schema_sql, options=ExecutionOptions(stmt_type=StatementType.DDL))
        return model_class

    def _setup_multiple_models(self, model_classes: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        return tuple(self._setup_model(model_class, scenario_name, table_name) for model_class, table_name in model_classes)

    async def _setup_multiple_models_async(self, model_classes: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        result = []
        for model_class, table_name in model_classes:
            result.append(await self._setup_model_async(model_class, scenario_name, table_name))
        return tuple(result)

    def setup_query_test_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(User, scenario_name, "users")

    def setup_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models([(User, "users"), (Order, "orders"), (OrderItem, "order_items")], scenario_name)

    def setup_blog_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models([(User, "users"), (Post, "posts"), (Comment, "comments")], scenario_name)

    def setup_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        import pytest

        backend_class, config = get_scenario(scenario_name)
        JsonUser.configure(config, backend_class)
        backend_instance = JsonUser.__backend__
        self._track_backend(backend_instance, self._active_backends)
        if not backend_instance.dialect.supports_json_type():
            pytest.skip(f"JSON type not supported by Oracle version {backend_instance.get_server_version()}")
        return (self._setup_model(JsonUser, scenario_name, "json_users"),)

    def setup_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        return (self._setup_model(Node, scenario_name, "nodes"),)

    def setup_extended_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models([(User, "users"), (ExtendedOrder, "extended_orders"), (ExtendedOrderItem, "extended_order_items")], scenario_name)

    def setup_combined_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models([(User, "users"), (Order, "orders"), (OrderItem, "order_items"), (Post, "posts"), (Comment, "comments")], scenario_name)

    def setup_annotated_query_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.annotated_adapter_models import SearchableItem
        return self._setup_multiple_models([(SearchableItem, "searchable_items")], scenario_name)

    def setup_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models([(MappedUser, "users"), (MappedPost, "posts"), (MappedComment, "comments")], scenario_name)

    # --- Profile fixtures ---

    def setup_profile_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord]]:
        # Resolve Profile via the relationship descriptor so we get the same
        # class that batch loading will find (version-specific model files
        # define their own Profile classes; importing models.Profile would
        # target the wrong class on Python 3.10+).
        Profile = User.get_relation('profile').get_related_model(User)

        return self._setup_multiple_models([(User, "users"), (Profile, "profiles")], scenario_name)

    async def setup_async_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser, AsyncOrder, AsyncOrderItem
        return await self._setup_multiple_models_async([(AsyncUser, "users"), (AsyncOrder, "orders"), (AsyncOrderItem, "order_items")], scenario_name)

    async def setup_async_blog_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_blog_models import AsyncUser, AsyncPost, AsyncComment
        return await self._setup_multiple_models_async([(AsyncUser, "users"), (AsyncPost, "posts"), (AsyncComment, "comments")], scenario_name)

    async def setup_async_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        import pytest
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_json_models import AsyncJsonUser
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

        _, config = get_scenario(scenario_name)
        await AsyncJsonUser.configure(config, AsyncOracleBackend)
        backend_instance = AsyncJsonUser.__backend__
        self._track_backend(backend_instance, self._active_async_backends)
        if not backend_instance.dialect.supports_json_type():
            pytest.skip(f"JSON type not supported by Oracle version {await backend_instance.get_server_version()}")
        return (await self._setup_model_async(AsyncJsonUser, scenario_name, "json_users"),)

    async def setup_async_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_cte_models import AsyncNode
        return (await self._setup_model_async(AsyncNode, scenario_name, "nodes"),)

    async def setup_async_extended_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_extended_models import AsyncUser, AsyncExtendedOrder, AsyncExtendedOrderItem
        return await self._setup_multiple_models_async([(AsyncUser, "users"), (AsyncExtendedOrder, "extended_orders"), (AsyncExtendedOrderItem, "extended_order_items")], scenario_name)

    async def setup_async_combined_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser, AsyncOrder, AsyncOrderItem
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_blog_models import AsyncPost, AsyncComment
        return await self._setup_multiple_models_async([(AsyncUser, "users"), (AsyncOrder, "orders"), (AsyncOrderItem, "order_items"), (AsyncPost, "posts"), (AsyncComment, "comments")], scenario_name)

    async def setup_async_annotated_query_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_annotated_adapter_models import AsyncSearchableItem
        return await self._setup_multiple_models_async([(AsyncSearchableItem, "searchable_items")], scenario_name)

    async def setup_async_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_mapped_models import AsyncMappedUser, AsyncMappedPost, AsyncMappedComment
        return await self._setup_multiple_models_async([(AsyncMappedUser, "users"), (AsyncMappedPost, "posts"), (AsyncMappedComment, "comments")], scenario_name)

    async def setup_async_profile_fixtures(
        self, scenario_name: str
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import (
            AsyncUser,
            AsyncProfile,
        )

        return await self._setup_multiple_models_async(
            [(AsyncUser, "users"), (AsyncProfile, "profiles")], scenario_name
        )

    def _load_oracle_schema(self, filename: str) -> str:
        schema_dir = os.path.join(os.path.dirname(__file__), "..", "rhosocial", "activerecord_oracle_test", "feature", "query", "schema")
        with open(os.path.join(schema_dir, filename), "r", encoding="utf-8") as f:
            return f.read()

    def _cleanup_tables_sync(self, backend_instance) -> None:
        for table_name in self._table_cleanup_order():
            try:
                self._drop_table_sync(backend_instance, table_name)
            except Exception:
                pass

    async def _cleanup_tables_async(self, backend_instance) -> None:
        for table_name in self._table_cleanup_order():
            try:
                await self._drop_table_async(backend_instance, table_name)
            except Exception:
                pass

    def cleanup_after_test(self, scenario_name: str):
        from rhosocial.activerecord.backend.impl.oracle import OracleBackend

        for backend_instance in self._active_backends:
            if not isinstance(backend_instance, OracleBackend):
                continue
            try:
                self._cleanup_tables_sync(backend_instance)
            finally:
                try:
                    backend_instance.disconnect()
                except Exception:
                    pass
        self._active_backends.clear()

    async def cleanup_after_test_async(self, scenario_name: str) -> None:
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

        for backend_instance in self._active_async_backends:
            if not isinstance(backend_instance, AsyncOracleBackend):
                continue
            try:
                await self._cleanup_tables_async(backend_instance)
            finally:
                try:
                    await backend_instance.disconnect()
                except Exception:
                    pass
        self._active_async_backends.clear()

    def get_worker_connection_params(self, scenario_name: str, fixture_type: str = "order") -> dict:
        from .scenarios import SCENARIO_MAP

        is_async = fixture_type and fixture_type.startswith("async_")
        backend_class_name = "AsyncOracleBackend" if is_async else "OracleBackend"
        base_fixture_type = fixture_type.replace("async_", "") if fixture_type else "order"

        if scenario_name not in SCENARIO_MAP:
            if SCENARIO_MAP:
                scenario_name = next(iter(SCENARIO_MAP))
            else:
                raise ValueError("No scenarios registered")

        return {
            "backend_module": "rhosocial.activerecord.backend.impl.oracle",
            "backend_class_name": backend_class_name,
            "config_class_module": "rhosocial.activerecord.backend.impl.oracle.config",
            "config_class_name": "OracleConnectionConfig",
            "config_kwargs": SCENARIO_MAP[scenario_name],
            "schema_sql": self._get_schema_sql_for_fixture_type(base_fixture_type),
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

    @staticmethod
    def _table_cleanup_order() -> List[str]:
        return [
            "extended_order_items", "order_items", "comments", "posts", "extended_orders",
            "orders", "nodes", "json_users", "searchable_items", "users",
        ]
