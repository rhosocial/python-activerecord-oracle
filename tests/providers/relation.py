# tests/providers/relation.py
"""
Oracle backend implementation of the `IRelationProvider` interface defined in
the `rhosocial-activerecord-testsuite` package.

This provider wires the backend-agnostic relation feature tests to the Oracle
database. It reuses the shared fixture model classes shipped by the testsuite
(Employee, Department, Author, Book, Chapter, Profile, User, Post, Comment,
BoundaryOwner, BoundaryProfile, BoundaryPost, plus their async counterparts)
and is responsible for:

1. Reporting the enabled Oracle test scenarios.
2. Configuring the ActiveRecord model classes with an Oracle backend, sharing
   a single backend instance across related models where appropriate.
3. Resetting the underlying schema (dropping and recreating the Oracle tables)
   via PL/SQL anonymous blocks plus schema SQL files shipped with this project.
4. Cleaning up resources after each test.
"""
import asyncio
import os
import logging
from typing import Dict, List, Tuple, Type

from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord
from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
from rhosocial.activerecord.testsuite.feature.relation.interfaces import IRelationProvider

# Base (Python 3.8+) relation fixture models
from rhosocial.activerecord.testsuite.feature.relation.fixtures.models import (
    Employee, Department, Author, Book, Chapter, Profile,
    User, Post, Comment,
    AsyncUser, AsyncPost, AsyncComment,
    BoundaryOwner, BoundaryProfile, BoundaryPost,
    AsyncBoundaryOwner, AsyncBoundaryProfile, AsyncBoundaryPost,
)

from .scenarios import get_enabled_scenarios, get_scenario

logger = logging.getLogger(__name__)


class RelationProvider(IRelationProvider):
    """
    Oracle backend provider for the relation feature tests.

    The provider shares a single Oracle backend across the related model
    classes that participate in a given test group (e.g. User/Post/Comment)
    so that cross-table relations resolve against the same connection. Schema
    reset is performed per table using PL/SQL anonymous blocks that drop the
    table if it exists before the corresponding ``.sql`` schema file is loaded
    and executed.
    """

    def __init__(self):
        self._active_backends: List = []
        self._active_async_backends: List = []
        self._sync_user_post_comment_setup = False
        self._async_user_post_comment_setup = False
        self._sync_relation_boundary_setup = False
        self._async_relation_boundary_setup = False

    # ------------------------------------------------------------------
    # Scenario helpers
    # ------------------------------------------------------------------

    def get_test_scenarios(self) -> List[str]:
        return list(get_enabled_scenarios().keys())

    def _track_backend(self, backend_instance, collection: List) -> None:
        if backend_instance not in collection:
            collection.append(backend_instance)

    # ------------------------------------------------------------------
    # Oracle DDL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ddl_options() -> ExecutionOptions:
        return ExecutionOptions(stmt_type=StatementType.DDL)

    def _drop_table_sync(self, backend_instance, table_name: str) -> None:
        """Drop a table using a PL/SQL anonymous block, ignoring errors."""
        try:
            backend_instance.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=self._ddl_options(),
            )
        except Exception:
            pass

    async def _drop_table_async(self, backend_instance, table_name: str) -> None:
        """Drop a table using a PL/SQL anonymous block, ignoring errors."""
        try:
            await backend_instance.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; "
                f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=self._ddl_options(),
            )
        except Exception:
            pass

    def _load_oracle_schema(self, filename: str) -> str:
        """
        Load a SQL schema file from this project's relation fixture schema
        directory: ``tests/rhosocial/activerecord_oracle_test/feature/relation/schema/``.

        The directory may not exist yet; callers should ensure the referenced
        ``.sql`` files are present before exercising the provider.
        """
        schema_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "rhosocial",
            "activerecord_oracle_test",
            "feature",
            "relation",
            "schema",
        )
        schema_path = os.path.join(schema_dir, filename)
        with open(schema_path, "r", encoding="utf-8") as f:
            return f.read()

    def _reset_table_sync(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        """Drop and recreate the table for the given model (sync)."""
        backend = model_class.__backend__
        self._drop_table_sync(backend, table_name)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        backend.execute(schema_sql, options=self._ddl_options())

    async def _reset_table_async(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        """Drop and recreate the table for the given model (async)."""
        backend = model_class.__backend__
        await self._drop_table_async(backend, table_name)
        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        await backend.execute(schema_sql, options=self._ddl_options())

    def _initialize_model_schema(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        """Initialize schema for a model that shares a backend with another model."""
        self._reset_table_sync(model_class, table_name)

    async def _initialize_async_model_schema(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        """Initialize schema for an async model that shares a backend with another model."""
        await self._reset_table_async(model_class, table_name)

    # ------------------------------------------------------------------
    # Multiple-model setup helpers (shared backend)
    # ------------------------------------------------------------------

    def _setup_multiple_models(
        self,
        model_classes: List[Tuple[Type[ActiveRecord], str]],
        scenario_name: str,
    ) -> Tuple[Type[ActiveRecord], ...]:
        """
        Set up multiple related models for a test, sharing a single backend
        across all of them. The first model is configured normally; subsequent
        models are bound to the same backend before their schema is reset.
        """
        if not model_classes:
            return tuple()

        first_model_class, first_table_name = model_classes[0]
        backend_class, config = get_scenario(scenario_name)
        first_model_class.configure(config, backend_class)
        first_backend = first_model_class.__backend__
        self._track_backend(first_backend, self._active_backends)
        self._reset_table_sync(first_model_class, first_table_name)

        result: List[Type[ActiveRecord]] = [first_model_class]

        for model_class, table_name in model_classes[1:]:
            model_class.__connection_config__ = first_model_class.__connection_config__
            model_class.__backend_class__ = first_model_class.__backend_class__
            model_class.__backend__ = first_backend
            self._track_backend(first_backend, self._active_backends)
            self._initialize_model_schema(model_class, table_name)
            result.append(model_class)

        return tuple(result)

    async def _setup_multiple_async_models(
        self,
        model_classes: List[Tuple[Type[ActiveRecord], str]],
        scenario_name: str,
    ) -> Tuple[Type[ActiveRecord], ...]:
        """
        Set up multiple related async models for a test, sharing a single
        async backend across all of them.
        """
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

        if not model_classes:
            return tuple()

        first_model_class, first_table_name = model_classes[0]
        _, config = get_scenario(scenario_name)
        await first_model_class.configure(config, AsyncOracleBackend)
        first_backend = first_model_class.__backend__
        self._track_backend(first_backend, self._active_async_backends)
        await self._reset_table_async(first_model_class, first_table_name)

        result: List[Type[ActiveRecord]] = [first_model_class]

        for model_class, table_name in model_classes[1:]:
            model_class.__connection_config__ = first_model_class.__connection_config__
            model_class.__backend_class__ = first_model_class.__backend_class__
            model_class.__backend__ = first_backend
            self._track_backend(first_backend, self._active_async_backends)
            await self._initialize_async_model_schema(model_class, table_name)
            result.append(model_class)

        return tuple(result)

    # ------------------------------------------------------------------
    # Employee / Department
    # ------------------------------------------------------------------

    def setup_employee_department_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models(
            [
                (Employee, "employees"),
                (Department, "departments"),
            ],
            scenario_name,
        )

    # ------------------------------------------------------------------
    # Author / Book / Chapter / Profile
    # ------------------------------------------------------------------

    def setup_author_book_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[
        Type[ActiveRecord],
        Type[ActiveRecord],
        Type[ActiveRecord],
        Type[ActiveRecord],
    ]:
        return self._setup_multiple_models(
            [
                (Author, "authors"),
                (Book, "books"),
                (Chapter, "chapters"),
                (Profile, "profiles"),
            ],
            scenario_name,
        )

    # ------------------------------------------------------------------
    # User / Post / Comment (sync)
    # ------------------------------------------------------------------

    def _setup_user_post_comment_sync(self, scenario_name: str) -> None:
        if not self._sync_user_post_comment_setup:
            self._configure_json_field_adapters_sync()
            self._setup_multiple_models(
                [
                    (User, "users"),
                    (Post, "posts"),
                    (Comment, "comments"),
                ],
                scenario_name,
            )
            self._sync_user_post_comment_setup = True

    def _configure_json_field_adapters_sync(self) -> None:
        """Hook for registering JSON field adapters on sync models, if needed."""
        # Oracle stores JSON as CLOB; the shared models declare the columns as
        # plain strings, so no field adapter is required by default. Subclasses
        # or future versions may override this to wire Oracle-specific adapters.
        return

    def setup_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        self._setup_user_post_comment_sync(scenario_name)
        return User

    def setup_post_model(self, scenario_name: str) -> Type[ActiveRecord]:
        self._setup_user_post_comment_sync(scenario_name)
        return Post

    def setup_comment_model(self, scenario_name: str) -> Type[ActiveRecord]:
        self._setup_user_post_comment_sync(scenario_name)
        return Comment

    # ------------------------------------------------------------------
    # User / Post / Comment (async)
    # ------------------------------------------------------------------

    def _setup_user_post_comment_async(self, scenario_name: str) -> None:
        if not self._async_user_post_comment_setup:
            self._configure_json_field_adapters_async()
            asyncio.run(self._setup_multiple_async_models(
                [
                    (AsyncUser, "users"),
                    (AsyncPost, "posts"),
                    (AsyncComment, "comments"),
                ],
                scenario_name,
            ))
            self._async_user_post_comment_setup = True

    def _configure_json_field_adapters_async(self) -> None:
        """Hook for registering JSON field adapters on async models, if needed."""
        return

    async def _ensure_user_post_comment_async_schema(self) -> None:
        backend = AsyncUser.__backend__
        self._track_backend(backend, self._active_async_backends)
        await self._reset_table_async(AsyncUser, "users")
        await self._reset_table_async(AsyncPost, "posts")
        await self._reset_table_async(AsyncComment, "comments")

    def setup_async_user_model(self, scenario_name: str) -> Type[AsyncActiveRecord]:
        self._setup_user_post_comment_async(scenario_name)
        return AsyncUser

    def setup_async_post_model(self, scenario_name: str) -> Type[AsyncActiveRecord]:
        self._setup_user_post_comment_async(scenario_name)
        return AsyncPost

    def setup_async_comment_model(self, scenario_name: str) -> Type[AsyncActiveRecord]:
        self._setup_user_post_comment_async(scenario_name)
        return AsyncComment

    # ------------------------------------------------------------------
    # Relation boundary (sync)
    # ------------------------------------------------------------------

    def _setup_relation_boundary_sync(self, scenario_name: str) -> None:
        if not self._sync_relation_boundary_setup:
            self._setup_multiple_models(
                [
                    (BoundaryOwner, "relation_boundary_owners"),
                    (BoundaryProfile, "relation_boundary_profiles"),
                    (BoundaryPost, "relation_boundary_posts"),
                ],
                scenario_name,
            )
            self._sync_relation_boundary_setup = True

    def setup_relation_boundary_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        self._setup_relation_boundary_sync(scenario_name)
        return BoundaryOwner, BoundaryProfile, BoundaryPost

    # ------------------------------------------------------------------
    # Relation boundary (async)
    # ------------------------------------------------------------------

    async def _setup_relation_boundary_async(self, scenario_name: str) -> None:
        if not self._async_relation_boundary_setup:
            await self._setup_multiple_async_models(
                [
                    (AsyncBoundaryOwner, "relation_boundary_owners"),
                    (AsyncBoundaryProfile, "relation_boundary_profiles"),
                    (AsyncBoundaryPost, "relation_boundary_posts"),
                ],
                scenario_name,
            )
            self._async_relation_boundary_setup = True

    def setup_async_relation_boundary_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[
        Type[AsyncActiveRecord],
        Type[AsyncActiveRecord],
        Type[AsyncActiveRecord],
    ]:
        # Configure and reset schema synchronously using an async event loop,
        # then return the configured model classes. The testsuite calls this
        # method synchronously and exercises the async models separately.
        asyncio.run(self._setup_relation_boundary_async(scenario_name))
        return AsyncBoundaryOwner, AsyncBoundaryProfile, AsyncBoundaryPost

    # ------------------------------------------------------------------
    # Relation boundary dataset loading (sync)
    # ------------------------------------------------------------------

    def load_relation_boundary_dataset(
        self,
        scenario_name: str,
        dataset_name: str,
    ) -> Dict[str, int]:
        self._setup_relation_boundary_sync(scenario_name)
        return self._load_relation_boundary_dataset(dataset_name)

    def _load_relation_boundary_dataset(self, dataset_name: str) -> Dict[str, int]:
        if dataset_name == "null_foreign_key":
            profile = BoundaryProfile(bio="No owner", owner_id=None)
            profile.save()
            return {"profile_id": profile.id}

        if dataset_name == "orphan_foreign_key":
            missing_owner_id = 999999
            post = BoundaryPost(title="Orphan post", owner_id=missing_owner_id)
            post.save()
            return {"post_id": post.id, "missing_owner_id": missing_owner_id}

        if dataset_name == "owner_without_children":
            owner = BoundaryOwner(name="Owner without children")
            owner.save()
            return {"owner_id": owner.id}

        if dataset_name == "multiple_has_one_matches":
            owner = BoundaryOwner(name="Owner with duplicate profiles")
            owner.save()
            first = BoundaryProfile(bio="First profile", owner_id=owner.id)
            first.save()
            second = BoundaryProfile(bio="Second profile", owner_id=owner.id)
            second.save()
            return {
                "owner_id": owner.id,
                "first_profile_id": first.id,
                "second_profile_id": second.id,
            }

        raise ValueError(f"Unknown relation boundary dataset: {dataset_name}")

    # ------------------------------------------------------------------
    # Relation boundary dataset loading (async)
    # ------------------------------------------------------------------

    async def load_async_relation_boundary_dataset(
        self,
        scenario_name: str,
        dataset_name: str,
    ) -> Dict[str, int]:
        await self._setup_relation_boundary_async(scenario_name)
        return await self._load_async_relation_boundary_dataset(dataset_name)

    async def _load_async_relation_boundary_dataset(self, dataset_name: str) -> Dict[str, int]:
        if dataset_name == "null_foreign_key":
            profile = AsyncBoundaryProfile(bio="No owner", owner_id=None)
            await profile.save()
            return {"profile_id": profile.id}

        if dataset_name == "orphan_foreign_key":
            missing_owner_id = 999999
            post = AsyncBoundaryPost(title="Orphan post", owner_id=missing_owner_id)
            await post.save()
            return {"post_id": post.id, "missing_owner_id": missing_owner_id}

        if dataset_name == "owner_without_children":
            owner = AsyncBoundaryOwner(name="Owner without children")
            await owner.save()
            return {"owner_id": owner.id}

        if dataset_name == "multiple_has_one_matches":
            owner = AsyncBoundaryOwner(name="Owner with duplicate profiles")
            await owner.save()
            first = AsyncBoundaryProfile(bio="First profile", owner_id=owner.id)
            await first.save()
            second = AsyncBoundaryProfile(bio="Second profile", owner_id=owner.id)
            await second.save()
            return {
                "owner_id": owner.id,
                "first_profile_id": first.id,
                "second_profile_id": second.id,
            }

        raise ValueError(f"Unknown relation boundary dataset: {dataset_name}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    @staticmethod
    def _relation_table_cleanup_order() -> List[str]:
        # Drop dependents first to avoid constraint violations.
        return [
            "comments",
            "posts",
            "users",
            "chapters",
            "books",
            "profiles",
            "authors",
            "departments",
            "employees",
            "relation_boundary_posts",
            "relation_boundary_profiles",
            "relation_boundary_owners",
        ]

    def _reset_setup_state(self) -> None:
        self._sync_user_post_comment_setup = False
        self._async_user_post_comment_setup = False
        self._sync_relation_boundary_setup = False
        self._async_relation_boundary_setup = False

    def cleanup_after_test(self, scenario_name: str) -> None:
        from rhosocial.activerecord.backend.impl.oracle import OracleBackend

        for backend_instance in self._active_backends:
            if not isinstance(backend_instance, OracleBackend):
                continue
            try:
                for table_name in self._relation_table_cleanup_order():
                    self._drop_table_sync(backend_instance, table_name)
            finally:
                try:
                    backend_instance.disconnect()
                except Exception:
                    pass
        self._active_backends.clear()

        for backend_instance in self._active_async_backends:
            try:
                for table_name in self._relation_table_cleanup_order():
                    try:
                        asyncio.run(self._drop_table_async(backend_instance, table_name))
                    except Exception:
                        pass
            finally:
                try:
                    asyncio.run(backend_instance.disconnect())
                except Exception:
                    pass
        self._active_async_backends.clear()

        self._reset_setup_state()

    async def cleanup_after_test_async(self, scenario_name: str) -> None:
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

        for backend_instance in self._active_backends:
            try:
                backend_instance.disconnect()
            except Exception:
                pass
        self._active_backends.clear()

        for backend_instance in self._active_async_backends:
            if not isinstance(backend_instance, AsyncOracleBackend):
                continue
            try:
                for table_name in self._relation_table_cleanup_order():
                    await self._drop_table_async(backend_instance, table_name)
            finally:
                try:
                    await backend_instance.disconnect()
                except Exception:
                    pass
        self._active_async_backends.clear()

        self._reset_setup_state()
