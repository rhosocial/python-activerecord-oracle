"""
This file provides the concrete implementation of the `IBasicProvider` interface
that is defined in the `rhosocial-activerecord-testsuite` package.

Its main responsibilities are:
1.  Reporting which test scenarios (database configurations) are available.
2.  Setting up the database environment for a given test. This includes:
    - Getting the correct database configuration for the scenario.
    - Configuring the ActiveRecord model with a database connection.
    - Dropping any old tables and creating the necessary table schema.
3.  Cleaning up any resources after a test runs.
"""
import os
import sys
import logging
from typing import Type, List, Tuple, Optional

logger = logging.getLogger(__name__)

from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType

# Import the fixture selector utility
from rhosocial.activerecord.testsuite.utils import select_fixture

# Import base version models (Python 3.8+)
from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import (
    User as UserBase, TypeCase as TypeCaseBase, ValidatedFieldUser as ValidatedFieldUserBase,
    TypeTestModel as TypeTestModelBase, ValidatedUser as ValidatedUserBase,
    TypeAdapterTest as TypeAdapterTestBase, YesOrNoBooleanAdapter,
    MappedUser as MappedUserBase, MappedPost as MappedPostBase, MappedComment as MappedCommentBase,
    ColumnMappingModel as ColumnMappingModelBase, MixedAnnotationModel as MixedAnnotationModelBase
)
# Import async base models
from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import (
    AsyncUser as AsyncUserBase, AsyncTypeCase as AsyncTypeCaseBase,
    AsyncValidatedUser as AsyncValidatedUserBase, AsyncValidatedFieldUser as AsyncValidatedFieldUserBase,
    AsyncTypeTestModel as AsyncTypeTestModelBase, AsyncTypeAdapterTest as AsyncTypeAdapterTestBase,
    AsyncMappedUser as AsyncMappedUserBase, AsyncMappedPost as AsyncMappedPostBase,
    AsyncMappedComment as AsyncMappedCommentBase,
    AsyncColumnMappingModel as AsyncColumnMappingModelBase, AsyncMixedAnnotationModel as AsyncMixedAnnotationModelBase
)

# Conditionally import Python 3.10+ models
User310 = TypeCase310 = ValidatedFieldUser310 = TypeTestModel310 = ValidatedUser310 = None
TypeAdapterTest310 = MappedUser310 = MappedPost310 = MappedComment310 = None
ColumnMappingModel310 = MixedAnnotationModel310 = None
AsyncUser310 = AsyncTypeCase310 = AsyncValidatedFieldUser310 = AsyncTypeTestModel310 = None
AsyncValidatedUser310 = AsyncTypeAdapterTest310 = AsyncMappedUser310 = AsyncMappedPost310 = None
AsyncMappedComment310 = AsyncColumnMappingModel310 = AsyncMixedAnnotationModel310 = None

if sys.version_info >= (3, 10):
    try:
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models_py310 import (
            User as User310, TypeCase as TypeCase310, ValidatedFieldUser as ValidatedFieldUser310,
            TypeTestModel as TypeTestModel310, ValidatedUser as ValidatedUser310,
            TypeAdapterTest as TypeAdapterTest310,
            MappedUser as MappedUser310, MappedPost as MappedPost310, MappedComment as MappedComment310,
            ColumnMappingModel as ColumnMappingModel310, MixedAnnotationModel as MixedAnnotationModel310
        )
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models_py310 import (
            AsyncUser as AsyncUser310, AsyncTypeCase as AsyncTypeCase310,
            AsyncValidatedUser as AsyncValidatedUser310, AsyncValidatedFieldUser as AsyncValidatedFieldUser310,
            AsyncTypeTestModel as AsyncTypeTestModel310, AsyncTypeAdapterTest as AsyncTypeAdapterTest310,
            AsyncMappedUser as AsyncMappedUser310, AsyncMappedPost as AsyncMappedPost310,
            AsyncMappedComment as AsyncMappedComment310,
            AsyncColumnMappingModel as AsyncColumnMappingModel310, AsyncMixedAnnotationModel as AsyncMixedAnnotationModel310
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.10+ fixtures: {e}")


# Select appropriate fixture classes based on Python version
def _select_model_class(base_cls, py310_cls, model_name: str) -> Type:
    """Select the most appropriate model class for the current Python version."""
    candidates = [c for c in [py310_cls, base_cls] if c is not None]
    selected = select_fixture(*candidates)
    logger.info(f"Selected {model_name}: {selected.__name__} from {selected.__module__}")
    return selected


# Select sync models
User = _select_model_class(UserBase, User310, "User")
TypeCase = _select_model_class(TypeCaseBase, TypeCase310, "TypeCase")
ValidatedFieldUser = _select_model_class(ValidatedFieldUserBase, ValidatedFieldUser310, "ValidatedFieldUser")
TypeTestModel = _select_model_class(TypeTestModelBase, TypeTestModel310, "TypeTestModel")
ValidatedUser = _select_model_class(ValidatedUserBase, ValidatedUser310, "ValidatedUser")
TypeAdapterTest = _select_model_class(TypeAdapterTestBase, TypeAdapterTest310, "TypeAdapterTest")
MappedUser = _select_model_class(MappedUserBase, MappedUser310, "MappedUser")
MappedPost = _select_model_class(MappedPostBase, MappedPost310, "MappedPost")
MappedComment = _select_model_class(MappedCommentBase, MappedComment310, "MappedComment")
ColumnMappingModel = _select_model_class(ColumnMappingModelBase, ColumnMappingModel310, "ColumnMappingModel")
MixedAnnotationModel = _select_model_class(MixedAnnotationModelBase, MixedAnnotationModel310, "MixedAnnotationModel")

# Select async models
AsyncUser = _select_model_class(AsyncUserBase, AsyncUser310, "AsyncUser")
AsyncTypeCase = _select_model_class(AsyncTypeCaseBase, AsyncTypeCase310, "AsyncTypeCase")
AsyncValidatedFieldUser = _select_model_class(AsyncValidatedFieldUserBase, AsyncValidatedFieldUser310, "AsyncValidatedFieldUser")
AsyncTypeTestModel = _select_model_class(AsyncTypeTestModelBase, AsyncTypeTestModel310, "AsyncTypeTestModel")
AsyncValidatedUser = _select_model_class(AsyncValidatedUserBase, AsyncValidatedUser310, "AsyncValidatedUser")
AsyncTypeAdapterTest = _select_model_class(AsyncTypeAdapterTestBase, AsyncTypeAdapterTest310, "AsyncTypeAdapterTest")
AsyncMappedUser = _select_model_class(AsyncMappedUserBase, AsyncMappedUser310, "AsyncMappedUser")
AsyncMappedPost = _select_model_class(AsyncMappedPostBase, AsyncMappedPost310, "AsyncMappedPost")
AsyncMappedComment = _select_model_class(AsyncMappedCommentBase, AsyncMappedComment310, "AsyncMappedComment")
AsyncColumnMappingModel = _select_model_class(AsyncColumnMappingModelBase, AsyncColumnMappingModel310, "AsyncColumnMappingModel")
AsyncMixedAnnotationModel = _select_model_class(AsyncMixedAnnotationModelBase, AsyncMixedAnnotationModel310, "AsyncMixedAnnotationModel")

from rhosocial.activerecord.testsuite.feature.basic.interfaces import IBasicProvider
from rhosocial.activerecord.testsuite.core.protocols import WorkerTestProtocol
# ...and the scenarios are defined specifically for this backend.
from .scenarios import get_enabled_scenarios, get_scenario


class BasicProvider(IBasicProvider, WorkerTestProtocol):
    """
    This is the Oracle backend's implementation for the basic features test group.
    It connects the generic tests in the testsuite with the actual Oracle database.
    """

    def __init__(self):
        self._active_backends = []
        self._active_async_backends = []

    def get_test_scenarios(self) -> List[str]:
        """Returns a list of names for all enabled scenarios for this backend."""
        return list(get_enabled_scenarios().keys())

    def _track_backend(self, backend_instance, collection: List) -> None:
        if backend_instance not in collection:
            collection.append(backend_instance)

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        """A generic helper method to handle the setup for any given model."""
        backend_class, config = get_scenario(scenario_name)
        model_class.configure(config, backend_class)

        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_backends)

        self._reset_table_sync(model_class, table_name)
        return model_class

    async def _setup_async_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        """A generic helper method to handle the setup for any given async model."""
        from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend

        _, config = get_scenario(scenario_name)
        await model_class.configure(config, AsyncOracleBackend)

        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_async_backends)

        await self._reset_table_async(model_class, table_name)
        return model_class

    def _reset_table_sync(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            # Oracle: drop table with PURGE to completely remove it
            model_class.__backend__.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=ddl_options
            )
        except Exception:
            pass

        # Ensure USERS tablespace exists (Oracle 21c XE may not have it by default)
        try:
            model_class.__backend__.execute(
                "CREATE TABLESPACE users DATAFILE '/opt/oracle/oradata/XE/users01.dbf' SIZE 100M AUTOEXTEND ON NEXT 10M",
                options=ddl_options
            )
        except Exception:
            pass

        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        model_class.__backend__.execute(schema_sql, options=ddl_options)

    async def _reset_table_async(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            # Oracle: drop table with PURGE to completely remove it
            await model_class.__backend__.execute(
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
                options=ddl_options
            )
        except Exception:
            pass

        # Ensure USERS tablespace exists (Oracle 21c XE may not have it by default)
        try:
            await model_class.__backend__.execute(
                "CREATE TABLESPACE users DATAFILE '/opt/oracle/oradata/XE/users01.dbf' SIZE 100M AUTOEXTEND ON NEXT 10M",
                options=ddl_options
            )
        except Exception:
            pass

        schema_sql = self._load_oracle_schema(f"{table_name}.sql")
        await model_class.__backend__.execute(schema_sql, options=ddl_options)

    async def _initialize_async_model_schema(self, model_class: Type[ActiveRecord], table_name: str):
        """Initialize schema for a model that shares backend with another model."""
        await self._reset_table_async(model_class, table_name)

    def _initialize_model_schema(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        """Initialize schema for a model that shares backend with another model."""
        self._reset_table_sync(model_class, table_name)

    def _setup_multiple_models(self, model_classes: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Helper to set up multiple related models for a test, sharing a single backend."""
        if not model_classes:
            return tuple()

        first_model_class, first_table_name = model_classes[0]
        first_model = self._setup_model(first_model_class, scenario_name, first_table_name)
        shared_backend = first_model.__backend__

        result = [first_model]

        for model_class, table_name in model_classes[1:]:
            model_class.__connection_config__ = first_model.__connection_config__
            model_class.__backend_class__ = first_model.__backend_class__
            model_class.__backend__ = shared_backend
            self._track_backend(shared_backend, self._active_backends)
            self._initialize_model_schema(model_class, table_name)
            result.append(model_class)

        return tuple(result)

    # --- Implementation of the IBasicProvider interface ---

    def setup_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for user model tests."""
        return self._setup_model(User, scenario_name, "users")

    async def setup_async_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async user model tests."""
        return await self._setup_async_model(AsyncUser, scenario_name, "users")

    def setup_type_case_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for type case model tests."""
        return self._setup_model(TypeCase, scenario_name, "type_cases")

    async def setup_async_type_case_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async type case model tests."""
        return await self._setup_async_model(AsyncTypeCase, scenario_name, "type_cases")

    def setup_type_test_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for type test model tests."""
        return self._setup_model(TypeTestModel, scenario_name, "type_tests")

    async def setup_async_type_test_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async type test model tests."""
        return await self._setup_async_model(AsyncTypeTestModel, scenario_name, "type_tests")

    def setup_validated_field_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for validated field user model tests."""
        return self._setup_model(ValidatedFieldUser, scenario_name, "validated_field_users")

    async def setup_async_validated_field_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async validated field user model tests."""
        return await self._setup_async_model(AsyncValidatedFieldUser, scenario_name, "validated_field_users")

    def setup_validated_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for validated user model tests."""
        return self._setup_model(ValidatedUser, scenario_name, "validated_users")

    async def setup_async_validated_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async validated user model tests."""
        return await self._setup_async_model(AsyncValidatedUser, scenario_name, "validated_users")

    def setup_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for MappedUser, MappedPost, and MappedComment models."""
        return self._setup_multiple_models([
            (MappedUser, "users"),
            (MappedPost, "posts"),
            (MappedComment, "comments")
        ], scenario_name)

    async def setup_async_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for AsyncMappedUser, AsyncMappedPost, and AsyncMappedComment models."""
        user = await self._setup_async_model(AsyncMappedUser, scenario_name, "users")
        shared_backend = user.__backend__

        post_model_class = AsyncMappedPost
        post_model_class.__connection_config__ = user.__connection_config__
        post_model_class.__backend_class__ = user.__backend_class__
        post_model_class.__backend__ = shared_backend
        await self._initialize_async_model_schema(post_model_class, "posts")

        comment_model_class = AsyncMappedComment
        comment_model_class.__connection_config__ = user.__connection_config__
        comment_model_class.__backend_class__ = user.__backend_class__
        comment_model_class.__backend__ = shared_backend
        await self._initialize_async_model_schema(comment_model_class, "comments")

        return user, post_model_class, comment_model_class

    def setup_mixed_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for ColumnMappingModel and MixedAnnotationModel."""
        return self._setup_multiple_models([
            (ColumnMappingModel, "column_mapping_items"),
            (MixedAnnotationModel, "mixed_annotation_items")
        ], scenario_name)

    async def setup_async_mixed_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for AsyncColumnMappingModel and AsyncMixedAnnotationModel."""
        column_mapping_model = await self._setup_async_model(AsyncColumnMappingModel, scenario_name, "column_mapping_items")
        shared_backend = column_mapping_model.__backend__

        mixed_annotation_model_class = AsyncMixedAnnotationModel
        mixed_annotation_model_class.__connection_config__ = column_mapping_model.__connection_config__
        mixed_annotation_model_class.__backend_class__ = column_mapping_model.__backend_class__
        mixed_annotation_model_class.__backend__ = shared_backend
        await self._initialize_async_model_schema(mixed_annotation_model_class, "mixed_annotation_items")

        return column_mapping_model, mixed_annotation_model_class

    def setup_type_adapter_model_and_schema(self, scenario_name: Optional[str] = None) -> Type[ActiveRecord]:
        """Sets up the database for the `TypeAdapterTest` model tests."""
        if scenario_name is None:
            scenario_name = self.get_test_scenarios()[0] if self.get_test_scenarios() else "default"
        return self._setup_model(TypeAdapterTest, scenario_name, "type_adapter_tests")

    async def setup_async_type_adapter_model_and_schema(self, scenario_name: Optional[str] = None) -> Type[ActiveRecord]:
        """Sets up the database for the `AsyncTypeAdapterTest` model tests."""
        if scenario_name is None:
            scenario_name = self.get_test_scenarios()[0] if self.get_test_scenarios() else "default"
        return await self._setup_async_model(AsyncTypeAdapterTest, scenario_name, "type_adapter_tests")

    def get_yes_no_adapter(self) -> 'BaseSQLTypeAdapter':
        """Returns an instance of the YesOrNoBooleanAdapter."""
        return YesOrNoBooleanAdapter()

    def _load_oracle_schema(self, filename: str) -> str:
        """Helper to load a SQL schema file from this project's fixtures."""
        schema_dir = os.path.join(os.path.dirname(__file__), "..", "rhosocial", "activerecord_oracle_test", "feature", "basic", "schema")
        schema_path = os.path.join(schema_dir, filename)

        with open(schema_path, 'r', encoding='utf-8') as f:
            return f.read()

    def cleanup_after_test(self, scenario_name: str):
        """
        Performs cleanup after a test, dropping all tables and disconnecting backends.
        """
        tables_to_drop = [
            'users', 'type_cases', 'type_tests', 'validated_field_users',
            'validated_users', 'type_adapter_tests', 'posts', 'comments',
            'column_mapping_items', 'mixed_annotation_items'
        ]
        for backend_instance in self._active_backends:
            try:
                for table_name in tables_to_drop:
                    try:
                        backend_instance.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;")
                    except Exception:
                        pass
            finally:
                try:
                    backend_instance.disconnect()
                except Exception:
                    pass

        self._active_backends.clear()

    async def cleanup_after_test_async(self, scenario_name: str):
        """
        Performs async cleanup after a test, dropping all tables and disconnecting backends.
        """
        tables_to_drop = [
            'users', 'type_cases', 'type_tests', 'validated_field_users',
            'validated_users', 'type_adapter_tests', 'posts', 'comments',
            'column_mapping_items', 'mixed_annotation_items'
        ]
        for backend_instance in self._active_async_backends:
            try:
                for table_name in tables_to_drop:
                    try:
                        await backend_instance.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;")
                    except Exception:
                        pass
            finally:
                try:
                    await backend_instance.disconnect()
                except Exception:
                    pass

        self._active_async_backends.clear()

    # --- Implementation of WorkerTestProtocol ---

    def get_worker_connection_params(self, scenario_name: str, fixture_type: str = None) -> dict:
        """
        Return serializable connection parameters for Worker processes.
        """
        from .scenarios import SCENARIO_MAP

        is_async = fixture_type and fixture_type.startswith('async_')
        backend_class_name = 'AsyncOracleBackend' if is_async else 'OracleBackend'

        table_name = 'users'
        if fixture_type:
            base_type = fixture_type.replace('async_', '')
            table_map = {
                'user': 'users',
                'type_case': 'type_cases',
                'type_test': 'type_tests',
                'validated_field_user': 'validated_field_users',
                'validated_user': 'validated_users',
                'type_adapter_test': 'type_adapter_tests',
            }
            table_name = table_map.get(base_type, 'users')

        if scenario_name not in SCENARIO_MAP:
            if SCENARIO_MAP:
                scenario_name = next(iter(SCENARIO_MAP))
            else:
                raise ValueError("No scenarios registered")

        config_dict = SCENARIO_MAP[scenario_name]

        return {
            'backend_module': 'rhosocial.activerecord.backend.impl.oracle',
            'backend_class_name': backend_class_name,
            'config_class_module': 'rhosocial.activerecord.backend.impl.oracle.config',
            'config_class_name': 'OracleConnectionConfig',
            'config_kwargs': config_dict,
            'schema_sql': self._load_oracle_schema(f'{table_name}.sql'),
        }

    def get_worker_schema_sql(self, scenario_name: str, table_name: str) -> str:
        """
        Return the SQL statement to create a specific table.
        """
        return self._load_oracle_schema(f'{table_name}.sql')
