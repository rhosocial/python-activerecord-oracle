# src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/async_migrations.py
"""
AsyncNamedMigration subclasses for Oracle migration examples.

Async counterpart of :mod:`migrations`. Each class inherits from
:class:`AsyncNamedMigration` and implements ``async def up()`` /
``async def down()`` methods that call named expressions via
``await ctx.execute(...)``.

These migrations are executed by :class:`AsyncMigrationRunner` and are
selected through the CLI ``--async`` flag.
"""

from rhosocial.activerecord.backend.migration import (
    AsyncNamedMigration,
    AsyncMigrationContext,
)

_EXPRESSIONS_MODULE = (
    "rhosocial.activerecord.backend.impl.oracle.examples"
    ".named_migrations.expressions"
)


class V001CreateUsersAsync(AsyncNamedMigration):
    """Create the ``users`` table (async)."""

    version = "v001_create_users"

    async def up(self, ctx: AsyncMigrationContext) -> None:
        await ctx.execute(f"{_EXPRESSIONS_MODULE}.create_users_table")

    async def down(self, ctx: AsyncMigrationContext) -> None:
        await ctx.execute(f"{_EXPRESSIONS_MODULE}.drop_users_table")


class V002CreatePostsAsync(AsyncNamedMigration):
    """Create the ``posts`` table after ``users`` exists (async)."""

    version = "v002_create_posts"
    dependencies = ["v001_create_users"]

    async def up(self, ctx: AsyncMigrationContext) -> None:
        await ctx.execute(f"{_EXPRESSIONS_MODULE}.create_posts_table")

    async def down(self, ctx: AsyncMigrationContext) -> None:
        await ctx.execute(f"{_EXPRESSIONS_MODULE}.drop_posts_table")


class V003CreateCustomTableAsync(AsyncNamedMigration):
    """Create a table with a user-specified name (async).

    Accepts a ``table`` parameter (default ``custom_table``).
    Usage::

        named-migration ... V003CreateCustomTableAsync --param table=my_config --async
    """

    version = "v003_create_custom_table"
    table: str = "custom_table"

    async def up(self, ctx: AsyncMigrationContext) -> None:
        await ctx.execute(
            f"{_EXPRESSIONS_MODULE}.create_custom_table",
            {"table": self.table},
        )

    async def down(self, ctx: AsyncMigrationContext) -> None:
        await ctx.execute(
            f"{_EXPRESSIONS_MODULE}.drop_custom_table",
            {"table": self.table},
        )
