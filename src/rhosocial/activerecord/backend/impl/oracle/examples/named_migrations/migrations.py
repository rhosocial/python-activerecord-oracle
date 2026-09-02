# src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/migrations.py
"""
NamedMigration subclasses for Oracle migration examples.

Each class represents a single schema migration with version, optional
dependencies, and up()/down() methods that call named expressions.
"""

from rhosocial.activerecord.backend.migration import NamedMigration, MigrationContext


class V001CreateUsers(NamedMigration):
    """Create the ``users`` table."""

    version = "v001_create_users"

    def up(self, ctx: MigrationContext) -> None:
        ctx.execute(
            "rhosocial.activerecord.backend.impl.oracle.examples.named_migrations"
            ".expressions.create_users_table"
        )

    def down(self, ctx: MigrationContext) -> None:
        ctx.execute(
            "rhosocial.activerecord.backend.impl.oracle.examples.named_migrations"
            ".expressions.drop_users_table"
        )


class V002CreatePosts(NamedMigration):
    """Create the ``posts`` table after ``users`` exists."""

    version = "v002_create_posts"
    dependencies = ["v001_create_users"]

    def up(self, ctx: MigrationContext) -> None:
        ctx.execute(
            "rhosocial.activerecord.backend.impl.oracle.examples.named_migrations"
            ".expressions.create_posts_table"
        )

    def down(self, ctx: MigrationContext) -> None:
        ctx.execute(
            "rhosocial.activerecord.backend.impl.oracle.examples.named_migrations"
            ".expressions.drop_posts_table"
        )


class V003CreateCustomTable(NamedMigration):
    """Create a table with a user-specified name.

    Accepts a ``table`` parameter (default ``custom_table``).
    Usage::

        named-migration ... V003CreateCustomTable --param table=my_config
    """

    version = "v003_create_custom_table"
    table: str = "custom_table"

    def up(self, ctx: MigrationContext) -> None:
        ctx.execute(
            "rhosocial.activerecord.backend.impl.oracle.examples.named_migrations"
            ".expressions.create_custom_table",
            {"table": self.table},
        )

    def down(self, ctx: MigrationContext) -> None:
        ctx.execute(
            "rhosocial.activerecord.backend.impl.oracle.examples.named_migrations"
            ".expressions.drop_custom_table",
            {"table": self.table},
        )
