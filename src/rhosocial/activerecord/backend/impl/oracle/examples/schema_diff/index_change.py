"""
Schema diff: detect index changes (drop one index, add another).

Builds a ``SchemaSnapshot`` before and after dropping an existing index and
creating a new one (via standalone ``DropIndexExpression`` /
``CreateIndexExpression``), then uses ``OracleSchemaDiffer`` to report the
added/removed indexes.

Supported versions: Oracle 12c+
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.oracle import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig

config = OracleConnectionConfig(
    host=os.getenv("ORACLE_HOST", "localhost"),
    port=int(os.getenv("ORACLE_PORT", "1521")),
    username=os.getenv("ORACLE_USER", "system"),
    password=os.getenv("ORACLE_PASSWORD", ""),
    service_name=os.getenv("ORACLE_SERVICE", "XEPDB1"),
)
backend = OracleBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
dialect = backend.dialect

# Clean up any leftover tables
from rhosocial.activerecord.backend.expression import (  # noqa: E402
    DropTableExpression, CreateTableExpression, ColumnDefinition,
    ColumnConstraint, ColumnConstraintType, IndexDefinition,
)
from rhosocial.activerecord.backend.expression.types import (  # noqa: E402
    IntegerType, VarCharType,
)

expr = DropTableExpression(dialect, "USERS", if_exists=True)
sql, params = expr.to_sql()
backend.execute(sql, params)

# Baseline table with a non-unique index on EMAIL
expr = CreateTableExpression(
    dialect=dialect, table="USERS", columns=[
        ColumnDefinition("ID", IntegerType(),
            constraints=[
                ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY, is_auto_increment=True),
            ]),
        ColumnDefinition("EMAIL", VarCharType(255)),
    ],
    indexes=[IndexDefinition("IDX_EMAIL", ["EMAIL"])],
)
sql, params = expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.schema import (  # noqa: E402
    SyncSchemaSnapshotBuilder,
)
from rhosocial.activerecord.backend.impl.oracle.schema.differ import (  # noqa: E402
    OracleSchemaDiffer,
)
from rhosocial.activerecord.backend.expression.statements.ddl_index import (  # noqa: E402
    CreateIndexExpression, DropIndexExpression,
)

builder = SyncSchemaSnapshotBuilder(backend.introspector, dialect)
snapshot_before = builder.build()

# Drop the non-unique IDX_EMAIL and add a unique IDX_EMAIL_UNIQUE
# via standalone DROP INDEX / CREATE INDEX statements.
backend.execute(*DropIndexExpression(
    dialect, index_name="IDX_EMAIL", table_name="USERS"
).to_sql())
backend.execute(*CreateIndexExpression(
    dialect, index_name="IDX_EMAIL_UNIQUE", table_name="USERS",
    columns=["EMAIL"], unique=True,
).to_sql())

snapshot_after = builder.build()

differ = OracleSchemaDiffer()
diff = differ.compare(snapshot_before, snapshot_after)

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
print(f"Modified tables: {diff.modified_tables}")

if "USERS" in diff.table_diffs:
    td = diff.table_diffs["USERS"]
    for idx in td.added_indexes:
        print(f"  Added index:   {idx.name} unique={idx.is_unique} columns={idx.columns}")
    for idx in td.removed_indexes:
        print(f"  Removed index: {idx.name} unique={idx.is_unique} columns={idx.columns}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
expr = DropTableExpression(dialect, "USERS", if_exists=True)
sql, params = expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
