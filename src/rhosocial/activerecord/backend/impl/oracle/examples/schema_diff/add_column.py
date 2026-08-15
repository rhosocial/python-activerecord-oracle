"""
Schema diff: detect an added column.

Oracle ``ALTER TABLE ADD`` always appends columns at the end of the table;
there is no ``AFTER`` clause. The ``OracleSchemaDiffer`` reports the new
column as added.

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
    ColumnConstraint, ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.types import (  # noqa: E402
    IntegerType, VarCharType,
)

expr = DropTableExpression(dialect, "USERS", if_exists=True)
sql, params = expr.to_sql()
backend.execute(sql, params)

# Baseline table: ID, NAME, EMAIL
expr = CreateTableExpression(
    dialect=dialect, table="USERS", columns=[
        ColumnDefinition("ID", IntegerType(),
            constraints=[
                ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY, is_auto_increment=True),
            ]),
        ColumnDefinition("NAME", VarCharType(100)),
        ColumnDefinition("EMAIL", VarCharType(255)),
    ]
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
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (  # noqa: E402
    AlterTableExpression, AddColumn,
)

builder = SyncSchemaSnapshotBuilder(backend.introspector, dialect)
snapshot_before = builder.build()

# Add `AGE` column. Oracle appends at the end (no AFTER support).
add_col = AddColumn(dialect, ColumnDefinition("AGE", IntegerType()))
alter_expr = AlterTableExpression(dialect, "USERS", [add_col])
sql, params = alter_expr.to_sql()
backend.execute(sql, params)

snapshot_after = builder.build()

differ = OracleSchemaDiffer()
diff = differ.compare(snapshot_before, snapshot_after)

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
print(f"Modified tables: {diff.modified_tables}")

if "USERS" in diff.table_diffs:
    td = diff.table_diffs["USERS"]
    for cd in td.column_diffs:
        kind = "added" if cd.is_added else "modified" if cd.is_modified else "removed"
        old_pos = cd.old.ordinal_position if cd.old else "-"
        new_pos = cd.new.ordinal_position if cd.new else "-"
        print(f"  Column '{cd.column_name}': {kind} (ordinal: {old_pos}->{new_pos})")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
expr = DropTableExpression(dialect, "USERS", if_exists=True)
sql, params = expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
