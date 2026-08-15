"""
Schema diff: snapshot serialization round-trip.

A ``SchemaSnapshot`` can be serialized to a plain dict (JSON-safe) with
``to_dict()`` and reconstructed with ``from_dict()``. This is useful for
persisting a baseline schema to disk and later comparing it against the
live database.

Supported versions: Oracle 12c+
"""

import json

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

expr = CreateTableExpression(
    dialect=dialect, table="USERS", columns=[
        ColumnDefinition("ID", IntegerType(),
            constraints=[
                ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY, is_auto_increment=True),
            ]),
        ColumnDefinition("NAME", VarCharType(100)),
    ]
)
sql, params = expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.schema import (  # noqa: E402
    SyncSchemaSnapshotBuilder, SchemaSnapshot,
)
from rhosocial.activerecord.backend.impl.oracle.schema.differ import (  # noqa: E402
    OracleSchemaDiffer,
)

builder = SyncSchemaSnapshotBuilder(backend.introspector, dialect)
snapshot = builder.build()

# Serialize -> JSON string -> deserialize
payload = snapshot.to_dict()
json_str = json.dumps(payload, default=str, indent=2)
reloaded = json.loads(json_str)
snapshot_restored = SchemaSnapshot.from_dict(reloaded)

# A snapshot compared against itself must produce an empty diff
differ = OracleSchemaDiffer()
diff = differ.compare(snapshot, snapshot_restored)

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
print(f"Snapshot tables:       {list(snapshot.tables.keys())}")
print(f"Restored tables:       {list(snapshot_restored.tables.keys())}")
print(f"JSON payload length:   {len(json_str)} bytes")
print(f"Round-trip diff empty: {diff.is_empty}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
expr = DropTableExpression(dialect, "USERS", if_exists=True)
sql, params = expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
