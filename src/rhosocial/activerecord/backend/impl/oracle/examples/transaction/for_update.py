"""
Oracle FOR UPDATE — Enhanced Locking (NOWAIT / WAIT / SKIP LOCKED).

Oracle's FOR UPDATE clause offers three extensions beyond standard SQL:
- NOWAIT — fail immediately instead of blocking
- WAIT n — block up to n seconds before failing
- SKIP LOCKED — skip any rows already locked by other sessions

This example demonstrates:
1. Standard FOR UPDATE (pessimistic row locking)
2. FOR UPDATE NOWAIT (non-blocking)
3. FOR UPDATE WAIT n seconds
4. FOR UPDATE SKIP LOCKED
5. FOR UPDATE of specific columns

The example also shows LOCK TABLE for higher-level table locks.

Oracle Version Support: FOR UPDATE SKIP LOCKED 11g+ / WAIT 9i+ / NOWAIT 8i+
"""

import os

from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

config = OracleConnectionConfig(
    host=os.getenv('ORACLE_HOST', '127.0.0.1'),
    port=int(os.getenv('ORACLE_PORT', '11522')),
    username=os.getenv('ORACLE_USER', 'system'),
    password=os.getenv('ORACLE_PASSWORD', 'Password1!'),
    service_name=os.getenv('ORACLE_SERVICE', 'xepdb1'),
)

backend = OracleBackend(connection_config=config)
backend.connect()

DDL = ExecutionOptions(stmt_type=StatementType.DDL)
DQL = ExecutionOptions(stmt_type=StatementType.DQL)

backend.execute("""
    CREATE TABLE lock_demo (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(100) NOT NULL,
        amount NUMBER(10, 2) DEFAULT 0.00
    )
""", options=DDL)

for i, n, a in [(1, "Item-A", 100.00), (2, "Item-B", 200.00), (3, "Item-C", 300.00)]:
    backend.execute(
        "INSERT INTO lock_demo (id, name, amount) VALUES (:1, :2, :3)",
        (i, n, a), options=ExecutionOptions(stmt_type=StatementType.INSERT),
    )

print("=" * 60)
print("Oracle Enhanced FOR UPDATE / LOCK TABLE Examples")
print("=" * 60)

with backend.transaction():
    print("\n[1] FOR UPDATE (standard row locking)")
    result = backend.execute(
        "SELECT id, name, amount FROM lock_demo WHERE id = 1 FOR UPDATE",
        options=DQL,
    )
    row = result.data[0]
    print(f"  Locked row: {row['name']} (amount={row['amount']})")
    backend.execute(
        "UPDATE lock_demo SET amount = :1 WHERE id = :2",
        (150.00, 1), options=ExecutionOptions(stmt_type=StatementType.UPDATE),
    )
    print(f"  Updated amount to 150.00")

print("\n[2] FOR UPDATE NOWAIT")
try:
    with backend.transaction():
        result = backend.execute(
            "SELECT id, name FROM lock_demo WHERE id = 2 FOR UPDATE NOWAIT",
            options=DQL,
        )
        print(f"  Locked row 2: {result.data[0]['name']} (NOWAIT succeeded)")
except Exception as e:
    print(f"  NOWAIT failed: {type(e).__name__}")

print("\n[3] FOR UPDATE WAIT n")
try:
    with backend.transaction():
        result = backend.execute(
            "SELECT id, name FROM lock_demo WHERE id = 3 FOR UPDATE WAIT 5",
            options=DQL,
        )
        print(f"  Locked row 3: {result.data[0]['name']} (WAIT 5 succeeded)")
except Exception as e:
    print(f"  WAIT 5 failed: {type(e).__name__}")

print("\n[4] FOR UPDATE SKIP LOCKED")
with backend.transaction():
    result = backend.execute(
        "SELECT id, name FROM lock_demo FOR UPDATE SKIP LOCKED",
        options=DQL,
    )
    print(f"  Got {len(result.data)} un-locked row(s):")
    for row in result.data:
        print(f"    {row['name']}")

print("\n[5] LOCK TABLE SHARE MODE")
try:
    backend.execute("LOCK TABLE lock_demo IN SHARE MODE", options=DDL)
    print("  LOCK TABLE SHARE MODE succeeded")
except Exception as e:
    print(f"  LOCK TABLE failed: {type(e).__name__}")

backend.execute("DROP TABLE lock_demo PURGE", options=DDL)
backend.disconnect()
print("\nFOR UPDATE / LOCK TABLE examples completed successfully.")