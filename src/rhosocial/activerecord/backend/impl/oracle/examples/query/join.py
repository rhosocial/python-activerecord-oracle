"""
JOIN Queries — INNER / LEFT / RIGHT / CROSS joins.

This example demonstrates:
1. CROSS JOIN
2. INNER JOIN with ON clause
3. LEFT / RIGHT OUTER JOIN
4. Oracle-specific (+) outer join syntax (traditional style)

Oracle Version Support: 12c+ (ANSI join syntax since Oracle 9i)
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
    CREATE TABLE join_a (id NUMBER PRIMARY KEY, label VARCHAR2(50))
""", options=DDL)
backend.execute("""
    CREATE TABLE join_b (id NUMBER PRIMARY KEY, label VARCHAR2(50), a_id NUMBER)
""", options=DDL)

for i, l in [(1, "A1"), (2, "A2"), (3, "A3")]:
    backend.execute("INSERT INTO join_a (id, label) VALUES (:1, :2)", (i, l),
                    options=ExecutionOptions(stmt_type=StatementType.INSERT))
for i, l, a in [(10, "B1", 1), (20, "B2", 1), (30, "B3", None)]:
    backend.execute("INSERT INTO join_b (id, label, a_id) VALUES (:1, :2, :3)", (i, l, a),
                    options=ExecutionOptions(stmt_type=StatementType.INSERT))

print("=" * 60)
print("JOIN Query Examples")
print("=" * 60)

print("\n[1] CROSS JOIN")
result = backend.execute(
    "SELECT a.label AS a_label, b.label AS b_label FROM join_a a CROSS JOIN join_b b",
    options=DQL,
)
for row in result.data:
    print(f"  {row['a_label']} x {row['b_label']}")

print("\n[2] INNER JOIN")
result = backend.execute(
    "SELECT a.label, b.label AS b_label FROM join_a a INNER JOIN join_b b ON a.id = b.a_id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['label']} -> {row['b_label']}")

print("\n[3] LEFT OUTER JOIN")
result = backend.execute(
    "SELECT a.label, b.label AS b_label "
    "FROM join_a a LEFT OUTER JOIN join_b b ON a.id = b.a_id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['label']} -> {row['b_label']}")

print("\n[4] RIGHT OUTER JOIN")
result = backend.execute(
    "SELECT b.label, a.label AS a_label "
    "FROM join_a a RIGHT OUTER JOIN join_b b ON a.id = b.a_id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['label']} <- {row['a_label']}")

print("\n[5] Oracle traditional (+) outer join")
result = backend.execute(
    "SELECT a.label, b.label AS b_label FROM join_a a, join_b b WHERE a.id = b.a_id(+)",
    options=DQL,
)
for row in result.data:
    print(f"  {row['label']} -> {row['b_label']}")

backend.execute("DROP TABLE join_b PURGE", options=DDL)
backend.execute("DROP TABLE join_a PURGE", options=DDL)
backend.disconnect()
print("\nJOIN examples completed successfully.")