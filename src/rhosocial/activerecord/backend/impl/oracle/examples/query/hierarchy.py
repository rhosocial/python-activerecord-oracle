"""
Oracle Hierarchical Queries (START WITH / CONNECT BY).

Oracle's hierarchical query capability is one of its distinctive features.
Using START WITH ... CONNECT BY PRIOR, you can traverse tree structures
(employee hierarchies, bill-of-materials, category trees) in a single SQL
statement.

This example demonstrates:
1. Basic START WITH ... CONNECT BY
2. LEVEL pseudo-column — depth in the tree
3. ORDER SIBLINGS BY — sort at each level
4. SYS_CONNECT_BY_PATH — build a "path" string from root to leaf
5. CONNECT_BY_ROOT — reference a column value from the root of a branch
6. CONNECT_BY_ISLEAF — flag leaf nodes

Oracle Version Support: 12c+ (CONNECT BY available since Oracle 2!)
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
    CREATE TABLE employees (
        emp_id NUMBER PRIMARY KEY,
        name VARCHAR2(100) NOT NULL,
        mgr_id NUMBER,
        title VARCHAR2(50),
        CONSTRAINT fk_mgr FOREIGN KEY (mgr_id) REFERENCES employees(emp_id)
    )
""", options=DDL)

rows = [
    (1, "Alice (CEO)", None, "CEO"),
    (2, "Bob (VP Sales)", 1, "VP Sales"),
    (3, "Charlie (VP Eng)", 1, "VP Engineering"),
    (4, "Diana (Lead)", 2, "Sales Lead"),
    (5, "Eve (Rep)", 4, "Sales Rep"),
    (6, "Frank (Sr Dev)", 3, "Senior Dev"),
    (7, "Grace (Jr Dev)", 6, "Junior Dev"),
]
for eid, name, mgr, title in rows:
    backend.execute(
        "INSERT INTO employees (emp_id, name, mgr_id, title) VALUES (:1, :2, :3, :4)",
        (eid, name, mgr, title), options=ExecutionOptions(stmt_type=StatementType.INSERT),
    )

print("=" * 60)
print("Oracle Hierarchical Query Examples")
print("=" * 60)

print("\n[1] Full org chart (START WITH ... CONNECT BY PRIOR)")
result = backend.execute(
    "SELECT emp_id, name, mgr_id FROM employees "
    "START WITH mgr_id IS NULL "
    "CONNECT BY PRIOR emp_id = mgr_id",
    options=DQL,
)
for row in result.data:
    print(f"  emp_id={row['emp_id']}, name={row['name']}, mgr_id={row['mgr_id']}")

print("\n[2] With LEVEL and ORDER SIBLINGS BY")
result = backend.execute(
    "SELECT LEVEL, LPAD(' ', LEVEL * 2) || name AS org_name "
    "FROM employees "
    "START WITH mgr_id IS NULL "
    "CONNECT BY PRIOR emp_id = mgr_id "
    "ORDER SIBLINGS BY name",
    options=DQL,
)
for row in result.data:
    print(f"  {row['org_name']}")

print("\n[3] SYS_CONNECT_BY_PATH — full chain from root")
result = backend.execute(
    "SELECT name, SYS_CONNECT_BY_PATH(name, ' -> ') AS path "
    "FROM employees "
    "START WITH mgr_id IS NULL "
    "CONNECT BY PRIOR emp_id = mgr_id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']} -> {row['path']}")

print("\n[4] CONNECT_BY_ROOT + CONNECT_BY_ISLEAF")
result = backend.execute(
    "SELECT name, CONNECT_BY_ROOT name AS root_name, "
    "CONNECT_BY_ISLEAF AS is_leaf "
    "FROM employees "
    "START WITH mgr_id IS NULL "
    "CONNECT BY PRIOR emp_id = mgr_id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']}  (root={row['root_name']}, leaf={row['is_leaf']})")

backend.execute("DROP TABLE employees PURGE", options=DDL)
backend.disconnect()
print("\nHierarchical query examples completed successfully.")