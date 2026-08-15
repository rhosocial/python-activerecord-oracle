"""
Oracle 23ai VECTOR Type and Distance Metrics.

Oracle 23ai supports the VECTOR data type for AI vector embeddings.
It provides native vector distance operators: cosine, euclidean,
inner product, and L2 normalization.

This example demonstrates:
1. Creating a table with a VECTOR column
2. Inserting VECTOR embeddings
3. Using VECTOR_DISTANCE() function with different metrics
4. Vector normalization
5. Top-K nearest neighbor via ORDER BY VECTOR_DISTANCE

Oracle Version Support: 23ai+ (VECTOR available in Oracle 23ai FREEPDB1)
"""

import os
import sys

from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


def get_config():
    host = os.getenv('ORACLE_HOST', '127.0.0.1')
    port = int(os.getenv('ORACLE_PORT', '11523'))
    user = os.getenv('ORACLE_USER', 'system')
    password = os.getenv('ORACLE_PASSWORD', 'Password1!')
    service = os.getenv('ORACLE_SERVICE', 'FREEPDB1')
    return OracleConnectionConfig(
        host=host, port=port, username=user, password=password, service_name=service,
    )


config = get_config()
backend = OracleBackend(connection_config=config)
backend.connect()

version = backend.get_server_version()
print("=" * 60)
print("Oracle VECTOR Type Examples (23ai+)")
print("=" * 60)
print(f"Server version: {'.'.join(map(str, version))}")

if version < (23, 0, 0):
    print("\nWARNING: This example requires Oracle 23ai. "
          f"Current server version is {version[0]}.{version[1]}.")
    print("VECTOR type is only available in Oracle Database Free 23ai or higher.")
    backend.disconnect()
    sys.exit(0)

DDL = ExecutionOptions(stmt_type=StatementType.DDL)
DQL = ExecutionOptions(stmt_type=StatementType.DQL)

try:
    backend.execute("""
        CREATE TABLE vector_demo (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100) NOT NULL,
            embedding VECTOR(3, FLOAT32)
        )
    """, options=DDL)
except Exception as e:
    print(f"\nVECTOR type not supported on this instance: {e}")
    backend.disconnect()
    sys.exit(0)

# 3-dimensional vectors [v1, v2, v3]
vectors = [
    (1, "Red",   "[1.0, 0.0, 0.0]"),
    (2, "Green", "[0.0, 1.0, 0.0]"),
    (3, "Blue",  "[0.0, 0.0, 1.0]"),
    (4, "Pink",  "[1.0, 0.5, 0.5]"),
]
for id_, name, vec in vectors:
    backend.execute(
        "INSERT INTO vector_demo (id, name, embedding) VALUES (:1, :2, TO_VECTOR(:3))",
        (id_, name, vec),
        options=ExecutionOptions(stmt_type=StatementType.INSERT),
    )

print("\n[1] Cosine distance (smaller = more similar)")
result = backend.execute(
    "SELECT a.name, b.name, "
    "VECTOR_DISTANCE(a.embedding, b.embedding, COSINE) AS cos_dist "
    "FROM vector_demo a CROSS JOIN vector_demo b "
    "WHERE a.id < b.id ORDER BY cos_dist",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']} vs {row['b.name']}: cos_dist={row['cos_dist']:.4f}")

print("\n[2] Euclidean distance (L2)")
result = backend.execute(
    "SELECT a.name, b.name, "
    "VECTOR_DISTANCE(a.embedding, b.embedding, EUCLIDEAN) AS euc_dist "
    "FROM vector_demo a CROSS JOIN vector_demo b "
    "WHERE a.id < b.id ORDER BY euc_dist",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']} vs {row['b.name']}: euclidean={row['euc_dist']:.4f}")

print("\n[3] Top-K nearest neighbor (K=2 nearest to 'Red')")
result = backend.execute(
    "SELECT name FROM vector_demo WHERE name <> 'Red' ORDER BY "
    "VECTOR_DISTANCE(embedding, "
    "(SELECT embedding FROM vector_demo WHERE name='Red'), COSINE) "
    "FETCH FIRST 2 ROWS ONLY",
    options=DQL,
)
for row in result.data:
    print(f"  nearest to Red: {row['name']}")

backend.execute("DROP TABLE vector_demo PURGE", options=DDL)
backend.disconnect()
print("\nVECTOR examples completed successfully.")