"""
Oracle Spatial / SDO_GEOMETRY — Spatial Queries and Predicates.

Oracle Spatial requires the Spatial option (included in most editions).
SDO_GEOMETRY is the canonical geometric type, backed by spatial indexes.

This example demonstrates:
1. SDO_GEOMETRY constructor (point, line)
2. SDO_GEOM.SDO_DISTANCE — distance between geometries
3. SDO_WITHIN_DISTANCE — spatial predicate
4. SDO_CONTAINS / SDO_INSIDE — topological predicates
5. SDO_RELATE with mask — generic relationship query

Oracle Version Support: Oracle 12c+ with Spatial option.
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

try:
    backend.execute("""
        CREATE TABLE spatial_demo (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100) NOT NULL,
            geom SDO_GEOMETRY
        )
    """, options=DDL)
except Exception as e:
    print(f"\nSDO_GEOMETRY type not available on this instance.")
    print(f"Error: {e}")
    print("Oracle Spatial option is required for SDO_GEOMETRY.")
    backend.disconnect()
    import sys
    sys.exit(0)

# Point at longitude=121.5, latitude=31.2, SRID=4326
backend.execute(
    "INSERT INTO spatial_demo (id, name, geom) VALUES (:1, :2, "
    "SDO_GEOMETRY(2001, 4326, SDO_POINT_TYPE(121.5, 31.2, NULL), NULL, NULL))",
    (1, "Shanghai"), options=ExecutionOptions(stmt_type=StatementType.INSERT),
)
backend.execute(
    "INSERT INTO spatial_demo (id, name, geom) VALUES (:1, :2, "
    "SDO_GEOMETRY(2001, 4326, SDO_POINT_TYPE(116.4, 39.9, NULL), NULL, NULL))",
    (2, "Beijing"), options=ExecutionOptions(stmt_type=StatementType.INSERT),
)
backend.execute(
    "INSERT INTO spatial_demo (id, name, geom) VALUES (:1, :2, "
    "SDO_GEOMETRY(2001, 4326, SDO_POINT_TYPE(121.6, 31.3, NULL), NULL, NULL))",
    (3, "Pudong"), options=ExecutionOptions(stmt_type=StatementType.INSERT),
)

print("=" * 60)
print("Oracle Spatial / SDO_GEOMETRY Examples")
print("=" * 60)

print("\n[1] SDO_GEOMETRY inspection")
result = backend.execute(
    "SELECT name, geom.GET_GTYPE() AS gtype, "
    "geom.SDO_POINT.X AS lon, geom.SDO_POINT.Y AS lat "
    "FROM spatial_demo ORDER BY id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']}: type={row['gtype']}, lon={row['lon']}, lat={row['lat']}")

print("\n[2] SDO_GEOM.SDO_DISTANCE")
result = backend.execute(
    "SELECT name, SDO_GEOM.SDO_DISTANCE("
    "(SELECT geom FROM spatial_demo WHERE name='Shanghai'), geom, 0.005) AS dist_m "
    "FROM spatial_demo WHERE id <> 1 ORDER BY dist_m",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']}: distance from Shanghai = {row['dist_m']:.0f}m")

print("\n[3] SDO_WITHIN_DISTANCE (spatial predicate)")
result = backend.execute(
    "SELECT name FROM spatial_demo WHERE SDO_WITHIN_DISTANCE("
    "(SELECT geom FROM spatial_demo WHERE name='Shanghai'), geom, "
    "'distance=300000') = 'TRUE'",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']} is within 300 km of Shanghai")

print("\n[4] SDO_RELATE (generic spatial relationship)")
result = backend.execute(
    "SELECT a.name AS loc1, b.name AS loc2 FROM spatial_demo a, spatial_demo b "
    "WHERE SDO_RELATE(a.geom, b.geom, 'mask=ANYINTERACT') = 'TRUE' "
    "AND a.id < b.id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['loc1']} <-> {row['loc2']}")

backend.execute("DROP TABLE spatial_demo PURGE", options=DDL)
backend.disconnect()
print("\nSpatial examples completed successfully.")