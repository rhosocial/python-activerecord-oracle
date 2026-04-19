"""
Oracle Type System Examples.

This example demonstrates the Oracle-specific type definitions:
1. INTERVAL types (YEAR TO MONTH, DAY TO SECOND)
2. ROWID/UROWID types
3. XMLType
4. SDO_GEOMETRY spatial type
5. VECTOR type (23ai+)
"""

import sys
sys.path.insert(0, 'src')

print("=" * 60)
print("Oracle Type System Examples")
print("=" * 60)

print("\n" + "-" * 40)
print("1. INTERVAL Types")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.types import (
    IntervalYearToMonth,
    IntervalDayToSecond,
)

ym = IntervalYearToMonth(years=2, months=6)
print(f"IntervalYearToMonth: {ym}")
print(f"  ISO 8601: {ym.to_iso8601()}")
print(f"  Total months: {ym.total_months()}")

ds = IntervalDayToSecond(days=5, hours=12, minutes=30, seconds=45)
print(f"\nIntervalDayToSecond: {ds}")
print(f"  Total seconds: {ds.total_seconds()}")

from datetime import timedelta
td = timedelta(days=3, hours=6, minutes=15)
ds2 = IntervalDayToSecond.from_timedelta(td)
print(f"\nFrom timedelta ({td}): {ds2}")

print("\n" + "-" * 40)
print("2. ROWID Types")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.types import (
    OracleRowID,
    OracleURowID,
    parse_rowid,
)

rowid = OracleRowID("AAASdqAAEAAAAInAAA")
print(f"ROWID: {rowid}")
print(f"  Data object number: {rowid.data_object_number}")
print(f"  File number: {rowid.file_number}")
print(f"  Block number: {rowid.block_number}")
print(f"  Row number: {rowid.row_number}")

urowid = OracleURowID("some_urowid_value")
print(f"\nUROWID: {urowid}")
print(f"  Length: {len(urowid)}")

print("\n" + "-" * 40)
print("3. XMLType")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.types import OracleXMLType

xml = OracleXMLType("<root><name>John</name><age>30</age></root>")
print(f"XMLType: {xml.content[:50]}...")
print(f"  Is valid: {xml.is_valid}")
print(f"  Extract /root/name: {xml.extract('/root/name')}")

print("\n" + "-" * 40)
print("4. SDO_GEOMETRY (Spatial)")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.types import (
    SDOGeometry,
    SDOPoint,
    SDOGeometryType,
)

point = SDOGeometry.point(10.0, 20.0)
print(f"Point geometry:")
print(f"  GTYPE: {point.sdo_gtype}")
print(f"  Type: {point.geometry_type.name}")
print(f"  Dimension: {point.dimension}")
print(f"  SQL constructor: {point.to_constructor_sql()}")

polygon = SDOGeometry.polygon([
    (0, 0), (10, 0), (10, 10), (0, 10), (0, 0)
])
print(f"\nPolygon geometry:")
print(f"  GTYPE: {polygon.sdo_gtype}")
print(f"  Type: {polygon.geometry_type.name}")
print(f"  Ordinates count: {len(polygon.sdo_ordinates)}")

print("\n" + "-" * 40)
print("5. VECTOR Type (23ai+)")
print("-" * 40)

from rhosocial.activerecord.backend.impl.oracle.types import OracleVector

vec1 = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
vec2 = OracleVector(dimensions=3, values=[1.0, 0.0, 0.0])

print(f"Vector 1: {vec1}")
print(f"Vector 2: {vec2}")
print(f"  String format: {vec1.to_string()}")
print(f"  Cosine similarity: {vec1.cosine_similarity(vec2):.4f}")
print(f"  Euclidean distance: {vec1.euclidean_distance(vec2):.4f}")

vec_normalized = vec1.normalize()
print(f"\nNormalized v1: {vec_normalized}")
print(f"  L2 norm: {vec_normalized.l2_norm():.4f}")

print("\n" + "=" * 60)
print("All type examples completed successfully!")
print("=" * 60)
