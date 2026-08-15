"""
Oracle XMLType — Native XML Storage, XPath Queries, and XML Processing.

Oracle's XMLType provides native XML-aware storage with built-in
validation, extraction, and transformation functions.

This example demonstrates:
1. Creating a table with XMLTYPE column
2. Inserting XML documents
3. XMLEXISTS — check element/attribute existence
4. XMLCast — cast an XPath result to a scalar
5. Extract — extract an XML fragment via XPath

Oracle Version Support: Oracle 12c+ (XMLType available since Oracle 9i)
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
    CREATE TABLE xml_demo (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(100) NOT NULL,
        doc XMLTYPE
    )
""", options=DDL)

xml1 = """<order>
    <customer id="C1">Alice</customer>
    <items>
        <item sku="S1">Widget</item>
        <item sku="S2">Gadget</item>
    </items>
    <total>49.99</total>
</order>"""

xml2 = """<order>
    <customer id="C2">Bob</customer>
    <items>
        <item sku="S3">Tool</item>
    </items>
    <status>shipped</status>
    <total>19.99</total>
</order>"""

backend.execute(
    "INSERT INTO xml_demo (id, name, doc) VALUES (:1, :2, XMLTYPE(:3))",
    (1, "Order 1", xml1),
    options=ExecutionOptions(stmt_type=StatementType.INSERT),
)
backend.execute(
    "INSERT INTO xml_demo (id, name, doc) VALUES (:1, :2, XMLTYPE(:3))",
    (2, "Order 2", xml2),
    options=ExecutionOptions(stmt_type=StatementType.INSERT),
)

print("=" * 60)
print("Oracle XMLType Examples")
print("=" * 60)

print("\n[1] XMLCast — extract scalar value via XPath")
result = backend.execute(
    "SELECT name, XMLCast(XMLQuery('/order/customer/text()' PASSING doc RETURNING CONTENT) "
    "AS VARCHAR2(100)) AS customer_name FROM xml_demo ORDER BY id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']}: customer = {row['customer_name']}")

print("\n[2] Extract — get XML fragment")
result = backend.execute(
    "SELECT name, doc.extract('/order/customer') AS customer_fragment FROM xml_demo ORDER BY id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']}: {row['customer_fragment']}")

print("\n[3] ExtractValue — legacy direct scalar extraction")
result = backend.execute(
    "SELECT name, XMLCast(XMLQuery('/order/total/text()' PASSING doc RETURNING CONTENT) "
    "AS NUMBER(10,2)) AS total FROM xml_demo ORDER BY id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']}: total = {row['total']}")

print("\n[4] XMLEXISTS — check for element existence")
result = backend.execute(
    "SELECT name, XMLExists('/order/status' PASSING doc) AS has_status FROM xml_demo ORDER BY id",
    options=DQL,
)
for row in result.data:
    print(f"  {row['name']}: has_status = {row['has_status']}")

backend.execute("DROP TABLE xml_demo PURGE", options=DDL)
backend.disconnect()
print("\nXMLType examples completed successfully.")