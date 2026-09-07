# Oracle Specific Features

This section covers Oracle-specific features that differ from other backends. rhosocial-activerecord uses a two-layer architecture for many features: a **core layer** provides common interfaces and default implementations, while each backend's **dialect layer** overrides formatting and adds backend-specific capabilities.

When you encounter a feature in this section, check whether it is a backend-specific extension or a core feature with backend-specific formatting — the documentation will indicate which layer applies.

## Contents

- [Dialect Expressions](dialect.md): Two-layer expression system — common core and Oracle-specific overrides
- [Field Types](field_types.md): Core DataType hierarchy and Oracle-specific type extensions
- [Indexing](indexing.md): Oracle-specific index types and optimization strategies
- [EXPLAIN](explain.md): Query execution plan analysis (Oracle-specific syntax)
- [Introspection](introspection.md): Database metadata queries and schema inspection
- [Partitioning](partition.md): Table partitioning (Oracle-specific, optional)
- [DDL Feature Specs](ddl_spec.md): declarative DDL Specs claimed by the Oracle dialect

## Feature Highlights

| Feature | Common Layer | Oracle-Specific Layer |
|---------|-------------|----------------------|
| Expressions | Core expression classes (Column, Literal, FunctionCall, etc.) | Dialect overrides and Oracle-specific expression classes |
| Type System | Core DataType hierarchy (IntegerType, VarCharType, etc.) | Oracle-specific DataType subclasses and type adapters |
| EXPLAIN | ExplainExpression interface | Oracle-specific EXPLAIN PLAN syntax and result parsing |
| Introspection | Introspector interface | Oracle-specific metadata queries (ALL_TABLES, USER_TAB_COLUMNS, etc.) |

## Oracle-Unique Features

| Feature | Description |
|---------|-------------|
| **Sequences** | Oracle-native sequence objects for auto-increment (NEXTVAL/CURRVAL) |
| **Flashback Queries** | Query historical data using `AS OF TIMESTAMP` syntax |
| **Hierarchical Queries** | Tree traversal using `CONNECT BY` / `START WITH` |
| **Spatial Data** | SDO_GEOMETRY type for geographic and geometric data |
| **Materialized Views** | Refreshable materialized views with multiple refresh strategies |
| **PIVOT/UNPIVOT** | Row-to-column and column-to-row transformations |
| **Synonyms** | Database object aliases for location transparency |
| **PL/SQL** | Stored procedures, functions, packages, and triggers |
| **Advanced Queuing** | Oracle AQ for message-based workflows |
| **Vector Search** | AI/ML vector type for similarity search (23ai+) |

## Related Topics

- [Type Adapters](../type_adapters/README.md) — type mapping and custom adapters
- [DDL Operations](../ddl/README.md) — schema management
- [Core: Expression System](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/backend/expression)
- [Core: Backend System](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/backend)
