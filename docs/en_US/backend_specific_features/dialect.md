# Oracle Dialect Expressions

## Overview

Oracle provides its own SQL dialect with PL/SQL extensions.

## DUAL Table

Oracle requires the DUAL table for queries without a FROM clause:

```python
# Query expressions using DUAL
# SELECT 1 FROM DUAL
```

## NULLS FIRST/LAST

Oracle supports NULLS FIRST and NULLS LAST in ORDER BY:

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause

# Sort with NULLS LAST
order = OrderByClause(dialect, expressions=[Column(dialect, "name")], nulls="LAST")
```

## String Functions

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall

# CONCAT function
func = FunctionCall(dialect, "CONCAT", Column(dialect, "first_name"), Column(dialect, "last_name"))
sql, params = func.to_sql()
# sql: CONCAT(first_name, last_name)
# params: ()
```

## FETCH FIRST Clause

Oracle 12c+ supports ANSI FETCH FIRST:

```python
# SELECT * FROM users FETCH FIRST 10 ROWS ONLY
```

## See Also

- [Field Types](./field_types.md) — Oracle data types
- [Indexing](./indexing.md) — Index types

💡 *AI Prompt:* "How does Oracle's SQL dialect differ from standard SQL?"
