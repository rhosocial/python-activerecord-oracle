# Custom Expressions

## Overview

The Oracle backend extends core expression classes with Oracle-specific SQL syntax. You can create new expression types to add support for Oracle-unique SQL constructs.

## Expression Design Principles

Expressions are **declarative** — they collect all parameters and delegate SQL generation to the dialect:

```python
class MyExpression(BaseExpression):
    def __init__(self, dialect, **params):
        self.dialect = dialect
        self.params = params

    def to_sql(self, dialect):
        # Delegate to dialect for SQL generation
        return dialect.format_my_expression(**self.params)
```

## Creating Custom Expressions

### Step 1: Define the Expression Class

```python
from rhosocial.activerecord.backend.expression.base import BaseExpression
from rhosocial.activerecord.backend.expression import Column

class OracleNVLExpression(BaseExpression):
    """Oracle NVL expression for NULL substitution."""

    def __init__(self, dialect, column, default):
        self.dialect = dialect
        self.column = column
        self.default = default

    def to_sql(self, dialect):
        return f"NVL({self.column.to_sql(dialect)}, {self.default})"
```

### Step 2: Register with Dialect

```python
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

class CustomOracleDialect(OracleDialect):
    def format_nvl(self, column, default):
        return f"NVL({column}, {default})"
```

### Step 3: Use in Code

```python
# Build an expression and inspect the generated SQL and parameters
column = Column(dialect, "name")
expr = BinaryExpression(dialect, column, "=", Literal(dialect, "Tom"))
sql, params = expr.to_sql()
# sql: name = :1
# params: ('Tom',)
```

> **Note**: Use expression classes (`BinaryExpression`, `Column`, `Literal`, `FunctionCall`) instead of raw string queries. Build the expression, then call `expr.to_sql()` to obtain `sql` and `params`.

## Oracle-Specific Function Examples

Use `FunctionCall` for Oracle functions such as `NVL`, `COALESCE`, `DECODE`, and `TO_CHAR`:

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall

# NVL function
func = FunctionCall(dialect, "NVL", Column(dialect, "name"), Literal(dialect, "unknown"))
sql, params = func.to_sql()
# sql: NVL(name, :1)
# params: ('unknown',)
```

## Operator Mixins

Use operator mixins for common comparison and arithmetic operations:

```python
from rhosocial.activerecord.backend.expression.operators import ComparisonMixin, ArithmeticMixin

class MyExpression(ComparisonMixin, ArithmeticMixin, BaseExpression):
    pass

# Now supports ==, !=, <, >, +, -, *, /, etc.
expr = MyExpression(dialect, Column(dialect, "amount")) > 100
```

## Serialization

Expressions support serialization/deserialization for caching and logging:

```python
# Serialize
data = expr.serialize()

# Deserialize
expr = BaseExpression.deserialize(data)
```

## See Also

- [Core Expression System](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/expression) — expression base classes and operators
- [Oracle Dialect](../backend_specific_features/dialect.md) — Oracle-specific SQL functions

💡 *AI Prompt:* "How do I create a custom expression for an Oracle function like NVL?"

