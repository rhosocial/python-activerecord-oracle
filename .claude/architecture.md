# Architecture Guide - python-activerecord-oracle

> Oracle backend implementation for rhosocial-activerecord

## Project Overview

| Item | Value |
|------|-------|
| **Database** | Oracle Database |
| **Python Driver** | oracledb |
| **Python Version** | 3.9+ |

## Directory Structure

```
python-activerecord-oracle/
├── src/rhosocial/activerecord/backend/impl/oracle/
│   ├── __init__.py           # Backend initialization
│   ├── backend.py            # Sync backend implementation
│   ├── async_backend.py      # Async backend implementation
│   ├── config.py             # Configuration
│   ├── dialect.py            # Oracle dialect
│   ├── protocols.py          # Protocol definitions
│   ├── transaction.py       # Transaction management
│   ├── adapters.py           # Type adapters
│   ├── mixins.py             # Oracle-specific mixins
│   ├── functions.py          # Oracle-specific functions
│   ├── introspection/        # Schema introspection
│   └── show/                 # SHOW statements
├── tests/
│   └── rhosocial/activerecord_oracle_test/
└── pyproject.toml
```

## Oracle-Specific Features

- **SYSDATE/SYSTIMESTAMP**: System date/time functions
- **ROWNUM/ROW_NUMBER**: Row numbering
- **CONNECT BY**: Hierarchical queries
- **RETURNING INTO**: Returning clause
- **PL/SQL support**: Stored procedure integration

## Expression-Dialect System

Like all backends, Oracle uses the Expression-Dialect separation:
- Expression classes define query structure
- Dialect classes handle SQL generation

Oracle-specific expressions handle Oracle-specific syntax.

## Reference

- [Core architecture](../python-activerecord/.claude/architecture.md)
- [Backend development guide](../python-activerecord/.claude/backend_development.md)