# Command-Line Interface

## Overview

Every Oracle backend includes a command-line interface (CLI) for database operations. The CLI provides commands for querying, introspecting, and managing your Oracle database without writing Python code.

The CLI commands fall into two categories:

1. **Backend-specific commands** — Oracle-unique operations (query, introspect, info, status)
2. **Core inherited commands** — shared across all backends (named-expression, named-procedure, named-migration, named-connection)

## Invocation

The CLI is installed as `rhosocial-activerecord-oracle` when you install the package:

```bash
pip install rhosocial-activerecord-oracle
```

Then invoke commands directly:

```bash
rhosocial-activerecord-oracle <command> [options]
```

This command is registered in `pyproject.toml` and is equivalent to `python -m rhosocial.activerecord.backend.impl.oracle`.

## Output Formats

The CLI supports multiple output formats via the `-o` / `--output` option:

| Format | Description | Rich Required |
|--------|-------------|---------------|
| `table` | Human-readable table with borders (default) | Yes |
| `json` | JSON array of objects | No |
| `csv` | Comma-separated values | No |
| `tsv` | Tab-separated values | No |

When Rich is installed, the `table` format provides beautified output with colored borders. When Rich is not available, the CLI automatically falls back to `json` format.

### Rich Integration

The CLI integrates with the [Rich](https://github.com/Textualize/rich) library for enhanced terminal output:

- **Colored borders**: Unicode box-drawing characters for table borders
- **ASCII fallback**: Use `--rich-ascii` to force ASCII borders (`+`, `-`, `|`)
- **Auto-detection**: If Rich is not installed, falls back to JSON output automatically

```bash
# Default table output (Unicode borders)
rhosocial-activerecord-oracle query ... "SELECT * FROM users"

# ASCII borders (for terminals without Unicode support)
rhosocial-activerecord-oracle query ... --rich-ascii "SELECT * FROM users"

# Force JSON output
rhosocial-activerecord-oracle query ... -o json "SELECT * FROM users"
```

### Output Examples

**Table format (default):**
```
┌─────┬─────────┬───────┐
│ ID  │ NAME    │ EMAIL │
├─────┼─────────┼───────┤
│ 1   │ Alice   │ a@x   │
│ 2   │ Bob     │ b@x   │
└─────┴─────────┴───────┘
```

**JSON format:**
```json
[
  {"id": 1, "name": "Alice", "email": "a@x"},
  {"id": 2, "name": "Bob", "email": "b@x"}
]
```

**CSV format:**
```csv
id,name,email
1,Alice,a@x
2,Bob,b@x
```

**TSV format:**
```tsv
id	name	email
1	Alice	a@x
2	Bob	b@x
```

## Backend-Specific Commands

These commands are implemented by the Oracle backend and interact directly with Oracle:

| Command | Description | Connection Required |
|---------|-------------|-------------------|
| `info` | Display environment and protocol information | No |
| `query` | Execute SQL queries | Yes |
| `introspect` | Database introspection (tables, columns, indexes, etc.) | Yes |
| `status` | Display server status and configuration | Yes |

### info

Display environment information without requiring a database connection:

```bash
rhosocial-activerecord-oracle info
```

### query

Execute SQL queries directly:

```bash
rhosocial-activerecord-oracle query \
    --host localhost --port 1521 --database ORCLPDB1 \
    --user system --password secret \
    "SELECT * FROM users WHERE ROWNUM <= 10"
```

### introspect

Inspect database metadata:

```bash
# List all tables
rhosocial-activerecord-oracle introspect tables \
    --host localhost --port 1521 --database ORCLPDB1

# Describe a specific table
rhosocial-activerecord-oracle introspect table users \
    --host localhost --port 1521 --database ORCLPDB1

# List columns
rhosocial-activerecord-oracle introspect columns users \
    --host localhost --port 1521 --database ORCLPDB1
```

#### Introspection Types

Each backend supports different introspection types:

| Type | Description |
|------|-------------|
| `tables` | List all tables |
| `views` | List all views |
| `table` | Describe a specific table |
| `columns` | List columns for a table |
| `indexes` | List indexes for a table |
| `foreign-keys` | List foreign keys for a table |
| `triggers` | List triggers |
| `database` | Database information |

Backend-specific types:

| Backend | Extra Types |
|---------|------------|
| PostgreSQL | `extensions` |
| **Oracle** | **`sequences`, `synonyms`, `materialized-views`, ` tablespaces`** |

### status

Display server status:

```bash
rhosocial-activerecord-oracle status \
    --host localhost --port 1521 --database ORCLPDB1
```

## Core Inherited Commands

These commands are **inherited from the core `python-activerecord` library** and work identically across all backends. The backend CLI simply delegates to shared core infrastructure:

```
Backend CLI adapter (thin wrapper)
    └── Core CLI helper (shared logic)
        ├── Resolver — loads Python callables by fully-qualified name
        ├── Runner — executes with transaction management
        └── CLI adapter — argument parsing and output
```

### Why Named Features?

Named features let you **encode complex configurations as a single name**, avoiding verbose command-line arguments and enabling parameter combinations that cannot be expressed through CLI flags alone.

**Named Connection** — encapsulates all connection parameters:

```bash
# Without named connection: long argument list
rhosocial-activerecord-oracle query \
    --host prod-db.example.com --port 1521 --database ORCLPDB1 \
    --user readonly --password secret --encoding UTF-8 \
    "SELECT * FROM users"

# With named connection: one name holds everything
rhosocial-activerecord-oracle query \
    --named-connection myapp.connections.prod_readonly \
    "SELECT * FROM users"
```

**Named Expression** — encapsulates complex query logic:

```bash
# Without named expression: complex SQL that's hard to shell-escape
rhosocial-activerecord-oracle query \
    "SELECT u.name, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.created_at >= DATE '2026-01-01' GROUP BY u.id HAVING COUNT(o.id) > 5 ORDER BY order_count DESC FETCH FIRST 20 ROWS ONLY"

# With named expression: one name, typed parameters
rhosocial-activerecord-oracle named-expression \
    myapp.queries.high_value_customers \
    --param since=2026-01-01 --param min_orders=5
```

**Named Procedure** — encapsulates multi-step workflows:

```bash
# Without named procedure: multiple sequential commands
rhosocial-activerecord-oracle query "BEGIN ..."
rhosocial-activerecord-oracle query "UPDATE inventory ..."
rhosocial-activerecord-oracle query "INSERT INTO orders ..."
rhosocial-activerecord-oracle query "COMMIT;"

# With named procedure: one command, transaction managed
rhosocial-activerecord-oracle named-procedure \
    myapp.workflows.place_order \
    --param user_id=42 --param product_id=100 --param quantity=3
```

**Named Migration** — encapsulates versioned schema changes with dependencies:

```bash
rhosocial-activerecord-oracle named-migration up add_users_table
rhosocial-activerecord-oracle named-migration down add_users_table
```

| Feature | Benefit |
|---------|---------|
| Named Connection | Store connection config in versionable Python code; share across scripts |
| Named Expression | Encapsulate complex SQL; type-safe parameters; reuse across tools |
| Named Procedure | Multi-query workflows with transaction management; parallel execution |
| Named Migration | Versioned schema changes with dependency tracking; up/down support |

### Command Reference

| Command | Source Module | Description |
|---------|--------------|-------------|
| `named-expression` | `backend.named_expression` | Execute type-safe parameterized SQL defined in Python |
| `named-procedure` | `backend.named_expression.procedure` | Execute multi-query orchestration with transaction support |
| `named-procedure-graph` | `backend.named_expression.procedure` | Execute procedure graphs (DAG workflows) |
| `named-migration` | `backend.migration` | Execute versioned schema changes with dependency tracking |
| `named-connection` | `backend.named_connection` | Manage and test named connection configurations |

### named-expression

Execute named expressions (parameterized SQL defined in Python modules):

```bash
rhosocial-activerecord-oracle named-expression <expression_name> \
    --host localhost --port 1521 --database ORCLPDB1
```

### named-procedure

Execute named procedures:

```bash
rhosocial-activerecord-oracle named-procedure <procedure_name> \
    --host localhost --port 1521 --database ORCLPDB1
```

### named-migration

Execute named migrations:

```bash
# Run migration up
rhosocial-activerecord-oracle named-migration up <migration_name> \
    --host localhost --port 1521 --database ORCLPDB1

# Run migration down
rhosocial-activerecord-oracle named-migration down <migration_name> \
    --host localhost --port 1521 --database ORCLPDB1
```

### named-connection

Manage and test named connection configurations:

```bash
rhosocial-activerecord-oracle named-connection <connection_name> \
    --params key=value
```

## Connection Arguments

All commands that require a database connection accept these common arguments:

| Argument | Description |
|----------|-------------|
| `--host` | Database server hostname |
| `--port` | Database server port (default: 1521) |
| `--database` | Oracle service name or SID |
| `--user` | Authentication username |
| `--password` | Authentication password |
| `--async` | Use async backend |
| `--named-connection` | Use a named connection configuration |
| `--conn-param` | Additional connection parameters |
| `--log-level` | Set logging level (DEBUG, INFO, WARNING, ERROR) |

### Backend-Specific Connection Arguments

| Backend | Extra Arguments |
|---------|----------------|
| MySQL | `--charset` |
| PostgreSQL | (none extra) |
| SQL Server | `--trusted-connection`, `--driver`, `--encrypt`/`--no-encrypt`, `--trust-server-certificate` |
| **Oracle** | **`--service` (alias for `--database`), `--mode` (thin/thick)** |
| Firebird | `--charset`, `--role` |

## Global Options

| Option | Description |
|--------|-------------|
| `-h`, `--help` | Show help message and exit |
| `--log-level` | Set logging level (DEBUG, INFO, WARNING, ERROR) |

## Architecture

The CLI follows a consistent architecture across all backends:

```
backend/impl/oracle/
├── __main__.py          # Entry point, builds parser, dispatches to handlers
└── cli/
    ├── __init__.py      # COMMAND_NAMES list, register_commands()
    ├── connection.py    # Connection argument parsing and backend creation
    ├── output.py        # Output format providers (Rich/JSON/CSV/TSV)
    │
    │   # Backend-specific commands
    ├── info.py          # 'info' command handler
    ├── query.py         # 'query' command handler
    ├── introspect.py    # 'introspect' command handler
    ├── status.py        # 'status' command handler
    │
    │   # Core inherited commands (thin adapters)
    ├── named_expression.py      # Delegates to core named_expression.cli
    ├── named_procedure.py       # Delegates to core named_expression.procedure.cli
    ├── named_procedure_graph.py # Delegates to core named_expression.procedure.cli
    ├── named_migration.py       # Delegates to core migration.cli
    └── named_connection.py      # Delegates to core named_connection.cli
```

## See Also

- [Installation Guide](../installation_and_configuration/installation.md) — setup instructions
- [Connection Management](../installation_and_configuration/pool.md) — connection configuration
- [Core Named Features](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US) — named connection, expression, procedure, migration documentation

💡 *AI Prompt:* "How do I list all tables in my Oracle database from the command line?"
