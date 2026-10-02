# rhosocial-activerecord Oracle Backend Documentation

The Oracle backend is the Oracle Database backend implementation for
[rhosocial-activerecord](https://github.com/rhosocial/python-activerecord). It uses the
`oracledb` driver. In Oracle the `schema_name` of this library is the object's **owner**,
identifiers fold to upper case, and a column reference on an unaliased range carries three
parts.

## Table of Contents

- **[Schema Namespaces](oracle_specific_features/schema_namespace.md)**: declaring
  `__schema_name__`, the schema-as-owner rule, upper-case folding, three-part column
  references, `SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')`, and synonym placement

## Key facts at a glance

| Question | Answer |
|---|---|
| What does `schema_name` mean? | The owner of the object |
| Qualified table renders as | `"APP"."ORDERS"` — identifiers fold to upper case |
| Column references | Three parts on unaliased ranges: `"APP"."ORDERS"."ID"` |
| After an alias | `"O"."ID"` — Oracle has no `AS` before a table alias |
| Current schema | `SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL` — the `FROM DUAL` is required |
| `CREATE SCHEMA` / `DROP SCHEMA` | Not supported — a schema is created as a user |
| Aliases | `CREATE SYNONYM s FOR t` — the schema qualifies the target table, not the synonym |

## Related documentation

- **[Schema Namespaces (core guide)](https://github.com/rhosocial/python-activerecord/tree/docs/docs/modeling/schema_namespace.md)**:
  the dialect-independent rules that every backend shares

---

> ⚠️ **Dependency note**: this backend depends on the core library
> `rhosocial-activerecord`. Install it together with the core library rather than
> independently.