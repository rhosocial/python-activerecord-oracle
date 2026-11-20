# Directory databases

Every backend test directory owns one logical database on every configured
scenario's server. There is no registration step: the name is derived from the
directory path, so a new directory needs no `conftest.py` of its own.

| Test directory | Logical database |
|---|---|
| `execute/connection` | `test_backend_execute_connection` |
| `execute/crud` | `test_backend_execute_crud` |
| `execute/query` | `test_backend_execute_query` |
| `execute/join` | `test_backend_execute_join` |
| `execute/transaction` | `test_backend_execute_transaction` |
| `execute/ddl_table` | `test_backend_execute_ddl_table` |
| `execute/ddl_column` | `test_backend_execute_ddl_column` |
| `execute/ddl_constraint` | `test_backend_execute_ddl_constraint` |
| `execute/ddl_index` | `test_backend_execute_ddl_index` |
| `execute/ddl_view` | `test_backend_execute_ddl_view` |
| `execute/dml_insert` | `test_backend_execute_dml_insert` |
| `execute/dml_update_delete` | `test_backend_execute_dml_update_delete` |
| `execute/json` | `test_backend_execute_json` |
| `execute/partition` | `test_backend_execute_partition` |
| `execute/introspection` | `test_backend_execute_introspection` |
| `execute/errors` | `test_backend_execute_errors` |
| `execute/concurrency` | `test_backend_execute_concurrency` |
| `execute/cli` | `test_backend_execute_cli` |
| `execute/functions` | `test_backend_execute_functions` |
| `execute/sequence_identity` | `test_backend_execute_sequence_identity` |
| `execute/fulltext_spatial` | `test_backend_execute_fulltext_spatial` |
| `execute/show_admin` | `test_backend_execute_show_admin` |
| `execute/routine_domain` | `test_backend_execute_routine_domain` |
| `execute/oracle/literals` | `test_backend_execute_oracle_literals` |
| `execute/oracle/property_graph` | `test_backend_execute_oracle_property_graph` |
| `execute/oracle/listagg` | `test_backend_execute_oracle_listagg` |

Rules:

- **One test directory = one database.** A directory that only groups leaves
  (`execute/`, `execute/oracle/`) holds no test files and owns no database;
  only its leaf subdirectories do.
- The name is derived from the directory: the path below `backend` joined with
  underscores after `test_backend` (see `_database_for` in the backend
  `conftest.py`). Adding or moving a leaf needs no table update, but the
  derived name does change with the path.
- A directory's tests stay on one xdist worker
  (`xdist_group("oracle-backend::<path>")`), so directories run serially within
  themselves and in parallel with each other. Modules that carry an explicit
  `xdist_group` keep their own group: those span directories on purpose (the
  shared `AR_CRM`/`AR_SHOP` schema users).
- The database is created once per directory and scenario, before that
  directory's first fixture request; it is not dropped afterwards.
- Construct-layer directories (`construct/`) own no database: their tests
  build expressions and never connect.
