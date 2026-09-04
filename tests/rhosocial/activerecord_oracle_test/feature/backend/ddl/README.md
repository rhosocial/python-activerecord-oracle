# ddl tests

Expression-level CreateTableExpression.diff() coverage for the Oracle dialect plus DDL rendering regressions: capability hooks (in-place MODIFY COLUMN type changes, no property subclauses, no ALTER TABLE index actions), alter/rebuild routing, auto-increment/IDENTITY DDL compilation, standard types, boolean defaults and timestamps, and DROP TABLE ... CASCADE CONSTRAINTS rendering.

## Key files

- `test_create_table_expression_diff.py` — CreateTableExpression.diff() with Oracle capability hooks
- `test_auto_increment_ddl.py` — IDENTITY / defaults regressions
- `test_drop_table_cascade.py` — CASCADE CONSTRAINTS rendering
