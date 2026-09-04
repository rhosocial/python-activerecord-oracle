# oracle tests

Vendor-specific subtree for Oracle implementation details: Phase 1 module restructuring/importability checks, real-world property graph (PGQ) scenarios for Oracle 23ai+ built purely from expressions, SQL/XML standard support boundaries, PGQ dialect version gating and formatting, Oracle-specific dialect expressions (ALTER TABLE clauses, ANALYZE, COMMENT ON, FLASHBACK family, INSERT ALL/FIRST, materialized views, MERGE enhancements, PL/SQL routines/packages, sequences, SYNONYM / DATABASE LINK), dialect identifier security, trigger/function/XMLType formatting, and the partition test suite (generic RANGE/LIST/HASH phase 4, backend-specific partition expressions, INTERVAL/REFERENCE/COMPOSITE phase 5, partition maintenance statements, plus real-scenario execution).

## Key files

- `test_phase1_standalone.py` — module importability (standalone)
- `test_phase1_structure.py` — phase 1 restructuring
- `test_property_graph_query_scenarios.py` — PGQ scenarios on Oracle 23ai+
- `test_property_graph_query_format.py` — PGQ version gating and formatting
- `test_sqlxml_support.py` — SQL/XML support
- `test_oracle_alter_table_clauses.py` — SET UNUSED, MOVE, SHRINK SPACE, ...
- `test_oracle_analyze_expressions.py` — ANALYZE TABLE expressions
- `test_oracle_comment_expressions.py` — COMMENT ON expressions
- `test_oracle_flashback_expressions.py` — AS OF / VERSIONS BETWEEN / PURGE
- `test_oracle_insert_all_expressions.py` — INSERT ALL / FIRST formatters
- `test_oracle_materialized_view_expressions.py` — materialized view DDL
- `test_oracle_merge_enhancements.py` — MERGE WHEN MATCHED ... DELETE etc.
- `test_oracle_routine_expressions.py` — procedures/functions/packages DDL
- `test_oracle_sequence_expressions.py` — sequence values and DDL
- `test_oracle_synonym_database_link_expressions.py` — synonyms and database links
- `test_oracle_dialect_security.py` — identifier security (OracleDialect)
- `test_trigger_functions_xml.py` — trigger DDL, function factories, XMLType
- `partition/` — phase 4/5 partition tests (expressions, strategies, maintenance, real runs)

## Vendor-specific tests

Vendor-specific tests: everything under this directory exercises Oracle-only implementation behavior that has no cross-backend equivalent.
