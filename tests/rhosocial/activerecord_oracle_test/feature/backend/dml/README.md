# dml tests

Oracle DML integration: backend CRUD basics, a standalone schema-qualified DML round trip across two user schemas (mirrors the cross_schema provider fixtures), async CRUD parity and DML depth on the real async backend, plus upsert capability detection.

## Key files

- `test_crud_backend.py` — connection + CRUD
- `test_crud_backend_async.py` — async connection + CRUD (sync twin: `test_crud_backend.py`)
- `test_schema_qualified_dml.py` — schema-qualified DML round trip
- `test_dml_deep_async.py` — execute_many batches, transaction boundaries, RETURNING INTO (sync twin `test_dml_deep.py`: not yet present — Tier-2 fill)
- `test_insert_on_conflict_clauses.py` — MERGE-based upsert capability
