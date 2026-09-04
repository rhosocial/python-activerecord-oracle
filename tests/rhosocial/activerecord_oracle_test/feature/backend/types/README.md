# types tests

Oracle type layer: offline formatting tests for type rendering (mixins/types.py) and the Python<->Oracle value adapters, plus the Phase 2 adapter/type additions (INTERVAL, ROWID, XML, SDO_GEOMETRY, VECTOR, BOOLEAN, ...).

## Key files

- `test_oracle_types_adapters.py` — type rendering and adapter round trips
- `test_phase2_adapters.py` — phase 2 adapters
- `test_phase2_types.py` — phase 2 type system
