# introspection tests

Oracle introspection coverage: deep tests for introspection/introspector.py (SQL builders and _parse_* helpers offline, live schema probing and status surface) and live-server status introspection (overview, configuration, performance, storage).

## Key files

- `test_introspector_deep.py` — SQL builders, parse helpers, live probing
- `test_status_introspection.py` — live status introspector
