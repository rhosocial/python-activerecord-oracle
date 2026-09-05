# concurrency tests

Oracle concurrency hint: verifies that `OracleBackend` implements the `ConcurrencyAware` protocol and that `get_concurrency_hint()` returns `None` (when `V$PARAMETER` is unprivileged) or a `ConcurrencyHint` with `max_concurrency > 0`.

## Key files

- `test_concurrency_protocol.py` — protocol conformance + concurrency hint