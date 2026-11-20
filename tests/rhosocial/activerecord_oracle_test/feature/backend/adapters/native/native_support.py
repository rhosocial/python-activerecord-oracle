# tests/rhosocial/activerecord_oracle_test/feature/backend/adapters/native/native_support.py
"""Helpers shared by the Oracle backend's native-type test modules.

The native tests live against a real server, so they must be able to say
*which* capability is missing before deciding to skip.  Everything here is a
plain function: no fixtures, no global connection state.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

DQL = ExecutionOptions(stmt_type=StatementType.DQL)
DML = ExecutionOptions(stmt_type=StatementType.DML)
DDL = ExecutionOptions(stmt_type=StatementType.DDL)

SCHEMA_FILE = Path(__file__).with_name("schema") / "native_types.sql"

_TABLE_BLOCK = re.compile(
    r"^-- table: ([A-Za-z0-9_]+)\s*\n(.*?);",
    re.MULTILINE | re.DOTALL,
)


def table_statements() -> Dict[str, str]:
    """Return ``table name -> CREATE TABLE statement`` from the schema file."""
    text = SCHEMA_FILE.read_text(encoding="utf-8")
    statements = {}
    for name, body in _TABLE_BLOCK.findall(text):
        statements[name] = body.strip()
    if not statements:
        raise ValueError(f"{SCHEMA_FILE} defines no tables")
    return statements


TABLE_STATEMENTS = table_statements()


def fetch(backend, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    """Run a raw SELECT and return the driver's unadapted rows."""
    result = backend.execute(sql, tuple(params), options=DQL)
    return list(result.data or [])


def fetch_one(backend, sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = fetch(backend, sql, params)
    if len(rows) != 1:
        raise AssertionError(f"expected one row, got {len(rows)}: {sql}")
    return rows[0]


def execute(backend, sql: str, params: Sequence[Any] = ()) -> None:
    backend.execute(sql, tuple(params), options=DML)


def existing_tables(backend) -> set:
    rows = fetch(backend, "SELECT table_name FROM user_tables")
    return {row["table_name"] for row in rows}


def ensure_tables(backend, tables: Sequence[str]) -> None:
    """Create the named native-type tables that are missing in this schema.

    The caller names every table it needs.  There is deliberately no
    "create all" default: a table family the connected server cannot
    represent would otherwise abort an unrelated test's setup with
    ``ORA-00902: invalid datatype``.
    """
    existing = existing_tables(backend)
    for name in tables:
        if name.upper() in existing:
            continue
        backend.execute(TABLE_STATEMENTS[name], (), options=DDL)


def clear_rows(backend, tables: Sequence[str]) -> None:
    """Delete every row from the named native-type tables that exist."""
    existing = existing_tables(backend)
    for name in tables:
        if name.upper() not in existing:
            continue
        backend.execute(f"DELETE FROM {name}", (), options=DML)


def server_version(backend) -> Tuple[int, ...]:
    """Return the measured server version tuple (``(23, 0, 0)`` on 23ai)."""
    version = getattr(backend, "_version", None)
    return tuple(version) if version else ()


def skip_unless_version(backend, minimum: Tuple[int, ...], capability: str) -> None:
    """Skip with the measured version when the server is too old."""
    version = server_version(backend)
    if version and version[: len(minimum)] >= minimum:
        return
    import pytest

    pytest.skip(
        f"{capability} needs server version {'/'.join(map(str, minimum))} or newer; "
        f"connected server reports {'.'.join(map(str, version)) or 'unknown'}"
    )


def spatial_available(backend) -> bool:
    """Whether the server ships Oracle Spatial (``MDSYS.SDO_GEOMETRY``)."""
    rows = fetch(
        backend,
        "SELECT COUNT(*) AS n FROM all_type_attrs "
        "WHERE owner = 'MDSYS' AND type_name = 'SDO_GEOMETRY'",
    )
    return bool(rows and rows[0]["n"])


SPATIAL_SKIP_REASON = (
    "this Oracle Free container has no Spatial component: MDSYS.SDO_GEOMETRY "
    "is absent from ALL_TYPE_ATTRS and CREATE TABLE ... SDO_GEOMETRY raises "
    "ORA-00902, so no live SDO_GEOMETRY column can be created"
)
