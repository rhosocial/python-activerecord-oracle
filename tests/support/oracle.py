# tests/support/oracle.py
"""The Oracle backend's test scenarios.

The scenario file holds server connection parameters only; the logical name
comes from the test directory and is injected here.  Oracle has no database
per connection: a user *is* a schema, so the logical name becomes the schema
user a test connects as, while maintenance work (creating and dropping that
user) runs as the scenario's configured administrative user on the same
service.
"""

import os
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict

import yaml

from rhosocial.activerecord.backend.impl.oracle.backend import (
    AsyncOracleBackend,
    OracleBackend,
)
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

CONFIG_PATH = Path(
    os.environ.get("ORACLE_SCENARIOS_CONFIG_PATH")
    or Path(__file__).resolve().parents[1] / "config" / "oracle_scenarios.yaml"
)

# The scenario file describes a server, never a schema: the logical name is
# derived from the test directory.  ``database`` and ``enabled`` are not
# connection parameters, so they are dropped when present.
SERVER_ONLY_KEYS = ("database", "enabled")

#: Password of the per-directory schema users this backend creates.  The
#: scenario's own credentials are those of the administrative user, which a
#: test must never share: two directories would then write into one schema.
SCHEMA_PASSWORD = "OraclePool1"

DDL_OPTIONS = ExecutionOptions(stmt_type=StatementType.DDL)

#: Dropping a user that does not exist raises ORA-01918; that is the expected
#: outcome on a first run and must not abort the reset.
_DROP_USER = """BEGIN
  EXECUTE IMMEDIATE 'DROP USER {name} CASCADE';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -1918 THEN RAISE; END IF;
END;"""

_CREATE_USER = (
    'CREATE USER {name} IDENTIFIED BY "{password}" '
    "DEFAULT TABLESPACE users QUOTA UNLIMITED ON users"
)

_GRANT_USER = "GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO {name}"


def _load_scenarios(path: Path = CONFIG_PATH) -> Dict[str, Dict[str, Any]]:
    """Load one server-parameter dict per scenario from the YAML file."""
    with open(path, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    entries = data.get("scenarios")
    if not entries:
        raise ValueError(f"{path} defines no scenarios")
    return {
        name: {key: value for key, value in entry.items() if key not in SERVER_ONLY_KEYS}
        for name, entry in entries.items()
    }


SCENARIO_MAP = _load_scenarios()
SCENARIOS = tuple(SCENARIO_MAP)


@dataclass(frozen=True)
class OracleScenario:
    """One Oracle test scenario: a server plus the backend classes."""

    name: str
    server: Dict[str, Any]
    backend_class: Any = OracleBackend
    async_backend_class: Any = AsyncOracleBackend

    def server_config(self) -> OracleConnectionConfig:
        """Return the scenario's own (administrative) configuration."""
        return OracleConnectionConfig(**self.server)

    def make_config(self, database: str) -> OracleConnectionConfig:
        """Return the configuration that connects as ``database``'s schema user."""
        return replace(
            self.server_config(),
            database=database,
            username=database,
            password=SCHEMA_PASSWORD,
        )

    def reset_database(self, database: str) -> None:
        """Drop and recreate ``database`` as an empty schema user."""
        backend = self.backend_class(connection_config=self.server_config())
        backend.connect()
        try:
            backend.execute(_DROP_USER.format(name=database), (), options=DDL_OPTIONS)
            backend.execute(
                _CREATE_USER.format(name=database, password=SCHEMA_PASSWORD),
                (),
                options=DDL_OPTIONS,
            )
            backend.execute(_GRANT_USER.format(name=database), (), options=DDL_OPTIONS)
        finally:
            backend.disconnect()


def scenario(name: str) -> OracleScenario:
    """Return the scenario called ``name``."""
    try:
        server = SCENARIO_MAP[name]
    except KeyError:
        raise KeyError(f"Unknown Oracle scenario: {name}") from None
    return OracleScenario(name, server)


def get_enabled_scenarios():
    """Return the scenarios this backend enables, as a name -> server map."""
    return {name: dict(server) for name, server in SCENARIO_MAP.items()}


def get_scenario(name: str):
    """Return ``(backend_class, config)`` for a benchmark database."""
    current = scenario(name)
    current.reset_database("test_benchmark")
    return current.backend_class, current.make_config("test_benchmark")


def get_scenario_raw(name: str):
    """Return ``(backend_class, config)`` for a server-level connection.

    The configuration keeps the scenario's own administrative user, so callers
    reach the whole service rather than one directory's schema.  Falls back to
    the first configured scenario when CI enables only one.
    """
    resolved = name if name in SCENARIO_MAP else next(iter(SCENARIO_MAP), None)
    if resolved is None:
        raise KeyError("No %s scenario configured" % name)
    current = scenario(resolved)
    return current.backend_class, current.server_config()
