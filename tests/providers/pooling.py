# tests/providers/pooling.py
"""Database pooling helpers for the Oracle test providers.

Under parallel (pytest-xdist) runs with a positive pool size the providers
reuse a per-worker pooled schema user ``{database}_{index}`` on the scenario's
Oracle PDB instead of the shared admin account, so scenario variants of the
same test can run concurrently on different workers without conflicting.

Oracle has no ``database`` field in its scenario config (connections use
``service_name``), so the pool base name is NOT registered and falls back to
the pool default ``test_db``; the pooled identifier ``test_db_{index}`` is used
as a user (schema) on the PDB. Serial runs (no ``-n``) keep the previous
behaviour: the provider connects with the scenario's configured admin
credentials.

The scenario name selects the server (host/port/service); the pool index
selects the schema user. The two are deliberately unrelated.
"""

import oracledb

from rhosocial.activerecord.testsuite.core.pool import (
    pooled_database_name,
    register_base_database,
    register_pool_reset_handler,
)

from .scenarios import POOLED_USER_PASSWORD, SCENARIO_MAP, get_scenario_raw

# Derive each scenario's pooled-database base name from its configured
# ``database`` when present (the YAML ``database`` field). Oracle scenarios do
# not define one, so they keep the pool default base (``test_db``) and the
# pooled identifier is used as a schema user instead of a database.
for _scenario_name, _scenario_config in SCENARIO_MAP.items():
    if "database" in _scenario_config:
        register_base_database(_scenario_name, _scenario_config["database"])


def resolve_database_name(scenario_name: str):
    """
    Return the pooled schema user name (e.g. ``test_db_3``) used by a test for
    the given scenario, or ``None`` when pooling is inactive (callers then fall
    back to the scenario's configured admin credentials).
    """
    return pooled_database_name(scenario_name)


def _reset_oracle_database(scenario_name: str, db_name: str) -> None:
    """Ensure the pooled schema user exists and is empty on the scenario's PDB.

    Connects as the scenario's admin user, drops the pooled ``db_name`` user if
    it exists (CASCADE removes all its objects) and recreates it empty with
    basic privileges, so the test starts from a clean state. Errors are
    swallowed: a failed reset must not hide the underlying test failure.
    """
    if scenario_name not in SCENARIO_MAP:
        return
    _, config = get_scenario_raw(scenario_name)
    try:
        dsn = config.get_dsn() if hasattr(config, "get_dsn") else f"{config.host}:{config.port}/{config.service_name}"
        conn = oracledb.connect(user=config.username, password=config.password, dsn=dsn)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "BEGIN "
                    "  EXECUTE IMMEDIATE 'DROP USER "
                    + db_name
                    + " CASCADE'; "
                    "  EXCEPTION WHEN OTHERS THEN "
                    "    IF SQLCODE != -1918 THEN RAISE; END IF; "
                    "END;"
                )
                cursor.execute(
                    f'CREATE USER {db_name} IDENTIFIED BY "{POOLED_USER_PASSWORD}" '
                    "DEFAULT TABLESPACE users QUOTA UNLIMITED ON users"
                )
                cursor.execute(f"GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO {db_name}")
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        import traceback

        print(f"[ORACLE-POOL-PREP] failed for {scenario_name} {db_name}: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()


register_pool_reset_handler(_reset_oracle_database)
