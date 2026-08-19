# src/rhosocial/activerecord/backend/impl/oracle/examples/named_connections/development.py
"""Development environment connection examples.

All configuration values can be overridden via environment variables:
    ORACLE_HOST, ORACLE_PORT, ORACLE_USER, ORACLE_PASSWORD, ORACLE_SERVICE
"""

import os

from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig


def _env_or_default(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int_or_default(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


def local_dev():
    """Local development Oracle database connection.

    Reads connection parameters from environment variables with
    fallback to localhost defaults.

    Returns:
        OracleConnectionConfig: Development database configuration.
    """
    return OracleConnectionConfig(
        host=_env_or_default("ORACLE_HOST", "127.0.0.1"),
        port=_env_int_or_default("ORACLE_PORT", 1521),
        username=_env_or_default("ORACLE_USER", "system"),
        password=_env_or_default("ORACLE_PASSWORD", "Password1!"),
        service_name=_env_or_default("ORACLE_SERVICE", "FREEPDB1"),
    )


def local_dev_xepdb():
    """Local development Oracle connection to the XEPDB1 pluggable database.

    XEPDB1 is the default pluggable database shipped with Oracle XE.

    Returns:
        OracleConnectionConfig: XEPDB1 database configuration.
    """
    return OracleConnectionConfig(
        host=_env_or_default("ORACLE_HOST", "127.0.0.1"),
        port=_env_int_or_default("ORACLE_PORT", 1521),
        username=_env_or_default("ORACLE_USER", "system"),
        password=_env_or_default("ORACLE_PASSWORD", "Password1!"),
        service_name=_env_or_default("ORACLE_SERVICE", "xepdb1"),
    )


def local_dev_no_password():
    """Local Oracle connection with empty password.

    Useful for local Express Edition setups with no authentication.

    Returns:
        OracleConnectionConfig: No-password database configuration.
    """
    return OracleConnectionConfig(
        host=_env_or_default("ORACLE_HOST", "127.0.0.1"),
        port=_env_int_or_default("ORACLE_PORT", 1521),
        username=_env_or_default("ORACLE_USER", "system"),
        password=_env_or_default("ORACLE_PASSWORD", ""),
        service_name=_env_or_default("ORACLE_SERVICE", "xepdb1"),
    )
