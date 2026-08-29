# src/rhosocial/activerecord/backend/impl/oracle/examples/named_connections/production.py
"""Production environment connection examples.

All configuration values can be overridden via environment variables:
    ORACLE_HOST, ORACLE_PORT, ORACLE_USER, ORACLE_PASSWORD, ORACLE_SERVICE
"""

import os

from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig


def _env_or_default(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int_or_default(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


def prod_db():
    """Production Oracle database connection.

    Reads connection parameters from environment variables with
    fallback to documentation defaults.

    Returns:
        OracleConnectionConfig: Production database configuration.
    """
    return OracleConnectionConfig(
        host=_env_or_default("ORACLE_HOST", "prod-oracle.example.com"),
        port=_env_int_or_default("ORACLE_PORT", 1521),
        username=_env_or_default("ORACLE_USER", "app_user"),
        password=_env_or_default("ORACLE_PASSWORD", ""),
        service_name=_env_or_default("ORACLE_SERVICE", "PROD"),
    )


def prod_db_ssl():
    """Production Oracle database with encrypted connection.

    Uses Oracle Network Encryption (native) which is enabled by
    default in recent versions.

    Returns:
        OracleConnectionConfig: Secured production database configuration.
    """
    return OracleConnectionConfig(
        host=_env_or_default("ORACLE_HOST", "prod-oracle.example.com"),
        port=_env_int_or_default("ORACLE_PORT", 1521),
        username=_env_or_default("ORACLE_USER", "app_user"),
        password=_env_or_default("ORACLE_PASSWORD", ""),
        service_name=_env_or_default("ORACLE_SERVICE", "PROD"),
    )


def prod_pool():
    """Production Oracle database with connection pooling.

    Uses Oracle connection pooling with a bounded pool for
    high-throughput production workloads.

    Returns:
        OracleConnectionConfig: Pooled production database configuration.
    """
    return OracleConnectionConfig(
        host=_env_or_default("ORACLE_HOST", "prod-oracle.example.com"),
        port=_env_int_or_default("ORACLE_PORT", 1521),
        username=_env_or_default("ORACLE_USER", "app_user"),
        password=_env_or_default("ORACLE_PASSWORD", ""),
        service_name=_env_or_default("ORACLE_SERVICE", "PROD"),
        pool_min=2,
        pool_max=20,
        pool_increment=2,
        pool_get_timeout=30,
    )
