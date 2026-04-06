# src/rhosocial/activerecord/backend/impl/oracle/config.py
"""Oracle-specific connection configuration

This module provides Oracle-specific connection configuration classes that extend
the base ConnectionConfig with Oracle-specific parameters and functionality.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any

from rhosocial.activerecord.backend.config import (
    ConnectionConfig,
    ConnectionPoolMixin,
    SSLMixin,
    TimezoneMixin,
    VersionMixin,
    LoggingMixin
)


@dataclass
class OracleConnectionConfig(
    ConnectionConfig,
    ConnectionPoolMixin,
    SSLMixin,
    TimezoneMixin,
    VersionMixin,
    LoggingMixin
):
    """Oracle connection configuration with Oracle-specific parameters.

    This class extends the base ConnectionConfig with Oracle-specific
    parameters and functionality including connection pooling, SSL,
    timezone handling, and logging options.

    Oracle-specific parameters:
    - service_name: Oracle service name (alternative to SID)
    - sid: Oracle SID (alternative to service_name)
    - dsn: Data Source Name (full connection string)
    - mode: Connection mode (e.g., SYSDBA, SYSOPER)
    - encoding: Character encoding (default: UTF-8)
    - nencoding: National character encoding
    - edition: Edition name for Edition-Based Redefinition
    """

    # Oracle-specific connection options
    service_name: Optional[str] = None
    sid: Optional[str] = None
    dsn: Optional[str] = None
    mode: Optional[str] = None
    encoding: str = "UTF-8"
    nencoding: Optional[str] = None
    edition: Optional[str] = None

    # Oracle-specific pool options (for oracledb)
    pool_min: Optional[int] = None
    pool_max: Optional[int] = None
    pool_increment: Optional[int] = None
    pool_get_timeout: Optional[int] = None

    # Oracle-specific session options
    stmtcachesize: int = 20
    prefetchrows: Optional[int] = None
    arraysize: int = 100

    # Oracle-specific flags
    threaded: bool = True
    events: bool = False

    # Default port for Oracle
    port: int = 1521

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary, including Oracle-specific parameters."""
        # Get base config
        config_dict = super().to_dict()

        # Add Oracle-specific parameters
        oracle_params = {
            'service_name': self.service_name,
            'sid': self.sid,
            'dsn': self.dsn,
            'mode': self.mode,
            'encoding': self.encoding,
            'nencoding': self.nencoding,
            'edition': self.edition,
            'pool_min': self.pool_min,
            'pool_max': self.pool_max,
            'pool_increment': self.pool_increment,
            'pool_get_timeout': self.pool_get_timeout,
            'stmtcachesize': self.stmtcachesize,
            'prefetchrows': self.prefetchrows,
            'arraysize': self.arraysize,
            'threaded': self.threaded,
            'events': self.events,
        }

        # Only include non-None values
        for key, value in oracle_params.items():
            if value is not None:
                config_dict[key] = value

        return config_dict

    def get_dsn(self) -> str:
        """Get Oracle DSN (Data Source Name) for connection.

        Returns:
            DSN string in format: host:port/service_name or host:port:sid
        """
        if self.dsn:
            return self.dsn

        if self.service_name:
            return f"{self.host}:{self.port}/{self.service_name}"
        elif self.sid:
            return f"{self.host}:{self.port}:{self.sid}"
        else:
            # Default to service_name same as database name
            return f"{self.host}:{self.port}/{self.database}"
