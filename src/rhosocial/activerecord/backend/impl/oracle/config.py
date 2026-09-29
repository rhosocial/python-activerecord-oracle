# src/rhosocial/activerecord/backend/impl/oracle/config.py
"""Oracle-specific connection configuration

This module provides Oracle-specific connection configuration classes that extend
the base ConnectionConfig with Oracle-specific parameters and functionality.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    import ssl

from rhosocial.activerecord.backend.config import (
    ConnectionConfig,
    ConnectionPoolMixin,
    SSLMixin,
    TimezoneMixin,
    VersionMixin,
    LoggingMixin
)

#: Values of the generic ``ssl_mode`` field that select Oracle's TCPS protocol.
SSL_MODES_REQUIRING_TLS = frozenset({"require", "verify-ca", "verify-full", "tcps", "enable", "enabled"})

#: Values of the generic ``ssl_mode`` field that explicitly select plain TCP.
SSL_MODES_DISABLING_TLS = frozenset({"disable", "disabled", "off", "none", "tcp"})


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

    # Oracle-native TLS options, named after the oracledb connection keywords.
    # ``None`` means "not specified", which leaves python-oracledb's own default
    # in place (notably ``ssl_server_dn_match``, which defaults to True there
    # but to False in the generic SSLMixin).
    protocol: Optional[str] = None  # 'tcp' or 'tcps'
    wallet_location: Optional[str] = None
    wallet_password: Optional[str] = None
    ssl_server_dn_match: Optional[bool] = None
    ssl_server_cert_dn: Optional[str] = None

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
            'protocol': self.protocol,
            'wallet_location': self.wallet_location,
            'wallet_password': self.wallet_password,
            'ssl_server_dn_match': self.ssl_server_dn_match,
            'ssl_server_cert_dn': self.ssl_server_cert_dn,
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

    def _build_ssl_context(self) -> Optional["ssl.SSLContext"]:
        """Build an :class:`ssl.SSLContext` from the generic SSL fields.

        python-oracledb has no ``ssl_ca``/``ssl_cert``/``ssl_key`` connection
        keywords: thin mode accepts TLS material either through a wallet or
        through a pre-built ``ssl_context``. This adapts the generic
        :class:`~rhosocial.activerecord.backend.config.SSLMixin` fields onto the
        latter so a PEM based setup actually reaches the driver.

        Returns:
            The configured context, or None when no PEM material was supplied.
        """
        if not (self.ssl_ca or self.ssl_cert or self.ssl_key):
            return None

        if self.ssl_cert and not self.ssl_key:
            raise ValueError("ssl_cert requires ssl_key for Oracle connections")

        import ssl

        if self.ssl_verify_cert:
            context = ssl.create_default_context(cafile=self.ssl_ca)
        else:
            context = ssl.create_default_context()
            if self.ssl_ca:
                context.load_verify_locations(cafile=self.ssl_ca)
            # check_hostname must be cleared before downgrading verify_mode.
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        if self.ssl_cert:
            context.load_cert_chain(certfile=self.ssl_cert, keyfile=self.ssl_key)
        if self.ssl_ciphers:
            context.set_ciphers(self.ssl_ciphers)

        return context

    def get_ssl_connection_params(self) -> Dict[str, Any]:
        """Get the TLS keywords that should be forwarded to ``oracledb.connect``.

        Returns:
            Dictionary of oracledb connection keywords. Empty when the
            configuration asks for no TLS at all, in which case python-oracledb
            keeps its own defaults.
        """
        params: Dict[str, Any] = {}

        if self.wallet_location:
            params['wallet_location'] = self.wallet_location
        if self.wallet_password:
            params['wallet_password'] = self.wallet_password
        if self.ssl_server_cert_dn:
            params['ssl_server_cert_dn'] = self.ssl_server_cert_dn
        if self.ssl_server_dn_match is not None:
            params['ssl_server_dn_match'] = self.ssl_server_dn_match

        context = self._build_ssl_context()
        if context is not None:
            params['ssl_context'] = context

        if self.protocol:
            params['protocol'] = self.protocol
        else:
            ssl_mode = (self.ssl_mode or '').lower()
            if self.wallet_location or context is not None or ssl_mode in SSL_MODES_REQUIRING_TLS:
                params['protocol'] = 'tcps'
            elif ssl_mode in SSL_MODES_DISABLING_TLS:
                params['protocol'] = 'tcp'

        return params
