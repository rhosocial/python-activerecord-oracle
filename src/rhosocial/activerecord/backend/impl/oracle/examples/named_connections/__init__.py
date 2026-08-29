# src/rhosocial/activerecord/backend/impl/oracle/examples/named_connections/__init__.py
"""Named connection examples for Oracle backend.

This module provides example named connection configurations
that can be used with the named connection system.

Examples:
    >>> from rhosocial.activerecord.backend.impl.oracle.examples.named_connections import local_dev
    >>> config = local_dev()
"""

__all__ = ["local_dev", "local_dev_xepdb", "local_dev_no_password", "prod_db", "prod_db_ssl", "prod_pool"]

from rhosocial.activerecord.backend.impl.oracle.examples.named_connections.development import (
    local_dev,
    local_dev_xepdb,
    local_dev_no_password,
)
from rhosocial.activerecord.backend.impl.oracle.examples.named_connections.production import (
    prod_db,
    prod_db_ssl,
    prod_pool,
)
