# src/rhosocial/activerecord/backend/impl/oracle/collation.py
"""
Oracle collation names supported by the dialect whitelist.
"""

from enum import Enum
from typing import Optional, Tuple


class OracleCollation(Enum):
    """Common Oracle collations for expression-level COLLATE."""

    BINARY = "BINARY"
    BINARY_CI = "BINARY_CI"
    BINARY_AI = "BINARY_AI"


_ORACLE_COLLATIONS = {collation.value for collation in OracleCollation}
_ORACLE_COLLATE_VERSION = (12, 2, 0)


def validate_oracle_collation_name(
    name: str,
    version: Optional[Tuple[int, int, int]] = None,
) -> str:
    normalized = name.upper()
    if normalized not in _ORACLE_COLLATIONS:
        raise ValueError(f"Unsupported Oracle collation: {name!r}")
    if version is not None and version < _ORACLE_COLLATE_VERSION:
        raise ValueError("Oracle expression-level COLLATE requires Oracle 12.2+: " f"{name!r}")
    return normalized
