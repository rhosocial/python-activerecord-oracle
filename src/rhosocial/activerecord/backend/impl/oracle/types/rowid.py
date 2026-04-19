# types/rowid.py
"""
Oracle ROWID type definitions.

ROWID is a pseudo-column that uniquely identifies a row in a table.
Oracle supports:
- Extended ROWID: 18-character base64 encoded string (Oracle 8i+)
- UROWID: Universal ROWID for accessing non-Oracle data

ROWID format: OOOOOO.FFF.BBBBBB.RRR
- OOOOOO: Data object number (6 characters)
- FFF: Tablespace-relative datafile number (3 characters)
- BBBBBB: Data block number (6 characters)
- RRR: Row number in block (3 characters)
"""

from dataclasses import dataclass
import re
from typing import Optional


@dataclass(frozen=True)
class OracleRowID:
    """Oracle extended ROWID (18-character format).
    
    The ROWID pseudo-column provides the physical address of a row.
    Extended ROWID format is used in Oracle 8i and later.
    
    Example:
        >>> rowid = OracleRowID("AAASdqAAEAAAAInAAA")
        >>> rowid.data_object_number
        'AAASdq'
    
    Attributes:
        value: The 18-character ROWID string
    """
    value: str
    
    def __post_init__(self):
        if not re.match(r'^[A-Za-z0-9+/]{18}$', self.value):
            raise ValueError(f"Invalid ROWID format: {self.value}")
    
    @property
    def data_object_number(self) -> str:
        """Return the data object number component.
        
        Identifies the segment (table, partition, or cluster).
        
        Returns:
            6-character data object number
        """
        return self.value[0:6]
    
    @property
    def file_number(self) -> str:
        """Return the file number component.
        
        Identifies the datafile within the tablespace.
        
        Returns:
            3-character file number
        """
        return self.value[6:9]
    
    @property
    def block_number(self) -> str:
        """Return the block number component.
        
        Identifies the block within the datafile.
        
        Returns:
            6-character block number
        """
        return self.value[9:15]
    
    @property
    def row_number(self) -> str:
        """Return the row number component.
        
        Identifies the row within the block.
        
        Returns:
            3-character row number
        """
        return self.value[15:18]
    
    def __str__(self) -> str:
        return self.value
    
    def __repr__(self) -> str:
        return f"OracleRowID('{self.value}')"
    
    def to_dict(self) -> dict:
        """Convert to dictionary representation.
        
        Returns:
            Dictionary with ROWID components
        """
        return {
            'value': self.value,
            'data_object_number': self.data_object_number,
            'file_number': self.file_number,
            'block_number': self.block_number,
            'row_number': self.row_number
        }


@dataclass(frozen=True)
class OracleURowID:
    """Oracle universal ROWID (UROWID).
    
    UROWID can represent:
    - Physical rowids of rows in Oracle tables
    - Logical rowids of rows in index-organized tables
    - Foreign rowids from non-Oracle databases via gateway
    
    Unlike OracleRowID, UROWID has variable length and format.
    
    Attributes:
        value: The UROWID string (variable length)
    """
    value: str
    
    def __post_init__(self):
        if not self.value:
            raise ValueError("UROWID cannot be empty")
    
    def __str__(self) -> str:
        return self.value
    
    def __repr__(self) -> str:
        return f"OracleURowID('{self.value}')"
    
    def __len__(self) -> int:
        return len(self.value)
    
    def is_logical(self) -> bool:
        """Check if this is a logical ROWID (for IOTs).
        
        Logical ROWIDs are used for index-organized tables.
        
        Returns:
            True if likely a logical ROWID
        """
        # Logical ROWIDs often contain '*' character
        return '*' in self.value
    
    def is_foreign(self) -> bool:
        """Check if this is a foreign ROWID.
        
        Foreign ROWIDs come from non-Oracle databases.
        
        Returns:
            True if likely a foreign ROWID
        """
        # Foreign ROWIDs have variable format, hard to detect
        # This is a heuristic based on length
        return len(self.value) > 18 or len(self.value) < 18


def parse_rowid(value: str) -> OracleRowID | OracleURowID:
    """Parse a ROWID string into appropriate type.
    
    Args:
        value: ROWID string to parse
        
    Returns:
        OracleRowID if 18-character extended format,
        OracleURowID otherwise
    """
    if len(value) == 18 and re.match(r'^[A-Za-z0-9+/]{18}$', value):
        return OracleRowID(value)
    return OracleURowID(value)
