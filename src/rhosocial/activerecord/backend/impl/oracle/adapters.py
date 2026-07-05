# src/rhosocial/activerecord/backend/impl/oracle/adapters.py
"""
Oracle-specific type adapters for converting between Python and Oracle types.

Oracle has some unique type handling requirements:
- Boolean: Oracle uses NUMBER(1) for boolean (0/1)
- DateTime: Oracle DATE includes time, TIMESTAMP for higher precision
- JSON: Oracle stores JSON as VARCHAR2 or CLOB (pre-21c) or native JSON type (21c+)
- BLOB: Binary data stored as BLOB
- INTERVAL: YEAR TO MONTH and DAY TO SECOND types
- ROWID: Extended ROWID (18-char) and UROWID
- XMLType: Native XML storage
- SDO_GEOMETRY: Spatial data type
- VECTOR: AI/ML vector type (23ai+)
"""

import json
import uuid
from datetime import datetime, date, time
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Optional, Type, Union, get_origin, get_args

from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter


class OracleBooleanAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python bool and Oracle NUMBER(1)."""

    def __init__(self):
        super().__init__()
        self._register_type(bool, int)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return 1 if bool(value) else 0

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return bool(value)


class OracleDateTimeAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python datetime and Oracle TIMESTAMP."""

    def __init__(self):
        super().__init__()
        self._register_type(datetime, str)
        self._register_type(datetime, int)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        # Oracle thin client handles datetime objects directly
        return value

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, (int, float, Decimal)):
            return datetime.fromtimestamp(float(value))
        if isinstance(value, datetime):
            # Oracle TIMESTAMP WITH TIME ZONE returns datetime without tzinfo
            # If the datetime has no timezone but the field expects one,
            # we assume UTC (since TIMESTAMP WITH TIME ZONE stores in UTC)
            if value.tzinfo is None:
                # Add UTC timezone for consistency
                from datetime import timezone
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, str):
            # Handle Oracle's default date format: 'DD-MON-YY' or 'DD-MON-YYYY HH24:MI:SS'
            # Example: '06-APR-26' or '06-APR-2026 14:33:36'
            try:
                # Try ISO format first
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                pass

            # Try Oracle format
            import re
            oracle_pattern = r'(\d{2})-([A-Z]{3})-(\d{2,4})(?:\s+(\d{2}):(\d{2}):(\d{2}))?'
            match = re.match(oracle_pattern, value.upper())
            if match:
                day, month_str, year, hour, minute, second = match.groups()
                month_map = {
                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                }
                month = month_map.get(month_str, 1)
                year_int = int(year)
                # Handle 2-digit year
                if year_int < 100:
                    year_int += 2000 if year_int < 50 else 1900

                return datetime(
                    year_int, month, int(day),
                    int(hour) if hour else 0,
                    int(minute) if minute else 0,
                    int(second) if second else 0
                )
        return value


class OracleDateAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python date and Oracle DATE."""

    def __init__(self):
        super().__init__()
        self._register_type(date, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return value

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, datetime):
            # Oracle DATE can return as datetime
            return value.date()
        return value


class OracleTimeAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python time and Oracle INTERVAL or VARCHAR2."""

    def __init__(self):
        super().__init__()
        self._register_type(time, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        # Store as ISO format string for simplicity
        return value.isoformat()

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, str):
            try:
                return time.fromisoformat(value)
            except ValueError:
                pass
        return value


class OracleDecimalAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python Decimal and Oracle NUMBER."""

    def __init__(self):
        super().__init__()
        self._register_type(Decimal, float)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return float(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        return value


class OracleJSONAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python dict/list and Oracle JSON storage."""

    def __init__(self):
        super().__init__()
        self._register_type(dict, str)
        self._register_type(list, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return json.dumps(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if hasattr(value, 'read'):
            value = value.read()
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                pass
        # Oracle 21c+ might return native Oracle JSON object
        if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
            try:
                return dict(value) if isinstance(value, dict) else list(value)
            except (TypeError, ValueError):
                pass
        return value


class OracleBytesAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python bytes and Oracle BLOB."""

    def __init__(self):
        super().__init__()
        self._register_type(bytes, bytes)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return value

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, bytes):
            return value
        # Oracle might return oracledb.LOB
        if hasattr(value, 'read'):
            return value.read()
        return value


class OracleIntervalAdapter(BaseSQLTypeAdapter):
    """Adapter for Oracle INTERVAL types (YEAR TO MONTH, DAY TO SECOND)."""

    def __init__(self):
        super().__init__()
        # Import here to avoid circular imports
        from .types import IntervalYearToMonth, IntervalDayToSecond
        self.IntervalYearToMonth = IntervalYearToMonth
        self.IntervalDayToSecond = IntervalDayToSecond
        self._register_type(IntervalYearToMonth, str)
        self._register_type(IntervalDayToSecond, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return str(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, str):
            # Determine if it's YEAR TO MONTH or DAY TO SECOND
            if ' ' in value or ':' in value:
                # DAY TO SECOND format: '5 12:30:45'
                return self.IntervalDayToSecond.from_string(value)
            elif '-' in value:
                # YEAR TO MONTH format: '01-06'
                return self.IntervalYearToMonth.from_string(value)
        return value


class OracleRowIDAdapter(BaseSQLTypeAdapter):
    """Adapter for Oracle ROWID/UROWID types."""

    def __init__(self):
        super().__init__()
        from .types import OracleRowID, OracleURowID
        self.OracleRowID = OracleRowID
        self.OracleURowID = OracleURowID
        self._register_type(OracleRowID, str)
        self._register_type(OracleURowID, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if hasattr(value, 'value'):
            return value.value
        return str(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, str):
            if len(value) == 18:
                try:
                    return self.OracleRowID(value)
                except ValueError:
                    return self.OracleURowID(value)
            return self.OracleURowID(value)
        return value


class OracleXMLAdapter(BaseSQLTypeAdapter):
    """Adapter for Oracle XMLType."""

    def __init__(self):
        super().__init__()
        from .types import OracleXMLType
        self.OracleXMLType = OracleXMLType
        self._register_type(OracleXMLType, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if hasattr(value, 'content'):
            return value.content
        return str(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, str):
            return self.OracleXMLType(value)
        return value


class OracleSDOGeometryAdapter(BaseSQLTypeAdapter):
    """Adapter for Oracle SDO_GEOMETRY spatial type."""

    def __init__(self):
        super().__init__()
        from .types import SDOGeometry, SDOPoint
        self.SDOGeometry = SDOGeometry
        self.SDOPoint = SDOPoint
        self._register_type(SDOGeometry, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if hasattr(value, 'to_constructor_sql'):
            return value.to_constructor_sql()
        return str(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, dict):
            return self.SDOGeometry.from_dict(value)
        return value


class OracleVectorAdapter(BaseSQLTypeAdapter):
    """Adapter for Oracle VECTOR type (23ai+)."""

    def __init__(self):
        super().__init__()
        from .types import OracleVector
        self.OracleVector = OracleVector
        self._register_type(OracleVector, str)
        self._register_type(list, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if hasattr(value, 'to_string'):
            return value.to_string()
        elif isinstance(value, list):
            return '[' + ', '.join(str(v) for v in value) + ']'
        return str(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, str):
            return self.OracleVector.from_string(value)
        elif isinstance(value, list):
            return self.OracleVector.from_list(value)
        return value


class OracleUUIDAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python ``uuid.UUID`` and Oracle CHAR(36)/VARCHAR2(36).

    Oracle has no native UUID type. The conventional storage format is a 36-character
    hyphenated string in a CHAR(36) or VARCHAR2(36) column. Callers may also pass a
    ``storage_format='bytes'`` option to use RAW(16) instead (16-byte big-endian form,
    suitable for storing the UUID's integer value).
    """

    def __init__(self):
        super().__init__()
        self._register_type(uuid.UUID, str)
        self._register_type(uuid.UUID, bytes)

    def _do_to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]],
    ) -> Any:
        if not isinstance(value, uuid.UUID):
            value = uuid.UUID(str(value))
        if target_type is bytes:
            return value.bytes
        return str(value)

    def _do_from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]],
    ) -> Any:
        if isinstance(value, uuid.UUID):
            return value
        if isinstance(value, (bytes, bytearray)):
            return uuid.UUID(bytes=bytes(value))
        return uuid.UUID(str(value))

    def _on_none_from_database(self, target_type: Type, **kwargs) -> Any:
        return None


class OracleEnumAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python ``enum.Enum`` and Oracle VARCHAR2.

    Oracle has no native ENUM type. Enums are conventionally modeled as a
    VARCHAR2 column with a CHECK constraint listing the allowed values.

    Storage policy:
      - ``'name'``  (default): store ``enum_member.name`` (e.g. ``'ACTIVE'``)
      - ``'value'``:           store ``enum_member.value`` (must be str/int)

    On read, the adapter reconstructs the enum by name first, then by value.
    """

    def __init__(self, storage: str = 'name'):
        super().__init__()
        if storage not in ('name', 'value'):
            raise ValueError(
                f"OracleEnumAdapter storage must be 'name' or 'value', got {storage!r}"
            )
        self._storage = storage
        self._register_type(Enum, str)
        self._register_type(Enum, object)

    def _do_to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]],
    ) -> Any:
        if not isinstance(value, Enum):
            return value
        if target_type is int and isinstance(value.value, int):
            return value.value
        return value.name if self._storage == 'name' else value.value

    def _do_from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]],
    ) -> Any:
        if isinstance(value, Enum) or value is None:
            return value
        original_type = options.get('original_type') if options else target_type
        if not (isinstance(original_type, type) and issubclass(original_type, Enum)):
            return value
        try:
            return original_type[value]
        except KeyError:
            pass
        try:
            return original_type(value)
        except (ValueError, KeyError):
            return value

    def _on_none_from_database(self, target_type: Type, **kwargs) -> Any:
        return None


class OracleStringAdapter(BaseSQLTypeAdapter):
    """Adapter for Oracle string types.

    Oracle treats empty strings ('') as NULL. This adapter converts None back to ''
    for non-Optional str fields when reading from the database, preserving the
    original Python default value semantics.

    For Optional[str] fields, None is preserved as-is (genuine NULL).
    """

    def __init__(self):
        super().__init__()
        self._register_type(str, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return value

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return value

    def _on_none_from_database(self, target_type: Type, **kwargs) -> Any:
        original_type = kwargs.get('original_type', target_type)
        origin = get_origin(original_type)
        import types
        is_union = origin is Union or (hasattr(types, "UnionType") and origin is types.UnionType)
        if is_union:
            args = get_args(original_type)
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1 and non_none[0] is str:
                return None
        return ''


# List of all Oracle adapters for easy registration
oracle_adapters = [
    OracleBooleanAdapter,
    OracleDateTimeAdapter,
    OracleDateAdapter,
    OracleTimeAdapter,
    OracleDecimalAdapter,
    OracleJSONAdapter,
    OracleBytesAdapter,
    OracleStringAdapter,
    OracleUUIDAdapter,
    OracleEnumAdapter,
    OracleIntervalAdapter,
    OracleRowIDAdapter,
    OracleXMLAdapter,
    OracleSDOGeometryAdapter,
    OracleVectorAdapter,
]
