# src/rhosocial/activerecord/backend/impl/oracle/adapters.py
"""
Oracle-specific type adapters for converting between Python and Oracle types.

Oracle has some unique type handling requirements:
- Boolean: Oracle uses NUMBER(1) for boolean (0/1)
- DateTime: Oracle DATE includes time, TIMESTAMP for higher precision
- JSON: Oracle stores JSON as VARCHAR2 or CLOB (pre-21c) or native JSON type (21c+)
- BLOB: Binary data stored as BLOB
"""

import json
from datetime import datetime, date, time
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple, Type, Set

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
