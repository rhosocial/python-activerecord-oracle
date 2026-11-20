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
from typing import Any, Callable, Dict, Optional, Type, Union, get_origin, get_args

from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter


class OracleBooleanAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python bool and Oracle booleans.

    Oracle stores booleans either as ``NUMBER(1)`` (the classic portable
    carrier) or as the native ``BOOLEAN`` type (23ai+).  The target decides:
    ``int`` binds 0/1, ``bool`` binds a real boolean so a native ``BOOLEAN``
    column never goes through the numeric carrier.
    """

    def __init__(self):
        super().__init__()
        self.register_type(bool, int)
        self.register_type(bool, bool)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if target_type is bool:
            return bool(value)
        return 1 if bool(value) else 0

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return bool(value)


class OracleDateTimeAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python datetime and Oracle TIMESTAMP."""

    def __init__(self):
        super().__init__()
        self.register_type(datetime, str)
        self.register_type(datetime, int)

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
        self.register_type(date, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return value

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, datetime):
            # Oracle DATE can return as datetime
            return value.date()
        return value


class OracleTimeAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python time and Oracle ``VARCHAR2(8)``.

    Oracle has no ``TIME`` column type; the dialect's generic time carrier is
    ``VARCHAR2(8)`` (see ``format_data_type_time``), which holds ``HH:MM:SS``.
    Sub-second precision cannot be stored there, so it is dropped on write
    rather than overflowing the column (ORA-12899).  A wider carrier can be
    declared explicitly when fractional seconds matter.
    """

    def __init__(self):
        super().__init__()
        self.register_type(time, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return value.strftime('%H:%M:%S')

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, str):
            try:
                return time.fromisoformat(value)
            except ValueError:
                pass
        return value


class OracleDecimalAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python numeric types and Oracle NUMBER.

    ``Decimal`` and ``float`` are both supported as the Python source type so
    that model fields declared ``balance: float`` can round-trip through
    ``NUMBER``/``NUMBER(10,2)`` columns.  ``float`` is the common ActiveRecord
    declaration for monetary values; without registering it the column
    has no adapter and the read path silently drops the value.
    """

    def __init__(self):
        super().__init__()
        self.register_type(Decimal, float)
        self.register_type(float, float)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return float(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        return value


class OracleJSONAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python dict/list and Oracle JSON storage.

    The optional ``default`` and ``object_hook`` mirror their ``json.dumps`` /
    ``json.loads`` namesakes.  Leaving both as ``None`` keeps the historical
    behaviour byte-for-byte identical.  Oracle may hand back a native JSON
    value that ``oracledb`` already decoded to ``dict``/``list``; ``object_hook``
    is then run over that decoded value by re-encoding and decoding it, so the
    hook sees every object just as it would through ``json.loads``.
    """

    def __init__(
        self,
        default: Optional[Callable[[Any], Any]] = None,
        object_hook: Optional[Callable[[dict], Any]] = None,
    ):
        super().__init__()
        self.default = default
        self.object_hook = object_hook
        self.register_type(dict, str)
        self.register_type(list, str)

    def _apply_object_hook(self, value: Any) -> Any:
        """Run ``object_hook`` over an already-decoded JSON value."""
        if self.object_hook is None:
            return value
        return json.loads(json.dumps(value, default=self.default), object_hook=self.object_hook)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return json.dumps(value, default=self.default)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if hasattr(value, 'read'):
            value = value.read()
        if isinstance(value, str):
            try:
                parsed = json.loads(value, object_hook=self.object_hook)
                # If the model declares the column as a string field, keep
                # the JSON string instead of returning a parsed object;
                # other backends (and ``JsonUser.preferences`` tests in the
                # testsuite) expect the raw string when the field is ``str``.
                if target_type is str:
                    return value
                return parsed
            except (json.JSONDecodeError, ValueError):
                pass
        # Oracle 21c+ might return native Oracle JSON object
        if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
            try:
                native = dict(value) if isinstance(value, dict) else list(value)
                if target_type is str:
                    # Serialise back so the consumer still sees a JSON string
                    return json.dumps(native, ensure_ascii=False, default=self.default)
                return self._apply_object_hook(native)
            except (TypeError, ValueError):
                pass
        return value


class OracleBytesAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between Python bytes and Oracle BLOB."""

    def __init__(self):
        super().__init__()
        self.register_type(bytes, bytes)

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
    """Adapter for Oracle INTERVAL types (YEAR TO MONTH, DAY TO SECOND).

    The Python surface is the two wrapper dataclasses plus ``timedelta`` for
    ``DAY TO SECOND``.  Reads also accept the driver's own values: oracledb
    returns ``oracledb.IntervalYM`` for ``YEAR TO MONTH`` and ``datetime.
    timedelta`` for ``DAY TO SECOND``; both are converted to the declared
    Python type instead of passing the driver object through (which would fail
    pydantic validation).

    ``YEAR TO MONTH`` is deliberately **not** converted to ``timedelta``: a
    month's length is not a constant, so a timedelta cannot carry it honestly.
    Declare ``IntervalYearToMonth`` for that column.
    """

    def __init__(self):
        super().__init__()
        # Import here to avoid circular imports
        from datetime import timedelta

        from .type_values import IntervalDayToSecond, IntervalYearToMonth

        self.IntervalYearToMonth = IntervalYearToMonth
        self.IntervalDayToSecond = IntervalDayToSecond
        self.register_type(IntervalYearToMonth, str)
        self.register_type(IntervalDayToSecond, str)
        self.register_type(timedelta, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        from datetime import timedelta

        if isinstance(value, timedelta):
            return str(self.IntervalDayToSecond.from_timedelta(value))
        return str(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        from datetime import timedelta

        if isinstance(value, str):
            text = value.strip()
            # DAY TO SECOND is 'D HH:MM:SS[.ffffff]' (a lone time part is
            # possible); YEAR TO MONTH is '[-]YY-MM'.
            if ':' in text or ' ' in text:
                parsed = self.IntervalDayToSecond.from_string(text)
            elif '-' in text:
                parsed = self.IntervalYearToMonth.from_string(text)
            else:
                raise ValueError(f"Unrecognised Oracle interval text: {text!r}")
            return self._coerce(parsed, target_type)

        if isinstance(value, timedelta):
            if target_type is timedelta:
                return value
            if target_type is self.IntervalDayToSecond:
                return self.IntervalDayToSecond.from_timedelta(value)
            if target_type is self.IntervalYearToMonth:
                raise ValueError(
                    "a DAY TO SECOND interval cannot be expressed as YEAR TO MONTH"
                )
            return value

        # oracledb.IntervalYM(years=..., months=...) for YEAR TO MONTH.
        if hasattr(value, 'years') and hasattr(value, 'months'):
            parsed_ym = self.IntervalYearToMonth(
                years=int(value.years), months=int(value.months)
            )
            if target_type is timedelta:
                raise ValueError(
                    "a YEAR TO MONTH interval cannot be expressed as timedelta: "
                    "months have no fixed length"
                )
            return parsed_ym

        return value

    @staticmethod
    def _coerce(parsed: Any, target_type: Type) -> Any:
        """Return the parsed interval in the shape the field declared."""
        from datetime import timedelta

        if target_type is timedelta:
            if not hasattr(parsed, 'to_timedelta'):
                raise ValueError(
                    "a YEAR TO MONTH interval cannot be expressed as timedelta: "
                    "months have no fixed length"
                )
            return parsed.to_timedelta()
        return parsed


class OracleRowIDAdapter(BaseSQLTypeAdapter):
    """Adapter for Oracle ROWID/UROWID types.

    oracledb returns both as plain ``str``.  The read path therefore has to
    choose the wrapper from what the field declares: ``OracleRowID`` for an
    ``ROWID`` column, ``OracleURowID`` for ``UROWID``.  Only when the target
    is not one of those wrappers does it fall back to shape detection.
    """

    def __init__(self):
        super().__init__()
        from .type_values import OracleRowID, OracleURowID
        self.OracleRowID = OracleRowID
        self.OracleURowID = OracleURowID
        self.register_type(OracleRowID, str)
        self.register_type(OracleURowID, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if hasattr(value, 'value'):
            return value.value
        return str(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if not isinstance(value, str):
            return value
        if target_type is self.OracleURowID:
            return self.OracleURowID(value)
        if target_type is self.OracleRowID:
            try:
                return self.OracleRowID(value)
            except ValueError:
                return self.OracleURowID(value)
        if len(value) == 18:
            try:
                return self.OracleRowID(value)
            except ValueError:
                return self.OracleURowID(value)
        return self.OracleURowID(value)


class OracleXMLAdapter(BaseSQLTypeAdapter):
    """Adapter for Oracle XMLType."""

    def __init__(self):
        super().__init__()
        from .type_values import OracleXMLType
        self.OracleXMLType = OracleXMLType
        self.register_type(OracleXMLType, str)

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
        from .type_values import SDOGeometry, SDOPoint
        self.SDOGeometry = SDOGeometry
        self.SDOPoint = SDOPoint
        self.register_type(SDOGeometry, str)

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
        from .type_values import OracleVector
        self.OracleVector = OracleVector
        self.register_type(OracleVector, str)

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
        self.register_type(uuid.UUID, str)
        self.register_type(uuid.UUID, bytes)

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
        self.register_type(Enum, str)
        self.register_type(Enum, object)

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

    ``oracledb`` returns Oracle 21c+ native ``JSON`` columns as
    ``oracledb.JSON`` wrapper objects whose ``repr`` is the JSON-encoded
    text.  For ``str`` fields we serialise that wrapper back to a regular
    Python string so consumers that declared ``preferences: Optional[str]``
    (and the testsuite's ``JsonUser`` fixture) see the raw JSON string
    rather than a ``list``/``dict`` that pydantic will reject.
    """

    def __init__(self):
        super().__init__()
        self.register_type(str, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        return value

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        # ``oracledb`` exposes native ``JSON`` columns via ``oracledb.JSON``;
        # iterating the wrapper yields key/value pairs as for a dict, but the
        # testsuite expects a string for ``Optional[str]`` JSON fields.
        if value is not None and target_type is str and isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
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

# Neutral value adapters live in their own module (this file stays a single
# backend-native adapter module) but are re-exported here so callers have one
# import site.
from .neutral_adapters import (  # noqa: E402  (re-export after the native set)
    OracleNeutralBitStringAdapter,  # noqa: F401
    OracleNeutralGeometryAdapter,  # noqa: F401
    OracleNeutralMacAddressAdapter,  # noqa: F401
    OracleNeutralRangeAdapter,  # noqa: F401
    OracleNeutralVectorAdapter,  # noqa: F401
    neutral_adapters,  # noqa: F401
)
