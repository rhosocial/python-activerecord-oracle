# type_values/interval.py
"""
Oracle INTERVAL type definitions.

Oracle supports two interval types:
- INTERVAL YEAR TO MONTH: For year and month intervals
- INTERVAL DAY TO SECOND: For day, hour, minute, second intervals

These types are useful for datetime arithmetic and represent
durations between two datetime values.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Tuple

from .._scan import read_digits


def _parse_year_to_month(text: str) -> Optional[Tuple[int, str, str]]:
    """Parse ``[+-]?digits-digits`` exactly.

    Returns the sign, the year digits and the month digits, or ``None`` when the
    whole string is not that shape.
    """
    index = 0
    sign = 1
    if text.startswith("+"):
        index = 1
    elif text.startswith("-"):
        sign = -1
        index = 1
    years, index = read_digits(text, index)
    if not years or index >= len(text) or text[index] != "-":
        return None
    months, index = read_digits(text, index + 1)
    if not months or index != len(text):
        return None
    return sign, years, months


def _parse_day_to_second_clock(text: str) -> Optional[Tuple[str, str, str, str]]:
    """Read ``HH:MM:SS[.fraction]`` from the start of *text*.

    This is a prefix parse: whatever follows the clock face is ignored, and a
    trailing dot without digits is not a fraction. Returns the hour, minute,
    second and fraction digit strings, or ``None`` when no clock face starts
    *text*.
    """
    hours, index = read_digits(text, 0)
    if not hours or index >= len(text) or text[index] != ":":
        return None
    minutes, index = read_digits(text, index + 1)
    if not minutes or index >= len(text) or text[index] != ":":
        return None
    seconds, index = read_digits(text, index + 1)
    if not seconds:
        return None
    fraction = ""
    if index < len(text) and text[index] == ".":
        fraction, _ = read_digits(text, index + 1)
    return hours, minutes, seconds, fraction


@dataclass
class IntervalYearToMonth:
    """Oracle INTERVAL YEAR TO MONTH type.
    
    Represents a period of time in years and months.
    Useful for date arithmetic with year/month granularity.
    
    Examples:
        >>> interval = IntervalYearToMonth(years=1, months=3)
        >>> str(interval)
        '01-03'
        >>> interval.to_iso8601()
        'P1Y3M'
    
    Attributes:
        years: Number of years (can be negative)
        months: Number of months (must be -11 to 11)
    """
    years: int = 0
    months: int = 0
    
    def __post_init__(self):
        if self.months < -11 or self.months > 11:
            raise ValueError(f"Months must be between -11 and 11, got {self.months}")
    
    def __str__(self) -> str:
        sign = '-' if self.years < 0 or (self.years == 0 and self.months < 0) else ''
        return f"{sign}{abs(self.years):02d}-{abs(self.months):02d}"
    
    def __repr__(self) -> str:
        return f"IntervalYearToMonth(years={self.years}, months={self.months})"
    
    def __neg__(self) -> 'IntervalYearToMonth':
        return IntervalYearToMonth(years=-self.years, months=-self.months)
    
    def __add__(self, other: 'IntervalYearToMonth') -> 'IntervalYearToMonth':
        total_months = self.total_months() + other.total_months()
        years = total_months // 12
        months = total_months % 12
        return IntervalYearToMonth(years=years, months=months)
    
    def __sub__(self, other: 'IntervalYearToMonth') -> 'IntervalYearToMonth':
        return self + (-other)
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IntervalYearToMonth):
            return NotImplemented
        return self.total_months() == other.total_months()
    
    def __lt__(self, other: 'IntervalYearToMonth') -> bool:
        return self.total_months() < other.total_months()
    
    def __le__(self, other: 'IntervalYearToMonth') -> bool:
        return self.total_months() <= other.total_months()
    
    def to_iso8601(self) -> str:
        """Convert to ISO 8601 duration format.
        
        Returns:
            ISO 8601 duration string (e.g., 'P1Y3M')
        """
        total_months = abs(self.years) * 12 + abs(self.months)
        sign = '-' if self.years < 0 or (self.years == 0 and self.months < 0) else ''
        return f"{sign}P{total_months // 12}Y{total_months % 12}M"
    
    def total_months(self) -> int:
        """Return total number of months.
        
        Returns:
            Total months as integer
        """
        return self.years * 12 + self.months
    
    @classmethod
    def from_string(cls, s: str) -> 'IntervalYearToMonth':
        """Parse Oracle interval string format.
        
        Args:
            s: Oracle interval string (e.g., '+01-03', '-02-06')
            
        Returns:
            New IntervalYearToMonth instance
            
        Raises:
            ValueError: If string format is invalid
        """
        s = s.strip()
        parsed = _parse_year_to_month(s)
        if parsed is None:
            raise ValueError(f"Invalid INTERVAL YEAR TO MONTH format: {s}")
        sign, years, months = parsed
        return cls(
            years=sign * int(years),
            months=sign * int(months)
        )
    
    @classmethod
    def from_months(cls, total_months: int) -> 'IntervalYearToMonth':
        """Create from total months.
        
        Args:
            total_months: Total number of months
            
        Returns:
            New IntervalYearToMonth instance
        """
        years = total_months // 12
        months = total_months % 12
        return cls(years=years, months=months)


@dataclass
class IntervalDayToSecond:
    """Oracle INTERVAL DAY TO SECOND type.
    
    Represents a period of time in days, hours, minutes, seconds,
    and fractional seconds. Useful for high-precision datetime arithmetic.
    
    Examples:
        >>> interval = IntervalDayToSecond(days=5, hours=12, minutes=30, seconds=45)
        >>> str(interval)
        '5 12:30:45'
        >>> td = interval.to_timedelta()
    
    Attributes:
        days: Number of days (can be negative)
        hours: Number of hours (must be -23 to 23)
        minutes: Number of minutes (must be -59 to 59)
        seconds: Number of seconds (must be -59 to 59)
        fractional_seconds: Fractional seconds (microseconds, 0-999999)
    """
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0
    fractional_seconds: float = 0.0
    
    def __post_init__(self):
        if self.hours < -23 or self.hours > 23:
            raise ValueError(f"Hours must be between -23 and 23, got {self.hours}")
        if self.minutes < -59 or self.minutes > 59:
            raise ValueError(f"Minutes must be between -59 and 59, got {self.minutes}")
        if self.seconds < -59 or self.seconds > 59:
            raise ValueError(f"Seconds must be between -59 and 59, got {self.seconds}")
    
    def __str__(self) -> str:
        sign = '-' if self.days < 0 else ''
        frac = f".{abs(int(self.fractional_seconds)):06d}" if self.fractional_seconds else ''
        return f"{sign}{abs(self.days)} {abs(self.hours):02d}:{abs(self.minutes):02d}:{abs(self.seconds):02d}{frac}"
    
    def __repr__(self) -> str:
        return (f"IntervalDayToSecond(days={self.days}, hours={self.hours}, "
                f"minutes={self.minutes}, seconds={self.seconds}, "
                f"fractional_seconds={self.fractional_seconds})")
    
    def __neg__(self) -> 'IntervalDayToSecond':
        return IntervalDayToSecond(
            days=-self.days,
            hours=-self.hours,
            minutes=-self.minutes,
            seconds=-self.seconds,
            fractional_seconds=-self.fractional_seconds
        )
    
    def to_timedelta(self) -> timedelta:
        """Convert to Python timedelta.
        
        Returns:
            Python timedelta object
        """
        total_seconds = (
            self.days * 86400 +
            self.hours * 3600 +
            self.minutes * 60 +
            self.seconds +
            self.fractional_seconds / 1000000
        )
        return timedelta(seconds=total_seconds)
    
    def total_seconds(self) -> float:
        """Return total seconds.
        
        Returns:
            Total seconds as float
        """
        return self.to_timedelta().total_seconds()
    
    @classmethod
    def from_timedelta(cls, td: timedelta) -> 'IntervalDayToSecond':
        """Create from Python timedelta.
        
        Args:
            td: Python timedelta object
            
        Returns:
            New IntervalDayToSecond instance
        """
        total_seconds = td.total_seconds()
        days = int(total_seconds // 86400)
        remaining = total_seconds % 86400
        hours = int(remaining // 3600)
        remaining = remaining % 3600
        minutes = int(remaining // 60)
        seconds = int(remaining % 60)
        fractional = (remaining % 1) * 1000000
        return cls(
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            fractional_seconds=fractional
        )
    
    @classmethod
    def from_string(cls, s: str) -> 'IntervalDayToSecond':
        """Parse Oracle interval string format.
        
        Args:
            s: Oracle interval string (e.g., '5 12:30:45.123456')
            
        Returns:
            New IntervalDayToSecond instance
            
        Raises:
            ValueError: If string format is invalid
        """
        s = s.strip()
        
        # Handle negative sign
        sign = 1
        if s.startswith('-'):
            sign = -1
            s = s[1:]
        
        # Split days and time
        parts = s.split()
        if len(parts) == 2:
            days = sign * int(parts[0])
            time_part = parts[1]
        else:
            days = 0
            time_part = parts[0] if parts else s
        
        # Parse time part
        time_values = _parse_day_to_second_clock(time_part)
        if time_values is None:
            raise ValueError(f"Invalid INTERVAL DAY TO SECOND format: {s}")
        hours_text, minutes_text, seconds_text, fraction_text = time_values
        
        hours = sign * int(hours_text)
        minutes = sign * int(minutes_text)
        seconds = sign * int(seconds_text)
        frac = sign * int(fraction_text or '0')
        
        return cls(
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            fractional_seconds=frac
        )
    
    @classmethod
    def from_seconds(cls, total_seconds: float) -> 'IntervalDayToSecond':
        """Create from total seconds.
        
        Args:
            total_seconds: Total number of seconds
            
        Returns:
            New IntervalDayToSecond instance
        """
        return cls.from_timedelta(timedelta(seconds=total_seconds))
