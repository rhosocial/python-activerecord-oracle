# Date/Time Expressions

The Oracle backend renders the framework's temporal expression nodes through
`OracleDateTimeMixin` (`mixins/datetime_op.py`). This page covers the
`seconds_between` operation, whose Oracle spelling is not the PostgreSQL one.

## `seconds_between`

```python
from rhosocial.activerecord.backend.expression import seconds_between

expr = seconds_between(dialect, started_at, ended_at).as_("elapsed")
sql, params = expr.to_sql()
```

`TimestampColumn.seconds_between(ended_at)` is the same operation on the
column surface, and both return a `SecondsBetweenExpression` (a
`NumericValue`). The answer is **signed** and **fractional**: it is
`end - start` in seconds, so an end earlier than the start is negative and
equal instants are exactly zero.

## Oracle spelling

Two DATE values subtract to a `NUMBER` of days in Oracle; a subtraction
involving a `TIMESTAMP` yields an `INTERVAL DAY TO SECOND`, and an interval
cannot be multiplied into a number. The backend therefore casts both operands
to `TIMESTAMP(9)` and sums the interval components:

```sql
(EXTRACT(DAY FROM (CAST(end AS TIMESTAMP(9)) - CAST(start AS TIMESTAMP(9)))) * 86400
 + EXTRACT(HOUR FROM (CAST(end AS TIMESTAMP(9)) - CAST(start AS TIMESTAMP(9)))) * 3600
 + EXTRACT(MINUTE FROM (CAST(end AS TIMESTAMP(9)) - CAST(start AS TIMESTAMP(9)))) * 60
 + EXTRACT(SECOND FROM (CAST(end AS TIMESTAMP(9)) - CAST(start AS TIMESTAMP(9))))
```

The cast is what makes the rendering work for both `DATE` and `TIMESTAMP`
columns, and `TIMESTAMP(9)` keeps the nanosecond place that a bare
`TIMESTAMP` cast rounds to microseconds. `EXTRACT(SECOND FROM ...)` carries
the fractional part, and on a negative interval every component carries the
sign, so the sum is the signed duration (measured on Oracle 23.26.1).

`end` is written before `start` in each of the four extractions, so its bind
parameters come first and are repeated once per extraction. Bind order
follows the written SQL, not the constructor's argument order.

## Limitations

- **`TIMESTAMP WITH TIME ZONE`**: `CAST(... AS TIMESTAMP(9))` drops the zone
  and keeps the wall-clock reading, so two zoned values are compared after
  that truncation rather than as instants. Normalize with
  `SYS_EXTRACT_UTC(...)` before calling `seconds_between` when the zone
  matters.
- **Fractional precision**: Oracle's interval stores at most nine fractional
  second digits. A value finer than a nanosecond cannot be represented.
- **Session time zone**: evaluating `CURRENT_TIMESTAMP` or a zoned column
  follows the session's time zone; the expression does not pin one.
- **DATE precision**: a `DATE` operand stores whole seconds, so its
  contribution is integral even though the result type is fractional.
