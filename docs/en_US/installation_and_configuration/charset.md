# Character Set / Encoding

## Overview

Oracle supports multiple character encodings. The default encoding is UTF-8.

## Configuration

```python
config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    database="ORCLPDB1",
    username="system",
    password="password",
    encoding="UTF-8",       # Database character set
    nencoding="UTF-16"      # National character set (optional)
)
```

## Common Encodings

| Encoding | Description |
|----------|-------------|
| `UTF-8` | Universal character set (recommended) |
| `AL32UTF8` | Oracle's UTF-8 implementation |
| `WE8ISO8859P1` | Western European |
| `JA16EUC` | Japanese EUC |

## Best Practices

- Always use `UTF-8` or `AL32UTF8` for new databases
- Ensure the client encoding matches the database encoding
- National character set (`nencoding`) is optional and only needed for `NCHAR`/`NVARCHAR2` columns

## See Also

- [Connection Configuration](configuration.md) — connection parameters
