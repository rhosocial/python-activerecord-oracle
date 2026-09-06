# Supported Versions

## Oracle Database Version Support

| Oracle Version | Support Status | Notes |
|----------------|----------------|-------|
| 11g | ❌ End of Life | No longer supported by Oracle |
| 12c / 12.1 / 12.2 | ⚠️ Extended Support | Some features require 12c+ (e.g., FETCH FIRST) |
| 18c | ✅ Supported | Recommended baseline |
| 19c | ✅ Supported | Latest Long Term Support release |
| 21c | ✅ Supported | Adds native JSON type support |
| 23ai | ✅ Supported | Adds AI VECTOR type, JSON Relational Duality |

> **Important**: This backend is designed exclusively for Oracle databases. The dialect behavior is tightly coupled with Oracle-specific features (PL/SQL, sequences, DUAL, ROWNUM/FETCH FIRST). **Do not use this backend with other Oracle-compatible databases.**

⚠️ **Note**:

- Oracle 11g has reached End of Life; using it is not recommended
- Oracle 12c+ is required for ANSI `FETCH FIRST` / `OFFSET` pagination; earlier versions use `ROWNUM`
- Some features (native JSON, VECTOR) are only available in specific versions; refer to feature documentation

## Python Version Requirements

| Python Version | Support Status | Notes |
|---------------|----------------|-------|
| 3.8 | ✅ Supported | |
| 3.9 | ✅ Supported | |
| 3.10 | ✅ Supported | |
| 3.11 | ✅ Supported | |
| 3.12 | ✅ Supported | |
| 3.13 | ✅ Supported | Supports free-threaded build (3.13t) |
| 3.14 | ✅ Supported | Supports free-threaded build (3.14t) |

**Free-Threaded Python**: Starting from Python 3.13, a free-threaded (no-GIL) build is available as `python3.13t`, `python3.14t`, etc. This backend is compatible with free-threaded Python, though some threading-specific features may behave differently.

## Dependency Requirements

| Dependency | Version | Notes |
|-----------|---------|-------|
| rhosocial-activerecord | >=1.0.0 | Core library |
| oracledb | >=3.4.2 | Oracle driver (thin or thick mode) |

⚠️ **Important**: This backend only supports the `oracledb` driver. The async backend uses `oracledb` **thin mode** (no Oracle Instant Client required); **thick mode** requires Oracle Instant Client and provides additional OCI features.

## Connection Modes

| Mode | Oracle Client Required | Use Case |
|------|------------------------|----------|
| Thin (default) | No | TCP/IP direct connection, includes async support |
| Thick | Yes | OCI features, some Oracle-specific protocols |

💡 *AI Prompt:* "How does Oracle's thin mode differ from thick mode in the oracledb driver?"

