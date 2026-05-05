# Testing Guide - python-activerecord-oracle

> Oracle backend implementation for rhosocial-activerecord

## Project-Specific Information

| Item | Value |
|------|-------|
| **Python Version** | 3.9+ |
| **Database Driver** | oracledb |
| **Free-Threading Support** | ✅ Yes |

## Dependencies

```toml
dependencies = [
    "oracledb>=3.4.2",
]
```

## Quick Test Commands

```bash
cd /mnt/i/GitHubRepositories/rhosocial/python-activerecord-oracle
source .venv/bin/activate
export PYTHONPATH=src
pytest tests/ -v
```

## Key Differences from Core

- Uses Oracle-specific dialect in `src/rhosocial/activerecord/backend/impl/oracle/dialect.py`
- Schema files in `tests/rhosocial/activerecord_oracle_test/`
- Provider implementation in `tests/providers/`

## Reference

- [Core testing guide](../python-activerecord/.claude/testing.md)
- [Oracle backend development](../python-activerecord/.claude/backend_development.md)