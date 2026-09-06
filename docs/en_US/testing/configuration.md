# Test Configuration

## Overview

This section describes how to configure the testing environment for the Oracle backend.

For general testing strategies (DummyBackend, SQLite integration testing), see the [Core Backend Testing Guide](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/testing/backend_testing.md).

## End-to-End Testing with Oracle Backend

For complete Oracle behavior testing, use the Oracle backend:

```python
import os
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig


class User(ActiveRecord):
    name: str
    email: str

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def table_name(cls) -> str:
        return 'users'


# Read configuration from environment variables
config = OracleConnectionConfig(
    host=os.environ.get('ORACLE_HOST', 'localhost'),
    port=int(os.environ.get('ORACLE_PORT', 1521)),
    database=os.environ.get('ORACLE_DATABASE', 'ORCLPDB1'),
    username=os.environ.get('ORACLE_USER', 'system'),
    password=os.environ.get('ORACLE_PASSWORD', ''),
)
User.configure(config, OracleBackend)
```

## Test Fixtures

```python
import pytest
from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig


@pytest.fixture
def oracle_config():
    return OracleConnectionConfig(
        host='localhost',
        port=1521,
        database='ORCLPDB1',
        username='system',
        password='password',
    )


@pytest.fixture
def oracle_backend(oracle_config):
    backend = OracleBackend(connection_config=oracle_config)
    backend.connect()
    yield backend
    backend.disconnect()


def test_connection(oracle_backend):
    version = oracle_backend.get_server_version()
    assert version is not None
```

## Oracle-Specific Test Considerations

- **DDL auto-commits**: In Oracle, DDL statements implicitly commit the current transaction. Clean up test data with `DELETE`/`TRUNCATE` rather than relying on transaction rollback to undo DDL.
- **Empty string = NULL**: Oracle treats empty strings as `NULL`. Tests asserting on empty-string behavior need to account for this.
- **Sequences**: Auto-increment uses sequences; tests that insert many rows should be aware of `NEXTVAL`/`CURRVAL` semantics.
- **Version detection**: `get_server_version()` queries `PRODUCT_COMPONENT_VERSION` and defaults to `(19, 0, 0)` if it cannot be determined.

💡 *AI Prompt:* "What is the difference between unit tests, integration tests, and end-to-end tests?"

