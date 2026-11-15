# tests/rhosocial/activerecord_oracle_test/feature/backend/adapters/native/conftest.py
"""Fixtures for the Oracle native-type tests.

The parent ``feature/backend/conftest.py`` already provides ``oracle_backend``
(a real connection to the directory's own schema user).  This conftest adds the
native tables and binds the models to that same connection.
"""

import pytest

import native_models
import native_support


@pytest.fixture(scope="function")
def native_db(oracle_backend):
    """A live backend whose native-type tables exist and are empty."""
    native_support.ensure_tables(oracle_backend)
    native_support.clear_rows(oracle_backend)
    native_models.bind_models(oracle_backend)
    try:
        yield oracle_backend
    finally:
        native_support.clear_rows(oracle_backend)
