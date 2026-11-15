# tests/rhosocial/activerecord_oracle_test/feature/backend/adapters/native/conftest.py
"""Fixtures for the Oracle native-type tests.

The parent ``feature/backend/conftest.py`` already provides ``oracle_backend``
(a real connection to the directory's own schema user).  This conftest adds the
native tables and binds the models to that same connection.

Each module declares the tables it needs in ``TABLES``; only those
``CREATE TABLE`` statements run.  That matters on the older servers: ``JSON``
is a 21c type and ``BOOLEAN``/``VECTOR`` arrive in 23ai, so a module that never
touches them must not create them -- Oracle rejects the whole statement with
``ORA-00902: invalid datatype`` and would take an unrelated test's setup down
with it.
"""

import pytest

import native_models
import native_support

#: The dialect's own support switches for the column types a native table
#: needs.  The conftest never inspects a server version itself: it asks the
#: dialect, which answers per version and per capability.  A table absent from
#: a module's ``TABLES`` is never created, and its switch is never asked.
_TABLE_TYPE_SWITCHES = (
    ("native_numeric", "supports_identity_column", "IDENTITY column"),
    ("native_temporal", "supports_identity_column", "IDENTITY column"),
    ("native_xml_json", "supports_json_type", "native JSON column"),
    ("native_boolean_vector", "supports_boolean_type", "native BOOLEAN column"),
    ("native_boolean_vector", "supports_vector_type", "VECTOR column"),
    ("native_raw_bits", "supports_identity_column", "IDENTITY column"),
)


@pytest.fixture(scope="function")
def native_db(oracle_backend, request):
    """A live backend with the module's declared native tables, empty."""
    names = tuple(getattr(request.module, "TABLES", ()))
    dialect = oracle_backend.dialect
    for table, probe, concept in _TABLE_TYPE_SWITCHES:
        if table in names and not getattr(dialect, probe)():
            pytest.skip(
                f"this Oracle server cannot hold a {concept} ({probe} is False)"
            )
    native_support.ensure_tables(oracle_backend, names)
    native_support.clear_rows(oracle_backend, names)
    native_models.bind_models(oracle_backend)
    try:
        yield oracle_backend
    finally:
        native_support.clear_rows(oracle_backend, names)
