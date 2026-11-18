# tests/rhosocial/activerecord_oracle_test/feature/basic/types_long_content/conftest.py
"""Use the shared ``basic/types_long_content`` test set.

The shared ``LongRow`` / ``AsyncLongRow`` fixtures declare ``id: Optional[int]``
but no ``__primary_key__``. Since the primary-key-declaration refactor, a bare
``ActiveRecord`` subclass is keyless (``primary_key() is None``), so ``save()``
no longer writes back the generated id and ``find_one()`` refuses to address
the row. Oracle renders the shared content concepts, so its round-trip tests
actually run here.

The declaration belongs in the shared fixture, but until it is added there the
Oracle bridge restores it locally. Setting the class attribute (rather than
subclassing) keeps the shared fixtures and the imported test module operating
on the very same model objects.
"""

from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter
from rhosocial.activerecord.testsuite.feature.basic.types_long_content.conftest import *  # noqa: F401,F403
from rhosocial.activerecord.testsuite.feature.basic.fixtures.types_long_content_models import (
    AsyncLongRow,
    LongRow,
)


class _OracleXmlTextAdapter(BaseSQLTypeAdapter):
    """Read an ``XMLTYPE`` column back as the exact text that was written.

    Oracle serialises an ``XMLTYPE`` with a trailing newline; the shared
    long-content contract compares the original string byte for byte.
    """

    def __init__(self):
        super().__init__()
        self.register_type(str, str)

    def _do_to_database(self, value, target_type, options):
        return value

    def _do_from_database(self, value, target_type, options):
        return value.rstrip("\n") if isinstance(value, str) else value


for _long_row_class in (LongRow, AsyncLongRow):
    if _long_row_class.__primary_key__ is None:
        _long_row_class.__primary_key__ = "id"
    # Scoped to this bridge: keep the other fields' default adapters intact.
    _adapters = dict(_long_row_class.__field_adapters__ or {})
    _adapters["markup"] = [(_OracleXmlTextAdapter(), str)]
    _long_row_class.__field_adapters__ = _adapters

del _long_row_class, _adapters
