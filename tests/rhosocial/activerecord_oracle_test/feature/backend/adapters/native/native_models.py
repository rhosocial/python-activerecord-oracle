# tests/rhosocial/activerecord_oracle_test/feature/backend/adapters/native/native_models.py
"""Models for the Oracle native-type tests.

Every model uses only the adapters this backend already ships.  Fields whose
Python type has no default suggestion (interval wrappers, ROWID wrappers,
XMLTYPE, RAW bit strings) declare their adapter explicitly with
``Annotated[Optional[T], UseAdapter(...)]``.

``NativeBooleanVector.flag`` declares the ``bool`` target so a native
``BOOLEAN`` column is bound as a boolean, not through the ``NUMBER(1)``
carrier the default suggestion uses.
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Annotated, ClassVar, Optional

from pydantic import ConfigDict

from rhosocial.activerecord.base.field_proxy import FieldProxy
from rhosocial.activerecord.base.fields import UseAdapter
from rhosocial.activerecord.backend.adapters import BitString, Vector
from rhosocial.activerecord.backend.impl.oracle.adapters import (
    OracleBooleanAdapter,
    OracleIntervalAdapter,
    OracleRowIDAdapter,
    OracleXMLAdapter,
)
from rhosocial.activerecord.backend.impl.oracle.neutral_adapters import (
    OracleNeutralBitStringAdapter,
)
from rhosocial.activerecord.backend.impl.oracle.type_values import (
    IntervalYearToMonth,
    OracleRowID,
    OracleURowID,
    OracleXMLType,
)
from rhosocial.activerecord.model import ActiveRecord


class _NativeModel(ActiveRecord):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    c: ClassVar[FieldProxy] = FieldProxy()
    id: Optional[int] = None


class NativeNumeric(_NativeModel):
    @classmethod
    def table_name(cls):
        return "native_numeric"

    dec_val: Optional[Decimal] = None
    plain_val: Optional[Decimal] = None
    bfloat_val: Optional[float] = None
    bdouble_val: Optional[float] = None


class NativeTemporal(_NativeModel):
    @classmethod
    def table_name(cls):
        return "native_temporal"

    date_val: Optional[datetime] = None
    ts_val: Optional[datetime] = None
    tstz_val: Optional[datetime] = None
    tsltz_val: Optional[datetime] = None
    ytm_val: Annotated[
        Optional[IntervalYearToMonth], UseAdapter(OracleIntervalAdapter(), str)
    ] = None
    dts_val: Annotated[Optional[timedelta], UseAdapter(OracleIntervalAdapter(), str)] = None


class NativeIdentifiers(_NativeModel):
    @classmethod
    def table_name(cls):
        return "native_identifiers"

    rid: Annotated[Optional[OracleRowID], UseAdapter(OracleRowIDAdapter(), str)] = None
    urid: Annotated[Optional[OracleURowID], UseAdapter(OracleRowIDAdapter(), str)] = None


class NativeXmlJson(_NativeModel):
    @classmethod
    def table_name(cls):
        return "native_xml_json"

    xml_val: Annotated[Optional[OracleXMLType], UseAdapter(OracleXMLAdapter(), str)] = None
    json_val: Optional[dict] = None


class NativeBooleanVector(_NativeModel):
    @classmethod
    def table_name(cls):
        return "native_boolean_vector"

    flag: Annotated[Optional[bool], UseAdapter(OracleBooleanAdapter(), bool)] = None
    emb: Optional[Vector] = None


class NativeRawBits(_NativeModel):
    @classmethod
    def table_name(cls):
        return "native_raw_bits"

    raw_bits: Annotated[
        Optional[BitString], UseAdapter(OracleNeutralBitStringAdapter(), bytes)
    ] = None


MODELS = (
    NativeNumeric,
    NativeTemporal,
    NativeIdentifiers,
    NativeXmlJson,
    NativeBooleanVector,
    NativeRawBits,
)


def bind_models(backend) -> None:
    """Point every native model at the fixture's live backend.

    ``ActiveRecord.configure()`` would open a second connection; the tests
    share the fixture's connection instead (the same class attributes the
    testsuite's provider session sets).
    """
    config = backend.config
    backend_class = type(backend)
    for model in MODELS:
        model.__connection_config__ = config
        model.__backend_class__ = backend_class
        model.__backend__ = backend
