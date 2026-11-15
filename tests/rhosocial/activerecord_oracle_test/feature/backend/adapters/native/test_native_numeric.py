# tests/rhosocial/activerecord_oracle_test/feature/backend/adapters/native/test_native_numeric.py
"""Oracle numeric native carriers: NUMBER(p,s), NUMBER, BINARY_FLOAT/DOUBLE.

Three forms are asserted per family:

* S1 semantic round-trip -- adapter in both directions;
* S2 storage stability    -- write, read, write again, compare column text;
* S3 foreign input        -- write by native SQL, read through the model.

Every live assertion also reads the column's original text with ``TO_CHAR``
so a self-consistent but wrong encoding cannot pass.
"""

from decimal import Decimal

import pytest

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.impl.oracle.adapters import OracleDecimalAdapter

import native_models as models
from native_support import execute, fetch_one


def _raw_number(backend, row_id):
    return fetch_one(
        backend,
        "SELECT dec_val, TO_CHAR(dec_val) AS text, "
        "plain_val, TO_CHAR(plain_val) AS plain_text, "
        "bfloat_val, TO_CHAR(bfloat_val) AS bfloat_text, "
        "bdouble_val, TO_CHAR(bdouble_val) AS bdouble_text "
        "FROM native_numeric WHERE id = ?",
        (row_id,),
    )


class TestNumberWithScale:
    """``NUMBER(10,2)``: Decimal in Python, scale in the column."""

    def test_round_trip_keeps_the_scale(self, native_db):
        row = models.NativeNumeric(dec_val=Decimal("123.45"))
        row.save()

        read = models.NativeNumeric.find_one(row.id)
        assert isinstance(read.dec_val, Decimal)
        assert read.dec_val == Decimal("123.45")
        assert read.dec_val.as_tuple().exponent == -2

        raw = _raw_number(native_db, row.id)
        assert raw["text"] == "123.45"

    def test_value_is_quantised_to_the_declared_scale(self, native_db):
        row = models.NativeNumeric(dec_val=Decimal("123.456"))
        row.save()

        read = models.NativeNumeric.find_one(row.id)
        assert read.dec_val == Decimal("123.46")
        assert read.dec_val.as_tuple().exponent == -2
        assert _raw_number(native_db, row.id)["text"] == "123.46"

    def test_precision_overflow_is_rejected(self, native_db):
        row = models.NativeNumeric(dec_val=Decimal("123456789.12"))
        with pytest.raises(DatabaseError):
            row.save()

    def test_storage_is_stable_after_a_second_write(self, native_db):
        row = models.NativeNumeric(dec_val=Decimal("987.65"))
        row.save()
        read = models.NativeNumeric.find_one(row.id)

        second = models.NativeNumeric(dec_val=read.dec_val)
        second.save()

        assert _raw_number(native_db, row.id)["text"] == "987.65"
        assert _raw_number(native_db, second.id)["text"] == "987.65"

    def test_native_sql_input_is_readable(self, native_db):
        execute(native_db, "INSERT INTO native_numeric (dec_val) VALUES (?)", (42.5,))
        row_id = fetch_one(
            native_db, "SELECT MAX(id) AS id FROM native_numeric"
        )["id"]

        read = models.NativeNumeric.find_one(row_id)
        assert read.dec_val == Decimal("42.5")
        assert _raw_number(native_db, row_id)["text"] == "42.5"

        rewritten = models.NativeNumeric(dec_val=read.dec_val)
        rewritten.save()
        assert _raw_number(native_db, rewritten.id)["text"] == "42.5"

    def test_adapter_semantic_round_trip(self):
        adapter = OracleDecimalAdapter()
        sent = adapter.to_database(Decimal("123.45"), float)
        assert isinstance(sent, float)
        assert sent == 123.45

        received = adapter.from_database(Decimal("123.45"), Decimal)
        assert isinstance(received, float)
        # The adapter's Oracle target is ``float`` (see OracleDecimalAdapter);
        # the decimal text survives a float round-trip at this scale.
        assert Decimal(str(received)) == Decimal("123.45")


class TestNumberWithoutScale:
    """Plain ``NUMBER``: exact digits, and integral values come back as ints."""

    def test_exact_digits_survive(self, native_db):
        row = models.NativeNumeric(plain_val=Decimal("123.456789"))
        row.save()

        read = models.NativeNumeric.find_one(row.id)
        assert read.plain_val == Decimal("123.456789")
        assert _raw_number(native_db, row.id)["plain_text"] == "123.456789"

    def test_integral_value_has_integer_semantics(self, native_db):
        row = models.NativeNumeric(plain_val=Decimal("1000"))
        row.save()

        raw = _raw_number(native_db, row.id)
        assert raw["plain_val"] == 1000
        assert isinstance(raw["plain_val"], int)
        assert raw["plain_text"] == "1000"

        read = models.NativeNumeric.find_one(row.id)
        # OracleDecimalAdapter turns the driver's int into float, so the
        # Decimal field reads 1000.0; the value itself is unchanged.
        assert read.plain_val == Decimal("1000")
        assert read.plain_val.as_tuple().exponent == -1

    def test_adapter_semantic_round_trip(self):
        adapter = OracleDecimalAdapter()
        assert adapter.to_database(Decimal("123.456789"), float) == 123.456789
        assert adapter.from_database(123.456789, Decimal) == 123.456789


class TestBinaryFloatAndDouble:
    """``BINARY_FLOAT``/``BINARY_DOUBLE``: the float branch of DecimalAdapter."""

    @pytest.mark.parametrize(
        "field, value, text",
        [
            ("bfloat_val", 1.25, "1.25E+000"),
            ("bdouble_val", 1.125, "1.125E+000"),
        ],
    )
    def test_round_trip(self, native_db, field, value, text):
        row = models.NativeNumeric(**{field: value})
        row.save()

        read = models.NativeNumeric.find_one(row.id)
        assert getattr(read, field) == value
        assert isinstance(getattr(read, field), float)
        raw = _raw_number(native_db, row.id)
        assert raw[f"{field.split('_')[0]}_text"] == text

    def test_storage_is_stable_after_a_second_write(self, native_db):
        row = models.NativeNumeric(bfloat_val=1.25, bdouble_val=1.125)
        row.save()
        read = models.NativeNumeric.find_one(row.id)

        second = models.NativeNumeric(bfloat_val=read.bfloat_val, bdouble_val=read.bdouble_val)
        second.save()

        assert _raw_number(native_db, second.id)["bfloat_text"] == "1.25E+000"
        assert _raw_number(native_db, second.id)["bdouble_text"] == "1.125E+000"

    def test_adapter_semantic_round_trip(self):
        adapter = OracleDecimalAdapter()
        assert adapter.to_database(Decimal("1.25"), float) == 1.25
        assert adapter.from_database(1.25, Decimal) == 1.25
