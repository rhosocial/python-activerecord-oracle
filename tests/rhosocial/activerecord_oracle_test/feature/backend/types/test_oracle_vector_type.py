# tests/rhosocial/activerecord_oracle_test/feature/backend/types/test_oracle_vector_type.py
"""Tests for the Oracle VECTOR type class.

Covers ``types/vector.py`` ``OracleVector``: construction validation,
sequence protocol methods, string/binary/JSON serialisation, list/numpy
conversion, normalisation and the geometric operations (dot product,
cosine similarity, euclidean / manhattan distance) plus the classmethod
factories (``from_string`` / ``from_binary`` / ``from_list`` / ``zeros`` /
``random``).

Pure-logic tests: no database connection is required. The numpy-dependent
``to_numpy`` method is exercised with a fake ``numpy`` module injected via
``sys.modules`` so the suite stays green without numpy installed.
"""

import math
import sys

import pytest

from rhosocial.activerecord.backend.impl.oracle.types.vector import OracleVector


class _FakeNumpy:
    """Minimal numpy stand-in recording array construction calls."""

    float32 = "float32"

    def __init__(self):
        """Initialise the recorded calls list."""
        self.calls = []

    def array(self, values, dtype=None):
        """Record the call and return a distinguishable tuple."""
        self.calls.append((list(values), dtype))
        return list(values), dtype


class TestConstruction:
    """``OracleVector`` construction and validation."""

    def test_valid_construction(self):
        """A valid vector is constructed with its dimensions."""
        vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        assert vec.dimensions == 3, "dimensions must match the constructor"
        assert vec.values == [1.0, 2.0, 3.0], "values must match the constructor"

    def test_default_format_is_float32(self):
        """The storage format defaults to FLOAT32."""
        vec = OracleVector(dimensions=2, values=[1.0, 2.0])
        assert vec.format == "FLOAT32", "default format must be FLOAT32"

    def test_value_count_mismatch_raises(self):
        """A values/dimensions mismatch raises ValueError."""
        with pytest.raises(ValueError, match="Expected 3 values"):
            OracleVector(dimensions=3, values=[1.0, 2.0]), "count mismatch must raise"

    def test_unsupported_format_raises(self):
        """An unsupported storage format raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported format"):
            OracleVector(dimensions=2, values=[1.0, 2.0], format="FLOAT16"), (
                "unknown format must raise"
            )

    def test_non_positive_dimensions_raise(self):
        """Zero or negative dimensions raise ValueError."""
        with pytest.raises(ValueError, match="Dimensions must be positive"):
            OracleVector(dimensions=0, values=[]), "zero dimensions must raise"


class TestSequenceProtocol:
    """The sequence protocol methods."""

    def test_len(self):
        """``len`` returns the vector dimension."""
        vec = OracleVector(dimensions=4, values=[1.0, 2.0, 3.0, 4.0])
        assert len(vec) == 4, "len must equal dimensions"

    def test_getitem(self):
        """Indexing returns the matching value."""
        vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        assert vec[1] == 2.0, "indexing must return the value at the position"

    def test_iteration(self):
        """Iteration yields the values in order."""
        vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        assert list(iter(vec)) == [1.0, 2.0, 3.0], "iteration must match the values"

    def test_repr(self):
        """``repr`` summarises dimensions and format."""
        vec = OracleVector(dimensions=2, values=[1.0, 2.0])
        assert repr(vec) == "OracleVector(dimensions=2, format=FLOAT32)", "repr must match"

    def test_str(self):
        """``str`` delegates to the vector string literal form."""
        vec = OracleVector(dimensions=2, values=[1.0, 2.0])
        assert str(vec) == "[1.0, 2.0]", "str must use the bracketed literal form"


class TestSerialization:
    """String / binary / JSON serialisation."""

    def test_to_string(self):
        """``to_string`` joins values with comma+space inside brackets."""
        vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.5])
        assert vec.to_string() == "[1.0, 2.0, 3.5]", "string form must be comma-space joined"

    def test_to_binary_float32(self):
        """FLOAT32 binary is packed with 32-bit little-endian floats."""
        vec = OracleVector(dimensions=2, values=[1.0, 2.0], format="FLOAT32")
        assert vec.to_binary() == b"\x00\x00\x80?\x00\x00\x00@", "float32 packing expected"

    def test_to_binary_float64(self):
        """FLOAT64 binary is packed with 64-bit little-endian doubles."""
        vec = OracleVector(dimensions=1, values=[1.0], format="FLOAT64")
        assert vec.to_binary() == b"\x00\x00\x00\x00\x00\x00\xf0?", "float64 packing expected"

    def test_to_binary_int8(self):
        """INT8 binary is packed with signed 8-bit integers."""
        vec = OracleVector(dimensions=2, values=[1.0, -2.0], format="INT8")
        assert vec.to_binary() == b"\x01\xfe", "int8 packing expected"

    def test_to_binary_unknown_format_raises(self):
        """An internally-corrupt format triggers the to_binary guard."""
        vec = object.__new__(OracleVector)
        vec.values = [1.0]
        vec.format = "FLOAT16"
        with pytest.raises(ValueError, match="Unknown format"):
            vec.to_binary(), "unknown storage format must raise in to_binary"

    def test_to_json(self):
        """``to_json`` emits dimensions, values and format."""
        vec = OracleVector(dimensions=2, values=[1.0, 2.0])
        import json

        payload = json.loads(vec.to_json())
        assert payload["dimensions"] == 2, "json dimensions must match"
        assert payload["values"] == [1.0, 2.0], "json values must match"
        assert payload["format"] == "FLOAT32", "json format must match"

    def test_to_list_returns_copy(self):
        """``to_list`` returns a copy that does not alias the source values."""
        vec = OracleVector(dimensions=2, values=[1.0, 2.0])
        result = vec.to_list()
        assert result == [1.0, 2.0], "list must match the values"
        result.append(99.0)
        assert vec.values == [1.0, 2.0], "mutating the copy must not alter the vector"


class TestNumpyConversion:
    """The optional numpy conversion."""

    def test_to_numpy(self, monkeypatch):
        """``to_numpy`` builds an array with dtype float32."""
        fake = _FakeNumpy()
        monkeypatch.setitem(sys.modules, "numpy", fake)
        vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        result = vec.to_numpy()
        assert fake.calls == [([1.0, 2.0, 3.0], "float32")], "array must receive values+dtype"
        assert result == ([1.0, 2.0, 3.0], "float32"), "return value must be the array result"


class TestNormalization:
    """L2 normalisation behaviour."""

    def test_normalize_unit_vector(self):
        """Normalising a non-zero vector yields unit length."""
        vec = OracleVector(dimensions=3, values=[3.0, 0.0, 0.0])
        normalized = vec.normalize()
        assert normalized.values[0] == pytest.approx(1.0), "x-axis value must become 1.0"
        assert normalized.values[1] == pytest.approx(0.0), "y-axis value must stay 0.0"
        assert normalized.format == "FLOAT32", "format must be preserved"

    def test_normalize_zero_vector(self):
        """Normalising a zero vector yields the zero vector."""
        vec = OracleVector(dimensions=3, values=[0.0, 0.0, 0.0])
        normalized = vec.normalize()
        assert normalized.values == [0.0, 0.0, 0.0], "zero vector must stay zero"

    def test_l2_norm(self):
        """``l2_norm`` is the Euclidean length of the vector."""
        vec = OracleVector(dimensions=2, values=[3.0, 4.0])
        assert vec.l2_norm() == pytest.approx(5.0), "norm of (3,4) must be 5"

    def test_l2_norm_zero(self):
        """``l2_norm`` of a zero vector is zero."""
        vec = OracleVector(dimensions=2, values=[0.0, 0.0])
        assert vec.l2_norm() == pytest.approx(0.0), "norm of the zero vector must be 0"


class TestGeometricOperations:
    """Dot product and distance / similarity operations."""

    def test_dot_product(self):
        """``dot_product`` sums the element-wise products."""
        a = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        b = OracleVector(dimensions=3, values=[4.0, 5.0, 6.0])
        assert a.dot_product(b) == pytest.approx(32.0), "dot product must be 1*4+2*5+3*6"

    def test_dot_product_dimension_mismatch_raises(self):
        """A dimension mismatch in dot_product raises ValueError."""
        a = OracleVector(dimensions=2, values=[1.0, 2.0])
        b = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        with pytest.raises(ValueError, match="Dimension mismatch"):
            a.dot_product(b), "dot product needs equal dimensions"

    def test_cosine_similarity(self):
        """``cosine_similarity`` returns dot/(norm_a*norm_b)."""
        a = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        b = OracleVector(dimensions=3, values=[1.0, 0.0, 0.0])
        expected = 1.0 / math.sqrt(14.0)
        assert a.cosine_similarity(b) == pytest.approx(expected), "cosine value must match"

    def test_cosine_similarity_zero_norm(self):
        """A zero-norm operand yields 0.0 similarity."""
        a = OracleVector(dimensions=2, values=[0.0, 0.0])
        b = OracleVector(dimensions=2, values=[1.0, 2.0])
        assert a.cosine_similarity(b) == 0.0, "zero norm must short-circuit to 0.0"

    def test_cosine_similarity_dimension_mismatch_raises(self):
        """A dimension mismatch in cosine_similarity raises ValueError."""
        a = OracleVector(dimensions=2, values=[1.0, 2.0])
        b = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        with pytest.raises(ValueError, match="Dimension mismatch"):
            a.cosine_similarity(b), "cosine similarity needs equal dimensions"

    def test_euclidean_distance(self):
        """``euclidean_distance`` returns the L2 distance between vectors."""
        a = OracleVector(dimensions=2, values=[0.0, 0.0])
        b = OracleVector(dimensions=2, values=[3.0, 4.0])
        assert a.euclidean_distance(b) == pytest.approx(5.0), "distance must be 5"

    def test_euclidean_distance_dimension_mismatch_raises(self):
        """A dimension mismatch in euclidean_distance raises ValueError."""
        a = OracleVector(dimensions=2, values=[1.0, 2.0])
        b = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        with pytest.raises(ValueError, match="Dimension mismatch"):
            a.euclidean_distance(b), "euclidean distance needs equal dimensions"

    def test_manhattan_distance(self):
        """``manhattan_distance`` sums absolute element differences."""
        a = OracleVector(dimensions=2, values=[1.0, 2.0])
        b = OracleVector(dimensions=2, values=[4.0, 6.0])
        assert a.manhattan_distance(b) == pytest.approx(7.0), "distance must be 3+4"

    def test_manhattan_distance_dimension_mismatch_raises(self):
        """A dimension mismatch in manhattan_distance raises ValueError."""
        a = OracleVector(dimensions=2, values=[1.0, 2.0])
        b = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        with pytest.raises(ValueError, match="Dimension mismatch"):
            a.manhattan_distance(b), "manhattan distance needs equal dimensions"


class TestClassmethodFactories:
    """The classmethod constructors."""

    def test_from_string(self):
        """``from_string`` parses the bracketed vector literal."""
        vec = OracleVector.from_string("[1.5, 2.5, 3.5]")
        assert vec.dimensions == 3, "parsed dimensions must equal value count"
        assert vec.values == [1.5, 2.5, 3.5], "parsed values must match"

    def test_from_string_invalid_format_raises(self):
        """A literal without brackets raises ValueError."""
        with pytest.raises(ValueError, match="Invalid VECTOR format"):
            OracleVector.from_string("1.5, 2.5"), "unbracketed input must raise"

    def test_from_string_empty(self):
        """An empty bracket pair raises because dimensions cannot be zero."""
        with pytest.raises(ValueError, match="Dimensions must be positive"):
            OracleVector.from_string("[]"), "empty vector must raise"

    def test_from_binary_float32_roundtrip(self):
        """FLOAT32 binary round-trips through from_binary."""
        vec = OracleVector(dimensions=3, values=[1.0, -2.0, 3.5], format="FLOAT32")
        restored = OracleVector.from_binary(vec.to_binary(), 3, "FLOAT32")
        assert restored.values == [1.0, -2.0, 3.5], "float32 round-trip must preserve values"

    def test_from_binary_float64_roundtrip(self):
        """FLOAT64 binary round-trips through from_binary."""
        vec = OracleVector(dimensions=3, values=[1.0, -2.0, 3.5], format="FLOAT64")
        restored = OracleVector.from_binary(vec.to_binary(), 3, "FLOAT64")
        assert restored.values == [1.0, -2.0, 3.5], "float64 round-trip must preserve values"

    def test_from_binary_int8_roundtrip(self):
        """INT8 binary round-trips through from_binary as floats."""
        vec = OracleVector(dimensions=2, values=[1.0, -2.0], format="INT8")
        restored = OracleVector.from_binary(vec.to_binary(), 2, "INT8")
        assert restored.values == [1.0, -2.0], "int8 round-trip must preserve values"

    def test_from_binary_unknown_format_raises(self):
        """An unknown binary format raises ValueError."""
        with pytest.raises(ValueError, match="Unknown format"):
            OracleVector.from_binary(b"\x00", 1, "FLOAT16"), "unknown format must raise"

    def test_from_list(self):
        """``from_list`` derives dimensions from the value count."""
        vec = OracleVector.from_list([1.0, 2.0, 3.0])
        assert vec.dimensions == 3, "dimensions must equal list length"
        assert vec.values == [1.0, 2.0, 3.0], "values must match the list"

    def test_from_list_with_format(self):
        """``from_list`` honours the explicit format."""
        vec = OracleVector.from_list([1.0, 2.0], format="INT8")
        assert vec.format == "INT8", "format must be propagated"

    def test_zeros(self):
        """``zeros`` builds an all-zero vector."""
        vec = OracleVector.zeros(4)
        assert vec.dimensions == 4, "zero vector dimensions must match"
        assert vec.values == [0.0, 0.0, 0.0, 0.0], "zero vector values must be zero"

    def test_random_with_seed(self):
        """``random`` with a seed is deterministic and in [0, 1)."""
        first = OracleVector.random(3, seed=42)
        second = OracleVector.random(3, seed=42)
        assert first.values == second.values, "same seed must reproduce the vector"
        assert all(0.0 <= v < 1.0 for v in first.values), "random values must be in [0, 1)"

    def test_random_without_seed(self):
        """``random`` without a seed still produces a valid vector."""
        vec = OracleVector.random(3)
        assert vec.dimensions == 3, "random vector dimensions must match"
        assert len(vec.values) == 3, "random vector must carry all values"