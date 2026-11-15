# tests/rhosocial/activerecord_oracle_test/feature/backend/adapters/test_neutral_adapters.py
"""DB-free contract tests for the Oracle neutral value adapters.

These adapters only transform values: no connection is opened.  The tests pin
the text/bytes wire forms, the read path from oracledb-style SDO objects and
LOB wrappers, the ``TypeConversionError`` wrapping, and the inventory.
"""

from datetime import date, datetime
from types import SimpleNamespace

import pytest

from rhosocial.activerecord.backend.adapters import (
    BitString,
    Geometry,
    GeometryCollection,
    LineString,
    MacAddress,
    MultiPolygon,
    Multirange,
    Point,
    Polygon,
    Range,
    Vector,
)
from rhosocial.activerecord.backend.errors import TypeConversionError
from rhosocial.activerecord.backend.impl.oracle.backend import (
    AsyncOracleBackend,
    OracleBackend,
)
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.backend.impl.oracle.neutral_adapters import (
    OracleNeutralBitStringAdapter,
    OracleNeutralGeometryAdapter,
    OracleNeutralMacAddressAdapter,
    OracleNeutralRangeAdapter,
    OracleNeutralVectorAdapter,
)
from rhosocial.activerecord.backend.impl.oracle.type_values import (
    OracleVector,
    SDOGeometry,
)

_NEUTRAL_CLASSES = (
    OracleNeutralBitStringAdapter,
    OracleNeutralGeometryAdapter,
    OracleNeutralMacAddressAdapter,
    OracleNeutralRangeAdapter,
    OracleNeutralVectorAdapter,
)


def _config():
    """An inert connection config: construction never opens a socket."""
    return OracleConnectionConfig(
        host="localhost",
        port=1521,
        service_name="UNIT",
        username="unit",
        password="unit",
    )


@pytest.fixture(params=[OracleBackend, AsyncOracleBackend])
def backend(request):
    return request.param(connection_config=_config())


def _class_key(adapter):
    adapter_class = type(adapter)
    return (adapter_class.__module__, adapter_class.__qualname__)


class _FakeLob:
    """Stand-in for oracledb's LOB wrapper, which exposes ``.read()``."""

    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return self._payload


class TestInventory:
    def test_every_neutral_adapter_is_in_the_catalogue(self, backend):
        names = {type(adapter).__name__ for adapter in backend.get_type_adapters()}
        for adapter_class in _NEUTRAL_CLASSES:
            assert adapter_class.__name__ in names

    def test_neutral_classes_appear_once(self, backend):
        keys = [_class_key(adapter) for adapter in backend.get_type_adapters()]
        assert len(keys) == len(set(keys))


class TestGeometryAdapter:
    def test_wkt_round_trip_with_srid(self):
        adapter = OracleNeutralGeometryAdapter()
        point = Point(1, 2, srid=4326)
        text = adapter.to_database(point, str)
        assert text == "SRID=4326;POINT (1.0 2.0)"
        assert adapter.from_database(text, Geometry) == point

    def test_wkt_round_trip_without_srid(self):
        adapter = OracleNeutralGeometryAdapter()
        line = LineString(((0, 0), (1, 1)))
        assert adapter.to_database(line, str) == "LINESTRING (0.0 0.0, 1.0 1.0)"
        assert adapter.from_database(adapter.to_database(line, str), Geometry) == line

    def test_srid_can_be_suppressed_for_plain_text_columns(self):
        adapter = OracleNeutralGeometryAdapter()
        point = Point(1, 2, srid=4326)
        assert adapter.to_database(point, str, {"include_srid": False}) == ("POINT (1.0 2.0)")

    def test_sdo_constructor_text_is_explicitly_an_sql_expression(self):
        adapter = OracleNeutralGeometryAdapter()
        text = adapter.to_database(Point(1, 2, srid=4326), str, {"format": "sdo_constructor"})
        assert text.startswith("SDO_GEOMETRY(")
        assert "SDO_POINT_TYPE(1.0, 2.0, NULL)" in text

    def test_decodes_sdo_dict_point(self):
        adapter = OracleNeutralGeometryAdapter()
        value = {
            "SDO_GTYPE": 2001,
            "SDO_SRID": 4326,
            "SDO_POINT": {"X": 1.0, "Y": 2.0},
            "SDO_ELEM_INFO": None,
            "SDO_ORDINATES": None,
        }
        assert adapter.from_database(value, Geometry) == Point(1.0, 2.0, srid=4326)

    def test_decodes_sdo_dict_line_and_polygon(self):
        adapter = OracleNeutralGeometryAdapter()
        line = {
            "SDO_GTYPE": 2002,
            "SDO_SRID": 4326,
            "SDO_ELEM_INFO": [1, 2, 1],
            "SDO_ORDINATES": [0, 0, 1, 1],
        }
        assert adapter.from_database(line, Geometry) == LineString(
            ((0.0, 0.0), (1.0, 1.0)), srid=4326
        )
        polygon = {
            "SDO_GTYPE": 2003,
            "SDO_SRID": 4326,
            "SDO_ELEM_INFO": [1, 1003, 1],
            "SDO_ORDINATES": [0, 0, 10, 0, 10, 10, 0, 10, 0, 0],
        }
        assert adapter.from_database(polygon, Geometry) == Polygon(
            (((0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)),),
            srid=4326,
        )

    def test_decodes_a_lowercase_sdo_dict(self):
        adapter = OracleNeutralGeometryAdapter()
        value = {
            "sdo_gtype": 2001,
            "sdo_srid": 4326,
            "sdo_point": {"x": 1.0, "y": 2.0, "z": None},
        }
        assert adapter.from_database(value, Geometry) == Point(1.0, 2.0, srid=4326)

    def test_decodes_a_multipolygon(self):
        adapter = OracleNeutralGeometryAdapter()
        value = {
            "SDO_GTYPE": 2007,
            "SDO_SRID": 4326,
            "SDO_ELEM_INFO": [1, 1003, 1, 9, 1003, 1],
            "SDO_ORDINATES": [
                0,
                0,
                10,
                0,
                10,
                10,
                0,
                0,
                20,
                20,
                30,
                20,
                30,
                30,
                20,
                20,
            ],
        }
        expected = MultiPolygon(
            (
                (((0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 0.0)),),
                (((20.0, 20.0), (30.0, 20.0), (30.0, 30.0), (20.0, 20.0)),),
            ),
            srid=4326,
        )
        assert adapter.from_database(value, Geometry) == expected

    def test_decodes_a_collection_of_point_line_and_polygon(self):
        adapter = OracleNeutralGeometryAdapter()
        value = {
            "SDO_GTYPE": 2004,
            "SDO_SRID": 4326,
            "SDO_ELEM_INFO": [1, 1, 1, 3, 2, 1, 7, 1003, 1],
            "SDO_ORDINATES": [
                1,
                2,
                0,
                0,
                1,
                1,
                0,
                0,
                10,
                0,
                10,
                10,
                0,
                0,
            ],
        }
        expected = GeometryCollection(
            (
                Point(1.0, 2.0),
                LineString(((0.0, 0.0), (1.0, 1.0))),
                Polygon((((0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 0.0)),)),
            ),
            srid=4326,
        )
        assert adapter.from_database(value, Geometry) == expected

    def test_an_unsupported_collection_element_raises(self):
        adapter = OracleNeutralGeometryAdapter()
        value = {
            "SDO_GTYPE": 2004,
            "SDO_SRID": 4326,
            "SDO_ELEM_INFO": [1, 4, 1],
            "SDO_ORDINATES": [0, 0, 1, 1],
        }
        with pytest.raises(TypeConversionError):
            adapter.from_database(value, Geometry)

    def test_three_dimensional_multipolygon_is_rejected(self):
        adapter = OracleNeutralGeometryAdapter()
        value = {
            "SDO_GTYPE": 3007,
            "SDO_SRID": 4326,
            "SDO_ELEM_INFO": [1, 1003, 1],
            "SDO_ORDINATES": [0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0],
        }
        with pytest.raises(TypeConversionError):
            adapter.from_database(value, Geometry)

    def test_compound_polygon_element_type_raises(self):
        adapter = OracleNeutralGeometryAdapter()
        value = {
            "SDO_GTYPE": 2003,
            "SDO_SRID": 4326,
            "SDO_ELEM_INFO": [1, 1005, 1, 1, 1003, 1],
            "SDO_ORDINATES": [0, 0, 10, 0, 10, 10, 0, 0],
        }
        with pytest.raises(TypeConversionError):
            adapter.from_database(value, Geometry)

    def test_an_existing_sdo_value_is_accepted_on_write(self):
        adapter = OracleNeutralGeometryAdapter()
        sdo = SDOGeometry.point(1.0, 2.0, srid=4326)
        assert adapter.to_database(sdo, str) == "SRID=4326;POINT (1.0 2.0)"

        value = {
            "SDO_GTYPE": 2002,
            "SDO_SRID": 4326,
            "SDO_ELEM_INFO": [1, 2, 1],
            "SDO_ORDINATES": [0, 0, 1, 1],
        }
        assert adapter.to_database(value, str) == ("SRID=4326;LINESTRING (0.0 0.0, 1.0 1.0)")

    def test_native_passthrough_returns_the_same_sdo_object(self):
        adapter = OracleNeutralGeometryAdapter()
        sdo = SDOGeometry.point(1.0, 2.0, srid=4326)
        assert adapter.to_database(sdo, SDOGeometry, {"format": "native"}) is sdo

    def test_native_output_without_an_sdo_object_is_wrapped(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError):
            adapter.to_database(Point(1, 2), str, {"format": "native"})

    def test_decodes_the_sdo_wrapper_and_an_object_style_value(self):
        adapter = OracleNeutralGeometryAdapter()
        assert adapter.from_database(
            SDOGeometry.line([(0, 0), (1, 1)], srid=4326), Geometry
        ) == LineString(((0.0, 0.0), (1.0, 1.0)), srid=4326)

        object_value = SimpleNamespace(
            SDO_GTYPE=2001,
            SDO_SRID=4326,
            SDO_POINT=SimpleNamespace(X=1.0, Y=2.0, Z=None),
            SDO_ELEM_INFO=None,
            SDO_ORDINATES=None,
        )
        assert adapter.from_database(object_value, Geometry) == Point(1.0, 2.0, srid=4326)

    def test_three_dimensional_sdo_is_rejected(self):
        adapter = OracleNeutralGeometryAdapter()
        value = {
            "SDO_GTYPE": 3001,
            "SDO_SRID": 4326,
            "SDO_POINT": {"X": 1.0, "Y": 2.0, "Z": 3.0},
        }
        with pytest.raises(TypeConversionError):
            adapter.from_database(value, Geometry)

    def test_uncovered_sdo_type_is_rejected(self):
        adapter = OracleNeutralGeometryAdapter()
        value = {
            "SDO_GTYPE": 2008,  # solid: no neutral counterpart
            "SDO_SRID": 4326,
            "SDO_ELEM_INFO": [1, 1003, 1],
            "SDO_ORDINATES": [0, 0, 1, 0, 1, 1, 0, 0],
        }
        with pytest.raises(TypeConversionError):
            adapter.from_database(value, Geometry)

    def test_wrong_target_type_is_wrapped(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError) as excinfo:
            adapter.to_database(Point(1, 2), bytes)
        assert "Cannot convert Point to Geometry" in str(excinfo.value)

    def test_invalid_wkt_is_wrapped(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database("POINT (1 2", Geometry)


class TestVectorAdapter:
    def test_text_round_trip(self):
        adapter = OracleNeutralVectorAdapter()
        vector = Vector((1, 2.5, -3))
        assert adapter.to_database(vector, str) == "[1.0, 2.5, -3.0]"
        assert adapter.from_database("[1.0, 2.5, -3.0]", Vector) == vector

    def test_decodes_the_wrapper_and_a_sequence(self):
        adapter = OracleNeutralVectorAdapter()
        assert adapter.from_database(
            OracleVector(dimensions=2, values=[1.0, 2.0]), Vector
        ) == Vector((1.0, 2.0))
        assert adapter.from_database([1.0, 2.0], Vector) == Vector((1.0, 2.0))

    def test_non_finite_components_are_rejected_on_write(self):
        adapter = OracleNeutralVectorAdapter()
        with pytest.raises(TypeConversionError):
            adapter.to_database(Vector((float("inf"),), allow_non_finite=True), str)

    def test_malformed_text_is_wrapped(self):
        adapter = OracleNeutralVectorAdapter()
        for value in ("1,2", "[]", "[1,]", "[a]"):
            with pytest.raises(TypeConversionError):
                adapter.from_database(value, Vector)


class TestBitStringAdapter:
    def test_raw_bytes_round_trip(self):
        adapter = OracleNeutralBitStringAdapter()
        bits = BitString.from_bits("1010")
        assert adapter.to_database(bits, bytes) == b"\xa0"
        assert adapter.from_database(b"\xa0", BitString, {"bit_length": 4}) == bits

    def test_without_a_declared_length_the_byte_length_is_used(self):
        adapter = OracleNeutralBitStringAdapter()
        assert adapter.from_database(b"\xa0", BitString).to_bits() == "10100000"

    def test_lob_wrapper_is_read(self):
        adapter = OracleNeutralBitStringAdapter()
        assert adapter.from_database(
            _FakeLob(b"\xa0"), BitString, {"bit_length": 4}
        ) == BitString.from_bits("1010")

    def test_empty_raw_is_rejected(self):
        adapter = OracleNeutralBitStringAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database(b"", BitString)

    def test_text_round_trip(self):
        adapter = OracleNeutralBitStringAdapter()
        bits = BitString.from_bits("1010")
        assert adapter.to_database(bits, str) == "1010"
        assert adapter.from_database("1010", BitString) == bits

    def test_wrong_target_type_is_wrapped(self):
        adapter = OracleNeutralBitStringAdapter()
        with pytest.raises(TypeConversionError):
            adapter.to_database(BitString.from_bits("1"), int)


class TestRangeAdapter:
    def test_range_json_round_trip(self):
        adapter = OracleNeutralRangeAdapter()
        value = Range(1, 10, "[)")
        text = adapter.to_database(value, str)
        assert text == '{"lower":1,"upper":10,"bounds":"[)","empty":false,"element_type":"int"}'
        assert adapter.from_database(text, Range) == value

    def test_date_bounds_survive_the_round_trip(self):
        adapter = OracleNeutralRangeAdapter()
        value = Range(date(2024, 1, 1), date(2024, 12, 31), "[)")
        text = adapter.to_database(value, str)
        assert '"element_type":"date"' in text
        assert adapter.from_database(text, Range) == value

    def test_multirange_json_round_trip(self):
        adapter = OracleNeutralRangeAdapter()
        value = Multirange((Range(1, 5), Range(10, 15)))
        text = adapter.to_database(value, str)
        assert adapter.from_database(text, Multirange) == value

    def test_decoded_json_values_and_lob_wrappers(self):
        adapter = OracleNeutralRangeAdapter()
        payload = {
            "lower": datetime(2024, 1, 1, 12, 0).isoformat(),
            "upper": datetime(2024, 1, 2, 12, 0).isoformat(),
            "bounds": "[)",
            "empty": False,
            "element_type": "datetime",
        }
        assert adapter.from_database(payload, Range) == Range(
            datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 2, 12, 0), "[)"
        )
        text = adapter.to_database(Range(1, 5), str)
        assert adapter.from_database(_FakeLob(text), Range) == Range(1, 5)

    def test_malformed_json_is_wrapped(self):
        adapter = OracleNeutralRangeAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database("not json", Range)

    def test_target_type_mismatch_is_wrapped(self):
        adapter = OracleNeutralRangeAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database(
                '{"lower":1,"upper":2,"bounds":"[)","empty":false,"element_type":"int"}',
                Multirange,
            )

    def test_wrong_target_type_on_write_is_wrapped(self):
        adapter = OracleNeutralRangeAdapter()
        with pytest.raises(TypeConversionError):
            adapter.to_database(Range(1, 2), dict)


class TestMacAddressAdapter:
    def test_canonical_text_round_trip(self):
        adapter = OracleNeutralMacAddressAdapter()
        mac = MacAddress.parse("08:00:2B:01:02:03")
        assert adapter.to_database(mac, str) == "08:00:2b:01:02:03"
        assert adapter.from_database("08:00:2b:01:02:03", MacAddress) == mac

    def test_bytes_decode(self):
        adapter = OracleNeutralMacAddressAdapter()
        assert adapter.from_database(b"\x08\x00\x2b\x01\x02\x03", MacAddress) == (
            MacAddress.parse("08:00:2b:01:02:03")
        )

    def test_invalid_input_is_wrapped(self):
        adapter = OracleNeutralMacAddressAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database("08:00:2b:01:02", MacAddress)
        with pytest.raises(TypeConversionError):
            adapter.to_database(MacAddress.parse("08:00:2b:01:02:03"), int)
