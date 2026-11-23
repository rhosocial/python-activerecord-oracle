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
    IPAddress,
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
    OracleNeutralIPAddressAdapter,
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
    OracleNeutralIPAddressAdapter,
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


class TestIPAddressAdapter:
    @pytest.mark.parametrize(
        "text", ["192.0.2.1", "2001:db8::1", "::1", "2001:db8::ffff:192.0.2.1"]
    )
    def test_canonical_text_round_trip(self, text):
        adapter = OracleNeutralIPAddressAdapter()
        address = IPAddress.parse(text)
        wire = adapter.to_database(address, str)
        assert wire == str(address)
        assert adapter.from_database(wire, IPAddress) == address

    def test_invalid_input_is_wrapped(self):
        adapter = OracleNeutralIPAddressAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database("not-an-ip", IPAddress)
        with pytest.raises(TypeConversionError):
            adapter.from_database(42, IPAddress)


# ---------------------------------------------------------------------------
# Remaining branches: the refusals and pass-throughs the suites above do not
# reach. Each is a value the adapter must reject, or an already-neutral value
# it must hand back untouched.
# ---------------------------------------------------------------------------


class TestGeometryRefusals:
    def test_an_unknown_output_format_is_named(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="unknown geometry output format"):
            adapter.to_database(Point(1, 2), str, {"format": "bogus"})

    def test_a_concrete_geometry_is_carried_through_on_read(self):
        adapter = OracleNeutralGeometryAdapter()
        value = Point(1, 2)
        assert adapter.from_database(value, Geometry) is value

    def test_a_plain_mapping_is_not_a_geometry_wire_value(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="must use SDO_"):
            adapter.from_database({"type": "Point", "coordinates": [1, 2]}, Geometry)

    def test_a_non_geometry_value_is_refused(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="must decode from WKT/EWKT text"):
            adapter.from_database(42, Geometry)

    def test_an_sdo_without_both_coordinates_is_wrapped_on_write(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="X/Y coordinates"):
            adapter.to_database({"SDO_GTYPE": 2001, "SDO_POINT": {"X": 1.0}}, str)


class TestVectorRefusals:
    def test_the_wire_form_is_text_only(self):
        adapter = OracleNeutralVectorAdapter()
        with pytest.raises(TypeConversionError, match="target_type must be str"):
            adapter.to_database(Vector((1.0,)), bytes)

    def test_a_neutral_vector_is_carried_through_on_read(self):
        adapter = OracleNeutralVectorAdapter()
        value = Vector((1.0, 2.0))
        assert adapter.from_database(value, Vector) is value

    def test_bytes_and_mappings_are_not_vector_wire_forms(self):
        adapter = OracleNeutralVectorAdapter()
        for value in (b"[1.0]", {"values": [1.0]}):
            with pytest.raises(TypeConversionError, match="must decode from text"):
                adapter.from_database(value, Vector)

    def test_a_non_iterable_is_refused(self):
        adapter = OracleNeutralVectorAdapter()
        with pytest.raises(TypeConversionError, match="must decode from text"):
            adapter.from_database(42, Vector)


class TestBitStringRefusals:
    def test_a_neutral_bit_string_is_carried_through_on_read(self):
        adapter = OracleNeutralBitStringAdapter()
        value = BitString.from_bits("1010")
        assert adapter.from_database(value, BitString) is value

    def test_an_unterminated_literal_is_refused(self):
        adapter = OracleNeutralBitStringAdapter()
        with pytest.raises(TypeConversionError, match="unterminated"):
            adapter.from_database("B'1010", BitString)

    def test_a_literal_may_only_hold_binary_digits(self):
        adapter = OracleNeutralBitStringAdapter()
        with pytest.raises(TypeConversionError, match="may only contain"):
            adapter.from_database("B'10x1'", BitString)

    def test_a_short_literal_is_padded_to_the_declared_length(self):
        adapter = OracleNeutralBitStringAdapter()
        bits = adapter.from_database("B'101'", BitString, {"bit_length": 8})
        assert bits.to_bits() == "10100000"

    def test_a_literal_longer_than_the_declaration_is_refused(self):
        adapter = OracleNeutralBitStringAdapter()
        with pytest.raises(TypeConversionError, match="more than the declared"):
            adapter.from_database("B'1010'", BitString, {"bit_length": 3})

    def test_a_declared_length_must_be_a_positive_integer(self):
        adapter = OracleNeutralBitStringAdapter()
        with pytest.raises(TypeConversionError, match=r"must be int, got bool"):
            adapter.from_database("B'1010'", BitString, {"length": True})
        with pytest.raises(TypeConversionError, match="must be at least 1"):
            adapter.from_database("B'1010'", BitString, {"length": 0})


class TestRangeRefusals:
    def test_a_neutral_range_is_carried_through_on_read(self):
        adapter = OracleNeutralRangeAdapter()
        value = Range(1, 2, "[)")
        assert adapter.from_database(value, Range) is value

    def test_bytes_json_is_read(self):
        adapter = OracleNeutralRangeAdapter()
        payload = b'{"lower": 1, "upper": 2, "bounds": "[)", "empty": false, "element_type": "int"}'
        assert adapter.from_database(payload, Range) == Range(1, 2, "[)")

    def test_invalid_json_bytes_are_named(self):
        adapter = OracleNeutralRangeAdapter()
        with pytest.raises(TypeConversionError, match="invalid range JSON bytes"):
            adapter.from_database(b"not json", Range)

    def test_a_scalar_is_not_a_range_wire_value(self):
        adapter = OracleNeutralRangeAdapter()
        with pytest.raises(TypeConversionError, match="must decode from JSON text"):
            adapter.from_database(42, Range)

    def test_a_json_object_cannot_be_a_multirange(self):
        adapter = OracleNeutralRangeAdapter()
        payload = {"lower": 1, "upper": 2, "bounds": "[)", "empty": False, "element_type": "int"}
        with pytest.raises(TypeConversionError, match="represents a range, not a multirange"):
            adapter.from_database(payload, Multirange)

    def test_a_json_array_cannot_be_a_range(self):
        adapter = OracleNeutralRangeAdapter()
        payload = [{"lower": 1, "upper": 2, "bounds": "[)", "empty": False, "element_type": "int"}]
        with pytest.raises(TypeConversionError, match="represents a multirange, not a range"):
            adapter.from_database(payload, Range)


class TestMacAddressRefusals:
    def test_a_neutral_mac_is_carried_through_on_read(self):
        adapter = OracleNeutralMacAddressAdapter()
        value = MacAddress.parse("08:00:2b:01:02:03")
        assert adapter.from_database(value, MacAddress) is value

    def test_a_non_mac_value_is_refused(self):
        adapter = OracleNeutralMacAddressAdapter()
        with pytest.raises(TypeConversionError, match="must decode from text or bytes"):
            adapter.from_database(42, MacAddress)


class TestIPAddressRefusals:
    def test_a_non_text_value_is_refused(self):
        adapter = OracleNeutralIPAddressAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database(b"192.0.2.1", IPAddress)


# ---------------------------------------------------------------------------
# SDO decoding: the shapes the numeric rules refuse.
# ---------------------------------------------------------------------------


def _sdo(**overrides):
    """A minimally valid SDO_GEOMETRY mapping, with the test's own fields."""
    base = {"SDO_GTYPE": 2001, "SDO_SRID": None, "SDO_POINT": None,
            "SDO_ELEM_INFO": None, "SDO_ORDINATES": None}
    base.update(overrides)
    return base


class TestSdoDecodingRefusals:
    def test_an_odd_ordinate_count_is_refused(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="whole number of"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2002, SDO_ORDINATES=[0, 0, 1]), Geometry
            )

    def test_elem_info_must_be_triples(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="non-empty array of triples"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2003, SDO_ELEM_INFO=[1, 1003], SDO_ORDINATES=[0, 0, 0, 1]), Geometry
            )

    def test_an_offset_outside_the_ordinates_is_refused(self):
        """A later element's offset that lands before the current one is a
        broken SDO_ELEM_INFO, not an empty slice."""
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="points outside SDO_ORDINATES"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2003, SDO_ELEM_INFO=[5, 1003, 1, 2, 1003, 1],
                     SDO_ORDINATES=[0, 0, 0, 1, 1, 1, 0, 0]), Geometry
            )

    def test_an_open_ring_is_refused(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="ring is not closed"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2003, SDO_ELEM_INFO=[1, 1003, 1],
                     SDO_ORDINATES=[0, 0, 0, 1, 1, 1, 1, 0]), Geometry
            )

    def test_a_multipolygon_interior_needs_an_exterior_first(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="interior SDO ring appears without an exterior"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2007, SDO_ELEM_INFO=[1, 2003, 1],
                     SDO_ORDINATES=[0, 0, 0, 1, 1, 1, 0, 0]), Geometry
            )

    def test_a_collection_interior_needs_an_exterior_first(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="interior SDO ring appears without an exterior"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2004, SDO_ELEM_INFO=[1, 2003, 1],
                     SDO_ORDINATES=[0, 0, 0, 1, 1, 1, 0, 0]), Geometry
            )

    def test_a_point_with_z_is_not_2d(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="found a Z ordinate"):
            adapter.from_database(_sdo(SDO_POINT={"X": 1.0, "Y": 2.0, "Z": 3.0}), Geometry)

    def test_an_sdo_point_is_only_valid_for_a_point(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="only valid for point geometries"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2002, SDO_POINT={"X": 1.0, "Y": 2.0}), Geometry
            )

    def test_a_point_without_coordinates_or_ordinates_is_refused(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="neither an SDO_POINT nor SDO_ORDINATES"):
            adapter.from_database(_sdo(SDO_GTYPE=2002, SDO_ORDINATES=[]), Geometry)

    def test_a_point_sdo_must_hold_exactly_one_pair(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="exactly one ordinate pair"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2001, SDO_ORDINATES=[0, 0, 1, 1]), Geometry
            )

    def test_the_constructor_text_is_only_for_point_line_and_polygon(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="only available for Point, LineString"):
            adapter.to_database(
                GeometryCollection((Point(1, 2),)), str, {"format": "sdo_constructor"}
            )

    def test_the_constructor_text_covers_line_and_polygon(self):
        adapter = OracleNeutralGeometryAdapter()

        line = adapter.to_database(
            LineString(((0, 0), (1, 1))), str, {"format": "sdo_constructor"}
        )
        assert line.startswith("SDO_GEOMETRY(") and "SDO_ORDINATE_ARRAY(" in line

        polygon = adapter.to_database(
            Polygon((((0, 0), (4, 0), (4, 4), (0, 0)),)), str, {"format": "sdo_constructor"}
        )
        assert polygon.startswith("SDO_GEOMETRY(")

    def test_a_multipolygon_element_must_be_a_ring(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="unsupported SDO multipolygon element type"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2007, SDO_ELEM_INFO=[1, 1, 1],
                     SDO_ORDINATES=[0, 0]), Geometry
            )

    def test_a_collection_point_element_must_hold_one_pair(self):
        adapter = OracleNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError, match="exactly one ordinate pair"):
            adapter.from_database(
                _sdo(SDO_GTYPE=2004, SDO_ELEM_INFO=[1, 1, 1],
                     SDO_ORDINATES=[0, 0, 1, 1]), Geometry
            )

    def test_a_geometry_value_that_is_not_bytes_is_refused(self):
        adapter = OracleNeutralBitStringAdapter()
        with pytest.raises(TypeConversionError, match="expected RAW/BLOB bytes"):
            adapter.from_database(1.5, BitString)

    def test_a_mapping_without_a_target_type_decodes_by_its_shape(self):
        """A caller that passes no neutral target gets the shape the JSON has:
        an object is a range, an array is a multirange."""
        adapter = OracleNeutralRangeAdapter()

        range_payload = {"lower": 1, "upper": 2, "bounds": "[)", "empty": False,
                         "element_type": "int"}
        assert adapter.from_database(range_payload, str) == Range(1, 2, "[)")

        multirange_payload = [range_payload]
        assert adapter.from_database(multirange_payload, str) == Multirange((Range(1, 2, "[)"),))
