# src/rhosocial/activerecord/backend/impl/oracle/neutral_adapters.py
"""Oracle value adapters for the core neutral types.

These adapters use the driver-independent types from
``rhosocial.activerecord.backend.adapters`` and are separate from the legacy
``OracleVectorAdapter`` / ``OracleSDOGeometryAdapter``, whose Python surface is
the Oracle wrapper types.

Storage choices, and their honest limits:

- geometry: the default wire form is WKT (or ``SRID=n;WKT`` when the geometry
  has an SRID) for ``VARCHAR2``/``CLOB`` columns.  Oracle cannot bind a WKT
  string directly to a native ``SDO_GEOMETRY`` column: creating an SDO object
  from WKT either needs the connection's SDO object type (``conn.gettype``,
  a database round trip) or must happen server-side.  This adapter therefore
  does **not** claim native SDO binding.  ``options={"format":
  "sdo_constructor"}`` emits the ``SDO_GEOMETRY(...)`` SQL expression built
  from the existing ``type_values.spatial`` helpers; that text is an SQL
  expression, not a bind value, and the caller must place it in SQL.  Reading
  native values works: oracledb's SDO object, its dict form, and
  :class:`SDOGeometry` are all converted to the neutral shape.  Writes accept
  those same existing SDO values (no database round trip) and, with
  ``options={"format": "native"}``, bind an existing SDO object unchanged.
  Only 2D point/line/polygon/multipoint/multipolygon values and collections of
  points, lines and simple polygon rings are decoded; 3D and compound
  (``1005``) forms raise instead of being flattened.
- vector (23ai): Oracle ``VECTOR`` text form ``[1.0, 2.0, ...]``.
- bit string: raw bytes (``RAW``/``BLOB``) or canonical bit text
  (``VARCHAR2``/``CLOB``).  The binary carrier holds whole bytes only, so a
  non-byte-aligned length must come from the column metadata via
  ``options={"bit_length": n}``; the text carrier keeps the bit length itself.
- range / multirange: JSON text in ``VARCHAR2``/``CLOB``.
- MAC address: canonical lowercase colon-separated text.
"""

from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

from rhosocial.activerecord.backend.adapters import (
    BitString,
    BitStringAdapterBase,
    Geometry,
    GeometryAdapterBase,
    GeometryCollection,
    LineString,
    MacAddress,
    MacAddressAdapterBase,
    MultiPoint,
    MultiPolygon,
    Multirange,
    Point,
    Polygon,
    Range,
    RangeAdapterBase,
    Vector,
    VectorAdapterBase,
)

from .type_values.spatial import SDOPoint, SDOGeometry
from .type_values.vector import OracleVector

# --- Geometry --------------------------------------------------------------


def _sdo_value(container: Any, upper: str, lower: str) -> Any:
    """Read an SDO field from a dict or an object using either casing."""
    if isinstance(container, dict):
        if upper in container:
            return container[upper]
        return container.get(lower)
    value = getattr(container, upper, None)
    if value is None:
        value = getattr(container, lower, None)
    return value


def _sdo_point_coordinates(point: Any) -> Tuple[float, float, Optional[float]]:
    """Read ``(x, y, z)`` from ``SDOPoint`` or an oracledb SDO_POINT object."""
    if isinstance(point, SDOPoint):
        return point.x, point.y, point.z
    x = _sdo_value(point, "X", "x")
    y = _sdo_value(point, "Y", "y")
    z = _sdo_value(point, "Z", "z")
    if x is None or y is None:
        raise ValueError("SDO_POINT does not carry X/Y coordinates")
    return x, y, z


def _ordinate_pairs(ordinates: Sequence[Any]) -> Tuple[Tuple[float, float], ...]:
    """Group a flat ordinate array into 2D pairs."""
    values = list(ordinates)
    if len(values) % 2:
        raise ValueError(
            "SDO ordinates contain {} values, which is not a whole number of "
            "2D pairs".format(len(values))
        )
    return tuple((float(values[i]), float(values[i + 1])) for i in range(0, len(values), 2))


def _sdo_element_slices(
    elem_info: Sequence[Any], ordinates: Sequence[Any]
) -> List[Tuple[int, List[Any]]]:
    """Split SDO_ORDINATES into ``(element_type, ordinates)`` slices.

    SDO_ELEM_INFO is a flat array of triples ``(offset, element_type,
    interpretation)``; ``offset`` is 1-based into SDO_ORDINATES and the next
    element's offset ends the current slice.
    """
    elements = list(elem_info)
    if not elements or len(elements) % 3:
        raise ValueError("SDO_ELEM_INFO must be a non-empty array of triples")

    slices: List[Tuple[int, List[Any]]] = []
    for index in range(0, len(elements), 3):
        offset, element_type, _interpretation = (
            int(elements[index]),
            int(elements[index + 1]),
            int(elements[index + 2]),
        )
        start = offset - 1
        if index + 3 < len(elements):
            end = int(elements[index + 3]) - 1
        else:
            end = len(ordinates)
        if start < 0 or end < start or end > len(ordinates):
            raise ValueError("SDO_ELEM_INFO offset points outside SDO_ORDINATES")
        slices.append((element_type, list(ordinates[start:end])))
    return slices


def _ring_from_ordinates(values: Sequence[Any]) -> Tuple[Tuple[float, float], ...]:
    """Convert one element's ordinates into a closed 2D ring."""
    ring = _ordinate_pairs(values)
    if ring and ring[0] != ring[-1]:
        raise ValueError("SDO polygon ring is not closed")
    return ring


def _polygon_rings_from_slices(
    slices: Sequence[Tuple[int, List[Any]]],
) -> Tuple[Tuple[Tuple[float, float], ...], ...]:
    """Convert ring elements (1003/2003) into one polygon's rings."""
    rings: List[Tuple[Tuple[float, float], ...]] = []
    for element_type, values in slices:
        if element_type not in (1003, 2003):
            raise ValueError(
                "unsupported SDO polygon element type {}; only simple exterior/interior "
                "rings (1003/2003) are decoded".format(element_type)
            )
        rings.append(_ring_from_ordinates(values))
    if not rings:
        raise ValueError("SDO polygon contains no ring")
    return tuple(rings)


def _polygons_from_ring_slices(
    slices: Sequence[Tuple[int, List[Any]]],
) -> Tuple[Tuple[Tuple[Tuple[float, float], ...], ...], ...]:
    """Group exterior (1003) and interior (2003) rings into polygons."""
    polygons: List[List[Tuple[Tuple[float, float], ...]]] = []
    current: List[Tuple[Tuple[float, float], ...]] = []
    for element_type, values in slices:
        if element_type == 1003:
            if current:
                polygons.append(current)
            current = [_ring_from_ordinates(values)]
        elif element_type == 2003:
            if not current:
                raise ValueError("an interior SDO ring appears without an exterior ring")
            current.append(_ring_from_ordinates(values))
        else:
            raise ValueError(
                "unsupported SDO multipolygon element type {}; only rings "
                "(1003/2003) are decoded".format(element_type)
            )
    if current:
        polygons.append(current)
    if not polygons:
        raise ValueError("SDO multipolygon contains no polygon")
    return tuple(tuple(polygon) for polygon in polygons)


def _sdo_collection_from_slices(
    slices: Sequence[Tuple[int, List[Any]]], srid: Optional[int]
) -> GeometryCollection:
    """Build a neutral collection from SDO collection elements."""
    geometries: List[Geometry] = []
    pending_rings: List[Tuple[Tuple[float, float], ...]] = []

    def flush_polygon() -> None:
        if pending_rings:
            geometries.append(Polygon(tuple(pending_rings)))
            pending_rings.clear()

    for element_type, values in slices:
        if element_type == 1:
            flush_polygon()
            pairs = _ordinate_pairs(values)
            if len(pairs) != 1:
                raise ValueError("a point element must contain exactly one ordinate pair")
            geometries.append(Point(pairs[0][0], pairs[0][1]))
        elif element_type == 2:
            flush_polygon()
            geometries.append(LineString(_ordinate_pairs(values)))
        elif element_type == 1003:
            flush_polygon()
            pending_rings.append(_ring_from_ordinates(values))
        elif element_type == 2003:
            if not pending_rings:
                raise ValueError("an interior SDO ring appears without an exterior ring")
            pending_rings.append(_ring_from_ordinates(values))
        else:
            raise ValueError(
                "unsupported SDO collection element type {}; only points (1), lines "
                "(2) and simple polygon rings (1003/2003) are decoded".format(element_type)
            )

    flush_polygon()
    if not geometries:
        raise ValueError("SDO geometry collection contains no element")
    return GeometryCollection(tuple(geometries), srid=srid)


def _sdo_to_geometry(sdo: SDOGeometry) -> Geometry:
    """Convert an :class:`SDOGeometry` into a neutral geometry."""
    gtype = int(sdo.sdo_gtype or 0)
    srid = sdo.sdo_srid
    if gtype // 1000 != 2:
        raise ValueError("only 2D SDO_GEOMETRY values are supported; got gtype {}".format(gtype))

    kind = gtype % 100
    if sdo.sdo_point is not None:
        x, y, z = _sdo_point_coordinates(sdo.sdo_point)
        if z is not None:
            raise ValueError("only 2D SDO_GEOMETRY values are supported; found a Z ordinate")
        if kind not in (0, 1):
            raise ValueError(
                "an SDO_POINT is only valid for point geometries; got gtype {}".format(gtype)
            )
        return Point(x, y, srid=srid)

    ordinates = list(sdo.sdo_ordinates)
    if not ordinates:
        raise ValueError("SDO_GEOMETRY has neither an SDO_POINT nor SDO_ORDINATES")

    if kind == 1:
        pairs = _ordinate_pairs(ordinates)
        if len(pairs) != 1:
            raise ValueError("a point SDO_GEOMETRY must contain exactly one ordinate pair")
        return Point(pairs[0][0], pairs[0][1], srid=srid)
    if kind == 2:
        return LineString(_ordinate_pairs(ordinates), srid=srid)
    if kind == 5:
        return MultiPoint(_ordinate_pairs(ordinates), srid=srid)

    slices = _sdo_element_slices(sdo.sdo_elem_info, ordinates)
    if kind == 3:
        return Polygon(_polygon_rings_from_slices(slices), srid=srid)
    if kind == 7:
        return MultiPolygon(_polygons_from_ring_slices(slices), srid=srid)
    if kind == 4:
        return _sdo_collection_from_slices(slices, srid)

    raise ValueError(
        "SDO_GEOMETRY type {} is not decoded; only 2D points, lines, polygons, "
        "multipoints, multipolygons and collections of those are covered".format(gtype)
    )


def _looks_like_sdo(value: Any) -> bool:
    """Return whether ``value`` is an existing SDO geometry (any of its forms)."""
    if isinstance(value, SDOGeometry):
        return True
    if isinstance(value, dict):
        return any(key in value for key in ("SDO_GTYPE", "sdo_gtype"))
    return _sdo_value(value, "SDO_GTYPE", "sdo_gtype") is not None


def _sdo_from_value(value: Any) -> SDOGeometry:
    """Normalize an :class:`SDOGeometry`, a dict, or an oracledb SDO object."""
    if isinstance(value, SDOGeometry):
        return value
    sdo_point = _sdo_value(value, "SDO_POINT", "sdo_point")
    return SDOGeometry(
        sdo_gtype=int(_sdo_value(value, "SDO_GTYPE", "sdo_gtype")),
        sdo_srid=_sdo_value(value, "SDO_SRID", "sdo_srid"),
        sdo_point=_to_sdo_point(sdo_point) if sdo_point is not None else None,
        sdo_elem_info=list(_sdo_value(value, "SDO_ELEM_INFO", "sdo_elem_info") or []),
        sdo_ordinates=list(_sdo_value(value, "SDO_ORDINATES", "sdo_ordinates") or []),
    )


def _geometry_to_sdo_constructor(geometry: Geometry) -> str:
    """Build the ``SDO_GEOMETRY(...)`` SQL expression for a neutral geometry.

    This is an SQL expression, not a bindable value; see the module docstring.
    """
    if isinstance(geometry, Point):
        sdo = SDOGeometry.point(geometry.x, geometry.y, srid=geometry.srid)
    elif isinstance(geometry, LineString):
        sdo = SDOGeometry.line(list(geometry.points), srid=geometry.srid)
    elif isinstance(geometry, Polygon):
        exterior = list(geometry.rings[0])
        interiors = [list(ring) for ring in geometry.rings[1:]]
        sdo = SDOGeometry.polygon(exterior, interiors, srid=geometry.srid)
    else:
        raise ValueError(
            "native SDO constructor text is only available for Point, LineString "
            "and Polygon; store the WKT text for {}".format(type(geometry).__name__)
        )
    return sdo.to_constructor_sql()


def _ewkt(geometry: Geometry, force_srid: Optional[bool]) -> str:
    """Render WKT or EWKT depending on the SRID and the caller's preference."""
    wkt = geometry.to_wkt()
    if force_srid is False or geometry.srid is None:
        return wkt
    return "SRID={};{}".format(geometry.srid, wkt)


class OracleNeutralGeometryAdapter(GeometryAdapterBase):
    """Neutral geometry adapter for Oracle text columns (WKT/EWKT).

    The default encoding targets ``VARCHAR2``/``CLOB`` columns.  It does not
    bind to a native ``SDO_GEOMETRY`` column: see the module docstring for the
    database round trip that native binding would require.  ``options={"format":
    "sdo_constructor"}`` returns the ``SDO_GEOMETRY(...)`` SQL expression for
    the caller to embed in SQL, still not a bind parameter.

    Decoding accepts WKT/EWKT text, a dict or oracledb-style SDO object, and
    :class:`SDOGeometry`; the result is always a neutral geometry.
    """

    def __init__(self) -> None:
        super().__init__()
        self.register_type(Geometry, str)

    def _do_to_database(
        self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]
    ) -> Any:
        """Bind an existing SDO object unchanged when ``format="native"``.

        A native SDO value cannot be constructed from WKT without a database
        connection, but an SDO object already obtained from the connection can
        be bound as-is.  Without that option an SDO value is converted to the
        neutral geometry and encoded as text.
        """
        options_dict = options or {}
        output_format = str(options_dict.get("format", "wkt")).lower()
        if output_format in ("native", "sdo_object", "object"):
            if _looks_like_sdo(value):
                return value
            self._conversion_error(
                value,
                "native SDO output requires an existing SDOGeometry or oracledb SDO "
                "object; constructing one from WKT needs a database connection",
            )
        return super()._do_to_database(value, target_type, options)

    def _coerce_geometry(self, value: Any) -> Geometry:
        """Accept an existing SDO geometry on write without a database round trip."""
        if _looks_like_sdo(value):
            try:
                return _sdo_to_geometry(_sdo_from_value(value))
            except (TypeError, ValueError) as exc:
                self._conversion_error(value, str(exc))
        return super()._coerce_geometry(value)

    def _encode(self, geometry: Geometry, target_type: Type, options: Dict[str, Any]) -> str:
        if target_type is not str:
            raise TypeError(
                "Oracle neutral geometry encodes as WKT/EWKT text or SDO constructor "
                "SQL; target_type must be str"
            )

        output_format = str(options.get("format", "wkt")).lower()
        if output_format in ("sdo", "sdo_constructor", "constructor"):
            return _geometry_to_sdo_constructor(geometry)
        if output_format not in ("wkt", "ewkt", "text"):
            raise ValueError("unknown geometry output format: {!r}".format(output_format))

        force_srid = options.get("include_srid")
        return _ewkt(geometry, force_srid)

    def _decode(self, value: Any, options: Dict[str, Any]) -> Geometry:
        if isinstance(value, Geometry) and type(value) is not Geometry:
            return value
        if _looks_like_sdo(value):
            return _sdo_to_geometry(_sdo_from_value(value))
        if isinstance(value, dict):
            raise TypeError("dictionary geometry values must use SDO_* field names")
        if isinstance(value, str):
            return Geometry.from_wkt(value)
        raise TypeError(
            "Oracle geometry must decode from WKT/EWKT text, SDO_GEOMETRY, or a "
            "neutral Geometry, got {}".format(type(value).__name__)
        )


def _to_sdo_point(point: Any) -> SDOPoint:
    """Normalize an already-read SDO_POINT object into :class:`SDOPoint`."""
    x, y, z = _sdo_point_coordinates(point)
    return SDOPoint(x=x, y=y, z=z)


# --- Vector ----------------------------------------------------------------


def _oracle_vector_text(vector: Vector) -> str:
    """Render the Oracle VECTOR text form."""
    for value in vector.values:
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(
                "Oracle VECTOR has no spelling for NaN or Infinity; the vector "
                "contains a non-finite component"
            )
    return "[" + ", ".join(repr(float(value)) for value in vector.values) + "]"


def _parse_oracle_vector_text(text: str) -> Vector:
    stripped = text.strip()
    if not stripped.startswith("[") or not stripped.endswith("]"):
        raise ValueError("Oracle VECTOR text must look like [v1, v2, ...]")
    inner = stripped[1:-1].strip()
    if not inner:
        raise ValueError("Oracle VECTOR text must contain at least one component")
    values: List[float] = []
    for part in inner.split(","):
        token = part.strip()
        if not token:
            raise ValueError("Oracle VECTOR text contains an empty component")
        try:
            values.append(float(token))
        except ValueError:
            raise ValueError("invalid Oracle VECTOR component: {!r}".format(token)) from None
    return Vector(tuple(values))


class OracleNeutralVectorAdapter(VectorAdapterBase):
    """Encode neutral vectors as Oracle 23ai ``VECTOR`` text.

    Oracle's text form is ``[1.0, 2.0, ...]``.  Non-finite components are
    rejected on write because Oracle ``VECTOR`` has no NaN/Infinity spelling.
    Reads accept text, a Python sequence, and :class:`OracleVector`.
    """

    def __init__(self) -> None:
        super().__init__()
        self.register_type(Vector, str)

    def _encode(self, vector: Vector, target_type: Type, options: Optional[Dict[str, Any]]) -> str:
        if target_type is not str:
            raise TypeError("Oracle VECTOR binds as text; target_type must be str")
        return _oracle_vector_text(vector)

    def _decode(self, value: Any, options: Optional[Dict[str, Any]]) -> Vector:
        if isinstance(value, Vector):
            return value
        if isinstance(value, OracleVector):
            return Vector(tuple(value.values))
        if isinstance(value, str):
            return _parse_oracle_vector_text(value)
        if isinstance(value, (bytes, bytearray, dict)):
            raise TypeError(
                "Oracle VECTOR must decode from text, a sequence, or OracleVector, got "
                "{}".format(type(value).__name__)
            )
        # oracledb returns a native VECTOR column as ``array.array`` of floats
        # (and other drivers may return a memoryview or another sequence), so
        # any non-text iterable of components is accepted.
        if isinstance(value, (list, tuple)) or hasattr(value, '__iter__'):
            return Vector.from_json_value(list(value))
        raise TypeError(
            "Oracle VECTOR must decode from text, a sequence, or OracleVector, got "
            "{}".format(type(value).__name__)
        )


# --- Bit string ------------------------------------------------------------


def _declared_bit_length(options: Optional[Dict[str, Any]]) -> Optional[int]:
    """Read the declared bit length from options (RAW carries whole bytes only)."""
    if not options:
        return None
    for key in ("bit_length", "length"):
        raw = options.get(key)
        if raw is None:
            continue
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise ValueError("{} must be int, got {}".format(key, type(raw).__name__))
        if raw < 1:
            raise ValueError("{} must be at least 1, got {}".format(key, raw))
        return raw
    return None


def _read_bytes(value: Any) -> bytes:
    """Return bytes from RAW/BLOB values, reading oracledb LOB wrappers."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    reader = getattr(value, "read", None)
    if callable(reader):
        data = reader()
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
    raise TypeError("expected RAW/BLOB bytes, got {}".format(type(value).__name__))


class OracleNeutralBitStringAdapter(BitStringAdapterBase):
    """Encode neutral bit strings as Oracle ``RAW``/``BLOB`` bytes or text.

    The default wire form is ``RAW``/``BLOB`` bytes: a ``RAW`` column holds
    whole bytes, so ``bit_length`` must be supplied through
    ``options={"bit_length": n}`` when it is not a multiple of eight; otherwise
    the length decoded is the byte length.  A ``VARCHAR2``/``CLOB`` carrier
    binds the bit text instead (``"00101"``), which keeps the bit length in the
    text itself and is the form the shared suite uses.  Reading also accepts an
    oracledb LOB wrapper.
    """

    def __init__(self) -> None:
        super().__init__()
        self.register_type(BitString, bytes)
        self.register_type(BitString, str)

    def _encode(
        self, bits: BitString, target_type: Type, options: Optional[Dict[str, Any]]
    ) -> Any:
        if target_type is bytes:
            return bits.data
        if target_type is str:
            return bits.to_bits()
        raise TypeError(
            "Oracle bit strings are stored as RAW/BLOB bytes or canonical bit "
            "text; target_type must be bytes or str"
        )

    def _decode(self, value: Any, options: Optional[Dict[str, Any]]) -> BitString:
        if isinstance(value, BitString):
            return value
        if isinstance(value, str):
            text = value.strip()
            if len(text) >= 3 and text[0] in ("B", "b") and text[1] == "'":
                if not text.endswith("'"):
                    raise ValueError("unterminated B'...' bit string")
                text = text[2:-1]
            if not text or any(char not in "01" for char in text):
                raise ValueError("bit string text may only contain '0' and '1'")
            declared = _declared_bit_length(options)
            if declared is not None:
                if len(text) > declared:
                    raise ValueError(
                        "bit string text has {} bits, more than the declared {}".format(
                            len(text), declared
                        )
                    )
                text = text + "0" * (declared - len(text))
            return BitString.from_bits(text)

        raw = _read_bytes(value)
        if not raw:
            raise ValueError("bit string RAW value must not be empty")
        return BitString.from_bytes(raw, _declared_bit_length(options))


# --- Range / multirange ----------------------------------------------------


def _range_json_text(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


class OracleNeutralRangeAdapter(RangeAdapterBase):
    """Encode neutral ranges as JSON text for ``VARCHAR2``/``CLOB``.

    The JSON document is the neutral, lossless representation (``element_type``
    keeps a ``date`` endpoint distinguishable from a ``datetime``).  Reads
    accept the JSON text, a decoded ``dict``/``list``, and an oracledb LOB
    wrapper.  Oracle has no native range type, so this is a text-carried
    contract rather than a native bind.
    """

    def __init__(self) -> None:
        super().__init__()
        self.register_type(Range, str)
        self.register_type(Multirange, str)

    def _encode(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> str:
        if target_type is not str:
            raise TypeError("Oracle range/multirange JSON is text; target_type must be str")
        return _range_json_text(value.to_json_value())

    def _decode(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, (Range, Multirange)):
            return value

        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, ValueError) as exc:
                raise ValueError("invalid range JSON text: {}".format(exc)) from exc
        elif isinstance(value, (bytes, bytearray)):
            try:
                value = json.loads(bytes(value).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                raise ValueError("invalid range JSON bytes: {}".format(exc)) from exc
        else:
            reader = getattr(value, "read", None)
            if callable(reader):
                return self._decode(reader(), target_type, options)
            if not isinstance(value, (dict, list)):
                raise TypeError(
                    "Oracle range/multirange must decode from JSON text or a "
                    "dict/list, got {}".format(type(value).__name__)
                )

        if target_type is Multirange:
            if isinstance(value, dict):
                raise ValueError("a JSON object represents a range, not a multirange")
            return Multirange.from_json_value(value)
        if target_type is Range:
            if isinstance(value, list):
                raise ValueError("a JSON array represents a multirange, not a range")
            return Range.from_json_value(value)
        if isinstance(value, dict):
            return Range.from_json_value(value)
        return Multirange.from_json_value(value)


# --- MAC address -----------------------------------------------------------


class OracleNeutralMacAddressAdapter(MacAddressAdapterBase):
    """Encode neutral MAC addresses as canonical text (``VARCHAR2``/``RAW``)."""

    def __init__(self) -> None:
        super().__init__()
        self.register_type(MacAddress, str)

    def _encode(self, mac: MacAddress, target_type: Type, options: Optional[Dict[str, Any]]) -> str:
        if target_type is not str:
            raise TypeError(
                "Oracle MAC addresses encode as canonical text; target_type must be str"
            )
        return str(mac)

    def _decode(self, value: Any, options: Optional[Dict[str, Any]]) -> MacAddress:
        if isinstance(value, MacAddress):
            return value
        if isinstance(value, (str, bytes, bytearray)):
            return MacAddress.parse(value)
        raise TypeError(
            "Oracle MAC address must decode from text or bytes, got "
            "{}".format(type(value).__name__)
        )


#: Neutral adapters registered by the Oracle backend alongside the legacy set.
neutral_adapters = [
    OracleNeutralGeometryAdapter,
    OracleNeutralVectorAdapter,
    OracleNeutralBitStringAdapter,
    OracleNeutralRangeAdapter,
    OracleNeutralMacAddressAdapter,
]

__all__ = [
    "OracleNeutralBitStringAdapter",
    "OracleNeutralGeometryAdapter",
    "OracleNeutralMacAddressAdapter",
    "OracleNeutralRangeAdapter",
    "OracleNeutralVectorAdapter",
    "neutral_adapters",
]
