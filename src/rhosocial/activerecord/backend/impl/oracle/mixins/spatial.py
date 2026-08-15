# src/rhosocial/activerecord/backend/impl/oracle/mixins/spatial.py
from typing import Any


class OracleSpatialMixin(object):
    """Oracle Spatial (SDO_GEOMETRY) dialect mixin.

    Oracle Spatial is the de-facto richest commercial spatial database,
    exposing geometry storage through the ``SDO_GEOMETRY`` object type in
    the ``MDSYS`` schema and spatial indexing via the ``MDSYS.SPATIAL_INDEX``
    indextype. This mixin provides capability queries and DDL/expression
    formatting helpers that the dialect layer can delegate to.
    """

    def supports_spatial_type(self) -> bool:
        """Return whether SDO_GEOMETRY spatial types are supported."""
        return True

    def supports_spatial_index(self) -> bool:
        """Return whether spatial indexes are supported.

        Oracle Spatial indexes are created through the DDL clause
        ``INDEXTYPE IS MDSYS.SPATIAL_INDEX`` on a spatially-typed column.
        """
        return True

    def supports_srs(self) -> bool:
        """Return whether an SRID coordinate reference system is supported.

        The ``SDO_GEOMETRY`` object stores the coordinate reference system
        identifier in its embedded ``SDO_SRID`` attribute.
        """
        return True

    def supports_3d_geometry(self) -> bool:
        """Return whether 3D geometry storage is supported."""
        return True

    def supports_geodetic_index(self) -> bool:
        """Return whether geodetic (lon/lat) spatial indexes are supported."""
        return True

    def format_spatial_literal(self, geom: Any) -> str:
        """Format a Python geometry value as an SDO_GEOMETRY SQL literal.

        When the supplied object exposes an ``sdo_geom_to_wkt`` helper, the
        serialized well-known-text representation is wrapped through the
        ``MDSYS.SDO_GEOMETRY`` constructor. Otherwise the value is coerced
        to its string form, which is expected to be already valid SQL.
        """
        if geom is None:
            return "NULL"
        if hasattr(geom, "sdo_geom_to_wkt"):
            wkt = geom.sdo_geom_to_wkt()
            return f"MDSYS.SDO_GEOMETRY('{wkt}')"
        return str(geom)

    def format_spatial_function(self, name: str, *args) -> str:
        """Format a generic Oracle Spatial function invocation.

        Spatial operators and functions in Oracle live under the ``MDSYS``
        schema (e.g. ``MDSYS.SDO_CONTAINS``, ``MDSYS.SDO_ANYINTERACT``).
        The supplied function name is uppercased and joined with its
        argument list to form the invocation text.
        """
        return f"MDSYS.{name.upper()}({', '.join(args)})"

    def format_st_function(self, pg_name: str, *args) -> str:
        """Map a PostGIS-style ``ST_*`` function name onto Oracle Spatial.

        A small lookup table translates the common PostGIS predicate and
        measurement functions to their Oracle Spatial equivalents. Any
        unmapped name is passed through prefixed with the ``MDSYS`` schema
        qualifier so callers can still target Oracle packages such as
        ``MDSYS.SDO_GEOM``.
        """
        mapping = {
            "ST_Distance": "SDO_GEOM.SDO_DISTANCE",
            "ST_Contains": "SDO_CONTAINS",
            "ST_Within": "SDO_INSIDE",
            "ST_Intersects": "SDO_ANYINTERACT",
            "ST_Equals": "SDO_EQUAL",
        }
        oracle_name = mapping.get(pg_name, pg_name)
        if oracle_name not in mapping.values():
            oracle_name = f"MDSYS.{oracle_name}"
        return f"{oracle_name}({', '.join(args)})"

    def format_spatial_index_options(self, options) -> str:
        """Format the DDL clause appended to a CREATE INDEX statement.

        Oracle Spatial indexes are materialized by declaring the
        ``MDSYS.SPATIAL_INDEX`` indextype. Optional indextype parameters
        (e.g. ``layer_gtype=POINT``) are forwarded as ``PARAMETERS (...)``
        when a non-empty ``options`` mapping is supplied.
        """
        clause = "INDEXTYPE IS MDSYS.SPATIAL_INDEX"
        if options:
            if isinstance(options, dict):
                pairs = [f"'{key}={value}'" for key, value in options.items()]
                params = " ".join(pairs)
            else:
                params = str(options)
            clause = f"{clause} PARAMETERS ({params})"
        return clause
