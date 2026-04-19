# types/spatial.py
"""
Oracle SDO_GEOMETRY spatial type definitions.

Oracle Spatial provides the SDO_GEOMETRY type for storing
and manipulating spatial data (points, lines, polygons, etc.).
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from enum import IntEnum


class SDOGeometryType(IntEnum):
    """SDO_GTYPE values for different geometry types.
    
    Format: DLTT where:
    - D: Dimension (1, 2, 3, or 4)
    - L: LRS measure (usually 0)
    - TT: Geometry type (01-09)
    
    Common values:
    - 2001: 2D point
    - 2002: 2D line
    - 2003: 2D polygon
    - 2005: 2D multipoint
    - 2006: 2D multiline
    - 2007: 2D multipolygon
    """
    UNKNOWN = 0
    POINT = 1
    LINE = 2
    POLYGON = 3
    COLLECTION = 4
    MULTIPOINT = 5
    MULTILINE = 6
    MULTIPOLYGON = 7
    SOLID = 8
    MULTISOLID = 9
    
    # Convenience 2D types
    POINT_2D = 2001
    LINE_2D = 2002
    POLYGON_2D = 2003
    COLLECTION_2D = 2004
    MULTIPOINT_2D = 2005
    MULTILINE_2D = 2006
    MULTIPOLYGON_2D = 2007
    
    # Convenience 3D types
    POINT_3D = 3001
    LINE_3D = 3002
    POLYGON_3D = 3003


@dataclass
class SDOPoint:
    """SDO_POINT_TYPE for point geometries.
    
    Used for optimized storage of point geometries.
    When a geometry is a point, it can be stored in
    SDO_POINT instead of SDO_ELEM_INFO/SDO_ORDINATES.
    
    Attributes:
        x: X coordinate (longitude)
        y: Y coordinate (latitude)
        z: Z coordinate (elevation, optional)
    """
    x: float
    y: float
    z: Optional[float] = None
    
    def to_tuple(self) -> Tuple[float, ...]:
        """Convert to tuple representation.
        
        Returns:
            Tuple of (x, y) or (x, y, z)
        """
        if self.z is not None:
            return (self.x, self.y, self.z)
        return (self.x, self.y)
    
    def to_dict(self) -> dict:
        """Convert to dictionary.
        
        Returns:
            Dictionary with coordinates
        """
        result = {'X': self.x, 'Y': self.y}
        if self.z is not None:
            result['Z'] = self.z
        return result
    
    @classmethod
    def from_dict(cls, d: dict) -> 'SDOPoint':
        """Create from dictionary.
        
        Args:
            d: Dictionary with X, Y, and optionally Z
            
        Returns:
            New SDOPoint instance
        """
        return cls(
            x=d.get('X', 0.0),
            y=d.get('Y', 0.0),
            z=d.get('Z')
        )


@dataclass
class SDOGeometry:
    """Oracle SDO_GEOMETRY spatial type.
    
    SDO_GEOMETRY is Oracle's native spatial data type for
    storing points, lines, polygons, and other geometries.
    
    Example:
        >>> point = SDOGeometry.point(10.0, 20.0)
        >>> point.geometry_type
        <SDOGeometryType.POINT: 1>
        
        >>> polygon = SDOGeometry.polygon(
        ...     [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        ... )
    
    Attributes:
        sdo_gtype: Geometry type code (e.g., 2001 for 2D point)
        sdo_srid: Spatial reference system ID (SRID)
        sdo_point: Point coordinates (for point geometries)
        sdo_elem_info: Element info array describing geometry structure
        sdo_ordinates: Coordinate array
    """
    sdo_gtype: int
    sdo_srid: Optional[int] = None
    sdo_point: Optional[SDOPoint] = None
    sdo_elem_info: List[int] = field(default_factory=list)
    sdo_ordinates: List[float] = field(default_factory=list)
    
    @property
    def geometry_type(self) -> SDOGeometryType:
        """Get the geometry type (last 2 digits of gtype).
        
        Returns:
            SDOGeometryType enum value
        """
        return SDOGeometryType(self.sdo_gtype % 100)
    
    @property
    def dimension(self) -> int:
        """Get the dimensionality.
        
        Returns:
            Number of dimensions (1-4)
        """
        return self.sdo_gtype // 1000
    
    @property
    def is_lrs(self) -> bool:
        """Check if this is a Linear Referencing System geometry.
        
        Returns:
            True if LRS geometry
        """
        return (self.sdo_gtype // 100) % 10 == 1
    
    @property
    def is_point(self) -> bool:
        """Check if this is a point geometry."""
        return self.geometry_type == SDOGeometryType.POINT
    
    @property
    def is_polygon(self) -> bool:
        """Check if this is a polygon geometry."""
        return self.geometry_type == SDOGeometryType.POLYGON
    
    @property
    def is_line(self) -> bool:
        """Check if this is a line geometry."""
        return self.geometry_type == SDOGeometryType.LINE
    
    def to_constructor_sql(self) -> str:
        """Generate SDO_GEOMETRY constructor SQL.
        
        Returns:
            SQL string for creating geometry
        """
        parts = [str(self.sdo_gtype)]
        
        # SRID
        parts.append('NULL' if self.sdo_srid is None else str(self.sdo_srid))
        
        # Point
        if self.sdo_point:
            z = str(self.sdo_point.z) if self.sdo_point.z is not None else 'NULL'
            parts.append(f"SDO_POINT_TYPE({self.sdo_point.x}, {self.sdo_point.y}, {z})")
        else:
            parts.append("NULL")
        
        # Elem info
        if self.sdo_elem_info:
            parts.append(f"SDO_ELEM_INFO_ARRAY({', '.join(map(str, self.sdo_elem_info))})")
        else:
            parts.append("NULL")
        
        # Ordinates
        if self.sdo_ordinates:
            parts.append(f"SDO_ORDINATE_ARRAY({', '.join(map(str, self.sdo_ordinates))})")
        else:
            parts.append("NULL")
        
        return f"SDO_GEOMETRY({', '.join(parts)})"
    
    @classmethod
    def point(cls, x: float, y: float, z: Optional[float] = None,
              srid: Optional[int] = None) -> 'SDOGeometry':
        """Create a point geometry.
        
        Args:
            x: X coordinate
            y: Y coordinate
            z: Z coordinate (optional)
            srid: Spatial reference ID
            
        Returns:
            New SDOGeometry instance
        """
        gtype = 3001 if z is not None else 2001
        return cls(
            sdo_gtype=gtype,
            sdo_srid=srid,
            sdo_point=SDOPoint(x=x, y=y, z=z)
        )
    
    @classmethod
    def line(cls, coords: List[Tuple[float, float]],
             srid: Optional[int] = None) -> 'SDOGeometry':
        """Create a line geometry from coordinate list.
        
        Args:
            coords: List of (x, y) tuples
            srid: Spatial reference ID
            
        Returns:
            New SDOGeometry instance
        """
        if len(coords) < 2:
            raise ValueError("Line requires at least 2 points")
        
        ordinates = [c for coord in coords for c in coord]
        elem_info = [1, 2, 1]  # offset, type=LINE, interpretation
        
        return cls(
            sdo_gtype=2002,
            sdo_srid=srid,
            sdo_elem_info=elem_info,
            sdo_ordinates=ordinates
        )
    
    @classmethod
    def polygon(cls, exterior: List[Tuple[float, float]],
                interiors: Optional[List[List[Tuple[float, float]]]] = None,
                srid: Optional[int] = None) -> 'SDOGeometry':
        """Create a polygon geometry.
        
        Args:
            exterior: Exterior ring coordinates (closed)
            interiors: List of interior ring coordinates (holes)
            srid: Spatial reference ID
            
        Returns:
            New SDOGeometry instance
        """
        if len(exterior) < 4:
            raise ValueError("Polygon requires at least 4 points (closed ring)")
        
        elem_info = [1, 1003, 1]  # Exterior: offset=1, type=EXT_RING, interpretation
        ordinates = [c for coord in exterior for c in coord]
        
        if interiors:
            for interior in interiors:
                offset = len(ordinates) + 1
                elem_info.extend([offset, 2003, 1])  # Interior ring
                ordinates.extend([c for coord in interior for c in coord])
        
        return cls(
            sdo_gtype=2003,
            sdo_srid=srid,
            sdo_elem_info=elem_info,
            sdo_ordinates=ordinates
        )
    
    @classmethod
    def from_dict(cls, d: dict) -> 'SDOGeometry':
        """Create from dictionary (oracledb output format).
        
        Args:
            d: Dictionary with SDO_GEOMETRY fields
            
        Returns:
            New SDOGeometry instance
        """
        sdo_point = None
        if d.get('SDO_POINT'):
            sdo_point = SDOPoint.from_dict(d['SDO_POINT'])
        
        return cls(
            sdo_gtype=d.get('SDO_GTYPE', 0),
            sdo_srid=d.get('SDO_SRID'),
            sdo_point=sdo_point,
            sdo_elem_info=d.get('SDO_ELEM_INFO', []),
            sdo_ordinates=d.get('SDO_ORDINATES', [])
        )
    
    @classmethod
    def from_wkt(cls, wkt: str, srid: Optional[int] = None) -> 'SDOGeometry':
        """Create from WKT (Well-Known Text) format.
        
        Args:
            wkt: WKT string (e.g., 'POINT(10 20)')
            srid: Spatial reference ID
            
        Returns:
            New SDOGeometry instance
        """
        # Basic WKT parsing - for production use a proper WKT parser
        wkt = wkt.strip().upper()
        
        if wkt.startswith('POINT'):
            # POINT(x y) or POINT(x y z)
            coords_str = wkt[wkt.index('(') + 1:wkt.index(')')]
            coords = [float(c) for c in coords_str.split()]
            if len(coords) >= 3:
                return cls.point(coords[0], coords[1], coords[2], srid)
            return cls.point(coords[0], coords[1], srid=srid)
        
        # Add more WKT types as needed
        raise ValueError(f"Unsupported WKT type: {wkt[:20]}")
