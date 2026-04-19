# types/vector.py
"""
Oracle VECTOR type definition (Oracle 23ai+).

The VECTOR type is designed for AI/ML applications, providing
native support for vector similarity search and operations.
"""

from dataclasses import dataclass
from typing import List, Union
import struct
import json


@dataclass
class OracleVector:
    """Oracle VECTOR type for AI/ML applications (23ai+).
    
    Vectors store numerical data for machine learning operations
    like similarity search, classification, and clustering.
    
    Example:
        >>> vec = OracleVector(dimensions=3, values=[1.0, 2.0, 3.0])
        >>> vec.to_string()
        '[1.0, 2.0, 3.0]'
        >>> vec2 = OracleVector(dimensions=3, values=[1.0, 0.0, 0.0])
        >>> vec.cosine_similarity(vec2)
        0.8017837257372732
    
    Attributes:
        dimensions: Number of dimensions
        values: List of floating-point values
        format: Storage format (FLOAT32, FLOAT64, or INT8)
    """
    dimensions: int
    values: List[float]
    format: str = "FLOAT32"
    
    def __post_init__(self):
        if len(self.values) != self.dimensions:
            raise ValueError(
                f"Expected {self.dimensions} values, got {len(self.values)}"
            )
        if self.format not in ("FLOAT32", "FLOAT64", "INT8"):
            raise ValueError(f"Unsupported format: {self.format}")
        if self.dimensions <= 0:
            raise ValueError(f"Dimensions must be positive: {self.dimensions}")
    
    def __len__(self) -> int:
        return self.dimensions
    
    def __getitem__(self, index: int) -> float:
        return self.values[index]
    
    def __iter__(self):
        return iter(self.values)
    
    def __repr__(self) -> str:
        return f"OracleVector(dimensions={self.dimensions}, format={self.format})"
    
    def __str__(self) -> str:
        return self.to_string()
    
    def to_string(self) -> str:
        """Convert to Oracle VECTOR string format.
        
        Returns:
            String representation '[1.0, 2.0, ...]'
        """
        return '[' + ', '.join(str(v) for v in self.values) + ']'
    
    def to_binary(self) -> bytes:
        """Convert to binary format for storage.
        
        Returns:
            Binary representation of vector
        """
        if self.format == "FLOAT32":
            return struct.pack(f'<{len(self.values)}f', *self.values)
        elif self.format == "FLOAT64":
            return struct.pack(f'<{len(self.values)}d', *self.values)
        elif self.format == "INT8":
            return struct.pack(
                f'<{len(self.values)}b',
                *[int(round(v)) for v in self.values]
            )
        raise ValueError(f"Unknown format: {self.format}")
    
    def to_json(self) -> str:
        """Convert to JSON format.
        
        Returns:
            JSON string representation
        """
        return json.dumps({
            "dimensions": self.dimensions,
            "values": self.values,
            "format": self.format
        })
    
    def to_list(self) -> List[float]:
        """Convert to Python list.
        
        Returns:
            List of values
        """
        return self.values.copy()
    
    def to_numpy(self):
        """Convert to numpy array (if numpy is available).
        
        Returns:
            numpy.ndarray
            
        Raises:
            ImportError: If numpy is not installed
        """
        import numpy as np
        return np.array(self.values, dtype=np.float32)
    
    def normalize(self) -> 'OracleVector':
        """Return L2-normalized vector.
        
        Returns:
            New normalized vector
        """
        norm = self.l2_norm()
        if norm == 0:
            return OracleVector(
                dimensions=self.dimensions,
                values=[0.0] * self.dimensions,
                format=self.format
            )
        return OracleVector(
            dimensions=self.dimensions,
            values=[v / norm for v in self.values],
            format=self.format
        )
    
    def l2_norm(self) -> float:
        """Calculate L2 (Euclidean) norm.
        
        Returns:
            L2 norm value
        """
        return sum(v * v for v in self.values) ** 0.5
    
    def dot_product(self, other: 'OracleVector') -> float:
        """Calculate dot product with another vector.
        
        Args:
            other: Another vector
            
        Returns:
            Dot product value
        """
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Dimension mismatch: {self.dimensions} vs {other.dimensions}"
            )
        return sum(a * b for a, b in zip(self.values, other.values))
    
    def cosine_similarity(self, other: 'OracleVector') -> float:
        """Calculate cosine similarity with another vector.
        
        Cosine similarity measures the angle between vectors.
        Range: [-1, 1] where 1 means identical direction.
        
        Args:
            other: Another vector
            
        Returns:
            Cosine similarity value
        """
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Dimension mismatch: {self.dimensions} vs {other.dimensions}"
            )
        dot = self.dot_product(other)
        norm_a = self.l2_norm()
        norm_b = other.l2_norm()
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)
    
    def euclidean_distance(self, other: 'OracleVector') -> float:
        """Calculate Euclidean distance from another vector.
        
        Args:
            other: Another vector
            
        Returns:
            Euclidean distance value
        """
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Dimension mismatch: {self.dimensions} vs {other.dimensions}"
            )
        return sum((a - b) ** 2 for a, b in zip(self.values, other.values)) ** 0.5
    
    def manhattan_distance(self, other: 'OracleVector') -> float:
        """Calculate Manhattan distance from another vector.
        
        Args:
            other: Another vector
            
        Returns:
            Manhattan distance value
        """
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"Dimension mismatch: {self.dimensions} vs {other.dimensions}"
            )
        return sum(abs(a - b) for a, b in zip(self.values, other.values))
    
    @classmethod
    def from_string(cls, s: str, format: str = "FLOAT32") -> 'OracleVector':
        """Parse from Oracle VECTOR string format.
        
        Args:
            s: String in format '[1.0, 2.0, ...]'
            format: Storage format
            
        Returns:
            New OracleVector instance
        """
        s = s.strip()
        if not s.startswith('[') or not s.endswith(']'):
            raise ValueError(f"Invalid VECTOR format: {s}")
        values = [float(v.strip()) for v in s[1:-1].split(',') if v.strip()]
        return cls(dimensions=len(values), values=values, format=format)
    
    @classmethod
    def from_binary(cls, data: bytes, dimensions: int,
                    format: str = "FLOAT32") -> 'OracleVector':
        """Create from binary format.
        
        Args:
            data: Binary data
            dimensions: Number of dimensions
            format: Storage format
            
        Returns:
            New OracleVector instance
        """
        if format == "FLOAT32":
            values = list(struct.unpack(f'<{dimensions}f', data))
        elif format == "FLOAT64":
            values = list(struct.unpack(f'<{dimensions}d', data))
        elif format == "INT8":
            values = [float(v) for v in struct.unpack(f'<{dimensions}b', data)]
        else:
            raise ValueError(f"Unknown format: {format}")
        return cls(dimensions=dimensions, values=values, format=format)
    
    @classmethod
    def from_list(cls, values: List[float], format: str = "FLOAT32") -> 'OracleVector':
        """Create from list of values.
        
        Args:
            values: List of floating-point values
            format: Storage format
            
        Returns:
            New OracleVector instance
        """
        return cls(dimensions=len(values), values=values, format=format)
    
    @classmethod
    def zeros(cls, dimensions: int, format: str = "FLOAT32") -> 'OracleVector':
        """Create zero vector.
        
        Args:
            dimensions: Number of dimensions
            format: Storage format
            
        Returns:
            Zero vector
        """
        return cls(dimensions=dimensions, values=[0.0] * dimensions, format=format)
    
    @classmethod
    def random(cls, dimensions: int, format: str = "FLOAT32",
               seed: int = None) -> 'OracleVector':
        """Create random vector.
        
        Args:
            dimensions: Number of dimensions
            format: Storage format
            seed: Random seed (optional)
            
        Returns:
            Random vector with values in [0, 1)
        """
        import random
        if seed is not None:
            random.seed(seed)
        values = [random.random() for _ in range(dimensions)]
        return cls(dimensions=dimensions, values=values, format=format)
