# src/rhosocial/activerecord/backend/impl/oracle/type_compatibility.py
"""Oracle type casting compatibility checks."""

from typing import Set, Tuple, Optional


DIRECT_COMPATIBLE_CASTS: Set[Tuple[str, str]] = {
    ("varchar2", "varchar2"),
    ("nvarchar2", "nvarchar2"),
    ("char", "char"),
    ("nchar", "nchar"),
    ("clob", "clob"),
    ("nclob", "nclob"),
    ("number", "number"),
    ("float", "float"),
    ("binary_float", "binary_float"),
    ("binary_double", "binary_double"),
    ("date", "date"),
    ("timestamp", "timestamp"),
    ("blob", "blob"),
    ("raw", "raw"),
    ("number", "integer"),
    ("integer", "number"),
    ("varchar2", "clob"),
    ("clob", "varchar2"),
    ("nvarchar2", "nclob"),
    ("nclob", "nvarchar2"),
    ("number", "float"),
    ("float", "number"),
    ("number", "binary_double"),
    ("binary_double", "number"),
    ("raw", "blob"),
    ("blob", "raw"),
    ("char", "varchar2"),
    ("varchar2", "char"),
    ("nchar", "nvarchar2"),
    ("nvarchar2", "nchar"),
    ("date", "timestamp"),
    ("timestamp", "date"),
}


def check_cast_compatibility(source_type: Optional[str], target_type: str) -> bool:
    if source_type is None:
        return True
    if source_type.lower() == target_type.lower():
        return True
    if (source_type.lower(), target_type.lower()) not in DIRECT_COMPATIBLE_CASTS:
        import warnings
        warnings.warn(
            f"Type cast from '{source_type}' to '{target_type}' may fail or lose data in Oracle.",
            UserWarning,
            stacklevel=3,
        )
    return True


def get_compatible_types(source_type: str) -> Set[str]:
    source_lower = source_type.lower()
    return {target for (source, target) in DIRECT_COMPATIBLE_CASTS if source == source_lower}


__all__ = [
    "DIRECT_COMPATIBLE_CASTS",
    "check_cast_compatibility",
    "get_compatible_types",
]