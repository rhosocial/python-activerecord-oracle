# src/rhosocial/activerecord/backend/impl/oracle/protocols/data_type.py
"""Oracle data-type rendering and parsing protocol.

Named ``OracleDataTypeSupport`` rather than the plan's ``OracleTypeSupport``:
that name is already a public alias of :class:`OracleTypeDDLSupport` (the
``CREATE TYPE`` / ``ALTER TYPE`` DDL protocol, see ``ddl_type.py``), and
reusing it for the column data-type family would make one name mean two
protocols. The intent — a stated shape for the family rather than a naming
pattern the reader has to infer — is kept.
"""

from __future__ import annotations

from typing import Dict, Protocol, Tuple, runtime_checkable


@runtime_checkable
class OracleDataTypeSupport(Protocol):
    """Protocol for the Oracle column data-type family.

    Two halves, and both are required for a type to be usable:

    * **Rendering** — ``format_data_type_<name>`` / ``supports_data_type_<name>``
      for the concept's ``name``. For the core concepts that is the core
      ``name`` (``varchar``, ``text``, ``interval``, ``xml``, ``custom``, …);
      for Oracle's own spelling of a concept it is the ``oracle_``-prefixed
      one (``oracle_nvarchar2``, ``oracle_raw``, …). The naming convention *is*
      the dispatch: :meth:`DataTypeMixin.format_data_type` looks the method up
      from ``type(data_type).name``, so a name without a formatter is a type
      that cannot be written.
    * **Stance** — ``suggested_data_types()``, for the core concepts Oracle
      stores some other way. A concept that is neither rendered nor named as a
      substitute leaves the caller with "unsupported" and nothing to go on,
      which is indistinguishable from never having considered it.

    The ``oracle_*`` half is exactly the set of types **no core formatter here
    writes**, and that is the whole of the test it has to pass: a prefixed key is
    a claim that Oracle has a type the core hierarchy has no name for, and a
    class whose rendering duplicates a core concept's only adds a name. So there
    is deliberately no ``oracle_integer`` / ``oracle_smallint`` /
    ``oracle_bigint`` (Oracle has one integer type and this dialect already
    writes ``NUMBER(p)`` for the three core concepts), no
    ``oracle_varchar2`` / ``oracle_char`` (``format_data_type_varchar`` writes
    ``VARCHAR2``, ``format_data_type_char`` writes ``CHAR``) and no
    ``oracle_blob`` (``format_data_type_blob`` writes ``BLOB``). Beyond that,
    ``oracle_clob`` and ``oracle_xml`` are absent for the same reason one concept
    gets one class: plain ``CLOB`` is ``TextType(spelling="clob")`` and
    ``XMLTYPE`` is core ``XmlType``.

    What remains is the set for which Oracle's own word names a type the
    framework models under a *different* Oracle word too — so a round trip
    preserves the catalog spelling instead of collapsing to the generic core
    rendering.
    """

    # --- dispatch entry points ---

    def format_data_type(self, data_type) -> Tuple[str, tuple]:
        """Render any ``DataType`` through the ``format_data_type_<name>`` family."""
        ...

    def parse_type(self, raw: str):
        """Parse a raw Oracle type string into the ``DataType`` that owns it."""
        ...

    def supports_data_types(self) -> Dict[str, type]:
        """``{generic name: concrete DataType class}`` for everything rendered."""
        ...

    def suggested_data_types(self) -> Dict[str, type]:
        """``{core concept name: the DataType Oracle stores instead}``."""
        ...

    # --- core concepts with a distinct Oracle spelling ---

    def format_data_type_interval(self, data_type) -> Tuple[str, tuple]:
        """``INTERVAL YEAR TO MONTH`` / ``INTERVAL DAY TO SECOND``."""
        ...

    def format_data_type_xml(self, data_type) -> Tuple[str, tuple]:
        """``XMLTYPE`` — the native XML type, an infoset rather than text."""
        ...

    def format_data_type_custom(self, data_type) -> Tuple[str, tuple]:
        """A type name the framework does not model, written verbatim."""
        ...

    # --- Oracle-namespaced types ---

    def format_data_type_oracle_nvarchar2(self, data_type) -> Tuple[str, tuple]:
        """``NVARCHAR2(n)`` — variable-length, national character set."""
        ...

    def format_data_type_oracle_nclob(self, data_type) -> Tuple[str, tuple]:
        """``NCLOB`` — unbounded character data, national character set."""
        ...

    def format_data_type_oracle_long(self, data_type) -> Tuple[str, tuple]:
        """``LONG`` — deprecated, and not a CLOB."""
        ...

    def format_data_type_oracle_raw(self, data_type) -> Tuple[str, tuple]:
        """``RAW(n)`` — a bounded byte string."""
        ...

    def format_data_type_oracle_long_raw(self, data_type) -> Tuple[str, tuple]:
        """``LONG RAW`` — deprecated, and not a BLOB."""
        ...

    def format_data_type_oracle_binary_float(self, data_type) -> Tuple[str, tuple]:
        """``BINARY_FLOAT`` — 32-bit IEEE, no parameters."""
        ...

    def format_data_type_oracle_binary_double(self, data_type) -> Tuple[str, tuple]:
        """``BINARY_DOUBLE`` — 64-bit IEEE, no parameters."""
        ...

    def format_data_type_oracle_timestamp_ltz(self, data_type) -> Tuple[str, tuple]:
        """``TIMESTAMP(n) WITH LOCAL TIME ZONE`` — no stored offset."""
        ...

    # --- the capability half of each pair above ---

    def supports_data_type_interval(self) -> bool: ...

    def supports_data_type_xml(self) -> bool: ...

    def supports_data_type_custom(self) -> bool: ...

    def supports_data_type_oracle_nvarchar2(self) -> bool: ...

    def supports_data_type_oracle_nclob(self) -> bool: ...

    def supports_data_type_oracle_long(self) -> bool: ...

    def supports_data_type_oracle_raw(self) -> bool: ...

    def supports_data_type_oracle_long_raw(self) -> bool: ...

    def supports_data_type_oracle_binary_float(self) -> bool: ...

    def supports_data_type_oracle_binary_double(self) -> bool: ...

    def supports_data_type_oracle_timestamp_ltz(self) -> bool: ...


__all__ = ["OracleDataTypeSupport"]