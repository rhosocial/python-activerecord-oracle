# src/rhosocial/activerecord/backend/impl/oracle/schema/differ.py
"""Oracle schema differ.

Oracle column equivalence relies on the core ``SchemaDiffer._columns_equivalent``
implementation, which compares the structured ``parsed_data_type`` field
(populated by ``OracleIntrospector``) via ``DataType.is_equivalent()`` and
falls back to ``data_type`` string comparison when the parsed type is
unavailable.

This aligns with core #108: column definitions carry ``DataType`` instances,
and the differ no longer overrides ``_columns_equivalent`` with a raw
``data_type`` string comparison. Oracle has no ``ordinal_position``
sensitivity beyond what the core check already covers (``ALTER TABLE ADD``
appends; column re-ordering is not a native Oracle operation), so no
extra backend-specific rule is required here.
"""

from rhosocial.activerecord.backend.schema.differ import SchemaDiffer


class OracleSchemaDiffer(SchemaDiffer):
    """Oracle schema differ.

    Inherits the core column/type/nullability/default comparison. No
    Oracle-specific override is needed once ``parsed_data_type`` is
    populated by the introspector (core #108).
    """
    pass
