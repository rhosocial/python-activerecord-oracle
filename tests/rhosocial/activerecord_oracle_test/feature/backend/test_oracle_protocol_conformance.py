# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_protocol_conformance.py
"""Tests to verify OracleDialect generic-protocol conformance.

This test ensures:
1. OracleDialect implements every generic dialect protocol it claims to support.
2. OracleDialect deliberately does NOT implement the generic protocols it does
   not claim (listed explicitly so accidental support fails the negative test).
3. Every generic dialect protocol is classified into exactly one list, so new
   protocols added upstream cannot silently escape the contract.
"""

import inspect
import sys

from typing import Any, Protocol

if sys.version_info >= (3, 13):
    from typing import get_protocol_members
elif sys.version_info >= (3, 12):
    from typing import _get_protocol_attrs as get_protocol_members

import pytest
from rhosocial.activerecord.backend.dialect import protocols as dialect_protocols
from rhosocial.activerecord.backend.impl.oracle import dialect as oracle_dialect


def get_all_protocol_methods(proto: type) -> set:
    """Extract all public method names from a protocol, including inherited."""
    members = set()
    if sys.version_info >= (3, 12):
        members = get_protocol_members(proto)
    else:
        # Walk MRO to include methods from parent protocols
        for cls in proto.__mro__:
            if cls is object:
                continue
            for name in cls.__dict__:
                if name.startswith("_"):
                    continue
                val = cls.__dict__[name]
                if callable(val) or isinstance(val, (property, classmethod, staticmethod)):
                    members.add(name)
            members.update(k for k in getattr(cls, "__annotations__", {}) if not k.startswith("_"))
    return members


# Generic dialect protocols OracleDialect satisfies.
# Generic dialect protocols OracleDialect satisfies.
#
# The naming protocols and the DDL protocols are now separate trees: a
# *ObjectSupport protocol says which namespace levels a name may carry, a
# statement-specific protocol says whether a given statement is emitted and how.
# Both are listed here -- satisfying one without the other is a real and
# interesting state, not an accident.
ORACLE_PROTOCOLS = [
    # --- Named objects: which namespace levels a name may carry ---
    dialect_protocols.ForeignTableObjectSupport,
    dialect_protocols.IndexObjectSupport,
    dialect_protocols.MaterializedViewObjectSupport,
    dialect_protocols.NamespaceSupport,
    dialect_protocols.RoutineObjectSupport,
    dialect_protocols.SequenceObjectSupport,
    dialect_protocols.SynonymObjectSupport,
    dialect_protocols.TableObjectSupport,
    dialect_protocols.TriggerObjectSupport,
    dialect_protocols.TypeObjectSupport,
    dialect_protocols.ViewObjectSupport,
    # --- Query and DML ---
    dialect_protocols.AdvancedGroupingSupport,
    dialect_protocols.ArraySupport,
    dialect_protocols.CTESupport,
    dialect_protocols.DateTimeSupport,
    dialect_protocols.DqlOrderSupport,
    dialect_protocols.ExplainSupport,
    dialect_protocols.FilterClauseSupport,
    dialect_protocols.GraphSupport,
    dialect_protocols.GraphTableSupport,
    dialect_protocols.JSONSupport,
    dialect_protocols.JoinSupport,
    dialect_protocols.LateralJoinSupport,
    dialect_protocols.LockingSupport,
    dialect_protocols.MergeSupport,
    dialect_protocols.OrderedSetAggregationSupport,
    dialect_protocols.PivotSupport,
    dialect_protocols.QualifyClauseSupport,
    dialect_protocols.ReturningSupport,
    dialect_protocols.SetOperationSupport,
    dialect_protocols.TemporalTableSupport,
    dialect_protocols.UpsertSupport,
    dialect_protocols.WildcardSupport,
    dialect_protocols.WindowFunctionSupport,
    # --- SQL/XML ---
    dialect_protocols.SQLXMLAggregationSupport,
    dialect_protocols.SQLXMLConstructionSupport,
    dialect_protocols.SQLXMLParsingSupport,
    dialect_protocols.SQLXMLQueryingSupport,
    dialect_protocols.SQLXMLSerializationSupport,
    dialect_protocols.SQLXMLSupport,
    # --- Misc features ---
    dialect_protocols.CollationSupport,
    dialect_protocols.FulltextIndexSupport,
    dialect_protocols.GeneratedColumnSupport,
    dialect_protocols.PartitionSupport,
    dialect_protocols.SQLFunctionSupport,
    dialect_protocols.TransactionControlSupport,
    # --- Introspection ---
    dialect_protocols.IntrospectionSupport,
    # --- DDL statements ---
    dialect_protocols.AlterSequenceSupport,
    dialect_protocols.AlterTableModifierSupport,
    dialect_protocols.AlterTableSupport,
    dialect_protocols.AlterTypeSupport,
    # Two mechanisms, two protocols: the bare AUTO_INCREMENT marker and the
    # parameterised GENERATED ... AS IDENTITY clause. Oracle accepts only the
    # latter, and declares it True from 12c on.
    dialect_protocols.AutoIncrementColumnSupport,
    dialect_protocols.IdentityColumnSupport,
    dialect_protocols.ColumnAttributeSupport,
    dialect_protocols.CommentSupport,
    dialect_protocols.ConstraintSupport,
    dialect_protocols.CreateIndexSupport,
    dialect_protocols.CreateSchemaSupport,
    dialect_protocols.CreateSequenceSupport,
    dialect_protocols.CreateTableAsSupport,
    dialect_protocols.CreateTableCloneSupport,
    dialect_protocols.CreateTableLikeSupport,
    dialect_protocols.CreateTableSupport,
    dialect_protocols.CreateTableUsingTemplateSupport,
    dialect_protocols.CreateTypeSupport,
    dialect_protocols.CreateViewSupport,
    # DDLTypeSupport is deliberately absent: it is an alias for this entry, and
    # listing both would parametrize the same protocol twice. The alias itself
    # is asserted in test_ddl_type_support_is_an_alias_of_data_type_support.
    dialect_protocols.DataTypeSupport,
    dialect_protocols.DropIndexSupport,
    dialect_protocols.DropSchemaSupport,
    dialect_protocols.DropSequenceSupport,
    dialect_protocols.DropTableSupport,
    dialect_protocols.DropTypeSupport,
    dialect_protocols.DropViewSupport,
    dialect_protocols.MaterializedViewSupport,
    dialect_protocols.TruncateSupport,
]


class TestOracleDialectProtocolConformance:
    """Assert OracleDialect implements all generic protocols it declares to support."""

    @pytest.fixture
    def dialect(self) -> oracle_dialect.OracleDialect:
        """Create an OracleDialect instance for testing."""
        return oracle_dialect.OracleDialect(version=(23, 0, 0))

    @pytest.mark.parametrize("protocol", ORACLE_PROTOCOLS)
    def test_implements_protocol(
        self,
        dialect: oracle_dialect.OracleDialect,
        protocol: Any,
    ) -> None:
        """OracleDialect should implement each protocol in ORACLE_PROTOCOLS."""
        assert isinstance(dialect, protocol), (
            f"OracleDialect does not implement protocol {protocol.__name__}, "
            f"missing methods: {get_all_protocol_methods(protocol) - set(dir(dialect))}"
        )


# Generic protocols OracleDialect intentionally does NOT implement.
#
# Listing them makes the omission a deliberate, tested contract: if Oracle ever
# satisfies one by accident, the negative test fails and forces a conscious
# decision (move to ORACLE_PROTOCOLS or revert).
ORACLE_NOT_IMPLEMENTED = [
    # --- Intentional non-support ---
    # Oracle has no DATABASE, DOMAIN or schema-namespace DDL: schemas come from
    # CREATE USER. See OracleSchemaMixin, which answers the DDL side False and
    # the naming side True.
    dialect_protocols.AlterDatabaseSupport,
    dialect_protocols.CreateDatabaseSupport,
    dialect_protocols.DropDatabaseSupport,
    dialect_protocols.AlterDomainSupport,
    dialect_protocols.CreateDomainSupport,
    dialect_protocols.DropDomainSupport,
    # The core declares one routine protocol per statement; Oracle's PL/SQL
    # bodies and packages are not expressed by them, and are exposed through
    # the backend's own OracleRoutineMixin / OracleTypeDDLSupport instead.
    dialect_protocols.CreateRoutineSupport,
    dialect_protocols.DropRoutineSupport,
    # Likewise for triggers: Oracle's compound, system and PL/SQL-body triggers
    # are beyond what the core statement protocols describe.
    dialect_protocols.CreateTriggerSupport,
    dialect_protocols.DropTriggerSupport,
    # Oracle has no ILIKE operator; case-insensitive matching goes through
    # UPPER()/LOWER() + LIKE (documented in the ILIKESupport protocol docstring).
    dialect_protocols.ILIKESupport,
]


def get_all_generic_protocols() -> dict:
    """Discover every generic dialect protocol, once, under its own name.

    ``protocols`` re-exports the protocols from the submodules that declare
    them, and it also re-exports retired spellings -- ``DDLTypeSupport`` is
    literally ``DataTypeSupport``. Keying discovery on the class's own
    ``__name__`` drops the alias without a hand-maintained exclusion list: a
    protocol is discovered under the name it was declared with, so an alias
    cannot be counted as a second protocol awaiting classification.
    """
    discovered = {}
    for _name, obj in inspect.getmembers(dialect_protocols, inspect.isclass):
        if Protocol in getattr(obj, "__mro__", []) and obj.__name__.endswith("Support"):
            discovered[obj.__name__] = obj
    return discovered


class TestOracleDialectNegativeProtocolConformance:
    """Assert OracleDialect does not implement intentionally-unsupported protocols."""

    @pytest.fixture
    def dialect(self) -> oracle_dialect.OracleDialect:
        return oracle_dialect.OracleDialect(version=(23, 0, 0))

    @pytest.mark.parametrize("protocol", ORACLE_NOT_IMPLEMENTED)
    def test_does_not_implement_protocol(
        self,
        dialect: oracle_dialect.OracleDialect,
        protocol: Any,
    ) -> None:
        """OracleDialect must NOT implement any protocol in ORACLE_NOT_IMPLEMENTED."""
        assert not isinstance(dialect, protocol), (
            f"OracleDialect unexpectedly implements {protocol.__name__}. "
            f"If intentional, move it from ORACLE_NOT_IMPLEMENTED to ORACLE_PROTOCOLS "
            f"(and implement the behaviour fully)."
        )

    def test_ddl_type_support_is_an_alias_of_data_type_support(self) -> None:
        """``DDLTypeSupport`` is a retired spelling of ``DataTypeSupport``.

        The core collapsed the two: a dialect is typed by its data types, not by
        whether it declares DDL for them, so there is one protocol under two
        names. Oracle classifies it once, under the name it is declared with --
        which is why discovery keys on ``__name__`` and an alias cannot be
        counted as a second protocol still awaiting a verdict.
        """
        assert dialect_protocols.DDLTypeSupport is dialect_protocols.DataTypeSupport
        discovered = get_all_generic_protocols()
        assert "DDLTypeSupport" not in discovered
        assert discovered["DataTypeSupport"] is dialect_protocols.DataTypeSupport

    def test_declaring_a_user_defined_type_is_a_separate_protocol(self) -> None:
        """Naming a column's type is not declaring a type.

        The distinction this file used to draw -- ``DataTypeSupport`` versus
        ``DDLTypeSupport`` -- no longer exists, but the concern behind it does:
        a dialect must not be able to satisfy "I can type a column" and thereby
        claim "I can CREATE TYPE". That claim lives on the per-statement
        protocols, which Oracle reaches through its own OracleTypeDDLSupport.
        """
        for statement_protocol in (
            dialect_protocols.CreateTypeSupport,
            dialect_protocols.AlterTypeSupport,
            dialect_protocols.DropTypeSupport,
        ):
            assert statement_protocol is not dialect_protocols.DataTypeSupport
            assert statement_protocol.__name__ in get_all_generic_protocols()

    def test_positive_and_negative_lists_partition_all_protocols(self) -> None:
        """Every generic protocol must be classified for Oracle."""
        all_protos = set(get_all_generic_protocols())
        positive = {p.__name__ for p in ORACLE_PROTOCOLS}
        negative = {p.__name__ for p in ORACLE_NOT_IMPLEMENTED}

        overlap = positive & negative
        assert not overlap, f"Protocols in BOTH lists: {sorted(overlap)}"

        unclassified = all_protos - positive - negative
        assert not unclassified, (
            f"Generic protocols not classified for Oracle: {sorted(unclassified)}. "
            f"Add each to ORACLE_PROTOCOLS or ORACLE_NOT_IMPLEMENTED."
        )
