# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_protocol_conformance.py
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

from typing import Protocol

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
ORACLE_PROTOCOLS = [
    dialect_protocols.AdvancedGroupingSupport,
    dialect_protocols.AlterTableModifierSupport,
    dialect_protocols.ArraySupport,
    dialect_protocols.AutoIncrementSupport,
    dialect_protocols.CTESupport,
    dialect_protocols.CollationSupport,
    dialect_protocols.ConstraintSupport,
    dialect_protocols.DDLTypeSupport,
    dialect_protocols.ExplainSupport,
    dialect_protocols.FilterClauseSupport,
    dialect_protocols.GeneratedColumnSupport,
    dialect_protocols.GraphSupport,
    dialect_protocols.GraphTableSupport,
    dialect_protocols.IndexSupport,
    dialect_protocols.IntrospectionSupport,
    dialect_protocols.JSONSupport,
    dialect_protocols.JoinSupport,
    dialect_protocols.LateralJoinSupport,
    dialect_protocols.LockingSupport,
    dialect_protocols.MergeSupport,
    dialect_protocols.OrderedSetAggregationSupport,
    dialect_protocols.PartitionSupport,
    dialect_protocols.QualifyClauseSupport,
    dialect_protocols.ReturningSupport,
    dialect_protocols.SQLFunctionSupport,
    dialect_protocols.SQLXMLAggregationSupport,
    dialect_protocols.SQLXMLConstructionSupport,
    dialect_protocols.SQLXMLParsingSupport,
    dialect_protocols.SQLXMLQueryingSupport,
    dialect_protocols.SQLXMLSerializationSupport,
    dialect_protocols.SQLXMLSupport,
    dialect_protocols.SchemaSupport,
    dialect_protocols.SequenceSupport,
    dialect_protocols.SetOperationSupport,
    dialect_protocols.TableSupport,
    dialect_protocols.TemporalTableSupport,
    dialect_protocols.TransactionControlSupport,
    dialect_protocols.TruncateSupport,
    dialect_protocols.UpsertSupport,
    dialect_protocols.ViewSupport,
    dialect_protocols.WildcardSupport,
    dialect_protocols.WindowFunctionSupport,
]


class TestOracleDialectProtocolConformance:
    """Assert OracleDialect implements all generic protocols it declares to support."""

    @pytest.fixture
    def dialect(self):
        """Create an OracleDialect instance for testing."""
        return oracle_dialect.OracleDialect(version=(23, 0, 0))

    @pytest.mark.parametrize("protocol", ORACLE_PROTOCOLS)
    def test_implements_protocol(self, dialect, protocol):
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
    # Oracle has no SQL/PSM generic function protocol; routines are exposed
    # through the Oracle-specific OracleRoutineSupport / OracleFunctionFormatSupport
    # capability protocols instead.
    dialect_protocols.FunctionSupport,
    # Oracle exposes trigger DDL through its own OracleTriggerSupport protocol
    # (compound/system triggers, PL/SQL bodies) rather than the generic
    # SQL:1999/PSM TriggerSupport.
    dialect_protocols.TriggerSupport,
    # Oracle has no ILIKE operator; case-insensitive matching goes through
    # UPPER()/LOWER() + LIKE (documented in the ILIKESupport protocol docstring).
    dialect_protocols.ILIKESupport,
]


def get_all_generic_protocols() -> dict:
    """Discover every generic dialect protocol defined in protocols.py."""
    discovered = {}
    for name, obj in inspect.getmembers(dialect_protocols, inspect.isclass):
        if Protocol in getattr(obj, "__mro__", []) and name.endswith("Support"):
            discovered[name] = obj
    return discovered


class TestOracleDialectNegativeProtocolConformance:
    """Assert OracleDialect does not implement intentionally-unsupported protocols."""

    @pytest.fixture
    def dialect(self):
        return oracle_dialect.OracleDialect(version=(23, 0, 0))

    @pytest.mark.parametrize("protocol", ORACLE_NOT_IMPLEMENTED)
    def test_does_not_implement_protocol(self, dialect, protocol):
        """OracleDialect must NOT implement any protocol in ORACLE_NOT_IMPLEMENTED."""
        assert not isinstance(dialect, protocol), (
            f"OracleDialect unexpectedly implements {protocol.__name__}. "
            f"If intentional, move it from ORACLE_NOT_IMPLEMENTED to ORACLE_PROTOCOLS "
            f"(and implement the behaviour fully)."
        )

    def test_positive_and_negative_lists_partition_all_protocols(self):
        """Every generic protocol must be classified for Oracle."""
        all_protos = set(get_all_generic_protocols())
        positive = {p.__name__ for p in ORACLE_PROTOCOLS if p.__module__ == dialect_protocols.__name__}
        negative = {p.__name__ for p in ORACLE_NOT_IMPLEMENTED}

        overlap = positive & negative
        assert not overlap, f"Protocols in BOTH lists: {sorted(overlap)}"

        unclassified = all_protos - positive - negative
        assert not unclassified, (
            f"Generic protocols not classified for Oracle: {sorted(unclassified)}. "
            f"Add each to ORACLE_PROTOCOLS or ORACLE_NOT_IMPLEMENTED."
        )
