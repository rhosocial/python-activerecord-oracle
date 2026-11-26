# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/dialect/test_protocol_conformance.py
"""Tests to verify OracleDialect generic-protocol conformance.

This test ensures:
1. OracleDialect implements every generic dialect protocol it claims to support.
2. OracleDialect deliberately does NOT implement the generic protocols it does
   not claim (listed explicitly so accidental support fails the negative test).
3. A family is classified by its answers, not by whether its members exist:
   core's composition mixin makes a refused family structurally reachable, so
   the introspection families are split into claimed and refused lists.
4. Every generic dialect protocol is classified into exactly one list, so new
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
from rhosocial.activerecord.backend.dialect import mixins as dialect_mixins
from rhosocial.activerecord.backend.dialect import protocols as dialect_protocols
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.suggestion import protocols as suggestion_protocols
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


# The four introspection families, split out of the old single protocol.
#
# A family is *claimed* only when at least one of its ``supports_*`` probes
# answers ``True``. ``isinstance`` is never the classifier: core's composition
# mixin gives every dialect every family member, so a refused family is still
# structurally present. The claimed/refused split is what callers are promised.
ORACLE_INTROSPECTION_CLAIMED = [
    dialect_protocols.StructureIntrospectionSupport,
    dialect_protocols.AsyncStructureIntrospectionSupport,
    dialect_protocols.DataProfileIntrospectionSupport,
    dialect_protocols.AsyncDataProfileIntrospectionSupport,
]

# Every probe answers False and every formatter refuses. These stay apart from
# ORACLE_NOT_IMPLEMENTED, whose members are not structurally reachable at all.
ORACLE_INTROSPECTION_REFUSED = [
    dialect_protocols.OperationalStatsIntrospectionSupport,
    dialect_protocols.AsyncOperationalStatsIntrospectionSupport,
    dialect_protocols.DdlExtractionIntrospectionSupport,
    dialect_protocols.AsyncDdlExtractionIntrospectionSupport,
]

# A claimed family can still decline a member: the probe answers False and the
# formatter refuses. That is a member-level gap inside a claimed family, not a
# whole-family refusal. (family, protocol, declined probes, refused formatters)
INTROSPECTION_CLAIMED_DECLINED = [
    (
        "structure",
        dialect_protocols.StructureIntrospectionSupport,
        ("supports_extensions",),
        (),
    ),
    (
        "data profile",
        dialect_protocols.DataProfileIntrospectionSupport,
        ("supports_join_coverage",),
        ("format_join_coverage_query_statement",),
    ),
]


# Generic dialect protocols OracleDialect satisfies.
#
# The naming protocols and the DDL protocols are now separate trees: a
# *ObjectSupport protocol says which namespace levels a name may carry, a
# statement-specific protocol says whether a given statement is emitted and how.
# Both are listed here -- satisfying one without the other is a real and
# interesting state, not an accident.
ORACLE_PROTOCOLS = [
    suggestion_protocols.PartitionBuildSuggestionSupport,
    suggestion_protocols.PartitionTableDropSuggestionSupport,
    suggestion_protocols.PartitionTableAlterSuggestionSupport,
    suggestion_protocols.PartitionAddSuggestionSupport,
    suggestion_protocols.PartitionDropSuggestionSupport,
    suggestion_protocols.PartitionSplitSuggestionSupport,
    suggestion_protocols.PartitionMergeSuggestionSupport,
    suggestion_protocols.PartitionRetentionSuggestionSupport,
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
    dialect_protocols.TimestampSupport,
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
    # The column-class table: which column a common Python type resolves
    # to. OracleColumnTypeMixin supplies the table; the storage half is
    # DataTypeSupport, classified with the DDL protocols below.
    suggestion_protocols.ColumnTypeSuggestionSupport,
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
    suggestion_protocols.FunctionSuggestionSupport,
    dialect_protocols.TransactionControlSupport,
    # --- Introspection ---
    # Only the families whose probes answer at least one question are claimed
    # here; the two that answer none stay in ORACLE_INTROSPECTION_REFUSED.
    *ORACLE_INTROSPECTION_CLAIMED,
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
    suggestion_protocols.GeneratedValueSuggestionSupport,
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
    # DataTypeSupport is deliberately absent: it is an alias for this entry, and
    # listing both would parametrize the same protocol twice. The alias itself
    # is asserted in test_ddl_type_support_is_an_alias_of_data_type_support.
    dialect_protocols.DataTypeSupport,
    dialect_protocols.DatabaseAssignedColumnSupport,
    dialect_protocols.DropIndexSupport,
    dialect_protocols.DropSchemaSupport,
    dialect_protocols.DropSequenceSupport,
    dialect_protocols.DropTableSupport,
    dialect_protocols.DropTypeSupport,
    dialect_protocols.DropViewSupport,
    dialect_protocols.MaterializedViewSupport,
    dialect_protocols.TruncateSupport,
    # --- Core additions from the migration refactor ---
    # These are the shared implementation protocols the core split out into
    # their own modules. OracleDialect composes the corresponding mixins (via
    # SQLDialectBase and the per-feature generic mixins), so each is satisfied;
    # REPEAT is the one deliberate exception, classified below.
    dialect_protocols.DDLColumnSupport,
    dialect_protocols.DMLSupport,
    dialect_protocols.DQLSupport,
    dialect_protocols.DatabaseNameSupport,
    dialect_protocols.DateTimeSupport,
    dialect_protocols.ExpressionSupport,
    dialect_protocols.FunctionCallSupport,
    dialect_protocols.LpadSupport,
    dialect_protocols.PredicateSupport,
    dialect_protocols.PropertyGraphNameSupport,
    dialect_protocols.RelationSourceSupport,
    dialect_protocols.RpadSupport,
    dialect_protocols.SchemaNameSupport,
    dialect_protocols.TrimSupport,
]


class TestDialectProtocolConformance:
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
    # DDL in Oracle commits implicitly, so it is not transactional.
    dialect_protocols.TransactionalDDLSupport,
    # UUID value expressions (generation / nil-max constants / cast) are not
    # implemented yet on this dialect. Listed here so the omission is a
    # recorded decision rather than a gap; move it to the implemented list
    # when the mixin lands.
    dialect_protocols.UUIDSupport,
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
    # Oracle has no REPEAT function (ORA-00904, live-verified 2026-10-09), so
    # the generic RepeatSupport is deliberately not composed.
    dialect_protocols.RepeatSupport,
]


def get_all_generic_protocols() -> dict:
    """Discover every generic dialect protocol, once, under its own name.

    ``protocols`` re-exports the protocols from the submodules that declare
    them, and it also re-exports retired spellings -- ``DataTypeSupport`` is
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


class TestDialectNegativeProtocolConformance:
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

    def test_data_type_support_is_the_single_canonical_protocol(self) -> None:
        """Core collapsed DDL/data type support into one ``DataTypeSupport``.

        A dialect is typed by its data types, not by whether it declares DDL for
        them, so there is a single protocol discovered under its own name and no
        retired ``DDLTypeSupport`` alias left to classify a second time.
        """
        discovered = get_all_generic_protocols()
        assert discovered["DataTypeSupport"] is dialect_protocols.DataTypeSupport
        assert not hasattr(dialect_protocols, "DDLTypeSupport")

    def test_declaring_a_user_defined_type_is_a_separate_protocol(self) -> None:
        """Naming a column's type is not declaring a type.

        The distinction this file used to draw -- ``DataTypeSupport`` versus
        ``DataTypeSupport`` -- no longer exists, but the concern behind it does:
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

    def test_the_classification_lists_partition_all_protocols(self) -> None:
        """Every generic protocol must be classified for Oracle."""
        all_protos = set(get_all_generic_protocols())
        positive = {p.__name__ for p in ORACLE_PROTOCOLS}
        negative = {p.__name__ for p in ORACLE_NOT_IMPLEMENTED}
        refused = {p.__name__ for p in ORACLE_INTROSPECTION_REFUSED}

        overlap = positive & (negative | refused)
        assert not overlap, f"Protocols in BOTH lists: {sorted(overlap)}"
        assert not (
            negative & refused
        ), f"Protocols in both refused and negative lists: {sorted(negative & refused)}"

        unclassified = all_protos - positive - negative - refused
        assert not unclassified, (
            f"Generic protocols not classified for Oracle: {sorted(unclassified)}. "
            f"Add each to ORACLE_PROTOCOLS, ORACLE_INTROSPECTION_REFUSED or "
            f"ORACLE_NOT_IMPLEMENTED."
        )


class TestIntrospectionFamilyClassification:
    """Claimed vs refused, decided by answers rather than by attributes.

    ``isinstance`` passes for all eight protocols because core's composition
    mixin supplies every member, so it is never the classifier here. A family is
    claimed only when at least one probe answers ``True``; a family whose every
    probe answers ``False`` and whose every formatter refuses is refused. A
    claimed family may still decline one member, recorded separately.
    """

    @pytest.fixture
    def dialect(self) -> oracle_dialect.OracleDialect:
        return oracle_dialect.OracleDialect(version=(23, 0, 0))

    @staticmethod
    def _probes(protocol: Any) -> list:
        return sorted(
            name
            for name, member in inspect.getmembers(protocol)
            if callable(member) and name.startswith("supports_")
        )

    @staticmethod
    def _formatters(protocol: Any) -> list:
        return sorted(
            name
            for name, member in inspect.getmembers(protocol)
            if callable(member) and name.startswith("format_")
        )

    def _probe_values(self, dialect: oracle_dialect.OracleDialect, protocol: Any) -> dict:
        return {probe: getattr(dialect, probe)() for probe in self._probes(protocol)}

    @pytest.mark.parametrize("protocol", ORACLE_INTROSPECTION_CLAIMED, ids=lambda p: p.__name__)
    def test_claimed_family_answers_at_least_one_probe(self, dialect, protocol) -> None:
        values = self._probe_values(dialect, protocol)
        assert values, f"{protocol.__name__} declares no supports_* probe"
        assert any(
            values.values()
        ), f"{protocol.__name__} is claimed but no probe answers True: {values}"

    @pytest.mark.parametrize("protocol", ORACLE_INTROSPECTION_REFUSED, ids=lambda p: p.__name__)
    def test_refused_family_answers_false_everywhere(self, dialect, protocol) -> None:
        values = self._probe_values(dialect, protocol)
        assert values, f"{protocol.__name__} declares no supports_* probe"
        assert not any(
            values.values()
        ), f"{protocol.__name__} is refused but a probe answers True: {values}"

    @pytest.mark.parametrize("protocol", ORACLE_INTROSPECTION_REFUSED, ids=lambda p: p.__name__)
    def test_refused_family_refuses_every_formatter(self, dialect, protocol) -> None:
        formatters = self._formatters(protocol)
        assert formatters, f"{protocol.__name__} declares no formatter"
        for formatter in formatters:
            with pytest.raises(UnsupportedFeatureError) as excinfo:
                getattr(dialect, formatter)(None)
            assert formatter in str(excinfo.value)

    @pytest.mark.parametrize(
        "family, protocol, declined, refused",
        INTROSPECTION_CLAIMED_DECLINED,
        ids=[row[0] for row in INTROSPECTION_CLAIMED_DECLINED],
    )
    def test_a_claimed_family_records_its_declined_members(
        self, dialect, family, protocol, declined, refused
    ) -> None:
        for probe in declined:
            assert getattr(dialect, probe)() is False, f"{family}: {probe}"
        for formatter in refused:
            with pytest.raises(UnsupportedFeatureError):
                getattr(dialect, formatter)(None)

    def test_every_family_is_structurally_present(self, dialect) -> None:
        """The classifier cannot be isinstance: all eight protocols pass it."""
        for protocol in ORACLE_INTROSPECTION_CLAIMED + ORACLE_INTROSPECTION_REFUSED:
            assert isinstance(dialect, protocol), protocol.__name__

    def test_an_absent_family_is_not_satisfied(self) -> None:
        """A dialect without the mixins fails every family, not just the probes."""

        class Bare:
            name = "Bare"

        for protocol in ORACLE_INTROSPECTION_CLAIMED + ORACLE_INTROSPECTION_REFUSED:
            assert not isinstance(Bare(), protocol), protocol.__name__

    def test_the_default_structure_mixin_refuses_by_name(self) -> None:
        """An unanswered probe makes the formatter refuse, rather than run."""

        class BareStructure(dialect_mixins.StructureIntrospectionMixin):
            name = "BareStructure"

        with pytest.raises(UnsupportedFeatureError, match="table introspection"):
            BareStructure().format_table_list_query_statement(None)


class TestTheBackendOwnAnswersAreTheEffectiveOnes:
    """A backend mixin's method must resolve to a backend class.

    The other checks in this file are about the *declaration*: the mixin
    declares the method, and the protocol declares it too. This one is
    about the *resolution*. A core class that wins the MRO makes the
    backend's implementation dead code, and the answer a caller gets is
    core's rather than the backend's -- so the backend never says what it
    means about itself, and nothing else in the suite notices: the
    declaration is there either way.

    A protocol class is a declaration rather than an implementation (its
    body is ``...``), so a core class may legitimately precede it; only
    the mixins are held to this.
    """

    def test_no_backend_method_is_shadowed_by_a_core_class(self):
        mro = type(oracle_dialect.OracleDialect(version=(23, 0, 0))).__mro__
        own = "rhosocial.activerecord.backend.impl.oracle"
        shadowed = []
        for klass in mro:
            if not klass.__module__.startswith(own):
                continue
            if not klass.__name__.endswith("Mixin"):
                continue
            for name in vars(klass):
                if not name.startswith(("format_", "supports_", "get_")):
                    continue
                if not callable(vars(klass)[name]):
                    continue
                winner = next((k for k in mro if name in k.__dict__), None)
                if winner is None or winner.__module__.startswith(own):
                    continue
                shadowed.append(f"{klass.__name__}.{name} -> {winner.__name__}")
        assert not shadowed, (
            "a core class wins these backend methods, so the backend's "
            f"implementation is dead code: {shadowed}"
        )
