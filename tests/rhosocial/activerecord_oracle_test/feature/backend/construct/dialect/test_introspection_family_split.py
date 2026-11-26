# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/dialect/test_introspection_family_split.py
"""Guard the Oracle-facing surface after core split its introspection protocol.

Core replaced the single ``IntrospectionSupport`` with four family protocols
(and async twins) and deleted ``supports_partition_info`` without aliases. This
file pins the consequences that belong to this backend:

* a family is claimed only when at least one of its probes answers ``True``;
  a family whose every probe answers ``False`` and whose every formatter
  refuses is refused as a family, even though core's composition mixin makes
  its members reachable (``isinstance`` is never the classifier);
* the four families import from the new ``protocols.introspect`` modules, and
  the old names are gone;
* a constructed dialect no longer has ``supports_partition_info``;
* the CLI's ``partitions`` family still resolves to the partition-metadata
  probe, which is version-gated from 11g -- the probe the deleted one
  under-reported.
"""

import inspect

import pytest

from rhosocial.activerecord.backend.dialect import mixins as dialect_mixins
from rhosocial.activerecord.backend.dialect import protocols as dialect_protocols
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.cli import introspect as introspect_cli
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord_oracle_test.feature.backend.construct.dialect.test_protocol_conformance import (
    INTROSPECTION_CLAIMED_DECLINED,
    ORACLE_INTROSPECTION_CLAIMED,
    ORACLE_INTROSPECTION_REFUSED,
)

#: family -> (sync name, async name, declaring core module)
FAMILIES = [
    (
        "StructureIntrospectionSupport",
        "AsyncStructureIntrospectionSupport",
        "rhosocial.activerecord.backend.dialect.protocols.introspect.structure",
    ),
    (
        "OperationalStatsIntrospectionSupport",
        "AsyncOperationalStatsIntrospectionSupport",
        "rhosocial.activerecord.backend.dialect.protocols.introspect.operational_stats",
    ),
    (
        "DataProfileIntrospectionSupport",
        "AsyncDataProfileIntrospectionSupport",
        "rhosocial.activerecord.backend.dialect.protocols.introspect.data_profile",
    ),
    (
        "DdlExtractionIntrospectionSupport",
        "AsyncDdlExtractionIntrospectionSupport",
        "rhosocial.activerecord.backend.dialect.protocols.introspect.ddl_extraction",
    ),
]


def _probes(protocol: type) -> list:
    """Every ``supports_*`` probe a family protocol declares."""
    return sorted(
        name
        for name, member in inspect.getmembers(protocol)
        if callable(member) and name.startswith("supports_")
    )


def _formatters(protocol: type) -> list:
    """Every ``format_*`` formatter a family protocol declares."""
    return sorted(
        name
        for name, member in inspect.getmembers(protocol)
        if callable(member) and name.startswith("format_")
    )


@pytest.mark.parametrize("protocol", ORACLE_INTROSPECTION_CLAIMED, ids=lambda p: p.__name__)
def test_a_claimed_family_has_at_least_one_answered_probe(protocol) -> None:
    """The positive list is a capability claim: at least one probe must answer."""
    dialect = OracleDialect(version=(23, 0, 0))
    probes = _probes(protocol)
    values = {probe: getattr(dialect, probe)() for probe in probes}
    assert probes, f"{protocol.__name__} declares no supports_* probe"
    assert any(
        values.values()
    ), f"{protocol.__name__} is in the claimed list but every probe answers False: {values}"


@pytest.mark.parametrize("protocol", ORACLE_INTROSPECTION_REFUSED, ids=lambda p: p.__name__)
def test_a_refused_family_says_false_and_refuses_by_name(protocol) -> None:
    """The negative list is a refusal: every probe False, every formatter refuses."""
    dialect = OracleDialect(version=(23, 0, 0))
    probes = _probes(protocol)
    values = {probe: getattr(dialect, probe)() for probe in probes}
    assert probes, f"{protocol.__name__} declares no supports_* probe"
    assert not any(
        values.values()
    ), f"{protocol.__name__} is in the refused list but a probe answers True: {values}"

    formatters = _formatters(protocol)
    assert formatters, f"{protocol.__name__} declares no formatter"
    for formatter in formatters:
        with pytest.raises(UnsupportedFeatureError) as excinfo:
            getattr(dialect, formatter)(None)
        assert formatter in str(excinfo.value), (
            f"{protocol.__name__}.{formatter} refused without naming the formatter: "
            f"{excinfo.value}"
        )


def test_claimed_and_refused_families_are_disjoint_and_complete() -> None:
    """Both twins of all four families are classified exactly once."""
    claimed = {protocol.__name__ for protocol in ORACLE_INTROSPECTION_CLAIMED}
    refused = {protocol.__name__ for protocol in ORACLE_INTROSPECTION_REFUSED}
    expected = {name for name, _async, _module in FAMILIES} | {
        async_name for _name, async_name, _module in FAMILIES
    }

    assert not (claimed & refused), f"Families in BOTH lists: {sorted(claimed & refused)}"
    assert (
        claimed | refused == expected
    ), f"Families not classified: {sorted(expected - (claimed | refused))}"


@pytest.mark.parametrize(
    "family, protocol, declined, refused",
    INTROSPECTION_CLAIMED_DECLINED,
    ids=[row[0] for row in INTROSPECTION_CLAIMED_DECLINED],
)
def test_a_claimed_family_keeps_its_declined_members(family, protocol, declined, refused) -> None:
    """A declined member is recorded inside a claimed family, not promoted."""
    dialect = OracleDialect(version=(23, 0, 0))
    values = {probe: getattr(dialect, probe)() for probe in declined}
    assert declined and not any(
        values.values()
    ), f"{family} declines these members, but one answers True: {values}"
    for formatter in refused:
        with pytest.raises(UnsupportedFeatureError):
            getattr(dialect, formatter)(None)


@pytest.mark.parametrize(
    "sync_name, async_name, module", FAMILIES, ids=[row[0] for row in FAMILIES]
)
def test_protocols_come_from_the_family_modules(sync_name, async_name, module) -> None:
    """Both twins are the classes declared in their own ``introspect`` module."""
    sync = getattr(dialect_protocols, sync_name)
    async_twin = getattr(dialect_protocols, async_name)

    assert sync.__module__ == module
    assert async_twin.__module__ == module


def test_the_old_single_name_is_not_exported() -> None:
    """No alias survives the split, in either the protocol or dialect namespace."""
    assert not hasattr(dialect_protocols, "IntrospectionSupport")
    assert not hasattr(dialect_protocols, "AsyncIntrospectionSupport")

    from rhosocial.activerecord.backend import dialect

    assert not hasattr(dialect, "IntrospectionSupport")
    assert not hasattr(dialect, "AsyncIntrospectionSupport")


@pytest.mark.parametrize("version", [None, (10, 2, 0), (11, 2, 0), (23, 0, 0)])
def test_supports_partition_info_is_absent(version) -> None:
    """The deleted probe is gone at every version, not merely answering False."""
    dialect = OracleDialect(version=version)
    assert not hasattr(dialect, "supports_partition_info")


def test_the_partitions_family_resolves_to_the_metadata_probe() -> None:
    """The CLI reads Oracle's own switch, which the old core probe never did."""
    assert "partitions" in introspect_cli.INTROSPECT_TYPES
    assert (
        introspect_cli._TYPE_CAPABILITIES["partitions"]
        == "supports_partition_metadata_introspection"
    )

    dialect = OracleDialect(version=(23, 0, 0))
    probe = getattr(dialect, introspect_cli._TYPE_CAPABILITIES["partitions"])
    assert callable(probe)
    assert probe() is True


@pytest.mark.parametrize(
    "version, expected", [(None, False), ((10, 2, 0), False), ((11, 2, 0), True)]
)
def test_the_partition_family_is_version_gated(version, expected) -> None:
    """The metadata probe carries a release gate; the old probe carried none."""
    dialect = OracleDialect(version=version)
    assert dialect.supports_partition_metadata_introspection() is expected


def test_the_aggregates_declare_no_members_of_their_own() -> None:
    """Composition-only aggregates keep every family's override reachable."""
    assert [
        name for name in vars(dialect_mixins.IntrospectionMixin) if not name.startswith("_")
    ] == []
    assert [
        name for name in vars(dialect_mixins.AsyncIntrospectionMixin) if not name.startswith("_")
    ] == []

    assert dialect_mixins.IntrospectionMixin.__bases__ == (
        dialect_mixins.StructureIntrospectionMixin,
        dialect_mixins.OperationalStatsIntrospectionMixin,
        dialect_mixins.DataProfileIntrospectionMixin,
        dialect_mixins.DdlExtractionIntrospectionMixin,
    )
