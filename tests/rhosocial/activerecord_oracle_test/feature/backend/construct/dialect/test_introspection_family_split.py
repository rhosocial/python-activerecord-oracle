# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/dialect/test_introspection_family_split.py
"""Guard the Oracle-facing surface after core split its introspection protocol.

Core replaced the single ``IntrospectionSupport`` with four family protocols
(and async twins) and deleted ``supports_partition_info`` without aliases. This
file pins the consequences that belong to this backend:

* the four families import from the new ``protocols.introspect`` modules, and
  the old names are gone;
* a constructed dialect no longer has ``supports_partition_info``;
* the CLI's ``partitions`` family still resolves to the partition-metadata
  probe, which is version-gated from 11g -- the probe the deleted one
  under-reported.
"""

import pytest

from rhosocial.activerecord.backend.dialect import mixins as dialect_mixins
from rhosocial.activerecord.backend.dialect import protocols as dialect_protocols
from rhosocial.activerecord.backend.impl.oracle.cli import introspect as introspect_cli
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

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


@pytest.mark.parametrize("sync_name, async_name, module", FAMILIES, ids=[f[0] for f in FAMILIES])
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
