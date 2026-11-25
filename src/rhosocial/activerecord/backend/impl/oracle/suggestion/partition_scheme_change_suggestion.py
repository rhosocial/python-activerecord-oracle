# src/rhosocial/activerecord/backend/impl/oracle/suggestion/partition_scheme_change_suggestion.py
"""Oracle's partition-scheme-change suggestion.

This is the migration-time counterpart of the partition-lifecycle suggestions:
the frozen point carries the old and new partition declarations (method, keys,
and -- when declared -- a HASH partition count), and the answer reuses the
existing node set only where Oracle really can change a scheme in place.

The only such in-place change here is shrinking a HASH-partitioned table by one
partition: ``ALTER TABLE ... COALESCE PARTITION``. Oracle's clause is count-less
and HASH-only, so it is reused through ``suggest_partition_table_alter`` only
when the count drops by exactly one; any other shrink needs repeated
statements and is declined rather than silently collapsed. Changing the method,
the key columns, adding/removing partitioning altogether, or growing a HASH
table needs a rebuild (new table, copy, drop old) that this node set does not
expose. Statement-level ``IF EXISTS`` on such a rebuild is itself gated on 19c
RU 28 / 23ai (see ``mixins/if_exists.py``), so no rebuild DDL is fabricated.
"""

from __future__ import annotations

from typing import Any

from rhosocial.activerecord.backend.suggestion import PartitionSchemeChangeSuggestion

from ._change_common import answer, projection_params

__all__ = ["OraclePartitionSchemeChangeSuggestionMixin"]

_HASH = "HASH"


def _method(projection: dict) -> str:
    """Read the strategy word from a frozen partition projection.

    A core ``PartitionClause`` projection carries ``method``; an Oracle HASH
    clause projects its own constructor instead, where the count key is the
    tell that the strategy is HASH (only HASH declares ``partitions_count``).
    """
    method = projection.get("method")
    if method:
        return str(method).strip().upper()
    if "partitions_count" in projection:
        return _HASH
    return ""


class OraclePartitionSchemeChangeSuggestionMixin:
    """Reuse COALESCE for a one-partition HASH shrink; decline all other changes."""

    def suggest_partition_scheme_change(self, point: Any) -> PartitionSchemeChangeSuggestion:
        """Answer with a real COALESCE chain only for a one-step HASH shrink.

        Both projections are decoded from the frozen point. Equal (or both
        absent) schemes need no action (``None``). A HASH table whose partition
        count drops by exactly one is expressed with the existing
        ``OracleCoalescePartitionExpression`` through
        ``suggest_partition_table_alter``. Everything else is a rebuild, which
        this backend declines explicitly (``False``) rather than fake.
        """
        old_raw = point.old_partition
        new_raw = point.new_partition
        if old_raw is None and new_raw is None:
            return answer(point, None)
        if old_raw == new_raw:
            return answer(point, None)

        old = projection_params(old_raw)
        new = projection_params(new_raw)
        if old is None or new is None:
            # Adding or removing partitioning entirely requires a table rebuild.
            return answer(point, False)

        old_method = _method(old)
        new_method = _method(new)
        if old_method != new_method:
            # Changing the partitioning method is a rebuild, not an in-place DDL.
            return answer(point, False)

        if old_method == _HASH:
            old_count = old.get("partitions_count")
            new_count = new.get("partitions_count")
            if (
                isinstance(old_count, int)
                and not isinstance(old_count, bool)
                and isinstance(new_count, int)
                and not isinstance(new_count, bool)
                and new_count == old_count - 1
            ):
                # COALESCE PARTITION is count-less and HASH-only: one statement
                # removes one partition. Reuse the existing table-alter
                # suggestion so capability gating and SQL stay in one place.
                altered = self.suggest_partition_table_alter(
                    _HASH, {"table": point.table_name, "count": 1}
                )
                return answer(point, altered.operations)
            # Any other HASH count change (grow, or shrink by more than one)
            # cannot be a single count-less coalesce.
            return answer(point, False)

        # RANGE / LIST / INTERVAL / REFERENCE cannot change their scheme in
        # place; the honest answer is the explicit "no mechanism" state.
        return answer(point, False)
