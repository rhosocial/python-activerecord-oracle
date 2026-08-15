# src/rhosocial/activerecord/backend/impl/oracle/mixins/partition.py
"""Oracle table partitioning mixin.

This module implements the generic ``PartitionSupport`` protocol for
Oracle, plus the Oracle-specific partition strategies declared in
:class:`~rhosocial.activerecord.backend.impl.oracle.protocols.partition.OraclePartitionSupport`.

Phase 5 scope:

* Generic RANGE / LIST / HASH through the core ``PartitionClause``
  expression (Phase 4 backward compatibility). Partition definitions are
  read from ``PartitionClause.dialect_options`` for the legacy path.
* Backend-specific
  :class:`~rhosocial.activerecord.backend.impl.oracle.expression.partition.OraclePartitionByRange`
  / ``...ByList`` / ``...ByHash`` expressions with structured
  :class:`OraclePartitionDefinition` definitions.
* Oracle-specific INTERVAL / REFERENCE partitioning.
* Composite partitioning via :class:`OracleSubpartitionClause`.
* Partition maintenance statements (ADD / DROP / SPLIT / MERGE /
  EXCHANGE / MOVE / TRUNCATE) implemented in
  :mod:`.partition_lifecycle_mixin` (mixed in by the dialect).

Dispatch rule (expression-dialect-architecture §7.1): the public
``format_partition_clause`` formatter inspects the expression type via
``isinstance`` and dispatches to the backend-specific public formatters
(``format_partition_by_range`` etc.). The generic ``PartitionClause``
path is preserved for Phase 4 backward compatibility.

Note on parameter binding: Oracle does not accept bind variables in DDL
statements (CREATE TABLE ... PARTITION BY ...). Partition boundary
values are therefore rendered as safely-escaped inline literals by
``format_partition_value`` (mirroring the MySQL approach) rather than
returned as parameters. The values are still supplied through
``OraclePartitionValue`` / ``Literal`` expressions so they flow through
the Expression/Dialect layer and are escaped by the dialect.
"""
from datetime import date, datetime
from decimal import Decimal
from math import isfinite
from typing import Any, List, Sequence, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.core import Literal

from ..expression.partition import (
    OracleIntervalPartitionClause,
    OraclePartitionByHash,
    OraclePartitionByList,
    OraclePartitionByRange,
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
    OracleReferencePartitionClause,
    OracleSubpartitionClause,
    OracleSubpartitionDefinition,
)

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.statements import PartitionClause


# Minimum Oracle version that supports basic table partitioning.
# Oracle introduced range partitioning in 8.0, list in 9i, hash in 8i.
# 11g is used as the conservative baseline because it is the oldest version
# this backend actively supports and tests against.
_ORACLE_PARTITION_MIN_VERSION: Tuple[int, int, int] = (11, 0, 0)

# INTERVAL partitioning was introduced in Oracle 11g.
_ORACLE_INTERVAL_MIN_VERSION: Tuple[int, int, int] = (11, 1, 0)

# REFERENCE partitioning was introduced in Oracle 11g.
_ORACLE_REFERENCE_MIN_VERSION: Tuple[int, int, int] = (11, 1, 0)


class OraclePartitionMixin:
    """Oracle table partitioning implementation.

    Implements the generic ``PartitionSupport`` protocol for RANGE / LIST /
    HASH strategies and the Oracle-specific ``OraclePartitionSupport``
    protocol for INTERVAL / REFERENCE / composite partitioning and
    maintenance statements.
    """

    # ------------------------------------------------------------------
    # Capability methods (PartitionSupport)
    # ------------------------------------------------------------------
    def supports_table_partitioning(self) -> bool:
        """Oracle supports table partitioning since 8.0; backend tests 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_partitioned_table_creation(self) -> bool:
        """CREATE TABLE ... PARTITION BY is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_partition_metadata_introspection(self) -> bool:
        """Partition metadata is queryable through ALL_TAB_PARTITIONS."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_range_table_partitioning(self) -> bool:
        """RANGE partitioning is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_list_table_partitioning(self) -> bool:
        """LIST partitioning is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_hash_table_partitioning(self) -> bool:
        """HASH partitioning is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_subpartitioning(self) -> bool:
        """Oracle composite partitioning (subpartitioning) is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_interval_partitioning(self) -> bool:
        """INTERVAL partitioning is supported for Oracle 11g+."""
        return self.version >= _ORACLE_INTERVAL_MIN_VERSION

    def supports_reference_partitioning(self) -> bool:
        """REFERENCE partitioning is supported for Oracle 11g+."""
        return self.version >= _ORACLE_REFERENCE_MIN_VERSION

    # Maintenance statement capabilities. The actual formatters live on
    # the PartitionLifecycleMixin (mixed into the dialect alongside this
    # mixin). These flags gate the public API.
    def supports_add_partition(self) -> bool:
        """ADD PARTITION maintenance statement is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_drop_partition(self) -> bool:
        """DROP PARTITION maintenance statement is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_truncate_partition(self) -> bool:
        """TRUNCATE PARTITION maintenance statement is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_reorganize_partition(self) -> bool:
        """Partition reorganization (SPLIT/MERGE) is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_attach_partition(self) -> bool:
        """ATTACH PARTITION is not applicable to Oracle (use EXCHANGE)."""
        return False

    def supports_detach_partition(self) -> bool:
        """DETACH PARTITION is not applicable to Oracle (use EXCHANGE)."""
        return False

    def supports_split_partition(self) -> bool:
        """SPLIT PARTITION maintenance statement is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_merge_partition(self) -> bool:
        """MERGE PARTITIONS maintenance statement is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_exchange_partition(self) -> bool:
        """EXCHANGE PARTITION maintenance statement is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    def supports_move_partition(self) -> bool:
        """MOVE PARTITION maintenance statement is supported for 11g+."""
        return self.version >= _ORACLE_PARTITION_MIN_VERSION

    # ------------------------------------------------------------------
    # Top-level dispatch formatter
    # ------------------------------------------------------------------
    def format_partition_clause(self, expr: "PartitionClause") -> Tuple[str, tuple]:
        """Format Oracle ``PARTITION BY`` clause from a partition expression.

        Dispatches by ``isinstance`` to the backend-specific public
        formatters first (``format_partition_by_range`` etc.), falling
        back to the legacy generic ``PartitionClause`` path (Phase 4
        compatibility) that reads ``expr.dialect_options``.

        Args:
            expr: a core ``PartitionClause`` (legacy) or an Oracle-specific
                subclass (``OraclePartitionByRange`` etc.).

        Returns:
            Tuple of (SQL string, parameters tuple). The SQL string is
            prefixed with a leading space so it can be appended directly
            to a CREATE TABLE statement. Partition boundary values are
            inlined as escaped literals (Oracle DDL does not accept bind
            variables), so the parameters tuple is always empty.

        Raises:
            UnsupportedFeatureError: if table partitioning or the requested
                strategy is not supported by the current Oracle version.
            ValueError: if the partition method is invalid or required
                partition definition data is missing/malformed.
            TypeError: if partition definition fields have wrong types.
        """
        if not self.supports_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "table partitioning",
                f"Oracle {self.version} does not support table partitioning.",
            )
        if not self.supports_partitioned_table_creation():
            raise UnsupportedFeatureError(
                self.name,
                "partitioned table creation",
                f"Oracle {self.version} cannot create partitioned tables.",
            )

        # Backend-specific expressions (Phase 5 structured form).
        if isinstance(expr, OracleIntervalPartitionClause):
            return self.format_interval_partition(expr)
        if isinstance(expr, OracleReferencePartitionClause):
            return self.format_reference_partition(expr)
        if isinstance(expr, OraclePartitionByRange):
            return self.format_partition_by_range(expr)
        if isinstance(expr, OraclePartitionByList):
            return self.format_partition_by_list(expr)
        if isinstance(expr, OraclePartitionByHash):
            return self.format_partition_by_hash(expr)

        # Legacy generic PartitionClause path (Phase 4 compatibility).
        method = expr.method.upper()
        if method == "RANGE":
            return self._format_legacy_range(expr)
        if method == "LIST":
            return self._format_legacy_list(expr)
        if method == "HASH":
            return self._format_legacy_hash(expr)
        raise ValueError(
            f"Invalid Oracle partition method: {expr.method!r}. "
            "Supported: RANGE, LIST, HASH, INTERVAL, REFERENCE."
        )

    # ------------------------------------------------------------------
    # Backend-specific strategy formatters
    # ------------------------------------------------------------------
    def format_partition_by_range(self, expr: OraclePartitionByRange) -> Tuple[str, tuple]:
        """Format ``PARTITION BY RANGE(col) (...)`` with structured definitions."""
        if not self.supports_range_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "RANGE partitioning",
                f"Oracle {self.version} does not support RANGE partitioning.",
            )
        key_sql = self.format_partition_keys(expr.keys)
        sql = f"PARTITION BY RANGE ({key_sql})"
        if expr.subpartition_by is not None:
            sub_sql, _ = expr.subpartition_by.to_sql()
            sql = f"{sql} {sub_sql.strip()}"
        if expr.partitions:
            part_sqls = [self.format_partition_definition(p, strategy="RANGE")[0] for p in expr.partitions]
            sql = f"{sql} ({', '.join(part_sqls)})"
        return f" {sql}", ()

    def format_partition_by_list(self, expr: OraclePartitionByList) -> Tuple[str, tuple]:
        """Format ``PARTITION BY LIST(col) (...)`` with structured definitions."""
        if not self.supports_list_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "LIST partitioning",
                f"Oracle {self.version} does not support LIST partitioning.",
            )
        key_sql = self.format_partition_keys(expr.keys)
        sql = f"PARTITION BY LIST ({key_sql})"
        if expr.subpartition_by is not None:
            sub_sql, _ = expr.subpartition_by.to_sql()
            sql = f"{sql} {sub_sql.strip()}"
        if expr.partitions:
            part_sqls = [self.format_partition_definition(p, strategy="LIST")[0] for p in expr.partitions]
            sql = f"{sql} ({', '.join(part_sqls)})"
        return f" {sql}", ()

    def format_partition_by_hash(self, expr: OraclePartitionByHash) -> Tuple[str, tuple]:
        """Format ``PARTITION BY HASH(col) PARTITIONS N`` (or explicit list)."""
        if not self.supports_hash_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "HASH partitioning",
                f"Oracle {self.version} does not support HASH partitioning.",
            )
        key_sql = self.format_partition_keys(expr.keys)
        sql = f"PARTITION BY HASH ({key_sql})"
        if expr.subpartition_by is not None:
            sub_sql, _ = expr.subpartition_by.to_sql()
            sql = f"{sql} {sub_sql.strip()}"
        if expr.partitions_count is not None:
            sql = f"{sql} PARTITIONS {expr.partitions_count}"
        elif expr.partitions:
            part_sqls = [self.format_partition_definition(p, strategy="HASH")[0] for p in expr.partitions]
            sql = f"{sql} ({', '.join(part_sqls)})"
        return f" {sql}", ()

    def format_interval_partition(self, expr: OracleIntervalPartitionClause) -> Tuple[str, tuple]:
        """Format ``PARTITION BY RANGE(col) INTERVAL(expr) (...)`` (11g+)."""
        if not self.supports_interval_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "INTERVAL partitioning",
                f"Oracle {self.version} does not support INTERVAL partitioning (requires 11g+).",
            )
        if not self.supports_range_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "RANGE partitioning",
                f"Oracle {self.version} does not support RANGE partitioning.",
            )
        key_sql = self.format_partition_keys(expr.keys)
        interval_sql, interval_params = expr.interval.to_sql()
        if interval_params:
            raise ValueError(
                "INTERVAL partitioning expression must not produce bind "
                "parameters (Oracle DDL does not accept bind variables). "
                "Use a RawSQLExpression for the interval expression."
            )
        sql = f"PARTITION BY RANGE ({key_sql}) INTERVAL ({interval_sql})"
        part_sqls = [self.format_partition_definition(p, strategy="RANGE")[0] for p in expr.partitions]
        if part_sqls:
            sql = f"{sql} ({', '.join(part_sqls)})"
        return f" {sql}", ()

    def format_reference_partition(self, expr: OracleReferencePartitionClause) -> Tuple[str, tuple]:
        """Format ``PARTITION BY REFERENCE(fk_constraint)`` (11g+)."""
        if not self.supports_reference_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "REFERENCE partitioning",
                f"Oracle {self.version} does not support REFERENCE partitioning (requires 11g+).",
            )
        fk_sql = self.format_identifier(expr.fk_constraint)
        return f" PARTITION BY REFERENCE ({fk_sql})", ()

    def format_subpartition_by(self, expr: OracleSubpartitionClause) -> Tuple[str, tuple]:
        """Format ``SUBPARTITION BY {RANGE|LIST|HASH}(...)`` template clause."""
        if not self.supports_subpartitioning():
            raise UnsupportedFeatureError(
                self.name,
                "subpartitioning",
                f"Oracle {self.version} does not support composite partitioning.",
            )
        strategy = expr.strategy.value
        if strategy == "HASH":
            key_sql = self.format_partition_keys(expr.keys) if expr.keys else ""
            sql = f"SUBPARTITION BY HASH ({key_sql})"
            if expr.count is not None:
                sql = f"{sql} SUBPARTITIONS {expr.count}"
            return f" {sql}", ()
        # RANGE / LIST subpartition template: only the BY clause is emitted
        # here; individual subpartition definitions are emitted per-partition
        # in format_partition_definition.
        key_sql = self.format_partition_keys(expr.keys) if expr.keys else ""
        sql = f"SUBPARTITION BY {strategy} ({key_sql})"
        return f" {sql}", ()

    # ------------------------------------------------------------------
    # Partition / subpartition definition formatters
    # ------------------------------------------------------------------
    def format_partition_definition(
        self,
        definition: OraclePartitionDefinition,
        *,
        strategy: str,
    ) -> Tuple[str, tuple]:
        """Format a single ``PARTITION ... VALUES ...`` definition.

        Args:
            definition: an :class:`OraclePartitionDefinition`.
            strategy: ``"RANGE"`` / ``"LIST"`` / ``"HASH"`` — determines
                which boundary form to emit.

        Returns:
            Tuple of (SQL fragment string, empty parameters tuple).
        """
        if not isinstance(definition, OraclePartitionDefinition):
            raise TypeError(
                "definition must be an OraclePartitionDefinition, "
                f"got {type(definition).__name__}"
            )
        name_sql = self.format_identifier(definition.name)
        if strategy.upper() == "RANGE":
            if definition.less_than is None:
                raise ValueError(
                    f"RANGE partition {definition.name!r} requires 'less_than' boundary values"
                )
            value_parts = [self.format_partition_boundary_value(v) for v in definition.less_than]
            body = f"VALUES LESS THAN ({', '.join(value_parts)})"
        elif strategy.upper() == "LIST":
            if definition.in_values is None:
                raise ValueError(
                    f"LIST partition {definition.name!r} requires 'in_values' boundary values"
                )
            value_parts: List[str] = []
            for value in definition.in_values:
                if isinstance(value, (list, tuple)):
                    inner = [self.format_partition_boundary_value(v) for v in value]
                    value_parts.append(f"({', '.join(inner)})")
                else:
                    value_parts.append(self.format_partition_boundary_value(value))
            body = f"VALUES ({', '.join(value_parts)})"
        elif strategy.upper() == "HASH":
            # HASH partitions have no VALUES clause; only optional subpartitions.
            body = None
        else:
            raise ValueError(
                f"Invalid partition strategy for definition: {strategy!r}. "
                "Expected RANGE, LIST or HASH."
            )

        parts: List[str] = [f"PARTITION {name_sql}"]
        if body is not None:
            parts.append(body)
        if definition.subpartition_definitions:
            sub_parts = [
                self.format_subpartition_definition(d)[0] for d in definition.subpartition_definitions
            ]
            parts.append(f"({', '.join(sub_parts)})")
        return " ".join(parts), ()

    def format_subpartition_definition(
        self,
        definition: OracleSubpartitionDefinition,
    ) -> Tuple[str, tuple]:
        """Format a single ``SUBPARTITION name [VALUES ...]`` definition."""
        if not isinstance(definition, OracleSubpartitionDefinition):
            raise TypeError(
                "definition must be an OracleSubpartitionDefinition, "
                f"got {type(definition).__name__}"
            )
        name_sql = self.format_identifier(definition.name)
        parts: List[str] = [f"SUBPARTITION {name_sql}"]
        if definition.less_than is not None:
            value_parts = [self.format_partition_boundary_value(v) for v in definition.less_than]
            parts.append(f"VALUES LESS THAN ({', '.join(value_parts)})")
        elif definition.in_values is not None:
            value_parts: List[str] = []
            for value in definition.in_values:
                if isinstance(value, (list, tuple)):
                    inner = [self.format_partition_boundary_value(v) for v in value]
                    value_parts.append(f"({', '.join(inner)})")
                else:
                    value_parts.append(self.format_partition_boundary_value(value))
            parts.append(f"VALUES ({', '.join(value_parts)})")
        return " ".join(parts), ()

    def format_partition_value(self, expr: OraclePartitionValue) -> Tuple[str, tuple]:
        """Format a partition boundary value expression.

        Renders ``MAXVALUE`` for :class:`OraclePartitionMaxValue`, and an
        escaped inline literal for :class:`OraclePartitionValue`. Oracle
        DDL does not accept bind variables, so the parameters tuple is
        always empty.

        Returns:
            Tuple of (literal SQL string, empty parameters tuple).
        """
        if isinstance(expr, OraclePartitionMaxValue):
            return "MAXVALUE", ()
        if isinstance(expr, OraclePartitionValue):
            return self.render_partition_literal(expr.value), ()
        raise TypeError(
            "expr must be an OraclePartitionValue or OraclePartitionMaxValue, "
            f"got {type(expr).__name__}"
        )

    # ------------------------------------------------------------------
    # Shared helpers (public, no leading underscore per architecture rules)
    # ------------------------------------------------------------------
    def format_partition_keys(self, keys: Sequence[BaseExpression]) -> str:
        """Format the partition key column list.

        Partition keys are column references (``Column`` expressions), not
        user-supplied values, so they render as identifiers without
        parameters.
        """
        key_sql_parts: List[str] = []
        for key in keys:
            if not isinstance(key, BaseExpression):
                raise TypeError(
                    "partition keys must be BaseExpression instances, "
                    f"got {type(key).__name__}"
                )
            key_sql, _ = key.to_sql()
            key_sql_parts.append(key_sql)
        return ", ".join(key_sql_parts)

    def format_partition_boundary_value(self, value: Any) -> str:
        """Render a partition boundary value as a safe inline SQL literal.

        Oracle DDL does not accept bind variables, so boundary values are
        rendered as escaped literals directly in the SQL string. Values are
        supplied through ``OraclePartitionValue`` / ``Literal`` expressions
        (or :class:`OraclePartitionMaxValue`) so they still flow through the
        Expression/Dialect layer; the special string ``"MAXVALUE"``
        renders as the Oracle ``MAXVALUE`` keyword (upper bound for RANGE
        partitions).

        Args:
            value: an :class:`OraclePartitionValue`, an
                :class:`OraclePartitionMaxValue`, a ``Literal`` expression,
                or the string ``"MAXVALUE"``.

        Returns:
            SQL literal string (no parameters).
        """
        if isinstance(value, OraclePartitionMaxValue):
            return "MAXVALUE"
        if isinstance(value, OraclePartitionValue):
            return self.render_partition_literal(value.value)
        if isinstance(value, str) and value.upper() == "MAXVALUE":
            return "MAXVALUE"
        if isinstance(value, Literal):
            return self.render_partition_literal(value.value)
        raise TypeError(
            "partition boundary value must be an OraclePartitionValue, "
            "OraclePartitionMaxValue, Literal, or the string 'MAXVALUE', "
            f"got {type(value).__name__}"
        )

    def render_partition_literal(self, value: Any) -> str:
        """Render a Python scalar as a safe inline SQL literal.

        Mirrors MySQL ``format_partition_value`` semantics. Only scalar
        types that are valid Oracle partition boundary values are accepted;
        all string/byte values are escaped through ``_escape_sql_string``.
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            raise TypeError("partition boundary value must not be bool")
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if not isfinite(value):
                raise ValueError("partition boundary float must be finite")
            return repr(value)
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise ValueError("partition boundary Decimal must be finite")
            return str(value)
        if isinstance(value, datetime):
            escaped = self._escape_sql_string(value.isoformat(sep=" "))
            return f"TIMESTAMP '{escaped}'"
        if isinstance(value, date):
            escaped = self._escape_sql_string(value.isoformat())
            return f"DATE '{escaped}'"
        if isinstance(value, str):
            escaped = self._escape_sql_string(value)
            return f"'{escaped}'"
        raise TypeError(
            "partition boundary value must be str, int, float, Decimal, "
            "date, datetime, or None, got "
            f"{type(value).__name__}"
        )

    # ------------------------------------------------------------------
    # Legacy generic PartitionClause path (Phase 4 compatibility)
    # ------------------------------------------------------------------
    def _format_legacy_range(self, expr: "PartitionClause") -> Tuple[str, tuple]:
        """Legacy RANGE path reading ``expr.dialect_options['partitions']``."""
        key_sql = self.format_partition_keys(expr.keys)
        sql = f"PARTITION BY RANGE ({key_sql})"
        partitions = expr.dialect_options.get("partitions") or []
        if partitions:
            parts = [self._format_legacy_range_definition(p) for p in partitions]
            sql = f"{sql} ({', '.join(parts)})"
        return f" {sql}", ()

    def _format_legacy_list(self, expr: "PartitionClause") -> Tuple[str, tuple]:
        """Legacy LIST path reading ``expr.dialect_options['partitions']``."""
        key_sql = self.format_partition_keys(expr.keys)
        sql = f"PARTITION BY LIST ({key_sql})"
        partitions = expr.dialect_options.get("partitions") or []
        if partitions:
            parts = [self._format_legacy_list_definition(p) for p in partitions]
            sql = f"{sql} ({', '.join(parts)})"
        return f" {sql}", ()

    def _format_legacy_hash(self, expr: "PartitionClause") -> Tuple[str, tuple]:
        """Legacy HASH path reading ``expr.dialect_options['partitions_count']``."""
        key_sql = self.format_partition_keys(expr.keys)
        sql = f"PARTITION BY HASH ({key_sql})"
        partitions_count = expr.dialect_options.get("partitions_count")
        if partitions_count is not None:
            if not isinstance(partitions_count, int) or isinstance(partitions_count, bool):
                raise TypeError(
                    "partitions_count must be an int, "
                    f"got {type(partitions_count).__name__}"
                )
            if partitions_count <= 0:
                raise ValueError(
                    f"partitions_count must be a positive integer, got {partitions_count}"
                )
            sql = f"{sql} PARTITIONS {partitions_count}"
        return f" {sql}", ()

    def _format_legacy_range_definition(self, partition: Any) -> str:
        """Render a single legacy RANGE partition definition dict."""
        if not isinstance(partition, dict):
            raise TypeError(
                f"RANGE partition definition must be a dict, got {type(partition).__name__}"
            )
        name = partition.get("name")
        less_than = partition.get("less_than")
        if not name or not isinstance(name, str):
            raise TypeError("RANGE partition definition requires a non-empty string 'name'")
        if less_than is None:
            raise ValueError(f"RANGE partition {name!r} requires 'less_than' boundary values")
        if not isinstance(less_than, (list, tuple)):
            raise TypeError(
                "'less_than' must be a list or tuple, "
                f"got {type(less_than).__name__}"
            )
        value_sql_parts = [self.format_partition_boundary_value(v) for v in less_than]
        return (
            f"PARTITION {self.format_identifier(name)} "
            f"VALUES LESS THAN ({', '.join(value_sql_parts)})"
        )

    def _format_legacy_list_definition(self, partition: Any) -> str:
        """Render a single legacy LIST partition definition dict."""
        if not isinstance(partition, dict):
            raise TypeError(
                f"LIST partition definition must be a dict, got {type(partition).__name__}"
            )
        name = partition.get("name")
        in_values = partition.get("in_values")
        if not name or not isinstance(name, str):
            raise TypeError("LIST partition definition requires a non-empty string 'name'")
        if in_values is None:
            raise ValueError(f"LIST partition {name!r} requires 'in_values' boundary values")
        if not isinstance(in_values, (list, tuple)):
            raise TypeError(
                "'in_values' must be a list or tuple, "
                f"got {type(in_values).__name__}"
            )
        value_sql_parts: List[str] = []
        for value in in_values:
            if isinstance(value, (list, tuple)):
                inner = [self.format_partition_boundary_value(v) for v in value]
                value_sql_parts.append(f"({', '.join(inner)})")
            else:
                value_sql_parts.append(self.format_partition_boundary_value(value))
        return (
            f"PARTITION {self.format_identifier(name)} "
            f"VALUES ({', '.join(value_sql_parts)})"
        )
