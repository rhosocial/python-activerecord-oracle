# src/rhosocial/activerecord/backend/impl/oracle/partition/maintenance.py
"""Oracle partition maintenance: plan in, tree out, per-statement DDL.

The executor is deliberately owner-agnostic. It knows a plan's parent, keys,
policies and name pattern; it knows nothing about which application owns the
table, which retention decision produced the cutoff, or which application
table a partition belongs to.

Oracle's DDL is **not** transactional: ``ALTER TABLE ... ADD/DROP PARTITION``
commits implicitly. The executor therefore never promises that a group of
statements succeeds or fails together. It:

* reads the dictionary first and only issues statements for missing partitions,
* executes one statement at a time,
* records every failure in ``OraclePartitionReport.errors``, and
* reports what actually exists afterwards.

An earlier success stands even when a later statement fails; that is Oracle's
behaviour, not a hidden transaction.

Public surface is mirrored sync/async:

* :class:`OraclePartitionMaintenance`
* :class:`AsyncOraclePartitionMaintenance`

Both expose ``ensure``, ``list``, ``drop_before``, ``drop_partition`` and
``validate``.
"""

from dataclasses import replace
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.expression import Column, Literal, QueryStatement
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.operators import RawSQL
from rhosocial.activerecord.backend.expression.predicates import (
    ComparisonPredicate,
    LogicalPredicate,
)
from rhosocial.activerecord.backend.impl.oracle.expression.partition import (
    OraclePartitionDefinition,
    OraclePartitionMaxValue,
    OraclePartitionValue,
    OracleSubpartitionDefinition,
)
from rhosocial.activerecord.backend.impl.oracle.expression.partition_lifecycle import (
    OracleAddPartitionStatement,
    OracleDropPartitionStatement,
)

from .high_value import parse_high_value
from .plan import (
    OraclePartitionPlan,
    OraclePartitionSpec,
    OracleSubpartitionSpec,
)
from .report import (
    OraclePartitionInfo,
    OraclePartitionOperation,
    OraclePartitionReport,
)

__all__ = [
    "AsyncOraclePartitionMaintenance",
    "OraclePartitionMaintenance",
]

_NO_STRATEGY = "NONE"
_MAXVALUE = "MAXVALUE"


class _OraclePartitionMaintenanceBase:
    """Plan/catalog/DDL logic shared by the sync and async executors.

    Subclasses implement only the three I/O seams: ``_fetch_rows``,
    ``_execute`` and ``_infos``. Everything else -- expected specs,
    missing/orphan comparison, continuity checks, DDL rendering and failure
    recording -- lives here once, so the two executors cannot drift apart.
    """

    def __init__(self, backend: Any) -> None:
        self.backend = backend
        self.dialect = backend.dialect

    # ------------------------------------------------------------------ I/O
    def _fetch_rows(self, sql: str, params: Tuple[Any, ...]):
        raise NotImplementedError

    def _execute(self, sql: str, params: Tuple[Any, ...]) -> None:
        raise NotImplementedError

    def _infos(self, plan: OraclePartitionPlan) -> Tuple[OraclePartitionInfo, ...]:
        raise NotImplementedError

    # --------------------------------------------------------------- catalog
    def _table(self, plan: OraclePartitionPlan) -> Table:
        return Table(self.dialect, plan.parent, schema_name=plan.schema)

    def _dictionary_query(
        self,
        dictionary_view: str,
        owner_column: str,
        columns: Sequence[str],
        schema: Optional[str],
        target_name: str,
    ) -> QueryStatement:
        d = self.dialect
        table_condition = ComparisonPredicate(
            d, "eq", Column(d, "TABLE_NAME"), Literal(d, target_name.upper())
        )
        if schema:
            owner_condition = ComparisonPredicate(
                d, "eq", Column(d, owner_column), Literal(d, schema.upper())
            )
        else:
            owner_condition = ComparisonPredicate(
                d,
                "eq",
                Column(d, owner_column),
                RawSQL(d, "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')"),
            )
        where = LogicalPredicate(d, "AND", table_condition, owner_condition)
        return QueryStatement(
            d,
            select=[Column(d, name) for name in columns],
            from_=Table(d, dictionary_view),
            where=where,
        )

    def _root_query(self, plan: OraclePartitionPlan) -> QueryStatement:
        return self._dictionary_query(
            "ALL_PART_TABLES",
            "OWNER",
            (
                "OWNER",
                "TABLE_NAME",
                "PARTITIONING_TYPE",
                "SUBPARTITIONING_TYPE",
                "PARTITION_COUNT",
                "DEF_SUBPARTITION_COUNT",
            ),
            plan.schema,
            plan.parent,
        )

    def _partition_query(self, plan: OraclePartitionPlan) -> QueryStatement:
        return self._dictionary_query(
            "ALL_TAB_PARTITIONS",
            "TABLE_OWNER",
            (
                "PARTITION_NAME",
                "SUBPARTITION_COUNT",
                "COMPOSITE",
                "HIGH_VALUE",
                "PARTITION_POSITION",
            ),
            plan.schema,
            plan.parent,
        )

    def _subpartition_query(self, plan: OraclePartitionPlan) -> QueryStatement:
        return self._dictionary_query(
            "ALL_TAB_SUBPARTITIONS",
            "TABLE_OWNER",
            (
                "PARTITION_NAME",
                "SUBPARTITION_NAME",
                "HIGH_VALUE",
                "SUBPARTITION_POSITION",
            ),
            plan.schema,
            plan.parent,
        )

    def _parse_rows(
        self,
        plan: OraclePartitionPlan,
        root_rows: Sequence[Dict[str, Any]],
        partition_rows: Sequence[Dict[str, Any]],
        subpartition_rows: Sequence[Dict[str, Any]],
    ) -> Tuple[OraclePartitionInfo, ...]:
        if not root_rows:
            return ()
        root_row = root_rows[0]
        subpartition_method = root_row.get("subpartitioning_type")
        if subpartition_method == _NO_STRATEGY:
            subpartition_method = None
        root = OraclePartitionInfo(
            name=root_row.get("table_name") or plan.parent,
            parent=None,
            level=0,
            schema=root_row.get("owner") or plan.schema,
            method=root_row.get("partitioning_type"),
            subpartition_method=subpartition_method,
            subpartition_count=_optional_int(root_row.get("def_subpartition_count")),
            is_partitioned=True,
        )
        infos: List[OraclePartitionInfo] = [root]
        partitions = sorted(
            partition_rows,
            key=lambda row: int(row.get("partition_position") or 0),
        )
        previous_end: Any = None
        for row in partitions:
            count = _optional_int(row.get("subpartition_count"))
            bound = row.get("high_value")
            extras = _bound_extras(root.method, bound, previous_end)
            if root.method == "RANGE":
                previous_end = extras.get("range_end")
            infos.append(
                OraclePartitionInfo(
                    name=row["partition_name"],
                    parent=root.name,
                    level=1,
                    schema=root.schema,
                    method=root.method,
                    subpartition_method=subpartition_method,
                    bound=bound,
                    subpartition_count=count,
                    is_partitioned=bool(count),
                    extras=extras,
                )
            )
        subpartitions = sorted(
            subpartition_rows,
            key=lambda row: (
                int(row.get("subpartition_position") or 0),
                str(row.get("partition_name") or ""),
            ),
        )
        for row in subpartitions:
            bound = row.get("high_value")
            extras = _bound_extras(subpartition_method, bound, None)
            infos.append(
                OraclePartitionInfo(
                    name=row["subpartition_name"],
                    parent=row["partition_name"],
                    level=2,
                    schema=root.schema,
                    method=subpartition_method,
                    subpartition_method=subpartition_method,
                    bound=bound,
                    extras=extras,
                )
            )
        return tuple(infos)

    # ---------------------------------------------------------------- expected
    def _expected(
        self, plan: OraclePartitionPlan, through: Optional[Any]
    ) -> Tuple[OraclePartitionSpec, ...]:
        specs = tuple(plan.iter_specs(through=through))
        if not specs:
            raise ValueError("plan describes no partitions")
        return specs

    def _safe_expected(
        self, plan: OraclePartitionPlan, through: Optional[Any] = None
    ) -> Tuple[OraclePartitionSpec, ...]:
        """Return expected specs, or nothing when the plan has no horizon."""
        try:
            return self._expected(plan, through)
        except ValueError:
            return ()

    @staticmethod
    def _actual_names(
        infos: Sequence[OraclePartitionInfo],
    ) -> Dict[str, OraclePartitionInfo]:
        return {info.name.upper(): info for info in infos if info.level == 1}

    def _missing(
        self,
        specs: Sequence[OraclePartitionSpec],
        actual: Dict[str, OraclePartitionInfo],
    ) -> Tuple[Tuple[str, str], ...]:
        return tuple(
            (spec.parent, spec.name)
            for spec in specs
            if spec.name.upper() not in actual
        )

    def _needs_create(
        self,
        spec: OraclePartitionSpec,
        actual: Dict[str, OraclePartitionInfo],
    ) -> bool:
        """Whether *spec* is absent or present with an untrustworthy bound."""
        info = actual.get(spec.name.upper())
        if info is None:
            return True
        return bool(self._bound_issues(spec, info))

    def _orphans(
        self,
        plan: OraclePartitionPlan,
        specs: Sequence[OraclePartitionSpec],
        actual: Dict[str, OraclePartitionInfo],
        through: Optional[Any],
    ) -> Tuple[str, ...]:
        if not plan.is_fully_bounded(through=through):
            return ()
        expected_names = {spec.name.upper() for spec in specs}
        return tuple(
            sorted(name for name in actual if name not in expected_names)
        )

    # -------------------------------------------------------------- rendering
    def _subpartition_definition(
        self,
        spec: OracleSubpartitionSpec,
        multi_column: bool,
    ) -> OracleSubpartitionDefinition:
        d = self.dialect
        less_than = None
        if spec.less_than is not None:
            less_than = [_boundary(d, value) for value in spec.less_than]
        in_values = None
        if spec.in_values is not None:
            if multi_column:
                in_values = [
                    tuple(_boundary(d, value) for value in group)
                    for group in spec.in_values
                ]
            else:
                in_values = [
                    _boundary(d, value)
                    for group in spec.in_values
                    for value in group
                ]
        return OracleSubpartitionDefinition(
            name=spec.name,
            less_than=less_than,
            in_values=in_values,
        )

    def _definition(self, plan: OraclePartitionPlan, spec: OraclePartitionSpec) -> OraclePartitionDefinition:
        d = self.dialect
        less_than = None
        if spec.less_than is not None:
            less_than = [_boundary(d, value) for value in spec.less_than]
        in_values = None
        if spec.in_values is not None:
            if len(spec.columns) > 1:
                in_values = [
                    tuple(_boundary(d, value) for value in group)
                    for group in spec.in_values
                ]
            else:
                in_values = [
                    _boundary(d, value)
                    for group in spec.in_values
                    for value in group
                ]
        subpartition_definitions = None
        if spec.subpartitions:
            multi_column = bool(
                plan.subpartition and len(plan.subpartition.columns) > 1
            )
            subpartition_definitions = [
                self._subpartition_definition(sub, multi_column)
                for sub in spec.subpartitions
            ]
        return OraclePartitionDefinition(
            name=spec.name,
            less_than=less_than,
            in_values=in_values,
            subpartition_definitions=subpartition_definitions,
            subpartitions_count=spec.subpartitions_count,
        )

    def _add_statement(
        self, plan: OraclePartitionPlan, spec: OraclePartitionSpec
    ) -> Tuple[str, Tuple[Any, ...]]:
        statement = OracleAddPartitionStatement(
            self.dialect,
            self._table(plan),
            self._definition(plan, spec),
        )
        return statement.to_sql()

    def _drop_statement(
        self, plan: OraclePartitionPlan, name: str
    ) -> Tuple[str, Tuple[Any, ...]]:
        statement = OracleDropPartitionStatement(
            self.dialect,
            self._table(plan),
            name,
        )
        return statement.to_sql()

    # -------------------------------------------------------------- validation
    def _continuity_issues(
        self, specs: Sequence[OraclePartitionSpec]
    ) -> Tuple[str, ...]:
        issues: List[str] = []
        group = [
            spec
            for spec in specs
            if spec.range_start is not None and spec.range_end is not None
        ]
        ordered = sorted(group, key=lambda item: item.range_start)
        for previous, current in zip(ordered, ordered[1:]):
            if current.range_start > previous.range_end:
                issues.append(
                    "gap in %s between %s and %s"
                    % (current.parent, previous.name, current.name)
                )
            elif current.range_start < previous.range_end:
                issues.append(
                    "overlap in %s between %s and %s"
                    % (current.parent, previous.name, current.name)
                )
        return tuple(issues)

    def _range_bound_issues(
        self, spec: OraclePartitionSpec, info: OraclePartitionInfo
    ) -> Tuple[str, ...]:
        expected = tuple(spec.less_than or ())
        if not expected:
            return ()
        text = info.bound
        if text is None or not str(text).strip():
            return (
                "partition %s bound %r is an unparsed bound (empty HIGH_VALUE)"
                % (spec.name, text),
            )
        parsed = parse_high_value(text, "RANGE")
        if not parsed.parsed:
            return (
                "partition %s bound %r is an unparsed bound (%s)"
                % (spec.name, text, parsed.error),
            )
        if not _bound_equal(parsed.values, expected):
            return (
                "partition %s boundary mismatch: catalogue has %s, plan expects %s"
                % (spec.name, _display_values(parsed.values), _display_values(expected)),
            )
        return ()

    def _list_bound_issues(
        self, spec: OraclePartitionSpec, info: OraclePartitionInfo
    ) -> Tuple[str, ...]:
        expected = _expected_list_values(spec)
        if not expected:
            return ()
        text = info.bound
        if text is None or not str(text).strip():
            return (
                "partition %s bound %r is an unparsed bound (empty HIGH_VALUE)"
                % (spec.name, text),
            )
        parsed = parse_high_value(text, "LIST")
        if not parsed.parsed:
            return (
                "partition %s bound %r is an unparsed bound (%s)"
                % (spec.name, text, parsed.error),
            )
        if not _unordered_equal(parsed.values, expected):
            return (
                "partition %s boundary mismatch: catalogue has %s, plan expects %s"
                % (spec.name, _display_values(parsed.values), _display_values(expected)),
            )
        return ()

    def _bound_issues(
        self, spec: OraclePartitionSpec, info: OraclePartitionInfo
    ) -> Tuple[str, ...]:
        if spec.method == "RANGE":
            return self._range_bound_issues(spec, info)
        if spec.method == "LIST":
            return self._list_bound_issues(spec, info)
        return ()

    def _bound_name_conflict(
        self,
        spec: OraclePartitionSpec,
        actual: Dict[str, OraclePartitionInfo],
        planned_names: Sequence[str],
    ) -> Optional[OraclePartitionInfo]:
        """Find an unplanned partition that carries *spec*'s planned bound."""
        if spec.method == "RANGE":
            expected = tuple(spec.less_than or ())
        elif spec.method == "LIST":
            expected = _expected_list_values(spec)
        else:
            return None
        if not expected:
            return None
        planned = set(planned_names)
        for name, info in actual.items():
            if name in planned:
                continue
            parsed = parse_high_value(info.bound, spec.method)
            if not parsed.parsed:
                continue
            if _unordered_equal(parsed.values, expected):
                return info
        return None

    def _structure_issues(
        self,
        plan: OraclePartitionPlan,
        infos: Sequence[OraclePartitionInfo],
        specs: Sequence[OraclePartitionSpec],
    ) -> Tuple[str, ...]:
        issues: List[str] = []
        roots = [info for info in infos if info.level == 0]
        if not roots:
            issues.append(
                "%s is not a partitioned table, or does not exist" % plan.parent
            )
            return tuple(issues)
        root = roots[0]
        if (root.method or "").upper() != plan.method:
            issues.append(
                "parent %s strategy mismatch: partitioned by %s, plan says %s"
                % (plan.parent, root.method, plan.method)
            )
        planned_sub_method = (
            plan.subpartition.method if plan.subpartition is not None else None
        )
        actual_sub_method = root.subpartition_method
        if planned_sub_method != actual_sub_method:
            issues.append(
                "parent %s subpartition strategy mismatch: uses %s subpartitions, "
                "plan says %s"
                % (
                    plan.parent,
                    actual_sub_method or "no",
                    planned_sub_method or "no",
                )
            )
        actual = self._actual_names(infos)
        subpartitions_by_partition = _subpartition_names(infos)
        planned_names = tuple(spec.name.upper() for spec in specs)
        for spec in specs:
            info = actual.get(spec.name.upper())
            if info is None:
                conflict = self._bound_name_conflict(spec, actual, planned_names)
                if conflict is not None:
                    issues.append(
                        "partition %s is missing; partition %s carries its "
                        "planned bound (boundary mismatch)"
                        % (spec.name, conflict.name)
                    )
                continue
            issues.extend(self._bound_issues(spec, info))
            expected_count = plan.expected_subpartition_count(spec)
            if expected_count is not None:
                actual_count = info.subpartition_count
                if actual_count != expected_count:
                    issues.append(
                        "partition %s has %s subpartitions, plan expects %s"
                        % (spec.name, actual_count, expected_count)
                    )
            if spec.subpartitions:
                expected_names = {sub.name.upper() for sub in spec.subpartitions}
                actual_names = set(subpartitions_by_partition.get(spec.name.upper(), ()))
                missing = sorted(expected_names - actual_names)
                if missing:
                    issues.append(
                        "partition %s is missing subpartitions %s"
                        % (spec.name, ", ".join(missing))
                    )
        if plan.method == "HASH" and (root.method or "").upper() == "HASH":
            actual_count = sum(
                1 for row in infos if row.level == 1
            )
            if actual_count != len(specs):
                issues.append(
                    "parent %s hash strategy mismatch: catalogue has %d "
                    "partitions, plan expects %d"
                    % (plan.parent, actual_count, len(specs))
                )
        # Oracle cannot ADD above an existing MAXVALUE partition; adding it
        # requires SPLIT. Report the impossibility rather than hiding it.
        maxvalue = [info for info in actual.values() if _is_maxvalue(info)]
        if maxvalue:
            terminator = maxvalue[0]
            for spec in specs:
                if spec.name.upper() in actual:
                    continue
                if spec.range_end is None:
                    continue
                if terminator.name.upper() not in {spec.name.upper()}:
                    issues.append(
                        "partition %s cannot be added while MAXVALUE partition %s "
                        "exists; SPLIT it first"
                        % (spec.name, terminator.name)
                    )
                    break
        return tuple(issues)

    def _report(
        self,
        plan: OraclePartitionPlan,
        infos: Sequence[OraclePartitionInfo],
        specs: Sequence[OraclePartitionSpec],
        through: Optional[Any],
        operations: Sequence[OraclePartitionOperation] = (),
        errors: Sequence[str] = (),
        dry_run: bool = False,
    ) -> OraclePartitionReport:
        actual = self._actual_names(infos)
        issues = self._structure_issues(plan, infos, specs) + self._continuity_issues(specs)
        return OraclePartitionReport(
            target=plan.parent,
            partitions=tuple(infos),
            missing=self._missing(specs, actual),
            orphans=self._orphans(plan, specs, actual, through),
            issues=issues,
            operations=tuple(operations),
            errors=tuple(errors),
            dry_run=dry_run,
        )

    def _render_creates(
        self, plan: OraclePartitionPlan, specs: Sequence[OraclePartitionSpec]
    ) -> Tuple[OraclePartitionOperation, ...]:
        return tuple(
            OraclePartitionOperation(
                action="create",
                name=spec.name,
                parent=spec.parent,
                sql=self._add_statement(plan, spec)[0],
                level=1,
            )
            for spec in specs
        )

    @staticmethod
    def _without(
        specs: Sequence[OraclePartitionSpec], names: Sequence[str]
    ) -> Tuple[OraclePartitionSpec, ...]:
        removed = {name.upper() for name in names}
        return tuple(spec for spec in specs if spec.name.upper() not in removed)

    def _drop_candidates(
        self,
        plan: OraclePartitionPlan,
        cutoff: Any,
        infos: Sequence[OraclePartitionInfo],
        through: Optional[Any],
    ) -> Tuple[OraclePartitionSpec, ...]:
        from datetime import date, datetime

        if not isinstance(cutoff, (date, datetime)):
            raise TypeError("cutoff must be a date or datetime")
        actual = self._actual_names(infos)
        specs = self._expected(plan, through=through if through is not None else cutoff)
        candidates: List[OraclePartitionSpec] = []
        for spec in specs:
            if spec.range_end is None:
                continue
            try:
                early = spec.range_end <= cutoff
            except TypeError as exc:
                raise TypeError(
                    "cutoff must match the plan's bound type (date vs datetime)"
                ) from exc
            if not early:
                continue
            info = actual.get(spec.name.upper())
            if info is not None:
                candidates.append(spec)
        return tuple(candidates)


class OraclePartitionMaintenance(_OraclePartitionMaintenanceBase):
    """Synchronous Oracle partition maintenance executor.

    Example:
        >>> maintenance = OraclePartitionMaintenance(backend)
        >>> report = maintenance.ensure(plan, through=date(2027, 1, 1))
        >>> report.missing
        ()
    """

    def _fetch_rows(self, sql: str, params: Tuple[Any, ...]):
        return self.backend.fetch_all(sql, params)

    def _execute(self, sql: str, params: Tuple[Any, ...]) -> None:
        self.backend.execute(sql, params)

    def _infos(self, plan: OraclePartitionPlan) -> Tuple[OraclePartitionInfo, ...]:
        root_sql, root_params = self._root_query(plan).to_sql()
        root_rows = self._fetch_rows(root_sql, root_params)
        if not root_rows:
            return ()
        partition_sql, partition_params = self._partition_query(plan).to_sql()
        subpartition_sql, subpartition_params = self._subpartition_query(plan).to_sql()
        return self._parse_rows(
            plan,
            root_rows,
            self._fetch_rows(partition_sql, partition_params),
            self._fetch_rows(subpartition_sql, subpartition_params),
        )

    def _run(self, action, specs: Sequence[OraclePartitionSpec]) -> None:
        """Execute one statement per spec, recording each failure."""
        for spec in specs:
            action(spec)

    def list(
        self,
        plan: OraclePartitionPlan,
        through: Optional[Any] = None,
    ) -> OraclePartitionReport:
        """Read the live tree and compare it with the plan.

        Args:
            plan: Tree description.
            through: Exclusive horizon used to expand generated RANGE policies.

        Returns:
            A report with the dictionary tree, missing partitions, orphans,
            strategy, boundary and subpartition issues plus continuity
            problems. No DDL is executed.
        """
        infos = self._infos(plan)
        specs = self._safe_expected(plan, through)
        return self._report(plan, infos, specs, through)

    def ensure(
        self,
        plan: OraclePartitionPlan,
        through: Optional[Any] = None,
    ) -> OraclePartitionReport:
        """Create every partition the plan expects but the dictionary lacks.

        A partition is present only when its name *and* parsed bound agree with
        the spec; one that exists under the right name but carries a different
        ``HIGH_VALUE`` is attempted again and the server failure is recorded.
        Oracle has no ``ADD PARTITION IF NOT EXISTS``, so idempotency comes
        from reading the dictionary first. Each ``ALTER TABLE ... ADD PARTITION``
        commits independently; a failure is recorded and the remaining
        partitions are still attempted.

        Args:
            plan: Tree description.
            through: Exclusive horizon used to expand generated RANGE policies.

        Returns:
            The post-operation report, with executed DDL in ``operations`` and
            failures in ``errors``.

        Raises:
            ValueError: if the parent is not an existing partitioned table.
        """
        infos = self._infos(plan)
        if not infos:
            raise ValueError("partitioned parent %s does not exist" % plan.parent)
        specs = self._expected(plan, through)
        actual = self._actual_names(infos)
        missing = tuple(
            spec for spec in specs if self._needs_create(spec, actual)
        )
        operations: List[OraclePartitionOperation] = []
        errors: List[str] = []

        def create(spec: OraclePartitionSpec) -> None:
            sql, params = self._add_statement(plan, spec)
            try:
                self._execute(sql, params)
            except DatabaseError as exc:
                errors.append("%s: %s" % (spec.name, exc))
                return
            operations.append(
                OraclePartitionOperation("create", spec.name, spec.parent, sql, 1)
            )

        self._run(create, missing)
        report = self.list(plan, through)
        return replace(
            report,
            operations=tuple(operations),
            errors=tuple(errors),
            dry_run=False,
        )

    def drop_before(
        self,
        plan: OraclePartitionPlan,
        cutoff: Any,
        dry_run: bool = True,
        through: Optional[Any] = None,
    ) -> OraclePartitionReport:
        """Drop every top-level partition whose whole range ends at or before *cutoff*.

        Oracle has no subpartition-level drop in this API: ``DROP PARTITION``
        removes the top-level partition and its subpartitions together. A
        ``MAXVALUE`` partition has no finite end and is never a candidate.
        The cutoff is compared per branch by each branch's own policy, which
        is what lets a monthly branch and a yearly branch answer the same
        cutoff differently.

        Args:
            plan: Tree description.
            cutoff: Exclusive retention boundary (date or datetime).
            dry_run: When true (the default), render the drops without
                executing them.
            through: Optional wider horizon for expected-spec expansion.

        Returns:
            A report whose ``operations`` lists the (possibly not executed)
            drops.
        """
        infos = self._infos(plan)
        specs = self._safe_expected(
            plan, through=through if through is not None else cutoff
        )
        candidates = self._drop_candidates(plan, cutoff, infos, through)
        operations = tuple(
            OraclePartitionOperation(
                action="drop",
                name=spec.name,
                parent=spec.parent,
                sql=self._drop_statement(plan, spec.name)[0],
                level=1,
            )
            for spec in candidates
        )
        errors: List[str] = []
        if not dry_run:

            def drop(spec: OraclePartitionSpec) -> None:
                sql, params = self._drop_statement(plan, spec.name)
                try:
                    self._execute(sql, params)
                except DatabaseError as exc:
                    errors.append("%s: %s" % (spec.name, exc))

            self._run(drop, candidates)
            infos = self._infos(plan)
            specs = self._without(specs, [spec.name for spec in candidates])
        return self._report(
            plan,
            infos,
            specs,
            through,
            operations,
            errors,
            dry_run=dry_run,
        )

    def drop_partition(
        self,
        plan: OraclePartitionPlan,
        name: str,
        dry_run: bool = False,
    ) -> OraclePartitionReport:
        """Drop one named top-level partition and its subpartitions.

        This is the explicit destructive operation; ``drop_before`` never
        removes a partition whose bound it did not calculate.

        Args:
            plan: Tree description.
            name: Partition name as the dictionary reports it.
            dry_run: When true, render the drop without executing it.

        Returns:
            The report before (dry run) or after the drop.
        """
        infos = self._infos(plan)
        sql, params = self._drop_statement(plan, name)
        operation = OraclePartitionOperation("drop", name, plan.parent, sql, 1)
        errors: List[str] = []
        if not dry_run:
            try:
                self._execute(sql, params)
            except DatabaseError as exc:
                errors.append("%s: %s" % (name, exc))
            infos = self._infos(plan)
        specs = self._safe_expected(plan)
        specs = self._without(specs, [name])
        return self._report(
            plan, infos, specs, None, (operation,), errors, dry_run=dry_run
        )

    def validate(
        self,
        plan: OraclePartitionPlan,
        through: Optional[Any] = None,
    ) -> OraclePartitionReport:
        """Report continuity and structure problems without changing anything.

        Checks gaps and overlaps in the plan's generated RANGE boundaries,
        parent/subpartition-strategy mismatches against the dictionary,
        subpartition-count mismatches, missing partitions and orphans. It never
        repairs a gap; ``ensure`` is the explicit repair.

        Args:
            plan: Tree description.
            through: Exclusive horizon used to expand generated RANGE policies.

        Returns:
            A report whose ``issues`` lists every problem found.
        """
        return self.list(plan, through)


class AsyncOraclePartitionMaintenance(_OraclePartitionMaintenanceBase):
    """Asynchronous Oracle partition maintenance executor.

    The API mirrors :class:`OraclePartitionMaintenance` exactly; every method
    is a coroutine. DDL still commits per statement, so the async executor
    reports partial completion the same way.
    """

    async def _fetch_rows(self, sql: str, params: Tuple[Any, ...]):
        return await self.backend.fetch_all(sql, params)

    async def _execute(self, sql: str, params: Tuple[Any, ...]) -> None:
        await self.backend.execute(sql, params)

    async def _infos(
        self, plan: OraclePartitionPlan
    ) -> Tuple[OraclePartitionInfo, ...]:
        root_sql, root_params = self._root_query(plan).to_sql()
        root_rows = await self._fetch_rows(root_sql, root_params)
        if not root_rows:
            return ()
        partition_sql, partition_params = self._partition_query(plan).to_sql()
        subpartition_sql, subpartition_params = self._subpartition_query(plan).to_sql()
        return self._parse_rows(
            plan,
            root_rows,
            await self._fetch_rows(partition_sql, partition_params),
            await self._fetch_rows(subpartition_sql, subpartition_params),
        )

    async def list(
        self,
        plan: OraclePartitionPlan,
        through: Optional[Any] = None,
    ) -> OraclePartitionReport:
        """Read the live tree and compare it with the plan asynchronously."""
        infos = await self._infos(plan)
        specs = self._safe_expected(plan, through)
        return self._report(plan, infos, specs, through)

    async def ensure(
        self,
        plan: OraclePartitionPlan,
        through: Optional[Any] = None,
    ) -> OraclePartitionReport:
        """Create every missing partition asynchronously."""
        infos = await self._infos(plan)
        if not infos:
            raise ValueError("partitioned parent %s does not exist" % plan.parent)
        specs = self._expected(plan, through)
        actual = self._actual_names(infos)
        missing = tuple(
            spec for spec in specs if self._needs_create(spec, actual)
        )
        operations: List[OraclePartitionOperation] = []
        errors: List[str] = []

        for spec in missing:
            sql, params = self._add_statement(plan, spec)
            try:
                await self._execute(sql, params)
            except DatabaseError as exc:
                errors.append("%s: %s" % (spec.name, exc))
                continue
            operations.append(
                OraclePartitionOperation("create", spec.name, spec.parent, sql, 1)
            )

        report = await self.list(plan, through)
        return replace(
            report,
            operations=tuple(operations),
            errors=tuple(errors),
            dry_run=False,
        )

    async def drop_before(
        self,
        plan: OraclePartitionPlan,
        cutoff: Any,
        dry_run: bool = True,
        through: Optional[Any] = None,
    ) -> OraclePartitionReport:
        """Drop leaves ending at or before *cutoff* asynchronously."""
        infos = await self._infos(plan)
        specs = self._safe_expected(
            plan, through=through if through is not None else cutoff
        )
        candidates = self._drop_candidates(plan, cutoff, infos, through)
        operations = tuple(
            OraclePartitionOperation(
                action="drop",
                name=spec.name,
                parent=spec.parent,
                sql=self._drop_statement(plan, spec.name)[0],
                level=1,
            )
            for spec in candidates
        )
        errors: List[str] = []
        if not dry_run:
            for spec in candidates:
                sql, params = self._drop_statement(plan, spec.name)
                try:
                    await self._execute(sql, params)
                except DatabaseError as exc:
                    errors.append("%s: %s" % (spec.name, exc))
            infos = await self._infos(plan)
            specs = self._without(specs, [spec.name for spec in candidates])
        return self._report(
            plan,
            infos,
            specs,
            through,
            operations,
            errors,
            dry_run=dry_run,
        )

    async def drop_partition(
        self,
        plan: OraclePartitionPlan,
        name: str,
        dry_run: bool = False,
    ) -> OraclePartitionReport:
        """Drop one named top-level partition asynchronously."""
        infos = await self._infos(plan)
        sql, params = self._drop_statement(plan, name)
        operation = OraclePartitionOperation("drop", name, plan.parent, sql, 1)
        errors: List[str] = []
        if not dry_run:
            try:
                await self._execute(sql, params)
            except DatabaseError as exc:
                errors.append("%s: %s" % (name, exc))
            infos = await self._infos(plan)
        specs = self._safe_expected(plan)
        specs = self._without(specs, [name])
        return self._report(
            plan, infos, specs, None, (operation,), errors, dry_run=dry_run
        )

    async def validate(
        self,
        plan: OraclePartitionPlan,
        through: Optional[Any] = None,
    ) -> OraclePartitionReport:
        """Report continuity and structure problems asynchronously."""
        return await self.list(plan, through)


def _bound_extras(
    method: Optional[str], text: Optional[str], start: Optional[Any] = None
) -> Dict[str, Any]:
    """Keep the raw bound and add parsed facts when the parser succeeds."""
    extras: Dict[str, Any] = {}
    if text is not None:
        extras["high_value"] = text
    normalized = (method or "").strip().upper()
    if normalized not in ("RANGE", "LIST"):
        return extras
    parsed = parse_high_value(text, normalized)
    if parsed.parsed:
        if normalized == "RANGE":
            if parsed.values:
                extras["range_end"] = (
                    parsed.values[0]
                    if len(parsed.values) == 1
                    else parsed.values
                )
                if start is not None:
                    extras["range_start"] = start
        else:
            extras["values"] = parsed.values
    elif parsed.error is not None:
        extras["unparsed"] = parsed.error
    return extras


def _expected_list_values(spec: OraclePartitionSpec) -> Tuple[Any, ...]:
    """Flatten a single-column LIST spec; keep row groups for multi-column."""
    groups = spec.in_values or ()
    if len(spec.columns) > 1:
        return tuple(groups)
    return tuple(value for group in groups for value in group)


def _canonical(value: Any) -> Any:
    """Normalise a parsed value so plan values and catalogue text compare."""
    if isinstance(value, datetime):
        if (
            value.tzinfo is None
            and value.hour == 0
            and value.minute == 0
            and value.second == 0
            and value.microsecond == 0
        ):
            return value.date()
        return value
    if isinstance(value, tuple):
        return tuple(_canonical(item) for item in value)
    return value


def _bound_equal(actual: Sequence[Any], expected: Sequence[Any]) -> bool:
    """Compare two ordered bound sequences."""
    if len(actual) != len(expected):
        return False
    return all(
        _canonical(left) == _canonical(right)
        for left, right in zip(actual, expected)
    )


def _unordered_equal(actual: Sequence[Any], expected: Sequence[Any]) -> bool:
    """Compare two value sets without depending on catalogue order."""
    return _canonical_set(actual) == _canonical_set(expected)


def _canonical_set(values: Sequence[Any]) -> Any:
    try:
        return frozenset(_canonical(value) for value in values)
    except TypeError:
        return tuple(sorted((_canonical(value) for value in values), key=repr))


def _display_values(values: Sequence[Any]) -> str:
    return repr(tuple(values))


def _optional_int(value: Any) -> Optional[int]:
    """Return ``int(value)`` or ``None`` for a missing dictionary value."""
    return None if value is None else int(value)


def _boundary(dialect: Any, value: Any):
    """Turn a plan boundary value into its Oracle boundary expression."""
    if isinstance(value, str) and value.upper() == _MAXVALUE:
        return OraclePartitionMaxValue(dialect)
    if isinstance(value, (OraclePartitionValue, OraclePartitionMaxValue)):
        return value
    return OraclePartitionValue(dialect, value)


def _subpartition_names(
    infos: Sequence[OraclePartitionInfo],
) -> Dict[str, Tuple[str, ...]]:
    """Map an upper-case top-level partition name to its subpartition names."""
    grouped: Dict[str, Tuple[str, ...]] = {}
    for info in infos:
        if info.level != 2 or info.parent is None:
            continue
        key = info.parent.upper()
        grouped[key] = grouped.get(key, ()) + (info.name.upper(),)
    return grouped


def _is_maxvalue(info: OraclePartitionInfo) -> bool:
    """Whether a catalog row's ``HIGH_VALUE`` is Oracle's ``MAXVALUE``."""
    return info.level == 1 and (info.bound or "").strip().upper() == _MAXVALUE
