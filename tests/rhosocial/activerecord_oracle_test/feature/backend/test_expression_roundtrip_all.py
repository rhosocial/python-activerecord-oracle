# tests/rhosocial/activerecord_oracle_test/feature/backend/test_expression_roundtrip_all.py
"""
Functional serialization coverage: every expression class Oracle can reach must
round-trip losslessly through all three encodings (dict / JSON string / XML),
and every one that renders must render the same way afterwards.

For each expression class that can be constructed:
  1. dict round-trip   : deserialize(serialize(e)).get_params() == e.get_params()
  2. JSON round-trip   : deserialize_json(serialize_json(e)).get_params() == ...
  3. XML round-trip    : deserialize_xml(serialize_xml(e)).get_params() == ...
  4. SQL consistency   : classified, never swallowed -- see below.

Which classes are covered
=========================

Both packages, not one:

* ``rhosocial.activerecord.backend.impl.oracle.expression`` -- Oracle's own
  statements, which every other backend's matrix also covers.
* ``rhosocial.activerecord.backend.expression`` -- the core package. The core
  classes are what every statement here is *built from*: a formatter reading a
  field core renamed is invisible to a matrix that only walks the backend. A
  formatter defect of exactly that shape reached CI once already (postgres read
  a column field core had removed months earlier), which is why the backend's
  own expressions being green was not evidence of anything.

Discovery walks the package, not ``ExpressionRegistry``
======================================================

``ExpressionRegistry`` is process-global and *grows* as sibling test modules
import their own backends, so its contents depend on which files pytest happened
to import first. A matrix built from it passes alone and fails beside the
statement tests, having picked up another backend's classes to render with
Oracle -- not a meaningful thing to assert. ``collect_expression_classes`` is a
deterministic package walk, and this file uses only that.

Why ``to_sql()`` is classified rather than caught
=================================================

This matrix used to call the testsuite's ``sql_consistent``, which wraps the
first render in ``try/except Exception: return``. That made every render
failure a green tick: the three SQL comparisons never ran, so a formatter reading
a field that no longer existed was indistinguishable from a formatter for a
feature Oracle does not support.

Each outcome is now named and asserted:

* **renders** -- all three encodings must restore byte-identical SQL *and*
  byte-identical bind parameters.
* ``UnsupportedFeatureError`` -- Oracle does not model the feature. Asserted as
  exactly that type, so a different error cannot hide behind it.
* ``NotImplementedError`` -- an expression-category base that deliberately names
  no formatter. Asserted as exactly that type, for the same reason.
* a member of :data:`LEGITIMATE_NON_RENDERS` -- a class that constructs but
  cannot render for a reason belonging to its own tree, which the generic
  constructor cannot fix because it never built a valid instance. Each entry
  pins the exception type *and* a message fragment, so a class that starts
  failing for a different reason fails here instead of staying quietly green.
* **anything else** -- a failure naming the class and the exception.

And what happens when a class cannot be constructed
===================================================

``make_instance(...) is None`` becomes a skip, but only for a class named in
:data:`UNCONSTRUCTIBLE`, and :func:`TestMatrixIntegrity.test_unconstructible_list_is_exact`
pins that tuple in both directions. A class that starts needing an exemption
fails CI instead of turning into a skip, and a stale entry fails too.

The namespace shape
===================

:class:`TestOracleNamespaceShape` is the reason this file exists in this form.
Oracle is the only dialect in the series whose namespace is the *reverse* of the
core default: a schema and no catalog, where core assumed a catalog outside a
schema. Before ``validate_namespace`` / ``format_qualified_name`` split, core's
two-slot renderer happened to fit Oracle because the empty outer slot was simply
not emitted -- the shape was never stated. It is stated now, and the tests below
pin it in the only way that cannot pass for the wrong reason: the value under
test does not appear anywhere else in the output, and removing it must change
the SQL.
"""

import inspect
from typing import Dict

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.core import Column, Literal
from rhosocial.activerecord.backend.expression.operators import RawSQLExpression
from rhosocial.activerecord.backend.expression.objects import (
    Function,
    Index,
    MaterializedView,
    Procedure,
    Schema,
    Sequence,
    Synonym,
    Table,
    Trigger,
    Type,
    View,
)
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression.serialization import (
    ExpressionRegistry,
    deserialize,
    deserialize_json,
    deserialize_xml,
    serialize,
    serialize_json,
    serialize_xml,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    TableConstraint,
    TableConstraintType,
)
from rhosocial.activerecord.backend.expression.statements.dql import QueryExpression
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
from rhosocial.activerecord.backend.expression.types import IntegerType
from rhosocial.activerecord.testsuite.utils.expression import (
    assert_params_equal,
    collect_expression_classes,
    make_instance,
    register_all,
    register_special_constructor,
)

ORACLE_EXPR_PKG = "rhosocial.activerecord.backend.impl.oracle.expression"
CORE_EXPR_PKG = "rhosocial.activerecord.backend.expression"


def assert_params_equal_local(a, b, path="params"):
    """Deep-compare two ``get_params()`` outputs, treating nested expressions as
    equal when their own params match.

    Thin re-export of the testsuite's helper so this module's intent is legible
    at the point of use; the comparison rules live with the other backends.
    """
    assert_params_equal(a, b, path)


# ---------------------------------------------------------------------------
# Discovery: walk both packages, never the process-global registry
# ---------------------------------------------------------------------------

def _collect_matrix_classes():
    """Every concrete expression class either package defines.

    Walked rather than read from ``ExpressionRegistry``, whose contents depend
    on what other test modules happened to import (see the module docstring).

    Several names are aliases of the same class -- ``ddl_alter`` exports
    ``AlterConstraint`` and ``AlterConstraintAction`` for one object, and the
    Oracle TYPE tree exports a long ``OracleXxx`` / ``OracleTypeXxx`` alias list
    for another. They are the same objects, so one entry per class.

    The name kept is the class's *own* ``__name__``, not whichever alias sorted
    first, and that is load-bearing rather than cosmetic:
    :meth:`ExpressionRegistry.register` keys on
    ``f"{cls.__module__}.{cls.__name__}"``, so a matrix entry named after an
    alias is a name the registry will never resolve -- the round-trip would look
    for ``...OracleAddTypeAttributeAction`` and find nothing, while
    ``register_all`` had filed the class under ``...OracleAlterTypeAddAttributeAction``.
    Keying on the canonical name makes the matrix's keys and the registry's the
    same strings by construction.
    """
    ExpressionRegistry._auto_register_builtins()
    collected: Dict[str, type] = {}
    for package in (ORACLE_EXPR_PKG, CORE_EXPR_PKG):
        collected.update(collect_expression_classes(package))
    by_identity: Dict[int, type] = {}
    for cls in collected.values():
        if not inspect.isabstract(cls):
            by_identity.setdefault(id(cls), cls)
    return {
        f"{cls.__module__}.{cls.__name__}": cls
        for cls in sorted(
            by_identity.values(), key=lambda c: (c.__module__, c.__name__)
        )
    }


REGISTERED = _collect_matrix_classes()
register_all(REGISTERED)


# ---------------------------------------------------------------------------
# Shared builders
# ---------------------------------------------------------------------------

def _table_obj(dialect, name="t"):
    """A table with a bare name and no namespace."""
    return Table(dialect, name)


def _column_predicate(dialect):
    """A predicate comparing two columns, so it renders with no bind parameters.

    DDL cannot carry bind parameters, so the predicates that appear inside
    CHECK constraints and DOMAIN CHECKs are built from columns rather than from
    a column and a literal.
    """
    return ComparisonPredicate(dialect, "=", Column(dialect, "a"), Column(dialect, "b"))


def _one_column_query(dialect):
    """A single-column ``SELECT`` over a table."""
    return QueryExpression(
        dialect, select=[Column(dialect, "id")], from_=_table_obj(dialect)
    )


# ---------------------------------------------------------------------------
# Catalogue-object builders, one per object the statements below name
# ---------------------------------------------------------------------------
#
# Since the statements were given the objects they name, the introspective
# constructor's ``"x"`` is the wrong value for every parameter annotated with one
# of these -- ``table``, ``index``, ``view``, ``sequence``, ``schema``,
# ``function``, ``type``, ``graph`` -- and the formatter now refuses a bare string
# by name. Each builder below supplies the object instead.
#
# They are deliberately bare-named and namespace-free, so the rendered SQL names
# exactly the object the builder passed in and nothing else. A qualified name
# would make it impossible to tell, from the SQL alone, whether the object
# survived the round-trip or a constant was printed; the shape tests in
# :class:`TestOracleNamespaceShape` cover the qualified case deliberately.


def _index_obj(dialect, name="i"):
    return Index(dialect, name)


def _view_obj(dialect, name="v"):
    return View(dialect, name)


def _materialized_view_obj(dialect, name="mv"):
    return MaterializedView(dialect, name)


def _sequence_obj(dialect, name="s"):
    return Sequence(dialect, name)


def _schema_obj(dialect, name="sch"):
    return Schema(dialect, name)


def _function_obj(dialect, name="f"):
    return Function(dialect, name)


def _type_obj(dialect, name="ty"):
    return Type(dialect, name)


def _integer_column(dialect, name="col"):
    """A column definition carrying a *dialect-bound* type.

    The binding is load-bearing: ``to_sql()`` dispatches the type through its own
    dialect, so an unbound ``IntegerType()`` raises the moment anything renders
    it.
    """
    from rhosocial.activerecord.backend.expression.statements import ddl_table

    return ddl_table.ColumnDefinition(dialect, name, IntegerType(dialect))


# ---------------------------------------------------------------------------
# Special constructors: a real value where the introspective guess is a lie
# ---------------------------------------------------------------------------
#
# ``make_instance`` reads each required parameter's annotation and guesses:
# ``"x"`` for a string, ``[]`` for a list, ``IntegerType()`` for a type. That is
# right for a name and wrong for every parameter that wants a catalogue object --
# a Table, an Index, a Synonym, an OraclePackage -- because a bare ``"x"`` is not
# one, and the formatter now refuses it by name. It is also wrong for the
# containers that require at least one member, for the Oracle TYPE actions whose
# payload is a declaration rather than a plain value, and for the predicates
# that must render without bind parameters.
#
# Every registration below replaces a guess that would otherwise have produced an
# instance the dialect cannot render. Suffixes are spelled relative to the
# expression package because ``make_instance`` matches with ``str.endswith`` and
# several modules export identically named classes.


def _register_core_specials():
    """Core classes whose introspective guess cannot build a renderable object."""
    from rhosocial.activerecord.backend.expression import xml as xml_mod
    from rhosocial.activerecord.backend.expression.query_parts import JoinClause
    from rhosocial.activerecord.backend.expression.statements import ddl_alter
    from rhosocial.activerecord.backend.expression.statements.ddl_domain import (
        AddDomainCheckAction,
        DomainCheckConstraint,
    )
    from rhosocial.activerecord.backend.expression.types.enum_ import EnumType

    # ALTER CONSTRAINT. `constraint_type` is keyword-only behind defaulted
    # positionals, so the introspective constructor skips it and the class
    # refuses an incomplete action; the enforcement keyword is mandatory, so
    # the factory picks a spelling.
    def alter_constraint(dialect):
        return ddl_alter.AlterConstraint(
            dialect,
            "c1",
            constraint_type=TableConstraintType.FOREIGN_KEY,
            enforced=True,
        )

    register_special_constructor(
        "statements.ddl_alter.AlterConstraint", alter_constraint
    )
    register_special_constructor(
        "statements.ddl_alter.AlterConstraintAction", alter_constraint
    )
    register_special_constructor(
        "statements.ddl_alter.AlterTableConstraint", alter_constraint
    )
    register_special_constructor(
        "statements.ddl_alter.ValidateConstraint",
        lambda d: ddl_alter.ValidateConstraint(d, "c1"),
    )
    register_special_constructor(
        "statements.ddl_alter.ValidateConstraintAction",
        lambda d: ddl_alter.ValidateConstraint(d, "c1"),
    )
    register_special_constructor(
        "statements.ddl_alter.ValidateTableConstraint",
        lambda d: ddl_alter.ValidateConstraint(d, "c1"),
    )

    # ADD DOMAIN CHECK. Widens a SQLPredicate into a DomainCheckConstraint and
    # needs one; the guess supplies a bare comparison whose literal would have to
    # render as a bind parameter, which DDL cannot carry.
    register_special_constructor(
        "statements.ddl_domain.AddDomainCheckAction",
        lambda d: AddDomainCheckAction(
            d, DomainCheckConstraint(d, _column_predicate(d), name="chk")
        ),
    )

    # ADD TABLE CONSTRAINT. The guess supplies a bare string where a
    # TableConstraint belongs, and the formatter reads its fields.
    register_special_constructor(
        "statements.ddl_alter.AddTableConstraint",
        lambda d: ddl_alter.AddTableConstraint(
            d,
            TableConstraint(
                d, TableConstraintType.PRIMARY_KEY, name="c", columns=["a"]
            ),
        ),
    )

    # ENUM type. `values` is keyword-only behind a defaulted positional, so the
    # introspective constructor skips it and the type declares no members.
    register_special_constructor(
        "types.enum_.EnumType", lambda d: EnumType(d, ["first", "second"])
    )

    # Window pieces. A window definition is a name plus a *specification*, and
    # the guess supplies a string where the specification belongs; a clause is a
    # list of definitions and the guess supplies an empty one, which the formatter
    # refuses. CASE likewise needs at least one WHEN/THEN pair.
    from rhosocial.activerecord.backend.expression.advanced_functions import (
        CaseExpression,
        WindowClause,
        WindowDefinition,
        WindowSpecification,
    )

    def window_specification(dialect):
        return WindowSpecification(dialect, partition_by=[Column(dialect, "a")])

    register_special_constructor(
        "advanced_functions.WindowSpecification", window_specification
    )
    register_special_constructor(
        "advanced_functions.WindowDefinition",
        lambda dialect: WindowDefinition(
            dialect, "w", window_specification(dialect)
        ),
    )
    register_special_constructor(
        "advanced_functions.WindowClause",
        lambda dialect: WindowClause(
            dialect,
            [WindowDefinition(dialect, "w", window_specification(dialect))],
        ),
    )
    register_special_constructor(
        "advanced_functions.CaseExpression",
        lambda dialect: CaseExpression(
            dialect,
            cases=[(_column_predicate(dialect), Literal(dialect, 1))],
            else_result=Literal(dialect, 0),
        ),
    )

    # ALTER TABLE column actions. Each wants a ColumnDefinition carrying a
    # dialect-bound type, and the guess supplies a bare string where the formatter
    # reads `.name` and `.data_type`.
    from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
        AddColumn,
        ModifyColumn,
    )

    register_special_constructor(
        "statements.ddl_alter.AddColumn",
        lambda dialect: AddColumn(dialect, column=_integer_column(dialect)),
    )
    register_special_constructor(
        "statements.ddl_alter.ModifyColumn",
        lambda dialect: ModifyColumn(dialect, column=_integer_column(dialect)),
    )

    # CREATE TRIGGER. `timing` and `events` are enums and the formatter reads
    # `.value` off each; the guess supplies strings, so it fails on the first
    # attribute access rather than on a type check.
    from rhosocial.activerecord.backend.expression.statements.ddl_trigger import (
        CreateTriggerExpression,
        TriggerEvent,
        TriggerTiming,
    )

    register_special_constructor(
        "statements.ddl_trigger.CreateTriggerExpression",
        lambda dialect: CreateTriggerExpression(
            dialect,
            trigger=Trigger(dialect, "trg"),
            table=_table_obj(dialect),
            timing=TriggerTiming.BEFORE,
            events=[TriggerEvent.INSERT],
            function=Function(dialect, "fn"),
        ),
    )

    # MERGE. `action_type` is an enum and a WHEN MATCHED branch must name its
    # alias; the guess supplies a string for the enum and None for the alias, so
    # the rendered clause is not the one a MERGE statement requires.
    from rhosocial.activerecord.backend.expression.statements.dml import (
        MergeAction,
        MergeActionType,
    )

    register_special_constructor(
        "statements.dml.MergeAction",
        lambda dialect: MergeAction(
            dialect,
            MergeActionType.UPDATE,
            {"a": Literal(dialect, 1)},
            _column_predicate(dialect),
            "matched",
        ),
    )

    # Column constraint. `constraint_type` is an enum and the formatter reads it
    # as one; the guess supplies a string, which the class itself refuses.
    from rhosocial.activerecord.backend.expression.statements.ddl_table import (
        ColumnConstraint,
        ColumnConstraintType,
    )

    register_special_constructor(
        "statements.ddl_table.ColumnConstraint",
        lambda dialect: ColumnConstraint(
            dialect, ColumnConstraintType.NOT_NULL, name="c"
        ),
    )

    # REFERENCES. A referenced column list is mandatory -- REFERENCES naming no
    # columns names nothing -- and the guess supplies an empty one.
    from rhosocial.activerecord.backend.expression.statements.ddl_table import (
        ReferencesClause,
    )

    register_special_constructor(
        "statements.ddl_table.ReferencesClause",
        lambda dialect: ReferencesClause(dialect, Table(dialect, "other"), ["b"]),
    )

    # COLLATE. The collation name is validated against a whitelist, and the
    # guess's "x" is not a collation any engine has.
    from rhosocial.activerecord.backend.expression.collation import CollateExpression
    from rhosocial.activerecord.backend.impl.oracle.collation import OracleCollation

    register_special_constructor(
        "collation.CollateExpression",
        lambda dialect: CollateExpression(
            dialect, Column(dialect, "name"), OracleCollation.BINARY_CI
        ),
    )

    # Time travel. An empty options dict is refused by the formatter, so an
    # AS OF clause needs an actual option.
    from rhosocial.activerecord.backend.expression.datetime import (
        TemporalOptionsExpression,
    )

    register_special_constructor(
        "datetime.TemporalOptionsExpression",
        lambda dialect: TemporalOptionsExpression(
            dialect, {"as_of": "2020-01-01"}
        ),
    )

    # SQL graph patterns. A path is a sequence of vertices and edges, and the
    # guess supplies a bare string where a GraphVertex belongs.
    from rhosocial.activerecord.backend.expression import graph as graph_mod
    from rhosocial.activerecord.backend.expression.objects import (
        EdgeTable as EdgeTableObject,
        NodeTable,
    )

    def graph_vertex(dialect):
        return graph_mod.GraphVertex(dialect, "n", NodeTable(dialect, "people"))

    register_special_constructor(
        "graph.GraphVertex", graph_vertex
    )
    register_special_constructor(
        "graph.PathPattern",
        lambda dialect: graph_mod.PathPattern(dialect, graph_vertex(dialect)),
    )
    register_special_constructor(
        "graph.MatchClause",
        lambda dialect: graph_mod.MatchClause(
            dialect, graph_mod.PathPattern(dialect, graph_vertex(dialect))
        ),
    )

    # FROM references. A named relation carries a relation *object*, and the
    # formatter now refuses a bare string by name -- so the guess's "x" is
    # refused rather than rendered.
    def named_relation(dialect):
        return NamedRelationRef(dialect, _table_obj(dialect))

    register_special_constructor("sources.relation.NamedRelationRef", named_relation)

    def oracle_named_relation(dialect):
        from rhosocial.activerecord.backend.impl.oracle.expression.sources import (
            OracleNamedRelationRef,
        )

        return OracleNamedRelationRef(dialect, _table_obj(dialect))

    register_special_constructor(
        "sources.OracleNamedRelationRef", oracle_named_relation
    )

    # SQL/XML. Every one of these takes a sequence, and a sequence of nothing is
    # not a statement: XMLATTRIBUTES over no attribute, XMLCONCAT over no part,
    # XMLFOREST over no item, XMLTABLE with no columns and no row document.
    register_special_constructor(
        "xml.XMLAttributesExpression",
        lambda d: xml_mod.XMLAttributesExpression(
            d, [xml_mod.XMLAttribute(Literal(d, "v"), "a")]
        ),
    )
    register_special_constructor(
        "xml.XMLConcatExpression",
        lambda d: xml_mod.XMLConcatExpression(d, [Literal(d, "a"), Literal(d, "b")]),
    )
    register_special_constructor(
        "xml.XMLForestExpression",
        lambda d: xml_mod.XMLForestExpression(
            d, [xml_mod.XMLForestItem(Literal(d, "v"), "a")]
        ),
    )
    # Statements that name a catalogue object. Every parameter below is annotated
    # with the object type, so the introspective guess -- `"x"` for anything it
    # cannot type -- hands the constructor a bare string, and the formatter
    # refuses it by name rather than rendering a name it was not given. Each
    # factory passes the object the builders above return.
    from rhosocial.activerecord.backend.expression.statements import (
        ddl_comment,
        ddl_index,
        ddl_schema,
        ddl_sequence,
        ddl_trigger,
        ddl_truncate,
        ddl_type,
        ddl_view,
        dml,
    )
    from rhosocial.activerecord.backend.expression.statements import (
        ddl_function as ddl_fn,
    )
    from rhosocial.activerecord.backend.expression.statements import ddl_table as ddl_tbl
    register_special_constructor(
        "statements.ddl_table.DropTableExpression",
        lambda d: ddl_tbl.DropTableExpression(d, table=_table_obj(d)),
    )
    register_special_constructor(
        "statements.ddl_truncate.TruncateExpression",
        lambda d: ddl_truncate.TruncateExpression(d, table=_table_obj(d)),
    )
    register_special_constructor(
        "statements.ddl_index.CreateIndexExpression",
        lambda d: ddl_index.CreateIndexExpression(
            d, index=_index_obj(d), table=_table_obj(d), columns=[Column(d, "a")]
        ),
    )
    register_special_constructor(
        "statements.ddl_index.DropIndexExpression",
        lambda d: ddl_index.DropIndexExpression(d, index=_index_obj(d)),
    )
    register_special_constructor(
        "statements.ddl_index.CreateFulltextIndexExpression",
        lambda d: ddl_index.CreateFulltextIndexExpression(
            d, index=_index_obj(d), table=_table_obj(d), columns=[Column(d, "a")]
        ),
    )
    register_special_constructor(
        "statements.ddl_index.DropFulltextIndexExpression",
        lambda d: ddl_index.DropFulltextIndexExpression(
            d, index=_index_obj(d), table=_table_obj(d)
        ),
    )
    register_special_constructor(
        "statements.ddl_schema.CreateSchemaExpression",
        lambda d: ddl_schema.CreateSchemaExpression(d, schema=_schema_obj(d)),
    )
    register_special_constructor(
        "statements.ddl_schema.DropSchemaExpression",
        lambda d: ddl_schema.DropSchemaExpression(d, schema=_schema_obj(d)),
    )
    register_special_constructor(
        "statements.ddl_sequence.CreateSequenceExpression",
        lambda d: ddl_sequence.CreateSequenceExpression(d, sequence=_sequence_obj(d)),
    )
    register_special_constructor(
        "statements.ddl_sequence.DropSequenceExpression",
        lambda d: ddl_sequence.DropSequenceExpression(d, sequence=_sequence_obj(d)),
    )
    register_special_constructor(
        "statements.ddl_sequence.AlterSequenceExpression",
        lambda d: ddl_sequence.AlterSequenceExpression(
            d, sequence=_sequence_obj(d), start=1
        ),
    )
    register_special_constructor(
        "statements.ddl_view.CreateViewExpression",
        lambda d: ddl_view.CreateViewExpression(
            d, view=_view_obj(d), query=_one_column_query(d)
        ),
    )
    register_special_constructor(
        "statements.ddl_view.DropViewExpression",
        lambda d: ddl_view.DropViewExpression(d, view=_view_obj(d)),
    )
    register_special_constructor(
        "statements.ddl_view.CreateMaterializedViewExpression",
        lambda d: ddl_view.CreateMaterializedViewExpression(
            d, view=_materialized_view_obj(d), query=_one_column_query(d)
        ),
    )
    register_special_constructor(
        "statements.ddl_view.DropMaterializedViewExpression",
        lambda d: ddl_view.DropMaterializedViewExpression(
            d, view=_materialized_view_obj(d)
        ),
    )
    register_special_constructor(
        "statements.ddl_view.RefreshMaterializedViewExpression",
        lambda d: ddl_view.RefreshMaterializedViewExpression(
            d, view=_materialized_view_obj(d)
        ),
    )
    # A column's type is dispatched through its own dialect, so the type must be
    # bound to the one under test. The testsuite's shared factory supplies an
    # unbound IntegerType, which renders nothing; re-registering it here is what
    # makes the class render at all.
    register_special_constructor(
        "statements.ddl_table.ColumnDefinition", _integer_column
    )
    register_special_constructor(
        "statements.ddl_table.CreateTableExpression",
        lambda d: ddl_tbl.CreateTableExpression(
            d, table=_table_obj(d), columns=[_integer_column(d)]
        ),
    )
    register_special_constructor(
        "statements.ddl_table.CreateTableAsExpression",
        lambda d: ddl_tbl.CreateTableAsExpression(
            d, table=_table_obj(d), as_query=_one_column_query(d)
        ),
    )
    register_special_constructor(
        "statements.ddl_table.CreateTableLikeExpression",
        lambda d: ddl_tbl.CreateTableLikeExpression(
            d, table=_table_obj(d), like_table=Table(d, "other")
        ),
    )
    register_special_constructor(
        "statements.ddl_table.CreateTableCloneExpression",
        lambda d: ddl_tbl.CreateTableCloneExpression(
            d,
            table=_table_obj(d),
            source_table=Table(d, "other"),
            mode=ddl_tbl.CreateTableCloneMode.CLONE,
        ),
    )
    register_special_constructor(
        "statements.ddl_table.CreateTableFromTemplateExpression",
        lambda d: ddl_tbl.CreateTableFromTemplateExpression(
            d, table=_table_obj(d), template=_one_column_query(d)
        ),
    )
    register_special_constructor(
        "statements.ddl_comment.CommentOnExpression",
        lambda d: ddl_comment.CommentOnExpression(
            d, ddl_comment.CommentObjectType.TABLE, _table_obj(d), "a comment"
        ),
    )
    register_special_constructor(
        "statements.ddl_function.CreateFunctionExpression",
        lambda d: ddl_fn.CreateFunctionExpression(
            d,
            function=_function_obj(d),
            returns=IntegerType(d),
            body="BEGIN RETURN 1; END;",
        ),
    )
    # ALTER TABLE's actions name the index they drop, and the statement names the
    # table it alters; both are objects, so both need one.
    register_special_constructor(
        "statements.ddl_alter.AlterTableExpression",
        lambda d: ddl_alter.AlterTableExpression(
            d, table=_table_obj(d), actions=[ddl_alter.DropColumn(d, "a")]
        ),
    )
    register_special_constructor(
        "statements.ddl_alter.DropIndex",
        lambda d: ddl_alter.DropIndex(d, index=_index_obj(d)),
    )
    # INSERT names the table it writes; a VALUES source needs one row.
    # CREATE TYPE carries a *type definition*, not a bare value: the formatter
    # reads `.definition_kind` off it and then renders it. An object type also
    # refuses to declare no attributes.
    def create_type(dialect):
        from rhosocial.activerecord.backend.impl.oracle.expression.ddl.type import (
            OracleObjectTypeDefinition,
            OracleTypeAttribute,
        )

        return ddl_type.CreateTypeExpression(
            dialect,
            type=_type_obj(dialect),
            definition=OracleObjectTypeDefinition(
                dialect,
                attributes=[
                    OracleTypeAttribute(name="attr", data_type=IntegerType(dialect))
                ],
            ),
        )

    register_special_constructor(
        "statements.ddl_type.CreateTypeExpression", create_type
    )
    register_special_constructor(
        "statements.ddl_type.DropTypeExpression",
        lambda d: ddl_type.DropTypeExpression(d, type=_type_obj(d)),
    )
    register_special_constructor(
        "statements.ddl_trigger.DropTriggerExpression",
        lambda d: ddl_trigger.DropTriggerExpression(
            d, trigger=Trigger(d, "trg"), table=_table_obj(d)
        ),
    )
    # INSERT names the table it writes; a VALUES source needs one row.
    register_special_constructor(
        "statements.dml.InsertExpression",
        lambda d: dml.InsertExpression(
            d,
            into=_table_obj(d),
            columns=["a"],
            source=dml.ValuesSource(d, [[Literal(d, 1)]]),
        ),
    )
    register_special_constructor(
        "dml.ValuesSource",
        lambda d: dml.ValuesSource(d, [[Literal(d, 1)]]),
    )
    register_special_constructor(
        "statements.dml.UpdateExpression",
        lambda d: dml.UpdateExpression(
            d, table=_table_obj(d), assignments={"a": Literal(d, 1)}
        ),
    )
    register_special_constructor(
        "dml.DeleteExpression",
        lambda d: dml.DeleteExpression(d, tables=[_table_obj(d)]),
    )
    # A JOIN carries no condition or USING clause unless one is given, so the
    # empty join the guess builds is refused by the formatter.
    register_special_constructor(
        "query_parts.JoinClause",
        lambda d: JoinClause(
            d,
            left_table=Table(d, "a"),
            right_table=Table(d, "b"),
            condition=_column_predicate(d),
        ),
    )

    # The property-graph tree names the graph, the node table and the edge table
    # rather than carrying their names, so the guess's strings are refused here
    # too. The graph expressions render through the SQL/PGQ layer, which Oracle
    # has not adapted; the object-bearing ones below are built anyway so the
    # matrix can say so by name.
    from rhosocial.activerecord.backend.expression import graph as graph_mod

    def _node_table(dialect):
        return graph_mod.NodeTable(dialect, "n")

    def _edge_table_obj(dialect):
        return graph_mod.EdgeTableObject(dialect, "e")

    def _property_graph(dialect):
        return graph_mod.PropertyGraph(dialect, "g")

    # A vertex is bound to a *variable name*, and the formatter validates that
    # name -- so a Literal is refused where a name belongs.
    def _graph_vertex(dialect):
        return graph_mod.GraphVertex(dialect, "v", _node_table(dialect))

    # PathPattern and MatchClause are both varargs, not sequences: passing a list
    # nests one level too deep and the element check fails.
    def _path_pattern(dialect):
        return graph_mod.PathPattern(dialect, _graph_vertex(dialect))

    def _match_clause(dialect):
        return graph_mod.MatchClause(dialect, _path_pattern(dialect))

    register_special_constructor(
        "graph.GraphVertex", _graph_vertex
    )
    register_special_constructor(
        "graph.PathPattern", _path_pattern
    )
    register_special_constructor(
        "graph.MatchClause", _match_clause
    )
    register_special_constructor(
        "graph.VertexTable",
        lambda d: graph_mod.VertexTable(d, _node_table(d)),
    )
    register_special_constructor(
        "graph.EdgeTable",
        lambda d: graph_mod.EdgeTable(d, _edge_table_obj(d), ["a"], ["b"]),
    )
    register_special_constructor(
        "graph.CreatePropertyGraphExpression",
        lambda d: graph_mod.CreatePropertyGraphExpression(
            d,
            graph=_property_graph(d),
            vertex_tables=[graph_mod.VertexTable(d, _node_table(d))],
            edge_tables=[
                graph_mod.EdgeTable(d, _edge_table_obj(d), ["a"], ["b"])
            ],
        ),
    )
    register_special_constructor(
        "graph.DropPropertyGraphExpression",
        lambda d: graph_mod.DropPropertyGraphExpression(d, graph=_property_graph(d)),
    )
    register_special_constructor(
        "graph.AlterPropertyGraphExpression",
        lambda d: graph_mod.AlterPropertyGraphExpression(
            d, graph=_property_graph(d), action="DROP", target="LABEL v"
        ),
    )
    register_special_constructor(
        "graph.GraphTableExpression",
        lambda d: graph_mod.GraphTableExpression(
            d,
            graph=_property_graph(d),
            match=_match_clause(d),
            # ColumnsClause is varargs too, and a GraphColumn is a (variable,
            # property) pair rather than a single name.
            columns=graph_mod.ColumnsClause(d, graph_mod.GraphColumn("v", "p")),
        ),
    )

    # XMLTABLE. A COLUMNS list of nothing is not a projection list, and an empty one
    # would hide what the column is here for: the matrix can only observe whether
    # the encoding carries a column if there is one to carry. Both the row pattern
    # and the column path are RawSQL rather than Literal, because DDL carries no
    # bind parameters and a Literal would render as one.
    def xml_table(dialect):
        from rhosocial.activerecord.backend.expression.xml import (
            XMLTableColumn,
            XMLTableExpression,
        )

        return XMLTableExpression(
            dialect,
            RawSQLExpression(dialect, "'.'"),
            [
                XMLTableColumn(
                    "c", "VARCHAR(10)", path=RawSQLExpression(dialect, "'.'")
                )
            ],
        )

    register_special_constructor("xml.XMLTableExpression", xml_table)

    # CUSTOM. Its ``raw`` slot exists to carry the SQL type name and defaults to the
    # empty string, which the filler skips and the class refuses while constructing.
    # Handed a real name it builds and renders, so it is a constructor rather than a
    # skip -- and for that same reason it is absent from LEGITIMATE_NON_RENDERS below,
    # which would otherwise pin a rendering nothing could ask for.
    def custom_type(dialect):
        from rhosocial.activerecord.backend.expression.types.custom import (
            CustomType,
        )

        return CustomType(dialect, "VARCHAR2(10)")

    register_special_constructor("types.custom.CustomType", custom_type)

    # The column must survive all three encodings, not just dict. XMLTableColumn was
    # a plain class, so core's JSON and XML encoders dropped the whole COLUMNS list
    # and every round-trip lost it; core fixed that in ae5b7f9 and the round-trip
    # comparison below is now what holds the line, per channel.


def _register_oracle_specials():
    """Oracle's own expressions, each of which carries an object or a payload."""
    from rhosocial.activerecord.backend.impl.oracle.expression import (
        materialized_view as mv_mod,
        partition as part_mod,
        partition_lifecycle as pl_mod,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.alter_table import (
        OracleReadOnlyAction,
        OracleRowMovementAction,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.analyze import (
        OracleAnalyzeExpression,
        OracleAnalyzeMode,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.column import (
        OracleColumnDefinition,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.comment import (
        OracleCommentExpression,
        OracleCommentObjectType,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.ddl.routine import (
        OracleCreateFunctionExpression,
        OracleCreatePackageBodyExpression,
        OracleCreatePackageExpression,
        OracleCreateProcedureExpression,
        OracleDropRoutineExpression,
        OracleDropRoutineObjectType,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.ddl.synonym import (
        OracleCreateSynonymExpression,
        OracleDropSynonymExpression,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.ddl.type import (
        OracleAlterTypeAddAttributeAction,
        OracleAlterTypeAddMethodAction,
        OracleAlterTypeDropAttributeAction,
        OracleAlterTypeDropMethodAction,
        OracleAlterTypeElementTypeAction,
        OracleAlterTypeFinalAction,
        OracleAlterTypeInstantiableAction,
        OracleAlterTypeLimitAction,
        OracleCreateTypeBodyExpression,
        OracleDropTypeBodyExpression,
        OracleDropTypeExpression,
        OracleNestedTableTypeDefinition,
        OracleObjectTypeDefinition,
        OracleSqljTypeDefinition,
        OracleTypeAttribute,
        OracleTypeMethod,
        OracleVarrayTypeDefinition,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.flashback import (
        OracleAsOfClause,
        OracleAsOfMode,
        OracleFlashbackTableExpression,
        OraclePurgeExpression,
        OraclePurgeObjectType,
        OracleVersionsBetweenClause,
        OracleVersionsBetweenMode,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.materialized_view import (
        OracleCreateMaterializedViewExpression,
        OracleCreateMaterializedViewLogExpression,
        OracleDropMaterializedViewExpression,
        OracleRefreshMaterializedViewExpression,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.objects import (
        OraclePackage,
        OracleTable,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.sequence import (
        OracleCreateSequenceExpression,
        OracleDropSequenceExpression,
    )
    from rhosocial.activerecord.backend.impl.oracle.expression.trigger import (
        DisableTriggerExpression,
        EnableTriggerExpression,
    )

    def analyze_expression(d):
        return OracleAnalyzeExpression(
            d, table=_table_obj(d), mode=OracleAnalyzeMode.COMPUTE_STATISTICS
        )

    def read_only_action(d):
        # READ ONLY / READ WRITE is mandatory in the action's grammar, so the
        # introspective no-argument guess is refused; this factory picks a
        # spelling.
        return OracleReadOnlyAction(d, read_only=True)

    def row_movement_action(d):
        # ENABLE / DISABLE ROW MOVEMENT is mandatory in the action's grammar.
        return OracleRowMovementAction(d, enable=True)

    def comment_expression(d):
        return OracleCommentExpression(
            d,
            object_type=OracleCommentObjectType.TABLE,
            object=_table_obj(d),
            comment="test",
        )

    def as_of_clause(d):
        return OracleAsOfClause(
            d, mode=OracleAsOfMode.TIMESTAMP, value="SYSTIMESTAMP"
        )

    def versions_between_clause(d):
        return OracleVersionsBetweenClause(
            d,
            mode=OracleVersionsBetweenMode.TIMESTAMP,
            low_value="SYSTIMESTAMP - 1",
            high_value="SYSTIMESTAMP",
        )

    def flashback_table_expression(d):
        return OracleFlashbackTableExpression(
            d, table=_table_obj(d), to_before_drop=True, rename_to="t2"
        )

    def purge_expression(d):
        return OraclePurgeExpression(
            d, object_type=OraclePurgeObjectType.TABLE, target=_table_obj(d)
        )

    def materialized_view_log_expression(d):
        return OracleCreateMaterializedViewLogExpression(
            d, table=_table_obj(d), with_rowid=True
        )

    def create_materialized_view(d):
        return OracleCreateMaterializedViewExpression(
            d, view=MaterializedView(d, "mv"), query=_one_column_query(d)
        )

    def drop_materialized_view(d):
        return OracleDropMaterializedViewExpression(d, view=MaterializedView(d, "mv"))

    def refresh_materialized_view(d):
        return OracleRefreshMaterializedViewExpression(d, view=MaterializedView(d, "mv"))

    def drop_routine_expression(d):
        return OracleDropRoutineExpression(
            d,
            object_type=OracleDropRoutineObjectType.PROCEDURE,
            routine=Procedure(d, "p"),
        )

    def subpartition_clause(d):
        return part_mod.OracleSubpartitionClause(
            d, strategy=part_mod.OracleSubpartitionStrategy.HASH, count=4
        )

    def partition_clause(d):
        return part_mod.OraclePartitionClause(d, "RANGE", [Column(d, "id")])

    def partition_by_range(d):
        return part_mod.OraclePartitionByRange(d, [Column(d, "id")])

    def partition_by_list(d):
        return part_mod.OraclePartitionByList(d, [Column(d, "id")])

    def partition_by_hash(d):
        return part_mod.OraclePartitionByHash(d, [Column(d, "id")], partitions_count=4)

    def interval_partition_clause(d):
        # The interval is a RawSQLExpression, not a Literal: Oracle DDL carries no
        # bind parameters, and a Literal would render as one -- which the
        # formatter refuses rather than inlining.
        return part_mod.OracleIntervalPartitionClause(
            d,
            [Column(d, "created_at")],
            interval=RawSQLExpression(d, "INTERVAL '1' MONTH"),
            partitions=[
                part_mod.OraclePartitionDefinition(
                    name="p1", less_than=[part_mod.OraclePartitionValue(d, 100)]
                )
            ],
        )

    def add_partition_expression(d):
        definition = part_mod.OraclePartitionDefinition(
            name="p_new", less_than=[part_mod.OraclePartitionMaxValue(d)]
        )
        return pl_mod.OracleAddPartitionExpression(
            d, table=_table_obj(d), partition=definition
        )

    def drop_partition_expression(d):
        return pl_mod.OracleDropPartitionExpression(
            d, table=_table_obj(d), partition_name="p1"
        )

    def split_partition_expression(d):
        # SPLIT PARTITION splits one partition into exactly two, so the class
        # refuses a definition carrying any other number -- which is why the
        # introspective guess (no new partitions at all) cannot build it.
        return pl_mod.OracleSplitPartitionExpression(
            d,
            table=_table_obj(d),
            partition_name="p_old",
            at_values=[part_mod.OraclePartitionValue(d, 100)],
            new_partitions=[
                part_mod.OraclePartitionDefinition(
                    name="p1", less_than=[part_mod.OraclePartitionValue(d, 100)]
                ),
                part_mod.OraclePartitionDefinition(
                    name="p2", less_than=[part_mod.OraclePartitionMaxValue(d)]
                ),
            ],
        )

    def merge_partitions_expression(d):
        return pl_mod.OracleMergePartitionsExpression(
            d,
            table=_table_obj(d),
            partition_names=["p1", "p2"],
            into_partition=part_mod.OraclePartitionDefinition(
                name="p_merged", less_than=[part_mod.OraclePartitionMaxValue(d)]
            ),
        )

    def exchange_partition_expression(d):
        return pl_mod.OracleExchangePartitionExpression(
            d,
            table=_table_obj(d),
            partition_name="p1",
            with_table=Table(d, "stage"),
        )

    def move_partition_expression(d):
        return pl_mod.OracleMovePartitionExpression(
            d, table=_table_obj(d), partition_name="p1"
        )

    def truncate_partition_expression(d):
        return pl_mod.OracleTruncatePartitionExpression(
            d, table=_table_obj(d), partition_name="p1"
        )

    def partition_maintenance_base(d):
        return pl_mod._OraclePartitionMaintenanceExpression(d, table=_table_obj(d))

    def oracle_table(d):
        return OracleTable(d, "orders", schema_name="ar_crm")

    def oracle_column_definition(d):
        return OracleColumnDefinition(d, "c", IntegerType(d))

    def create_procedure(d):
        return OracleCreateProcedureExpression(
            d, procedure=Procedure(d, "p"), body="BEGIN NULL; END;"
        )

    def create_function(d):
        return OracleCreateFunctionExpression(
            d,
            function=Function(d, "f"),
            return_type="NUMBER",
            body="BEGIN RETURN 1; END;",
        )

    def _package(d):
        return OraclePackage(d, "pkg", schema_name="ar_crm")

    def create_package(d):
        return OracleCreatePackageExpression(
            d, package=_package(d), body="END pkg;"
        )

    def create_package_body(d):
        return OracleCreatePackageBodyExpression(
            d, package=_package(d), body="END pkg;"
        )

    def create_synonym(d):
        return OracleCreateSynonymExpression(
            d, synonym=Synonym(d, "s"), table=_table_obj(d)
        )

    def drop_synonym(d):
        return OracleDropSynonymExpression(d, synonym=Synonym(d, "s"))

    def create_sequence(d):
        return OracleCreateSequenceExpression(d, sequence=Sequence(d, "seq"))

    def drop_sequence(d):
        return OracleDropSequenceExpression(d, sequence=Sequence(d, "seq"))

    def disable_trigger(d):
        return DisableTriggerExpression(d, trigger=Trigger(d, "trg"))

    def enable_trigger(d):
        return EnableTriggerExpression(d, trigger=Trigger(d, "trg"))

    def drop_type_body(d):
        return OracleDropTypeBodyExpression(d, type=_type_obj(d))

    def drop_type(d):
        return OracleDropTypeExpression(d, type=_type_obj(d), if_exists=True)

    def create_type_body(d):
        return OracleCreateTypeBodyExpression(d, type=Type(d, "t"), body="END;")

    def _attribute(d):
        return OracleTypeAttribute(name="attr", data_type=IntegerType(d))

    def _method(d):
        return OracleTypeMethod(
            declaration="MEMBER PROCEDURE do_it", kind=None
        )

    def add_attribute(d):
        return OracleAlterTypeAddAttributeAction(d, attribute=_attribute(d))

    def modify_attribute(d):
        return OracleAlterTypeAddAttributeAction(d, attribute=_attribute(d))

    def drop_attribute(d):
        return OracleAlterTypeDropAttributeAction(d, attribute_name="attr")

    def add_method(d):
        return OracleAlterTypeAddMethodAction(d, method=_method(d))

    def drop_method(d):
        return OracleAlterTypeDropMethodAction(d, method=_method(d))

    def final_action(d):
        # FINAL/NOT FINAL is mandatory in the action's grammar, so the
        # introspective no-argument guess is refused; this factory picks a
        # spelling.
        return OracleAlterTypeFinalAction(d, final=True)

    def instantiable_action(d):
        return OracleAlterTypeInstantiableAction(d, instantiable=True)

    def element_type(d):
        return OracleAlterTypeElementTypeAction(d, element_type="NUMBER")

    def limit_action(d):
        return OracleAlterTypeLimitAction(d, limit=10)

    def object_definition(d):
        return OracleObjectTypeDefinition(d, attributes=[_attribute(d)])

    def sqlj_definition(d):
        # A SQLJ type is mapped to a Java class through a USING clause, and both
        # it and the EXTERNAL NAME are mandatory. The introspective guess supplies
        # neither, which is why this class cannot be built without a factory.
        return OracleSqljTypeDefinition(
            d,
            attributes=[_attribute(d)],
            external_name="ledger.Entry",
            language="JAVA",
            using_clause="SQLData",
        )

    def varray_definition(d):
        return OracleVarrayTypeDefinition(
            d, element_type="NUMBER", size_limit=10
        )

    def nested_table_definition(d):
        return OracleNestedTableTypeDefinition(d, element_type="NUMBER")

    registrations = (
        ("alter_table.OracleReadOnlyAction", read_only_action),
        ("alter_table.OracleRowMovementAction", row_movement_action),
        ("analyze.OracleAnalyzeExpression", analyze_expression),
        ("column.OracleColumnDefinition", oracle_column_definition),
        ("comment.OracleCommentExpression", comment_expression),
        ("ddl.routine.OracleCreateProcedureExpression", create_procedure),
        ("ddl.routine.OracleCreateFunctionExpression", create_function),
        ("ddl.routine.OracleCreatePackageExpression", create_package),
        ("ddl.routine.OracleCreatePackageBodyExpression", create_package_body),
        ("ddl.routine.OracleDropRoutineExpression", drop_routine_expression),
        ("ddl.synonym.OracleCreateSynonymExpression", create_synonym),
        ("ddl.synonym.OracleDropSynonymExpression", drop_synonym),
        ("flashback.OracleAsOfClause", as_of_clause),
        ("flashback.OracleVersionsBetweenClause", versions_between_clause),
        ("flashback.OracleFlashbackTableExpression", flashback_table_expression),
        ("flashback.OraclePurgeExpression", purge_expression),
        ("materialized_view.OracleCreateMaterializedViewExpression", create_materialized_view),
        ("materialized_view.OracleCreateMaterializedViewLogExpression", materialized_view_log_expression),
        ("materialized_view.OracleDropMaterializedViewExpression", drop_materialized_view),
        ("materialized_view.OracleRefreshMaterializedViewExpression", refresh_materialized_view),
        ("objects.OracleTable", oracle_table),
        ("partition.OracleSubpartitionClause", subpartition_clause),
        ("partition.OraclePartitionClause", partition_clause),
        ("partition.OraclePartitionByRange", partition_by_range),
        ("partition.OraclePartitionByList", partition_by_list),
        ("partition.OraclePartitionByHash", partition_by_hash),
        ("partition.OracleIntervalPartitionClause", interval_partition_clause),
        ("partition_lifecycle.OracleAddPartitionExpression", add_partition_expression),
        ("partition_lifecycle.OracleDropPartitionExpression", drop_partition_expression),
        ("partition_lifecycle.OracleSplitPartitionExpression", split_partition_expression),
        ("partition_lifecycle.OracleMergePartitionsExpression", merge_partitions_expression),
        ("partition_lifecycle.OracleExchangePartitionExpression", exchange_partition_expression),
        ("partition_lifecycle.OracleMovePartitionExpression", move_partition_expression),
        ("partition_lifecycle.OracleTruncatePartitionExpression", truncate_partition_expression),
        ("sequence.OracleCreateSequenceExpression", create_sequence),
        ("sequence.OracleDropSequenceExpression", drop_sequence),
        ("trigger.DisableTriggerExpression", disable_trigger),
        ("trigger.EnableTriggerExpression", enable_trigger),
        # The Oracle TYPE tree exports every action and definition under two
        # names (OracleAlterTypeXxxAction and OracleTypeXxxAction). Both names
        # are collected, so both are registered to the same factory.
        ("type.OracleAlterTypeAddAttributeAction", add_attribute),
        ("type.OracleTypeAddAttributeAction", add_attribute),
        ("type.OracleAddTypeAttributeAction", add_attribute),
        # MODIFY shares ADD's payload -- the same attribute declaration, a
        # different keyword -- so it is built the same way.
        ("type.OracleAlterTypeModifyAttributeAction", modify_attribute),
        ("type.OracleTypeModifyAttributeAction", modify_attribute),
        ("type.OracleModifyTypeAttributeAction", modify_attribute),
        ("type.OracleAlterTypeDropAttributeAction", drop_attribute),
        ("type.OracleTypeDropAttributeAction", drop_attribute),
        ("type.OracleDropTypeAttributeAction", drop_attribute),
        ("type.OracleAlterTypeAddMethodAction", add_method),
        ("type.OracleTypeAddMethodAction", add_method),
        ("type.OracleAddTypeMethodAction", add_method),
        ("type.OracleAlterTypeDropMethodAction", drop_method),
        ("type.OracleTypeDropMethodAction", drop_method),
        ("type.OracleDropTypeMethodAction", drop_method),
        ("type.OracleAlterTypeFinalAction", final_action),
        ("type.OracleTypeFinalAction", final_action),
        ("type.OracleSetTypeFinalAction", final_action),
        ("type.OracleAlterTypeInstantiableAction", instantiable_action),
        ("type.OracleTypeInstantiableAction", instantiable_action),
        ("type.OracleSetTypeInstantiableAction", instantiable_action),
        ("type.OracleAlterTypeElementTypeAction", element_type),
        ("type.OracleTypeElementTypeAction", element_type),
        ("type.OracleAlterTypeLimitAction", limit_action),
        ("type.OracleTypeLimitAction", limit_action),
        ("type.OracleModifyTypeLimitAction", limit_action),
        ("type.OracleAlterTypeModifyLimitAction", limit_action),
        ("type.OracleObjectTypeDefinition", object_definition),
        ("type.OracleSqljTypeDefinition", sqlj_definition),
        ("type.OracleVarrayTypeDefinition", varray_definition),
        ("type.OracleNestedTableTypeDefinition", nested_table_definition),
        ("type.DropTypeBodyExpression", drop_type_body),
        ("type.OracleDropTypeBodyExpression", drop_type_body),
        ("type.OracleTypeBodyDropExpression", drop_type_body),
        # Oracle's own DROP TYPE adds FORCE / VALIDATE, and the guess hands it
        # strings for the type and the flags, which its superclass refuses.
        ("type.OracleDropTypeExpression", drop_type),
        ("type.OracleCreateTypeBodyExpression", create_type_body),
        ("type.OracleTypeBodyCreateExpression", create_type_body),
        # The abstract partition-maintenance base declares no formatter, so it is
        # built here only so the matrix can assert it says so by name.
        ("partition_lifecycle._OraclePartitionMaintenanceExpression", partition_maintenance_base),
    )
    for suffix, factory in registrations:
        register_special_constructor(suffix, factory)


_register_core_specials()
_register_oracle_specials()


# ---------------------------------------------------------------------------
# Lists that cannot grow or shrink silently
# ---------------------------------------------------------------------------

#: Classes the generic introspective constructor cannot build.
#:
#: Measured, not assumed: each entry below was built by hand first, with arguments
#: the class actually accepts, and refused while constructing.
#:
#: The three UUID nodes all refuse in ``__init__`` rather than in ``to_sql()`` --
#: Oracle spells no UUID SQL, so the answer is the same for every argument and no
#: registered constructor can change it. The nil/max constant only shows that once
#: it is given a *valid* ``which``: an invalid one raises ValueError first, which is
#: a different answer entirely and hides the dialect's.
#:
#: The tuple stays because a class that starts failing to build must be pinned here
#: with the fault located, not left to become a silent skip;
#: :func:`TestMatrixIntegrity.test_unconstructible_list_is_exact` fails in both
#: directions when the sets disagree.
UNCONSTRUCTIBLE: tuple = (
    "rhosocial.activerecord.backend.expression.uuid.UUIDCastExpression",
    "rhosocial.activerecord.backend.expression.uuid.UUIDConstantExpression",
    "rhosocial.activerecord.backend.expression.uuid.UUIDGenerationExpression",
)

#: Classes that construct but cannot render.
#:
#: Each entry pins the exception type *and* a message fragment, so a class that
#: starts failing for a different reason fails here instead of staying quietly
#: green -- and :func:`TestMatrixIntegrity.test_pinned_non_render_really_does_not_render`
#: re-checks every one still raises what it claims.
#:
#: The entries are grouped by *cause*, because the cause is what a reader needs in
#: order to judge the entry: groups one, four and five are contracts this dialect
#: has deliberately, groups two and three are gaps it has. Neither kind is hidden
#: -- both are named here, with the message that identifies them.


# --- group one: the dialect declares the feature absent, and composes no
# --- formatter for it. The UnsupportedFeatureError is the declared absence
# --- arriving, not a defect: `test_oracle_protocol_conformance.py` lists every
# --- one of these protocols in ORACLE_NOT_IMPLEMENTED, and asserts the dialect
# --- still does not satisfy it. A formatter here would *break* that contract
# --- test.
#
# Every entry below pins `does not support the '<formatter>' statement` rather
# than the bare formatter name, because that is now the whole of the message that
# identifies the gap. `to_sql()` used to report a missing formatter as an
# AttributeError naming `has no formatting method '<name>'`, which read like a
# broken object rather than a capability this dialect declined; core made the
# dispatch report the gap the way every other probe in the tree does, so the pin
# follows the message.
#
# Oracle has no CREATE/DROP/ALTER DATABASE (an Oracle database is an instance),
# no DOMAIN, no FULLTEXT, and no ILIKE -- case-insensitive matching goes through
# UPPER() + LIKE. PIVOT and UNPIVOT go through this backend's own PivotExpression
# and its format_pivot, so the core PivotMixin is not composed.
LEGITIMATE_NON_RENDERS: Dict[str, tuple] = {
    "rhosocial.activerecord.backend.expression.pivot.PivotExpression": (
        UnsupportedFeatureError, "does not support the 'format_pivot_expression' statement"
    ),
    "rhosocial.activerecord.backend.expression.pivot.UnpivotExpression": (
        UnsupportedFeatureError, "does not support the 'format_unpivot_expression' statement"
    ),
    "rhosocial.activerecord.backend.expression.predicates.ILIKEExpression": (
        UnsupportedFeatureError, "does not support the 'format_ilike_expression' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_database."
    "AlterDatabaseExpression": (
        UnsupportedFeatureError, "does not support the 'format_alter_database_statement' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_database."
    "CreateDatabaseExpression": (
        UnsupportedFeatureError, "does not support the 'format_create_database_statement' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_database."
    "DropDatabaseExpression": (
        UnsupportedFeatureError, "does not support the 'format_drop_database_statement' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "AddDomainCheckAction": (
        UnsupportedFeatureError, "does not support the 'format_domain_alter_action' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "AlterDomainExpression": (
        UnsupportedFeatureError, "does not support the 'format_alter_domain_statement' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "CreateDomainExpression": (
        UnsupportedFeatureError, "does not support the 'format_create_domain_statement' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "DomainCheckConstraint": (
        UnsupportedFeatureError, "does not support the 'format_domain_check_constraint' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "DomainValueExpression": (
        UnsupportedFeatureError, "does not support the 'format_domain_value_expression' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "DropDomainCheckAction": (
        UnsupportedFeatureError, "does not support the 'format_domain_alter_action' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "DropDomainDefaultAction": (
        UnsupportedFeatureError, "does not support the 'format_domain_alter_action' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "DropDomainExpression": (
        UnsupportedFeatureError, "does not support the 'format_drop_domain_statement' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "DropDomainNotNullAction": (
        UnsupportedFeatureError, "does not support the 'format_domain_alter_action' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "RenameDomainAction": (
        UnsupportedFeatureError, "does not support the 'format_domain_alter_action' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "SetDomainDefaultAction": (
        UnsupportedFeatureError, "does not support the 'format_domain_alter_action' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_domain."
    "SetDomainNotNullAction": (
        UnsupportedFeatureError, "does not support the 'format_domain_alter_action' statement"
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_function."
    "DropFunctionExpression": (
        UnsupportedFeatureError, "does not support the 'format_drop_function_statement' statement"
    ),
    # The root of the row-source tree. No concrete source renders through
    # `format_table_source`; each overrides `format_method` instead. The root
    # declares none, so there is nothing for the dialect to dispatch to.
    "rhosocial.activerecord.backend.expression.sources.base.TableSource": (
        UnsupportedFeatureError, "does not support the 'format_table_source' statement"
    ),
    # --- group two: a dispatch collision. This backend defines a formatter under
    # --- a core formatter's name with a different signature, so a core expression
    # --- that dispatches there reaches a method built for this backend's own
    # --- expression. Named here because the AttributeError is misleading -- it
    # --- reads as "a field is missing" rather than "this backend replaced the
    # --- formatter this expression names". These keep AttributeError, unlike
    # --- group one: the lookup found a formatter, and the failure is inside its
    # --- body reading a field the colliding expression does not have. The change
    # --- in `to_sql()` inspects what answers a lookup, not what a formatter does
    # --- once called.
    "rhosocial.activerecord.backend.expression.statements.ddl_function."
    "CreateFunctionExpression": (
        AttributeError, "has no attribute 'return_keyword'"
    ),
    "rhosocial.activerecord.backend.expression.query_sources."
    "JSONTableExpression": (
        AttributeError, "has no attribute 'args'"
    ),
    "rhosocial.activerecord.backend.expression.sources.json."
    "JsonTableSource": (
        TypeError, "missing 2 required positional arguments"
    ),
    "rhosocial.activerecord.backend.expression.statements.dml."
    "MergeExpression": (
        TypeError, "missing 5 required positional arguments"
    ),
    # --- group three: the SHOW query layer is defined but not composed.
    # --- OracleShowDialectMixin holds every compose_query_*_sql method and the
    # --- expressions below are the only callers, but neither the mixin nor
    # --- OracleShowMixin reaches OracleDialect or OracleBackend, so `show()` is
    # --- unreachable and these render nothing. Pinned so the eight are visible;
    # --- composing the mixin is a change to the feature's wiring, not to this
    # --- matrix.
    # ---
    # --- AttributeError, not UnsupportedFeatureError, and deliberately so: the
    # --- formatter each of these names *is* composed on OracleDialect. It is the
    # --- mixin's compose_query_*_sql method inside its body that the dialect does
    # --- not have, so the lookup succeeded and the missing method is a second
    # --- hop down. `to_sql()` inspecting what answers a lookup does not reach it,
    # --- which is the right boundary: the fault is Oracle's wiring, and naming it
    # --- UnsupportedFeatureError would read as core declining a feature.
    **{
        f"rhosocial.activerecord.backend.impl.oracle.expression.show."
        f"{cls}": (AttributeError, f"has no attribute 'compose_query_{stem}_sql'")
        for cls, stem in (
            ("OracleQuerySessionsExpression", "sessions"),
            ("OracleQueryRunningSQLExpression", "running_sql"),
            ("OracleQueryDatabaseInfoExpression", "database_info"),
            ("OracleQueryInstanceInfoExpression", "instance_info"),
            ("OracleQueryObjectsExpression", "objects"),
            ("OracleQueryLocksExpression", "locks"),
            ("OracleQueryWaitEventsExpression", "wait_events"),
            ("OracleQueryNlsParametersExpression", "nls_parameters"),
        )
    },
    # --- group four: roots and types this dialect does not model.
    # --- The root of the type tree declares no generic type name, so there is
    # --- nothing for format_data_type to dispatch on. Each type below is a core
    # --- type with no Oracle counterpart: Oracle spells arrays as nested tables
    # --- of a declared TYPE, binary as RAW/LONG RAW, and has no INT, ENUM,
    # --- VARBINARY, CUSTOM or SQL INTERVAL type. The error names the type, so a
    # --- dialect that starts modelling one fails here.
    "rhosocial.activerecord.backend.expression.types._base.DataType": (
        TypeError, "does not declare a valid generic type name"
    ),
    "rhosocial.activerecord.backend.expression.types.array.ArrayType": (
        TypeError, "does not support the generic type 'array'"
    ),
    "rhosocial.activerecord.backend.expression.types.binary.BinaryType": (
        TypeError, "does not support the generic type 'binary'"
    ),
    "rhosocial.activerecord.backend.expression.types.binary.VarBinaryType": (
        TypeError, "does not support the generic type 'varbinary'"
    ),
    "rhosocial.activerecord.backend.expression.types.enum_.EnumType": (
        TypeError, "does not support the generic type 'enum'"
    ),
    "rhosocial.activerecord.backend.expression.types.uuid_.UUIDType": (
        TypeError, "does not support the generic type 'uuid'"
    ),
    # --- group five: a generic partition clause. This backend requires one of
    # --- its own -- RANGE / LIST / HASH carry structured definitions, and the
    # --- generic clause's untyped dialect_options bag is no longer read. The
    # --- refusal names the four accepted types, so a partition clause this
    # --- backend starts accepting fails here.
    "rhosocial.activerecord.backend.expression.statements.ddl_partition."
    "PartitionClause": (
        TypeError, "Oracle table partitioning requires an Oracle-specific partition"
    ),
    "rhosocial.activerecord.backend.impl.oracle.expression.partition."
    "OraclePartitionClause": (
        TypeError, "Oracle table partitioning requires an Oracle-specific partition"
    ),
}


#: Channels on which *core* drops a class, where the fault is core's serialiser
#: rather than this backend's formatter.
#:
#: Empty, and that is the finding rather than an omission.
#:
#: It held one entry. :class:`~rhosocial.activerecord.backend.expression.xml.XMLTableExpression`
#: builds and renders -- ``XMLTABLE('.' COLUMNS ("C" VARCHAR(10) PATH '.'))`` --
#: and the ``dict`` channel restored that byte for byte, while ``json`` and ``xml``
#: did not: :class:`~rhosocial.activerecord.backend.expression.xml.XMLTableColumn`
#: was a plain class rather than a dataclass, so the JSON encoder emitted ``null``
#: and the XML encoder emitted ``str()`` of the object, the deserialiser handed
#: both straight back, and the renderer read ``.data_type`` off a ``None`` or off a
#: ``str``. ``d350f0a`` had already made ``XMLAttribute`` and ``XMLForestItem``
#: dataclasses for exactly that reason; ``XMLTableColumn`` was in the same module
#: and was missed.
#:
#: Core fixed it in ``ae5b7f9``, on this branch and after this entry was written,
#: by making ``XMLTableColumn`` a dataclass -- the form the serialiser encodes
#: field by field. Re-checked here before the entry was retired, per channel:
#: ``dict``, ``json`` and ``xml`` each restore
#: ``XMLTABLE('.' COLUMNS ("C" VARCHAR(10) PATH '.'))`` with no bind parameters,
#: and each returns a real ``XMLTableColumn`` carrying ``name='c'`` and
#: ``data_type='VARCHAR(10)'`` rather than a ``None`` or a ``str``. Not assumed
#: from the commit message.
#:
#: The retired entry named :class:`~rhosocial.activerecord.backend.expression.xml.XMLTableColumnOption`
#: alongside it as a second miss, and that was an over-claim: it is a ``str, Enum``,
#: never carried as an instance inside a column list, so the encoders encode it as
#: the string it is. It was named because it sat in the same module as the two
#: classes ``d350f0a`` had fixed, which is how the search that missed it was run.
#:
#: The mechanism stays. It is listed per *channel* rather than per class because
#: that is the shape of the defect: one channel can work while another does not,
#: and each is asserted in both directions -- a channel that starts failing has to
#: be listed, and a listed channel that starts working makes its entry stale and
#: fails here. So the next core serialiser that drops a class on one channel is
#: caught by the round-trip comparison rather than by this list.
CORE_SERIALISATION_DEFECTS: Dict[str, tuple] = {}


# ---------------------------------------------------------------------------
# The local SQL assertion: classify the outcome instead of swallowing it
# ---------------------------------------------------------------------------

def assert_sql_roundtrip_classified(fqn, instance, dialect):
    """Assert an expression's SQL survives the round-trip, or say precisely why not.

    Five outcomes, each asserted:

    * **renders** -- every encoding must restore byte-identical SQL *and*
      byte-identical bind parameters. An encoding listed in
      :data:`CORE_SERIALISATION_DEFECTS` is skipped, because core's serialiser is
      known to drop the class on that channel.
    * ``UnsupportedFeatureError`` -- Oracle does not model the feature. Asserted
      as exactly that type, so a formatter raising it for an unrelated reason is
      still visible as that type rather than as a pass.
    * ``NotImplementedError`` -- an expression-category base that names no
      formatter. Asserted as exactly that type, for the same reason.
    * a member of :data:`LEGITIMATE_NON_RENDERS` -- unrenderable by design,
      asserted as its exact type *and* message fragment.
    * **anything else** -- a failure naming the class and the exception.

    There is deliberately no branch for ``ProtocolNotImplementedError``, the third
    thing ``to_sql()`` now reports when what answered a format lookup was a
    probe-only protocol rather than a formatter. No class in either package
    reaches that path on this dialect: reaching it would mean a protocol is
    answering for a formatter Oracle composes, which is a wiring fault to report
    rather than a capability gap to accept.

    Returns:
        A short string naming the branch taken, so the coverage report can
        report the distribution.

    Raises:
        AssertionError: On a round-trip mismatch, on an unexpected exception
            type, or when a class's rendering outcome changed.
    """
    try:
        expected_sql, expected_params = instance.to_sql()
    except UnsupportedFeatureError as exc:
        assert type(exc) is UnsupportedFeatureError, fqn
        return "unsupported"
    except NotImplementedError as exc:
        # An expression-category base that deliberately declares no
        # `format_method`, so there is nothing for the dialect to dispatch to.
        assert type(exc) is NotImplementedError, fqn
        return "abstract"
    except Exception as exc:
        if fqn not in LEGITIMATE_NON_RENDERS:
            raise AssertionError(
                f"{fqn}: to_sql() raised {type(exc).__name__}, which is neither a "
                f"render nor a classified non-render, and this is a defect.\n"
                f"  UnsupportedFeatureError means Oracle lacks the feature and is "
                f"always allowed.\n"
                f"  NotImplementedError means the class is an expression-category "
                f"base and is always allowed.\n"
                f"  A class that cannot render for a reason belonging to its own "
                f"tree belongs in LEGITIMATE_NON_RENDERS.\n"
                f"  Exception: {exc}"
            ) from exc
        expected_type, fragment = LEGITIMATE_NON_RENDERS[fqn]
        assert type(exc) is expected_type, (
            f"{fqn}: LEGITIMATE_NON_RENDERS pins this class as a legitimate "
            f"non-render raising {expected_type.__name__}, but it raised "
            f"{type(exc).__name__}: {exc}"
        )
        assert fragment in str(exc), (
            f"{fqn}: expected the message to say {fragment!r}, got: {exc}"
        )
        return "non-render"

    for channel, decoded in (
        ("dict", deserialize(serialize(instance), dialect)),
        ("json", deserialize_json(serialize_json(instance), dialect)),
        ("xml", deserialize_xml(serialize_xml(instance), dialect)),
    ):
        try:
            decoded_sql, decoded_params = decoded.to_sql()
        except Exception as exc:
            if channel not in CORE_SERIALISATION_DEFECTS.get(fqn, ()):
                raise AssertionError(
                    f"{fqn}: the {channel} channel did not deserialise into "
                    f"something renderable.\n"
                    f"  {type(exc).__name__}: {exc}\n"
                    f"  A class listed in CORE_SERIALISATION_DEFECTS names the "
                    f"channels core is known to lose it on."
                ) from exc
            continue
        if channel in CORE_SERIALISATION_DEFECTS.get(fqn, ()):
            raise AssertionError(
                f"{fqn}: CORE_SERIALISATION_DEFECTS names the {channel} channel as "
                f"one core loses this class on, and it now round-trips. Core has "
                f"been fixed -- remove the entry."
            )
        assert decoded_sql == expected_sql, (
            f"{fqn}: {channel} round-trip changed the SQL.\n"
            f"  original: {expected_sql!r}\n"
            f"  {channel}: {decoded_sql!r}"
        )
        assert decoded_params == expected_params, (
            f"{fqn}: {channel} round-trip changed the bind parameters.\n"
            f"  original: {expected_params!r}\n"
            f"  {channel}: {decoded_params!r}"
        )
    return "rendered"


@pytest.fixture(scope="function")
def oracle_dialect():
    from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

    dialect = OracleDialect()
    dialect.version = (19, 0, 0)
    return dialect


@pytest.fixture(
    params=[fqn for fqn in sorted(REGISTERED)], ids=sorted(REGISTERED)
)
def oracle_expr_case(request, oracle_dialect):
    fqn = request.param
    cls = REGISTERED[fqn]
    instance, source = make_instance(cls, oracle_dialect)
    if instance is None:
        assert fqn in UNCONSTRUCTIBLE, (
            f"{fqn} cannot be built by the generic constructor ({source}) and is "
            f"not in UNCONSTRUCTIBLE. Either register a special constructor for "
            f"it or add it to the tuple with a reason -- do not let it disappear "
            f"into a skip."
        )
        pytest.skip(f"{fqn}: pinned in UNCONSTRUCTIBLE, cannot be constructed ({source})")
    return fqn, instance


class TestOracleExpressionRoundtrip:
    """All expression classes build, and each one either renders or is classified."""

    def test_get_params_roundtrip(self, oracle_expr_case, oracle_dialect):
        fqn, instance = oracle_expr_case
        original = instance.get_params()
        # The ``dict`` channel only, deliberately. ``get_params`` on the decoded
        # side is where core's JSON and XML encoders show their loss -- a plain
        # class arrives as ``None`` or as ``str()`` of itself -- and the SQL
        # assertion below is where that is classified, so asserting it here as
        # well would report one core defect twice and through two different
        # messages.
        assert_params_equal_local(
            deserialize(serialize(instance), oracle_dialect).get_params(),
            original,
            fqn,
        )

    def test_to_sql_roundtrip_classified(self, oracle_expr_case, oracle_dialect):
        """A render must survive the round-trip; a non-render must be classified."""
        fqn, instance = oracle_expr_case
        assert_sql_roundtrip_classified(fqn, instance, oracle_dialect)


class TestMatrixIntegrity:
    """Guards on the matrix and its lists, so neither can quietly change."""

    def test_unconstructible_list_is_exact(self, oracle_dialect):
        """Pin the unconstructible tuple against what the constructor really skips.

        Two directions are checked. A class named here that now builds has gained
        a constructor and the entry is stale; a class that fails to build without
        being named would become a silent skip. Both fail here.
        """
        actual = tuple(
            sorted(
                fqn
                for fqn in REGISTERED
                if make_instance(REGISTERED[fqn], oracle_dialect)[0] is None
            )
        )
        assert actual == tuple(sorted(UNCONSTRUCTIBLE)), (
            "the set of expression classes the generic constructor cannot build "
            "changed.\n"
            f"  now skipped but not named: "
            f"{sorted(set(actual) - set(UNCONSTRUCTIBLE))}\n"
            f"  named but now built: "
            f"{sorted(set(UNCONSTRUCTIBLE) - set(actual))}\n"
            "Each new entry needs a reason in the comment above UNCONSTRUCTIBLE."
        )

    def test_core_serialisation_defects_still_lose_the_class(self, oracle_dialect):
        """Each listed channel still drops its class, and ``dict`` still does not.

        The two directions matter separately. A channel core no longer breaks has
        made its entry wrong. A class that has started surviving *every* channel
        means core fixed the defect, and the whole entry should go.
        """
        for fqn, channels in CORE_SERIALISATION_DEFECTS.items():
            instance, _ = make_instance(REGISTERED[fqn], oracle_dialect)
            assert instance is not None, f"{fqn}: listed as a defect but will not build"
            expected_sql, _ = instance.to_sql()
            assert (
                deserialize(serialize(instance), oracle_dialect).to_sql()[0]
                == expected_sql
            ), (
                f"{fqn}: CORE_SERIALISATION_DEFECTS exists because core's JSON and "
                f"XML encoders drop this class. The dict channel now loses it too, "
                f"so the entry no longer describes the defect."
            )
            for channel in channels:
                payload = {
                    "json": lambda: serialize_json(instance),
                    "xml": lambda: serialize_xml(instance),
                }[channel]()
                decoded = {
                    "json": lambda: deserialize_json(payload, oracle_dialect),
                    "xml": lambda: deserialize_xml(payload, oracle_dialect),
                }[channel]()
                try:
                    decoded_sql, _ = decoded.to_sql()
                except Exception as exc:
                    continue  # still lost, which is what the entry claims
                raise AssertionError(
                    f"{fqn}: the {channel} channel is pinned in "
                    f"CORE_SERIALISATION_DEFECTS as one core loses this class on, "
                    f"and it now round-trips to {decoded_sql!r}. Core has been "
                    f"fixed -- remove the channel from the entry."
                )

    def test_unconstructible_entries_are_real_classes(self):
        """Every entry names a class that was actually collected.

        A typo in the tuple would otherwise exempt nothing while still reading as
        a deliberate decision.
        """
        unknown = set(UNCONSTRUCTIBLE) - set(REGISTERED)
        assert not unknown, (
            f"UNCONSTRUCTIBLE names classes that were not registered: {sorted(unknown)}"
        )

    def test_legitimate_non_renders_are_real_classes(self):
        """Every pinned non-render names a class that was actually collected."""
        unknown = set(LEGITIMATE_NON_RENDERS) - set(REGISTERED)
        assert not unknown, (
            f"LEGITIMATE_NON_RENDERS names classes that were not registered: "
            f"{sorted(unknown)}"
        )

    def test_core_serialisation_defects_are_real_classes(self):
        """Every pinned core serialisation defect names a collected class."""
        unknown = set(CORE_SERIALISATION_DEFECTS) - set(REGISTERED)
        assert not unknown, (
            f"CORE_SERIALISATION_DEFECTS names classes that were not registered: "
            f"{sorted(unknown)}"
        )

    def test_pinned_non_render_really_does_not_render(self, oracle_dialect):
        """Each pinned entry still raises what it claims, for the stated reason.

        Without this, an entry could sit in the tuple for a class that renders
        perfectly well, and the matrix would be asserting nothing about it.
        """
        for fqn, (expected_type, fragment) in LEGITIMATE_NON_RENDERS.items():
            instance, source = make_instance(REGISTERED[fqn], oracle_dialect)
            assert instance is not None, (
                f"{fqn} is pinned as a non-render but could not be constructed "
                f"({source})"
            )
            with pytest.raises(expected_type) as exc_info:
                instance.to_sql()
            assert fragment in str(exc_info.value), (
                f"{fqn}: expected the message to mention {fragment!r}, got: "
                f"{exc_info.value}"
            )

    def test_matrix_covers_both_packages(self):
        """The matrix covers every concrete class either package defines.

        Re-walked here rather than trusting the module-level collection, so a
        class that appeared after import is caught. The package walk is used
        rather than the registry because the registry also holds whatever
        backends other test modules happened to import.
        """
        ExpressionRegistry._auto_register_builtins()
        expected = set(_collect_matrix_classes())
        stray = [
            fqn
            for fqn in REGISTERED
            if not fqn.startswith(f"{ORACLE_EXPR_PKG}.")
            and not fqn.startswith(f"{CORE_EXPR_PKG}.")
        ]
        assert not stray, f"classes outside the two packages are in the matrix: {stray}"
        assert expected == set(REGISTERED), (
            "the set of classes the two packages define changed after collection."
            f"\n  now defined but not covered: {sorted(expected - set(REGISTERED))}"
            f"\n  covered but no longer defined: {sorted(set(REGISTERED) - expected)}"
        )
        assert len(REGISTERED) > 300, (
            f"only {len(REGISTERED)} classes collected; the package walk may "
            f"have stopped early"
        )

    def test_core_package_is_actually_covered(self):
        """Core's own classes are in scope, not an accident of the walk.

        A backend matrix that only covered its own expressions would leave the
        classes every statement is built from untested, which is precisely the
        blind spot that let a formatter read a field core had removed stay green.
        """
        core_classes = [
            fqn for fqn in REGISTERED if fqn.startswith(f"{CORE_EXPR_PKG}.")
        ]
        assert len(core_classes) > 200, (
            f"only {len(core_classes)} core classes in the matrix; the core "
            f"package is not being walked"
        )

    def test_every_covered_class_is_registered_for_deserialization(self):
        """A class in the matrix can be found again when deserializing.

        Deserialization looks the class up by name, so a class the matrix renders
        but the registry cannot resolve would round-trip into the wrong thing or
        nothing at all.
        """
        ExpressionRegistry._auto_register_builtins()
        unresolved = sorted(set(REGISTERED) - set(ExpressionRegistry._registry))
        assert not unresolved, (
            f"the matrix covers classes the registry cannot resolve: {unresolved}"
        )

    def test_coverage_report(self, oracle_dialect):
        """Surface the classification distribution, so coverage stays transparent.

        One class is counted once, by whichever branch
        :func:`assert_sql_roundtrip_classified` takes for it: ``rendered``,
        ``abstract``, ``unsupported``, ``non-render``, or ``unbuildable``. Nothing
        here is asserted against a pinned number, because the class count is core's
        to change -- a distribution pinned exactly would fail on every class core
        adds, and say nothing about this backend. The pins that do have to hold are
        the lists, and those are checked in both directions above.

        Where the counts sit as of this commit, counted over
        ``len(REGISTERED)`` sorted by fqn: 369 classes -- 278 render through all
        three encodings, 15 are expression-category bases raising
        ``NotImplementedError``, 53 raise ``UnsupportedFeatureError``, 23 are
        non-renders pinned in :data:`LEGITIMATE_NON_RENDERS`, none unbuildable.
        The 20 group-one entries that used to be counted as non-renders now fall
        into ``unsupported``, because ``to_sql()`` reports a missing formatter as
        ``UnsupportedFeatureError``; they are still pinned there, and
        :func:`test_pinned_non_render_really_does_not_render` is what keeps their
        message fragments honest, since the ``unsupported`` branch is taken before
        the dict is consulted.
        """
        ExpressionRegistry._auto_register_builtins()
        counts: Dict[str, int] = {}
        for fqn in sorted(REGISTERED):
            instance, _source = make_instance(REGISTERED[fqn], oracle_dialect)
            if instance is None:
                counts["unbuildable"] = counts.get("unbuildable", 0) + 1
                continue
            branch = assert_sql_roundtrip_classified(fqn, instance, oracle_dialect)
            counts[branch] = counts.get(branch, 0) + 1
        print(f"\nexpression matrix: {len(REGISTERED)} classes  {counts}")
        for fqn in UNCONSTRUCTIBLE:
            print(f"  not constructible: {fqn}")


# ---------------------------------------------------------------------------
# Oracle's namespace shape
# ---------------------------------------------------------------------------
#
# The one thing this backend is the sample for. Oracle has a schema and *no*
# catalog, where core's default assumed a catalog outside a schema; before
# validate_namespace / format_qualified_name split, that worked because the empty
# outer slot was simply not emitted. "Simply not emitted" is indistinguishable
# from "silently dropped", and a silently dropped level resolves the name
# against a different object than the caller named.
#
# So the tests below check three things, and the second is the one that would
# catch a regression the first would miss:
#
#   1. the shape -- exactly one level, then the name, joined by the separator;
#   2. the refusal -- an object carrying a catalog is reported, not truncated;
#   3. the level is load-bearing -- remove it and the SQL must change.
#
# (3) exists because a namespace test can pass while the formatter ignores the
# namespace it claims to test: if the value under test also appears elsewhere in
# the output, dropping it changes nothing and the test is vacuous. The schema
# value used here ("AR_CRM") appears nowhere else in the SQL, so any change to
# the level is visible.

_OWNER = "reporting"          # deliberately not a substring of any object name
_OBJECT = "ledger_entries"    # deliberately not the owner, in either direction


class TestOracleNamespaceShape:
    """Oracle names with one level -- the owner -- and refuses a catalog."""

    def test_one_level_shape(self, oracle_dialect):
        sql, params = Table(oracle_dialect, _OBJECT, schema_name=_OWNER).to_sql()
        assert sql == '"REPORTING"."LEDGER_ENTRIES"'
        assert params == ()

    def test_exactly_one_level_is_emitted(self, oracle_dialect):
        """One separator, not two: there is no level above the owner.

        Counting separators rather than comparing a whole string makes the
        assertion about the *number of levels*, which is the property, instead of
        about a spelling that could change.
        """
        sql, _ = Table(oracle_dialect, _OBJECT, schema_name=_OWNER).to_sql()
        assert sql.count(oracle_dialect.separator) == 1, sql

    def test_separator_is_a_dot(self, oracle_dialect):
        assert oracle_dialect.separator == "."

    def test_no_catalog_level_in_the_output(self, oracle_dialect):
        """The catalog slot does not appear at all -- not empty, not rendered.

        Asserted by building an object whose owner is *not* a catalog and
        checking the output has exactly the levels the object carries. A stray
        empty slot (`"."` twice, or a leading separator) would fail (1) already;
        this pins that nothing is being silently appended.
        """
        sql, _ = Table(oracle_dialect, _OBJECT, schema_name=_OWNER).to_sql()
        assert not sql.startswith(oracle_dialect.separator)
        assert not sql.endswith(oracle_dialect.separator)

    def test_catalog_is_refused_not_dropped(self, oracle_dialect):
        """A catalog-carrying object must be reported.

        This is the assertion the whole split exists for. If the level were
        dropped instead, the SQL would still be well-formed and would name
        something the caller did not ask for -- and the caller would never learn
        why, because Oracle would resolve it.
        """
        with pytest.raises(UnsupportedFeatureError) as exc_info:
            Table(
                oracle_dialect, _OBJECT, catalog_name="other", schema_name=_OWNER
            ).to_sql()
        assert "catalog" in str(exc_info.value)

    def test_catalog_alone_is_refused(self, oracle_dialect):
        """A catalog with no owner is still refused, not rendered as a bare name."""
        with pytest.raises(UnsupportedFeatureError) as exc_info:
            Table(oracle_dialect, _OBJECT, catalog_name="other").to_sql()
        assert "catalog" in str(exc_info.value)

    def test_owner_is_load_bearing(self, oracle_dialect):
        """Removing the owner must change the SQL.

        The reverse check from the core plan: if dropping the parameter left the
        output identical, the test above would be testing nothing. Here the two
        renderings differ by exactly the owner level.
        """
        with_owner, _ = Table(
            oracle_dialect, _OBJECT, schema_name=_OWNER
        ).to_sql()
        without_owner, _ = Table(oracle_dialect, _OBJECT).to_sql()
        assert with_owner != without_owner
        assert without_owner == '"LEDGER_ENTRIES"'
        assert _OWNER.upper() in with_owner

    @pytest.mark.parametrize(
        "kind",
        [
            View,
            MaterializedView,
            Index,
            Sequence,
            Trigger,
            Synonym,
            Type,
            Procedure,
        ],
    )
    def test_every_kind_has_the_same_shape(self, oracle_dialect, kind):
        """The shape is a property of the dialect, not of one object class.

        A kind that rendered a different number of levels would mean the naming
        path had a per-kind branch nobody intended.
        """
        sql, _ = kind(oracle_dialect, _OBJECT, schema_name=_OWNER).to_sql()
        assert sql == '"REPORTING"."LEDGER_ENTRIES"'
        assert sql.count(oracle_dialect.separator) == 1

    @pytest.mark.parametrize(
        "kind",
        [View, MaterializedView, Index, Sequence, Trigger, Synonym, Type, Procedure],
    )
    def test_every_kind_refuses_a_catalog(self, oracle_dialect, kind):
        with pytest.raises(UnsupportedFeatureError, match="catalog"):
            kind(
                oracle_dialect, _OBJECT, catalog_name="other", schema_name=_OWNER
            ).to_sql()
