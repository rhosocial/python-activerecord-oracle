# tests/rhosocial/activerecord_oracle_test/feature/backend/expression/test_select_wildcard_qualification.py

"""Oracle's select-list wildcard, qualified on the node.

Oracle rejects ``SELECT *, expr AS x`` with ORA-00923, so a ``*`` that shares
the select list with anything else has to be written ``t.*``. That is exactly
the shape ``find_all(derived=True)`` produces: ``ActiveQuery`` starts with one
bare ``AllColumnsWildcard`` and appends the derived expressions.

The qualification used to be a text post-process over the rendered statement:
find ``SELECT``, find `` FROM ``, split the span on commas, and look for an
item that is exactly ``*``. Three things were wrong with that and only the
first was visible in CI. It never fired when the source had no alias, because
it took the qualifier from ``TableSource.alias`` and returned the SQL
untouched when that was ``None`` -- which is every ``ActiveQuery``, whose FROM
is a ``NamedRelationRef(Table(...))`` with no alias. It split on every comma
in the span, so an inline literal such as ``CONCAT('x, y')`` moved the item
boundaries and the wildcard was missed or the wrong item rewritten. And with
more than one source it took the first one, guessing which source a ``*``
spans instead of refusing to.

These tests render only -- no Oracle is involved -- and pin each of those
shapes separately, so a regression names which of the three came back.
"""

import pytest
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    AllColumnsWildcard,
    Column,
    FunctionCall,
    Literal,
    NumericColumn,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.query_parts import JoinClause
from rhosocial.activerecord.backend.expression.sources import DerivedTableSource, NamedRelationRef

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

pytestmark = [pytest.mark.feature, pytest.mark.backend]


@pytest.fixture
def dialect():
    return OracleDialect(version=(23, 0, 0))


@pytest.fixture
def wildcard(dialect):
    return AllColumnsWildcard(dialect)


@pytest.fixture
def derived(dialect):
    return (NumericColumn(dialect, "price") * Literal(dialect, 0.9)).as_("discounted_price")


class TestUnqualifiedWildcardBesideExpressions:
    def test_unaliased_source_takes_its_own_name(self, dialect, wildcard, derived):
        """The shape ActiveQuery builds: no alias, so the table names itself."""
        sql, params = QueryStatement(
            dialect,
            select=[wildcard, derived],
            from_=NamedRelationRef(dialect, Table(dialect, "product")),
        ).to_sql()

        assert sql == 'SELECT "PRODUCT".*, "PRICE" * ? AS "DISCOUNTED_PRICE" FROM "PRODUCT"'
        assert params == (0.9,)

    def test_aliased_source_takes_its_alias(self, dialect, wildcard, derived):
        sql, params = QueryStatement(
            dialect,
            select=[wildcard, derived],
            from_=NamedRelationRef(dialect, Table(dialect, "product"), alias="p"),
        ).to_sql()

        assert sql == 'SELECT "P".*, "PRICE" * ? AS "DISCOUNTED_PRICE" FROM "PRODUCT" AS "P"'
        assert params == (0.9,)

    def test_lone_wildcard_stays_bare(self, dialect, wildcard):
        """One source and nothing else: ``SELECT *`` is legal Oracle as it is."""
        sql, params = QueryStatement(
            dialect,
            select=[wildcard],
            from_=NamedRelationRef(dialect, Table(dialect, "product")),
        ).to_sql()

        assert sql == 'SELECT * FROM "PRODUCT"'
        assert params == ()

    def test_already_qualified_wildcard_is_left_alone(self, dialect, derived):
        """An explicit ``t.*`` the caller wrote is not overridden."""
        explicit = AllColumnsWildcard(
            dialect, relation=NamedRelationRef(dialect, Table(dialect, "other"), alias="o")
        )
        sql, _ = QueryStatement(
            dialect,
            select=[explicit, derived],
            from_=NamedRelationRef(dialect, Table(dialect, "product")),
        ).to_sql()

        assert sql == 'SELECT "O".*, "PRICE" * ? AS "DISCOUNTED_PRICE" FROM "PRODUCT"'


class TestCommaInsideASelectItem:
    def test_inline_literal_with_a_comma(self, dialect, wildcard):
        """A comma in rendered text is not a select-item boundary.

        The old implementation split the span between ``SELECT`` and `` FROM ``
        on every comma, so this item became two and the wildcard handling went
        with them.
        """
        concat = FunctionCall(
            dialect, "CONCAT", Literal(dialect, "x, y"), Column(dialect, "name")
        ).as_("c")
        sql, params = QueryStatement(
            dialect,
            select=[wildcard, concat],
            from_=NamedRelationRef(dialect, Table(dialect, "product")),
        ).to_sql()

        assert sql == 'SELECT "PRODUCT".*, CONCAT(?, "NAME") AS "C" FROM "PRODUCT"'
        assert params == ("x, y",)

    def test_wildcard_after_a_comma_carrying_item(self, dialect):
        """The wildcard is found by node, whatever position the text puts it in."""
        concat = FunctionCall(dialect, "CONCAT", Literal(dialect, "a,b")).as_("c")
        sql, _ = QueryStatement(
            dialect,
            select=[concat, AllColumnsWildcard(dialect)],
            from_=NamedRelationRef(dialect, Table(dialect, "product")),
        ).to_sql()

        assert sql == 'SELECT CONCAT(?) AS "C", "PRODUCT".* FROM "PRODUCT"'


class TestSourcesWithNoSingleIdentity:
    def test_aliased_derived_table_is_named_by_its_alias(self, dialect, wildcard):
        inner = QueryStatement(
            dialect,
            select=[Column(dialect, "id")],
            from_=NamedRelationRef(dialect, Table(dialect, "t")),
        )
        source = DerivedTableSource(dialect, inner, alias="s")
        sql, _ = QueryStatement(
            dialect, select=[wildcard, Column(dialect, "id", "s")], from_=source
        ).to_sql()

        assert sql == 'SELECT "S".*, "S"."ID" FROM (SELECT "ID" FROM "T") AS "S"'

    def test_several_comma_separated_sources_are_refused(self, dialect, wildcard, derived):
        """Which of the two the ``*`` spans is not knowable here."""
        statement = QueryStatement(
            dialect,
            select=[wildcard, derived],
            from_=[
                NamedRelationRef(dialect, Table(dialect, "a")),
                NamedRelationRef(dialect, Table(dialect, "b")),
            ],
        )

        with pytest.raises(UnsupportedFeatureError, match="unqualified"):
            statement.to_sql()

    def test_join_without_an_alias_is_refused(self, dialect, wildcard, derived):
        statement = QueryStatement(
            dialect,
            select=[wildcard, derived],
            from_=JoinClause(dialect, "a", "b"),
        )

        with pytest.raises(UnsupportedFeatureError, match="unqualified"):
            statement.to_sql()

    def test_no_source_at_all_is_refused(self, dialect, wildcard, derived):
        statement = QueryStatement(dialect, select=[wildcard, derived])

        with pytest.raises(UnsupportedFeatureError, match="unqualified"):
            statement.to_sql()


class TestRenderingDoesNotRewriteTheStatement:
    def test_select_list_is_unchanged_after_rendering(self, dialect, wildcard, derived):
        """Qualification is a rendering-time binding, not a mutation."""
        statement = QueryStatement(
            dialect,
            select=[wildcard, derived],
            from_=NamedRelationRef(dialect, Table(dialect, "product")),
        )

        first, _ = statement.to_sql()
        second, _ = statement.to_sql()

        assert first == second
        assert statement.select[0].relation is None
        assert statement.select[0] is wildcard
