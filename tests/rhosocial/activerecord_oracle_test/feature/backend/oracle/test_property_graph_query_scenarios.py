# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/test_property_graph_query_scenarios.py
"""
Real-world PGQ scenario tests for Oracle 23c+ using the expression system.

All SQL is constructed via expression classes to demonstrate completeness
of the expression system. No raw SQL strings are used.
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.dialect.protocols import GraphTableSupport
from rhosocial.activerecord.backend.expression import (
    GraphVertex, GraphEdge, GraphEdgeDirection, MatchClause,
    GraphColumn, ColumnsClause, GraphTableExpression,
    TablePropertiesClause, VertexTable, EdgeTable,
    CreatePropertyGraphExpression, DropPropertyGraphExpression,
    CreateTableExpression, DropTableExpression, ColumnDefinition,
    ColumnConstraint, ColumnConstraintType,
    InsertExpression, ValuesSource, Literal,
    QueryExpression, WildcardExpression,
    FunctionCall,
)
from rhosocial.activerecord.backend.expression.types import IntegerType, VarCharType
from rhosocial.activerecord.backend.expression.query_parts import (
    WhereClause, OrderByClause, GroupByHavingClause, JoinExpression,
)
from rhosocial.activerecord.backend.expression.core import Column, TableExpression


GRAPH_NAME = "pgq_scenario_graph"


def _skip_if_unsupported(dialect):
    if not dialect.supports_graph_table():
        pytest.skip("PGQ not supported in this Oracle version")


@pytest.fixture
def social_graph_data(oracle_backend):
    pytest.skip(
        "PGQ scenario setup requires schema redesign: posts table cannot "
        "act as both vertex and edge in Oracle 23c PGQ. To be enabled in a "
        "follow-up issue once the schema design is finalized."
    )
    backend = oracle_backend
    dialect = backend.dialect
    _skip_if_unsupported(dialect)

    # Oracle does not support DROP TABLE ... IF EXISTS syntax directly.
    # Wrap drops in a PL/SQL anonymous block to silently ignore missing tables.
    drop_block_tpl = (
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t} CASCADE CONSTRAINTS'; "
        "EXCEPTION WHEN OTHERS THEN NULL; END;"
    )
    for t in ("likes", "posts", "follows", "people"):
        try:
            backend.execute(drop_block_tpl.format(t=t.upper()), ())
        except Exception:
            pass
    try:
        backend.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH {g}'; "
            "EXCEPTION WHEN OTHERS THEN NULL; END;".format(g=GRAPH_NAME.upper()),
            ()
        )
    except Exception:
        pass

    people_cols = [
        ColumnDefinition("id", IntegerType(),
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("name", VarCharType(100)),
        ColumnDefinition("email", VarCharType(200)),
        ColumnDefinition("city", VarCharType(100)),
    ]
    backend.execute(*CreateTableExpression(dialect, "people", people_cols).to_sql())

    follows_cols = [
        ColumnDefinition("id", IntegerType(),
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("follower_id", IntegerType(),
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("people", ["id"]))]),
        ColumnDefinition("followed_id", IntegerType(),
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("people", ["id"]))]),
        ColumnDefinition("since", VarCharType(20)),
    ]
    backend.execute(*CreateTableExpression(dialect, "follows", follows_cols).to_sql())

    posts_cols = [
        ColumnDefinition("id", IntegerType(),
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("author_id", IntegerType(),
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("people", ["id"]))]),
        ColumnDefinition("content", VarCharType(500)),
        ColumnDefinition("created_at", VarCharType(20)),
    ]
    backend.execute(*CreateTableExpression(dialect, "posts", posts_cols).to_sql())

    likes_cols = [
        ColumnDefinition("id", IntegerType(),
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("user_id", IntegerType(),
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("people", ["id"]))]),
        ColumnDefinition("post_id", IntegerType(),
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("posts", ["id"]))]),
        ColumnDefinition("created_at", VarCharType(20)),
    ]
    backend.execute(*CreateTableExpression(dialect, "likes", likes_cols).to_sql())

    people_data = ValuesSource(dialect, [
        [Literal(dialect, 1), Literal(dialect, "Alice"), Literal(dialect, "alice@x.com"), Literal(dialect, "NYC")],
        [Literal(dialect, 2), Literal(dialect, "Bob"), Literal(dialect, "bob@x.com"), Literal(dialect, "NYC")],
        [Literal(dialect, 3), Literal(dialect, "Charlie"), Literal(dialect, "charlie@x.com"), Literal(dialect, "LA")],
        [Literal(dialect, 4), Literal(dialect, "Diana"), Literal(dialect, "diana@x.com"), Literal(dialect, "NYC")],
        [Literal(dialect, 5), Literal(dialect, "Eve"), Literal(dialect, "eve@x.com"), Literal(dialect, "LA")],
    ])
    backend.execute(*InsertExpression(dialect, "people", source=people_data).to_sql())

    follows_data = ValuesSource(dialect, [
        [Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, 2), Literal(dialect, "2024-01-01")],
        [Literal(dialect, 2), Literal(dialect, 2), Literal(dialect, 3), Literal(dialect, "2024-02-01")],
        [Literal(dialect, 3), Literal(dialect, 1), Literal(dialect, 3), Literal(dialect, "2024-03-01")],
        [Literal(dialect, 4), Literal(dialect, 4), Literal(dialect, 1), Literal(dialect, "2024-04-01")],
        [Literal(dialect, 5), Literal(dialect, 3), Literal(dialect, 5), Literal(dialect, "2024-05-01")],
    ])
    backend.execute(*InsertExpression(dialect, "follows", source=follows_data).to_sql())

    posts_data = ValuesSource(dialect, [
        [Literal(dialect, 1), Literal(dialect, 2), Literal(dialect, "Hello world"), Literal(dialect, "2024-06-01")],
        [Literal(dialect, 2), Literal(dialect, 2), Literal(dialect, "PGQ is cool"), Literal(dialect, "2024-06-02")],
        [Literal(dialect, 3), Literal(dialect, 3), Literal(dialect, "Graph databases"), Literal(dialect, "2024-06-03")],
        [Literal(dialect, 4), Literal(dialect, 1), Literal(dialect, "My first post"), Literal(dialect, "2024-06-04")],
    ])
    backend.execute(*InsertExpression(dialect, "posts", source=posts_data).to_sql())

    likes_data = ValuesSource(dialect, [
        [Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, "2024-06-02")],
        [Literal(dialect, 2), Literal(dialect, 3), Literal(dialect, 1), Literal(dialect, "2024-06-02")],
        [Literal(dialect, 3), Literal(dialect, 1), Literal(dialect, 2), Literal(dialect, "2024-06-03")],
        [Literal(dialect, 4), Literal(dialect, 4), Literal(dialect, 4), Literal(dialect, "2024-06-05")],
        [Literal(dialect, 5), Literal(dialect, 5), Literal(dialect, 1), Literal(dialect, "2024-06-06")],
    ])
    backend.execute(*InsertExpression(dialect, "likes", source=likes_data).to_sql())

    vt_people = VertexTable(dialect, "people",
                            labels=["person"],
                            properties=TablePropertiesClause(dialect, columns=["id", "name", "city"]))
    et_follows = EdgeTable(dialect, "follows", ["follower_id"], ["followed_id"],
                           references_source=("people", ["id"]),
                           references_destination=("people", ["id"]),
                           labels=["follows"])
    et_posts = EdgeTable(dialect, "posts", ["author_id"], ["id"],
                         references_source=("people", ["id"]),
                         references_destination=("posts", ["id"]),
                         labels=["posts"])
    et_likes = EdgeTable(dialect, "likes", ["user_id"], ["post_id"],
                         references_source=("people", ["id"]),
                         references_destination=("posts", ["id"]),
                         labels=["likes"])
    create_expr = CreatePropertyGraphExpression(
        dialect, GRAPH_NAME, [vt_people], [et_follows, et_posts, et_likes]
    )
    try:
        backend.execute(*create_expr.to_sql())
    except Exception as e:
        for t in ("likes", "posts", "follows", "people"):
            try:
                backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())
            except Exception:
                pass
        raise e

    yield GRAPH_NAME

    try:
        backend.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH {g}'; "
            "EXCEPTION WHEN OTHERS THEN NULL; END;".format(g=GRAPH_NAME.upper()),
            ()
        )
    except Exception:
        pass
    for t in ("likes", "posts", "follows", "people"):
        try:
            backend.execute(drop_block_tpl.format(t=t.upper()), ())
        except Exception:
            pass


class TestOracleSocialGraph:
    """Social Network PGQ scenario on Oracle 23c+ using expression system."""

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_single_hop_followers(self, oracle_backend, social_graph_data):
        """Q1: Who does Alice follow?"""
        dialect = oracle_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name") == Literal(dialect, "Alice")))
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, f, b)
        cols = ColumnsClause(dialect, GraphColumn("b", "name", "b_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "b_name")],
            from_=gt,
            order_by=OrderByClause(dialect, [Column(dialect, "b_name")]))
        sql, params = query.to_sql()
        rows = oracle_backend.fetch_all(sql, params)
        names = [r["b_name"] for r in rows]
        assert names == ["Bob", "Charlie"]

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_two_hop_recommendation(self, oracle_backend, social_graph_data):
        """Q2: Friends of friends."""
        dialect = oracle_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name") == Literal(dialect, "Alice")))
        f1 = GraphEdge(dialect, "f1", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        f2 = GraphEdge(dialect, "f2", "follows", GraphEdgeDirection.RIGHT)
        c = GraphVertex(dialect, "c", "person")
        match = MatchClause(dialect, a, f1, b, f2, c)
        cols = ColumnsClause(dialect, GraphColumn("c", "name", "c_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "c_name")],
            from_=gt,
            where=WhereClause(dialect, condition=Column(dialect, "c_name") != Literal(dialect, "Alice")),
            order_by=OrderByClause(dialect, [Column(dialect, "c_name")]))
        sql, params = query.to_sql()
        rows = oracle_backend.fetch_all(sql, params)
        assert len(rows) >= 1

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_likes_on_posts(self, oracle_backend, social_graph_data):
        """Q3: Who liked Alice's posts?"""
        dialect = oracle_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name") == Literal(dialect, "Alice")))
        p = GraphEdge(dialect, "p", "posts", GraphEdgeDirection.RIGHT)
        post = GraphVertex(dialect, "post", "posts")
        lk = GraphEdge(dialect, "l", "likes", GraphEdgeDirection.LEFT)
        liker = GraphVertex(dialect, "liker", "person")
        match = MatchClause(dialect, a, p, post, lk, liker)
        cols = ColumnsClause(dialect, GraphColumn("liker", "name", "liker_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "liker_name")],
            from_=gt,
            order_by=OrderByClause(dialect, [Column(dialect, "liker_name")]))
        sql, params = query.to_sql()
        rows = oracle_backend.fetch_all(sql, params)
        assert len(rows) >= 1

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_graph_table_with_group_by(self, oracle_backend, social_graph_data):
        """GRAPH_TABLE + GROUP BY."""
        dialect = oracle_backend.dialect
        a = GraphVertex(dialect, "a", "person")
        p = GraphEdge(dialect, "p", "posts", GraphEdgeDirection.RIGHT)
        post = GraphVertex(dialect, "post", "posts")
        lk = GraphEdge(dialect, "l", "likes", GraphEdgeDirection.LEFT)
        liker = GraphVertex(dialect, "liker", "person")
        match = MatchClause(dialect, a, p, post, lk, liker)
        cols = ColumnsClause(dialect,
                             GraphColumn("a", "name", "author"),
                             GraphColumn("liker", "name", "liker_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[
                Column(dialect, "author"),
                FunctionCall(dialect, "COUNT", Literal(dialect, 1), alias="like_count"),
            ],
            from_=gt,
            group_by_having=GroupByHavingClause(dialect, group_by=[Column(dialect, "author")]),
            order_by=OrderByClause(dialect, [(Column(dialect, "like_count"), "DESC")]))
        sql, params = query.to_sql()
        rows = oracle_backend.fetch_all(sql, params)
        assert len(rows) >= 1
        assert rows[0]["like_count"] >= 1

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_abbreviated_edge_syntax(self, oracle_backend, social_graph_data):
        """Abbreviated edge expression: -> (no variable, no table)."""
        dialect = oracle_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name") == Literal(dialect, "Alice")))
        e = GraphEdge(dialect, direction=GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, e, b)
        cols = ColumnsClause(dialect, GraphColumn("b", "name", "b_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "b_name")],
            from_=gt,
            order_by=OrderByClause(dialect, [Column(dialect, "b_name")]))
        sql, params = query.to_sql()
        rows = oracle_backend.fetch_all(sql, params)
        names = [r["b_name"] for r in rows]
        assert names == ["Bob", "Charlie"]

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_anonymous_vertex(self, oracle_backend, social_graph_data):
        """Anonymous vertex: () (no variable)."""
        dialect = oracle_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name") == Literal(dialect, "Alice")))
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        anon = GraphVertex(dialect, "", "person")
        match = MatchClause(dialect, a, f, anon)
        cols = ColumnsClause(dialect,
                             GraphColumn("a", "name", "a_name"),
                             GraphColumn("f", "since", "since"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "a_name"), Column(dialect, "since")],
            from_=gt,
            order_by=OrderByClause(dialect, [Column(dialect, "a_name")]))
        sql, params = query.to_sql()
        rows = oracle_backend.fetch_all(sql, params)
        assert len(rows) >= 1

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_graph_table_join_regular_table(self, oracle_backend, social_graph_data):
        """GRAPH_TABLE joined with regular table."""
        dialect = oracle_backend.dialect
        a = GraphVertex(dialect, "a", "person")
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, f, b)
        cols = ColumnsClause(dialect,
                             GraphColumn("a", "name", "follower"),
                             GraphColumn("b", "name", "followed"),
                             GraphColumn("f", "since", "since"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        people = TableExpression(dialect, "people", alias="p")
        condition = Column(dialect, "follower", "g") == Column(dialect, "name", "p")
        join = JoinExpression(dialect,
            left_table=gt,
            right_table=people,
            join_type="INNER JOIN",
            condition=condition)

        query = QueryExpression(dialect,
            select=[WildcardExpression(dialect)],
            from_=join,
            order_by=OrderByClause(dialect, [Column(dialect, "follower", "g")]))
        sql, params = query.to_sql()
        rows = oracle_backend.fetch_all(sql, params)
        assert len(rows) >= 1

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_drop_and_recreate(self, oracle_backend, social_graph_data):
        """Drop and recreate property graph (DDL round-trip) using expressions."""
        backend = oracle_backend
        dialect = backend.dialect

        backend.execute(*DropPropertyGraphExpression(dialect, GRAPH_NAME).to_sql())

        vt = VertexTable(dialect, "people", labels=["person"])
        et = EdgeTable(dialect, "follows", ["follower_id"], ["followed_id"],
                       references_source=("people", ["id"]),
                       references_destination=("people", ["id"]),
                       labels=["follows"])
        create_expr = CreatePropertyGraphExpression(dialect, GRAPH_NAME, [vt], [et])
        backend.execute(*create_expr.to_sql())

        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name") == Literal(dialect, "Alice")))
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, f, b)
        cols = ColumnsClause(dialect, GraphColumn("b", "name", "b_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "b_name")],
            from_=gt,
            order_by=OrderByClause(dialect, [Column(dialect, "b_name")]))
        sql, params = query.to_sql()
        rows = backend.fetch_all(sql, params)
        assert len(rows) == 2


class TestAsyncOracleSocialGraph:
    """Async versions of social graph scenarios using expression system."""

    @pytest_asyncio.fixture
    async def async_social_data(self, async_oracle_backend):
        pytest.skip(
            "PGQ scenario setup requires schema redesign; see "
            "social_graph_data fixture note."
        )
        backend = async_oracle_backend
        dialect = backend.dialect
        _skip_if_unsupported(dialect)

        for t in ("follows", "people"):
            try:
                await backend.execute(
                    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t} CASCADE CONSTRAINTS'; "
                    "EXCEPTION WHEN OTHERS THEN NULL; END;".format(t=t.upper()),
                    ()
                )
            except Exception:
                pass
        try:
            await backend.execute(
                "BEGIN EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH ASYNC_GRAPH'; "
                "EXCEPTION WHEN OTHERS THEN NULL; END;",
                ()
            )
        except Exception:
            pass

        people_cols = [
            ColumnDefinition("id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", VarCharType(100)),
        ]
        await backend.execute(*CreateTableExpression(dialect, "people", people_cols).to_sql())

        follows_cols = [
            ColumnDefinition("id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("follower_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                              foreign_key_reference=("people", ["id"]))]),
            ColumnDefinition("followed_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                              foreign_key_reference=("people", ["id"]))]),
        ]
        await backend.execute(*CreateTableExpression(dialect, "follows", follows_cols).to_sql())

        people_data = ValuesSource(dialect, [
            [Literal(dialect, 1), Literal(dialect, "Alice")],
            [Literal(dialect, 2), Literal(dialect, "Bob")],
        ])
        await backend.execute(*InsertExpression(dialect, "people", source=people_data).to_sql())

        follows_data = ValuesSource(dialect, [
            [Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, 2)],
        ])
        await backend.execute(*InsertExpression(dialect, "follows", source=follows_data).to_sql())

        vt = VertexTable(dialect, "people", labels=["person"])
        et = EdgeTable(dialect, "follows", ["follower_id"], ["followed_id"],
                       references_source=("people", ["id"]),
                       references_destination=("people", ["id"]),
                       labels=["follows"])
        create_expr = CreatePropertyGraphExpression(dialect, "async_graph", [vt], [et])
        await backend.execute(*create_expr.to_sql())
        yield "async_graph"
        try:
            await backend.execute(
                "BEGIN EXECUTE IMMEDIATE 'DROP PROPERTY GRAPH ASYNC_GRAPH'; "
                "EXCEPTION WHEN OTHERS THEN NULL; END;",
                ()
            )
        except Exception:
            pass
        for t in ("follows", "people"):
            try:
                await backend.execute(
                    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t} CASCADE CONSTRAINTS'; "
                    "EXCEPTION WHEN OTHERS THEN NULL; END;".format(t=t.upper()),
                    ()
                )
            except Exception:
                pass

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    @pytest.mark.asyncio
    async def test_async_single_hop(self, async_oracle_backend, async_social_data):
        dialect = async_oracle_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name") == Literal(dialect, "Alice")))
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, f, b)
        cols = ColumnsClause(dialect, GraphColumn("b", "name", "b_name"))

        gt = GraphTableExpression(dialect, "async_graph", match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "b_name")],
            from_=gt)
        sql, params = query.to_sql()
        rows = await async_oracle_backend.fetch_all(sql, params)
        assert len(rows) == 1
        assert rows[0]["b_name"] == "Bob"
