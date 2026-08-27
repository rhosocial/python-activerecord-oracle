# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_pgq_format.py
"""
Tests for Oracle PGQ dialect version gating and SQL formatting.

Uses requires_protocol markers for protocol-level capability documentation.
"""
import pytest
from rhosocial.activerecord.backend.dialect.protocols import GraphSupport, GraphTableSupport
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.expression import (
    GraphVertex, GraphEdge, GraphEdgeDirection, MatchClause,
    GraphColumn, ColumnsClause, GraphTableExpression,
    TablePropertiesClause, VertexTable, EdgeTable,
    CreatePropertyGraphExpression, DropPropertyGraphExpression,
    AlterPropertyGraphExpression,
)
from rhosocial.activerecord.backend.expression.query_parts import WhereClause
from rhosocial.activerecord.backend.expression.core import Column, Literal
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class TestPGQProtocolVersionGating:
    """Version gating tests for PGQ support in Oracle."""

    def test_supports_graph_match_oracle_23c(self):
        d = OracleDialect((23, 0, 0))
        assert d.supports_graph_match() is True

    @pytest.mark.requires_protocol((GraphSupport, "supports_graph_match"))
    def test_supports_graph_match_oracle_12c(self):
        d = OracleDialect((12, 0, 0))
        assert d.supports_graph_match() is True  # PG features since 12c

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_supports_graph_table_oracle_23c(self):
        d = OracleDialect((23, 0, 0))
        assert d.supports_graph_table() is True

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_supports_graph_table_oracle_19c(self):
        d = OracleDialect((19, 0, 0))
        assert d.supports_graph_table() is False

    @pytest.mark.requires_protocol((GraphSupport, "supports_graph_match"))
    def test_protocols_implemented(self):
        d = OracleDialect((23, 0, 0))
        assert isinstance(d, GraphSupport)
        assert isinstance(d, GraphTableSupport)


@pytest.fixture
def o23c_dialect():
    return OracleDialect((23, 0, 0))


class TestPGQGraphVertexFormat:
    """SQL formatting tests for GraphVertex with Oracle 23c dialect."""

    def test_basic(self, o23c_dialect: OracleDialect):
        v = GraphVertex(o23c_dialect, "p", "person")
        sql, params = v.to_sql()
        assert "(p IS" in sql
        assert params == ()

    def test_with_where(self, o23c_dialect: OracleDialect):
        where = WhereClause(o23c_dialect,
                            condition=Column(o23c_dialect, "age") > Literal(o23c_dialect, 18))
        v = GraphVertex(o23c_dialect, "p", "person", where=where)
        sql, params = v.to_sql()
        assert "WHERE" in sql
        assert params == (18,)


class TestPGQGraphEdgeFormat:
    """SQL formatting tests for GraphEdge with Oracle 23c dialect."""

    def test_right(self, o23c_dialect: OracleDialect):
        e = GraphEdge(o23c_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        assert e.to_sql()[0] == '-[e IS "KNOWS"]->'

    def test_anonymous(self, o23c_dialect: OracleDialect):
        e = GraphEdge(o23c_dialect, direction=GraphEdgeDirection.RIGHT)
        assert e.to_sql()[0] == '-[]->'


class TestPGQGraphTableFormat:
    """SQL formatting tests for GraphTableExpression with Oracle 23c dialect."""

    def test_basic(self, o23c_dialect: OracleDialect):
        v = GraphVertex(o23c_dialect, "p", "person")
        cols = ColumnsClause(o23c_dialect, GraphColumn("p", "name"))
        m = MatchClause(o23c_dialect, v)
        gt = GraphTableExpression(o23c_dialect, "g", m, cols)
        sql, params = gt.to_sql()
        assert "GRAPH_TABLE" in sql.upper()
        assert "COLUMNS" in sql.upper()

    def test_with_where(self, o23c_dialect: OracleDialect):
        where = WhereClause(o23c_dialect,
                            condition=Column(o23c_dialect, "age") > Literal(o23c_dialect, 18))
        v = GraphVertex(o23c_dialect, "p", "person", where=where)
        cols = ColumnsClause(o23c_dialect, GraphColumn("p", "name"))
        m = MatchClause(o23c_dialect, v)
        gt = GraphTableExpression(o23c_dialect, "g", m, cols)
        sql, params = gt.to_sql()
        assert "WHERE" in sql
        assert params == (18,)


class TestPGQDDLFormat:
    """SQL formatting tests for PGQ DDL with Oracle 23c dialect."""

    def test_create_property_graph(self, o23c_dialect: OracleDialect):
        vt = VertexTable(o23c_dialect, "people",
                         labels=["person"],
                         key_columns=["id"],
                         properties=TablePropertiesClause(o23c_dialect, columns=["id", "name"]))
        et = EdgeTable(o23c_dialect, "knows", ["person_a"], ["person_b"],
                       labels=["knows"],
                       properties=TablePropertiesClause(o23c_dialect, columns=["since"]))
        expr = CreatePropertyGraphExpression(o23c_dialect, "test_graph", [vt], [et])
        sql, params = expr.to_sql()
        assert "CREATE PROPERTY GRAPH" in sql.upper()
        assert "SOURCE KEY" in sql.upper()
        assert "DESTINATION KEY" in sql.upper()

    def test_drop_property_graph(self, o23c_dialect: OracleDialect):
        expr = DropPropertyGraphExpression(o23c_dialect, "test_graph", if_exists=True)
        sql, params = expr.to_sql()
        assert "IF EXISTS" in sql.upper()

    def test_alter_add_vertex(self, o23c_dialect: OracleDialect):
        vt = VertexTable(o23c_dialect, "new_table", labels=["NewLabel"])
        expr = AlterPropertyGraphExpression(o23c_dialect, "g", "ADD", "VERTEX TABLES",
                                            vertex_tables=[vt])
        sql, params = expr.to_sql()
        assert "ALTER PROPERTY GRAPH" in sql.upper()
        assert "ADD" in sql.upper()


class TestPGQUnsupportedFormat:
    """Tests that pre-23c Oracle raises errors for PGQ formatting.

    Note:
        - supports_graph_match() returns True for Oracle >= 12c
          (per dialect.py:507-509), so MATCH clause (GraphVertex /
          GraphEdge) is supported on 19c. Tests that expect MATCH to
        - supports_graph_table() returns True only for Oracle >= 23c
          (per dialect.py:511-513), so GRAPH_TABLE Expression raises
            UnsupportedFeatureError on versions older than 23c.
    """

    @pytest.fixture
    def oracle_11g_dialect(self):
        # 11g is below the 12c threshold for supports_graph_match,
        # so GraphVertex / GraphEdge formatting must raise.
        return OracleDialect((11, 0, 0))

    @pytest.fixture
    def oracle_19c_dialect(self):
        return OracleDialect((19, 0, 0))

    def test_graph_vertex_unsupported(self, oracle_11g_dialect: OracleDialect):
        v = GraphVertex(oracle_11g_dialect, "p", "person")
        with pytest.raises(UnsupportedFeatureError):
            v.to_sql()

    def test_graph_edge_unsupported(self, oracle_11g_dialect: OracleDialect):
        e = GraphEdge(oracle_11g_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        with pytest.raises(UnsupportedFeatureError):
            e.to_sql()

    def test_graph_table_unsupported(self, oracle_19c_dialect: OracleDialect):
        v = GraphVertex(oracle_19c_dialect, "p", "person")
        cols = ColumnsClause(oracle_19c_dialect, GraphColumn("p", "name"))
        m = MatchClause(oracle_19c_dialect, v)
        gt = GraphTableExpression(oracle_19c_dialect, "g", m, cols)
        with pytest.raises(UnsupportedFeatureError):
            gt.to_sql()
