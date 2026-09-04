# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/test_sqlxml_support.py
"""Tests for Oracle SQL/XML standard support."""

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


class TestOracleSQLXMLSupport:
    """Test Oracle SQL/XML capability declarations."""

    def test_standard_sqlxml_support_methods(self):
        """Oracle exposes supported SQL/XML standard capabilities."""
        dialect = OracleDialect((9, 2, 0))

        assert dialect.supports_xmlparse() is False
        assert dialect.supports_xmlserialize() is True
        assert dialect.supports_xmlelement() is True
        assert dialect.supports_xmlattributes() is True
        assert dialect.supports_xmlforest() is True
        assert dialect.supports_xmlconcat() is True
        assert dialect.supports_xmlcomment() is True
        assert dialect.supports_xmlpi() is True
        assert dialect.supports_xmlroot() is True
        assert dialect.supports_xmlagg() is True
        assert dialect.supports_xmlquery() is True
        assert dialect.supports_xmlexists() is True
        assert dialect.supports_xmltable() is True

    def test_sqlxml_querying_requires_oracle_9_2(self):
        """Oracle XQuery table/query features require Oracle 9.2+."""
        dialect = OracleDialect((9, 0, 0))

        assert dialect.supports_xmlquery() is False
        assert dialect.supports_xmlexists() is False
        assert dialect.supports_xmltable() is False

    def test_standard_sqlxml_constructors_are_not_plain_functions(self):
        """Standard SQL/XML constructors are exposed as expression capabilities."""
        dialect = OracleDialect((19, 0, 0))
        functions = dialect.supports_functions()

        assert "xmltype" in functions
        assert "existsnode" in functions
        assert "xmltransform" in functions
        assert "xmlelement" not in functions
        assert "xmlforest" not in functions
        assert "xmlagg" not in functions
        assert "xmlquery" not in functions
        assert "xmltable" not in functions
        assert "xmlpi" not in functions
        assert "xmlroot" not in functions
        assert "xmlserialize" not in functions
