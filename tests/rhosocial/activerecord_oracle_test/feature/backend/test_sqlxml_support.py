# tests/rhosocial/activerecord_oracle_test/feature/backend/test_sqlxml_support.py
"""Tests for Oracle SQL/XML standard support."""

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect


class TestOracleSQLXMLSupport:
    """Test Oracle SQL/XML capability declarations."""

    def test_standard_sqlxml_support_methods(self):
        """Oracle exposes supported SQL/XML standard capabilities."""
        dialect = OracleDialect((10, 2, 0))

        assert dialect.supports_xmlparse() is True
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

    def test_xmlparse_requires_oracle_10_2(self):
        """``XMLPARSE`` enters the Oracle SQL Reference in 10.2, not 9.2/10.1.

        The SQL function list has no ``XMLPARSE`` entry in 9.2 or 10.1; it
        appears in 10.2 along with the rest of the SQL:2005 set. This used to
        be pinned ``False`` for every version while the vendor documents the
        function -- the contradiction this gate resolves.
        """
        assert OracleDialect((10, 1, 0)).supports_xmlparse() is False
        assert OracleDialect((10, 2, 0)).supports_xmlparse() is True

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


class TestOracleXMLParseAgainstTheServer:
    """The vendor-doc contradiction, resolved by measurement.

    ``XMLParse`` is documented in Oracle's SQL Reference from 10.2, yet the
    probe used to answer ``False`` for every version. This executes the SQL the
    core renderer emits -- ``XMLPARSE(DOCUMENT ?)`` nested in
    ``XMLSERIALIZE(... AS VARCHAR2)`` -- against the configured server, so the
    probe is pinned to the server's answer rather than to documentation alone.

    The serialization around the parse is deliberate: an ``XMLType`` result
    comes back from the driver as a LOB whose adaptation this backend does not
    currently handle, so asking the server to serialize proves the parse ran
    without depending on that separate limitation.
    """

    def test_core_rendered_xmlparse_executes(self, oracle_backend):
        from rhosocial.activerecord.backend.expression import functions as F
        from rhosocial.activerecord.backend.options import ExecutionOptions
        from rhosocial.activerecord.backend.schema import StatementType

        dialect = oracle_backend.dialect
        # Every configured server is above the 10.2 floor, so the probe is on
        # and the renderer emits the statement instead of refusing it.
        assert dialect.supports_xmlparse() is True

        for document in (True, False):
            serialized = F.xmlserialize(
                dialect,
                F.xmlparse(dialect, "<root/>", document=document),
                "VARCHAR2(100)",
            )
            sql, params = serialized.to_sql()
            result = oracle_backend.execute(
                f'SELECT {sql} AS "XML_TEXT" FROM DUAL',
                params,
                options=ExecutionOptions(stmt_type=StatementType.SELECT),
            )

            assert len(result.data) == 1
            assert result.data[0]["xml_text"] == "<root/>"
