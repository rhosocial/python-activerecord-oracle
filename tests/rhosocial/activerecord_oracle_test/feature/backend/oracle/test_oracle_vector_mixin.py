# tests/rhosocial/activerecord_oracle_test/feature/backend/oracle/test_oracle_vector_mixin.py
"""Tests for the Oracle vector dialect mixin.

Covers ``OracleVectorMixin`` (``mixins/vector.py``): capability flags
(``supports_vector_type`` / ``supports_vector_index`` /
``get_max_vector_dimension`` / ``supports_vector_distance_metric``), the
``format_vector_literal`` literal formatter and the
``format_vector_distance`` / ``format_vector_operand`` expression
formatters used to build ``VECTOR_DISTANCE(...)`` SQL.

Pure-construction tests: no database connection is required. The mixin is
exercised both through the assembled ``OracleDialect`` and as a standalone
instance so that no method is shadowed by a sibling mixin.
"""

import oracledb
import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.mixins.vector import OracleVectorMixin


class _ToStringVector:
    """Fake vector value exposing only ``to_string``."""

    def to_string(self):
        """Return the canned vector string literal."""
        return "[1.5, 2.5]"


class _FakeOracleVector:
    """Stand-in for ``oracledb.Vector`` used to exercise the isinstance branch."""


@pytest.fixture
def dialect():
    """Return a 23ai Oracle dialect with vector support."""
    return OracleDialect(version=(23, 4, 0))


@pytest.fixture
def mixin():
    """Return a standalone ``OracleVectorMixin`` instance."""
    return OracleVectorMixin()


class TestOracleVectorCapabilities:
    """Capability flags exposed by the vector mixin."""

    def test_supports_vector_type_on_23ai(self, dialect):
        """Vector support is enabled on Oracle 23ai+."""
        assert dialect.supports_vector_type() is True, "23ai dialect must support VECTOR"

    def test_supports_vector_type_disabled_on_older_versions(self):
        """Vector support is disabled below Oracle 23c."""
        old = OracleDialect(version=(19, 0, 0))
        assert old.supports_vector_type() is False, "pre-23 servers must not report VECTOR support"

    def test_supports_vector_type_default_version(self, mixin):
        """Standalone mixin defaults to 23ai when no version attribute is set."""
        assert mixin.supports_vector_type() is True, "missing version must fall back to 23ai"

    def test_supports_vector_type_with_explicit_old_version(self, mixin):
        """Standalone mixin honours an explicit pre-23 version."""
        mixin.version = (19, 0, 0)
        assert mixin.supports_vector_type() is False, "version (19, 0, 0) must disable VECTOR"

    def test_supports_vector_index(self, dialect):
        """VECTOR indexes are always reported as supported."""
        assert dialect.supports_vector_index() is True, "Oracle 23ai supports vector indexes"

    def test_max_vector_dimension(self, mixin):
        """The maximum VECTOR dimension constant is 65535."""
        assert mixin.get_max_vector_dimension() == 65535, "MAX_VECTOR_DIMENSION must be 65535"
        assert OracleVectorMixin.MAX_VECTOR_DIMENSION == 65535, "class constant must match"

    def test_supported_distance_metrics(self, mixin):
        """The supported metric set matches the documented constants."""
        expected = {"COSINE", "EUCLIDEAN", "DOT", "MANHATTAN", "HAMMING"}
        assert set(OracleVectorMixin.SUPPORTED_DISTANCE_METRICS) == expected, (
            "SUPPORTED_DISTANCE_METRICS must list the five Oracle metrics"
        )

    def test_supports_distance_metric_case_insensitive(self, mixin):
        """Metric names are accepted case-insensitively."""
        assert mixin.supports_vector_distance_metric("cosine") is True, "lowercase metric must match"
        assert mixin.supports_vector_distance_metric("Euclidean") is True, "mixed-case must match"

    def test_supports_distance_metric_rejects_unknown(self, mixin):
        """Unknown metric names are rejected."""
        assert mixin.supports_vector_distance_metric("L1") is False, "L1 is not supported"

    def test_supports_distance_metric_rejects_non_string(self, mixin):
        """Non-string metric arguments are rejected."""
        assert mixin.supports_vector_distance_metric(42) is False, "int metric must be rejected"


class TestFormatVectorLiteral:
    """The ``format_vector_literal`` SQL string formatter."""

    def test_none_becomes_null(self, mixin):
        """None is rendered as the NULL keyword."""
        assert mixin.format_vector_literal(None) == "NULL", "None must map to NULL"

    def test_object_with_to_string(self, mixin):
        """Objects exposing ``to_string`` are delegated to."""
        result = mixin.format_vector_literal(_ToStringVector())
        assert result == "[1.5, 2.5]", "to_string() output must be used verbatim"

    def test_oracledb_vector_instance(self, mixin, monkeypatch):
        """``oracledb.Vector`` instances render through ``str``."""
        monkeypatch.setattr(oracledb, "Vector", _FakeOracleVector, raising=False)
        fake = _FakeOracleVector()
        assert mixin.format_vector_literal(fake) == str(fake), "oracledb.Vector must use str()"

    def test_list_renders_bracketed(self, mixin):
        """Python lists render as a comma-joined bracket literal."""
        assert mixin.format_vector_literal([1.0, 2.0]) == "[1.0,2.0]", "list must join with commas"

    def test_tuple_renders_bracketed(self, mixin):
        """Python tuples render identically to lists."""
        assert mixin.format_vector_literal((3, 4)) == "[3,4]", "tuple must join with commas"

    def test_string_passthrough(self, mixin):
        """Plain strings are returned unchanged."""
        assert mixin.format_vector_literal("[9, 8]") == "[9, 8]", "string literal must pass through"

    def test_unsupported_type_raises(self, mixin):
        """A scalar value that is not vector-like raises TypeError."""
        with pytest.raises(TypeError, match="Cannot format vector literal"):
            mixin.format_vector_literal(123), "int cannot be formatted as a vector literal"


class TestFormatVectorDistance:
    """The ``format_vector_distance`` SQL + params builder."""

    def test_dict_form_default_metric(self, dialect):
        """A dict operand pair defaults to the COSINE metric."""
        sql, params = dialect.format_vector_distance({"vector1": "[1, 2]", "vector2": "[3, 4]"})
        assert sql == "VECTOR_DISTANCE(%s, %s, 'COSINE')", "default metric must be COSINE"
        assert params == ("[1, 2]", "[3, 4]"), "operands must be bound as parameters"

    def test_dict_form_uppercases_metric(self, dialect):
        """A dict metric is uppercased in the emitted SQL."""
        sql, params = dialect.format_vector_distance(
            {"vector1": "[1, 2]", "vector2": "[3, 4]", "metric": "cosine"}
        )
        assert sql == "VECTOR_DISTANCE(%s, %s, 'COSINE')", "metric must be uppercased"

    def test_attribute_form(self, dialect):
        """Objects exposing vector1/vector2/metric attributes are supported."""

        class _Expr:
            vector1 = "[1, 2]"
            vector2 = "[3, 4]"
            metric = "euclidean"

        sql, params = dialect.format_vector_distance(_Expr())
        assert sql == "VECTOR_DISTANCE(%s, %s, 'EUCLIDEAN')", "attribute metric must be used"
        assert params == ("[1, 2]", "[3, 4]"), "attribute operands must be bound"

    def test_attribute_form_empty_metric_defaults(self, dialect):
        """An empty attribute metric falls back to COSINE."""
        class _Expr:
            vector1 = "[1, 2]"
            vector2 = "[3, 4]"
            metric = ""

        sql, params = dialect.format_vector_distance(_Expr())
        assert sql == "VECTOR_DISTANCE(%s, %s, 'COSINE')", "empty metric must fall back to COSINE"

    def test_to_string_operands(self, dialect):
        """Operands exposing ``to_string`` are bound as their string form."""
        sql, params = dialect.format_vector_distance(
            {"vector1": _ToStringVector(), "vector2": [1, 2], "metric": "DOT"}
        )
        assert sql == "VECTOR_DISTANCE(%s, %s, 'DOT')", "to_string operands must use placeholders"
        assert params == ("[1.5, 2.5]", "[1,2]"), "both operand forms must be bound"

    def test_none_operands(self, dialect):
        """None operands render as NULL and contribute no parameters."""
        sql, params = dialect.format_vector_distance({"vector1": None, "vector2": None})
        assert sql == "VECTOR_DISTANCE(NULL, NULL, 'COSINE')", "None operands must inline NULL"
        assert params == (), "no parameters are expected for None operands"

    def test_unsupported_metric_raises(self, dialect):
        """An unsupported metric raises UnsupportedFeatureError."""
        with pytest.raises(UnsupportedFeatureError, match="VECTOR distance metric"):
            dialect.format_vector_distance(
                {"vector1": "[1, 2]", "vector2": "[3, 4]", "metric": "L1"}
            ), "L1 must be rejected"

    def test_unsupported_metric_raises_standalone(self, mixin):
        """The standalone mixin raises with its own name attribute."""
        mixin.name = "Oracle"
        with pytest.raises(UnsupportedFeatureError, match="Oracle"):
            mixin.format_vector_distance(
                {"vector1": "[1, 2]", "vector2": "[3, 4]", "metric": "L1"}
            ), "error must reference the backend name"


class TestFormatVectorOperand:
    """The ``format_vector_operand`` placeholder builder."""

    def test_none_operand(self, mixin):
        """None renders as NULL and appends no parameter."""
        params = []
        sql = mixin.format_vector_operand(None, params)
        assert sql == "NULL", "None must inline NULL"
        assert params == [], "None must not append a parameter"

    def test_string_operand(self, mixin):
        """A string operand is bound as a parameter."""
        params = []
        sql = mixin.format_vector_operand("[1, 2]", params)
        assert sql == "%s", "string operands must use the placeholder"
        assert params == ["[1, 2]"], "string operand must be appended"

    def test_to_string_operand(self, mixin):
        """A to_string operand is bound as its string form."""
        params = []
        sql = mixin.format_vector_operand(_ToStringVector(), params)
        assert sql == "%s", "to_string operands must use the placeholder"
        assert params == ["[1.5, 2.5]"], "to_string output must be appended"

    def test_list_operand(self, mixin):
        """A list operand is bound as a bracketed string."""
        params = []
        sql = mixin.format_vector_operand([1, 2, 3], params)
        assert sql == "%s", "list operands must use the placeholder"
        assert params == ["[1,2,3]"], "bracketed list must be appended"

    def test_arbitrary_operand(self, mixin):
        """Any other object is bound as-is."""
        params = []
        sql = mixin.format_vector_operand(object(), params)
        assert sql == "%s", "arbitrary operands must use the placeholder"
        assert len(params) == 1, "one parameter must be appended"
        assert isinstance(params[0], object), "arbitrary operand must pass through unchanged"