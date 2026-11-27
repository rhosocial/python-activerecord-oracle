# tests/rhosocial/activerecord_oracle_test/feature/backend/construct/ddl/test_constraint_state_actions.py
"""Oracle constraint-state ALTER TABLE actions.

Oracle changes a constraint's enforcement and validation state with
``ENABLE`` / ``DISABLE`` clauses and with ``MODIFY CONSTRAINT``; there is no
``ALTER CONSTRAINT`` to carry the SQL-standard ``ENFORCED`` vocabulary. All
forms below are measured on 21c XE; the bare ``VALIDATE CONSTRAINT`` clause is
refused (``ORA-01735``), which is why validation is only reachable through these
two prefixes.

Pure-construction tests: no database connection is required.
"""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import AlterTableStatement
from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.expression import (
    OracleDisableConstraintAction,
    OracleEnableConstraintAction,
    OracleModifyConstraintAction,
)


@pytest.fixture
def dialect():
    return OracleDialect((21, 3, 0))


def _render(dialect, action):
    return AlterTableStatement(dialect, Table(dialect, "t"), action).to_sql()


class TestEnableConstraint:
    def test_plain(self, dialect):
        sql, params = _render(dialect, OracleEnableConstraintAction(dialect, "ck"))
        assert sql == 'ALTER TABLE "T" ENABLE CONSTRAINT "CK"'
        assert params == ()

    def test_validate(self, dialect):
        sql, _ = _render(
            dialect, OracleEnableConstraintAction(dialect, "ck", validate=True)
        )
        assert sql == 'ALTER TABLE "T" ENABLE VALIDATE CONSTRAINT "CK"'

    def test_novalidate(self, dialect):
        sql, _ = _render(
            dialect, OracleEnableConstraintAction(dialect, "ck", novalidate=True)
        )
        assert sql == 'ALTER TABLE "T" ENABLE NOVALIDATE CONSTRAINT "CK"'

    def test_both_switches_are_refused(self, dialect):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _render(
                dialect,
                OracleEnableConstraintAction(
                    dialect, "ck", validate=True, novalidate=True
                ),
            )

    def test_blank_name_is_refused(self, dialect):
        with pytest.raises(ValueError, match="non-empty constraint name"):
            _render(dialect, OracleEnableConstraintAction(dialect, " "))


class TestDisableConstraint:
    def test_plain(self, dialect):
        sql, _ = _render(dialect, OracleDisableConstraintAction(dialect, "ck"))
        assert sql == 'ALTER TABLE "T" DISABLE CONSTRAINT "CK"'

    def test_novalidate(self, dialect):
        sql, _ = _render(
            dialect, OracleDisableConstraintAction(dialect, "ck", novalidate=True)
        )
        assert sql == 'ALTER TABLE "T" DISABLE NOVALIDATE CONSTRAINT "CK"'

    def test_both_switches_are_refused(self, dialect):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _render(
                dialect,
                OracleDisableConstraintAction(
                    dialect, "ck", validate=True, novalidate=True
                ),
            )


class TestModifyConstraint:
    def test_validate_is_the_default(self, dialect):
        sql, params = _render(dialect, OracleModifyConstraintAction(dialect, "ck"))
        assert sql == 'ALTER TABLE "T" MODIFY CONSTRAINT "CK" VALIDATE'
        assert params == ()

    def test_novalidate(self, dialect):
        sql, _ = _render(
            dialect, OracleModifyConstraintAction(dialect, "ck", novalidate=True)
        )
        assert sql == 'ALTER TABLE "T" MODIFY CONSTRAINT "CK" NOVALIDATE'

    def test_blank_name_is_refused(self, dialect):
        with pytest.raises(ValueError, match="non-empty constraint name"):
            _render(dialect, OracleModifyConstraintAction(dialect, "  "))


class TestCapabilityGates:
    def test_enable_uses_the_measured_probe(self, dialect):
        assert dialect.supports_enable_disable_constraint() is True
        assert dialect.supports_constraint_enable_disable() is True

    def test_bare_validate_is_not_advertised(self, dialect):
        assert dialect.supports_validate_constraint() is False

    def test_actions_refuse_when_the_probe_is_withdrawn(self, dialect, monkeypatch):
        monkeypatch.setattr(
            dialect, "supports_enable_disable_constraint", lambda: False
        )
        monkeypatch.setattr(
            dialect, "supports_constraint_enable_disable", lambda: False
        )
        for action in (
            OracleEnableConstraintAction(dialect, "ck"),
            OracleDisableConstraintAction(dialect, "ck"),
            OracleModifyConstraintAction(dialect, "ck"),
        ):
            with pytest.raises(UnsupportedFeatureError):
                _render(dialect, action)
