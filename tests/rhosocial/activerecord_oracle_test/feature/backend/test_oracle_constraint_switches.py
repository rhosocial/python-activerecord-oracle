# tests/rhosocial/activerecord_oracle_test/feature/backend/test_oracle_constraint_switches.py
"""Oracle constraint capability switches: values and their locality.

Every verdict asserted here was measured against a live server by running the
statement and reading the error, or reading ``user_constraints`` back:

* **Oracle 21c XE 21.3.0.0.0** at ``127.0.0.1:11522/xepdb1`` (user ``system``),
  through oracledb thin -- the source of every verdict below;
* **18c and 23c**, cross-checked, identical for every value asserted here.

The docstring each switch carries records the same measurement, ORA- code
included, so a verdict can be re-checked without re-measuring the family.

Three things are asserted, and the last two are the ones that are easy to lose:

* **the value** -- what the server was measured to do;
* **the locality** -- that the verdict resolves to a class in *this* backend's
  package. A switch that falls through to core's ``ConstraintMixin`` can have
  its default moved by a commit in another repository with nothing failing
  here. Declaring it locally turns that into a visible test failure instead of
  a quiet contract change;
* **the evidence** -- that the docstring still quotes the ORA- code or the
  accepted spelling the value came from.

Two corrections this module exists to pin, both of which a "tidy up" pass would
otherwise reintroduce:

* ``supports_deferrable_constraint`` and every ``supports_deferrable_*``
  probe are **True**. An earlier round of documentation claimed Oracle
  "accepts DEFERRABLE but keeps it IMMEDIATE, writes it out but does not
  take effect". Live measurement disproves that: a violating INSERT inside the
  transaction succeeds and the failure moves to COMMIT
  (``ORA-02091`` wrapping ``ORA-02290``). See
  :meth:`supports_deferrable_constraint`.
* ``supports_constraint_novalidate`` is **False** even though Oracle has the
  capability. Oracle's keyword is ``NOVALIDATE`` and it works; the spelling
  core's shared renderer emits is PostgreSQL's ``NOT VALID``, and *that* is
  refused with ``ORA-01735: invalid ALTER TABLE option``. The switch answers
  the spelling, so it is ``False``.
"""

import pytest

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

#: The server the verdicts below were measured on.
MEASURED_VERSION = (21, 3, 0, 0)

#: Cross-checked releases. Every value here is a statement about all three,
#: not about 21c alone.
CROSS_CHECKED_VERSIONS = ((18, 0, 0, 0), (21, 3, 0, 0), (23, 0, 0, 0))

#: Every constraint switch this backend relies on, with the verdict measured on
#: 21c XE. This mapping is the contract; the locality tests below walk it so a
#: switch cannot leave the backend without failing first.
CONSTRAINT_SWITCHES = {
    # -- Constraint kinds (SQL-86 / SQL-92).
    "supports_primary_key_constraint": True,
    "supports_unique_constraint": True,
    "supports_not_null_constraint": True,
    "supports_check_constraint": True,
    "supports_foreign_key_constraint": True,
    "supports_exclude_constraint": False,
    # -- Referential actions (SQL-92) and match modes (SQL:1999).
    "supports_fk_on_delete": True,
    "supports_fk_on_update": False,
    "supports_fk_match": False,
    "supports_fk_referential_action_cascade": True,
    "supports_fk_referential_action_set_null": True,
    "supports_fk_referential_action_restrict": False,
    "supports_fk_referential_action_no_action": False,
    "supports_fk_referential_action_set_default": False,
    # -- Deferral (SQL:1999). True for EVERY kind -- see the module docstring.
    "supports_deferrable_constraint": True,
    "supports_deferrable_primary_key_constraint": True,
    "supports_deferrable_unique_constraint": True,
    "supports_deferrable_foreign_key_constraint": True,
    "supports_deferrable_check_constraint": True,
    "supports_deferrable_not_null_constraint": True,
    # EXCLUDE does not exist on Oracle at all, so it cannot be deferrable.
    "supports_deferrable_exclude_constraint": False,
    # -- Enforcement control (SQL:2016) and Oracle's own vocabulary.
    # ENFORCED / NOT ENFORCED do not exist; ENABLE/DISABLE and RELY do.
    "supports_constraint_enforced": False,
    "supports_alter_constraint_enforced": False,
    "supports_constraint_enable_disable": True,
    "supports_constraint_rely": True,
    # -- Validation state. Bare VALIDATE and bare NOT VALID are both refused;
    # Oracle's own NOVALIDATE keyword works (recorded on the docstring).
    "supports_validate_constraint": False,
    "supports_constraint_novalidate": False,
    # -- ALTER TABLE lifecycle (SQL:92).
    "supports_add_constraint": True,
    "supports_drop_constraint": True,
    "supports_drop_constraint_if_exists": False,
    "supports_rename_constraint": True,
    "supports_alter_constraint": False,
    "supports_enable_disable_constraint": True,
    # -- Backend-derived state clause.
    "supports_using_index_clause": True,
}

#: The substring that must appear in each switch's docstring: the ORA- code,
#: the accepted spelling, or the exact dictionary column read back. A verdict
#: that cannot be traced to an observation is a guess.
EVIDENCE = {
    "supports_primary_key_constraint": "user_constraints",
    "supports_unique_constraint": "UNIQUE (a)",
    "supports_not_null_constraint": "user_constraints",
    "supports_check_constraint": "ORA-02290",
    "supports_foreign_key_constraint": "ORA-02291",
    "supports_exclude_constraint": "ORA-00902",
    "supports_fk_on_delete": "delete_rule",
    "supports_fk_on_update": "ORA-00905",
    "supports_fk_match": "ORA-01735",
    "supports_fk_referential_action_cascade": "CASCADE",
    "supports_fk_referential_action_set_null": "SET NULL",
    "supports_fk_referential_action_restrict": "ORA-00905",
    "supports_fk_referential_action_no_action": "ORA-00905",
    "supports_fk_referential_action_set_default": "ORA-03001",
    "supports_deferrable_constraint": "ORA-02091",
    "supports_deferrable_primary_key_constraint": "DEFERRABLE",
    "supports_deferrable_unique_constraint": "DEFERRABLE",
    "supports_deferrable_foreign_key_constraint": "ORA-02291",
    "supports_deferrable_check_constraint": "ORA-02290",
    "supports_deferrable_not_null_constraint": "DEFERRABLE",
    "supports_deferrable_exclude_constraint": "ORA-00902",
    "supports_constraint_enforced": "ORA-01735",
    "supports_alter_constraint_enforced": "ORA-01735",
    "supports_constraint_enable_disable": "ENABLE NOVALIDATE",
    "supports_constraint_rely": "RELY",
    "supports_validate_constraint": "ORA-01735",
    "supports_constraint_novalidate": "ORA-01735",
    "supports_add_constraint": "ADD CONSTRAINT",
    "supports_drop_constraint": "DROP CONSTRAINT",
    "supports_drop_constraint_if_exists": "ORA-01735",
    "supports_rename_constraint": "RENAME CONSTRAINT",
    "supports_alter_constraint": "ORA-01735",
    "supports_enable_disable_constraint": "ENABLE NOVALIDATE",
    "supports_using_index_clause": "USING INDEX",
}

#: Every capability must come from this package.
BACKEND_PACKAGE = "rhosocial.activerecord.backend.impl.oracle."


def _dialect(version=MEASURED_VERSION):
    return OracleDialect(version=version)


def _declaring_class(switch_name):
    """The first class in the dialect's MRO that declares ``switch_name``."""
    for cls in OracleDialect.__mro__:
        if switch_name in cls.__dict__:
            return cls
    raise AssertionError(f"no class in the MRO declares {switch_name}()")


class TestMeasuredConstraintSwitchValues:
    """What the server was measured to do."""

    @pytest.mark.parametrize(
        "switch,expected",
        sorted(CONSTRAINT_SWITCHES.items()),
        ids=sorted(CONSTRAINT_SWITCHES),
    )
    def test_switch_answers_the_measured_value(self, switch, expected):
        dialect = _dialect()
        assert getattr(dialect, switch)() is expected, (
            f"{switch}() no longer matches the Oracle 21c XE 21.3.0.0.0 "
            f"measurement; see the docstring on "
            f"rhosocial.activerecord.backend.impl.oracle.mixins.features for "
            f"the statement that decided it."
        )

    @pytest.mark.parametrize(
        "switch,expected",
        sorted(CONSTRAINT_SWITCHES.items()),
        ids=sorted(CONSTRAINT_SWITCHES),
    )
    def test_cross_checked_releases_agree(self, switch, expected):
        """18c / 21c / 23c were measured and gave the same answer each time.

        Asserting the range here is the claim this backend has actually made:
        none of these values is version-gated, so no version branch may exist
        for them. A version-sensitive value would need its own measurement
        rather than riding along here.
        """
        for version in CROSS_CHECKED_VERSIONS:
            dialect = OracleDialect(version=version)
            assert getattr(dialect, switch)() is expected, f"{switch} moved with version {version}"


class TestConstraintSwitchesAreDeclaredLocally:
    """The verdict must be this backend's, not an inherited core default."""

    @pytest.mark.parametrize("switch", sorted(CONSTRAINT_SWITCHES), ids=sorted(CONSTRAINT_SWITCHES))
    def test_switch_is_declared_by_this_backend(self, switch):
        declaring = _declaring_class(switch)
        assert declaring.__module__.startswith(BACKEND_PACKAGE), (
            f"{switch} is inherited from {declaring.__module__}; declare it in "
            f"this backend's own package so a moved core default becomes a "
            f"loud failure instead of a silent change of what Oracle renders."
        )

    def test_backend_mixins_precede_the_core_constraint_mixin(self):
        """MRO order is what makes the local declarations win at all.

        Oracle's mixins have to precede core's ``ConstraintMixin``; if that
        ordering were disturbed the local values would silently lose to the
        inherited ones even though both classes define the switch.
        """
        from rhosocial.activerecord.backend.dialect.mixins.ddl_table import (
            ConstraintMixin,
        )

        from rhosocial.activerecord.backend.impl.oracle.mixins.features import (
            OracleFeaturesMixin,
        )

        mro = OracleDialect.__mro__
        assert mro.index(OracleFeaturesMixin) < mro.index(ConstraintMixin)


class TestConstraintSwitchDocstringsCarryTheMeasurement:
    """A probe without its evidence is a number with no source."""

    @pytest.mark.parametrize("switch", sorted(EVIDENCE), ids=sorted(EVIDENCE))
    def test_docstring_records_the_observation(self, switch):
        declaring = _declaring_class(switch)
        doc = declaring.__dict__[switch].__doc__ or ""
        assert doc, f"{switch} has no docstring: the measurement has nowhere to live"
        assert EVIDENCE[switch] in doc, (
            f"{switch}'s docstring no longer quotes {EVIDENCE[switch]!r}, the "
            f"observation this value came from."
        )

    @pytest.mark.parametrize("switch", sorted(EVIDENCE), ids=sorted(EVIDENCE))
    def test_docstring_names_a_measured_release(self, switch):
        """Every constraint verdict names the release it was measured on.

        Without the release a reader cannot tell whether a value is a
        long-standing property of Oracle or a fact about one instance, and
        the two call for different treatment when Oracle changes.
        """
        declaring = _declaring_class(switch)
        doc = declaring.__dict__[switch].__doc__ or ""
        assert any(
            release in doc for release in ("21c", "18c", "23c")
        ), f"{switch}'s docstring names no measured Oracle release."


class TestDeferrableIsHonouredNotAbsorbed:
    """The claim this pass exists to protect.

    An earlier documentation round recorded that Oracle "accepts DEFERRABLE
    but keeps it IMMEDIATE -- writes it out but does not take effect". That
    was wrong, and it was load-bearing wrong: a ``False`` here would refuse
    clauses the server genuinely honours. The measurement, on 21c XE and
    identical on 18c and 23c:

    1. ``ADD CONSTRAINT ... DEFERRABLE INITIALLY DEFERRED`` is accepted, and
       ``user_constraints`` records ``(DEFERRABLE, DEFERRED)``.
    2. A violating INSERT *inside the transaction succeeds*.
    3. ``COMMIT`` raises ``ORA-02091`` wrapping ``ORA-02290``, and the row is
       gone (``COUNT`` returns to 0).
    4. ``SET CONSTRAINT cf IMMEDIATE`` makes the same INSERT fail at once
       with ``ORA-02291``.

    Step 4 is what rules out the no-op reading: if Oracle ignored the clause
    there would be nothing for ``SET CONSTRAINT`` to switch back.
    """

    def test_deferrable_is_supported(self):
        assert _dialect().supports_deferrable_constraint() is True

    @pytest.mark.parametrize(
        "switch",
        (
            "supports_deferrable_check_constraint",
            "supports_deferrable_primary_key_constraint",
            "supports_deferrable_unique_constraint",
            "supports_deferrable_foreign_key_constraint",
            "supports_deferrable_not_null_constraint",
        ),
        ids=lambda s: s,
    )
    def test_every_constraint_kind_is_deferrable(self, switch):
        """Oracle has no narrower per-kind answer.

        ``constraint_state`` hangs outside the whole of ``inline_constraint``
        / ``out_of_line_constraint``, so every branch carries ``DEFERRABLE``.
        PostgreSQL is the backend that has to split this -- its server refuses
        CHECK (``0A000: CHECK constraints cannot be marked DEFERRABLE``) and
        NOT NULL (``42601: misplaced DEFERRABLE clause``) -- and a narrower
        Oracle declaration would be copying PostgreSQL's split onto a server
        that has none.
        """
        assert getattr(_dialect(), switch)() is True, (
            f"{switch} is False, but DEFERRABLE is measured accepted for that " f"kind on 21c XE."
        )

    def test_exclude_is_the_one_kind_without_deferrable(self):
        """Oracle has no EXCLUDE constraint, so it cannot be deferrable either."""
        assert _dialect().supports_deferrable_exclude_constraint() is False
        assert _dialect().supports_exclude_constraint() is False

    def test_deferrable_docstring_names_the_commit_time_failure(self):
        """The ``True`` has to carry the evidence that makes it a ``True``.

        Without the COMMIT-time codes in the docstring, the next reader has
        only the claim -- and the claim is exactly what an earlier round of
        documentation got wrong.
        """
        doc = (
            _declaring_class("supports_deferrable_constraint")
            .__dict__["supports_deferrable_constraint"]
            .__doc__
        )
        assert "ORA-02091" in doc
        assert "ORA-02290" in doc
        assert "SET CONSTRAINT" in doc

    def test_foreign_key_deferrable_docstring_carries_its_own_codes(self):
        """The FK deferral is the clearest proof, so its codes live on its probe."""
        doc = (
            _declaring_class("supports_deferrable_foreign_key_constraint")
            .__dict__["supports_deferrable_foreign_key_constraint"]
            .__doc__
        )
        assert "ORA-02091" in doc
        assert "ORA-02291" in doc
        assert "SET CONSTRAINT" in doc


class TestEnforcementVocabularyIsEnableDisableNotEnforced:
    """Oracle's answer to ``ENFORCED`` is a different clause, not a synonym."""

    def test_enforced_does_not_exist(self):
        assert _dialect().supports_constraint_enforced() is False

    def test_enable_disable_is_the_substitute(self):
        assert _dialect().supports_constraint_enable_disable() is True
        assert _dialect().supports_enable_disable_constraint() is True

    def test_rely_is_accepted(self):
        """``RELY`` is Oracle's query-rewrite hint, accepted on CHECK / UNIQUE / PK."""
        assert _dialect().supports_constraint_rely() is True

    def test_both_spellings_of_the_enable_disable_switch_agree(self):
        """The capability is reachable under two names; they must not drift.

        ``supports_constraint_enable_disable`` and
        ``supports_enable_disable_constraint`` are the same measurement under
        the two names the other backends and the coverage plan use. A rename
        in core S3 will collapse them; until then a divergence would mean one
        of the two no longer describes the server.
        """
        dialect = _dialect()
        assert (
            dialect.supports_constraint_enable_disable()
            is dialect.supports_enable_disable_constraint()
        )


class TestReferentialActionSurfaceIsTwoOfFive:
    """``ON DELETE`` exists; three of the SQL-standard five cannot be spelled."""

    def test_the_two_accepted_actions(self):
        dialect = _dialect()
        assert dialect.supports_fk_referential_action_cascade() is True
        assert dialect.supports_fk_referential_action_set_null() is True

    @pytest.mark.parametrize(
        "switch",
        (
            "supports_fk_referential_action_restrict",
            "supports_fk_referential_action_no_action",
            "supports_fk_referential_action_set_default",
        ),
        ids=lambda s: s,
    )
    def test_the_three_refused_actions(self, switch):
        assert getattr(_dialect(), switch)() is False

    def test_no_action_is_the_default_but_cannot_be_spelled(self):
        """The asymmetry that trips a caller who reads the dictionary back.

        A plain FK with no action clause records
        ``user_constraints.delete_rule = 'NO ACTION'``. Spelling it explicitly
        is refused with ``ORA-00905: missing keyword``. So a caller who wants
        Oracle's default must *omit* the clause -- asking for NO ACTION by name
        raises on a server that is already doing it.
        """
        assert _dialect().supports_fk_referential_action_no_action() is False

    def test_on_update_does_not_exist_at_all(self):
        """Every spelling fails at the ``UPDATE`` keyword itself."""
        dialect = _dialect()
        assert dialect.supports_fk_on_update() is False
        assert dialect.supports_foreign_key_on_update() is False


class TestValidationStateSpellings:
    """The bare forms are refused; ``ENABLE NOVALIDATE`` is not.

    Two separate refusals that read alike and mean different things:

    * ``ALTER TABLE t VALIDATE CONSTRAINT c`` -> ``ORA-01735``
    * ``ALTER TABLE t NOVALIDATE CONSTRAINT c`` -> ``ORA-01735``

    while ``ENABLE VALIDATE`` / ``ENABLE NOVALIDATE`` / ``MODIFY CONSTRAINT c
    NOVALIDATE`` are all accepted. And separately, Oracle's ADD-time keyword
    ``NOVALIDATE`` works while PostgreSQL's ``NOT VALID`` -- the literal core's
    shared renderer emits -- does not. That last asymmetry is why
    ``supports_constraint_novalidate`` is ``False`` even though the underlying
    capability exists.
    """

    def test_bare_validate_and_novalidate_are_refused(self):
        dialect = _dialect()
        assert dialect.supports_validate_constraint() is False
        assert dialect.supports_constraint_novalidate() is False

    def test_novalidate_docstring_records_that_oracle_can_do_it_anyway(self):
        """``False`` must not be read as "Oracle cannot skip validation".

        It can: ``ADD CONSTRAINT ck CHECK (...) NOVALIDATE`` is accepted with
        an already-violating row present and leaves ``validated =
        'NOT VALIDATED'``. The switch answers the ``NOT VALID`` spelling, so
        the docstring has to say so or the value reads as a missing feature.
        """
        doc = (
            _declaring_class("supports_constraint_novalidate")
            .__dict__["supports_constraint_novalidate"]
            .__doc__
        )
        assert "NOVALIDATE" in doc
        assert "NOT VALIDATED" in doc
        assert "ORA-01735" in doc


class TestAlterConstraintLifecycle:
    """``ALTER CONSTRAINT`` does not exist; ENABLE / DISABLE does."""

    def test_no_alter_constraint_statement(self):
        assert _dialect().supports_alter_constraint() is False
        assert _dialect().supports_alter_constraint_enforced() is False

    def test_the_lifecycle_verbs_that_exist(self):
        dialect = _dialect()
        assert dialect.supports_add_constraint() is True
        assert dialect.supports_drop_constraint() is True
        assert dialect.supports_rename_constraint() is True

    def test_drop_constraint_if_exists_does_not_exist(self):
        """A missing constraint raises ``ORA-02443`` instead; there is no IF EXISTS."""
        assert _dialect().supports_drop_constraint_if_exists() is False


class TestUsingIndexIsPerKindNotGlobal:
    """``USING INDEX`` is accepted for UNIQUE and PRIMARY KEY, refused for CHECK.

    A single boolean cannot express the gap, so this test documents that the
    probe is about the clause *existing* and that the refusal on CHECK has to
    happen by kind. Core's S3 may split it; this measurement has to survive
    the split.
    """

    def test_using_index_is_supported(self):
        assert _dialect().supports_using_index_clause() is True

    def test_check_cannot_carry_using_index(self):
        """``ORA-01735`` out of line; a caller must refuse this by kind."""
        doc = (
            _declaring_class("supports_using_index_clause")
            .__dict__["supports_using_index_clause"]
            .__doc__
        )
        assert "CHECK" in doc
        assert "ORA-01735" in doc


class TestRenderedDdlStillMatchesTheSwitches:
    """The answer has to decide the outcome, not merely be recorded.

    A switch that is measured correctly but not consulted renders SQL the
    server refuses. These are the cheap end-to-end checks: the ADD, DROP and
    RENAME forms the three lifecycle switches gate.
    """

    def test_add_table_constraint_renders(self):
        from rhosocial.activerecord.backend.expression import (
            Column,
            ComparisonPredicate,
        )
        from rhosocial.activerecord.backend.expression.statements import (
            AddTableConstraintAction,
            TableCheckConstraint,
            TableConstraint,
        )

        dialect = _dialect()
        # Column-to-column, so the rendered CHECK carries no bind parameter --
        # a DDL predicate with a bind is not something the server would take.
        predicate = ComparisonPredicate(dialect, ">", Column(dialect, "a"), Column(dialect, "b"))
        action = AddTableConstraintAction(
            dialect,
            TableConstraint(
                dialect, branch=TableCheckConstraint(dialect, predicate), name="CK"
            ),
        )
        sql, params = action.to_sql()
        assert sql.startswith("ADD CONSTRAINT ")
        assert "CHECK" in sql
        assert params == ()

    def test_drop_table_constraint_renders(self):
        from rhosocial.activerecord.backend.expression.statements import (
            DropTableConstraintAction,
        )

        dialect = _dialect()
        sql, params = DropTableConstraintAction(dialect, "CK").to_sql()
        assert "DROP CONSTRAINT" in sql
        assert "IF EXISTS" not in sql
        assert params == ()

    def test_rename_constraint_has_no_core_render_path_yet(self):
        """Documents the gap instead of pretending it is closed.

        Oracle accepts ``ALTER TABLE t RENAME CONSTRAINT old TO new`` (measured
        on 21c XE), and :meth:`supports_rename_constraint` is ``True``. Core
        exposes no node or formatter for the operation, though -- there is no
        ``RenameTableConstraint`` in ``expression.statements`` and no
        ``format_rename_constraint`` in the dialect mixins -- so the clause
        cannot be rendered yet. The switch is correct; the render path is the
        missing half, and this test is here so the gap is visible rather than
        discovered by a caller.
        """
        import rhosocial.activerecord.backend.expression.statements as statements

        assert _dialect().supports_rename_constraint() is True
        assert not hasattr(statements, "RenameTableConstraint")
        assert not hasattr(_dialect(), "format_rename_constraint")

    def test_deferred_clause_is_rendered_when_requested(self):
        """``supports_deferrable_constraint`` is True, so DEFERRABLE is rendered."""
        from rhosocial.activerecord.backend.expression.statements import (
            TableConstraint,
            TableUniqueConstraint,
        )

        dialect = _dialect()
        # The deferral trait rides the SQL:2016 kinds only -- PRIMARY KEY,
        # UNIQUE and FOREIGN KEY -- which is core's whitelist and not this
        # test's to widen; this test is about the trait being rendered.
        constraint = TableConstraint(
            dialect,
            branch=TableUniqueConstraint(dialect, ["a"]),
            name="CK",
            deferrable=True,
            initially_deferred=True,
        )
        sql, _params = dialect.format_table_constraint(constraint)
        assert "DEFERRABLE" in sql
        assert "INITIALLY DEFERRED" in sql

    def test_deferrable_is_refused_by_name_when_the_backend_says_no(self):
        """The gate is real: flipping the probe refuses the clause by name.

        This is what makes the ``True`` above load-bearing rather than
        decorative -- the render path consults the switch, so a wrong value
        changes behaviour rather than sitting in a docstring.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )
        from rhosocial.activerecord.backend.expression.statements import (
            TableConstraint,
            TableUniqueConstraint,
        )

        dialect = _dialect()
        constraint = TableConstraint(
            dialect,
            branch=TableUniqueConstraint(dialect, ["a"]),
            name="CK",
            deferrable=True,
        )
        dialect.supports_deferrable_constraint = lambda: False
        with pytest.raises(UnsupportedFeatureError):
            dialect.format_table_constraint(constraint)

    def test_not_enforced_is_refused_by_name(self):
        """``supports_constraint_enforced`` is False, so the clause raises."""
        from rhosocial.activerecord.backend.expression.core import Literal
        from rhosocial.activerecord.backend.expression.statements import (
            TableCheckConstraint,
            TableConstraint,
        )

        dialect = _dialect()
        constraint = TableConstraint(
            dialect,
            branch=TableCheckConstraint(dialect, Literal(dialect, 1)),
            name="CK",
            not_enforced=True,
        )
        with pytest.raises(Exception) as excinfo:
            dialect.format_table_constraint(constraint)
        assert "ENFORCED" in str(excinfo.value)
