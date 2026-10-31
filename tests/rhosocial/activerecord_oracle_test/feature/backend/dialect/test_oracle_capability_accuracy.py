# tests/rhosocial/activerecord_oracle_test/feature/backend/dialect/test_oracle_capability_accuracy.py
"""Oracle capability accuracy: statements a live server refused or honoured.

Two kinds of test live here.  The first is a plain accuracy assertion --
the declaration below is one an imprecise reading of the SQL standard would
have gotten wrong.  The rest pin every constraint capability that was
measured against a live Oracle server (21c XE 21.3.0.0.0, cross-checked
identical on 18c and 23c) so that a later "fix" cannot quietly flip a probe
to the value the standard implies instead of the one the parser enforces.

Each expected value carries the exact ORA- code that produced it, so a
failure names the server's answer rather than a guess.
"""

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect

# Every release probed. The answers do not depend on the version, so the
# assertions hold for all of them.
VERSIONS = ((11, 0, 0), (18, 0, 0), (21, 0, 0), (23, 0, 0))


def test_temporal_tables_unsupported():
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_temporal_tables() is False


def test_deferrable_constraint_is_supported_not_a_noop():
    """DEFERRABLE is genuinely honoured, so the probe must stay True.

    A naive "correct it" pass would flip this to False on the theory that
    Oracle accepts the clause and ignores it.  The measurement says
    otherwise: ``user_constraints`` records ``DEFERRABLE`` / ``DEFERRED``,
    a violating INSERT inside the transaction *succeeds*, and COMMIT raises
    ``ORA-02091`` wrapping ``ORA-02290`` -- the check ran, just at COMMIT
    time.  ``SET CONSTRAINT c IMMEDIATE`` then makes the same INSERT fail
    immediately with ``ORA-02290``.  The first attempt pinned the behaviour
    this probe is not allowed to lose.
    """
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_deferrable_constraint() is True


def test_deferrable_constraint_supported_on_every_release():
    for version in ((11, 0, 0), (18, 0, 0), (19, 0, 0), (21, 0, 0), (23, 0, 0)):
        assert OracleDialect(version=version).supports_deferrable_constraint() is True


def test_no_on_update_referential_action():
    """There is no ``ON UPDATE`` branch at all -- the failure is at UPDATE.

    Measured on 18c / 21c XE / 23c: ``ON UPDATE CASCADE`` and
    ``ON UPDATE RESTRICT`` are refused at the ``UPDATE`` keyword with
    ``ORA-00905: missing keyword`` (18c/21c) and ``ORA-02000: missing DELETE
    keyword`` (23c).
    """
    for version in VERSIONS:
        assert OracleDialect(version=version).supports_fk_on_update() is False
        assert OracleDialect(version=version).supports_foreign_key_on_update() is False


def test_fk_match_is_refused_by_name():
    """Measured on 21c XE: ORA-00907 inline, ORA-01735 out of line."""
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_fk_match() is False


def test_on_delete_is_two_of_the_five_actions():
    """``ON DELETE`` exists, but only CASCADE and SET NULL can be spelled.

    Measured on 21c XE: CASCADE and SET NULL are accepted and the dictionary
    records ``delete_rule`` accordingly; RESTRICT and NO ACTION are refused
    with ``ORA-00905: missing keyword``; SET DEFAULT with ``ORA-03001:
    unimplemented feature``.
    """
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_foreign_key_on_delete() is True
    assert dialect.supports_fk_referential_action_cascade() is True
    assert dialect.supports_fk_referential_action_set_null() is True
    assert dialect.supports_fk_referential_action_restrict() is False
    assert dialect.supports_fk_referential_action_no_action() is False
    assert dialect.supports_fk_referential_action_set_default() is False


def test_no_action_is_the_default_but_cannot_be_spelled():
    """The ``no_action`` probe is False even though the dictionary says so.

    A plain FK with no action clause records ``delete_rule = 'NO ACTION'``
    in ``user_constraints``; spelling it explicitly is refused with
    ``ORA-00905: missing keyword``.  The probe answers the spelling
    question, not the dictionary one, so a caller that wants "the default"
    must omit the clause rather than ask for NO ACTION.
    """
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_fk_referential_action_no_action() is False


def test_enforced_does_not_exist_on_any_release():
    """Measured on 21c XE: ORA-00907 inline, ORA-01735 out of line."""
    for version in VERSIONS:
        assert OracleDialect(version=version).supports_constraint_enforced() is False


def test_alter_constraint_enforced_does_not_exist():
    """Measured on 21c XE: ``ALTER TABLE t ALTER CONSTRAINT c ENABLE`` is
    refused with ``ORA-01735: invalid ALTER TABLE option`` -- there is no
    ``ALTER CONSTRAINT`` clause to carry an enforcement change.
    """
    for version in VERSIONS:
        assert OracleDialect(version=version).supports_alter_constraint_enforced() is False


def test_rely_and_enable_disable_are_oracles_enforcement_vocabulary():
    """Oracle enforces through ENABLE/DISABLE and RELY, not ENFORCED.

    Measured on 21c XE: ``ALTER TABLE t ENABLE CONSTRAINT c``,
    ``DISABLE CONSTRAINT c``, ``ENABLE VALIDATE CONSTRAINT c``,
    ``ENABLE NOVALIDATE CONSTRAINT c`` and ``MODIFY CONSTRAINT c RELY``
    (CHECK, UNIQUE and PRIMARY KEY alike) are all accepted.
    """
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_constraint_enable_disable() is True
    assert dialect.supports_constraint_rely() is True


def test_bare_validate_and_bare_novalidate_are_refused():
    """The bare ``ALTER TABLE`` forms are refused; the ENABLE forms are not.

    Measured on 21c XE: ``ALTER TABLE t VALIDATE CONSTRAINT c`` and
    ``ALTER TABLE t NOVALIDATE CONSTRAINT c`` are each refused with
    ``ORA-01735: invalid ALTER TABLE option``.  The accepted spellings --
    ``ENABLE VALIDATE CONSTRAINT c``, ``ENABLE NOVALIDATE CONSTRAINT c``,
    ``MODIFY CONSTRAINT c VALIDATE``, ``MODIFY CONSTRAINT c NOVALIDATE`` --
    go through ENABLE or MODIFY CONSTRAINT, so a validate/novalidate request
    must be rendered in one of those forms, never bare.
    """
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_validate_constraint() is False
    assert dialect.supports_constraint_novalidate() is False


def test_using_index_is_accepted_but_only_for_unique_and_pk():
    """Measured on 21c XE: UNIQUE and PRIMARY KEY accept ``USING INDEX``,
    CHECK is refused with ``ORA-01735`` (out of line) / ``ORA-00907``
    (inline).  The single probe is True because the kinds that can carry
    the clause accept it; a caller must still refuse it *by kind* on CHECK.
    """
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_using_index_clause() is True


def test_add_drop_and_rename_constraint_are_accepted():
    """Measured on 21c XE: ADD (all four kinds), DROP [CASCADE] and
    RENAME CONSTRAINT ... TO ... are accepted."""
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_add_constraint() is True
    assert dialect.supports_drop_constraint() is True
    assert dialect.supports_rename_constraint() is True


def test_drop_constraint_if_exists_is_refused():
    """Measured on 21c XE: ``DROP CONSTRAINT IF EXISTS`` -> ORA-01735."""
    for version in VERSIONS:
        assert OracleDialect(version=version).supports_drop_constraint_if_exists() is False


def test_exclude_constraint_does_not_exist():
    """Measured on 21c XE: ``ADD CONSTRAINT c EXCLUDE (a FROM ())`` is
    refused with ``ORA-00902: invalid datatype`` -- the parser reads
    ``EXCLUDE`` as a column datatype because no branch exists for it."""
    dialect = OracleDialect(version=(21, 0, 0))
    assert dialect.supports_exclude_constraint() is False
