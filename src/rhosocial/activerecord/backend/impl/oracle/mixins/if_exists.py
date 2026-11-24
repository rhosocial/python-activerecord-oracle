# src/rhosocial/activerecord/backend/impl/oracle/mixins/if_exists.py
"""Oracle's statement-level ``IF [NOT] EXISTS`` capability, in one place.

The clause has **two** documented anchors, and they do not have the same
shape, so one version tuple cannot express it:

* **19c, Release Update 19.28** -- the 19c SQL Language Reference's
  "Changes in This Release" page lists ``IF [NOT] EXISTS`` as introduced with
  Release 19.28, for CREATE (TABLE, VIEW, SEQUENCE, TYPE, INDEX, ...) and for
  ALTER/DROP (TABLE, VIEW, SEQUENCE, TYPE, INDEX, ...); the 19c ``ALTER
  TABLE`` page adds "You can only use IF EXISTS from Release 19.28 and up."
  ``_version`` reads ``(19, 0, 0)`` on every 19c server whatever its release
  update, so the threshold is read from ``VERSION_FULL`` (via ``ru_version``),
  not from the base tuple.

* **23ai, base release** -- the 23ai grammar pages show the clause on the
  same statements with no release note, because on that line it ships in the
  base release. From major 23 on the base version alone decides.

The 21c line is deliberately **not** answered by the 19.28 anchor even though
"19.28 and up" reads as though it should be. The 21c reference shows no
``IF EXISTS`` in its DROP TABLE / CREATE TABLE / VIEW / SEQUENCE / INDEX
grammar, and this repository's own measurement on 21.3 records ``CREATE TABLE
IF NOT EXISTS`` as ``ORA-00922``. A literal ``>= (19, 28, 0)`` comparison
would answer True on 21.3, which the server rejects.

Sources (Oracle SQL Language Reference):

* 19c changes page:
  https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Changes-in-This-Release-for-Oracle-Database-SQL-Language-Reference.html
* 19c ALTER TABLE (IF EXISTS release note):
  https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ALTER-TABLE.html
* 21c DROP TABLE grammar (no clause):
  https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/img_text/drop_table.html
* 23ai CREATE TABLE / DROP TABLE grammar (clause in base release):
  https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/img_text/create_table.html
  https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/img_text/drop_table.html
"""

from typing import Optional

__all__ = ["supports_statement_if_exists"]

#: The 19c line and the release update on it that introduced the clause.
_IF_EXISTS_LINE = 19
_IF_EXISTS_RU = 28

#: The line from which the clause is in the base release.
_IF_EXISTS_BASE_LINE = 23


def supports_statement_if_exists(dialect: object) -> bool:
    """Whether this server accepts statement-level ``IF [NOT] EXISTS``.

    The answer is ``True`` on 19c servers at Release Update 28 or later and on
    23ai/26ai servers of any release update. It is ``False`` on every older
    line, on 21c, and when the version cannot be named.

    Args:
        dialect: the Oracle dialect (or any object exposing ``_version``,
            ``version_full`` and ``_ru_version``).

    Returns:
        ``True`` only when the clause is known to be accepted.
    """
    version = getattr(dialect, "_version", None)
    if version is None:
        return False
    version = tuple(version)
    if not version:
        return False
    if version[0] >= _IF_EXISTS_BASE_LINE:
        # Base release: the clause is in it, with no release-update note.
        return True
    if version[0] != _IF_EXISTS_LINE:
        return False
    full_version = getattr(dialect, "version_full", None)
    configured_ru: Optional[int] = getattr(dialect, "_ru_version", None)
    ru_version = configured_ru
    if full_version and len(full_version) >= 2:
        full_version = tuple(full_version)
        if full_version[0] != _IF_EXISTS_LINE:
            return False
        if configured_ru is not None and configured_ru != full_version[1]:
            return False
        ru_version = full_version[1]
    return ru_version is not None and ru_version >= _IF_EXISTS_RU
