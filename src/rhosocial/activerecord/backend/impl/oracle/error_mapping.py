# src/rhosocial/activerecord/backend/impl/oracle/error_mapping.py
"""Translate python-oracledb exceptions into the framework error taxonomy.

Recognition order follows the error taxonomy contract: the ORA number first,
then the driver exception class, then message text. The ORA number is stable
across driver versions while ``oracledb`` may classify the same server error
under a different DB-API class from release to release.

``python-oracledb`` 26.x (thin mode) raises ``oracledb.exceptions.*`` whose
``args[0]`` is the plain ORA message string, for example
``"ORA-00001: unique constraint (SCOTT.UK_NAME) violated"``. Older or
wrapper-provided exceptions may carry a structured error object whose ``.code``
holds the ORA number, so :func:`extract_ora_code` accepts both shapes and
parses the message only as a last resort.

Every mapping attaches diagnostics (``operation``, ``driver_code``,
``backend`` and ``transient``) and keeps ``raise ... from error`` chaining in
the caller. Nothing here opens a connection, so the mapping is testable without
a database.
"""

import re
from typing import Any, NamedTuple, Optional

from oracledb.exceptions import (
    DatabaseError as OracleDatabaseError,
    Error as OracleError,
    IntegrityError as OracleIntegrityError,
    OperationalError as OracleOperationalError,
)

from rhosocial.activerecord.backend.errors import (
    ConnectionError,
    DatabaseError,
    DeadlockError,
    IntegrityError,
    LockError,
    OperationalError,
    QueryError,
    attach_diagnostics,
)

BACKEND_NAME = "oracle"

_ORA_CODE_PATTERN = re.compile(r"ORA-(\d{5})")

#: ORA numbers recognised by intent. The mapping table is the single place
#: message text would otherwise be scattered.
UNIQUE_CONSTRAINT = 1
FOREIGN_KEY_PARENT = 2291
FOREIGN_KEY_CHILD = 2292
NOT_NULL_INSERT = 1400
NOT_NULL_UPDATE = 1407
DEADLOCK = 60
SELF_DEADLOCK = 4020
RESOURCE_BUSY = 54
LOGIN_FAILURE = 1017
INVALID_TABLE = 942
INVALID_COLUMN = 904
INVALID_OBJECT = 933
SYNTAX_ERROR = 936
SNAPSHOT_TOO_OLD = 1555
NETWORK_LOST = 3113
NETWORK_END_OF_FILE = 3114
NO_LISTENER = 12541


class OracleErrorMapping(NamedTuple):
    """A translated driver error and the log label that describes it."""

    error: DatabaseError
    log_label: str


def extract_ora_code(error: Exception) -> Optional[int]:
    """Return the ORA number carried by *error*, or ``None``.

    The structured paths (``args[0].code``, a numeric ``args[0]`` or an
    ``error.code`` attribute) are checked before the message, because a
    wrapped error object is more reliable than text produced by a driver.

    Args:
        error: Driver exception to inspect.

    Returns:
        The ORA number as an ``int``, or ``None`` when no code is available.
    """
    args = getattr(error, "args", ()) or ()
    if args:
        first = args[0]
        code = _coerce_ora_code(getattr(first, "code", None))
        if code is not None:
            return code
        code = _coerce_ora_code(first)
        if code is not None:
            return code

    code = _coerce_ora_code(getattr(error, "code", None))
    if code is not None:
        return code

    match = _ORA_CODE_PATTERN.search(str(error))
    if match:
        return int(match.group(1))
    return None


def map_oracle_error(error: Exception) -> Optional[OracleErrorMapping]:
    """Translate a python-oracledb exception into a semantic framework error.

    Args:
        error: Exception raised by a driver call.

    Returns:
        An :class:`OracleErrorMapping` for driver errors, or ``None`` for
        exceptions that are not Oracle driver errors (the caller re-raises
        those unchanged).
    """
    if not isinstance(error, OracleError):
        return None

    message = str(error)
    code = extract_ora_code(error)

    if code is not None:
        mapping = _map_by_code(code, message)
        if mapping is not None:
            return mapping

    # Class-level fallback: the driver class is the next most stable signal.
    if isinstance(error, OracleIntegrityError):
        return _mapped(IntegrityError(message), "Integrity error", code, transient=False)
    if isinstance(error, OracleOperationalError):
        return _mapped(OperationalError(message), "Operational error", code)
    if isinstance(error, OracleDatabaseError):
        if "deadlock" in message.lower():
            return _mapped(DeadlockError(message), "Deadlock error", code, transient=True)
        return _mapped(DatabaseError(message), "Database error", code)
    return _mapped(DatabaseError(message), "Oracle error", code)


def _map_by_code(code: int, message: str) -> Optional[OracleErrorMapping]:
    """Map a recognised ORA number to its semantic error."""
    if code == UNIQUE_CONSTRAINT:
        return _mapped(
            IntegrityError(f"Unique constraint violation: {message}"),
            "Unique constraint violation",
            code,
            transient=False,
        )
    if code in (NOT_NULL_INSERT, NOT_NULL_UPDATE):
        # Oracle reports a NULL assigned to a NOT NULL column as ORA-01400 on
        # INSERT and ORA-01407 on UPDATE. The "cannot be null" prefix keeps the
        # cross-backend message contract used by the testsuite.
        return _mapped(
            IntegrityError(f"cannot be null: {message}"),
            "Not-null constraint violation",
            code,
            transient=False,
        )
    if code == FOREIGN_KEY_PARENT:
        return _mapped(
            IntegrityError(f"Foreign key constraint violation: {message}"),
            "Foreign key constraint violation",
            code,
            transient=False,
        )
    if code == FOREIGN_KEY_CHILD:
        return _mapped(
            IntegrityError(f"Child record exists: {message}"),
            "Child record exists",
            code,
            transient=False,
        )
    if code in (DEADLOCK, SELF_DEADLOCK):
        return _mapped(DeadlockError(message), "Deadlock error", code, transient=True)
    if code == RESOURCE_BUSY:
        return _mapped(LockError(message), "Lock error", code, transient=True)
    if code == LOGIN_FAILURE:
        # Authentication failures do not improve on retry. The same is true of
        # the connect() wrapper, which reports every handshake failure as a
        # non-transient ConnectionError.
        return _mapped(ConnectionError(message), "Connection error", code, transient=False)
    if code in (INVALID_TABLE, INVALID_COLUMN, INVALID_OBJECT, SYNTAX_ERROR):
        return _mapped(QueryError(message), "Query error", code, transient=False)
    if code in (SNAPSHOT_TOO_OLD, NETWORK_LOST, NETWORK_END_OF_FILE, NO_LISTENER):
        return _mapped(OperationalError(message), "Operational error", code, transient=True)
    return None


def _mapped(
    error: DatabaseError,
    log_label: str,
    driver_code: Optional[int],
    transient: Optional[bool] = None,
) -> OracleErrorMapping:
    """Attach standard Oracle diagnostics to a translated error."""
    return OracleErrorMapping(
        attach_diagnostics(
            error,
            operation="execute",
            driver_code=driver_code,
            backend=BACKEND_NAME,
            transient=transient,
        ),
        log_label,
    )


def _coerce_ora_code(value: Any) -> Optional[int]:
    """Accept an ORA number only when *value* really looks like one."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 < value < 100000 else None
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
    return None
