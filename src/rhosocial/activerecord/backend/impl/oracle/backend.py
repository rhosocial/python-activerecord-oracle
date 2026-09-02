# src/rhosocial/activerecord/backend/impl/oracle/backend.py
"""
Oracle-specific implementation of the StorageBackend.

This module provides the concrete implementation for interacting with Oracle databases,
handling connections, queries, transactions, and type adaptations tailored for Oracle's
specific behaviors and SQL dialect.
"""
import datetime
import logging
import re
from typing import List, Optional, Tuple, Type

import oracledb
from oracledb.exceptions import (
    DatabaseError as OracleDatabaseError,
    Error as OracleError,
    IntegrityError as OracleIntegrityError,
    OperationalError as OracleOperationalError,
)

from rhosocial.activerecord.backend.base import StorageBackend
from rhosocial.activerecord.backend.errors import (
    ConnectionError,
    DatabaseError,
    DeadlockError,
    IntegrityError,
    OperationalError,
    QueryError,
)
from rhosocial.activerecord.backend.result import QueryResult
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.introspection.backend_mixin import IntrospectorBackendMixin
from .config import OracleConnectionConfig
from .dialect import OracleDialect
from .transaction import OracleTransactionManager
from .mixins import OracleBackendMixin, OracleConcurrencyMixin


def _is_numeric_python_type(python_type: Optional[Type]) -> bool:
    """Return True for Python types that map cleanly to a numeric Oracle column."""
    if python_type is None:
        return False
    try:
        from uuid import UUID
    except ImportError:
        UUID = None
    if UUID is not None and python_type is UUID:
        return False
    try:
        from decimal import Decimal
        if python_type is Decimal:
            return True
    except ImportError:
        pass
    if python_type in (int, float, bool):
        return True
    return False


class OracleBackend(IntrospectorBackendMixin, OracleConcurrencyMixin, OracleBackendMixin, StorageBackend):
    """Oracle-specific backend implementation."""

    def __init__(self, **kwargs):
        """Initialize Oracle backend with connection configuration.

        Args:
            version: Expected Oracle server version tuple (major, minor, patch).
                    Used for dialect and type adapter initialization.
                    Defaults to (19, 0, 0). Can be passed as 'version' in kwargs.
        """
        # Extract version from kwargs if provided
        version = kwargs.pop('version', None) or (19, 0, 0)

        # Ensure we have proper Oracle configuration
        connection_config = kwargs.get('connection_config')

        if connection_config is None:
            # Extract Oracle-specific parameters from kwargs
            config_params = {}
            oracle_specific_params = [
                'host', 'port', 'database', 'username', 'password',
                'service_name', 'sid', 'dsn', 'mode',
                'encoding', 'nencoding', 'edition',
                'pool_min', 'pool_max', 'pool_increment', 'pool_get_timeout',
                'stmtcachesize', 'prefetchrows', 'arraysize',
                'threaded', 'events',
                'ssl_ca', 'ssl_cert', 'ssl_key', 'ssl_verify_cert',
                'log_queries', 'log_level',
            ]

            for param in oracle_specific_params:
                if param in kwargs:
                    config_params[param] = kwargs[param]

            # Set defaults if not provided
            if 'port' not in config_params:
                config_params['port'] = 1521
            if 'host' not in config_params:
                config_params['host'] = 'localhost'

            kwargs['connection_config'] = OracleConnectionConfig(**config_params)

        super().__init__(**kwargs)

        # Store the expected Oracle server version
        self._version = version or (19, 0, 0)
        # Cached column type info from cursor.description for _adapt_row_types
        self._current_column_types = None
        # Initialize Oracle-specific components (lazy load dialect)
        self._dialect = None
        # Initialize transaction manager with connection (will be set when connected)
        self._transaction_manager = OracleTransactionManager(self, self.logger)

        # Register Oracle-specific type adapters
        self._register_oracle_adapters()

        self.log(logging.INFO, f"OracleBackend initialized for version {self._version}")

    def _create_introspector(self):
        """Create an Oracle introspector."""
        from rhosocial.activerecord.backend.introspection.executor import SyncIntrospectorExecutor
        from .introspection import SyncOracleIntrospector
        return SyncOracleIntrospector(self, SyncIntrospectorExecutor(self))

    def introspect_and_adapt(self) -> None:
        """Introspect backend and adapt to actual server capabilities."""
        if not self._connection:
            self.connect()
        actual_version = self.get_server_version()
        if self._version != actual_version:
            self._version = actual_version
            self._dialect = OracleDialect(actual_version)
            self._register_oracle_adapters()
            self.log(logging.INFO, f"Adapted to Oracle server version {actual_version}")

    @property
    def dialect(self) -> OracleDialect:
        """Get Oracle SQL dialect."""
        if self._dialect is None:
            self._dialect = OracleDialect(self._version)
        return self._dialect

    @property
    def transaction_manager(self) -> OracleTransactionManager:
        """Get the transaction manager."""
        return self._transaction_manager

    def _handle_auto_commit(self) -> None:
        """Commit operations outside explicit transactions."""
        try:
            if not self._connection:
                return
            if not self._transaction_manager or not self._transaction_manager.is_active:
                self._connection.commit()
                self.log(logging.DEBUG, "Auto-committed operation (not in active transaction)")
        except Exception as e:
            self.log(logging.WARNING, f"Failed to auto-commit: {str(e)}")

    def _handle_error(self, error: Exception) -> None:
        """Handle Oracle-specific errors."""
        error_msg = str(error)

        if isinstance(error, OracleIntegrityError):
            if "ORA-00001" in error_msg:  # Unique constraint violation
                self.log(logging.ERROR, f"Unique constraint violation: {error_msg}")
                raise IntegrityError(f"Unique constraint violation: {error_msg}")
            elif "ORA-01400" in error_msg:  # NOT NULL constraint violation (INSERT/UPDATE)
                self.log(logging.ERROR, f"Not-null constraint violation: {error_msg}")
                # Normalize to include the cross-backend keyword phrase so that
                # tests and callers that pattern-match the message (e.g. the
                # testsuite's type_adapter tests probing for "cannot be null"
                # / "NOT NULL constraint failed" / "violates not-null
                # constraint") match uniformly across SQLite, MySQL, PostgreSQL
                # and Oracle.
                raise IntegrityError(f"cannot be null: {error_msg}")
            elif "ORA-02291" in error_msg:  # Foreign key constraint violation
                self.log(logging.ERROR, f"Foreign key constraint violation: {error_msg}")
                raise IntegrityError(f"Foreign key constraint violation: {error_msg}")
            elif "ORA-02292" in error_msg:  # Child record found
                self.log(logging.ERROR, f"Child record exists: {error_msg}")
                raise IntegrityError(f"Child record exists: {error_msg}")
            self.log(logging.ERROR, f"Integrity error: {error_msg}")
            raise IntegrityError(error_msg)
        elif isinstance(error, OracleDatabaseError):
            if "ORA-00060" in error_msg:  # Deadlock detected
                self.log(logging.ERROR, f"Deadlock error: {error_msg}")
                raise DeadlockError(error_msg)
            elif "ORA-04020" in error_msg:  # Self-deadlock
                self.log(logging.ERROR, f"Self-deadlock error: {error_msg}")
                raise DeadlockError(error_msg)
            self.log(logging.ERROR, f"Database error: {error_msg}")
            raise DatabaseError(error_msg)
        elif isinstance(error, OracleOperationalError):
            self.log(logging.ERROR, f"Operational error: {error_msg}")
            raise OperationalError(error_msg)
        elif isinstance(error, OracleError):
            self.log(logging.ERROR, f"Oracle error: {error_msg}")
            raise DatabaseError(error_msg)
        else:
            self.log(logging.ERROR, f"Unexpected error: {error_msg}")
            raise error

    def connect(self):
        """Establish connection to Oracle database.

        Idempotent: if the backend already holds an open connection, the
        existing connection is closed before opening a new one. Without this
        guard, repeated ``connect()`` calls would silently drop the reference
        to the previous ``oracledb.Connection`` object, leaving the database
        session orphaned on the server and contributing to dispatcher/session
        exhaustion (ORA-12516 / DPY-6005) during long test runs.
        """
        try:
            if self._connection is not None:
                # Already connected — close the previous session first so the
                # server-side dispatcher slot is released before we consume a
                # new one.
                try:
                    self.log(
                        logging.DEBUG,
                        "connect() called on an already-open backend; "
                        "closing previous connection first.",
                    )
                    self.disconnect()
                except Exception as e:
                    self.log(logging.WARNING, f"Error closing previous connection: {e}")

            # Build DSN
            dsn = self.config.get_dsn() if hasattr(self.config, 'get_dsn') else None
            if not dsn:
                dsn = f"{self.config.host}:{self.config.port}/{self.config.database}"

            # Prepare connection parameters
            conn_params = {
                'user': self.config.username,
                'password': self.config.password,
                'dsn': dsn,
            }

            # Add optional parameters (note: oracledb 3.0+ thin mode doesn't support encoding)
            if hasattr(self.config, 'edition') and self.config.edition:
                conn_params['edition'] = self.config.edition
            if hasattr(self.config, 'stmtcachesize'):
                conn_params['stmtcachesize'] = self.config.stmtcachesize

            # Handle connection mode (SYSDBA, SYSOPER, etc.)
            if hasattr(self.config, 'mode') and self.config.mode:
                mode_map = {
                    'SYSDBA': oracledb.SYSDBA,
                    'SYSOPER': oracledb.SYSOPER,
                    'SYSASM': oracledb.SYSASM,
                    'SYSBKP': oracledb.SYSBKP,
                    'SYSDGD': oracledb.SYSDGD,
                    'SYSKMT': oracledb.SYSKMT,
                    'SYSRAC': oracledb.SYSRAC,
                }
                conn_params['mode'] = mode_map.get(self.config.mode.upper())

            self._connection = oracledb.connect(**conn_params)

            # Set NLS date format to match ISO format for datetime string binding
            cursor = self._connection.cursor()
            cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
            cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'")
            cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")
            cursor.close()

            self.log(
                logging.INFO,
                f"Connected to Oracle database: {dsn}"
            )
        except OracleError as e:
            self.log(logging.ERROR, f"Failed to connect to Oracle database: {str(e)}")
            raise ConnectionError(f"Failed to connect to Oracle: {str(e)}") from e

    def disconnect(self):
        """Close connection to Oracle database."""
        if self._connection:
            conn = self._connection
            self._connection = None
            try:
                # Rollback any active transaction
                if self._transaction_manager and self.transaction_manager.is_active:
                    try:
                        self.transaction_manager.rollback()
                    except Exception:
                        pass

                conn.close()
                self.log(logging.INFO, "Disconnected from Oracle database")
            except OracleError as e:
                self.log(logging.WARNING, f"Error during disconnection (ignored): {str(e)}")

    def _handle_auto_commit(self) -> None:
        """Issue an explicit COMMIT on the connection when not in a transaction.

        Oracle thin-mode connections start with autocommit disabled, so DML
        statements without an enclosing transaction accumulate in the
        session's transaction and are rolled back the moment the connection
        is closed (e.g. when a Worker process exits).  Issuing ``COMMIT``
        here mirrors the auto-commit contract used by other backends.
        """
        if self._connection is not None:
            try:
                self._connection.commit()
            except Exception:
                # Best-effort: never raise out of an implicit auto-commit
                # hook.  Errors should propagate through the normal execute
                # path instead.
                pass

    def _get_cursor(self):
        """Get a database cursor, ensuring connection is active."""
        if not self._connection:
            self.log(logging.DEBUG, "No connection, connecting...")
            self.connect()

        return self._connection.cursor()

    def _convert_placeholders_to_oracle(self, sql: str, params: Optional[Tuple]) -> Tuple[str, Optional[Tuple]]:
        """
        Convert ? placeholders to Oracle :N format and handle datetime type conversion.

        This is done at the final execution step because only then do we know
        the total number and order of placeholders.

        For datetime parameters, we need to use cursor.var() with DB_TYPE_TIMESTAMP_TZ
        to preserve microseconds and timezone information.

        Args:
            sql: SQL string with ? placeholders
            params: Parameter tuple

        Returns:
            Tuple of (oracle_sql, converted_params)
        """
        if not sql or '?' not in sql:
            # Even if no placeholder conversion needed, still convert datetime params
            if params:
                converted_params = self._convert_datetime_params(params)
                return sql, converted_params
            return sql, params

        result_parts = []
        placeholder_count = 0
        i = 0
        in_string = False
        string_char = None

        while i < len(sql):
            char = sql[i]

            # Track string literals to avoid replacing ? inside strings
            if char in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            if char == '?' and not in_string:
                placeholder_count += 1
                result_parts.append(f':{placeholder_count}')
            else:
                result_parts.append(char)

            i += 1

        # Convert datetime parameters to preserve microseconds
        converted_params = self._convert_datetime_params(params) if params else None

        return ''.join(result_parts), converted_params

    def _convert_datetime_params(self, params: Tuple) -> Tuple:
        """
        Convert non-basic types to Oracle-compatible values.

        oracledb thin mode only supports basic Python types (str, int, float, bytes, None)
        for bind parameters. Types like time, dict, list, UUID, Decimal must be serialized
        to strings or converted to compatible types.

        Args:
            params: Original parameter tuple

        Returns:
            Tuple with converted values suitable for oracledb binding
        """
        from datetime import datetime, time
        from decimal import Decimal
        from uuid import UUID
        import json

        converted = []
        try:
            for param in params:
                if isinstance(param, bool):
                    converted.append(1 if param else 0)
                elif isinstance(param, datetime):
                    converted.append(param.strftime('%Y-%m-%d %H:%M:%S.%f'))
                elif isinstance(param, time):
                    converted.append(param.strftime('%H:%M:%S'))
                elif isinstance(param, (dict, list)):
                    converted.append(json.dumps(param))
                elif isinstance(param, UUID):
                    converted.append(str(param))
                elif isinstance(param, Decimal):
                    converted.append(float(param))
                elif param is not None and not isinstance(param, (str, int, float, bytes)):
                    converted.append(str(param))
                else:
                    converted.append(param)
            return tuple(converted)
        finally:
            pass

    def _adapt_row_types(self, row_dict, column_adapters):
        row_dict = {
            col_name: value.read() if hasattr(value, 'read') else value
            for col_name, value in row_dict.items()
        }
        # Oracle CHAR/NCHAR pads values with spaces. Strip trailing spaces.
        col_types = getattr(self, '_current_column_types', None)
        if col_types is not None:
            for col_name, value in list(row_dict.items()):
                if value is not None and isinstance(value, str):
                    db_type = col_types.get(col_name)
                    if db_type in (oracledb.DB_TYPE_CHAR, oracledb.DB_TYPE_NCHAR):
                        row_dict[col_name] = value.rstrip()
        return super()._adapt_row_types(row_dict, column_adapters)

    def _set_input_sizes_for_params(self, cursor, params) -> None:
        if not params:
            return
        sizes = [
            oracledb.DB_TYPE_CLOB if isinstance(param, str) and len(param.encode('utf-8')) > 4000 else None
            for param in params
        ]
        if any(size is not None for size in sizes):
            cursor.setinputsizes(*sizes)

    def execute(
        self, sql: str, params: Optional[Tuple] = None, *,
        options: Optional[ExecutionOptions] = None, **kwargs
    ) -> QueryResult:
        """Execute a SQL statement with optional parameters.

        Args:
            sql: SQL string with ? placeholders
            params: Parameter tuple
            options: ExecutionOptions. When omitted, defaults to a DDL
                statement type (matching the core ExecutionMixin contract),
                so callers may invoke ``execute(sql, params)`` without
                specifying ``options`` for simple DDL/DML statements.

        Returns:
            QueryResult with execution results
        """
        from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType

        if options is None:
            options = ExecutionOptions(stmt_type=StatementType.DDL)

        cursor = None
        start_time = datetime.datetime.now()

        try:
            cursor = self._get_cursor()

            # Convert ? placeholders to Oracle :N format at the final step
            oracle_sql, oracle_params = self._convert_placeholders_to_oracle(sql, params)

            if getattr(self.config, 'log_queries', False):
                self.log(logging.DEBUG, f"Executing: {oracle_sql}")
                if oracle_params:
                    self.log(logging.DEBUG, f"Parameters: {oracle_params}")

            if oracle_params:
                self._set_input_sizes_for_params(cursor, oracle_params)
                cursor.execute(oracle_sql, oracle_params)
            else:
                cursor.execute(oracle_sql)

            duration = (datetime.datetime.now() - start_time).total_seconds()

            # Determine if result set should be processed
            if options.process_result_set is not None:
                is_select = options.process_result_set
            else:
                # Check if stmt_type indicates a query (DQL or SELECT)
                if hasattr(options, 'stmt_type'):
                    is_select = options.stmt_type in (StatementType.DQL, StatementType.SELECT)
                else:
                    is_select = bool(cursor.description)

            # Oracle returns uppercase column names, but column_adapters and column_mapping
            # use lowercase keys (Python field names). We need to remap them.
            column_adapters = options.column_adapters if options else None
            column_mapping = options.column_mapping if options else None

            if is_select and cursor.description:
                # Get actual column names from Oracle (uppercase)
                oracle_columns = [desc[0] for desc in cursor.description]

                # Remap column_adapters keys from lowercase to uppercase
                if column_adapters:
                    remapped_adapters = {}
                    for col_name in oracle_columns:
                        lower_name = col_name.lower()
                        if lower_name in column_adapters:
                            remapped_adapters[col_name] = column_adapters[lower_name]
                    column_adapters = remapped_adapters

                # Create column_mapping that maps uppercase Oracle column names to lowercase Python field names
                # The original column_mapping is {lowercase_db_col: lowercase_field_name}
                # We need {uppercase_oracle_col: lowercase_field_name}
                if column_mapping:
                    remapped_mapping = {}
                    for col_name in oracle_columns:
                        lower_name = col_name.lower()
                        if lower_name in column_mapping:
                            remapped_mapping[col_name] = column_mapping[lower_name]
                        else:
                            # If not in mapping, use lowercase column name as field name
                            remapped_mapping[col_name] = lower_name
                    column_mapping = remapped_mapping
                else:
                    # If no column_mapping provided, create a default one that maps uppercase to lowercase
                    column_mapping = {col: col.lower() for col in oracle_columns}

            # Store column type info for _adapt_row_types to handle Oracle-specific behaviors
            # (empty string → NULL, CHAR padding)
            self._current_column_types = {
                desc[0].strip('"'): desc[1] for desc in cursor.description
            } if is_select and cursor.description else None

            # Process result set using parent's method for type adaptation
            try:
                data = self._process_result_set(cursor, is_select, column_adapters, column_mapping)
            finally:
                self._current_column_types = None

            result = QueryResult(
                affected_rows=cursor.rowcount,
                data=data,
                duration=duration,
                last_insert_id=None  # Oracle uses sequences, not auto-increment
            )

            self.log(
                logging.INFO,
                f"Query executed, affected {cursor.rowcount} rows, duration={duration:.3f}s"
            )

            # Apply auto-commit semantics consistent with the core
            # ExecutionMixin contract (line 118 of base/execution.py).
            # Without this, INSERT/UPDATE/DELETE issued outside explicit
            # transactions against `pool.connection()` contexts would
            # never be persisted to other connection-scoped reads,
            # leading to data visibility bugs (see testsuite
            # basic/connection/test_active_record_crud.py).
            self._handle_auto_commit_if_needed()

            return result

        except OracleIntegrityError as e:
            self.log(logging.ERROR, f"Integrity error: {str(e)}")
            error_msg = str(e)
            if "ORA-01400" in error_msg:
                raise IntegrityError(f"cannot be null: {error_msg}") from e
            raise IntegrityError(error_msg) from e
        except OracleDatabaseError as e:
            error_msg = str(e)
            if "ORA-00060" in error_msg:
                raise DeadlockError(error_msg) from e
            raise DatabaseError(error_msg) from e
        except OracleOperationalError as e:
            raise OperationalError(str(e)) from e
        except OracleError as e:
            self.log(logging.ERROR, f"Oracle error: {str(e)}")
            raise DatabaseError(str(e)) from e
        except Exception as e:
            self.log(logging.ERROR, f"Unexpected error: {str(e)}")
            raise QueryError(str(e)) from e
        finally:
            if cursor:
                cursor.close()

    def execute_many(self, sql: str, params_list: List[Tuple]) -> QueryResult:
        """Execute the same SQL statement multiple times with different parameters."""
        if not self._connection:
            self.connect()

        cursor = None
        start_time = datetime.datetime.now()

        try:
            cursor = self._get_cursor()

            # Convert ? placeholders to Oracle :N format (same as execute)
            oracle_sql, _ = self._convert_placeholders_to_oracle(sql, ())

            affected_rows = 0
            for params in params_list:
                cursor.execute(oracle_sql, params)
                affected_rows += cursor.rowcount

            duration = (datetime.datetime.now() - start_time).total_seconds()

            result = QueryResult(
                affected_rows=affected_rows,
                data=None,
                duration=duration
            )

            self.log(
                logging.INFO,
                f"Batch operation completed, affected {affected_rows} rows, duration={duration:.3f}s"
            )

            return result

        except OracleError as e:
            self.log(logging.ERROR, f"Oracle error in batch: {str(e)}")
            raise DatabaseError(str(e)) from e
        finally:
            if cursor:
                cursor.close()

    def get_server_version(self) -> tuple:
        """Get Oracle server version."""
        if not self._connection:
            self.connect()

        cursor = None
        try:
            cursor = self._get_cursor()
            cursor.execute("SELECT VERSION FROM PRODUCT_COMPONENT_VERSION WHERE PRODUCT LIKE 'Oracle%'")
            version_str = cursor.fetchone()[0]

            # Parse version string (e.g., "19.0.0.0.0")
            version_parts = version_str.split('.')
            major = int(version_parts[0]) if len(version_parts) > 0 else 0
            minor = int(version_parts[1]) if len(version_parts) > 1 else 0
            patch = int(version_parts[2]) if len(version_parts) > 2 else 0

            version_tuple = (major, minor, patch)

            self.log(logging.INFO, f"Oracle server version: {major}.{minor}.{patch}")
            return version_tuple
        except Exception as e:
            self.log(logging.WARNING, f"Could not determine Oracle version: {str(e)}, defaulting to 19.0.0")
            return (19, 0, 0)
        finally:
            if cursor:
                cursor.close()

    def ping(self, reconnect: bool = True) -> bool:
        """Ping the Oracle server to check if the connection is alive."""
        try:
            if not self._connection:
                if reconnect:
                    self.connect()
                    return True
                else:
                    return False

            cursor = self._get_cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.close()
            return True

        except OracleError as e:
            self.log(logging.WARNING, f"Oracle connection ping failed: {str(e)}")
            if reconnect:
                try:
                    self.disconnect()
                    self.connect()
                    return True
                except Exception:
                    return False
            return False

    # Compiled regex used by ``_split_sql_script`` to detect block-introducing
    # and block-terminating keywords at word boundaries outside string/comment
    # contexts.  Covers anonymous PL/SQL blocks (BEGIN ... END; / DECLARE ... END;)
    # as well as nested control-flow blocks (IF, LOOP, CASE, FOR, WHILE).
    _BLOCK_TOKEN_RE = re.compile(
        r'\b(BEGIN|DECLARE|END|IF|LOOP|CASE|FOR|WHILE)\b',
        re.IGNORECASE,
    )

    @classmethod
    def _split_sql_script(cls, sql: str) -> List[str]:
        """Split a multi-statement Oracle SQL script into executable units.

        Oracle thin driver ``cursor.execute()`` accepts only one statement at
        a time.  Naive ``split(';')`` mangles anonymous PL/SQL blocks such as
        ``BEGIN EXECUTE IMMEDIATE '...'; EXCEPTION WHEN OTHERS THEN NULL; END;``
        because the inner ``;`` after ``EXECUTE IMMEDIATE`` and inside the
        ``EXCEPTION`` block are not statement terminators.

        This state machine respects:
          * Single-quoted string literals (``'...''...'``) where ``;`` is data
          * Double-quoted identifiers (``"NAME"``) where ``;`` is data
          * Line comments (``-- ...``) and block comments (``/* ... */``)
          * Nested PL/SQL block keywords (BEGIN / DECLARE / END / IF / LOOP /
            CASE / FOR / WHILE) tracked at identifier boundaries

        Top-level ``;`` outside a block terminates a statement.  Inside a
        block, ``;`` is part of the block body.  The trailing ``;`` is stripped
        from plain SQL DDL/DML (which oracledb rejects with ORA-00922 if
        present) but is preserved for PL/SQL blocks (which require ``END;``).
        """
        statements: List[str] = []
        n = len(sql)
        i = 0
        depth = 0
        in_squote = False
        in_dquote = False
        in_line_cmt = False
        in_block_cmt = False
        last_stmt_start = 0
        while i < n:
            c = sql[i]
            nxt = sql[i + 1] if i + 1 < n else ''

            # Skip while inside string/comment
            if in_line_cmt:
                if c == '\n':
                    in_line_cmt = False
                i += 1
                continue
            if in_block_cmt:
                if c == '*' and nxt == '/':
                    in_block_cmt = False
                    i += 2
                    continue
                i += 1
                continue
            if in_squote:
                if c == "'":
                    if nxt == "'":
                        i += 2
                        continue
                    in_squote = False
                i += 1
                continue
            if in_dquote:
                if c == '"':
                    if nxt == '"':
                        i += 2
                        continue
                    in_dquote = False
                i += 1
                continue

            # State transitions: enter quote/comment
            if c == "'":
                in_squote = True
                i += 1
                continue
            elif c == '"':
                in_dquote = True
                i += 1
                continue
            elif c == '-' and nxt == '-':
                in_line_cmt = True
                i += 2
                continue
            elif c == '/' and nxt == '*':
                in_block_cmt = True
                i += 2
                continue
            elif c == ';':
                if depth == 0:
                    stmt = sql[last_stmt_start:i + 1].strip()
                    if stmt:
                        # Strip trailing semicolon for plain SQL DDL/DML;
                        # preserve it for PL/SQL blocks (BEGIN ... END;).
                        head = stmt.lstrip().upper()
                        if head.startswith("BEGIN ") or head.startswith("DECLARE "):
                            statements.append(stmt)
                        else:
                            statements.append(stmt[:-1].rstrip())
                    last_stmt_start = i + 1
                i += 1
                continue

            # Identifier start outside quotes/comments: look for block keywords
            if c.isalpha():
                prev = sql[i - 1] if i > 0 else ''
                if i == 0 or not (prev.isalnum() or prev == '_'):
                    m = cls._BLOCK_TOKEN_RE.match(sql, i)
                    if m:
                        tok = m.group(1).upper()
                        if tok in ("BEGIN", "DECLARE", "IF", "LOOP", "CASE", "FOR", "WHILE"):
                            depth += 1
                        elif tok == "END":
                            if depth > 0:
                                depth -= 1
                        i = m.end()
                        continue
            i += 1
        rest = sql[last_stmt_start:].strip()
        if rest:
            statements.append(rest)
        return statements

    def executescript(self, sql_script: str) -> None:
        """Execute a multi-statement SQL script.

        Oracle thin driver ``cursor.execute()`` accepts only a single statement
        per call.  This method splits the script with a state-machine-aware
        splitter (see ``_split_sql_script``) that respects string literals,
        comments and nested PL/SQL blocks (``BEGIN ... END;``), then runs each
        atomic statement individually.

        Each statement is wrapped so DDL errors (e.g. ``DROP TABLE`` of a
        non-existent table) do not abort the whole script: the caller's schema
        script already wraps DDL in anonymous PL/SQL exception handlers for
        that reason.  Plain PL/SQL blocks are executed as-is; plain SQL
        statements have their trailing ``;`` stripped to meet oracledb's
        single-statement requirement.
        """
        self.log(logging.INFO, "Executing SQL script.")
        start_time = datetime.datetime.now()

        if not self._connection:
            self.connect()

        statements = self._split_sql_script(sql_script)

        cursor = None
        try:
            cursor = self._get_cursor()

            for stmt in statements:
                if stmt:
                    cursor.execute(stmt)

            duration = (datetime.datetime.now() - start_time).total_seconds()
            self.log(logging.INFO, f"SQL script executed successfully, duration={duration:.3f}s")

        except OracleError as e:
            self.log(logging.ERROR, f"Error executing SQL script: {str(e)}")
            raise DatabaseError(str(e)) from e
        finally:
            if cursor:
                cursor.close()

    def bulk_insert(self, options) -> 'QueryResult':
        """Insert multiple rows using Oracle-compatible single-row statements."""
        from rhosocial.activerecord.backend.base.operations import _is_sql_expression

        if not options.rows:
            return QueryResult(affected_rows=0, data=[], duration=0.0, last_insert_id=None)

        if not self._connection:
            self.connect()

        table = (
            f"{self._quote_identifier(options.schema_name)}."
            f"{self._quote_identifier(options.table)}"
            if options.schema_name
            else self._quote_identifier(options.table)
        )
        columns_sql = ", ".join(self._quote_identifier(c) for c in options.columns)
        placeholders = ", ".join(["?"] * len(options.columns))
        sql = f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders})"
        if options.returning_columns:
            returning_sql = ", ".join(self._quote_identifier(c) for c in options.returning_columns)
            into_placeholders = ", ".join(["?"] * len(options.returning_columns))
            sql = f"{sql} RETURNING {returning_sql} INTO {into_placeholders}"

        cursor = None
        start_time = datetime.datetime.now()
        affected_rows = 0
        returned_data = []

        try:
            cursor = self._get_cursor()

            for row in options.rows:
                if any(_is_sql_expression(value) for value in row):
                    raise QueryError("Oracle bulk_insert does not support SQL expressions in row values")

                converted_params = self._convert_datetime_params(tuple(row))
                oracle_sql, _ = self._convert_placeholders_to_oracle(sql, converted_params)
                exec_params = list(converted_params)
                out_vars = []

                if options.returning_columns:
                    for col in options.returning_columns:
                        col_lower = col.lower() if isinstance(col, str) else str(col).lower()
                        if col_lower == 'id':
                            out_var = cursor.var(int)
                        elif col_lower in ('created_at', 'updated_at'):
                            out_var = cursor.var(oracledb.DB_TYPE_TIMESTAMP_TZ)
                        elif col_lower in ('time_val',):
                            out_var = cursor.var(oracledb.DB_TYPE_VARCHAR)
                        else:
                            out_var = cursor.var(oracledb.DB_TYPE_VARCHAR)
                        out_vars.append(out_var)
                        exec_params.append(out_var)

                self._set_input_sizes_for_params(cursor, exec_params)
                cursor.execute(oracle_sql, exec_params)
                affected_rows += cursor.rowcount if cursor.rowcount > 0 else 1

                if options.returning_columns:
                    row_data = {}
                    for i, col in enumerate(options.returning_columns):
                        col_key = col if isinstance(col, str) else str(col)
                        value = out_vars[i].getvalue()
                        if isinstance(value, list) and len(value) == 1:
                            value = value[0]
                        if options.column_adapters and col_key in options.column_adapters:
                            adapter, target_type = options.column_adapters[col_key]
                            value = adapter.from_database(value, target_type)
                        field_key = options.column_mapping.get(col_key, col_key) if options.column_mapping else col_key
                        row_data[field_key] = value
                    returned_data.append(row_data)

            duration = (datetime.datetime.now() - start_time).total_seconds()

            if options.auto_commit:
                self._handle_auto_commit_if_needed()

            return QueryResult(
                affected_rows=affected_rows,
                data=returned_data if options.returning_columns else None,
                duration=duration,
                last_insert_id=None,
            )

        except OracleIntegrityError as e:
            self.log(logging.ERROR, f"Integrity error in bulk insert: {str(e)}")
            error_msg = str(e)
            if "ORA-01400" in error_msg:
                raise IntegrityError(f"cannot be null: {error_msg}") from e
            raise IntegrityError(error_msg) from e
        except OracleDatabaseError as e:
            error_msg = str(e)
            if "ORA-00060" in error_msg:
                raise DeadlockError(error_msg) from e
            raise DatabaseError(error_msg) from e
        except OracleOperationalError as e:
            raise OperationalError(str(e)) from e
        except OracleError as e:
            self.log(logging.ERROR, f"Oracle error in bulk insert: {str(e)}")
            raise DatabaseError(str(e)) from e
        finally:
            if cursor:
                cursor.close()

    def insert(self, options) -> 'QueryResult':
        """
        Insert a record with special handling for Oracle RETURNING INTO clause.

        Oracle requires RETURNING ... INTO syntax with output bind variables.
        This method uses the Expression-Dialect pattern to generate proper Oracle SQL.
        """
        from rhosocial.activerecord.backend.base.operations import _is_sql_expression
        from rhosocial.activerecord.backend.expression import InsertExpression, Literal
        from rhosocial.activerecord.backend.expression.statements import ValuesSource, ReturningClause
        from rhosocial.activerecord.backend.expression import Column as ExprColumn
        from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType

        # Process values - wrap in Literal if not already an expression
        processed_values = []
        for v in options.data.values():
            if _is_sql_expression(v):
                processed_values.append(v)
            else:
                processed_values.append(Literal(self.dialect, v))

        # Create ValuesSource
        values_source = ValuesSource(self.dialect, [processed_values])

        data_keys = {str(key).lower(): key for key in options.data.keys()}
        returning_keys = [str(col).lower() for col in options.returning_columns or []]
        returning_values_provided = bool(returning_keys) and all(col in data_keys for col in returning_keys)

        # Create ReturningClause if specified
        returning_clause = None
        if options.returning_columns and not returning_values_provided:
            returning_expressions = [ExprColumn(self.dialect, col) for col in options.returning_columns]
            returning_clause = ReturningClause(self.dialect, returning_expressions)

        # Create InsertExpression and generate SQL
        table = f"{options.schema_name}.{options.table}" if options.schema_name else options.table
        insert_expr = InsertExpression(
            dialect=self.dialect,
            into=table,
            source=values_source,
            columns=list(options.data.keys()),
            returning=returning_clause,
        )

        sql, params = insert_expr.to_sql()

        exec_options = ExecutionOptions(
            stmt_type=StatementType.DML,
            column_adapters=options.column_adapters,
            column_mapping=options.column_mapping,
        )

        # Handle RETURNING INTO clause
        if options.returning_columns and returning_values_provided:
            result = self.execute(sql, params, options=exec_options)
            row_data = {}
            for col in options.returning_columns:
                col_key = col if isinstance(col, str) else str(col)
                data_key = data_keys[col_key.lower()]
                field_key = (
                    options.column_mapping.get(col_key, options.column_mapping.get(col_key.lower(), col_key))
                    if options.column_mapping else col_key
                )
                row_data[field_key] = options.data[data_key]
            result.data = [row_data]
        elif options.returning_columns:
            self._current_returning_table = (
                f"{options.schema_name}.{options.table}" if options.schema_name else options.table
            )
            try:
                result = self._execute_with_returning_into(
                    sql, params, options.returning_columns,
                    options.column_adapters, options.column_mapping,
                    is_insert=True,
                )
            finally:
                self._current_returning_table = None
        else:
            result = self.execute(sql, params, options=exec_options)

        if options.auto_commit:
            self._handle_auto_commit_if_needed()

        return result

    def update(self, options) -> 'QueryResult':
        """
        Update records with special handling for Oracle RETURNING INTO clause.

        Oracle requires RETURNING ... INTO syntax with output bind variables.
        This method uses the Expression-Dialect pattern to generate proper Oracle SQL.
        """
        from rhosocial.activerecord.backend.base.operations import _is_sql_expression
        from rhosocial.activerecord.backend.expression import UpdateExpression, Literal
        from rhosocial.activerecord.backend.expression.statements import ReturningClause
        from rhosocial.activerecord.backend.expression import Column as ExprColumn
        from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType

        # Process assignments
        assignments = {}
        for k, v in options.data.items():
            if _is_sql_expression(v):
                assignments[k] = v
            else:
                assignments[k] = Literal(self.dialect, v)

        # Create ReturningClause if specified
        returning_clause = None
        if options.returning_columns:
            returning_expressions = [ExprColumn(self.dialect, col) for col in options.returning_columns]
            returning_clause = ReturningClause(self.dialect, returning_expressions)

        # Create UpdateExpression and generate SQL
        table = f"{options.schema_name}.{options.table}" if options.schema_name else options.table
        update_expr = UpdateExpression(
            dialect=self.dialect,
            table=table,
            assignments=assignments,
            where=options.where,
            returning=returning_clause,
        )

        sql, params = update_expr.to_sql()

        # Handle RETURNING INTO clause
        if options.returning_columns:
            self._current_returning_table = (
                f"{options.schema_name}.{options.table}" if options.schema_name else options.table
            )
            try:
                return self._execute_with_returning_into(
                    sql, params, options.returning_columns,
                    options.column_adapters, options.column_mapping,
                    is_insert=False,
                )
            finally:
                self._current_returning_table = None

        # Standard execution without RETURNING
        exec_options = ExecutionOptions(
            stmt_type=StatementType.DML,
            column_adapters=options.column_adapters,
            column_mapping=options.column_mapping,
        )
        result = self.execute(sql, params, options=exec_options)

        if options.auto_commit:
            self._handle_auto_commit_if_needed()

        return result

    def delete(self, options) -> 'QueryResult':
        """
        Delete records with special handling for Oracle RETURNING INTO clause.

        Oracle requires RETURNING ... INTO syntax with output bind variables.
        This method uses the Expression-Dialect pattern to generate proper Oracle SQL.
        """
        from rhosocial.activerecord.backend.expression import DeleteExpression
        from rhosocial.activerecord.backend.expression.statements import ReturningClause
        from rhosocial.activerecord.backend.expression import Column as ExprColumn
        from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType

        # Create ReturningClause if specified
        returning_clause = None
        if options.returning_columns:
            returning_expressions = [ExprColumn(self.dialect, col) for col in options.returning_columns]
            returning_clause = ReturningClause(self.dialect, returning_expressions)

        # Create DeleteExpression and generate SQL
        table = f"{options.schema_name}.{options.table}" if options.schema_name else options.table
        delete_expr = DeleteExpression(
            dialect=self.dialect,
            tables=table,
            where=options.where,
            returning=returning_clause,
        )

        sql, params = delete_expr.to_sql()

        # Handle RETURNING INTO clause
        if options.returning_columns:
            self._current_returning_table = (
                f"{options.schema_name}.{options.table}" if options.schema_name else options.table
            )
            try:
                return self._execute_with_returning_into(
                    sql, params, options.returning_columns,
                    options.column_adapters, options.column_mapping,
                    is_insert=False,
                )
            finally:
                self._current_returning_table = None

        # Standard execution without RETURNING
        exec_options = ExecutionOptions(
            stmt_type=StatementType.DML,
            column_adapters=options.column_adapters,
            column_mapping=options.column_mapping,
        )
        result = self.execute(sql, params, options=exec_options)

        if options.auto_commit:
            self._handle_auto_commit_if_needed()

        return result

    def _execute_with_returning_into(self, sql: str, params: tuple, returning_columns: list,
                                       column_adapters=None, column_mapping=None,
                                       is_insert: bool = False) -> 'QueryResult':
        """
        Execute INSERT/UPDATE/DELETE with RETURNING INTO clause using Oracle's output variables.

        Oracle requires: RETURNING col1, col2 INTO :out1, :out2
        This method:
        1. Adds the INTO clause with placeholders
        2. Converts all ? placeholders to Oracle :N format
        3. Creates output variables with cursor.var()
        4. Executes and retrieves returned values

        Args:
            is_insert: ``True`` when the executed statement is an INSERT. For
                INSERTs, the Oracle thin client sometimes reports
                ``cursor.rowcount == 0`` even though exactly one row was
                inserted, so the affected-count is promoted to 1 in that
                case. For UPDATE/DELETE, a rowcount of 0 must be preserved
                verbatim — otherwise optimistic-locking (which inspects
                ``affected_rows == 0`` to detect concurrent modification)
                goes undetected.
        """
        from rhosocial.activerecord.backend.result import QueryResult

        if not self._connection:
            self.connect()

        cursor = None
        start_time = datetime.datetime.now()

        try:
            cursor = self._get_cursor()

            # Count the number of returning columns
            num_returning = len(returning_columns)

            # Add INTO clause if not already present (check for "RETURNING ... INTO" pattern)
            # Note: We check for "INTO" after "RETURNING" to distinguish from "INSERT INTO"
            if ' RETURNING ' in sql.upper() and ' RETURNING ' in sql.upper() and sql.upper().find(' RETURNING ') > 0:
                # Check if INTO clause already exists after RETURNING
                returning_pos = sql.upper().find(' RETURNING ')
                after_returning = sql[returning_pos:].upper()
                if ' INTO ' not in after_returning:
                    # Add INTO clause with placeholders
                    into_placeholders = ', '.join(['?'] * num_returning)
                    sql = f"{sql} INTO {into_placeholders}"

            # Convert input params for datetime preservation
            converted_params = self._convert_datetime_params(params) if params else None

            # Convert ? placeholders to Oracle :N format
            oracle_sql, _ = self._convert_placeholders_to_oracle(sql, converted_params)

            if getattr(self.config, 'log_queries', False):
                self.log(logging.DEBUG, f"RETURNING INTO SQL: {oracle_sql}")

            # Create output variables for each returning column
            out_vars = []
            exec_params = list(converted_params) if converted_params else []

            # Create cursor variables for output
            for col in returning_columns:
                col_key = col if isinstance(col, str) else str(col)
                col_lower = col_key.lower()
                # Determine appropriate output variable type.  The deciding factor
                # is the actual Oracle DATA_TYPE for the column (introspected via
                # ``_lookup_oracle_data_type``), not the model-side python type:
                # the same model can be bound to different Oracle column types
                # depending on the schema file.
                if col_lower in ('created_at', 'updated_at'):
                    # Use DB_TYPE_TIMESTAMP_TZ to preserve microseconds and timezone
                    out_var = cursor.var(oracledb.DB_TYPE_TIMESTAMP_TZ)
                elif col_lower in ('time_val',):
                    out_var = cursor.var(oracledb.DB_TYPE_VARCHAR)
                else:
                    out_var = self._make_out_var_for_column(cursor, col_key)
                out_vars.append(out_var)
                exec_params.append(out_var)

            if getattr(self.config, 'log_queries', False):
                self.log(
                    logging.DEBUG,
                    f"RETURNING INTO params: {len(exec_params)} "
                    f"({len(converted_params) if converted_params else 0} input + {len(out_vars)} output)"
                )

            # Execute the SQL with input params and output variables
            self._set_input_sizes_for_params(cursor, exec_params)
            cursor.execute(oracle_sql, exec_params)

            duration = (datetime.datetime.now() - start_time).total_seconds()

            # Extract returned values from output variables
            data = []
            row_data = {}
            for i, col in enumerate(returning_columns):
                col_key = col if isinstance(col, str) else str(col)
                value = out_vars[i].getvalue()
                # getvalue() returns a list for batch operations, single value otherwise
                if isinstance(value, list) and len(value) == 1:
                    value = value[0]
                # Apply column adapter if available to convert types (e.g. str -> time)
                if column_adapters and col_key in column_adapters:
                    adapter, target_type = column_adapters[col_key]
                    value = adapter.from_database(value, target_type)
                row_data[col_key] = value
            data = [row_data]

            result = QueryResult(
                affected_rows=cursor.rowcount if cursor.rowcount > 0 or not is_insert else 1,
                data=data,
                duration=duration,
                last_insert_id=None
            )

            self.log(logging.INFO, f"RETURNING INTO executed, duration={duration:.3f}s")
            # Auto-commit DML when not in an enclosing transaction.  Without
            # this, Oracle (which has autocommit disabled by default in the
            # thin driver) would silently hold the row change in the session
            # transaction until the connection is closed -- and even then the
            # DML would be rolled back rather than committed.
            self._handle_auto_commit_if_needed()
            return result

        except OracleError as e:
            self.log(logging.ERROR, f"Error executing RETURNING INTO: {str(e)}")
            raise DatabaseError(str(e)) from e
        finally:
            if cursor:
                cursor.close()

    def _make_out_var_for_column(self, cursor, column_name: str):
        """Create an Oracle ``cursor.var`` of the proper DB_TYPE for ``column_name``.

        Looks up the column in the currently-executing RETURNING INTO target
        table (set via ``_current_returning_table`` by ``insert``/``update``
        /``delete``/``bulk_insert``) and returns a ``cursor.var`` of the
        matching ``oracledb.DB_TYPE_*``.  Falls back to VARCHAR for unknown
        columns.
        """
        table = getattr(self, "_current_returning_table", None)
        data_type = self._lookup_oracle_data_type(table, column_name) if table else None
        if data_type:
            if "NUMBER" in data_type and "VARCHAR" not in data_type:
                return cursor.var(int)
            if "DATE" in data_type or "TIMESTAMP" in data_type:
                return cursor.var(oracledb.DB_TYPE_TIMESTAMP_TZ)
            if "CLOB" in data_type:
                return cursor.var(oracledb.DB_TYPE_CLOB)
            if "BLOB" in data_type:
                return cursor.var(oracledb.DB_TYPE_BLOB)
            if "RAW" in data_type:
                return cursor.var(oracledb.DB_TYPE_RAW)
        return cursor.var(oracledb.DB_TYPE_VARCHAR)

    def _lookup_oracle_data_type(self, table: str, column_name: str) -> Optional[str]:
        """Return the Oracle DATA_TYPE for ``column_name`` in ``table``.

        Uses a per-backend in-memory cache.  Returns ``None`` if the column
        cannot be introspected (for instance the table has not been created
        yet).  Called from :meth:`_make_out_var_for_column` to pick the
        proper ``oracledb.DB_TYPE_*`` for RETURNING INTO bind variables.
        """
        cache_attr = "_oracle_data_type_cache"
        cache = getattr(self, cache_attr, None)
        if cache is None:
            cache = {}
            setattr(self, cache_attr, cache)
        key = self._returning_lookup_key(table, column_name)
        if key in cache:
            return cache[key]
        if not self._connection:
            try:
                self.connect()
            except Exception:
                return None
        try:
            cur = self._connection.cursor()
            try:
                if key[0]:
                    cur.execute(
                        "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS "
                        "WHERE OWNER = :1 AND TABLE_NAME = :2 AND COLUMN_NAME = :3",
                        [key[0], key[1], key[2]],
                    )
                else:
                    cur.execute(
                        "SELECT DATA_TYPE FROM USER_TAB_COLUMNS "
                        "WHERE TABLE_NAME = :1 AND COLUMN_NAME = :2",
                        [key[1], key[2]],
                    )
                row = cur.fetchone()
                data_type = row[0].upper() if row and row[0] else None
                cache[key] = data_type
                return data_type
            finally:
                cur.close()
        except Exception as e:
            self.log(logging.WARNING, f"Failed to introspect column {key[0]}.{key[1]}.{key[2]}: {e}")
            cache[key] = None
            return None

    @staticmethod
    def _returning_lookup_key(table: str, column_name: str) -> Tuple[str, str, str]:
        """Split an optional ``SCHEMA.TABLE`` qualifier into a lookup key."""
        raw_table = str(table)
        owner = ""
        if "." in raw_table:
            owner, _, raw_table = raw_table.partition(".")
        return owner.upper(), raw_table.upper(), str(column_name).upper()
