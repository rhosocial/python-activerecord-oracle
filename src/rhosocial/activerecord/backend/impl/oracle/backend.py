# src/rhosocial/activerecord/backend/impl/oracle/backend.py
"""
Oracle-specific implementation of the StorageBackend.

This module provides the concrete implementation for interacting with Oracle databases,
handling connections, queries, transactions, and type adaptations tailored for Oracle's
specific behaviors and SQL dialect.
"""
import datetime
import logging
from typing import List, Optional, Tuple

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
from rhosocial.activerecord.backend.introspection.backend_mixin import IntrospectorBackendMixin
from .config import OracleConnectionConfig
from .dialect import OracleDialect
from .transaction import OracleTransactionManager
from .mixins import OracleBackendMixin


class OracleBackend(IntrospectorBackendMixin, OracleBackendMixin, StorageBackend):
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

    def _handle_error(self, error: Exception) -> None:
        """Handle Oracle-specific errors."""
        error_msg = str(error)

        if isinstance(error, OracleIntegrityError):
            if "ORA-00001" in error_msg:  # Unique constraint violation
                self.log(logging.ERROR, f"Unique constraint violation: {error_msg}")
                raise IntegrityError(f"Unique constraint violation: {error_msg}")
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
        """Establish connection to Oracle database."""
        try:
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

            # Disable SECUREFILE LOB to support SYSTEM tablespace (non-ASSM) on Oracle 21c XE
            cursor = self._connection.cursor()
            cursor.execute("ALTER SESSION SET db_securefile = 'NEVER'")
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
        Convert datetime parameters to use Oracle DB_TYPE_TIMESTAMP_TZ.

        This is necessary to preserve microseconds and timezone information
        when binding datetime values to Oracle.

        Args:
            params: Original parameter tuple

        Returns:
            Tuple with datetime values converted to oracledb variables
        """
        from datetime import datetime

        converted = []
        cursor = None
        try:
            for param in params:
                if isinstance(param, datetime):
                    # Get a cursor for creating variables (reuse if possible)
                    if cursor is None:
                        cursor = self._get_cursor()
                    # Create a variable with DB_TYPE_TIMESTAMP_TZ to preserve microseconds
                    var = cursor.var(oracledb.DB_TYPE_TIMESTAMP_TZ)
                    var.setvalue(0, param)
                    converted.append(var)
                else:
                    converted.append(param)
            return tuple(converted)
        finally:
            # Close the temporary cursor if we created one
            if cursor:
                cursor.close()

    def execute(self, sql: str, params: Optional[Tuple] = None, *, options, **kwargs) -> QueryResult:
        """Execute a SQL statement with optional parameters.

        Args:
            sql: SQL string with ? placeholders
            params: Parameter tuple
            options: ExecutionOptions (REQUIRED - must include stmt_type)

        Returns:
            QueryResult with execution results
        """
        from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType

        if options is None:
            raise ValueError("options parameter is required with stmt_type specified")

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

            # Process result set using parent's method for type adaptation
            data = self._process_result_set(cursor, is_select, column_adapters, column_mapping)

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

            return result

        except OracleIntegrityError as e:
            self.log(logging.ERROR, f"Integrity error: {str(e)}")
            raise IntegrityError(str(e)) from e
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

            affected_rows = 0
            for params in params_list:
                cursor.execute(sql, params)
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

    def executescript(self, sql_script: str) -> None:
        """Execute a multi-statement SQL script."""
        self.log(logging.INFO, "Executing SQL script.")
        start_time = datetime.datetime.now()

        if not self._connection:
            self.connect()

        # Oracle doesn't have a direct multi-statement execute
        # Split by semicolons and execute each statement
        statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]

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

    def insert(self, options) -> 'QueryResult':
        """
        Insert a record with special handling for Oracle RETURNING INTO clause.

        Oracle requires RETURNING ... INTO syntax with output bind variables.
        This method uses the Expression-Dialect pattern to generate proper Oracle SQL.
        """
        from rhosocial.activerecord.backend.result import QueryResult
        from rhosocial.activerecord.backend.base.operations import SQLOperationsMixin, _is_sql_expression
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

        # Create ReturningClause if specified
        returning_clause = None
        if options.returning_columns:
            returning_expressions = [ExprColumn(self.dialect, col) for col in options.returning_columns]
            returning_clause = ReturningClause(self.dialect, returning_expressions)

        # Create InsertExpression and generate SQL
        insert_expr = InsertExpression(
            dialect=self.dialect,
            into=options.table,
            source=values_source,
            columns=list(options.data.keys()),
            returning=returning_clause,
        )

        sql, params = insert_expr.to_sql()

        # Handle RETURNING INTO clause
        if options.returning_columns:
            return self._execute_with_returning_into(
                sql, params, options.returning_columns,
                options.column_adapters, options.column_mapping
            )

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
        update_expr = UpdateExpression(
            dialect=self.dialect,
            table=options.table,
            assignments=assignments,
            where=options.where,
            returning=returning_clause,
        )

        sql, params = update_expr.to_sql()

        # Handle RETURNING INTO clause
        if options.returning_columns:
            return self._execute_with_returning_into(
                sql, params, options.returning_columns,
                options.column_adapters, options.column_mapping
            )

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
        delete_expr = DeleteExpression(
            dialect=self.dialect,
            tables=options.table,
            where=options.where,
            returning=returning_clause,
        )

        sql, params = delete_expr.to_sql()

        # Handle RETURNING INTO clause
        if options.returning_columns:
            return self._execute_with_returning_into(
                sql, params, options.returning_columns,
                options.column_adapters, options.column_mapping
            )

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
                                       column_adapters=None, column_mapping=None) -> 'QueryResult':
        """
        Execute INSERT/UPDATE/DELETE with RETURNING INTO clause using Oracle's output variables.

        Oracle requires: RETURNING col1, col2 INTO :out1, :out2
        This method:
        1. Adds the INTO clause with placeholders
        2. Converts all ? placeholders to Oracle :N format
        3. Creates output variables with cursor.var()
        4. Executes and retrieves returned values
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
            for i, col in enumerate(returning_columns):
                col_lower = col.lower() if isinstance(col, str) else str(col).lower()
                # Determine appropriate type based on column name/type
                if col_lower == 'id':
                    out_var = cursor.var(int)
                elif col_lower in ('created_at', 'updated_at'):
                    # Use DB_TYPE_TIMESTAMP_TZ to preserve microseconds and timezone
                    out_var = cursor.var(oracledb.DB_TYPE_TIMESTAMP_TZ)
                else:
                    out_var = cursor.var(str)
                out_vars.append(out_var)
                exec_params.append(out_var)

            if getattr(self.config, 'log_queries', False):
                self.log(logging.DEBUG, f"RETURNING INTO params: {len(exec_params)} ({len(converted_params) if converted_params else 0} input + {len(out_vars)} output)")

            # Execute the SQL with input params and output variables
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
                row_data[col_key] = value
            data = [row_data]

            result = QueryResult(
                affected_rows=cursor.rowcount if cursor.rowcount > 0 else 1,
                data=data,
                duration=duration,
                last_insert_id=None
            )

            self.log(logging.INFO, f"RETURNING INTO executed, duration={duration:.3f}s")
            return result

        except OracleError as e:
            self.log(logging.ERROR, f"Error executing RETURNING INTO: {str(e)}")
            raise DatabaseError(str(e)) from e
        finally:
            if cursor:
                cursor.close()
