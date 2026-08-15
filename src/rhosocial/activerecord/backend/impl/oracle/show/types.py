# src/rhosocial/activerecord/backend/impl/oracle/show/types.py
"""
Oracle data dictionary introspection result types.

This module defines result dataclasses for Oracle data dictionary queries.
These types mirror the structure returned by USER_/ALL_/DBA_/V$ views.

The dataclasses intentionally use plain Python types (str, int, Optional).
Backend drivers are expected to convert Oracle native types (NUMBER, DATE,
TIMESTAMP, CLOB) to Python equivalents before constructing these objects.
"""

from dataclasses import dataclass, field
from typing import List, Optional


# ==================== Session / Process Results ====================


@dataclass
class OracleSessionInfo:
    """Row from V$SESSION.

    Attributes:
        sid: Session identifier.
        serial#: Session serial number (unique per sid).
        username: Oracle username (NULL for background processes).
        status: Active/INACTIVE/KILLED.
        machine: Client machine name.
        program: Client program name.
        module: Application module name (DBMS_APPLICATION_INFO).
        action: Application action name.
        logon_time: Session logon timestamp (ISO string when serialized).
        process: Client process OS pid.
    """

    sid: int
    serial_num: int
    username: Optional[str] = None
    status: Optional[str] = None
    machine: Optional[str] = None
    program: Optional[str] = None
    module: Optional[str] = None
    action: Optional[str] = None
    logon_time: Optional[str] = None
    process: Optional[str] = None


@dataclass
class OracleProcessInfo:
    """Row from V$PROCESS."""

    spid: Optional[str]
    pid: int
    program: Optional[str] = None
    background: Optional[str] = None


# ==================== Running SQL Results ====================


@dataclass
class OracleSqlInfo:
    """Row from V$SQL joined with V$SESSION."""

    sql_id: Optional[str]
    child_number: int
    sql_text: Optional[str]
    executions: int
    elapsed_seconds: float
    cpu_seconds: float
    buffer_gets: int
    disk_reads: int
    rows_processed: int
    parsing_schema_name: Optional[str] = None
    last_active_time: Optional[str] = None


# ==================== Database / Instance Results ====================


@dataclass
class OracleDatabaseInfo:
    """Row from V$DATABASE."""

    name: str
    dbid: int
    created: Optional[str] = None
    log_mode: Optional[str] = None
    open_mode: Optional[str] = None
    platform_name: Optional[str] = None
    role: Optional[str] = None


@dataclass
class OracleInstanceInfo:
    """Row from V$INSTANCE."""

    instance_name: str
    instance_num: int
    host_name: Optional[str] = None
    version: Optional[str] = None
    status: Optional[str] = None
    startup_time: Optional[str] = None
    archiver: Optional[str] = None


# ==================== Object / Schema Results ====================


@dataclass
class OracleObjectInfo:
    """Row from USER_OBJECTS / ALL_OBJECTS."""

    object_name: str
    object_type: str
    owner: Optional[str] = None
    object_id: Optional[int] = None
    created: Optional[str] = None
    last_ddl_time: Optional[str] = None
    status: Optional[str] = None
    temporary: Optional[str] = None


@dataclass
class OracleTableInfo:
    """Row from USER_TABLES / ALL_TABLES."""

    table_name: str
    owner: Optional[str] = None
    tablespace_name: Optional[str] = None
    num_rows: Optional[int] = None
    blocks: Optional[int] = None
    last_analyzed: Optional[str] = None
    partitioned: Optional[str] = None


@dataclass
class OracleColumnInfo:
    """Row from USER_TAB_COLUMNS / ALL_TAB_COLUMNS."""

    table_name: str
    column_name: str
    owner: Optional[str] = None
    data_type: Optional[str] = None
    data_length: Optional[int] = None
    data_precision: Optional[int] = None
    data_scale: Optional[int] = None
    nullable: Optional[str] = None
    column_id: Optional[int] = None
    data_default: Optional[str] = None


# ==================== Index / Constraint Results ====================


@dataclass
class OracleIndexInfo:
    """Row from USER_INDEXES / ALL_INDEXES."""

    index_name: str
    table_name: str
    owner: Optional[str] = None
    table_owner: Optional[str] = None
    index_type: Optional[str] = None
    uniqueness: Optional[str] = None
    tablespace_name: Optional[str] = None
    status: Optional[str] = None
    num_rows: Optional[int] = None


@dataclass
class OracleConstraintInfo:
    """Row from USER_CONSTRAINTS."""

    constraint_name: str
    constraint_type: Optional[str] = None
    table_name: Optional[str] = None
    owner: Optional[str] = None
    search_condition: Optional[str] = None
    status: Optional[str] = None
    delete_rule: Optional[str] = None


# ==================== Lock / Wait Event Results ====================


@dataclass
class OracleLockInfo:
    """Row from V$LOCK."""

    sid: int
    lock_type: Optional[str] = None
    id1: Optional[int] = None
    id2: Optional[int] = None
    lmode: Optional[int] = None
    request: Optional[int] = None
    block: Optional[int] = None


@dataclass
class OracleWaitEventInfo:
    """Row from V$SESSION_WAIT."""

    sid: int
    seq_num: int
    event: Optional[str] = None
    p1text: Optional[str] = None
    p1: Optional[int] = None
    wait_class: Optional[str] = None
    wait_time: Optional[int] = None
    seconds_in_wait: Optional[float] = None
    state: Optional[str] = None


# ==================== NLS Parameter Results ====================


@dataclass
class OracleNlsParameter:
    """Row from NLS_SESSION_PARAMETERS / NLS_DATABASE_PARAMETERS."""

    parameter: str
    value: Optional[str] = None


# ==================== Aggregate Container ====================


@dataclass
class OracleIntrospectionResult:
    """Generic container used by functionality helpers when packing lists."""

    items: List = field(default_factory=list)
    error: Optional[str] = None
