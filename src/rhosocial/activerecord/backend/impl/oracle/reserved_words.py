# src/rhosocial/activerecord/backend/impl/oracle/reserved_words.py
"""
Oracle reserved words list.

Source: Oracle 23c Documentation
"""

ORACLE_RESERVED_WORDS = frozenset({
    "access", "add", "all", "alter", "and", "any", "as", "asc", "audit",
    "between", "by", "char", "check", "cluster", "column", "comment",
    "compress", "connect", "create", "current", "date", "decimal", "default",
    "delete", "desc", "distinct", "drop", "else", "exclusive", "exists",
    "file", "float", "for", "from", "grant", "group", "having", "identified",
    "immediate", "in", "increment", "index", "initial", "insert", "integer",
    "intersect", "into", "is", "level", "like", "lock", "long", "maxextents",
    "minus", "mode", "modify", "noaudit", "not", "nowait", "null", "number",
    "of", "offline", "on", "online", "option", "or", "order", "pctfree",
    "prior", "public", "raw", "rename", "resource", "revoke", "row",
    "rowid", "rownum", "rows", "select", "session", "set", "share", "size",
    "smallint", "start", "successful", "synonym", "table", "then", "to",
    "trigger", "uid", "union", "unique", "update", "user", "validate",
    "values", "varchar", "varchar2", "view", "when", "whenever", "where",
    "with",
})
