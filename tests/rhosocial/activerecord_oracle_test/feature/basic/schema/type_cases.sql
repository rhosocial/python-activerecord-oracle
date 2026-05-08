-- Oracle version of the type_cases table schema

CREATE TABLE type_cases (
    id CHAR(36) NOT NULL PRIMARY KEY,
    username VARCHAR2(4000) NOT NULL,
    email VARCHAR2(4000) NOT NULL,
    tiny_int NUMBER(3),
    small_int NUMBER(5),
    big_int NUMBER(19),
    float_val BINARY_FLOAT,
    double_val BINARY_DOUBLE,
    decimal_val NUMBER(20,4),
    char_val CHAR(10),
    varchar_val VARCHAR2(4000),
    text_val CLOB,
    date_val DATE,
    time_val VARCHAR2(20),
    timestamp_val TIMESTAMP WITH TIME ZONE,
    blob_val BLOB,
    json_val JSON,
    array_val JSON,
    is_active NUMBER(1)
) LOB(text_val) STORE AS BASICFILE (TABLESPACE SYSTEM),
  LOB(blob_val) STORE AS BASICFILE (TABLESPACE SYSTEM),
  LOB(json_val) STORE AS BASICFILE (TABLESPACE SYSTEM),
  LOB(array_val) STORE AS BASICFILE (TABLESPACE SYSTEM)
