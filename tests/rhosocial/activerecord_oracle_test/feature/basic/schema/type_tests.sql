-- Oracle version of the type_tests table schema

CREATE TABLE type_tests (
    id CHAR(36) NOT NULL PRIMARY KEY,
    string_field VARCHAR2(255) DEFAULT 'test string' NOT NULL,
    int_field NUMBER DEFAULT 42 NOT NULL,
    float_field BINARY_FLOAT DEFAULT 3.14 NOT NULL,
    decimal_field BINARY_DOUBLE DEFAULT 10.99 NOT NULL,
    bool_field NUMBER(1) DEFAULT 1 NOT NULL,
    datetime_field VARCHAR2(4000) NOT NULL,
    json_field VARCHAR2(4000),
    nullable_field VARCHAR2(255)
)
