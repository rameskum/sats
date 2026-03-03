package com.sats.domain.enums;

/**
 * Supported target column data types, aligned with Spark SQL types.
 */
public enum DataType {
    STRING,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    TIMESTAMP,
    DATE,
    BINARY,
    STRUCT,
    ARRAY,
    MAP
}
