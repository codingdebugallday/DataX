package org.abigballofmud.datax.plugin.writer.hdfspluswriter.enums;

/**
 * <p>description</p>
 *
 * @author abigballofmud 2019-9-25 14:01:31
 */
public enum SupportHiveDataType {
    /**
     * number
     */
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,

    /**
     * date
     */
    TIMESTAMP,
    DATE,

    /**
     * string
     */
    STRING,
    VARCHAR,
    CHAR,
    BOOLEAN;
}
