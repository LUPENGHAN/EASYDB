package org.lupenghan.eazydb.table.models;

import lombok.Getter;

/**
 * 数据类型枚举
 */
@Getter
public enum DataType {
    INT(4),
    BIGINT(8),
    FLOAT(4),
    DOUBLE(8),
    BOOLEAN(1),
    VARCHAR(-1),
    DATE(4),
    TIMESTAMP(8);


    private final int defaultLength;

    DataType(int defaultLength) {
        this.defaultLength = defaultLength;
    }

    public boolean isVariableLength() {
        return this == VARCHAR;
    }

}
