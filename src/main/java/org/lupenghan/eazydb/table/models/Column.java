package org.lupenghan.eazydb.table.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 列定义类
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Column {
    private String name;           // 列名
    private DataType type;         // 数据类型（枚举）
    private int length;            // 字节长度（对 VARCHAR 或 BINARY 有效）
    private boolean nullable;      // 是否允许 NULL
    private boolean isPrimary;     // 是否是主键
    private String defaultValue;   // 默认值（字符串形式保存）
    private String checkExpr;      // check 约束表达式（可选）
}