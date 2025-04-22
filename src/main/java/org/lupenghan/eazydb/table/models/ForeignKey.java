package org.lupenghan.eazydb.table.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ForeignKey {
    private String columnName;         // 本表中的字段名
    private String referencedTable;    // 引用的目标表名
    private String referencedColumn;   // 引用的目标列名
    private String constraintName;     // 外键约束名
}