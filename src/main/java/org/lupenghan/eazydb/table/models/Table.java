package org.lupenghan.eazydb.table.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 表结构定义类（用于 Catalog 持久化表结构）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Table {
    private String name;                    // 表名
    private List<Column> columns;           // 字段定义列表
    private String primaryKey;              // 主键字段名（简化处理）
    private List<String> uniqueKeys;        // 唯一约束字段（支持多个）
    private List<ForeignKey> foreignKeys;   // 外键约束列表
    private long createdTime;               // 创建时间戳
    private long lastModifiedTime;          // 最后修改时间
    private int rowCount;                   // 行数统计（可选）
    private int pageCount;                  // 页数统计（可选）
}
