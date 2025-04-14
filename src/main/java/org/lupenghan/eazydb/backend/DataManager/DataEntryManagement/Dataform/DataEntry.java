package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform;

import lombok.Data;

@Data
public class DataEntry {
    private short length;        // 条目总长度
    private byte status;         // 状态（有效、已删除等）
    private long version;        // 版本号（用于MVCC）
    private long xid;            // 创建此版本的事务ID
    private byte[] data;         // 实际数据内容

    // 构造函数、访问器等
}