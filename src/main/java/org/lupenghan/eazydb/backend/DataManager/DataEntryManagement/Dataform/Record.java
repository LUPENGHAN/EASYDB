package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform;

import lombok.Data;

@Data
public class Record {
    private RecordID rid;      // 记录ID
    private byte[] data;       // 记录数据
    private byte status;       // 记录状态（有效、删除等）
    private long versionTS;    // 版本时间戳（MVCC使用）
    private long xid;          // 最后修改事务ID

    // 构造函数和访问器
}