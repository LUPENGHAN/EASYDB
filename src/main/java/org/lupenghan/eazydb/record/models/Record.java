package org.lupenghan.eazydb.record.models;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class Record {
    int length;              // 记录长度

    byte status;       // 0: 有效, 1: 删除, 2: 已更新

    long xid;                 // 所属事务 ID

    long beginTS;             // 可选：版本开始时间戳
    long endTS;               // 可选：版本结束时间戳

    long prevVersionPointer; // 上一版本的指针，可为 PageID+Offset 或 UndoLogRef

    byte[] nullBitmap;        // null 位图（变长字段可为空）
    short[] fieldOffsets;     // 每个字段起始位置（变长字段支持）

    byte[] data;              // 实际数据（变长字段拼接）

    // 记录引用
    int pageId;               // 记录所在页面ID
    int slotId;               // 记录所在槽位ID

    //用于B+树
    private Map<String, Object> fields = new HashMap<>();

}
