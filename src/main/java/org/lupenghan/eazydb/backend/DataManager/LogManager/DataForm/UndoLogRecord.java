package org.lupenghan.eazydb.backend.DataManager.LogManager.DataForm;

import lombok.Getter;

import java.nio.ByteBuffer;
@Getter
public class UndoLogRecord {
    // 操作类型常量
    public static final int INSERT = 0;
    public static final int DELETE = 1;
    public static final int UPDATE = 2;

    private long xid;           // 事务ID
    private int operationType;  // 操作类型（插入、删除、更新等）
    private byte[] undoData;    // 撤销数据（足够的信息来撤销操作）

    // 构造函数
    public UndoLogRecord() {}

    public UndoLogRecord(long xid, int operationType, byte[] undoData) {
        this.xid = xid;
        this.operationType = operationType;
        this.undoData = undoData;
    }


    // 序列化方法
    public byte[] serialize() {
        // 计算总长度：xid(8) + operationType(4) + undoData长度(4) + undoData
        int length = 8 + 4 + 4 + undoData.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);

        buffer.putLong(xid);
        buffer.putInt(operationType);
        buffer.putInt(undoData.length);
        buffer.put(undoData);

        return buffer.array();
    }

    // 反序列化方法
    public static UndoLogRecord deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        UndoLogRecord record = new UndoLogRecord();

        record.xid = buffer.getLong();
        record.operationType = buffer.getInt();

        int undoDataLen = buffer.getInt();
        record.undoData = new byte[undoDataLen];
        buffer.get(record.undoData);

        return record;
    }
}