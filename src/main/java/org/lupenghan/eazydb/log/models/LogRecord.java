package org.lupenghan.eazydb.log.models;

import lombok.Data;

import java.nio.ByteBuffer;

/**
 * 日志记录 (LogRecord) 类，表示一条WAL日志记录，支持序列化和反序列化。
 */
@Data
public class LogRecord {
    // 日志记录序列号 (Log Sequence Number)
    private long lsn;
    // 日志类型 (用于区分不同类型的日志记录)
    private int type;
    // 日志的具体数据内容
    private byte[] data;

    // 构造函数：用于创建一个新的日志记录（LSN 在 append 时由 LogManager 分配）
    public LogRecord(int type, byte[] data) {
        this.lsn = -1; // 尚未分配LSN
        this.type = type;
        // 存储数据的副本以防止外部修改
        this.data = (data != null ? data.clone() : new byte[0]);
    }

    // 构造函数：用于从已有数据恢复日志记录（LSN 已知的情况）
    public LogRecord(long lsn, int type, byte[] data) {
        this.lsn = lsn;
        this.type = type;
        this.data = (data != null ? data.clone() : new byte[0]);
    }



    // 获取日志数据（返回数据副本以保证不可变性）
    public byte[] getData() {
        return data.clone();
    }

    // 计算此 LogRecord 序列化后内容占用的字节大小（不含记录长度前缀）
    // 序列化格式: [LSN(8 bytes) | Type(4 bytes) | Data(N bytes)]
    public int getContentSize() {
        return 8 /*LSN*/ + 4 /*Type*/ + data.length;
    }

    // 计算此 LogRecord 在日志页中存储时占用的总大小（包括长度字段）
    public int getTotalSize() {
        // total size = 记录长度前缀(4字节) + 内容长度
        return 4 + getContentSize();
    }

    // 将此 LogRecord 序列化写入到 ByteBuffer 中（假定 buffer 有足够空间）
    public void writeTo(ByteBuffer buffer) {
        // 序列化格式：
        // 4字节：记录内容长度 (不含这4字节长度本身)
        // 8字节：LSN
        // 4字节：类型
        // N字节：数据
        int contentSize = getContentSize();
        buffer.putInt(contentSize);      // 写入记录内容长度
        buffer.putLong(lsn);
        buffer.putInt(type);
        buffer.put(data);
    }

    // 从 ByteBuffer 中读出一个 LogRecord（当前位置应指向记录长度字段开头）
    public static LogRecord readFrom(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return null;
        }
        // 读出记录内容长度
        int contentSize = buffer.getInt();
        // 读取字段
        long lsn = buffer.getLong();
        int type = buffer.getInt();
        int dataLength = contentSize - 8 - 4;  // 内容长度减去LSN和type的字节数，得到数据长度
        byte[] data = new byte[dataLength];
        if (dataLength > 0) {
            buffer.get(data);
        }
        // 构造并返回 LogRecord 对象
        return new LogRecord(lsn, type, data);
    }
}
