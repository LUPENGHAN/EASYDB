package org.lupenghan.eazydb.log.models;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 日志页 (LogPage) 类，固定大小4KB，包含页头和若干日志记录。
 * 提供将多个 LogRecord 组织成页并序列化/反序列化的功能。
 */
public class LogPage {
    // 页面大小和页头大小常量
    public static final int PAGE_SIZE = 4096;
    private static final int HEADER_SIZE = 12; // 页头: pageLSN(8 bytes) + entryCount(4 bytes)

    // 获取当前页的最大LSN
    // 页头字段
    @Getter
    private long pageLSN;    // 当前页包含的最大日志记录LSN
    // 获取当前页的记录条数
    @Getter
    private int entryCount;  // 当前页包含的日志记录数

    // 页内的日志记录列表
    private List<LogRecord> logRecords;
    // 当前已使用的字节数（包括页头和记录），用于判断剩余空间
    private int usedBytes;

    // 构造函数：创建一个空的日志页
    public LogPage() {
        this.pageLSN = -1;
        this.entryCount = 0;
        this.logRecords = new ArrayList<>();
        this.usedBytes = HEADER_SIZE;
    }

    // 获取页面内所有日志记录的列表（返回列表副本，保证封装性）
    public List<LogRecord> getRecords() {
        return new ArrayList<>(logRecords);
    }

    // 判断新的日志记录是否可以放入当前页（即剩余空间是否足够）
    public boolean hasSpaceFor(LogRecord logRecord) {
        int recordSize = logRecord.getTotalSize();
        return usedBytes + recordSize <= PAGE_SIZE;
    }

    // 将一条日志记录添加到当前页（假定调用前已经通过 hasSpaceFor 判断有足够空间）
    public void addRecord(LogRecord logRecord) {
        long lsn = logRecord.getLsn();
        if (lsn < 0) {
            // 新日志记录应由 LogManagerImpl 先分配LSN
            throw new IllegalStateException("LogRecord must have a valid LSN before adding to page");
        }
        logRecords.add(logRecord);
        entryCount++;
        // 更新页头中的最大LSN（假定 LSN 单调递增）
        if (lsn > pageLSN) {
            pageLSN = lsn;
        }
        // 更新已使用空间
        usedBytes += logRecord.getTotalSize();
    }

    // 将整个 LogPage 序列化为固定大小(4096字节)的字节数组
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        // 写入页头信息
        buffer.putLong(pageLSN);
        buffer.putInt(entryCount);
        // 写入所有日志记录
        for (LogRecord logRecords : logRecords) {
            logRecords.serialize();
        }
        // ByteBuffer.allocate已将多余空间初始化为0，无需额外填充
        return buffer.array();
    }

    // 从字节数组反序列化出一个 LogPage 对象
    public static LogPage deserialize(byte[] pageData) {
        if (pageData.length != PAGE_SIZE) {
            throw new IllegalArgumentException("Invalid page data size");
        }
        ByteBuffer buffer = ByteBuffer.wrap(pageData);
        long pageLSN = buffer.getLong();
        int entryCount = buffer.getInt();
        LogPage page = new LogPage();
        page.pageLSN = pageLSN;
        page.entryCount = entryCount;
        page.logRecords = new ArrayList<>();
        page.usedBytes = HEADER_SIZE;
        // 顺序读取指定数量的日志记录
        for (int i = 0; i < entryCount; i++) {
            if (buffer.remaining() < 1 + 4 + 8) {
                throw new IllegalStateException("Not enough bytes to read log header");
            }

            // 读取头部
            byte type = buffer.get();
            int bodyLen = buffer.getInt();
            long lsn = buffer.getLong();

            if (buffer.remaining() < bodyLen) {
                throw new IllegalStateException("Not enough bytes to read log body");
            }

            byte[] body = new byte[bodyLen];
            buffer.get(body);

            // 组合成完整 entry
            ByteBuffer entryBuffer = ByteBuffer.allocate(1 + 4 + 8 + bodyLen);
            entryBuffer.put(type);
            entryBuffer.putInt(bodyLen);
            entryBuffer.putLong(lsn);
            entryBuffer.put(body);

            LogRecord logRecord = LogRecord.deserialize(entryBuffer.array());
            if (logRecord == null) {
                throw new IllegalStateException("Failed to deserialize log record");
            }
            logRecord.setLsn(lsn);

            page.logRecords.add(logRecord);

        }
        return page;
    }
}
