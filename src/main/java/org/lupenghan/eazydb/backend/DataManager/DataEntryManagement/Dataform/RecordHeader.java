package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * 记录头部类，定义记录的元数据
 */
@Setter
@Getter
public class RecordHeader {
    // 记录状态常量
    public static final byte VALID = 0;       // 有效记录
    public static final byte DELETED = 1;      // 已删除记录
    public static final byte PLACEHOLDER = 2;  // 占位符记录（用于MVCC）

    // 头部各字段的大小
    private static final int LENGTH_SIZE = 2;  // 长度字段占2字节
    private static final int STATUS_SIZE = 1;  // 状态字段占1字节
    private static final int XID_SIZE = 8;     // 事务ID字段占8字节

    // 头部总大小
    public static final int HEADER_SIZE = LENGTH_SIZE + STATUS_SIZE + XID_SIZE;


    // 头部字段
    private short length;     // 记录总长度（包括头部）
    private byte status;      // 记录状态
    private long xid;         // 事务ID
    private long beginTS;            // 版本创建时间戳
    private long endTS;              // 版本结束时间戳
    private long prevVersionPointer; // 指向前一个版本的指针

    // 特殊值
    public static final long INFINITY_TS = Long.MAX_VALUE; // 表示"无穷大"时间戳
    public static final long NULL_POINTER = -1;

    /**
     * 创建一个空的记录头部
     */
    public RecordHeader() {
        this.length = 0;
        this.status = VALID;
        this.xid = 0;
        this.beginTS = 0;
        this.endTS = INFINITY_TS;
        this.prevVersionPointer = NULL_POINTER;
    }

    /**
     * 创建记录头部
     * @param length 记录总长度
     * @param status 记录状态
     * @param xid 事务ID
     * @param beginTS 版本开始时间戳
     * @param endTS 版本结束时间戳
     * @param prevVersionPointer 前一版本指针
     */
    public RecordHeader(short length, byte status, long xid, long beginTS, long endTS, long prevVersionPointer) {
        this.length = length;
        this.status = status;
        this.xid = xid;
        this.beginTS = beginTS;
        this.endTS = endTS;
        this.prevVersionPointer = prevVersionPointer;
    }

    /**
     * 序列化记录头部为字节数组
     * @return 序列化后的字节数组
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.putShort(length);
        buffer.put(status);
        buffer.putLong(xid);
        buffer.putLong(beginTS);
        buffer.putLong(endTS);
        buffer.putLong(prevVersionPointer);
        return buffer.array();
    }

    /**
     * 从字节数组反序列化记录头部
     * @param data 序列化的字节数组
     * @return 记录头部对象
     */
    public static RecordHeader deserialize(byte[] data) {
        if (data.length < HEADER_SIZE) {
            throw new IllegalArgumentException("数据长度不足以包含记录头部");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        short length = buffer.getShort();
        byte status = buffer.get();
        long xid = buffer.getLong();
        long beginTS = buffer.getLong();
        long endTS = buffer.getLong();
        long prevVersionPointer = buffer.getLong();

        return new RecordHeader(length, status, xid, beginTS, endTS, prevVersionPointer);
    }

    /**
     * 从记录数据中提取记录头部
     * @param recordData 完整的记录数据
     * @return 记录头部对象
     */
    public static RecordHeader fromRecord(byte[] recordData) {
        return deserialize(recordData);
    }

    /**
     * 计算数据部分的长度
     * @return 数据部分长度
     */
    public int getDataLength() {
        return length - HEADER_SIZE;
    }

    @Override
    public String toString() {
        return "RecordHeader{length=" + length +
                ", status=" + status +
                ", xid=" + xid +
                ", beginTS=" + beginTS +
                ", endTS=" + endTS +
                ", prevVersionPointer=" + prevVersionPointer + "}";
    }
}