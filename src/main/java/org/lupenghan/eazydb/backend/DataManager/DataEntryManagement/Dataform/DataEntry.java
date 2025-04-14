package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * 数据条目类，表示数据库中存储的一个数据项
 */
@Getter
@Setter
public class DataEntry {
    // 数据条目状态
    public static final byte STATUS_VALID = 0;    // 有效
    public static final byte STATUS_DELETED = 1;  // 已删除
    public static final byte STATUS_UPDATED = 2;  // 已更新

    // 头部字段大小
    private static final int LENGTH_SIZE = 2;     // 长度(2字节)
    private static final int STATUS_SIZE = 1;     // 状态(1字节)
    private static final int VERSION_SIZE = 8;    // 版本号(8字节)
    private static final int XID_SIZE = 8;        // 事务ID(8字节)

    // 头部总大小
    public static final int HEADER_SIZE = LENGTH_SIZE + STATUS_SIZE + VERSION_SIZE + XID_SIZE;


    // 条目字段
    private short length;        // 条目总长度

    private byte status;         // 状态（有效、已删除等）

    private long version;        // 版本号（用于MVCC）

    private long xid;            // 创建此版本的事务ID

    private byte[] data;         // 实际数据内容

    /**
     * 创建一个空的数据条目
     */
    public DataEntry() {
        this.length = HEADER_SIZE;
        this.status = STATUS_VALID;
        this.version = 0;
        this.xid = 0;
        this.data = new byte[0];
    }

    /**
     * 创建数据条目
     * @param status 状态
     * @param version 版本号
     * @param xid 事务ID
     * @param data 数据内容
     */
    public DataEntry(byte status, long version, long xid, byte[] data) {
        this.status = status;
        this.version = version;
        this.xid = xid;
        this.data = data != null ? data : new byte[0];
        this.length = (short) (HEADER_SIZE + this.data.length);
    }

    /**
     * 设置数据内容
     * @param data 数据内容
     */
    public void setData(byte[] data) {
        this.data = data != null ? data : new byte[0];
        this.length = (short) (HEADER_SIZE + this.data.length);
    }

    /**
     * 序列化数据条目为字节数组
     * @return 序列化后的字节数组
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.putShort(length);
        buffer.put(status);
        buffer.putLong(version);
        buffer.putLong(xid);
        buffer.put(data);
        return buffer.array();
    }

    /**
     * 从字节数组反序列化数据条目
     * @param bytes 序列化的字节数组
     * @return 数据条目对象
     */
    public static DataEntry deserialize(byte[] bytes) {
        if (bytes == null || bytes.length < HEADER_SIZE) {
            throw new IllegalArgumentException("数据长度不足以包含条目头部");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        short length = buffer.getShort();
        byte status = buffer.get();
        long version = buffer.getLong();
        long xid = buffer.getLong();

        // 提取数据部分
        int dataLength = length - HEADER_SIZE;
        byte[] data = new byte[dataLength];
        buffer.get(data);

        return new DataEntry(status, version, xid, data);
    }

    /**
     * 判断条目是否有效
     * @return 如果条目有效则返回true
     */
    public boolean isValid() {
        return status == STATUS_VALID;
    }

    /**
     * 判断条目是否已删除
     * @return 如果条目已删除则返回true
     */
    public boolean isDeleted() {
        return status == STATUS_DELETED;
    }

    @Override
    public String toString() {
        return "DataEntry{length=" + length + ", status=" + status +
                ", version=" + version + ", xid=" + xid + ", dataSize=" +
                (data != null ? data.length : 0) + "}";
    }
}