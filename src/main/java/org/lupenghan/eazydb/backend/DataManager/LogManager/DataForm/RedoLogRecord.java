package org.lupenghan.eazydb.backend.DataManager.LogManager.DataForm;

import java.nio.ByteBuffer;

public class RedoLogRecord {
    private long xid;           // 事务ID
    private int pageID;         // 页面ID
    private short offset;       // 页内偏移量
    private byte[] oldData;     // 修改前数据
    private byte[] newData;     // 修改后数据

    // 构造函数
    public RedoLogRecord() {
        this.oldData = new byte[0];
        this.newData = new byte[0];
    }

    public RedoLogRecord(long xid, int pageID, short offset, byte[] oldData, byte[] newData) {
        this.xid = xid;
        this.pageID = pageID;
        this.offset = offset;
        this.oldData = oldData != null ? oldData : new byte[0];
        this.newData = newData != null ? newData : new byte[0];
    }

    // Getters
    public long getXid() {
        return xid;
    }

    public int getPageID() {
        return pageID;
    }

    public short getOffset() {
        return offset;
    }

    public byte[] getOldData() {
        return oldData;
    }

    public byte[] getNewData() {
        return newData;
    }

    // 序列化方法
    public byte[] serialize() {
        // 计算总长度：xid(8) + pageID(4) + offset(2) + oldData长度(4) + newData长度(4) + oldData + newData
        int length = 8 + 4 + 2 + 4 + 4 + oldData.length + newData.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);

        buffer.putLong(xid);
        buffer.putInt(pageID);
        buffer.putShort(offset);
        buffer.putInt(oldData.length);
        buffer.putInt(newData.length);
        buffer.put(oldData);
        buffer.put(newData);

        return buffer.array();
    }

    // 反序列化方法
    public static RedoLogRecord deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        long xid = buffer.getLong();
        int pageID = buffer.getInt();
        short offset = buffer.getShort();

        int oldDataLen = buffer.getInt();
        int newDataLen = buffer.getInt();

        byte[] oldData = new byte[oldDataLen];
        byte[] newData = new byte[newDataLen];

        buffer.get(oldData);
        buffer.get(newData);

        return new RedoLogRecord(xid, pageID, offset, oldData, newData);
    }
}