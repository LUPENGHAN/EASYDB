package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform;

public class RecordHeader {
    // 记录状态常量
    public static final byte VALID = 0;
    public static final byte DELETED = 1;

    private short length;       // 记录总长度
    private byte status;        // 记录状态
    private long xid;           // 事务ID

    // 序列化和反序列化方法
    public byte[] serialize() {
        return null;
    }
    public static RecordHeader deserialize(byte[] data) {
        return null;
    }
}
