package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform;

import lombok.Getter;
import lombok.Setter;

/**
 * 表示记录的一个特定版本
 */
@Getter
@Setter
public class RecordVersion {
    private RecordID recordID;      // 记录ID
    private long xid;               // 创建该版本的事务ID
    private long beginTS;           // 版本开始时间戳
    private long endTS;             // 版本结束时间戳
    private byte[] data;            // 数据内容
    private byte status;            // 记录状态
    private long prevVersionPointer; // 前一版本指针

    /**
     * 创建一个记录版本
     * @param recordID 记录ID
     * @param xid 事务ID
     * @param beginTS 开始时间戳
     * @param endTS 结束时间戳
     * @param data 数据内容
     * @param status 记录状态
     * @param prevVersionPointer 前一版本指针
     */
    public RecordVersion(RecordID recordID, long xid, long beginTS, long endTS,
                         byte[] data, byte status, long prevVersionPointer) {
        this.recordID = recordID;
        this.xid = xid;
        this.beginTS = beginTS;
        this.endTS = endTS;
        this.data = data;
        this.status = status;
        this.prevVersionPointer = prevVersionPointer;
    }

    /**
     * 从记录数据创建记录版本
     * @param recordID 记录ID
     * @param recordData 完整记录数据
     * @return 记录版本
     */
    public static RecordVersion fromRecord(RecordID recordID, byte[] recordData) {
        RecordHeader header = RecordHeader.fromRecord(recordData);

        // 提取数据部分
        byte[] data = new byte[header.getDataLength()];
        System.arraycopy(recordData, RecordHeader.HEADER_SIZE, data, 0, data.length);

        return new RecordVersion(
                recordID,
                header.getXid(),
                header.getBeginTS(),
                header.getEndTS(),
                data,
                header.getStatus(),
                header.getPrevVersionPointer()
        );
    }

    /**
     * 将版本转换为完整记录数据
     * @return 完整记录数据
     */
    public byte[] toRecord() {
        short length = (short) (RecordHeader.HEADER_SIZE + data.length);
        RecordHeader header = new RecordHeader(
                length,
                status,
                xid,
                beginTS,
                endTS,
                prevVersionPointer
        );

        byte[] headerBytes = header.serialize();
        byte[] result = new byte[length];

        System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
        System.arraycopy(data, 0, result, headerBytes.length, data.length);

        return result;
    }

    /**
     * 判断记录是否有效
     * @return 如果记录有效则返回true
     */
    public boolean isValid() {
        return status == RecordHeader.VALID;
    }

    /**
     * 判断记录是否已删除
     * @return 如果记录已删除则返回true
     */
    public boolean isDeleted() {
        return status == RecordHeader.DELETED;
    }

    @Override
    public String toString() {
        return "RecordVersion{" +
                "recordID=" + recordID +
                ", xid=" + xid +
                ", beginTS=" + beginTS +
                ", endTS=" + endTS +
                ", status=" + status +
                ", dataSize=" + (data != null ? data.length : 0) +
                ", prevVersionPointer=" + prevVersionPointer +
                '}';
    }
}