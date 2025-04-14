package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform;

import lombok.Getter;
import lombok.Setter;

/**
 * 记录类，表示数据库中的一条记录
 */
@Setter
@Getter
public class Record {
    // 记录状态常量
    public static final byte STATUS_VALID = 0;    // 有效
    public static final byte STATUS_DELETED = 1;  // 已删除
    public static final byte STATUS_UPDATED = 2;  // 已更新（MVCC）


    private RecordID rid;      // 记录ID

    private byte[] data;       // 记录数据

    private byte status;       // 记录状态

    private long versionTS;    // 版本时间戳（MVCC使用）

    private long xid;          // 最后修改事务ID

    /**
     * 创建一个记录对象
     * @param rid 记录ID
     * @param data 记录数据
     * @param status 记录状态
     * @param versionTS 版本时间戳
     * @param xid 事务ID
     */
    public Record(RecordID rid, byte[] data, byte status, long versionTS, long xid) {
        this.rid = rid;
        this.data = data;
        this.status = status;
        this.versionTS = versionTS;
        this.xid = xid;
    }

    /**
     * 创建一个有效记录
     * @param rid 记录ID
     * @param data 记录数据
     * @param xid 事务ID
     */
    public Record(RecordID rid, byte[] data, long xid) {
        this(rid, data, STATUS_VALID, System.currentTimeMillis(), xid);
    }

    /**
     * 判断记录是否有效
     * @return 如果记录有效则返回true
     */
    public boolean isValid() {
        return status == STATUS_VALID;
    }

    /**
     * 判断记录是否已删除
     * @return 如果记录已删除则返回true
     */
    public boolean isDeleted() {
        return status == STATUS_DELETED;
    }

    /**
     * 判断记录是否已更新
     * @return 如果记录已更新则返回true
     */
    public boolean isUpdated() {
        return status == STATUS_UPDATED;
    }

    /**
     * 将记录标记为已删除
     * @param xid 删除操作的事务ID
     */
    public void markDeleted(long xid) {
        this.status = STATUS_DELETED;
        this.xid = xid;
        this.versionTS = System.currentTimeMillis();
    }

    /**
     * 将记录标记为已更新
     * @param xid 更新操作的事务ID
     */
    public void markUpdated(long xid) {
        this.status = STATUS_UPDATED;
        this.xid = xid;
        this.versionTS = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "Record{rid=" + rid + ", status=" + status +
                ", versionTS=" + versionTS + ", xid=" + xid +
                ", dataSize=" + (data != null ? data.length : 0) + "}";
    }
}