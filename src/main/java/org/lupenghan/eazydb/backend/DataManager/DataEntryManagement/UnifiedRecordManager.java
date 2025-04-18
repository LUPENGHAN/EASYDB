package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.Record;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordID;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordVersion;
import org.lupenghan.eazydb.backend.TransactionManager.utils.ReadView;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * 统一的记录管理器接口，支持基本操作和MVCC
 */
public interface UnifiedRecordManager {
    // ---- 基本记录操作 ----

    /**
     * 插入记录，返回记录ID
     * @param data 记录数据
     * @param xid 事务ID
     * @return 记录ID
     */
    RecordID insertRecord(byte[] data, long xid) throws Exception;

    /**
     * 删除记录
     * @param rid 记录ID
     * @param xid 事务ID
     * @return 是否成功
     */
    boolean deleteRecord(RecordID rid, long xid) throws Exception;

    /**
     * 更新记录
     * @param rid 记录ID
     * @param newData 新数据
     * @param xid 事务ID
     * @return 是否成功
     */
    boolean updateRecord(RecordID rid, byte[] newData, long xid) throws Exception;

    /**
     * 获取记录
     * @param rid 记录ID
     * @param xid 事务ID
     * @return 记录数据
     */
    byte[] getRecord(RecordID rid, long xid) throws Exception;

    /**
     * 扫描记录
     * @param predicate 过滤条件
     * @param xid 事务ID
     * @return 记录迭代器
     */
    Iterator<Record> scanRecords(Predicate<Record> predicate, long xid) throws Exception;

    // ---- MVCC记录操作 ----

    /**
     * 是否启用MVCC
     * @return 是否启用
     */
    boolean isMvccEnabled();

    /**
     * 启用或禁用MVCC
     * @param enabled 是否启用
     */
    void setMvccEnabled(boolean enabled);

    /**
     * 根据ReadView获取对当前事务可见的记录
     * @param rid 记录ID
     * @param readView 读视图
     * @return 记录数据
     */
    byte[] getRecordWithMVCC(RecordID rid, ReadView readView) throws Exception;

    /**
     * 获取记录的所有版本
     * @param rid 记录ID
     * @return 版本列表
     */
    List<RecordVersion> getAllVersions(RecordID rid) throws Exception;

    /**
     * 获取记录的指定版本
     * @param rid 记录ID
     * @param timestamp 时间戳
     * @return 记录版本
     */
    RecordVersion getRecordVersion(RecordID rid, long timestamp) throws Exception;

    /**
     * 清理过期版本
     * @param olderThan 时间戳
     * @return 清理的版本数量
     */
    int purgeOldVersions(long olderThan) throws Exception;
}