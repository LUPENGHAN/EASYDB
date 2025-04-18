package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordID;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordVersion;
import org.lupenghan.eazydb.backend.TransactionManager.ReadView;

import java.util.List;

/**
 * 支持MVCC的记录管理器接口
 */
public interface MVCCRecordManager extends RecordManager {
    /**
     * 根据ReadView获取对当前事务可见的记录
     * @param rid 记录ID
     * @param readView 读视图
     * @return 记录数据，如果记录不存在或不可见则返回null
     * @throws Exception 如果发生错误
     */
    byte[] getRecordWithMVCC(RecordID rid, ReadView readView) throws Exception;

    /**
     * 获取记录的所有版本
     * @param rid 记录ID
     * @return 版本列表，按时间戳递减排序
     * @throws Exception 如果发生错误
     */
    List<RecordVersion> getAllVersions(RecordID rid) throws Exception;

    /**
     * 获取记录的指定版本
     * @param rid 记录ID
     * @param timestamp 版本时间戳
     * @return 记录版本，如果不存在则返回null
     * @throws Exception 如果发生错误
     */
    RecordVersion getRecordVersion(RecordID rid, long timestamp) throws Exception;

    /**
     * 更新记录（创建新版本）
     * @param rid 记录ID
     * @param data 新数据
     * @param xid 事务ID
     * @param beginTS 版本开始时间戳
     * @return 是否成功
     * @throws Exception 如果发生错误
     */
    boolean updateRecordVersion(RecordID rid, byte[] data, long xid, long beginTS) throws Exception;

    /**
     * 删除记录（创建删除标记版本）
     * @param rid 记录ID
     * @param xid 事务ID
     * @param beginTS 版本开始时间戳
     * @return 是否成功
     * @throws Exception 如果发生错误
     */
    boolean deleteRecordVersion(RecordID rid, long xid, long beginTS) throws Exception;

    /**
     * 清理过期版本
     * @param olderThan 时间戳，早于此时间戳的版本将被清理
     * @return 清理的版本数量
     * @throws Exception 如果发生错误
     */
    int purgeOldVersions(long olderThan) throws Exception;
}