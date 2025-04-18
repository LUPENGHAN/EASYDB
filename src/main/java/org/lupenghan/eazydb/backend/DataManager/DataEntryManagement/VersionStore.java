package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordID;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordVersion;

import java.util.List;

/**
 * 版本存储接口，用于管理记录的版本链
 */
public interface VersionStore {
    /**
     * 添加新版本
     * @param version 新版本
     * @return 是否成功
     */
    boolean addVersion(RecordVersion version) throws Exception;

    /**
     * 获取最新版本
     * @param recordID 记录ID
     * @return 最新版本
     */
    RecordVersion getLatestVersion(RecordID recordID) throws Exception;

    /**
     * 获取指定版本
     * @param versionPointer 版本指针
     * @return 指定版本
     */
    RecordVersion getVersion(long versionPointer) throws Exception;

    /**
     * 获取版本链
     * @param recordID 记录ID
     * @return 版本链
     */
    List<RecordVersion> getVersionChain(RecordID recordID) throws Exception;

    /**
     * 更新版本的结束时间戳
     * @param versionPointer 版本指针
     * @param endTS 结束时间戳
     * @return 是否成功
     */
    boolean updateVersionEndTS(long versionPointer, long endTS) throws Exception;

    /**
     * 清理过期版本
     * @param olderThan 时间戳，早于此时间戳的版本将被清理
     * @return 清理的版本数量
     */
    int purgeOldVersions(long olderThan) throws Exception;
}